#!/usr/bin/env perl
# Relations export: encoding correctness (identifiers, relkinds, truncation)
# and hook robustness under nested invocations and schema changes.
# Prerequisites: ClickHouse container must be running (docker compose -f
#   docker/docker-compose.test.yml up -d); postgres_fdw gates the foreign-table case.

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping ClickHouse tests';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running. Start with: docker compose -f docker/docker-compose.test.yml up -d';
}

psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

my $node = psch_init_node_with_clickhouse('relations_export',
    flush_interval_ms => 100,
    batch_max => 100
);

$node->safe_psql('postgres', 'CREATE TABLE foo1(a int)');
$node->safe_psql('postgres', 'CREATE TABLE foo2(b int)');
$node->safe_psql('postgres', 'CREATE SCHEMA sch1');
$node->safe_psql('postgres', 'CREATE TABLE sch1.foo3(c int)');
$node->safe_psql('postgres', 'CREATE VIEW v1 AS SELECT * FROM foo1');

subtest 'plain table' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT * FROM foo1');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw WHERE query_text LIKE 'SELECT * FROM foo1%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/'foo1'/, "single table appears as foo1 (got: $names)");
};

subtest 'two plain tables' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT * FROM foo1, foo2');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw WHERE query_text LIKE 'SELECT * FROM foo1, foo2%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/'foo1'/, "includes foo1 (got: $names)");
    like($names, qr/'foo2'/, "includes foo2 (got: $names)");
};

subtest 'schema-qualified table' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT * FROM sch1.foo3, foo1');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $namespaces = psch_wait_for_clickhouse_query(
        "SELECT relation_namespaces FROM pg_stat_ch.events_raw WHERE query_text LIKE 'SELECT * FROM sch1.foo3%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    my $names = psch_query_clickhouse(
        "SELECT relation_names FROM pg_stat_ch.events_raw WHERE query_text LIKE 'SELECT * FROM sch1.foo3%' LIMIT 1"
    );
    like($namespaces, qr/'sch1'/, "sch1 namespace present (got: $namespaces)");
    like($names, qr/'foo3'/, "foo3 present (got: $names)");
    like($names, qr/'foo1'/, "foo1 also present (got: $names)");
};

subtest 'self-join deduplicates' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT * FROM foo1 AS a, foo1 AS b');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw WHERE query_text LIKE 'SELECT * FROM foo1 AS a%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    my $count = () = $names =~ /'foo1'/g;
    is($count, 1, "foo1 listed once, not twice (got: $names)");
};

subtest 'view reports relkind v' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT * FROM v1');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw WHERE query_text LIKE 'SELECT * FROM v1%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    my $kinds = psch_query_clickhouse(
        "SELECT relation_kinds FROM pg_stat_ch.events_raw WHERE query_text LIKE 'SELECT * FROM v1%' LIMIT 1"
    );
    like($names, qr/'v1'/, "v1 present (got: $names)");
    like($kinds, qr/'v'/, "v1 reports relkind 'v' (got: $kinds)");
    like($names, qr/'foo1'/, "underlying base table also present (got: $names)");
};

subtest 'truncation over budget is flagged and does not error' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # SET/RESET are session-local; safe_psql opens a fresh connection per
    # call, so SET and the query it affects must run in the same call.
    $node->safe_psql('postgres',
        'SET pg_stat_ch.relations_max_len = 8; ' .
        'SELECT * FROM foo1, foo2, sch1.foo3; ' .
        'RESET pg_stat_ch.relations_max_len');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{dsa_oom}, '==', 0, 'no DSA OOM from the tiny budget');

    my $ch_count = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw",
        sub { $_[0] >= 1 },
        10
    );
    cmp_ok($ch_count, '>=', 1, 'event still exported despite truncation');

    my $truncated = psch_query_clickhouse(
        "SELECT relations_truncated FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT * FROM foo1, foo2, sch1.foo3%' LIMIT 1"
    );
    is($truncated, 'true', "truncation is explicitly flagged (got: $truncated)");

    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    $node->safe_psql('postgres', 'SELECT * FROM foo1, foo2, sch1.foo3');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $not_truncated = psch_wait_for_clickhouse_query(
        "SELECT relations_truncated FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT * FROM foo1, foo2, sch1.foo3%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    is($not_truncated, 'false', "complete list is explicitly not flagged (got: $not_truncated)");
};

subtest 'minimum budget (header-only) produces an empty, truncated list' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # 2 is the new GUC floor -- room for the header, none for any entry.
    $node->safe_psql('postgres',
        'SET pg_stat_ch.relations_max_len = 2; ' .
        'SELECT * FROM foo1; ' .
        'RESET pg_stat_ch.relations_max_len');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{dsa_oom}, '==', 0, 'no DSA OOM at the minimum budget');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT * FROM foo1%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    is($names, '[]', "relation list is genuinely empty, not just under-filled (got: $names)");

    my $truncated = psch_query_clickhouse(
        "SELECT relations_truncated FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT * FROM foo1%' LIMIT 1"
    );
    is($truncated, 'true', "truncation is flagged even though zero entries fit (got: $truncated)");
};

subtest 'relations_enabled=off reports empty but not truncated' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres',
        'SET pg_stat_ch.relations_enabled = off; ' .
        'SELECT * FROM foo1, foo2; ' .
        'RESET pg_stat_ch.relations_enabled');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT * FROM foo1, foo2%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    is($names, '[]', "relation list is empty while disabled (got: $names)");

    my $truncated = psch_query_clickhouse(
        "SELECT relations_truncated FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT * FROM foo1, foo2%' LIMIT 1"
    );
    is($truncated, 'false', "opt-out is not reported as truncation (got: $truncated)");
};

# Encoding correctness: special characters, every relkind, the truncation
# marker, and the exact byte-budget boundary.

subtest 'identifiers with special characters stay unambiguous' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'CREATE TABLE "weird.name"(b int)');
    $node->safe_psql('postgres', 'CREATE TABLE "has,comma"(c int)');
    $node->safe_psql('postgres', 'CREATE TABLE "has""quote"(d int)');
    $node->safe_psql('postgres',
        'SELECT * FROM "weird.name", "has,comma", "has""quote"');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE '%weird.name%has,comma%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );

    # A comma or dot inside the name must not be treated as an element
    # separator or namespace/name boundary.
    like($names, qr/'has,comma'/, "has,comma survives as one element (got: $names)");
    like($names, qr/'weird\.name'/, "weird.name survives as one element, dot intact (got: $names)");
    like($names, qr/'has"quote'/, 'has"quote survives with embedded quote intact (got: ' . $names . ')');

    # Let ClickHouse count array elements itself -- counting raw commas in
    # the text form is wrong, since 'has,comma' has a literal comma inside
    # its own quoted element, not just between elements.
    my $count = psch_query_clickhouse(
        "SELECT length(relation_names) FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE '%weird.name%has,comma%' LIMIT 1"
    );
    is($count, '3', "exactly 3 elements, no extra split from the embedded comma (got names: $names)");
};

subtest 'all relkinds are reported distinctly' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # postgres_fdw is a contrib module; CI's TAP-enabled PostgreSQL build
    # (./configure --enable-tap-tests && make && make install, no
    # `make -C contrib install`) does not build it, so foreign-table
    # coverage degrades gracefully instead of hard-failing there.
    my $has_fdw = $node->safe_psql('postgres',
        "SELECT 1 FROM pg_available_extensions WHERE name = 'postgres_fdw'");

    $node->safe_psql('postgres', 'CREATE TABLE rk_table(a int)');
    $node->safe_psql('postgres', 'CREATE VIEW rk_view AS SELECT * FROM rk_table');
    $node->safe_psql('postgres', 'CREATE MATERIALIZED VIEW rk_matview AS SELECT * FROM rk_table');
    $node->safe_psql('postgres',
        'CREATE TABLE rk_part(id int, val text) PARTITION BY RANGE (id)');
    $node->safe_psql('postgres',
        'CREATE TABLE rk_part_child PARTITION OF rk_part FOR VALUES FROM (0) TO (100)');

    my $touch_tables = 'rk_table, rk_view, rk_matview, rk_part';
    if ($has_fdw eq '1') {
        $node->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS postgres_fdw');
        $node->safe_psql('postgres',
            "CREATE SERVER rk_loopback FOREIGN DATA WRAPPER postgres_fdw " .
            "OPTIONS (host 'localhost', port '" . $node->port . "', dbname 'postgres')");
        $node->safe_psql('postgres',
            "CREATE USER MAPPING FOR CURRENT_USER SERVER rk_loopback");
        $node->safe_psql('postgres',
            "CREATE FOREIGN TABLE rk_ftable(a int) SERVER rk_loopback OPTIONS (table_name 'rk_table')");
        $touch_tables .= ', rk_ftable';
    }

    $node->safe_psql('postgres', "SELECT * FROM $touch_tables");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $kinds = psch_wait_for_clickhouse_query(
        "SELECT relation_kinds FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE '%rk_table, rk_view%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );

    like($kinds, qr/'r'/, "plain table reports relkind 'r' (got: $kinds)");
    like($kinds, qr/'v'/, "view reports relkind 'v' (got: $kinds)");
    like($kinds, qr/'m'/, "materialized view reports relkind 'm' (got: $kinds)");
    like($kinds, qr/'p'/, "partitioned table reports relkind 'p' (got: $kinds)");

  SKIP: {
        skip 'postgres_fdw not available in this PostgreSQL build', 1
            unless $has_fdw eq '1';
        like($kinds, qr/'f'/, "foreign table reports relkind 'f' (got: $kinds)");
    }
};

subtest 'maximum-length identifier round-trips intact' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # NAMEDATALEN-1 (63) is the real max a Postgres identifier can hold --
    # anything longer is silently truncated by Postgres itself at parse time.
    my $max_name = 'a' x 63;
    $node->safe_psql('postgres', "CREATE TABLE $max_name(x int)");
    $node->safe_psql('postgres', "SELECT * FROM $max_name");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw WHERE length(relation_names) > 0 ORDER BY ts DESC LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/\Q$max_name\E/, "63-byte name round-trips without truncation or corruption (got: $names)");
};

subtest 'identifier made entirely of quote characters round-trips intact' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Craig's example: a table name that is itself all double-quote
    # characters. A length-prefixed field needs no escaping for this --
    # it's just 63 raw 0x22 bytes with an explicit length prefix.
    my $quote_name = '"' x 63;                    # the real, unquoted name
    my $sql_literal = '"' . ('""' x 63) . '"';     # how to spell it in SQL
    $node->safe_psql('postgres', "CREATE TABLE $sql_literal(x int)");
    $node->safe_psql('postgres', "SELECT * FROM $sql_literal");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw WHERE length(relation_names) > 0 ORDER BY ts DESC LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/\Q$quote_name\E/, "all-quote-character name survives intact, 63 raw bytes (got: $names)");
};

subtest 'exactly 7 max-length relations fit the 1023-byte budget, the 8th is dropped' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Worst-case per-relation cost: 1(ns_len) + 63(ns) + 1(name_len) +
    # 63(name) + 1(relkind) = 129 bytes. (1023 - 2-byte header) / 129 = 7.9,
    # so 7 fit and an 8th must be dropped with the truncated flag set.
    my $ns63 = 'n' x 63;
    $node->safe_psql('postgres', "CREATE SCHEMA $ns63");
    my @tables;
    for my $i (1 .. 8) {
        my $tname = ('r' x 61) . sprintf('%02d', $i);
        push @tables, "$ns63.$tname";
        $node->safe_psql('postgres', "CREATE TABLE $ns63.$tname(x int)");
    }
    $node->safe_psql('postgres', 'SELECT * FROM ' . join(', ', @tables));
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $count = psch_wait_for_clickhouse_query(
        "SELECT length(relation_names) FROM pg_stat_ch.events_raw WHERE length(relation_names) > 0 ORDER BY ts DESC LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    is($count, '7', "exactly 7 of 8 max-length relations fit the 1023-byte budget (got: $count)");

    my $truncated = psch_query_clickhouse(
        "SELECT relations_truncated FROM pg_stat_ch.events_raw WHERE length(relation_names) > 0 ORDER BY ts DESC LIMIT 1"
    );
    is($truncated, 'true', "the dropped 8th relation is explicitly flagged (got: $truncated)");
};

# Hook robustness: nested executor invocations (PL/pgSQL, dynamic EXECUTE,
# nested PL/pgSQL) must not clobber the outer statement's relations.

$node->safe_psql('postgres', 'CREATE TABLE outer_t(a int)');
$node->safe_psql('postgres', 'CREATE TABLE inner_t(b int)');
$node->safe_psql('postgres', q{
    CREATE FUNCTION touches_inner(x int) RETURNS bigint AS $BODY$
        SELECT count(*) FROM inner_t;
    $BODY$ LANGUAGE sql STRICT;
});
# Must have real rows: the per-row function call -- and the nested executor
# invocation it triggers -- never happens at all against an empty table.
$node->safe_psql('postgres', 'INSERT INTO outer_t VALUES (1), (2)');
$node->safe_psql('postgres', 'INSERT INTO inner_t VALUES (10)');

subtest 'nested executor invocation does not clobber the outer statement relations' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT a, touches_inner(a) FROM outer_t');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT a, touches_inner(a) FROM outer_t%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/'outer_t'/, "outer statement reports outer_t, not clobbered by the nested call (got: $names)");
    unlike($names, qr/'inner_t'/, "outer statement's own relations do not include inner_t (got: $names)");
};

$node->safe_psql('postgres', q{
    CREATE FUNCTION plpgsql_touches_inner(x int) RETURNS bigint AS $BODY$
    DECLARE
        cnt bigint;
    BEGIN
        SELECT count(*) INTO cnt FROM inner_t;
        RETURN cnt;
    END;
    $BODY$ LANGUAGE plpgsql;
});

subtest 'PL/pgSQL function body does not clobber the caller relations' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT a, plpgsql_touches_inner(a) FROM outer_t');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT a, plpgsql_touches_inner(a) FROM outer_t%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/'outer_t'/, "outer statement reports outer_t (got: $names)");
    unlike($names, qr/'inner_t'/, "not clobbered by the PL/pgSQL function's own query (got: $names)");
};

$node->safe_psql('postgres', q{
    CREATE FUNCTION plpgsql_touches_inner_dynamic(x int) RETURNS bigint AS $BODY$
    DECLARE
        cnt bigint;
    BEGIN
        EXECUTE 'SELECT count(*) FROM inner_t' INTO cnt;
        RETURN cnt;
    END;
    $BODY$ LANGUAGE plpgsql;
});

subtest 'PL/pgSQL dynamic EXECUTE does not clobber the caller relations' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT a, plpgsql_touches_inner_dynamic(a) FROM outer_t');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT a, plpgsql_touches_inner_dynamic(a) FROM outer_t%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/'outer_t'/, "outer statement reports outer_t (got: $names)");
    unlike($names, qr/'inner_t'/, "not clobbered by the dynamic EXECUTE's query (got: $names)");
};

$node->safe_psql('postgres', 'CREATE TABLE inner_t2(c int)');
$node->safe_psql('postgres', 'INSERT INTO inner_t2 VALUES (20)');
$node->safe_psql('postgres', q{
    CREATE FUNCTION plpgsql_level2() RETURNS bigint AS $BODY$
    DECLARE
        cnt bigint;
    BEGIN
        SELECT count(*) INTO cnt FROM inner_t2;
        RETURN cnt;
    END;
    $BODY$ LANGUAGE plpgsql;
});
$node->safe_psql('postgres', q{
    CREATE FUNCTION plpgsql_level1(x int) RETURNS bigint AS $BODY$
    DECLARE
        cnt bigint;
    BEGIN
        SELECT count(*) INTO cnt FROM inner_t;
        RETURN cnt + plpgsql_level2();
    END;
    $BODY$ LANGUAGE plpgsql;
});

subtest 'nested PL/pgSQL calling PL/pgSQL does not clobber the caller relations' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT a, plpgsql_level1(a) FROM outer_t');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT a, plpgsql_level1(a) FROM outer_t%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/'outer_t'/, "outer statement reports outer_t (got: $names)");
    unlike($names, qr/'inner_t'/, "not clobbered by the first level of PL/pgSQL nesting (got: $names)");
    unlike($names, qr/'inner_t2'/, "not clobbered by the second level of PL/pgSQL nesting (got: $names)");
};

subtest 'extension invoking SPI does not clobber the caller relations' => sub {
    # tablefunc is a contrib module; CI's TAP-enabled PostgreSQL build
    # (./configure --enable-tap-tests && make && make install, no
    # `make -C contrib install`) does not build it, same gap as postgres_fdw
    # above.
    my $has_tablefunc = $node->safe_psql('postgres',
        "SELECT 1 FROM pg_available_extensions WHERE name = 'tablefunc'");

  SKIP: {
        skip 'tablefunc not available in this PostgreSQL build', 2
            unless $has_tablefunc eq '1';

        psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
        psch_reset_stats($node);

        $node->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS tablefunc');
        $node->safe_psql('postgres', q{
            SELECT a FROM outer_t,
                crosstab('SELECT 1, 1, b FROM inner_t') AS ct(row_name int, val int)
        });
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

        my $names = psch_wait_for_clickhouse_query(
            "SELECT relation_names FROM pg_stat_ch.events_raw " .
            "WHERE query_text LIKE 'SELECT a FROM outer_t,%crosstab%' LIMIT 1",
            sub { $_[0] ne '' },
            10
        );
        like($names, qr/'outer_t'/, "outer statement reports outer_t (got: $names)");
        unlike($names, qr/'inner_t'/, "not clobbered by crosstab's internal SPI query (got: $names)");
    }
};

# Hook robustness: relation drop/recreate and rename must not leave stale
# state behind in CollectRelations's catalog-lookup path.

subtest 'table dropped and recreated with the same name reports cleanly' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'CREATE TABLE drtab(a int)');
    $node->safe_psql('postgres', 'SELECT * FROM drtab');
    $node->safe_psql('postgres', 'DROP TABLE drtab');
    $node->safe_psql('postgres', 'CREATE TABLE drtab(b text)');
    $node->safe_psql('postgres', 'SELECT * FROM drtab');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{dsa_oom}, '==', 0, 'no crash/OOM across drop-and-recreate');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT * FROM drtab%' ORDER BY ts DESC LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/'drtab'/, "post-recreate query still reports drtab cleanly (got: $names)");
};

subtest 'renamed table is reported under its new name' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'CREATE TABLE old_name_tab(a int)');
    $node->safe_psql('postgres', 'ALTER TABLE old_name_tab RENAME TO new_name_tab');
    $node->safe_psql('postgres', 'SELECT * FROM new_name_tab');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT * FROM new_name_tab%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/'new_name_tab'/, "query by the new name reports new_name_tab (got: $names)");
    unlike($names, qr/'old_name_tab'/, "no stale old name lingers anywhere (got: $names)");
};

subtest 'relations nesting depth recovers after a run of unsampled queries' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'CREATE TABLE nest_recover_t(a int)');

    # 20 deterministically-unsampled queries (sample_rate=0, high
    # min_duration_us) used to permanently saturate the nesting-depth stack.
    my $sql = 'SET pg_stat_ch.sample_rate = 0.0; ' .
        'SET pg_stat_ch.min_duration_us = 2000000000; ' .
        ('SELECT * FROM nest_recover_t; ' x 20) .
        'RESET pg_stat_ch.sample_rate; ' .
        'RESET pg_stat_ch.min_duration_us; ' .
        'SELECT * FROM nest_recover_t;';
    $node->safe_psql('postgres', $sql);
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT * FROM nest_recover_t%' ORDER BY ts DESC LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/'nest_recover_t'/,
        "relations still captured after a 20-query unsampled streak (got: $names)");
};

subtest 'nesting depth recovers after permission-check failures inside a PL/pgSQL exception' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'CREATE TABLE perm_fail_t(a int)');
    $node->safe_psql('postgres', 'CREATE ROLE perm_fail_role');

    # Each iteration fails inside ExecutorStart itself (permission denied,
    # before any execution), caught by the PL/pgSQL exception block -- which
    # runs it as its own subtransaction. 20 of these used to leak a nesting
    # level every time with no way to recover.
    my $sql =
        'SET ROLE perm_fail_role; ' .
        q{DO $$ BEGIN
            FOR i IN 1..20 LOOP
              BEGIN
                EXECUTE 'SELECT * FROM perm_fail_t';
              EXCEPTION WHEN insufficient_privilege THEN NULL;
              END;
            END LOOP;
          END $$;} .
        ' RESET ROLE; ' .
        'SELECT * FROM perm_fail_t;';
    $node->safe_psql('postgres', $sql);
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT * FROM perm_fail_t%' ORDER BY ts DESC LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/'perm_fail_t'/,
        "relations still captured after 20 permission-check failures (got: $names)");
};

subtest 'nesting depth recovers after a run of COPY statements' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'CREATE TABLE copy_leak_t(a int)');

    # COPY's own permission check bypasses ExecutorStart/End entirely, so 20
    # of these used to leak a nesting level every time with no way to recover.
    my $sql = ("COPY copy_leak_t TO '/dev/null'; " x 20) .
        'SELECT * FROM copy_leak_t;';
    $node->safe_psql('postgres', $sql);
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $names = psch_wait_for_clickhouse_query(
        "SELECT relation_names FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE 'SELECT * FROM copy_leak_t%' ORDER BY ts DESC LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($names, qr/'copy_leak_t'/,
        "relations still captured after 20 COPY statements (got: $names)");
};

$node->stop();
done_testing();
