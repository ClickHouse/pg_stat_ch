#!/usr/bin/env perl
# Parent query id linkage:
#   - top-level queries report parent_query_id = 0
#   - nested SPI queries report parent_query_id = outer's query_id
#   - log events captured while a nested SPI query is on the stack report
#     queryid = the running (inner) statement and parent_query_id = its
#     outer caller — catches the CaptureLogEvent off-by-one.
#     NOTE: we use RAISE WARNING for this rather than a caught ERROR.
#     emit_log_hook does not fire from errfinish for ERROR-level events
#     (errfinish PG_RE_THROWs without calling EmitErrorReport; emit_log_hook
#     only fires from PostgresMain's top-level catch, after all frames have
#     been popped — or never, for caught-in-EXCEPTION errors).  WARNING
#     goes through EmitErrorReport directly so the frame stack is intact
#     when our hook runs.
#
# Filters key off distinctive table/function names — these survive query
# normalization, where string/numeric literals get replaced with $N
# placeholders and would not.

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

my $node = psch_init_node_with_clickhouse('parent_qid',
    flush_interval_ms => 100,
    batch_max => 100,
);

# A dedicated table whose name appears verbatim in normalized query text.
$node->safe_psql('postgres', 'CREATE TABLE pqid_top_marker(x int)');

# Test 1: Top-level queries report parent_query_id = 0
subtest 'top-level parent_query_id is 0' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT * FROM pqid_top_marker');
    $node->safe_psql('postgres', 'SELECT count(*) FROM pqid_top_marker');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $matches = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw WHERE query LIKE '%pqid_top_marker%'",
        sub { $_[0] >= 2 },
        10,
    );
    cmp_ok($matches, '>=', 2, 'top-level marker rows landed');

    my $nonzero_parents = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.events_raw "
        . "WHERE query LIKE '%pqid_top_marker%' AND parent_query_id != 0"
    );
    is($nonzero_parents, '0', 'top-level queries report parent_query_id = 0');
};

# Test 2: Nested SPI queries report parent_query_id matching the outer's queryid
subtest 'nested SPI parent_query_id links to outer' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', q{
        CREATE TABLE pqid_inner_marker(x int);
        INSERT INTO pqid_inner_marker VALUES (1);
        CREATE FUNCTION pqid_outer_caller() RETURNS int
        LANGUAGE plpgsql AS $$
        DECLARE v int;
        BEGIN
            SELECT x INTO v FROM pqid_inner_marker;
            RETURN v;
        END$$;
    });

    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT pqid_outer_caller()');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Wait for both rows.
    psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw "
        . "WHERE query LIKE '%pqid_outer_caller%' OR query LIKE '%pqid_inner_marker%'",
        sub { $_[0] >= 2 },
        10,
    );

    # Self-join: inner row's parent_query_id must equal outer row's query_id.
    my $linked = psch_query_clickhouse(q{
        SELECT count() FROM pg_stat_ch.events_raw inner_q
        JOIN pg_stat_ch.events_raw outer_q
          ON inner_q.parent_query_id = outer_q.query_id
        WHERE inner_q.query LIKE '%pqid_inner_marker%'
          AND outer_q.query LIKE '%pqid_outer_caller%'
    });
    cmp_ok($linked, '>=', 1, 'nested SPI parent_query_id matches outer query_id');

    my $orphan = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.events_raw "
        . "WHERE query LIKE '%pqid_inner_marker%' AND parent_query_id = 0"
    );
    is($orphan, '0', 'nested SPI query is not reported as top-level');

    my $outer_self = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.events_raw "
        . "WHERE query LIKE '%pqid_outer_caller%' AND parent_query_id != 0"
    );
    is($outer_self, '0', 'outer call still reports parent_query_id = 0');
};

# Test 3: Log event captured inside a nested SPI query.
#   queryid         = the running (nested) query's id
#   parent_query_id = the outer caller's id
# Catches the CaptureLogEvent off-by-one — the old code read the wrong
# slot and would have attributed the warning to the outer caller (or
# emitted query_id = 0) instead of the inner SPI statement.
subtest 'log event inside nested SPI links queryid -> outer' => sub {
    $node->safe_psql('postgres', q{
        CREATE TABLE pqid_warn_tbl(x int);
        INSERT INTO pqid_warn_tbl VALUES (1);
        CREATE FUNCTION pqid_emit_warn(x int) RETURNS int
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE WARNING 'pqid_warn_marker' USING ERRCODE = '01001';
            RETURN x;
        END$$;
        CREATE FUNCTION pqid_warn_outer() RETURNS int
        LANGUAGE plpgsql AS $$
        DECLARE v int;
        BEGIN
            SELECT pqid_emit_warn(x) INTO v FROM pqid_warn_tbl;
            RETURN v;
        END$$;
    });

    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT pqid_warn_outer()');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # The log event itself carries no query text (CaptureLogEvent leaves
    # query empty) — identify it via cmd_type=UNKNOWN + err_sqlstate=01001
    # (our RAISE WARNING's custom code).
    psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw "
        . "WHERE cmd_type = 'UNKNOWN' AND err_sqlstate = '01001'",
        sub { $_[0] >= 1 },
        10,
    );

    # parent_query_id of the warning must equal the outer caller's query_id.
    my $warn_to_outer = psch_query_clickhouse(q{
        SELECT count() FROM pg_stat_ch.events_raw warn_q
        JOIN pg_stat_ch.events_raw outer_q
          ON warn_q.parent_query_id = outer_q.query_id
        WHERE warn_q.cmd_type = 'UNKNOWN'
          AND warn_q.err_sqlstate = '01001'
          AND outer_q.query LIKE '%pqid_warn_outer%'
    });
    cmp_ok($warn_to_outer, '>=', 1,
        'warning log event: parent_query_id = outer caller');

    # query_id of the warning must equal the inner SPI statement's query_id
    # (the running query), NOT the outer caller's query_id.
    my $warn_to_inner = psch_query_clickhouse(q{
        SELECT count() FROM pg_stat_ch.events_raw warn_q
        JOIN pg_stat_ch.events_raw inner_q
          ON warn_q.query_id = inner_q.query_id
        WHERE warn_q.cmd_type = 'UNKNOWN'
          AND warn_q.err_sqlstate = '01001'
          AND inner_q.query LIKE '%pqid_emit_warn%'
    });
    cmp_ok($warn_to_inner, '>=', 1,
        'warning log event: query_id = inner SPI statement (the running query)');

    # And those two must NOT be equal — running query is the child, parent
    # is its caller.
    my $self_parent = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.events_raw "
        . "WHERE cmd_type = 'UNKNOWN' AND err_sqlstate = '01001' "
        . "  AND query_id = parent_query_id AND query_id != 0"
    );
    is($self_parent, '0',
        'log event query_id and parent_query_id are distinct (no self-parent)');
};

$node->stop();
done_testing();
