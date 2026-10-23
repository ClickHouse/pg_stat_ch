#!/usr/bin/env perl
# Test: CPU time of nested statements is measured per statement
#
# Nested statements (here: foreign-key RI checks run via SPI from
# ExecutorFinish) must report only the CPU they used themselves, not the CPU
# the enclosing top-level statement had used since it started. Previously a
# single CPU baseline was captured at top-level ExecutorStart, so every RI
# check of a multi-row INSERT reported roughly the INSERT's whole CPU time.
# Prerequisites: ClickHouse container must be running (docker compose -f docker/docker-compose.test.yml up -d)

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

my $node = psch_init_node_with_clickhouse('nested_cpu',
    flush_interval_ms => 100,
    batch_max => 10000
);

my $nrows = 500;

$node->safe_psql('postgres', qq{
CREATE TABLE nested_cpu_parent(id int PRIMARY KEY);
CREATE TABLE nested_cpu_child(
    id int,
    parent_id int REFERENCES nested_cpu_parent(id),
    payload text
);
INSERT INTO nested_cpu_parent SELECT g FROM generate_series(1, $nrows) g;
});

subtest 'RI check CPU time is not inflated by the outer INSERT' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # The md5 loop burns CPU in ExecutorRun before any RI check runs (RI
    # checks are queued AFTER triggers fired in ExecutorFinish), so with the
    # old single-baseline accounting every RI check would report at least the
    # CPU of this whole phase.
    $node->safe_psql('postgres', qq{
INSERT INTO nested_cpu_child
SELECT g, g,
       (SELECT max(md5(g::text || i::text)) FROM generate_series(1, 2000) i)
FROM generate_series(1, $nrows) g;
});

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    my $stats = psch_get_stats($node);
    psch_wait_for_export($node, $stats->{enqueued}, 30);

    my $ri_filter = "query_text LIKE '%FOR KEY SHARE OF%' AND query_text LIKE '%nested_cpu_parent%'";
    my $ri_count = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw WHERE $ri_filter",
        sub { $_[0] >= $nrows },
        30
    );
    is($ri_count, $nrows, "one RI check event per inserted row ($ri_count)");

    my $outer = psch_query_clickhouse(
        "SELECT cpu_user_time_us + cpu_sys_time_us, duration_us FROM pg_stat_ch.events_raw " .
        "WHERE db_operation = 'INSERT' AND query_text LIKE '%INSERT INTO nested_cpu_child%' ORDER BY ts DESC LIMIT 1"
    );
    my ($outer_cpu, $outer_duration) = split(/\t/, $outer);
    ok(defined $outer_cpu && $outer_cpu ne '', "outer INSERT event exported ($outer)");
    $outer_cpu //= 0;
    $outer_duration //= 0;

    my $ri = psch_query_clickhouse(
        "SELECT sum(cpu_user_time_us + cpu_sys_time_us), max(cpu_user_time_us + cpu_sys_time_us), " .
        "sum(duration_us) FROM pg_stat_ch.events_raw WHERE $ri_filter"
    );
    my ($ri_cpu_sum, $ri_cpu_max, $ri_duration_sum) = split(/\t/, $ri);
    diag("outer INSERT: cpu=${outer_cpu}us duration=${outer_duration}us; " .
         "RI checks: cpu_sum=${ri_cpu_sum}us cpu_max=${ri_cpu_max}us duration_sum=${ri_duration_sum}us");

    # Sanity: the INSERT did enough work for the comparison to be meaningful.
    cmp_ok($outer_cpu, '>=', 50000, 'outer INSERT used measurable CPU');

    # Nested CPU is inclusive in the parent, so all RI checks together cannot
    # exceed the outer statement's CPU (allow some slack for rusage
    # granularity). Under the old accounting this sum was ~$nrows x outer_cpu.
    cmp_ok($ri_cpu_sum, '<=', $outer_cpu + 20000,
        'sum of RI check CPU time bounded by the outer INSERT CPU time');

    # No single RI check comes anywhere near the outer statement's CPU.
    cmp_ok($ri_cpu_max, '<', $outer_cpu / 2,
        'no single RI check reports the outer INSERT CPU time');
};

# Portals end out of start order, and aborted transactions free executor
# state without ExecutorEnd. Per-query CPU state must survive the former and
# be discarded by the latter.
subtest 'cursors and aborts' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Cursor started first, other statements start and end, cursor ends last.
    $node->safe_psql('postgres', q{
BEGIN;
DECLARE nested_cpu_cur CURSOR FOR
  SELECT max(md5(g::text)) AS nested_cpu_cursor_query FROM generate_series(1, 300000) g;
SELECT count(*) AS nested_cpu_between FROM nested_cpu_parent;
FETCH ALL FROM nested_cpu_cur;
SELECT count(*) AS nested_cpu_between2 FROM nested_cpu_child;
CLOSE nested_cpu_cur;
COMMIT;
});

    # Aborts: an open cursor rolled back, and an RI failure inside ExecutorFinish.
    $node->safe_psql('postgres', q{
BEGIN;
DECLARE nested_cpu_abort_cur CURSOR FOR SELECT g FROM generate_series(1, 10) g;
FETCH 1 FROM nested_cpu_abort_cur;
ROLLBACK;
});
    my ($ret) = $node->psql('postgres',
        'INSERT INTO nested_cpu_child SELECT g, g + 1000000, NULL FROM generate_series(1, 10) g');
    isnt($ret, 0, 'FK violation raised an error');

    # The backend keeps working and nested accounting is still per statement.
    $node->safe_psql('postgres', q{
INSERT INTO nested_cpu_child
SELECT g, g, (SELECT max(md5(g::text || i::text)) FROM generate_series(1, 2000) i)
FROM generate_series(1, 100) g;
});

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    my $stats = psch_get_stats($node);
    psch_wait_for_export($node, $stats->{enqueued}, 30);

    my $cursor = psch_wait_for_clickhouse_query(
        "SELECT cpu_user_time_us + cpu_sys_time_us, duration_us FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE '%nested_cpu_cursor_query%' AND db_operation = 'SELECT' LIMIT 1",
        sub { $_[0] ne '' },
        30
    );
    my ($cursor_cpu) = split(/\t/, $cursor);
    ok(defined $cursor_cpu && $cursor_cpu > 0, "cursor query reports its CPU time ($cursor)");

    my $outer_cpu = psch_query_clickhouse(
        "SELECT cpu_user_time_us + cpu_sys_time_us FROM pg_stat_ch.events_raw " .
        "WHERE db_operation = 'INSERT' AND query_text LIKE '%INSERT INTO nested_cpu_child%' " .
        "AND rows = 100 LIMIT 1"
    );
    my $ri_cpu_sum = psch_query_clickhouse(
        "SELECT sum(cpu_user_time_us + cpu_sys_time_us) FROM pg_stat_ch.events_raw " .
        "WHERE query_text LIKE '%FOR KEY SHARE OF%' AND query_text LIKE '%nested_cpu_parent%'"
    );
    diag("after aborts: outer INSERT cpu=${outer_cpu}us; RI checks cpu_sum=${ri_cpu_sum}us");
    cmp_ok($ri_cpu_sum, '<=', $outer_cpu + 20000,
        'RI check CPU still bounded by the outer INSERT after aborts');
};

$node->safe_psql('postgres', 'DROP TABLE nested_cpu_child, nested_cpu_parent');

$node->stop();
done_testing();
