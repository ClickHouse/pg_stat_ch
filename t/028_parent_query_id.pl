#!/usr/bin/env perl
# Parent query id linkage:
#   - top-level queries report parent_query_id = 0
#   - nested SPI queries report parent_query_id = outer's query_id
#   - log/error events report queryid = the running query and parent_query_id
#     = its caller (catches the CaptureLogEvent off-by-one where reading slot
#     nesting_level - 1 returns the *running* query, not its parent)
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

# Test 3: Error event captured inside a nested SPI query.
#   queryid         = the running (nested) query's id
#   parent_query_id = the outer caller's id
# Catches the CaptureLogEvent off-by-one: reading slot nesting_level - 1
# returns the running query (itself), not its caller.
subtest 'error inside nested SPI links queryid -> outer' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', q{
        CREATE TABLE pqid_err_inner(x int);
        INSERT INTO pqid_err_inner VALUES (0);
        CREATE FUNCTION pqid_err_outer() RETURNS int
        LANGUAGE plpgsql AS $$
        DECLARE v int;
        BEGIN
            BEGIN
                SELECT 1 / x INTO v FROM pqid_err_inner;
            EXCEPTION WHEN division_by_zero THEN
                NULL;
            END;
            RETURN 1;
        END$$;
    });

    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT pqid_err_outer()');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # The error-aborts longjmp means the inner SELECT never reaches its
    # ExecutorEnd, so we won't find a query-text row for it.  The error event
    # itself (cmd_type=UNKNOWN, err_sqlstate=22012 for division_by_zero) is
    # what carries the linkage signal.
    psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw "
        . "WHERE cmd_type = 'UNKNOWN' AND err_sqlstate = '22012'",
        sub { $_[0] >= 1 },
        10,
    );

    # Before the off-by-one fix, parent_query_id of a log event captured during
    # nested execution was the running query's *own* id (slot nesting_level - 1),
    # which would not match the outer caller's query_id. After the fix it
    # correctly points to the outer (slot nesting_level - 2).
    my $err_to_outer = psch_query_clickhouse(q{
        SELECT count() FROM pg_stat_ch.events_raw err_q
        JOIN pg_stat_ch.events_raw outer_q
          ON err_q.parent_query_id = outer_q.query_id
        WHERE err_q.cmd_type = 'UNKNOWN'
          AND err_q.err_sqlstate = '22012'
          AND outer_q.query LIKE '%pqid_err_outer%'
    });
    cmp_ok($err_to_outer, '>=', 1,
        'div-by-zero log event: parent_query_id = outer caller');

    # Before the fix, queryid was always 0 on log events.  After the fix it is
    # the currently-running (nested) statement's id, which we don't have a
    # textual handle on, but it must at least be non-zero.
    my $err_qid_nonzero = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.events_raw "
        . "WHERE cmd_type = 'UNKNOWN' AND err_sqlstate = '22012' AND query_id != 0"
    );
    cmp_ok($err_qid_nonzero, '>=', 1,
        'div-by-zero log event: query_id is the running statement (non-zero)');

    # And those two must NOT be equal — running query is the child, parent
    # is its caller.
    my $running_vs_parent = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.events_raw "
        . "WHERE cmd_type = 'UNKNOWN' AND err_sqlstate = '22012' "
        . "  AND query_id = parent_query_id AND query_id != 0"
    );
    is($running_vs_parent, '0',
        'log event query_id and parent_query_id are distinct (no self-parent)');
};

$node->stop();
done_testing();
