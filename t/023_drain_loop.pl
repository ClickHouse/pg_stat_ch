#!/usr/bin/env perl
# Test: Drain loop - bgworker exports all queued batches per cycle
#
# Verifies that the bgworker drains the entire queue in one wake-up cycle
# rather than exporting a single batch and sleeping. Uses a large
# flush_interval (10s) with a small batch_max (100) so that without the
# drain loop, 500 events would take ~50s (5 batches × 10s sleep). With
# the drain loop, all 500 export within seconds of a single flush signal.
#
# Prerequisites: ClickHouse container running

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time sleep);

use psch;

# Skip if Docker/ClickHouse not available
if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping ClickHouse tests';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running';
}

psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

# Large flush interval so the worker sleeps long between cycles.
# Small batch_max so multiple batches are needed to drain.
# Without the drain loop, only 1 batch (100 events) per 10s cycle.
my $flush_interval_ms = 10000;
my $batch_max = 100;
my $num_events = 500;

my $node = psch_init_node_with_clickhouse('drain_loop',
    flush_interval_ms => $flush_interval_ms,
    batch_max         => $batch_max,
);

# Wait for initial ClickHouse connection to establish
sleep(1);

subtest 'drain loop exports all batches in one cycle' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Generate burst of events (more than batch_max)
    for my $i (1..$num_events) {
        $node->safe_psql('postgres', "SELECT $i");
    }

    my $stats_before = psch_get_stats($node);
    cmp_ok($stats_before->{enqueued}, '>=', $num_events,
        "enqueued at least $num_events events");

    # Wake the worker with a single flush signal
    my $t0 = time();
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Wait for all events to be exported. The drain loop should finish
    # all 5 batches within a few seconds. Without the drain loop this
    # would need ~50s (5 × 10s sleep).
    my $timeout = 8;
    my $exported = psch_wait_for_export($node, $num_events, $timeout);
    my $elapsed = time() - $t0;

    cmp_ok($exported, '>=', $num_events,
        "all $num_events events exported (got $exported)");
    cmp_ok($elapsed, '<', $flush_interval_ms / 1000,
        "drain completed in ${elapsed}s, well under one flush interval (${\ ($flush_interval_ms / 1000)}s)");

    # Queue should be nearly empty; a small residual (1-2) is expected because
    # the flush() and stats() calls themselves generate events after the drain.
    my $stats_after = psch_get_stats($node);
    is($stats_after->{dropped}, 0, 'no events dropped');

    # Cross-check with ClickHouse
    my $ch_count = psch_query_clickhouse("SELECT count() FROM pg_stat_ch.events_raw");
    cmp_ok($ch_count, '>=', $num_events,
        "ClickHouse has all events (got $ch_count)");
};

subtest 'drain loop handles multiple cycles correctly' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # First burst
    for my $i (1..200) {
        $node->safe_psql('postgres', "SELECT $i");
    }
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    psch_wait_for_export($node, 200, 8);

    # Second burst
    for my $i (1..200) {
        $node->safe_psql('postgres', "SELECT $i + 200");
    }
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    psch_wait_for_export($node, 400, 8);

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{exported}, '>=', 400,
        'both bursts fully exported');
    cmp_ok($stats->{queue_size}, '<', $batch_max,
        "queue nearly drained (residual $stats->{queue_size} < batch_max $batch_max)");
    is($stats->{dropped}, 0, 'no drops across cycles');
};

$node->stop();
done_testing();
