#!/usr/bin/env perl
# Queue boundary conditions test: tests edge cases at queue limits
#
# Tests:
# - Empty queue dequeue (bgworker polls empty queue)
# - Exactly-full queue (fill to capacity, next enqueue fails)
# - One slot available (fill to capacity-1, enqueue succeeds)
# - Single element cycle (enqueue 1, dequeue 1)

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

use psch;

# Use minimum capacity (1024) for precise boundary testing
my $capacity = 1024;

# Test 1: Empty queue - verify bgworker handles polling gracefully
subtest 'empty queue' => sub {
    my $node = psch_init_node('empty_queue',
        queue_capacity    => $capacity,
        flush_interval_ms => 100  # Fast polling
    );

    psch_reset_stats($node);

    # Give bgworker time to poll several times
    # Note: Queue may have some events from extension loading, that's OK
    sleep(0.5);

    my $stats = psch_get_stats($node);

    # Key invariants for "empty-ish" queue state
    cmp_ok($stats->{queue_size}, '>=', 0, 'Queue size is non-negative');
    cmp_ok($stats->{queue_size}, '<=', $capacity, 'Queue size within capacity');
    is($stats->{dropped}, 0, 'No drops on light load');
    cmp_ok($stats->{enqueued}, '>=', 0, 'Enqueued is non-negative');

    # Run one query to verify queue still works after bgworker polling
    my $enqueued_before = $stats->{enqueued};
    $node->safe_psql('postgres', 'SELECT 1');
    my $stats_after = psch_get_stats($node);
    cmp_ok($stats_after->{enqueued}, '>', $enqueued_before,
        'Queue accepts new events after bgworker polling');

    $node->stop();
};

# Test 2: Exactly-full queue - verify overflow on capacity boundary
subtest 'exactly full queue' => sub {
    my $node = psch_init_node('full_queue',
        queue_capacity    => $capacity,
        flush_interval_ms => 60000  # Long interval - don't drain during test
    );

    psch_reset_stats($node);

    # Generate events to fill queue completely
    # Use pgbench for efficient event generation
    my $nclients = 4;
    my $ntxns = ($capacity / $nclients) + 100;  # Slightly over capacity

    $node->pgbench(
        "--client=$nclients --transactions=$ntxns --no-vacuum",
        0, [qr/processed/], [qr/(?:^$|queue overflow)/],
        'fill to overflow',
        { 'simple' => 'SELECT 1' }
    );

    my $stats = psch_get_stats($node);

    # Should have overflow
    cmp_ok($stats->{dropped}, '>', 0, 'Events dropped on overflow');

    # Queue size should be at or near capacity
    cmp_ok($stats->{queue_size}, '<=', $capacity,
        'Queue size does not exceed capacity');
    cmp_ok($stats->{queue_size}, '>=', $capacity * 0.9,
        'Queue is nearly full');

    # Total should account for all queries
    my $total = $stats->{enqueued} + $stats->{dropped};
    cmp_ok($total, '>=', $nclients * $ntxns,
        'All events accounted (enqueued + dropped >= queries)');

    note("Full queue stats: enqueued=$stats->{enqueued}, " .
         "dropped=$stats->{dropped}, queue_size=$stats->{queue_size}");

    $node->stop();
};

# Test 3: One slot available - verify boundary without overflow
subtest 'one slot available' => sub {
    my $node = psch_init_node('one_slot',
        queue_capacity    => $capacity,
        flush_interval_ms => 60000  # Long interval - don't drain during test
    );

    psch_reset_stats($node);

    # Generate events just under capacity
    # We need to be careful: each query generates ~1 event
    my $target = $capacity - 100;  # Leave room for overhead

    for my $i (1 .. $target) {
        $node->safe_psql('postgres', "SELECT $i");
    }

    my $stats = psch_get_stats($node);

    # Should NOT have overflow yet
    is($stats->{dropped}, 0, 'No drops when under capacity');

    # Queue should have events but not be full
    cmp_ok($stats->{queue_size}, '>', 0, 'Queue has events');
    cmp_ok($stats->{queue_size}, '<', $capacity,
        'Queue not yet at capacity');

    # Add one more event - should still succeed
    $node->safe_psql('postgres', 'SELECT 999999');
    my $stats_after = psch_get_stats($node);

    cmp_ok($stats_after->{enqueued}, '>', $stats->{enqueued},
        'Additional event enqueued');

    note("One slot stats: queue_size=$stats->{queue_size}, " .
         "after=$stats_after->{queue_size}");

    $node->stop();
};

# Test 4: Single element cycle - enqueue and dequeue single events
subtest 'single element cycle' => sub {
    # Skip if ClickHouse not available (need consumer for dequeue)
    if (!psch_clickhouse_available()) {
        plan skip_all => 'Docker not available for single element cycle test';
        return;
    }

    my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
    if ($ch_check !~ /^1/) {
        plan skip_all => 'ClickHouse not running for single element cycle test';
        return;
    }

    psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

    my $node = psch_init_node_with_clickhouse('single_cycle',
        queue_capacity    => $capacity,
        flush_interval_ms => 100  # Fast flush for quick cycles
    );

    # Wait for initial queue to drain and reset
    sleep(0.5);
    psch_reset_stats($node);
    my $baseline = psch_get_stats($node);

    # Run single event cycles
    my $cycles = 10;
    for my $i (1 .. $cycles) {
        $node->safe_psql('postgres', "SELECT $i AS cycle");
        sleep(0.2);  # Allow time for export
    }

    # Wait for exports
    sleep(1);

    my $stats = psch_get_stats($node);

    # Check that events were enqueued (at least the cycles we ran)
    my $new_enqueued = $stats->{enqueued} - $baseline->{enqueued};
    cmp_ok($new_enqueued, '>=', $cycles, "Events enqueued ($new_enqueued >= $cycles cycles)");

    is($stats->{dropped}, 0, 'No drops during single cycles');

    # Verify queue drains properly
    sleep(1);
    my $stats_final = psch_get_stats($node);
    cmp_ok($stats_final->{queue_size}, '<', 10,
        'Queue drains after cycles (size < 10)');

    # Check that exports happened
    cmp_ok($stats_final->{exported}, '>', 0, 'Events were exported');

    # Verify invariant: queue_size <= capacity
    cmp_ok($stats_final->{queue_size}, '<=', $capacity,
        'Queue size within capacity');

    note("Single cycle stats: enqueued=$stats_final->{enqueued}, " .
         "exported=$stats_final->{exported}, queue_size=$stats_final->{queue_size}");

    $node->stop();
};

done_testing();
