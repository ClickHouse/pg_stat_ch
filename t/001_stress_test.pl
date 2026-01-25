#!/usr/bin/env perl
# Stress test: pgbench hammers the ring buffer with many concurrent clients
# Verifies no events are lost under high load

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Initialize node with adequate buffer capacity
my $node = psch_init_node('stress',
    queue_capacity    => 65536,
    flush_interval_ms => 100  # Fast flush so events get exported
);

# Reset stats before test
psch_reset_stats($node);

# Verify clean slate
my $stats = psch_get_stats($node);
is($stats->{enqueued}, 0, 'Starts with zero enqueued events');
is($stats->{dropped}, 0, 'Starts with zero dropped events');

# Run pgbench: 8 clients, 1000 transactions each = 8000 queries minimum
# Each transaction does BEGIN + SELECT + COMMIT = more events
my $nclients = 8;
my $ntxns = 1000;
my $expected_min_queries = $nclients * $ntxns;

$node->pgbench(
    "--client=$nclients --transactions=$ntxns --no-vacuum",
    0,                                # expected exit code
    [qr/processed: \d+\/\d+/],        # stdout pattern
    [qr/^$/],                         # stderr pattern (empty)
    'ring buffer stress test',
    { 'simple' => 'SELECT 1' }        # custom script
);

# Wait for background worker to flush events
# The flush_interval_ms is 100ms, so 2 seconds should be plenty
sleep(2);

# Get final stats
$stats = psch_get_stats($node);

# Verify accounting: all events should be enqueued + dropped
my $total_accounted = $stats->{enqueued} + $stats->{dropped};
cmp_ok($total_accounted, '>=', $expected_min_queries,
    "All queries accounted for (got $total_accounted, expected >= $expected_min_queries)");

# With 65536 capacity and 100ms flush, we shouldn't drop anything
is($stats->{dropped}, 0,
    'No events dropped with adequate capacity and fast flush');

# Queue should be mostly drained after flush
cmp_ok($stats->{queue_size}, '<=', 100,
    "Queue mostly drained (size: $stats->{queue_size})");

# Capacity should match what we configured
is($stats->{capacity}, 65536, 'Queue capacity matches configuration');

# Log some diagnostic info
diag("Stress test results:");
diag("  Enqueued: $stats->{enqueued}");
diag("  Dropped:  $stats->{dropped}");
diag("  Exported: $stats->{exported}");
diag("  Queue size: $stats->{queue_size}");
diag("  Usage %: $stats->{usage_pct}");

$node->stop();
done_testing();
