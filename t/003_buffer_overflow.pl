#!/usr/bin/env perl
# Buffer overflow test: deliberately fill the buffer to test overflow behavior
#
# This test verifies that the lock-free overflow path works correctly:
# - Queue fills up to capacity
# - Events are dropped and counted
# - Completes quickly even under overflow (lock-free fast path)

use strict;
use warnings;
use lib 't';
use Test::More;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use psch;

# Use small capacity to trigger overflow quickly
my $capacity = 1024;
my $node = psch_init_node('overflow',
    queue_capacity    => $capacity,
    flush_interval_ms => 60000  # Long interval - don't flush during test
);

psch_reset_stats($node);
my $stats_before = psch_get_stats($node);

# Generate more events than buffer can hold
# With lock-free overflow, this should complete quickly
my $nclients = 4;
my $ntxns = 1250;  # 4 * 1250 = 5000 queries, well over 1024 capacity
$node->pgbench(
    "--client=$nclients --transactions=$ntxns --no-vacuum",
    0, [qr/processed: \d+\/\d+/], [qr/^$/],
    'overflow stress test',
    { 'simple' => 'SELECT 1' }
);

my $stats = psch_get_stats($node);

# Verify overflow behavior
cmp_ok($stats->{dropped}, '>', 0, "Events were dropped due to overflow");
cmp_ok($stats->{enqueued}, '>', 0, "Some events were enqueued");
cmp_ok($stats->{enqueued} + $stats->{dropped}, '>=', $nclients * $ntxns,
    "Total events accounted for (enqueued + dropped >= queries)");

# Queue should be at or near capacity
cmp_ok($stats->{queue_size}, '<=', $capacity, "Queue size does not exceed capacity");
is($stats->{capacity}, $capacity, "Queue capacity matches configuration");

note("Stats: enqueued=$stats->{enqueued}, dropped=$stats->{dropped}, queue_size=$stats->{queue_size}");

$node->stop();
done_testing();
