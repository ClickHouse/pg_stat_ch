#!/usr/bin/env perl
# Buffer overflow test: intentionally small buffer to test overflow behavior
# Verifies graceful degradation and proper logging

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Initialize node with TINY buffer and SLOW flush (so buffer fills up)
# Note: queue_capacity must be power of 2
my $tiny_capacity = 8;
my $node = psch_init_node('overflow',
    queue_capacity    => $tiny_capacity,
    flush_interval_ms => 60000  # 60 seconds - effectively no flush during test
);

# Reset stats
psch_reset_stats($node);

# Generate many more events than buffer can hold
my $num_queries = 100;
for my $i (1 .. $num_queries) {
    $node->safe_psql('postgres', "SELECT $i");
}

# Get stats immediately (before any flush)
my $stats = psch_get_stats($node);

# Verify buffer filled and overflow occurred
cmp_ok($stats->{enqueued}, '<=', $tiny_capacity,
    "Enqueued limited by capacity (enqueued: $stats->{enqueued}, capacity: $tiny_capacity)");

cmp_ok($stats->{dropped}, '>', 0,
    "Events were dropped due to overflow (dropped: $stats->{dropped})");

# Total should equal queries run
my $total = $stats->{enqueued} + $stats->{dropped};
is($total, $num_queries,
    "Total accounted for (enqueued + dropped = $total, expected $num_queries)");

# Queue size should be at capacity or less
cmp_ok($stats->{queue_size}, '<=', $tiny_capacity,
    "Queue size within capacity (size: $stats->{queue_size}, capacity: $tiny_capacity)");

# Check server log for overflow warning
# The warning should appear (but only once due to rate limiting)
my $log = $node->pg_log_content();
like($log, qr/queue overflow|dropped/i,
    'Overflow warning logged');

# Verify we don't spam the log with repeated warnings
my @overflow_warnings = ($log =~ /(queue overflow|dropped)/gi);
my $warning_count = scalar @overflow_warnings;
cmp_ok($warning_count, '<=', 10,
    "Overflow warning rate-limited (got $warning_count warnings)");

diag("Buffer overflow test results:");
diag("  Capacity: $tiny_capacity");
diag("  Queries run: $num_queries");
diag("  Enqueued: $stats->{enqueued}");
diag("  Dropped:  $stats->{dropped}");
diag("  Queue size: $stats->{queue_size}");
diag("  Overflow warnings in log: $warning_count");

$node->stop();
done_testing();
