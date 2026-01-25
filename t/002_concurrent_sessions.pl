#!/usr/bin/env perl
# Concurrent sessions test: multiple BackgroundPsql sessions writing simultaneously
# Verifies the ring buffer handles concurrent writers correctly

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::BackgroundPsql;
use Test::More;

use psch;

# Initialize node
my $node = psch_init_node('concurrent',
    queue_capacity    => 4096,
    flush_interval_ms => 100
);

# Reset stats
psch_reset_stats($node);

# Spawn 4 concurrent background sessions
my $num_sessions = 4;
my $queries_per_session = 100;
my @sessions;

for my $i (1 .. $num_sessions) {
    push @sessions, $node->background_psql('postgres');
}

# Each session runs queries as fast as possible
# We use generate_series which produces multiple rows but counts as 1 query
for my $i (0 .. $#sessions) {
    my $session = $sessions[$i];
    # Run a bunch of simple queries
    $session->query_until(qr/CONCURRENT_DONE_$i/, qq{
        DO \$\$
        BEGIN
            FOR i IN 1..$queries_per_session LOOP
                PERFORM 1;
            END LOOP;
        END
        \$\$;
        SELECT 'CONCURRENT_DONE_$i';
    });
}

# Wait for all sessions to complete and close
for my $session (@sessions) {
    $session->quit();
}

# Give bgworker time to flush
sleep(1);

# Get stats
my $stats = psch_get_stats($node);

# We expect at least num_sessions * 2 events (the DO block + SELECT for each)
# The PERFORM statements inside the DO block don't generate separate events
my $min_expected = $num_sessions * 2;
my $total_accounted = $stats->{enqueued} + $stats->{dropped};

cmp_ok($total_accounted, '>=', $min_expected,
    "Concurrent sessions accounted (got $total_accounted, expected >= $min_expected)");

# Should not drop with adequate capacity
is($stats->{dropped}, 0, 'No drops with adequate capacity');

# Queue should be mostly empty after flush
cmp_ok($stats->{queue_size}, '<', 100,
    "Queue drained after concurrent load (size: $stats->{queue_size})");

diag("Concurrent sessions test results:");
diag("  Sessions: $num_sessions");
diag("  Enqueued: $stats->{enqueued}");
diag("  Dropped:  $stats->{dropped}");
diag("  Queue size: $stats->{queue_size}");

$node->stop();
done_testing();
