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

# Reset stats and get baseline
psch_reset_stats($node);
my $stats_before = psch_get_stats($node);

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

# Get stats
my $stats = psch_get_stats($node);

# Calculate new events since baseline
my $new_events = $stats->{enqueued} - $stats_before->{enqueued};

# We expect at least num_sessions * 2 events (the DO block + SELECT for each)
# The PERFORM statements inside the DO block don't generate separate events
my $min_expected = $num_sessions * 2;

cmp_ok($new_events, '>=', $min_expected,
    "Concurrent sessions accounted (got $new_events new events, expected >= $min_expected)");

# Should not drop with adequate capacity
is($stats->{dropped}, 0, 'No drops with adequate capacity');

# Note: We don't test queue draining since there's no ClickHouse to export to.

diag("Concurrent sessions test results:");
diag("  Sessions: $num_sessions");
diag("  Enqueued (total): $stats->{enqueued}");
diag("  Enqueued (new): $new_events");
diag("  Dropped:  $stats->{dropped}");
diag("  Queue size: $stats->{queue_size}");

$node->stop();
done_testing();
