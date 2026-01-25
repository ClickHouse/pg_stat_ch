#!/usr/bin/env perl
# Query capture: verify queries are captured in the ring buffer via executor hooks

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

my $node = psch_init_node('capture');
psch_reset_stats($node);

# Get baseline after reset (may not be exactly zero due to internal queries)
my $stats_before = psch_get_stats($node);
is($stats_before->{dropped}, 0, 'Starts with zero dropped events');

# Execute a mix of test queries
$node->safe_psql('postgres', "SELECT 1 AS test_value");
$node->safe_psql('postgres', "SELECT 2 + 2 AS arithmetic");
$node->safe_psql('postgres', "CREATE TABLE test_cap (id int)");
$node->safe_psql('postgres', "INSERT INTO test_cap VALUES (1), (2), (3)");
$node->safe_psql('postgres', "SELECT * FROM test_cap WHERE id > 1");
$node->safe_psql('postgres', "UPDATE test_cap SET id = id + 10 WHERE id = 1");
$node->safe_psql('postgres', "DELETE FROM test_cap WHERE id = 2");
$node->safe_psql('postgres', "DROP TABLE test_cap");

# Get stats after queries
my $stats = psch_get_stats($node);

# Calculate new events since baseline
my $new_events = $stats->{enqueued} - $stats_before->{enqueued};

# Should have captured at least the queries we ran (8 statements)
cmp_ok($new_events, '>=', 8,
    "At least 8 queries captured (got $new_events new events)");

# With adequate capacity, no events should be dropped
is($stats->{dropped}, 0, 'No queries dropped with adequate buffer capacity');

diag("Query capture results:");
diag("  Enqueued: $stats->{enqueued}");
diag("  Dropped:  $stats->{dropped}");
diag("  Queue size: $stats->{queue_size}");

$node->stop();
done_testing();
