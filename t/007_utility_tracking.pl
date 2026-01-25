#!/usr/bin/env perl
# Utility tracking: verify DDL and utility statements are captured via ProcessUtility hook

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

my $node = psch_init_node('utility');
psch_reset_stats($node);

# Get baseline stats
my $before = psch_get_stats($node);
my $before_enqueued = $before->{enqueued};

# Execute various DDL/utility statements
$node->safe_psql('postgres', "CREATE TABLE test_ddl (id serial PRIMARY KEY)");
$node->safe_psql('postgres', "ALTER TABLE test_ddl ADD COLUMN name text");
$node->safe_psql('postgres', "CREATE INDEX test_ddl_name_idx ON test_ddl(name)");
$node->safe_psql('postgres', "VACUUM test_ddl");
$node->safe_psql('postgres', "ANALYZE test_ddl");
$node->safe_psql('postgres', "DROP INDEX test_ddl_name_idx");
$node->safe_psql('postgres', "DROP TABLE test_ddl");

# Wait for events to be processed
sleep(1);

# Get stats after DDL
my $after = psch_get_stats($node);
my $new_events = $after->{enqueued} - $before_enqueued;

# Should have captured at least the DDL statements we ran (7 statements)
cmp_ok($new_events, '>=', 7,
    "DDL statements captured (got $new_events new events)");

# No events should be dropped
is($after->{dropped}, 0, 'No events dropped');

diag("Utility tracking results:");
diag("  Events before: $before_enqueued");
diag("  Events after:  $after->{enqueued}");
diag("  New events:    $new_events");
diag("  Dropped:       $after->{dropped}");

$node->stop();
done_testing();
