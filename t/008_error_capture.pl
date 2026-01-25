#!/usr/bin/env perl
# Error capture: verify errors (WARNING and above) are captured via emit_log_hook

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

my $node = psch_init_node('error');
psch_reset_stats($node);

# Get baseline after reset
my $stats_before = psch_get_stats($node);
is($stats_before->{dropped}, 0, 'Starts with zero dropped events');

# Generate various errors (these will fail but should be captured)

# 1. Division by zero
eval { $node->safe_psql('postgres', 'SELECT 1/0'); };
# Expected to fail

# 2. Undefined table
eval { $node->safe_psql('postgres', 'SELECT * FROM nonexistent_table_xyz'); };
# Expected to fail

# 3. Syntax error
eval { $node->safe_psql('postgres', 'SELEC 1'); };
# Expected to fail

# 4. Permission error (create a restricted context)
$node->safe_psql('postgres', q{
    CREATE TABLE error_test_table (id int);
    CREATE ROLE error_test_role NOLOGIN;
    REVOKE ALL ON error_test_table FROM PUBLIC;
});

# Try to access as superuser but simulate permission scenario
eval {
    $node->safe_psql('postgres',
        "SET ROLE error_test_role; SELECT * FROM error_test_table;");
};
# Expected to fail

# 5. Constraint violation
$node->safe_psql('postgres', 'CREATE TABLE check_test (val int CHECK (val > 0))');
eval { $node->safe_psql('postgres', 'INSERT INTO check_test VALUES (-1)'); };
# Expected to fail

# Clean up
$node->safe_psql('postgres', 'DROP TABLE IF EXISTS error_test_table CASCADE');
$node->safe_psql('postgres', 'DROP ROLE IF EXISTS error_test_role');
$node->safe_psql('postgres', 'DROP TABLE IF EXISTS check_test');

# Get stats after errors
my $stats = psch_get_stats($node);

# Calculate new events since baseline
my $new_events = $stats->{enqueued} - $stats_before->{enqueued};

# We should have captured events for the errors
# At least 5 errors were generated (division by zero, undefined table, syntax,
# permission, constraint)
cmp_ok($new_events, '>=', 5,
    "At least 5 error events captured (got $new_events new events)");

# No events should be dropped with adequate capacity
is($stats->{dropped}, 0, 'No events dropped with adequate buffer capacity');

diag("Error capture results:");
diag("  Enqueued: $stats->{enqueued}");
diag("  Dropped:  $stats->{dropped}");
diag("  New events: $new_events");

$node->stop();
done_testing();
