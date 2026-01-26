#!/usr/bin/env perl
# Test that DROP DATABASE completes without hanging.
#
# This tests the fix for a bug where the bgworker used a custom SIGUSR1 handler
# instead of procsignal_sigusr1_handler, preventing it from acknowledging
# ProcSignalBarrier events. DROP DATABASE uses barriers to coordinate file
# descriptor closure across all backends, so it would hang indefinitely.

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time);

use psch;

my $node = psch_init_node('drop_db_barrier');

# Test 1: Verify bgworker is running (prerequisite)
my $bgworker_count = $node->safe_psql('postgres', q{
    SELECT count(*) FROM pg_stat_activity
    WHERE backend_type = 'pg_stat_ch exporter'
});
is($bgworker_count, '1', 'pg_stat_ch exporter background worker is running');

# Test 2: CREATE DATABASE should work
$node->safe_psql('postgres', 'CREATE DATABASE testdb_barrier');
pass('CREATE DATABASE completed');

# Test 3: DROP DATABASE should complete quickly (not hang on barrier)
# The bug caused DROP DATABASE to hang indefinitely waiting for barrier ack.
# With the fix, it should complete in under 5 seconds.
my $start = time();
$node->safe_psql('postgres', 'DROP DATABASE testdb_barrier');
my $elapsed = time() - $start;

ok($elapsed < 5.0, "DROP DATABASE completed in ${elapsed}s (< 5s threshold)");

# Test 4: Verify bgworker still running after barrier event
$bgworker_count = $node->safe_psql('postgres', q{
    SELECT count(*) FROM pg_stat_activity
    WHERE backend_type = 'pg_stat_ch exporter'
});
is($bgworker_count, '1', 'Background worker still running after DROP DATABASE');

# Test 5: Multiple CREATE/DROP cycles to stress test barrier handling
for my $i (1..3) {
    $node->safe_psql('postgres', "CREATE DATABASE testdb_cycle_$i");
    $node->safe_psql('postgres', "DROP DATABASE testdb_cycle_$i");
}
pass('Multiple CREATE/DROP DATABASE cycles completed');

# Test 6: Verify stats function still works (bgworker healthy)
my $stats = psch_get_stats($node);
ok(defined $stats->{enqueued}, 'Stats accessible after barrier events');

diag("DROP DATABASE barrier test completed");
diag("  DROP DATABASE time: ${elapsed}s");
diag("  Events enqueued: $stats->{enqueued}");

$node->stop();
done_testing();
