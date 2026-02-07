#!/usr/bin/env perl
# Overflow deadlock test: verify that HandleOverflow()'s ereport(WARNING) does
# not deadlock when emit_log_hook re-enters PschEnqueueEvent().
#
# The bug: HandleOverflow() calls ereport(WARNING) while the LWLock is held
# inside TryEnqueueLocked(). With log_min_elevel = WARNING, emit_log_hook
# fires and CaptureLogEvent calls PschEnqueueEvent(), which tries to acquire
# the same exclusive lock — causing a self-deadlock.
#
# Why this is hard to reproduce naturally:
# The deadlock requires the queue to appear NOT full in the lock-free fast path
# (so we enter the locked slow path), but BE full inside TryEnqueueLocked (so
# HandleOverflow is called while the lock is held). Then the re-entered
# PschEnqueueEvent must see the queue as NOT full (so it tries LWLockAcquire
# instead of taking the lock-free overflow path). This is a race condition
# that requires precise timing between producers and the consumer.
#
# Strategy: use pg_stat_ch.debug_force_locked_overflow to make TryEnqueueLocked
# always call HandleOverflow() regardless of actual queue state. This simulates
# the race deterministically — the lock-free check passes (queue is not full),
# but once inside the lock, HandleOverflow fires. The re-entered PschEnqueueEvent
# also sees the queue as not-full in the lock-free check and tries LWLockAcquire
# on the already-held lock — deadlock.

use strict;
use warnings;
use lib 't';
use Test::More;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use psch;

my $node = psch_init_node('deadlock',
    flush_interval_ms => 60000,  # Never flush during test
);

# Set log_min_elevel to WARNING so emit_log_hook captures the overflow WARNING
$node->append_conf('postgresql.conf', qq{
pg_stat_ch.log_min_elevel = warning
});
$node->reload();

# Reset stats to clear overflow_logged — HandleOverflow only calls
# ereport(WARNING) when this flag is clear.
psch_reset_stats($node);

# Enable debug flag and run a query in the same session.
#
# Without the fix, this deadlocks:
#   PschEnqueueEvent → LWLockAcquire → TryEnqueueLocked (forced overflow)
#   → HandleOverflow → ereport(WARNING) → emit_log_hook → CaptureLogEvent
#   → PschEnqueueEvent → lock-free check passes → LWLockAcquire → DEADLOCK
#
# With the fix (PschSuppressErrorCapture at top of PschEnqueueEvent):
#   PschEnqueueEvent sets disable_error_capture=true → LWLockAcquire
#   → TryEnqueueLocked → HandleOverflow → ereport(WARNING) → emit_log_hook
#   → ShouldCaptureLog sees disable_error_capture=true → returns false
#   → no re-entry → no deadlock
my $timed_out = 0;
my ($ret, $stdout, $stderr);
eval {
    ($ret, $stdout, $stderr) = $node->psql('postgres',
        "SET pg_stat_ch.debug_force_locked_overflow = on; SELECT 1;",
        timeout => 10);
};
if ($@) {
    $timed_out = 1;
    diag("psql timed out (deadlock detected): $@");
}

ok(!$timed_out && defined($ret) && $ret == 0,
    "No deadlock when HandleOverflow fires under lock");

if (!$timed_out) {
    # Verify HandleOverflow was actually called
    my $stats = psch_get_stats($node);
    cmp_ok($stats->{dropped}, '>', 0, "HandleOverflow was called (events dropped)");
    note("Stats: enqueued=$stats->{enqueued}, dropped=$stats->{dropped}");

    # Reset and try again — exercises the path with overflow_logged freshly cleared
    psch_reset_stats($node);

    eval {
        ($ret, $stdout, $stderr) = $node->psql('postgres',
            "SET pg_stat_ch.debug_force_locked_overflow = on; SELECT 1;",
            timeout => 10);
    };
    if ($@) {
        $timed_out = 1;
        diag("psql timed out on second attempt: $@");
    }

    ok(!$timed_out && defined($ret) && $ret == 0,
        "No deadlock on second attempt after stats reset");
} else {
    # Skip remaining tests since the server is likely in a bad state
    fail("No deadlock on second attempt after stats reset (skipped: server deadlocked)");
}

$node->stop();
done_testing();
