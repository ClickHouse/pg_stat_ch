#!/usr/bin/env perl
# TOCTOU race test: tests the double-checked locking in PschEnqueueEvent()
#
# The ring buffer has a TOCTOU pattern:
# 1. Lock-free fast path: if (head - tail >= capacity) return early
# 2. Slow path: acquire lock, re-check in TryEnqueueLocked()
#
# Tests:
# - Fast path race: multiple producers race for last slots
# - Re-check under lock: heavy contention with 4+ sessions

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::BackgroundPsql;
use Test::More;
use Time::HiRes qw(sleep);

use psch;

# Small capacity to trigger contention quickly
my $capacity = 1024;

# Test 1: Fast path race - multiple producers race for last slots
subtest 'fast path race' => sub {
    my $node = psch_init_node('fast_path_race',
        queue_capacity    => $capacity,
        flush_interval_ms => 60000  # Long interval - don't drain during test
    );

    psch_reset_stats($node);

    # Use 8 clients to maximize contention for the TOCTOU gap
    my $nclients = 8;
    # Generate more queries than capacity to force racing at the boundary
    my $ntxns = ($capacity / $nclients) + 50;
    my $total_queries = $nclients * $ntxns;

    $node->pgbench(
        "--client=$nclients --transactions=$ntxns --no-vacuum",
        0, [qr/processed/], [qr/(?:^$|queue overflow)/],
        'TOCTOU race test',
        { 'simple' => 'SELECT 1' }
    );

    my $stats = psch_get_stats($node);

    # The key invariant: no events should be lost in the TOCTOU gap
    # enqueued + dropped should account for all queries
    my $total_accounted = $stats->{enqueued} + $stats->{dropped};
    cmp_ok($total_accounted, '>=', $total_queries,
        "All events accounted (enqueued=$stats->{enqueued} + " .
        "dropped=$stats->{dropped} >= total=$total_queries)");

    # Queue size must never exceed capacity
    cmp_ok($stats->{queue_size}, '<=', $capacity,
        "Queue size ($stats->{queue_size}) <= capacity ($capacity)");

    # Should have some overflow given the load
    cmp_ok($stats->{dropped}, '>', 0, 'Some events dropped (overflow triggered)');

    note("Fast path race: total=$total_queries, enqueued=$stats->{enqueued}, " .
         "dropped=$stats->{dropped}, queue_size=$stats->{queue_size}");

    $node->stop();
};

# Test 2: Re-check under lock - heavy contention with BackgroundPsql sessions
subtest 'recheck under lock' => sub {
    my $node = psch_init_node('recheck_lock',
        queue_capacity    => $capacity,
        flush_interval_ms => 60000  # Long interval - don't drain during test
    );

    psch_reset_stats($node);

    # Spawn 4 background sessions for tight concurrent control
    my $num_sessions = 4;
    my $queries_per_session = 400;
    my @sessions;

    for my $i (1 .. $num_sessions) {
        push @sessions, $node->background_psql('postgres');
    }

    # Each session generates many queries as fast as possible
    # This creates heavy contention for the lock
    for my $i (0 .. $#sessions) {
        my $session = $sessions[$i];
        $session->query_until(qr/TOCTOU_DONE_$i/, qq{
            DO \$\$
            BEGIN
                FOR j IN 1..$queries_per_session LOOP
                    PERFORM 1;
                END LOOP;
            END
            \$\$;
            SELECT 'TOCTOU_DONE_$i';
        });
    }

    # Wait for all sessions to complete
    for my $session (@sessions) {
        $session->quit();
    }

    my $stats = psch_get_stats($node);

    # Check invariant: queue_size must never exceed capacity
    cmp_ok($stats->{queue_size}, '<=', $capacity,
        "Queue size ($stats->{queue_size}) <= capacity ($capacity) always");

    # Check that enqueued is non-negative (no underflow)
    cmp_ok($stats->{enqueued}, '>=', 0, 'Enqueued is non-negative');
    cmp_ok($stats->{dropped}, '>=', 0, 'Dropped is non-negative');

    # Queue should have some events (from DO blocks and final SELECTs)
    cmp_ok($stats->{enqueued}, '>', 0, 'Events were enqueued');

    note("Recheck under lock: enqueued=$stats->{enqueued}, " .
         "dropped=$stats->{dropped}, queue_size=$stats->{queue_size}");

    $node->stop();
};

# Test 3: Concurrent overflow - multiple producers hitting overflow simultaneously
subtest 'concurrent overflow' => sub {
    my $node = psch_init_node('concurrent_overflow',
        queue_capacity    => $capacity,
        flush_interval_ms => 60000  # Long interval - don't drain during test
    );

    psch_reset_stats($node);

    # First, fill the queue to near capacity
    my $prefill = $capacity - 50;
    for my $i (1 .. $prefill) {
        $node->safe_psql('postgres', "SELECT $i");
    }

    my $stats_prefill = psch_get_stats($node);
    note("After prefill: queue_size=$stats_prefill->{queue_size}");

    # Now spawn concurrent sessions that will all hit overflow
    my $num_sessions = 4;
    my $queries_per_session = 200;  # Each session tries to add 200 events
    my @sessions;

    for my $i (1 .. $num_sessions) {
        push @sessions, $node->background_psql('postgres');
    }

    # All sessions try to overflow simultaneously
    for my $i (0 .. $#sessions) {
        my $session = $sessions[$i];
        $session->query_until(qr/OVERFLOW_DONE_$i/, qq{
            DO \$\$
            BEGIN
                FOR j IN 1..$queries_per_session LOOP
                    PERFORM 1;
                END LOOP;
            END
            \$\$;
            SELECT 'OVERFLOW_DONE_$i';
        });
    }

    # Wait for completion
    for my $session (@sessions) {
        $session->quit();
    }

    my $stats = psch_get_stats($node);

    # Key assertions for TOCTOU correctness:
    # 1. Queue never exceeds capacity
    cmp_ok($stats->{queue_size}, '<=', $capacity,
        "Queue never exceeds capacity during concurrent overflow");

    # 2. Should have overflow
    cmp_ok($stats->{dropped}, '>', 0,
        'Events dropped during concurrent overflow');

    # 3. No events lost in the gap
    cmp_ok($stats->{enqueued} + $stats->{dropped}, '>=',
        $prefill + ($num_sessions * 2),  # At minimum: prefill + DO + SELECT per session
        'Events accounted during overflow contention');

    note("Concurrent overflow: enqueued=$stats->{enqueued}, " .
         "dropped=$stats->{dropped}, queue_size=$stats->{queue_size}");

    $node->stop();
};

done_testing();
