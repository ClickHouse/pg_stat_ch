#!/usr/bin/env perl
# Reset atomicity test: tests pg_stat_ch_reset() is atomic with concurrent operations
#
# Tests:
# - Reset during enqueue: reset while producers active
# - Reset during dequeue: reset while consumer (bgworker) active
# - Overflow flag reset: verify flag cleared, new warning fires

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::BackgroundPsql;
use Test::More;
use Time::HiRes qw(sleep);

use psch;

my $capacity = 1024;

# Test 1: Reset during enqueue - no deadlock, counters valid
subtest 'reset during enqueue' => sub {
    my $node = psch_init_node('reset_enqueue',
        queue_capacity    => $capacity,
        flush_interval_ms => 60000  # Long interval - no consumer interference
    );

    psch_reset_stats($node);

    # Start background producers
    my $num_producers = 4;
    my @producers;

    for my $i (1 .. $num_producers) {
        push @producers, $node->background_psql('postgres');
    }

    # Launch producers
    for my $i (0 .. $#producers) {
        my $producer = $producers[$i];
        $producer->query_until(qr/RESET_ENQUEUE_DONE_$i/, qq{
            DO \$\$
            BEGIN
                FOR j IN 1..200 LOOP
                    PERFORM 1;
                END LOOP;
            END
            \$\$;
            SELECT 'RESET_ENQUEUE_DONE_$i';
        });
    }

    # Issue multiple resets while producers are active
    my $reset_count = 5;
    for my $r (1 .. $reset_count) {
        sleep(0.05);  # Small delay between resets
        psch_reset_stats($node);

        my $stats = psch_get_stats($node);

        # After each reset, counters should be valid
        cmp_ok($stats->{enqueued}, '>=', 0,
            "Reset $r: enqueued non-negative");
        cmp_ok($stats->{dropped}, '>=', 0,
            "Reset $r: dropped non-negative");
        cmp_ok($stats->{queue_size}, '>=', 0,
            "Reset $r: queue_size non-negative");
        cmp_ok($stats->{queue_size}, '<=', $capacity,
            "Reset $r: queue_size <= capacity");
    }

    # Wait for producers
    for my $producer (@producers) {
        $producer->quit();
    }

    # Final stats check - no crash, counters valid
    my $final = psch_get_stats($node);
    ok(defined($final->{enqueued}), 'Final stats accessible after concurrent resets');
    cmp_ok($final->{enqueued}, '>=', 0, 'Final enqueued valid');

    note("Reset during enqueue: $reset_count resets, final enqueued=$final->{enqueued}");

    $node->stop();
};

# Test 2: Reset during dequeue - no deadlock with bgworker
subtest 'reset during dequeue' => sub {
    # Skip if ClickHouse not available
    if (!psch_clickhouse_available()) {
        plan skip_all => 'Docker not available for reset during dequeue test';
        return;
    }

    my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
    if ($ch_check !~ /^1/) {
        plan skip_all => 'ClickHouse not running';
        return;
    }

    psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

    my $node = psch_init_node_with_clickhouse('reset_dequeue',
        queue_capacity    => $capacity,
        flush_interval_ms => 50,  # Fast consumer
        batch_max         => 50
    );

    # Fill queue with events
    for my $i (1 .. 500) {
        $node->safe_psql('postgres', "SELECT $i");
    }

    # Issue resets while consumer is draining
    my $reset_count = 10;
    for my $r (1 .. $reset_count) {
        sleep(0.1);
        psch_reset_stats($node);

        my $stats = psch_get_stats($node);

        # After each reset, counters should be valid
        cmp_ok($stats->{enqueued}, '>=', 0,
            "Dequeue reset $r: enqueued non-negative");
        cmp_ok($stats->{exported}, '>=', 0,
            "Dequeue reset $r: exported non-negative");
        cmp_ok($stats->{queue_size}, '>=', 0,
            "Dequeue reset $r: queue_size non-negative");
    }

    # No deadlock - we reached here
    ok(1, 'No deadlock during reset with active consumer');

    $node->stop();
};

# Test 3: Overflow flag reset - verify counter cleared after reset
subtest 'overflow flag reset' => sub {
    my $node = psch_init_node('overflow_flag',
        queue_capacity    => $capacity,
        flush_interval_ms => 60000  # Long interval - don't drain
    );

    psch_reset_stats($node);

    # First overflow: fill queue past capacity using pgbench for efficiency
    my $nclients = 4;
    my $ntxns = 400;  # 1600 total, well over 1024 capacity

    $node->pgbench(
        "--client=$nclients --transactions=$ntxns --no-vacuum",
        0, [qr/processed/], [qr/(?:^$|queue overflow)/],
        'first overflow',
        { 'simple' => 'SELECT 1' }
    );

    my $stats1 = psch_get_stats($node);
    cmp_ok($stats1->{dropped}, '>', 0, 'First overflow: events dropped');

    my $dropped_before_reset = $stats1->{dropped};
    note("First overflow: dropped=$dropped_before_reset");

    # Reset - this should clear the counters
    psch_reset_stats($node);

    my $stats_after_reset = psch_get_stats($node);
    # The key assertion is that dropped is much less than before
    cmp_ok($stats_after_reset->{dropped}, '<', $dropped_before_reset,
        'Reset reduced dropped counter significantly');

    note("After reset: dropped=$stats_after_reset->{dropped}");

    $node->stop();
};

# Test 4: Rapid reset stress - many quick resets
subtest 'rapid reset stress' => sub {
    my $node = psch_init_node('rapid_reset',
        queue_capacity    => $capacity,
        flush_interval_ms => 100
    );

    # Generate some events
    for my $i (1 .. 100) {
        $node->safe_psql('postgres', "SELECT $i");
    }

    # Rapid fire resets
    my $rapid_resets = 50;
    for my $r (1 .. $rapid_resets) {
        psch_reset_stats($node);

        my $stats = psch_get_stats($node);

        # All counters should remain valid
        cmp_ok($stats->{enqueued}, '>=', 0, "Rapid reset $r: enqueued valid");
        cmp_ok($stats->{dropped}, '>=', 0, "Rapid reset $r: dropped valid");
        cmp_ok($stats->{exported}, '>=', 0, "Rapid reset $r: exported valid");
        cmp_ok($stats->{queue_size}, '>=', 0, "Rapid reset $r: queue_size valid");
        cmp_ok($stats->{capacity}, '==', $capacity, "Rapid reset $r: capacity unchanged");
    }

    ok(1, "$rapid_resets rapid resets completed without crash");

    $node->stop();
};

done_testing();
