#!/usr/bin/env perl
# Stats consistency test: tests counter invariants and snapshot integrity
#
# Tests:
# - Conservation law: enqueued = dropped + exported + queue_size
# - Impossible states: exported > enqueued, queue_size > capacity
# - Non-negative: all counters >= 0

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::BackgroundPsql;
use Test::More;
use Time::HiRes qw(sleep time);

use psch;

my $capacity = 4096;

# Test 1: Conservation law during load
subtest 'conservation law' => sub {
    # Try with ClickHouse if available, otherwise without
    my $node;
    my $has_clickhouse = 0;

    if (psch_clickhouse_available()) {
        my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
        if ($ch_check =~ /^1/) {
            psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");
            $node = psch_init_node_with_clickhouse('conservation',
                queue_capacity    => $capacity,
                flush_interval_ms => 100
            );
            $has_clickhouse = 1;
        }
    }

    if (!$has_clickhouse) {
        $node = psch_init_node('conservation',
            queue_capacity    => $capacity,
            flush_interval_ms => 100
        );
    }

    psch_reset_stats($node);

    # Start background producer
    my $producer = $node->background_psql('postgres');
    $producer->query_until(qr/CONSERVATION_DONE/, qq{
        DO \$\$
        BEGIN
            FOR i IN 1..1000 LOOP
                PERFORM 1;
            END LOOP;
        END
        \$\$;
        SELECT 'CONSERVATION_DONE';
    });

    # Sample stats while producer runs
    my @samples;
    my $sample_count = 20;
    my $violations = 0;

    for my $s (1 .. $sample_count) {
        sleep(0.05);
        my $stats = psch_get_stats($node);
        push @samples, $stats;

        # Check conservation: enqueued = dropped + exported + queue_size
        my $accounted = $stats->{dropped} + $stats->{exported} + $stats->{queue_size};
        my $diff = $stats->{enqueued} - $accounted;

        # During active operations, small discrepancy allowed due to in-flight events
        if (abs($diff) > 50) {
            $violations++;
            diag("Sample $s: conservation violation - enqueued=$stats->{enqueued}, " .
                 "accounted=$accounted, diff=$diff");
        }
    }

    $producer->quit();

    # No significant violations
    cmp_ok($violations, '<=', 2,
        "Conservation law held (violations=$violations out of $sample_count samples)");

    # Final check after quiescence - allow small tolerance
    # The conservation law may not be exact due to:
    # - Events in flight during measurement
    # - Timing between counter updates
    sleep(0.5);
    my $final = psch_get_stats($node);
    my $final_accounted = $final->{dropped} + $final->{exported} + $final->{queue_size};
    my $final_diff = abs($final->{enqueued} - $final_accounted);

    cmp_ok($final_diff, '<=', 20,
        "Final conservation within tolerance: enqueued=$final->{enqueued}, " .
        "accounted=$final_accounted, diff=$final_diff");

    $node->stop();
};

# Test 2: Impossible states - rapid sampling for race conditions
subtest 'impossible states' => sub {
    my $node = psch_init_node('impossible',
        queue_capacity    => $capacity,
        flush_interval_ms => 100
    );

    psch_reset_stats($node);

    # Start multiple producers
    my $num_producers = 4;
    my @producers;

    for my $i (1 .. $num_producers) {
        my $p = $node->background_psql('postgres');
        push @producers, $p;
        $p->query_until(qr/IMPOSSIBLE_DONE_$i/, qq{
            DO \$\$
            BEGIN
                FOR j IN 1..500 LOOP
                    PERFORM 1;
                END LOOP;
            END
            \$\$;
            SELECT 'IMPOSSIBLE_DONE_$i';
        });
    }

    # Rapid sampling
    my $sample_count = 100;
    my $impossible_exported = 0;
    my $impossible_size = 0;
    my $negative_counter = 0;

    for my $s (1 .. $sample_count) {
        my $stats = psch_get_stats($node);

        # Check for impossible states
        if ($stats->{exported} > $stats->{enqueued}) {
            $impossible_exported++;
            diag("Sample $s: exported ($stats->{exported}) > enqueued ($stats->{enqueued})");
        }

        if ($stats->{queue_size} > $stats->{capacity}) {
            $impossible_size++;
            diag("Sample $s: queue_size ($stats->{queue_size}) > capacity ($stats->{capacity})");
        }

        # Check for negative counters
        for my $key (qw(enqueued dropped exported queue_size)) {
            if ($stats->{$key} < 0) {
                $negative_counter++;
                diag("Sample $s: $key is negative ($stats->{$key})");
            }
        }

        # Small delay to allow state changes
        select(undef, undef, undef, 0.01);  # 10ms sleep
    }

    for my $p (@producers) {
        $p->quit();
    }

    is($impossible_exported, 0,
        "No samples with exported > enqueued");
    is($impossible_size, 0,
        "No samples with queue_size > capacity");
    is($negative_counter, 0,
        "No samples with negative counters");

    $node->stop();
};

# Test 3: Non-negative counters under stress
subtest 'non negative counters' => sub {
    my $node = psch_init_node('nonneg',
        queue_capacity    => 1024,  # Small capacity for overflow
        flush_interval_ms => 60000  # No consumer
    );

    psch_reset_stats($node);

    # Generate overflow
    my $nclients = 4;
    my $ntxns = 500;

    $node->pgbench(
        "--client=$nclients --transactions=$ntxns --no-vacuum",
        0, [qr/processed/], [qr/(?:^$|queue overflow)/],
        'overflow for non-neg test',
        { 'simple' => 'SELECT 1' }
    );

    # Sample many times
    my $violations = 0;
    for my $s (1 .. 50) {
        my $stats = psch_get_stats($node);

        for my $key (qw(enqueued dropped exported queue_size capacity)) {
            if ($stats->{$key} < 0) {
                $violations++;
                diag("Sample $s: $key is negative ($stats->{$key})");
            }
        }
    }

    is($violations, 0, 'All counters non-negative under overflow stress');

    $node->stop();
};

# Test 4: Counter monotonicity (with exceptions for reset)
subtest 'counter monotonicity' => sub {
    my $node = psch_init_node('monotonic',
        queue_capacity    => $capacity,
        flush_interval_ms => 60000  # No consumer - counters should only increase
    );

    psch_reset_stats($node);

    my $prev_enqueued = 0;
    my $prev_dropped = 0;
    my $mono_violations = 0;

    # Generate events and verify monotonicity
    for my $batch (1 .. 20) {
        for my $i (1 .. 50) {
            $node->safe_psql('postgres', "SELECT $i");
        }

        my $stats = psch_get_stats($node);

        # Enqueued should be monotonically increasing
        if ($stats->{enqueued} < $prev_enqueued) {
            $mono_violations++;
            diag("Batch $batch: enqueued decreased ($prev_enqueued -> $stats->{enqueued})");
        }

        # Dropped should be monotonically increasing
        if ($stats->{dropped} < $prev_dropped) {
            $mono_violations++;
            diag("Batch $batch: dropped decreased ($prev_dropped -> $stats->{dropped})");
        }

        $prev_enqueued = $stats->{enqueued};
        $prev_dropped = $stats->{dropped};
    }

    is($mono_violations, 0, 'Counters are monotonically increasing');

    $node->stop();
};

# Test 5: Usage percentage accuracy
subtest 'usage percentage accuracy' => sub {
    my $small_capacity = 1024;
    my $node = psch_init_node('usage_pct',
        queue_capacity    => $small_capacity,
        flush_interval_ms => 60000  # No drain
    );

    psch_reset_stats($node);

    # Add some events
    $node->safe_psql('postgres', qq{
        DO \$\$
        BEGIN
            FOR i IN 1..256 LOOP
                PERFORM i;
            END LOOP;
        END
        \$\$;
    });

    my $stats = psch_get_stats($node);

    # Usage percentage should be approximately (queue_size / capacity) * 100
    my $expected_pct = ($stats->{queue_size} / $stats->{capacity}) * 100;
    my $reported_pct = $stats->{usage_pct};

    # Allow 5% tolerance due to rounding
    cmp_ok(abs($expected_pct - $reported_pct), '<', 5,
        "Usage percentage accurate: expected ~$expected_pct%, got $reported_pct%");

    $node->stop();
};

done_testing();
