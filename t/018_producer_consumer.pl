#!/usr/bin/env perl
# Producer/consumer test: tests data integrity with simultaneous producers and consumer
#
# Requires ClickHouse for the consumer (bgworker) to have somewhere to export.
#
# Tests:
# - Interleaved ops: producers enqueue while consumer dequeues
# - Slot reuse safety: fast consumer reuses slots rapidly

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::BackgroundPsql;
use Test::More;
use Time::HiRes qw(sleep time);

use psch;

# Skip if Docker/ClickHouse not available
if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping producer/consumer tests';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse not running. Start with: docker compose -f docker/docker-compose.test.yml up -d';
}

my $capacity = 8192;  # Larger capacity for producer/consumer tests

# Test 1: Interleaved operations - verify invariants hold during load
subtest 'interleaved operations' => sub {
    psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

    my $node = psch_init_node_with_clickhouse('interleaved',
        queue_capacity    => $capacity,
        flush_interval_ms => 100,  # Moderate consumer speed
        batch_max         => 100
    );

    # Wait for startup and reset
    sleep(0.5);
    psch_reset_stats($node);

    # Start background producers
    my $num_producers = 4;
    my $queries_per_producer = 100;
    my @producers;

    for my $i (1 .. $num_producers) {
        push @producers, $node->background_psql('postgres');
    }

    # Launch all producers concurrently
    for my $i (0 .. $#producers) {
        my $producer = $producers[$i];
        $producer->query_until(qr/PRODUCER_DONE_$i/, qq{
            DO \$\$
            BEGIN
                FOR j IN 1..$queries_per_producer LOOP
                    PERFORM 1;
                END LOOP;
            END
            \$\$;
            SELECT 'PRODUCER_DONE_$i';
        });
    }

    # While producers are running, sample stats periodically
    my $sample_count = 5;
    for my $s (1 .. $sample_count) {
        sleep(0.1);
        my $stats = psch_get_stats($node);

        # Key invariants that must always hold:
        cmp_ok($stats->{queue_size}, '<=', $capacity,
            "Sample $s: queue_size <= capacity");
        cmp_ok($stats->{enqueued}, '>=', 0,
            "Sample $s: enqueued non-negative");
        cmp_ok($stats->{exported}, '>=', 0,
            "Sample $s: exported non-negative");
        cmp_ok($stats->{exported}, '<=', $stats->{enqueued} + 10,
            "Sample $s: exported <= enqueued (with tolerance)");
    }

    # Wait for producers to complete
    for my $producer (@producers) {
        $producer->quit();
    }

    # Wait for queue to drain
    sleep(2);

    my $final = psch_get_stats($node);

    # Should not have dropped with adequate capacity
    is($final->{dropped}, 0, 'No drops with adequate capacity');

    # Events should have been exported
    cmp_ok($final->{exported}, '>', 0, 'Events were exported');

    # Queue should be mostly drained
    cmp_ok($final->{queue_size}, '<', 100,
        'Queue mostly drained after producers complete');

    note("Interleaved: enqueued=$final->{enqueued}, exported=$final->{exported}, " .
         "queue_size=$final->{queue_size}");

    $node->stop();
};

# Test 2: Slot reuse safety - verify no corruption during rapid slot reuse
subtest 'slot reuse safety' => sub {
    psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

    my $node = psch_init_node_with_clickhouse('slot_reuse',
        queue_capacity    => $capacity,
        flush_interval_ms => 50,   # Fast consumer
        batch_max         => 200   # Larger batches for faster drain
    );

    # Wait for startup and reset
    sleep(0.5);
    psch_reset_stats($node);

    # Run multiple fill/drain cycles with moderate load
    my $cycles = 3;
    my $events_per_cycle = 100;

    for my $cycle (1 .. $cycles) {
        # Fill phase - use DO block for efficiency
        $node->safe_psql('postgres', qq{
            DO \$\$
            BEGIN
                FOR i IN 1..$events_per_cycle LOOP
                    PERFORM i;
                END LOOP;
            END
            \$\$;
        });

        # Allow partial drain
        sleep(0.5);

        my $stats = psch_get_stats($node);
        cmp_ok($stats->{queue_size}, '<=', $capacity,
            "Cycle $cycle: queue within capacity");
        cmp_ok($stats->{queue_size}, '>=', 0,
            "Cycle $cycle: queue_size non-negative");
    }

    # Wait for drain - give more time
    sleep(5);

    my $final = psch_get_stats($node);

    # Verify no corruption: counters should be valid
    cmp_ok($final->{enqueued}, '>', 0, 'Events were enqueued');
    cmp_ok($final->{exported}, '>', 0, 'Events were exported');
    is($final->{dropped}, 0, 'No drops during slot reuse');

    # Queue should be mostly drained (allow some slack for timing)
    cmp_ok($final->{queue_size}, '<', $capacity / 2,
        'Queue substantially drained after cycles');

    # Verify ClickHouse received events
    my $ch_count = psch_query_clickhouse("SELECT count() FROM pg_stat_ch.events_raw");
    cmp_ok($ch_count, '>', 0, "ClickHouse received events (got $ch_count)");

    note("Slot reuse: $cycles cycles, enqueued=$final->{enqueued}, " .
         "exported=$final->{exported}, ch_count=$ch_count");

    $node->stop();
};

# Test 3: High contention - many producers with aggressive consumer
subtest 'high contention' => sub {
    psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

    my $node = psch_init_node_with_clickhouse('high_contention',
        queue_capacity    => $capacity,
        flush_interval_ms => 50,
        batch_max         => 200
    );

    # Wait for startup and reset
    sleep(0.5);
    psch_reset_stats($node);

    # Many concurrent producers with pgbench
    my $nclients = 4;
    my $ntxns = 250;
    my $total_queries = $nclients * $ntxns;

    $node->pgbench(
        "--client=$nclients --transactions=$ntxns --no-vacuum",
        0, [qr/processed/], [qr/^$/],
        'high contention producer',
        { 'simple' => 'SELECT 1' }
    );

    # Wait for export
    sleep(3);

    my $final = psch_get_stats($node);

    # Should handle load without overflow (adequate capacity)
    is($final->{dropped}, 0, 'No drops under high contention');

    # Events should have been processed
    cmp_ok($final->{enqueued}, '>=', $total_queries,
        "All queries enqueued (got $final->{enqueued}, expected >= $total_queries)");

    # Most events should be exported
    cmp_ok($final->{exported}, '>=', $total_queries * 0.5,
        "Events being exported under contention");

    note("High contention: $total_queries queries, enqueued=$final->{enqueued}, " .
         "exported=$final->{exported}");

    $node->stop();
};

done_testing();
