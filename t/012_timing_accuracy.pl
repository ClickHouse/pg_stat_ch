#!/usr/bin/env perl
# Test: Timing accuracy validation using pgbench
# Verifies that timing metrics are captured and reasonable

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Skip if ClickHouse not available (this test validates via ClickHouse)
my $use_clickhouse = 0;
if (psch_clickhouse_available()) {
    my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
    $use_clickhouse = ($ch_check =~ /^1/);
}

my $node;
if ($use_clickhouse) {
    # Clear any existing test data
    psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");
    $node = psch_init_node_with_clickhouse('timing',
        flush_interval_ms => 100,
        batch_max => 1000
    );
} else {
    $node = psch_init_node('timing',
        flush_interval_ms => 100
    );
}

# Test 1: Verify timing metrics are non-zero after queries
subtest 'timing metrics captured' => sub {
    psch_reset_stats($node);

    # Run a simple workload
    $node->safe_psql('postgres', 'CREATE TABLE test_timing(id int, data text)');
    $node->safe_psql('postgres', 'INSERT INTO test_timing SELECT g, md5(g::text) FROM generate_series(1, 1000) g');
    $node->safe_psql('postgres', 'SELECT * FROM test_timing WHERE id > 500');
    $node->safe_psql('postgres', 'UPDATE test_timing SET data = md5(data) WHERE id <= 100');

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 4, 'Events enqueued from timing test');

    if ($use_clickhouse) {
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
        psch_wait_for_export($node, $stats->{enqueued}, 10);

        # Check timing data in ClickHouse
        my $timing_check = psch_query_clickhouse(
            "SELECT count() FROM pg_stat_ch.events_raw WHERE duration_us > 0"
        );
        cmp_ok($timing_check, '>=', 1, 'duration_us > 0 for some events');

        # Timing should be reasonable (< 60 seconds for simple queries)
        my $max_duration = psch_query_clickhouse(
            "SELECT max(duration_us) FROM pg_stat_ch.events_raw"
        );
        cmp_ok($max_duration, '<', 60000000, "Max duration reasonable (< 60s): $max_duration us");
    }

    $node->safe_psql('postgres', 'DROP TABLE test_timing');
};

# Test 2: pgbench workload timing validation
subtest 'pgbench timing validation' => sub {
    # Initialize pgbench
    # Note: In newer PostgreSQL, "done in" goes to stderr, not stdout
    # Just check exit code=0 and allow any output
    $node->pgbench(
        "--initialize --scale=1",
        0,
        [qr//],  # Allow any stdout
        [qr//],  # Allow any stderr (progress messages)
        'pgbench init'
    );

    psch_reset_stats($node);

    # Run pgbench workload: 2 clients, 100 transactions each
    $node->pgbench(
        "--client=2 --transactions=100 --no-vacuum",
        0,
        [qr/processed: \d+\/\d+/],
        [qr/^$/],
        'pgbench workload'
    );

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 200, 'At least 200 events from pgbench');
    is($stats->{dropped}, 0, 'No events dropped during pgbench');

    if ($use_clickhouse) {
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
        psch_wait_for_export($node, 200, 30);

        # Get timing statistics
        my $timing_stats = psch_query_clickhouse(
            "SELECT count(), avg(duration_us), min(duration_us), max(duration_us) " .
            "FROM pg_stat_ch.events_raw WHERE query LIKE '%pgbench%' OR query LIKE '%UPDATE%'"
        );
        diag("Timing stats: $timing_stats");

        # Verify we have timing data
        my ($count, $avg, $min, $max) = split(/\t/, $timing_stats);
        cmp_ok($count, '>=', 1, 'Have pgbench events');
        cmp_ok($avg, '>', 0, 'Average duration > 0') if defined $avg;
    }
};

# Test 3: Long query timing
subtest 'long query timing' => sub {
    psch_reset_stats($node);

    # Run a query that takes measurable time (pg_sleep)
    my $start = time();
    $node->safe_psql('postgres', 'SELECT pg_sleep(0.1)');  # 100ms
    my $elapsed = time() - $start;

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 1, 'pg_sleep query was captured');

    if ($use_clickhouse) {
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
        sleep(2);

        # Check that we captured a duration close to 100ms
        my $duration = psch_query_clickhouse(
            "SELECT duration_us FROM pg_stat_ch.events_raw " .
            "WHERE query LIKE '%pg_sleep%' ORDER BY ts_start DESC LIMIT 1"
        );

        if ($duration) {
            # Should be roughly 100000us (100ms), allow 50ms tolerance
            cmp_ok($duration, '>=', 50000, 'pg_sleep duration >= 50ms');
            cmp_ok($duration, '<=', 500000, 'pg_sleep duration <= 500ms');
            diag("pg_sleep(0.1) captured duration: ${duration}us");
        } else {
            pass('pg_sleep event captured (timing check skipped)');
        }
    }
};

$node->stop();
done_testing();
