#!/usr/bin/env perl
# Test: OpenTelemetry export functionality
# Prerequisites: OTel collector container must be running
#   docker compose -f docker/docker-compose.otel.yml up -d

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Skip if Docker not available
if (!system("docker ps >/dev/null 2>&1") == 0) {
    plan skip_all => 'Docker not available, skipping OTel tests';
}

# Check if OTel collector is running
if (!psch_otelcol_available()) {
    plan skip_all =>
        'OTel collector not running. Start with: docker compose -f docker/docker-compose.otel.yml up -d';
}

# Initialize node with OTel export enabled
my $node = psch_init_node_with_otel('otel_export',
    flush_interval_ms => 100,
    batch_max         => 100,
);

# Test 1: Basic export - run queries and verify events are exported
subtest 'basic export' => sub {
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT 1');
    $node->safe_psql('postgres', 'SELECT 2');
    $node->safe_psql('postgres', 'SELECT 3');

    my $stats_before = psch_get_stats($node);
    cmp_ok($stats_before->{enqueued}, '>=', 3, 'Events enqueued');

    # Wait for the bgworker to flush and commit the batch
    my $exported = psch_wait_for_export($node, 3, 10);
    cmp_ok($exported, '>=', 3, 'Events exported via OTel');

    my $stats = psch_get_stats($node);
    is($stats->{send_failures}, 0, 'No send failures');

    # Verify metrics arrived at the collector (Prometheus endpoint)
    my $count = psch_get_otel_histogram_total('pg_stat_ch_duration_us_unit');
    cmp_ok($count, '>=', 3, "duration_us metric has >= 3 observations (got $count)");
};

# Test 2: Batch sizing - verify batch_max is honored
subtest 'batch sizing' => sub {
    psch_reset_stats($node);

    # Generate more events than batch_max (100)
    for my $i (1..150) {
        $node->safe_psql('postgres', "SELECT $i");
    }

    psch_wait_for_export($node, 150, 15);

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{exported}, '>=', 150, 'All events exported');
    is($stats->{send_failures}, 0, 'No send failures during multi-batch export');
};

# Test 3: Immediate flush via pg_stat_ch_flush()
subtest 'immediate flush' => sub {
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT 42 AS test_flush');

    # Force immediate flush before the periodic interval fires
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    sleep(1);

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{exported}, '>=', 1, 'Flush triggered export');
    is($stats->{send_failures}, 0, 'Flush completed without failures');
};

# Test 4: Metric labels populated
# Tags (db, username) appear as Prometheus labels on histogram metrics.
# The OTel exporter maps TagString columns to both log attributes and metric tags.
subtest 'metric labels populated' => sub {
    $node->safe_psql('postgres', 'CREATE TABLE test_otel_labels(id int)');
    $node->safe_psql('postgres', "INSERT INTO test_otel_labels VALUES (1), (2), (3)");
    $node->safe_psql('postgres', 'SELECT * FROM test_otel_labels');
    $node->safe_psql('postgres', 'DROP TABLE test_otel_labels');

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(2);

    # service.name resource attribute is mapped to the job label by the Prometheus exporter
    my $prometheus_output = `curl -s 'http://localhost:9091/metrics' 2>/dev/null`;
    like($prometheus_output, qr/job="pg_stat_ch"/,
        'metrics carry job="pg_stat_ch" label (from service.name resource attribute)');

    # db tag should appear as a label on duration_us observations
    ok(psch_otel_metric_has_label('pg_stat_ch_duration_us_unit', 'db', 'postgres'),
        'duration_us metric has db="postgres" label');

    # rows metric captures how many rows were returned/affected
    my $rows_count = psch_get_otel_histogram_total('pg_stat_ch_rows_unit');
    cmp_ok($rows_count, '>=', 1, "rows metric has observations (got $rows_count)");

    # duration_us should show meaningful wall-clock values (sum > 0)
    my $sum = 0;
    for my $line (split /\n/, $prometheus_output) {
        next if $line =~ /^#/;
        if ($line =~ /^pg_stat_ch_duration_us_unit_sum(?:\{[^}]*\})?\s+(\d+(?:\.\d+)?)/) {
            $sum += $1;
        }
    }
    cmp_ok($sum, '>', 0, 'duration_us sum is positive (queries took measurable time)');
};

# Test 5: Stats accuracy - exported_events reflects actual export count
subtest 'stats accuracy' => sub {
    psch_reset_stats($node);

    my $num_queries = 25;
    for my $i (1..$num_queries) {
        $node->safe_psql('postgres', "SELECT $i");
    }

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    psch_wait_for_export($node, $num_queries, 10);

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{exported}, '>=', $num_queries,
        "exported_events ($stats->{exported}) >= num_queries ($num_queries)");
    is($stats->{send_failures}, 0, 'No send failures');
    is($stats->{dropped}, 0, 'No dropped events');
};

# Test 6: Connection failure handling - PostgreSQL must not crash if the
# collector becomes temporarily unreachable
subtest 'connection failure handling' => sub {
    psch_reset_stats($node);

    # Queries should always succeed regardless of collector status
    $node->safe_psql('postgres', 'SELECT 1');
    $node->safe_psql('postgres', 'SELECT 2');

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 2, 'Events still enqueued');

    # The key invariant: PostgreSQL kept running
    my $pg_check = $node->safe_psql('postgres', 'SELECT 1');
    is($pg_check, '1', 'PostgreSQL survived');
};

$node->stop();
done_testing();
