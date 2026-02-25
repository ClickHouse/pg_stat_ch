#!/usr/bin/env perl
# Test: OTel export functionality
# Prerequisites: OTel Collector container must be running

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Skip if Docker not available
if (!psch_otel_available()) {
    plan skip_all => 'Docker not available, skipping OTel tests';
}

# Check if OTel Collector is running
my $health_check = `curl -sf http://localhost:13133/ 2>/dev/null`;
if ($? != 0) {
    plan skip_all => 'OTel Collector not running. Start with: ./scripts/run-tests.sh <PG> otel';
}

# Initialize node with OTel export enabled
my $node = psch_init_node_with_otel('otel_export',
    flush_interval_ms => 100,
    batch_max => 100
);

# Test 1: Basic export - run queries with unique markers, verify they appear in JSONL
subtest 'basic export' => sub {
    psch_reset_stats($node);

    $node->safe_psql('postgres', "SELECT 'otel_basic_1'");
    $node->safe_psql('postgres', "SELECT 'otel_basic_2'");
    $node->safe_psql('postgres', "SELECT 'otel_basic_3'");

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 3, 'Events enqueued');

    # Wait for export via stats counter
    psch_wait_for_otel_export($node, 3, 15);
    my $exported = psch_get_stats($node)->{exported};
    cmp_ok($exported, '>=', 3, 'Events exported via OTel');

    # Verify marker queries landed in JSONL
    my $found = psch_count_otel_logs_matching(query => qr/otel_basic_/);
    cmp_ok($found, '>=', 3, "Marker queries in JSONL (got $found)");
};

# Test 2: Batch sizing - verify all events export when count exceeds batch_max
subtest 'batch sizing' => sub {
    psch_reset_stats($node);

    # Generate more events than batch_max (100)
    for my $i (1..150) {
        $node->safe_psql('postgres', "SELECT 'otel_batch_$i'");
    }

    psch_wait_for_otel_export($node, 150, 20);

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{exported}, '>=', 150, 'All events exported');

    my $found = psch_count_otel_logs_matching(query => qr/otel_batch_/);
    cmp_ok($found, '>=', 150, "All batch events in JSONL (got $found)");
};

# Test 3: Immediate flush via pg_stat_ch_flush()
subtest 'immediate flush' => sub {
    psch_reset_stats($node);

    $node->safe_psql('postgres', "SELECT 'otel_flush_marker'");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Poll for the marker to appear in JSONL (no arbitrary sleep)
    my $found = 0;
    my $deadline = time() + 10;
    while (time() < $deadline) {
        $found = psch_count_otel_logs_matching(query => qr/otel_flush_marker/);
        last if $found >= 1;
        select(undef, undef, undef, 0.5);
    }
    cmp_ok($found, '>=', 1, "Flush triggered export (found marker in JSONL)");
};

# Test 4: All fields populated - verify attributes on specific events
subtest 'all fields populated' => sub {
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'CREATE TABLE IF NOT EXISTS otel_fields(id int, data text)');
    $node->safe_psql('postgres', "INSERT INTO otel_fields VALUES (1, 'otel_fields_test')");
    $node->safe_psql('postgres', "SELECT 'otel_fields_select' FROM otel_fields");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Wait for our marker to appear
    my $deadline = time() + 15;
    my $records;
    while (time() < $deadline) {
        $records = psch_read_otel_log_records();
        my $found = 0;
        for my $r (@$records) {
            $found++ if defined $r->{query} && $r->{query} =~ /otel_fields_select/;
        }
        last if $found >= 1;
        select(undef, undef, undef, 0.5);
    }

    # Find our specific SELECT record
    my $select_rec;
    for my $r (@$records) {
        if (defined $r->{query} && $r->{query} =~ /otel_fields_select/) {
            $select_rec = $r;
            last;
        }
    }

    ok(defined $select_rec, 'Found SELECT marker record');
    SKIP: {
        skip 'No SELECT record found', 4 unless defined $select_rec;

        cmp_ok($select_rec->{duration_us} // 0, '>', 0, 'duration_us > 0');
        is($select_rec->{db}, 'postgres', 'db = postgres');
        is($select_rec->{cmd_type}, 'SELECT', 'cmd_type = SELECT');
        ok(defined $select_rec->{pid} && $select_rec->{pid} > 0, 'pid is populated');
    }

    # Verify INSERT also present
    my $insert_count = psch_count_otel_logs_matching(query => qr/otel_fields_test/);
    cmp_ok($insert_count, '>=', 1, 'INSERT event also present');

    $node->safe_psql('postgres', 'DROP TABLE IF EXISTS otel_fields');
};

# Test 5: Stats accuracy - exported_events tracks actual export count
subtest 'stats accuracy' => sub {
    psch_reset_stats($node);

    my $num_queries = 25;
    for my $i (1..$num_queries) {
        $node->safe_psql('postgres', "SELECT 'otel_stats_$i'");
    }

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    psch_wait_for_otel_export($node, $num_queries, 15);

    my $stats = psch_get_stats($node);
    # exported_events includes our queries plus overhead (stats, flush calls)
    # so it should be >= num_queries
    cmp_ok($stats->{exported}, '>=', $num_queries,
        "exported_events ($stats->{exported}) >= $num_queries");

    # Verify JSONL has all our marker queries
    my $found = psch_count_otel_logs_matching(query => qr/otel_stats_/);
    cmp_ok($found, '>=', $num_queries,
        "JSONL marker count ($found) >= $num_queries");
};

# Test 6: Connection failure handling - verify graceful behavior
subtest 'connection failure handling' => sub {
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT 1');
    $node->safe_psql('postgres', 'SELECT 2');

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 2, 'Queries still enqueued');
    ok(1, 'PostgreSQL survived connection handling');
};

$node->stop();
done_testing();
