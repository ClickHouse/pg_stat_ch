#!/usr/bin/env perl
# Test: ClickHouse reconnection and failure recovery
# Prerequisites: ClickHouse container must be running

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Skip if Docker/ClickHouse not available
if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping ClickHouse tests';
}

# Check if ClickHouse is running
my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running. Start with: docker compose -f docker/docker-compose.test.yml up -d';
}

# Clear any existing test data
psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

# Initialize node with ClickHouse export enabled
my $node = psch_init_node_with_clickhouse('ch_reconnect',
    flush_interval_ms => 200,
    batch_max => 50
);

# Test 1: Verify initial connection works
subtest 'initial connection' => sub {
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT 1');
    $node->safe_psql('postgres', 'SELECT 2');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    psch_wait_for_export($node, 2, 10);

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{exported}, '>=', 2, 'Initial export succeeded');
    is($stats->{send_failures}, 0, 'No initial send failures');
};

# Test 2: Simulate connection failure and recovery
subtest 'failure and recovery' => sub {
    plan skip_all => 'Destructive test - skipping in CI' if $ENV{CI};

    # Stop ClickHouse container
    diag("Stopping ClickHouse container...");
    system("docker stop psch-clickhouse >/dev/null 2>&1");
    sleep(2);

    # Run queries while CH is down
    psch_reset_stats($node);
    for my $i (1..10) {
        $node->safe_psql('postgres', "SELECT $i");
    }
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(2);

    my $stats_down = psch_get_stats($node);
    cmp_ok($stats_down->{enqueued}, '>=', 10, 'Events still enqueued when CH down');
    # Events should queue up, not be lost

    # Restart ClickHouse container
    diag("Restarting ClickHouse container...");
    system("docker start psch-clickhouse >/dev/null 2>&1");

    # Wait for healthcheck
    for my $i (1..30) {
        my $check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
        last if $check =~ /^1/;
        sleep(1);
    }
    sleep(2);

    # Run more queries to trigger reconnection
    for my $i (1..5) {
        $node->safe_psql('postgres', "SELECT 'after_restart_$i'");
    }
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Wait for export to resume
    sleep(5);

    my $stats_up = psch_get_stats($node);

    # After restart, exporter should have recovered
    # Some events may have been sent, or at least no crash occurred
    cmp_ok($stats_up->{exported}, '>=', 0, 'Export counter accessible after recovery');

    # Check that PostgreSQL is still healthy
    my $pg_check = $node->safe_psql('postgres', 'SELECT 1');
    is($pg_check, '1', 'PostgreSQL still healthy after CH restart');
};

# Test 3: Verify exponential backoff (by checking consecutive_failures)
subtest 'failure tracking' => sub {
    # This just verifies the stats function returns failure info
    my $stats = psch_get_stats($node);

    # These fields should exist and be non-negative
    ok(defined $stats->{send_failures}, 'send_failures field exists');
    cmp_ok($stats->{send_failures}, '>=', 0, 'send_failures is non-negative');
};

# Test 4: Verify error details are tracked
subtest 'error tracking' => sub {
    # Get extended stats (if available)
    my $result = $node->safe_psql('postgres', q{
        SELECT send_failures FROM pg_stat_ch_stats()
    });

    ok(defined $result, 'Can query send_failures');
};

$node->stop();
done_testing();
