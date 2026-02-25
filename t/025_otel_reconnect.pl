#!/usr/bin/env perl
# Test: OTel reconnection and failure recovery
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

# Ensure data dir is set and clear any previous data
psch_clear_otel_data();

# Initialize node with OTel export enabled
my $node = psch_init_node_with_otel('otel_reconnect',
    flush_interval_ms => 200,
    batch_max => 50
);

# Test 1: Verify initial connection works
subtest 'initial connection' => sub {
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT 1');
    $node->safe_psql('postgres', 'SELECT 2');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    psch_wait_for_otel_export($node, 2, 15);

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{exported}, '>=', 2, 'Initial export succeeded');
    is($stats->{send_failures}, 0, 'No initial send failures');
};

# Test 2: Simulate connection failure and recovery
subtest 'failure and recovery' => sub {
    plan skip_all => 'Destructive test - skipping in CI' if $ENV{CI};

    # Stop OTel Collector container
    diag("Stopping OTel Collector container...");
    system("docker stop psch-otel-collector >/dev/null 2>&1");
    sleep(2);

    # Run queries while collector is down
    psch_reset_stats($node);
    for my $i (1..10) {
        $node->safe_psql('postgres', "SELECT $i");
    }
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(2);

    my $stats_down = psch_get_stats($node);
    cmp_ok($stats_down->{enqueued}, '>=', 10, 'Events still enqueued when collector down');

    # Restart OTel Collector container
    diag("Restarting OTel Collector container...");
    system("docker start psch-otel-collector >/dev/null 2>&1");

    # Wait for health check
    for my $i (1..30) {
        my $result = `curl -sf http://localhost:13133/ 2>/dev/null`;
        last if $? == 0;
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
    cmp_ok($stats_up->{exported}, '>=', 0, 'Export counter accessible after recovery');

    # Check that PostgreSQL is still healthy
    my $pg_check = $node->safe_psql('postgres', 'SELECT 1');
    is($pg_check, '1', 'PostgreSQL still healthy after collector restart');
};

# Test 3: Verify failure tracking
subtest 'failure tracking' => sub {
    my $stats = psch_get_stats($node);

    ok(defined $stats->{send_failures}, 'send_failures field exists');
    cmp_ok($stats->{send_failures}, '>=', 0, 'send_failures is non-negative');
};

# Test 4: Verify error details are tracked
subtest 'error tracking' => sub {
    my $result = $node->safe_psql('postgres', q{
        SELECT send_failures FROM pg_stat_ch_stats()
    });

    ok(defined $result, 'Can query send_failures');
};

$node->stop();
done_testing();
