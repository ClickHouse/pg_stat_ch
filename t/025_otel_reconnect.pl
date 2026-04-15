#!/usr/bin/env perl
# Test: OTel collector reconnection and failure recovery
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
my $node = psch_init_node_with_otel('otel_reconnect',
    flush_interval_ms => 200,
    batch_max         => 50,
);

# Test 1: Verify initial connection and export succeed
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

# Test 2: Stop the collector, verify events queue up, restart and verify recovery.
# Marked as destructive (skipped in CI) because it stops a shared container.
subtest 'failure and recovery' => sub {
    plan skip_all => 'Destructive test - skipping in CI' if $ENV{CI};

    diag("Stopping OTel collector container...");
    system("docker stop psch-otelcol >/dev/null 2>&1");
    sleep(2);

    # Run queries while collector is down; they should queue up, not be lost
    psch_reset_stats($node);
    for my $i (1..10) {
        $node->safe_psql('postgres', "SELECT $i");
    }
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(3);   # Allow one flush cycle to fail

    my $stats_down = psch_get_stats($node);
    cmp_ok($stats_down->{enqueued}, '>=', 10, 'Events queued while collector was down');
    cmp_ok($stats_down->{send_failures}, '>=', 1,
        'send_failures increments when OTLP export fails');

    diag("Restarting OTel collector container...");
    system("docker start psch-otelcol >/dev/null 2>&1");

    # Wait for collector to become healthy
    for my $i (1..30) {
        my $check = `curl -sf 'http://localhost:13133/' 2>/dev/null`;
        last if $check =~ /Server available/;
        sleep(1);
    }
    sleep(2);

    # Run more queries to trigger a successful flush cycle
    for my $i (1..5) {
        $node->safe_psql('postgres', "SELECT 'after_restart_$i'");
    }
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Wait for export to resume; the OTel gRPC channel reconnects automatically
    sleep(5);

    my $stats_up = psch_get_stats($node);

    # After recovery, exported_events should have increased
    cmp_ok($stats_up->{exported}, '>=', 1, 'Export resumed after collector restart');

    # PostgreSQL must still be responsive
    my $pg_check = $node->safe_psql('postgres', 'SELECT 1');
    is($pg_check, '1', 'PostgreSQL still healthy after collector restart');
};

# Test 3: Failure tracking fields are accessible and non-negative
subtest 'failure tracking' => sub {
    my $stats = psch_get_stats($node);

    ok(defined $stats->{send_failures}, 'send_failures field exists');
    cmp_ok($stats->{send_failures}, '>=', 0, 'send_failures is non-negative');
    ok(defined $stats->{exported}, 'exported field exists');
    cmp_ok($stats->{exported}, '>=', 0, 'exported is non-negative');
};

# Test 4: Verify send_failures is queryable via SQL function
subtest 'failure stats queryable' => sub {
    my $result = $node->safe_psql('postgres', q{
        SELECT send_failures FROM pg_stat_ch_stats()
    });
    ok(defined $result, 'Can query send_failures via pg_stat_ch_stats()');
    like($result, qr/^\d+$/, 'send_failures is a number');
};

$node->stop();
done_testing();
