#!/usr/bin/env perl
# Test: producer-to-collector routing on pg_stat_ch.block_format.
#
# Validates the producer-side half of the Arrow exporter migration contract:
# the central OTel collector's routingconnector must be able to fan batches
# between the legacy (query_logs_arrow) receiver path and the new (events_raw)
# receiver path based on a self-identifying attribute the producer emits.
#
# This is the "generic" routing test — no CH receiver, no datagres-arrow-exporter.
# Stock otelcol-contrib with two file sinks instead. Catches:
#   - Producer-side regression in pg_stat_ch.block_format emission
#     (e.g. unified path accidentally falling back to "arrow_ipc")
#   - Routingconnector config-shape drift (e.g. attribute name typo, OTTL
#     statement that no longer matches)
#   - OTLP-side serialization regressions that would make the marker
#     unreachable to a routing connector matching on resource attributes
#
# Does NOT validate the events_raw column-set contract or the receiver's
# wire-format expectations — those live behind the datagres-arrow-exporter
# in clickgres-platform and are covered by a separate test there.
#
# Flow:
#   1. Spin up the routing collector via docker compose (otel-routing profile).
#   2. Start a node configured to ship to it; first with the unified GUC OFF,
#      then OFF + with otel_arrow_passthrough so the legacy ArrowBatchBuilder
#      path fires; finally with the unified GUC ON.
#   3. Wait for JSONL files in docker/otel-routing/output/ and assert each
#      arm landed where expected based on the marker value.

use strict;
use warnings;
use lib 't';
use Test::More;
use Time::HiRes qw(sleep time);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use psch;

if (!psch_routing_collector_available()) {
    # Try to bring it up; the helper dies if Docker isn't available.
    eval { psch_start_routing_collector() };
    if ($@) {
        plan skip_all => "Docker / routing collector not available: $@";
    }
}

my $project_dir = $ENV{PROJECT_DIR} // '.';
my $output_dir  = "$project_dir/docker/otel-routing/output";

# ----------------------------------------------------------------------------
# Helper: spin up a pg_stat_ch node configured to ship to the routing
# collector. Caller picks which GUC combination to enable.
# ----------------------------------------------------------------------------
sub start_node {
    my ($name, %opts) = @_;
    my $arrow_passthrough = $opts{arrow_passthrough} // 'off';
    my $unified           = $opts{unified}           // 'off';

    my $node = PostgreSQL::Test::Cluster->new($name);
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.queue_capacity = 65536
pg_stat_ch.flush_interval_ms = 100
pg_stat_ch.batch_max = 100
pg_stat_ch.use_otel = on
pg_stat_ch.otel_endpoint = 'localhost:14317'
pg_stat_ch.otel_arrow_passthrough = $arrow_passthrough
pg_stat_ch.use_unified_arrow_exporter = $unified
pg_stat_ch.hostname = 'routing-test-host'
});
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');
    return $node;
}

# Truncate-equivalent: remove any JSONL the collector wrote on a previous arm
# of this test. The collector keeps writing into the same file across pg
# clusters, so we have to snapshot byte-offsets per arm and only consider the
# tail.
sub current_size {
    my ($path) = @_;
    return -e $path ? (-s $path) : 0;
}

# Wait until file size at $path grows past $baseline, or timeout. Returns the
# new size on success, undef on timeout.
sub wait_for_growth {
    my ($path, $baseline, $timeout_s) = @_;
    my $deadline = time() + $timeout_s;
    while (time() < $deadline) {
        my $size = current_size($path);
        return $size if $size > $baseline;
        sleep(0.2);
    }
    return undef;
}

# ----------------------------------------------------------------------------
# Arm 1: legacy ArrowBatchBuilder path (unified=off, arrow_passthrough=on).
# Producer should emit pg_stat_ch.block_format=arrow_ipc; routingconnector
# fans to logs/legacy -> file/legacy. Other files should NOT grow.
# ----------------------------------------------------------------------------
{
    my $legacy_before     = current_size("$output_dir/legacy.jsonl");
    my $events_raw_before = current_size("$output_dir/events_raw.jsonl");
    my $default_before    = current_size("$output_dir/default.jsonl");

    my $node = start_node('routing_legacy_arm',
                          arrow_passthrough => 'on', unified => 'off');
    $node->safe_psql('postgres', 'SELECT 1 AS legacy_arm_marker');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $legacy_after = wait_for_growth("$output_dir/legacy.jsonl", $legacy_before, 15);
    ok(defined $legacy_after,
       'arm 1 (arrow_ipc): legacy.jsonl received bytes after producer flush');

    my $events_raw_after = current_size("$output_dir/events_raw.jsonl");
    is($events_raw_after, $events_raw_before,
       'arm 1 (arrow_ipc): events_raw.jsonl did NOT grow (routing kept it isolated)');

    my $default_after = current_size("$output_dir/default.jsonl");
    is($default_after, $default_before,
       'arm 1 (arrow_ipc): default.jsonl did NOT grow (no unmatched batches)');

    $node->stop();
}

# ----------------------------------------------------------------------------
# Arm 2: unified path (unified=on). Producer should emit
# pg_stat_ch.block_format=arrow_events_raw; routingconnector fans to
# logs/events_raw -> file/events_raw. Legacy file should NOT grow.
# ----------------------------------------------------------------------------
{
    my $legacy_before     = current_size("$output_dir/legacy.jsonl");
    my $events_raw_before = current_size("$output_dir/events_raw.jsonl");
    my $default_before    = current_size("$output_dir/default.jsonl");

    my $node = start_node('routing_unified_arm',
                          arrow_passthrough => 'on', unified => 'on');
    $node->safe_psql('postgres', 'SELECT 2 AS unified_arm_marker');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $events_raw_after =
        wait_for_growth("$output_dir/events_raw.jsonl", $events_raw_before, 15);
    ok(defined $events_raw_after,
       'arm 2 (arrow_events_raw): events_raw.jsonl received bytes after producer flush');

    my $legacy_after = current_size("$output_dir/legacy.jsonl");
    is($legacy_after, $legacy_before,
       'arm 2 (arrow_events_raw): legacy.jsonl did NOT grow (routing kept it isolated)');

    my $default_after = current_size("$output_dir/default.jsonl");
    is($default_after, $default_before,
       'arm 2 (arrow_events_raw): default.jsonl did NOT grow (no unmatched batches)');

    # Spot-check the JSONL: extract the new bytes since this arm started and
    # confirm the marker value appears. Catches a producer-side regression
    # that emits the right ATTRIBUTE NAME but the wrong value (which
    # routingconnector would catch by falling through to default — already
    # asserted above — but the explicit content check pins the contract).
    open my $fh, '<', "$output_dir/events_raw.jsonl"
        or die "open events_raw.jsonl: $!";
    seek($fh, $events_raw_before, 0);
    my $tail = do { local $/; <$fh> };
    close $fh;
    like($tail, qr/arrow_events_raw/,
         'arm 2: marker value "arrow_events_raw" appears in the routed JSONL');
    unlike($tail, qr/arrow_ipc/,
           'arm 2: legacy marker "arrow_ipc" does NOT appear in events_raw arm');

    $node->stop();
}

psch_stop_routing_collector();
done_testing();
