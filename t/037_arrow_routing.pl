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
#   2. Start a node configured to ship to it, two arms:
#        arm 1: unified GUC off + otel_arrow_passthrough on (legacy
#               ArrowBatchBuilder path)
#        arm 2: unified GUC on (new typed-column path)
#   3. Wait for JSONL files in docker/otel-routing/output/ and assert each
#      arm landed where expected based on the marker value.
#
# Constraint: the routing collector binds fixed host ports (14317 OTLP gRPC,
# 23133 health) and a fixed output directory, so at most one instance of this
# test can run per machine. CI runs the TAP suite sequentially (prove without
# -j); two concurrent local runs of the suite would collide here regardless
# of container/output naming, because the port bindings are fixed.

use strict;
use warnings;
use lib 't';
use Test::More;
use Time::HiRes qw(sleep time);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use psch;

# Track whether THIS run started the collector. If it was already up (e.g. a
# developer brought it up manually for debugging, or a crashed prior run left
# it behind), leave it alone on exit. The flag is set before the start attempt
# so a partial startup (compose up succeeded, health check timed out) is still
# torn down.
my $started_routing_collector = 0;

if (!psch_routing_collector_available()) {
    # Try to bring it up; the helper dies if Docker isn't available.
    $started_routing_collector = 1;
    eval { psch_start_routing_collector() };
    if ($@) {
        plan skip_all => "Docker / routing collector not available: $@";
    }
}

# Runs on every exit path — normal completion, plan skip_all above, or a die()
# anywhere in the test body. Without this, a mid-test failure would leak the
# collector and its host ports (14317, 23133); t/026_arrow_dump.pl and
# t/036_unified_arrow_e2e.pl rely on nothing listening on 14317.
END {
    # system() inside END sets $?, which would clobber the script's exit
    # code; preserve it.
    local $?;
    psch_stop_routing_collector() if $started_routing_collector;
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

# Wait until the combined size of @paths has been stable for $stable_s
# seconds (returns 1), or $timeout_s elapsed (returns 0). Used between arms:
# the previous arm's node can flush trailing batches right up through its
# stop(), and the collector writes them asynchronously — the fileexporter
# even emits the JSON line and its trailing newline as separate writes. If
# the next arm snapshots its baselines mid-write, those late bytes get
# mis-attributed to it.
sub wait_for_quiet {
    my ($stable_s, $timeout_s, @paths) = @_;
    my $total = sub { my $t = 0; $t += current_size($_) for @paths; return $t };
    my $deadline     = time() + $timeout_s;
    my $last_size    = $total->();
    my $stable_since = time();
    while (time() < $deadline) {
        sleep(0.2);
        my $size = $total->();
        if ($size != $last_size) {
            $last_size    = $size;
            $stable_since = time();
        }
        return 1 if time() - $stable_since >= $stable_s;
    }
    return 0;
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

    # Producer-side bookkeeping first: on failure this distinguishes
    # "producer never exported" from "producer exported but the collector /
    # routing dropped it" (same pattern as t/024_otel_export.pl).
    my $exported = psch_wait_for_export($node, 1, 10);
    cmp_ok($exported, '>=', 1,
           'arm 1 (arrow_ipc): producer reported exported events');
    my $stats = psch_get_stats($node);
    is($stats->{send_failures}, 0, 'arm 1 (arrow_ipc): no send failures');

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
    # Let arm 1's trailing collector writes settle before snapshotting this
    # arm's baselines (see wait_for_quiet).
    wait_for_quiet(1.5, 15,
                   map { "$output_dir/$_.jsonl" } qw(legacy events_raw default));

    my $legacy_before     = current_size("$output_dir/legacy.jsonl");
    my $events_raw_before = current_size("$output_dir/events_raw.jsonl");
    my $default_before    = current_size("$output_dir/default.jsonl");

    my $node = start_node('routing_unified_arm',
                          arrow_passthrough => 'on', unified => 'on');
    $node->safe_psql('postgres', 'SELECT 2 AS unified_arm_marker');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Producer-side bookkeeping first (see arm 1).
    my $exported = psch_wait_for_export($node, 1, 10);
    cmp_ok($exported, '>=', 1,
           'arm 2 (arrow_events_raw): producer reported exported events');
    my $stats = psch_get_stats($node);
    is($stats->{send_failures}, 0, 'arm 2 (arrow_events_raw): no send failures');

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
    # Only meaningful if the file actually grew; on timeout the ok() above
    # already failed and there is no tail to inspect.
  SKIP: {
        skip 'events_raw.jsonl never grew; no tail to spot-check', 2
            unless defined $events_raw_after;
        open my $fh, '<', "$output_dir/events_raw.jsonl"
            or die "open events_raw.jsonl: $!";
        seek($fh, $events_raw_before, 0);
        my $tail = do { local $/; <$fh> };
        close $fh;
        like($tail, qr/arrow_events_raw/,
             'arm 2: marker value "arrow_events_raw" appears in the routed JSONL');
        unlike($tail, qr/arrow_ipc/,
               'arm 2: legacy marker "arrow_ipc" does NOT appear in events_raw arm');
    }

    $node->stop();
}

# Collector teardown happens in the END block above (only if this run
# started it).
done_testing();
