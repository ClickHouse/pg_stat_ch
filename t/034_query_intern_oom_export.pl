#!/usr/bin/env perl
# Test: Round-trip contract under DSA exhaustion — events from the OOM window
# land in ClickHouse with empty query text but populated numeric fields.
#
# Prerequisites: ClickHouse container must be running
#   (docker compose -f docker/docker-compose.test.yml up -d)
#
# t/033_query_intern_oom.pl asserts the in-process contract (dsa_oom counter
# grows, enqueue still works) when interning fails on a tight DSA pool.  This
# test extends the same workload to the export path: with a real ClickHouse
# downstream we can confirm what actually *lands* — the row exists, all its
# numeric/identity columns are populated, and only the query text is empty.
# This is the customer-visible contract: telemetry survives storage pressure.
#
# Strategy:
#   1. Tight 8MB DSA pool, real ClickHouse, *slow* flush interval (5s) so the
#      producer can outrun the exporter and actually fill the pool.
#   2. Send the same many-distinct-query workload as 033.
#   3. After the workload, force-flush and wait for the drain to complete.
#   4. Ask ClickHouse for the slice of rows where the interner failed
#      (query = '') and assert those rows carry duration_us, db, cmd_type,
#      and pid — the contract that "numeric telemetry is preserved on intern
#      failure" actually holds end-to-end.

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::BackgroundPsql;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Skip if Docker/ClickHouse not available
if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping ClickHouse tests';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running. ' .
                     'Start with: docker compose -f docker/docker-compose.test.yml up -d';
}

psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

# ---------------------------------------------------------------------------
# Node config: tight DSA pool, real ClickHouse, *deliberately slow* flush.
#
# The 5-second flush interval gives the producer time to outrun the exporter
# and actually fill the DSA pool — the prerequisite for the OOM path we want
# to verify.  A fast 100ms flush would drain refs so quickly the pool may
# never accumulate enough live bodies to OOM, making the test flaky.  After
# the workload finishes we force-flush and wait for drain so ClickHouse can
# be queried for the resulting rows.
# ---------------------------------------------------------------------------
my $node = PostgreSQL::Test::Cluster->new('query_intern_oom_export');
$node->init();
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.queue_capacity = 8192
pg_stat_ch.string_area_size = 8MB
pg_stat_ch.flush_interval_ms = 5000
pg_stat_ch.batch_max = 1000
pg_stat_ch.clickhouse_host = 'localhost'
pg_stat_ch.clickhouse_port = 19000
pg_stat_ch.clickhouse_database = 'pg_stat_ch'
});
$node->start();
$node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

# Capture our pid so we can filter ClickHouse rows to events we produced
# (the cluster setup itself runs queries through pg_stat_ch hooks too).
psch_reset_stats($node);

# ---------------------------------------------------------------------------
# Distinct-query workload (same shape as 033).  We run through a persistent
# session so all events share one pid — easier to filter in ClickHouse.
# ---------------------------------------------------------------------------
my $distinct_count    = 6000;
my $aliases_per_query = 100;

my $session = $node->background_psql('postgres', on_error_stop => 1);

my ($pid_out, $ret) = $session->query('SELECT pg_backend_pid()');
die "failed to get backend pid" unless $ret == 0;
my ($pid) = $pid_out =~ /(\d+)/;
die "backend pid missing from psql output: $pid_out" unless defined $pid;

my $chunk_size = 250;
my $chunk      = '';
my $sent       = 0;

for (my $k = 1; $k <= $distinct_count; $k++) {
    my @aliases = map { "1 AS c_${k}_$_" } (1 .. $aliases_per_query);
    # LIMIT 0 suppresses the 100-column row from psql echo without changing
    # queryid/intern-key distinctness (see 033 for the rationale).
    $chunk .= "SELECT " . join(", ", @aliases) . " LIMIT 0;\n";

    if ($k % $chunk_size == 0) {
        (undef, $ret) = $session->query($chunk);
        last if $ret != 0;
        $chunk = '';
        $sent  = $k;
    }
}
if ($chunk ne '' && $ret == 0) {
    (undef, $ret) = $session->query($chunk);
    $sent = $distinct_count if $ret == 0;
}
is($ret, 0, "distinct queries dispatched ($sent of $distinct_count)");

# Force a final flush so any leftover events in the ring drain to CH before
# we start asking ClickHouse about row counts.
$session->query('SELECT pg_stat_ch_flush()');
$session->quit();

# Wait for the exporter to actually move everything through.  At our slow
# 5-second flush interval the bgworker may need a couple of cycles to clear
# the ring; psch_wait_for_export polls until exported >= the count or the
# timeout elapses.
psch_wait_for_export($node, $sent, 30);

my $stats = psch_get_stats($node);
note(sprintf("stats: enqueued=%s dropped=%s queue_size=%s dsa_oom=%s exported=%s",
             $stats->{enqueued}, $stats->{dropped},
             $stats->{queue_size}, $stats->{dsa_oom}, $stats->{exported}));

cmp_ok($stats->{dsa_oom}, '>', 0,
       "dsa_oom_count grew during the workload (proves intern OOM path ran)");

# ---------------------------------------------------------------------------
# Round-trip assertions
#
# Pull two slices from ClickHouse:
#   - "intern_failed": rows where query is empty.  These are the events whose
#     intern alloc failed (the producer set slot->query_len = 0).
#   - "intern_ok":     rows where query is non-empty.  Reference set; these
#     came from before the pool filled (or from queryids that were reused).
#
# Contract under test:
#   - intern_failed > 0  (we *did* drop some text; this proves the OOM path
#     reached the consumer, not just the in-process counter).
#   - every intern_failed row still carries duration_us, db, and cmd_type —
#     the numeric/identity telemetry the customer relies on for slow-query
#     analysis even when SQL text is unavailable.
# ---------------------------------------------------------------------------
my $intern_failed_rows = psch_wait_for_clickhouse_query(
    "SELECT count() FROM pg_stat_ch.events_raw " .
    "WHERE pid = $pid AND query = ''",
    sub { $_[0] >= 1 },
    15);
cmp_ok($intern_failed_rows, '>=', 1,
       "ClickHouse has rows with empty query (got $intern_failed_rows)");

# Among the intern_failed rows, count how many have duration_us > 0.
# We expect ALL of them to — a SELECT can complete in microseconds but never
# in *zero* microseconds at our clock resolution.  Allowing a tiny tail of
# zero-duration rows would mask a real regression (e.g. if a future change
# accidentally zeroed the fixed-prefix when intern failed), so we assert
# equality with the total count of empty-query rows.
my $intern_failed_with_metrics = psch_query_clickhouse(
    "SELECT count() FROM pg_stat_ch.events_raw " .
    "WHERE pid = $pid AND query = '' AND duration_us > 0 " .
    "AND db = 'postgres' AND cmd_type != ''");
cmp_ok($intern_failed_with_metrics, '>=', $intern_failed_rows,
       "every empty-query row carries duration_us, db, and cmd_type " .
       "($intern_failed_with_metrics of $intern_failed_rows)");

# Sanity: there should also be at least *some* rows whose query text did
# make it through.  Otherwise we're not testing degradation of an
# otherwise-working system, we're testing total failure.
my $intern_ok_rows = psch_query_clickhouse(
    "SELECT count() FROM pg_stat_ch.events_raw " .
    "WHERE pid = $pid AND query != ''");
cmp_ok($intern_ok_rows, '>=', 1,
       "ClickHouse has at least one row with non-empty query " .
       "(intern was working before pool filled; got $intern_ok_rows)");

$node->stop();
done_testing();
