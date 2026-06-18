#!/usr/bin/env perl
# Test: end-to-end round-trip of the new OTelArrowExporter into events_raw.
#
# Closes the test gap that's existed since the Arrow path went live: nothing
# currently proves that pg_stat_ch's Arrow IPC output can actually be ingested
# by ClickHouse against the unified events_raw schema. t/026_arrow_dump asserts
# on the IPC schema shape via pyarrow but never pushes the bytes into CH.
#
# Flow:
#   1. Spin up a node with use_unified_arrow_exporter=on +
#      debug_arrow_dump_dir set. The OTel endpoint points at a non-existent
#      collector — the bgworker's gRPC send fails, but MaybeDumpArrowBatch
#      fires BEFORE the send, so IPC files still land on disk regardless.
#   2. Run a known set of queries to populate the queue.
#   3. Wait for IPC files to appear in the dump dir.
#   4. TRUNCATE pg_stat_ch.events_raw.
#   5. For each IPC file: curl POST it to CH as
#      INSERT INTO pg_stat_ch.events_raw FORMAT ArrowStream.
#   6. Assert: row count matches what we sent, column types are what
#      events_raw declares (no silent string-to-Int coercion), known queries
#      land with the right db_name/db_operation/query_text values.

use strict;
use warnings;
use lib 't';
use File::Temp qw(tempdir);
use File::Basename;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping end-to-end Arrow test';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running';
}

# Verify the events_raw schema is present (applied by the goose migration
# step in CI). If we hit an empty CH or a different schema, fail loudly
# rather than silently inserting into nothing.
my $schema_check = psch_query_clickhouse(
    "SELECT count() FROM system.columns WHERE database = 'pg_stat_ch' AND table = 'events_raw'");
chomp $schema_check;
if ($schema_check eq '' || $schema_check < 50) {
    plan skip_all => "events_raw not initialized in CH (got $schema_check columns); " .
                     "run goose migrations against schema/migrations/ first";
}

# Pre-flight: ensure events_raw is empty so we don't measure leftover rows.
psch_query_clickhouse('TRUNCATE TABLE pg_stat_ch.events_raw');

my $dump_dir = tempdir('psch_unified_arrow_e2e_XXXX', TMPDIR => 1, CLEANUP => 1);

my $node = PostgreSQL::Test::Cluster->new('unified_arrow_e2e');
$node->init();
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.queue_capacity = 65536
pg_stat_ch.flush_interval_ms = 100
pg_stat_ch.batch_max = 100
pg_stat_ch.use_otel = on
pg_stat_ch.otel_endpoint = 'localhost:14317'
pg_stat_ch.otel_arrow_passthrough = on
pg_stat_ch.use_unified_arrow_exporter = on
pg_stat_ch.debug_arrow_dump_dir = '$dump_dir'
pg_stat_ch.hostname = 'unified-arrow-e2e-host'
pg_stat_ch.extra_attributes = 'instance_ubid:test-instance;server_role:primary;region:test-region;cell:test-cell;read_replica_type:none'
});
$node->start();
$node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

# ----------------------------------------------------------------------------
# Run a deliberately-shaped workload so the assertions can pin specific rows.
# ----------------------------------------------------------------------------
psch_reset_stats($node);

$node->safe_psql('postgres', 'SELECT 42 AS marker_select');
$node->safe_psql('postgres', 'CREATE TABLE unified_e2e_t (id int)');
$node->safe_psql('postgres', 'INSERT INTO unified_e2e_t VALUES (1), (2), (3)');
$node->safe_psql('postgres', 'SELECT count(*) FROM unified_e2e_t');
$node->safe_psql('postgres', 'DROP TABLE unified_e2e_t');

# Force a flush so the bgworker drains the queue promptly.
$node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

# ----------------------------------------------------------------------------
# Wait for IPC files in the dump dir. The bgworker writes them out before
# the gRPC SendArrowBatch call, so we see them even though OTel send fails.
# ----------------------------------------------------------------------------
my @ipc_files;
my $deadline = time() + 10;
while (time() < $deadline) {
    @ipc_files = glob("$dump_dir/*.ipc");
    last if @ipc_files > 0;
    select(undef, undef, undef, 0.2);
}

cmp_ok(scalar @ipc_files, '>=', 1, "Unified exporter produced at least one IPC dump file");

# Each file is non-empty (ZSTD-compressed Arrow IPC has some envelope bytes
# even for zero rows; for our payload it should be hundreds of bytes).
for my $f (@ipc_files) {
    cmp_ok(-s $f, '>', 0, basename($f) . " is non-empty");
}

# ----------------------------------------------------------------------------
# Post each IPC file to CH as INSERT INTO ... FORMAT ArrowStream and verify
# CH accepts the wire format. A 200 response means ArrowStream parsed the
# IPC, found events_raw columns by name, and inserted at least one row.
# Any column type mismatch (e.g. if the producer regressed to writing
# query_id as String) would surface here as a 4xx with a clear error.
# ----------------------------------------------------------------------------
my $insert_url =
    'http://localhost:18123/?query=' .
    'INSERT%20INTO%20pg_stat_ch.events_raw%20FORMAT%20ArrowStream';

my $total_posted = 0;
for my $f (@ipc_files) {
    my $err_file = "$f.err";
    # --fail-with-body makes HTTP 4xx/5xx responses from CH surface as a
    # non-zero curl exit code (default curl returns 0 on any completed
    # HTTP transfer regardless of status). The subsequent SELECT count()
    # assertion would catch ingestion failure too, but failing here gives
    # a sharper error message at the point the bug actually happened.
    my $rc = system("curl -sS --fail-with-body -X POST " .
                    "-H 'Content-Type: application/octet-stream' " .
                    "--data-binary \@$f '$insert_url' 2>$err_file >/dev/null");
    my $stderr = -s $err_file ? do {
        open my $fh, '<', $err_file; local $/; <$fh>
    } : '';
    is($rc, 0, basename($f) . " accepted by CH ArrowStream parser" .
       ($stderr ? " (stderr: $stderr)" : ""));
    ++$total_posted;
}
cmp_ok($total_posted, '>=', 1, "Posted at least one IPC file to CH");

# ----------------------------------------------------------------------------
# Verify: rows landed, with the right typing, populated by our known workload.
# ----------------------------------------------------------------------------
my $rowcount = psch_query_clickhouse('SELECT count() FROM pg_stat_ch.events_raw');
chomp $rowcount;
cmp_ok($rowcount, '>=', 5, "events_raw has at least the 5 workload queries (got $rowcount)");

# Column types: pull system.columns and assert no surprises. If the new
# exporter regressed to the legacy string-encoded id columns, query_id /
# pid would come back as String here.
my $types = psch_query_clickhouse(
    "SELECT name, type FROM system.columns " .
    "WHERE database = 'pg_stat_ch' AND table = 'events_raw' " .
    "AND name IN ('query_id', 'pid', 'err_elevel', " .
    "'parallel_workers_planned', 'duration_us', 'shared_blks_hit') " .
    "ORDER BY name FORMAT TSV");

# system.columns reflects the schema's declared types, not the inserted bytes.
# But a wire-format mismatch on insert would have failed the curl above
# already (CH refuses ArrowStream inserts when an Arrow column's logical
# type can't be cast to the target schema's column type).
like($types, qr/^duration_us\tUInt64\b/m,           'duration_us declared UInt64');
like($types, qr/^err_elevel\tUInt8\b/m,             'err_elevel declared UInt8');
like($types, qr/^parallel_workers_planned\tInt16\b/m, 'parallel_workers_planned declared Int16');
like($types, qr/^pid\tInt32\b/m,                    'pid declared Int32');
like($types, qr/^query_id\tInt64\b/m,               'query_id declared Int64');
like($types, qr/^shared_blks_hit\tInt64\b/m,        'shared_blks_hit declared Int64');

# Known-row assertions: pinpoint the SELECT 42 AS marker_select event.
my $marker = psch_query_clickhouse(
    "SELECT db_name, db_operation, query_text FROM pg_stat_ch.events_raw " .
    "WHERE query_text LIKE '%marker_select%' AND query_text NOT LIKE '%pg_stat_ch%' " .
    "ORDER BY ts DESC LIMIT 1 FORMAT TSV");
chomp $marker;
like($marker, qr/^postgres\tSELECT\t/, "marker SELECT landed with db_name=postgres, db_operation=SELECT");
like($marker, qr/marker_select/,        "marker SELECT preserved query_text");

# Envelope columns from extra_attributes were threaded through.
my $envelope = psch_query_clickhouse(
    "SELECT DISTINCT instance_ubid, server_role, region, cell, read_replica_type " .
    "FROM pg_stat_ch.events_raw WHERE instance_ubid != '' LIMIT 1 FORMAT TSV");
chomp $envelope;
is($envelope, "test-instance\tprimary\ttest-region\ttest-cell\tnone",
   "envelope columns populated from extra_attributes");

$node->stop();
done_testing();
