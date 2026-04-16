#!/usr/bin/env perl
# Test: Arrow IPC batch builder end-to-end via debug dump directory
#
# Verifies the full pipeline:
#   hooks → shared-memory queue → bgworker dequeue → ArrowBatchBuilder → IPC dump
#
# No external collector needed — uses pg_stat_ch.debug_arrow_dump_dir to capture
# the serialized Arrow IPC files to disk, then validates them with Arrow's IPC reader.

use strict;
use warnings;
use lib 't';
use File::Temp qw(tempdir);
use File::Basename;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# We need uv (for inline-script pyarrow) to validate IPC files.
my $have_uv = system("uv --version >/dev/null 2>&1") == 0;

# Create a temp directory for Arrow IPC dumps.
my $dump_dir = tempdir('psch_arrow_XXXX', TMPDIR => 1, CLEANUP => 1);

# Initialize node with Arrow passthrough + dump enabled.
# use_otel=on + otel_arrow_passthrough=on enables the Arrow export path.
# The otel_endpoint points at a non-existent collector — gRPC send will fail,
# but MaybeDumpArrowBatch() fires BEFORE the send, so IPC files still land.
my $node = PostgreSQL::Test::Cluster->new('arrow_dump');
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
pg_stat_ch.debug_arrow_dump_dir = '$dump_dir'
pg_stat_ch.hostname = 'test-arrow-host'
});
$node->start();
$node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

# ============================================================================
# Test 1: Arrow IPC files are produced when queries are executed
# ============================================================================
subtest 'arrow dump files produced' => sub {
    psch_reset_stats($node);

    # Run a few queries to generate events.
    $node->safe_psql('postgres', 'SELECT 1');
    $node->safe_psql('postgres', 'SELECT 2');
    $node->safe_psql('postgres', 'SELECT 3');

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 3, 'Events were enqueued');

    # Wait for the bgworker to dequeue and produce dump files.
    # The bgworker flushes every 100ms; give it a few cycles.
    my @ipc_files;
    my $deadline = time() + 10;
    while (time() < $deadline) {
        @ipc_files = glob("$dump_dir/*.ipc");
        last if @ipc_files > 0;
        select(undef, undef, undef, 0.2);    # sleep 200ms
    }

    cmp_ok(scalar @ipc_files, '>=', 1,
        "At least one .ipc dump file produced (got " . scalar(@ipc_files) . ")");

    # Each file should be non-empty.
    for my $f (@ipc_files) {
        my $size = -s $f;
        cmp_ok($size, '>', 0, basename($f) . " is non-empty ($size bytes)");
    }
};

# ============================================================================
# Test 2: Multiple batches when many events exceed max_block_bytes
# ============================================================================
subtest 'multiple dump files for large batch' => sub {
    # Clean previous dumps.
    unlink glob("$dump_dir/*.ipc");

    psch_reset_stats($node);

    # Use a small block size to force multiple Arrow batches.
    $node->safe_psql('postgres',
        "ALTER SYSTEM SET pg_stat_ch.otel_max_block_bytes = 65536");
    $node->safe_psql('postgres', 'SELECT pg_reload_conf()');
    sleep(1);

    # Generate enough events to exceed 64KB of Arrow IPC.
    for my $i (1 .. 200) {
        $node->safe_psql('postgres', "SELECT $i");
    }

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 200, 'Events enqueued for large batch');

    # Wait for dump files.
    my @ipc_files;
    my $deadline = time() + 15;
    while (time() < $deadline) {
        @ipc_files = glob("$dump_dir/*.ipc");
        last if @ipc_files >= 2;
        select(undef, undef, undef, 0.3);
    }

    cmp_ok(scalar @ipc_files, '>=', 2,
        "Multiple .ipc files produced (got " . scalar(@ipc_files) . ")");

    # Restore default.
    $node->safe_psql('postgres',
        "ALTER SYSTEM RESET pg_stat_ch.otel_max_block_bytes");
    $node->safe_psql('postgres', 'SELECT pg_reload_conf()');
};

# ============================================================================
# Test 3: Validate IPC file contents with pyarrow (if available)
# ============================================================================
SKIP: {
    skip 'uv not installed (needed for pyarrow validation)', 1 unless $have_uv;

    subtest 'ipc file contents valid' => sub {
        # Clean and produce a fresh dump.
        unlink glob("$dump_dir/*.ipc");

        psch_reset_stats($node);

        # Run a distinctive query we can look for.
        $node->safe_psql('postgres',
            'CREATE TABLE IF NOT EXISTS arrow_test(id int)');
        $node->safe_psql('postgres',
            "INSERT INTO arrow_test VALUES (42), (43), (44)");
        $node->safe_psql('postgres', 'SELECT * FROM arrow_test');
        $node->safe_psql('postgres', 'DROP TABLE arrow_test');

        # Wait for dump.
        my @ipc_files;
        my $deadline = time() + 10;
        while (time() < $deadline) {
            @ipc_files = glob("$dump_dir/*.ipc");
            last if @ipc_files > 0;
            select(undef, undef, undef, 0.2);
        }

        cmp_ok(scalar @ipc_files, '>=', 1, 'IPC dump file present for validation');

        # Validate with pyarrow via uv inline script.
        my $validation_script = <<'PYEOF';
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyarrow"]
# ///
import sys
import pyarrow as pa
import pyarrow.ipc

errors = []
total_rows = 0
schema = None

for path in sys.argv[1:]:
    try:
        with open(path, 'rb') as f:
            reader = pa.ipc.open_stream(f)
            schema = reader.schema
            for batch in reader:
                total_rows += batch.num_rows
    except Exception as e:
        errors.append(f"{path}: {e}")

if errors:
    print("ERRORS:" + "; ".join(errors))
    sys.exit(1)

# Check expected columns exist
expected = [
    'ts', 'db_name', 'db_user', 'db_operation', 'query_text',
    'duration_us', 'rows', 'pid', 'query_id',
    'shared_blks_hit', 'shared_blks_read',
    'wal_records', 'wal_bytes',
    'service_version', 'region',
]
missing = [c for c in expected if schema.get_field_index(c) == -1]
if missing:
    print(f"MISSING_COLUMNS:{','.join(missing)}")
    sys.exit(1)

# Check types for a few key columns
ts_field = schema.field('ts')
assert ts_field.type == pa.timestamp('ns', tz='UTC'), f"ts type wrong: {ts_field.type}"

dur_field = schema.field('duration_us')
assert dur_field.type == pa.uint64(), f"duration_us type wrong: {dur_field.type}"

rows_field = schema.field('rows')
assert rows_field.type == pa.uint64(), f"rows type wrong: {rows_field.type}"

print(f"OK:fields={len(schema)},rows={total_rows}")
PYEOF

        # Write script to temp file.
        my $script_path = "$dump_dir/_validate.py";
        open(my $fh, '>', $script_path) or die "Cannot write $script_path: $!";
        print $fh $validation_script;
        close $fh;

        my $file_args = join(' ', map { "'$_'" } @ipc_files);
        my $raw_output = `uv run '$script_path' $file_args 2>&1`;
        chomp($raw_output);
        # uv prints install progress on earlier lines; grab the last line.
        my @lines = split /\n/, $raw_output;
        my $output = $lines[-1] // '';

        like($output, qr/^OK:/, "pyarrow validation passed: $output");

        # Extract row count and verify we captured events.
        if ($output =~ /rows=(\d+)/) {
            cmp_ok($1, '>=', 3,
                "IPC files contain >= 3 rows (got $1)");
        }
    };
}

# ============================================================================
# Test 4: PostgreSQL stays healthy throughout (Arrow failures are non-fatal)
# ============================================================================
subtest 'postgres survives arrow export failures' => sub {
    my $result = $node->safe_psql('postgres', 'SELECT 1');
    is($result, '1', 'PostgreSQL is responsive after Arrow export attempts');

    # The bgworker should still be running.
    my $bgworker_count = $node->safe_psql('postgres', q{
        SELECT count(*) FROM pg_stat_activity
        WHERE backend_type = 'pg_stat_ch exporter'
    });
    is($bgworker_count, '1', 'Exporter bgworker is still alive');
};

$node->stop();
done_testing();
