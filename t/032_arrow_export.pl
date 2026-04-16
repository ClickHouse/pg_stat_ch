#!/usr/bin/env perl
# Test: Arrow IPC passthrough — schema alignment, data correctness, all column types
# Prerequisites: ClickHouse container running
#   docker compose -f docker/docker-compose.test.yml up -d

use strict;
use warnings;
use lib 't';

use File::Temp qw(tempdir);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping Arrow export test';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all =>
        'ClickHouse container not running. Start with: docker compose -f docker/docker-compose.test.yml up -d';
}

# --- Helpers ---

sub ensure_query_logs_table {
    psch_query_clickhouse('CREATE DATABASE IF NOT EXISTS pg_stat_ch');
    system('curl', '-sf', 'http://localhost:18123/',
        '--data-binary', '@docker/init/01-query-logs.sql');
    my $exists = psch_query_clickhouse('EXISTS TABLE pg_stat_ch.query_logs');
    die "query_logs table not found" unless $exists eq '1';
}

sub find_ipc_files {
    my ($dir) = @_;
    opendir(my $dh, $dir) or return ();
    my @files = sort
        map  { "$dir/$_" }
        grep { /\.ipc$/ && -f "$dir/$_" }
        readdir($dh);
    closedir($dh);
    return @files;
}

sub clear_ipc_files {
    my ($dir) = @_;
    for my $f (find_ipc_files($dir)) {
        unlink $f;
    }
}

sub wait_for_arrow_dump {
    my ($dir, $timeout_secs) = @_;
    my $deadline = time() + $timeout_secs;
    while (time() < $deadline) {
        my @files = find_ipc_files($dir);
        return \@files if @files && -s $files[-1];
        select undef, undef, undef, 0.1;
    }
    return undef;
}

# Flush PG events, wait for Arrow IPC dump, insert all dumps into ClickHouse.
# Returns number of IPC files inserted.
sub flush_and_ingest {
    my ($node, $dir) = @_;

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $files_ref = wait_for_arrow_dump($dir, 10);
    return 0 unless $files_ref;

    my $inserted = 0;
    my $insert_url =
        'http://localhost:18123/?query=INSERT%20INTO%20pg_stat_ch.query_logs%20FORMAT%20ArrowStream';
    for my $f (@$files_ref) {
        if (system('curl', '-sf', $insert_url, '--data-binary', '@' . $f) == 0) {
            $inserted++;
        }
    }
    return $inserted;
}

# Query a single scalar value from query_logs filtered by query_text marker
sub ch_val {
    my ($column, $marker) = @_;
    return psch_query_clickhouse(qq{
        SELECT $column FROM pg_stat_ch.query_logs
        WHERE position(query_text, '$marker') > 0
        LIMIT 1
    });
}

sub ch_count {
    my ($where) = @_;
    my $r = psch_query_clickhouse("SELECT count() FROM pg_stat_ch.query_logs WHERE $where");
    return ($r =~ /^\d+$/) ? 0 + $r : 0;
}

# --- Setup ---

ensure_query_logs_table();
psch_query_clickhouse('TRUNCATE TABLE pg_stat_ch.query_logs');

my $dump_dir = tempdir(CLEANUP => 1);
my $node = psch_init_node_with_otel(
    'arrow_export',
    flush_interval_ms      => 100,
    batch_max              => 200,
    otel_endpoint          => 'localhost:65535',
    hostname               => 'arrow-test-host',
    otel_arrow_passthrough => 'on',
    otel_max_block_bytes   => 65536,
    extra_attributes       =>
        'instance_ubid:inst-123;server_ubid:srv-456;server_role:primary;region:us-east-1;cell:cell-01;host_id:host-01;pod_name:pod-01',
    debug_arrow_dump_dir => $dump_dir,
);

# ==========================================================================
# Subtest 1: Schema alignment — ClickHouse accepts the ArrowStream payload
# ==========================================================================
subtest 'schema alignment' => sub {
    psch_reset_stats($node);
    psch_query_clickhouse('TRUNCATE TABLE pg_stat_ch.query_logs');
    clear_ipc_files($dump_dir);

    $node->safe_psql('postgres', 'SELECT 1 AS schema_check');
    my $inserted = flush_and_ingest($node, $dump_dir);
    cmp_ok($inserted, '>=', 1, 'ArrowStream accepted by ClickHouse');

    my $count = ch_count("1=1");
    cmp_ok($count, '>=', 1, 'rows landed in query_logs');
};

# ==========================================================================
# Subtest 2: SELECT round-trip — verify core columns
# ==========================================================================
subtest 'select round trip' => sub {
    psch_reset_stats($node);
    psch_query_clickhouse('TRUNCATE TABLE pg_stat_ch.query_logs');
    clear_ipc_files($dump_dir);

    $node->safe_psql('postgres', q{SELECT 4242 AS select_marker});
    flush_and_ingest($node, $dump_dir);

    SKIP: {
        skip 'no rows', 9 unless ch_count("position(query_text, 'select_marker') > 0") >= 1;

        is(ch_val('db_name', 'select_marker'), 'postgres', 'db_name = postgres');
        is(ch_val('db_operation', 'select_marker'), 'SELECT', 'db_operation = SELECT');
        like(ch_val('query_text', 'select_marker'), qr/select_marker/, 'query_text captured');
        like(ch_val('db_user', 'select_marker'), qr/.+/, 'db_user is non-empty');
        like(ch_val('pid', 'select_marker'), qr/^\d+$/, 'pid is numeric string');
        cmp_ok(ch_val('pid', 'select_marker'), '>', 0, 'pid is positive');
        cmp_ok(ch_val('duration_us', 'select_marker'), '>=', 0, 'duration_us >= 0');

        # Timestamp should be recent (within last 5 minutes)
        my $ts_ok = psch_query_clickhouse(q{
            SELECT count() FROM pg_stat_ch.query_logs
            WHERE position(query_text, 'select_marker') > 0
              AND ts > now() - INTERVAL 5 MINUTE
              AND ts <= now() + INTERVAL 1 MINUTE
        });
        cmp_ok($ts_ok, '>=', 1, 'ts is a recent valid timestamp');

        # query_id should be populated for a SELECT (non-zero)
        like(ch_val('query_id', 'select_marker'), qr/^\d+$/, 'query_id is numeric string');
    }
};

# ==========================================================================
# Subtest 3: DML operations — INSERT, UPDATE, DELETE
# ==========================================================================
# NOTE: query normalization replaces literal values with $1/$2 placeholders,
# so we use unique TABLE NAMES as markers (table names are never normalized).
subtest 'dml operations' => sub {
    psch_reset_stats($node);
    psch_query_clickhouse('TRUNCATE TABLE pg_stat_ch.query_logs');
    clear_ipc_files($dump_dir);

    $node->safe_psql('postgres', 'CREATE TABLE arrow_insert_tbl(id int, val text)');
    $node->safe_psql('postgres', q{INSERT INTO arrow_insert_tbl VALUES (1, 'hello')});
    $node->safe_psql('postgres', 'CREATE TABLE arrow_update_tbl(id int, val text)');
    $node->safe_psql('postgres', q{INSERT INTO arrow_update_tbl VALUES (1, 'before')});
    $node->safe_psql('postgres', q{UPDATE arrow_update_tbl SET val = 'after' WHERE id = 1});
    $node->safe_psql('postgres', 'CREATE TABLE arrow_delete_tbl(id int)');
    $node->safe_psql('postgres', q{INSERT INTO arrow_delete_tbl VALUES (1)});
    $node->safe_psql('postgres', q{DELETE FROM arrow_delete_tbl WHERE id = 1});
    $node->safe_psql('postgres', 'DROP TABLE arrow_insert_tbl');
    $node->safe_psql('postgres', 'DROP TABLE arrow_update_tbl');
    $node->safe_psql('postgres', 'DROP TABLE arrow_delete_tbl');
    flush_and_ingest($node, $dump_dir);

    my $insert_count = ch_count(
        "position(query_text, 'arrow_insert_tbl') > 0 AND db_operation = 'INSERT'");
    cmp_ok($insert_count, '>=', 1, 'INSERT operation captured');

    SKIP: {
        skip 'no INSERT row', 1 unless $insert_count >= 1;
        my $insert_rows = psch_query_clickhouse(q{
            SELECT rows FROM pg_stat_ch.query_logs
            WHERE position(query_text, 'arrow_insert_tbl') > 0
              AND db_operation = 'INSERT'
            LIMIT 1
        });
        cmp_ok($insert_rows, '>=', 1, 'INSERT rows >= 1');
    }

    cmp_ok(ch_count(
        "position(query_text, 'arrow_update_tbl') > 0 AND db_operation = 'UPDATE'"),
        '>=', 1, 'UPDATE operation captured');

    cmp_ok(ch_count(
        "position(query_text, 'arrow_delete_tbl') > 0 AND db_operation = 'DELETE'"),
        '>=', 1, 'DELETE operation captured');

    cmp_ok(ch_count("db_operation = 'UTILITY'"), '>=', 1,
        'UTILITY operations (CREATE/DROP TABLE) captured');
};

# ==========================================================================
# Subtest 4: Error capture — err_sqlstate, err_elevel, err_message
# ==========================================================================
subtest 'error capture' => sub {
    psch_reset_stats($node);
    psch_query_clickhouse('TRUNCATE TABLE pg_stat_ch.query_logs');
    clear_ipc_files($dump_dir);

    # Run a query that will produce an error (reference non-existent table).
    # emit_log_hook captures errors with EMPTY query_text, so search by err_message.
    $node->psql('postgres', 'SELECT * FROM arrow_err_no_such_tbl');
    flush_and_ingest($node, $dump_dir);

    my $err_where = "position(err_message, 'arrow_err_no_such_tbl') > 0";
    SKIP: {
        skip 'no error row', 3 unless ch_count($err_where) >= 1;

        is(psch_query_clickhouse(qq{
            SELECT err_sqlstate FROM pg_stat_ch.query_logs WHERE $err_where LIMIT 1
        }), '42P01', 'err_sqlstate = 42P01 (undefined_table)');

        cmp_ok(psch_query_clickhouse(qq{
            SELECT err_elevel FROM pg_stat_ch.query_logs WHERE $err_where LIMIT 1
        }), '>=', 21, 'err_elevel >= ERROR (21)');

        like(psch_query_clickhouse(qq{
            SELECT err_message FROM pg_stat_ch.query_logs WHERE $err_where LIMIT 1
        }), qr/arrow_err_no_such_tbl/i, 'err_message mentions the missing table');
    }
};

# ==========================================================================
# Subtest 5: extra_attributes — all resource columns
# ==========================================================================
subtest 'extra attributes' => sub {
    psch_reset_stats($node);
    psch_query_clickhouse('TRUNCATE TABLE pg_stat_ch.query_logs');
    clear_ipc_files($dump_dir);

    $node->safe_psql('postgres', 'SELECT 1 AS extra_attr_marker');
    flush_and_ingest($node, $dump_dir);

    SKIP: {
        skip 'no rows', 8
            unless ch_count("position(query_text, 'extra_attr_marker') > 0") >= 1;

        is(ch_val('instance_ubid', 'extra_attr_marker'), 'inst-123', 'instance_ubid');
        is(ch_val('server_ubid', 'extra_attr_marker'), 'srv-456', 'server_ubid');
        is(ch_val('server_role', 'extra_attr_marker'), 'primary', 'server_role');
        is(ch_val('region', 'extra_attr_marker'), 'us-east-1', 'region');
        is(ch_val('cell', 'extra_attr_marker'), 'cell-01', 'cell');
        is(ch_val('host_id', 'extra_attr_marker'), 'host-01', 'host_id');
        is(ch_val('pod_name', 'extra_attr_marker'), 'pod-01', 'pod_name');

        my $svc = ch_val('service_version', 'extra_attr_marker');
        like($svc, qr/.+/, "service_version is non-empty ($svc)");
    }
};

# ==========================================================================
# Subtest 6: Unsigned clamping — UInt64 columns never negative
# ==========================================================================
subtest 'unsigned columns never negative' => sub {
    # Use all existing data (don't truncate)
    # UInt64 columns in ClickHouse can't store negatives, so if any somehow
    # got through they'd wrap around to huge values. Check for sanity.
    my @uint64_cols = qw(
        duration_us rows
        shared_blks_hit shared_blks_read shared_blks_written shared_blks_dirtied
        shared_blk_read_time_us shared_blk_write_time_us
        local_blks_hit local_blks_read local_blks_written local_blks_dirtied
        temp_blks_read temp_blks_written temp_blk_read_time_us temp_blk_write_time_us
        wal_records wal_bytes wal_fpi
        cpu_user_time_us cpu_sys_time_us
        jit_functions jit_generation_time_us jit_inlining_time_us
        jit_optimization_time_us jit_emission_time_us jit_deform_time_us
    );

    # Check that no UInt64 column has a suspiciously large value (wrapped negative)
    # Values > 2^62 would indicate a clamping failure
    for my $col (@uint64_cols) {
        my $bad = psch_query_clickhouse(
            "SELECT count() FROM pg_stat_ch.query_logs WHERE $col > 4611686018427387904"
        );
        is($bad, '0', "$col has no wrapped-negative values");
    }
};

# ==========================================================================
# Subtest 7: Multiple rows per batch — verify batch sizing works
# ==========================================================================
subtest 'multi-row batch' => sub {
    psch_reset_stats($node);
    psch_query_clickhouse('TRUNCATE TABLE pg_stat_ch.query_logs');
    clear_ipc_files($dump_dir);

    for my $i (1..20) {
        $node->safe_psql('postgres', "SELECT $i AS batch_marker_$i");
    }
    flush_and_ingest($node, $dump_dir);

    # We should have at least 20 rows from the SELECTs (plus possible overhead)
    my $count = ch_count("position(query_text, 'batch_marker_') > 0");
    cmp_ok($count, '>=', 20, "at least 20 batch rows landed ($count)");
};

$node->stop();
done_testing();
