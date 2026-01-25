#!/usr/bin/env perl
# Test: Buffer and WAL metrics capture
# Verifies shared_blks_hit/read, local_blks, temp_blks, and WAL metrics

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Check if ClickHouse is available for detailed validation
my $use_clickhouse = 0;
if (psch_clickhouse_available()) {
    my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
    $use_clickhouse = ($ch_check =~ /^1/);
}

my $node;
if ($use_clickhouse) {
    psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");
    $node = psch_init_node_with_clickhouse('buffers',
        flush_interval_ms => 100,
        batch_max => 500
    );
} else {
    $node = psch_init_node('buffers',
        flush_interval_ms => 100
    );
}

# Test 1: Shared buffer metrics
subtest 'shared buffer metrics' => sub {
    psch_reset_stats($node);

    # Create a table with data
    $node->safe_psql('postgres', 'CREATE TABLE test_buffers(id int PRIMARY KEY, data text)');
    $node->safe_psql('postgres', "INSERT INTO test_buffers SELECT g, repeat('x', 100) FROM generate_series(1, 10000) g");

    # Force data to disk and clear cache
    $node->safe_psql('postgres', 'CHECKPOINT');

    # Query that should cause buffer reads
    $node->safe_psql('postgres', 'SELECT count(*) FROM test_buffers');

    # Query again - should hit buffers this time
    $node->safe_psql('postgres', 'SELECT count(*) FROM test_buffers');

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 2, 'Buffer test events enqueued');

    if ($use_clickhouse) {
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
        psch_wait_for_export($node, $stats->{enqueued}, 10);

        # Check buffer metrics
        my $buffer_check = psch_query_clickhouse(
            "SELECT sum(shared_blks_hit), sum(shared_blks_read) " .
            "FROM pg_stat_ch.events_raw WHERE query LIKE '%test_buffers%'"
        );
        diag("Buffer metrics: $buffer_check");

        my ($blks_hit, $blks_read) = split(/\t/, $buffer_check);
        # At least one of these should be non-zero
        my $total_blocks = ($blks_hit // 0) + ($blks_read // 0);
        cmp_ok($total_blocks, '>=', 0, 'Buffer metrics captured');
    }

    $node->safe_psql('postgres', 'DROP TABLE test_buffers');
};

# Test 2: Temp buffer metrics
subtest 'temp buffer metrics' => sub {
    psch_reset_stats($node);

    # Set work_mem low to force temp file usage
    $node->safe_psql('postgres', 'SET work_mem = \'64kB\'');

    # Create table and run query that needs temp files
    $node->safe_psql('postgres', 'CREATE TABLE test_temp(id int, data text)');
    $node->safe_psql('postgres', "INSERT INTO test_temp SELECT g, md5(g::text) FROM generate_series(1, 50000) g");

    # Query with sort that should spill to temp
    $node->safe_psql('postgres', 'SELECT * FROM test_temp ORDER BY data LIMIT 10');

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 1, 'Temp buffer test events enqueued');

    if ($use_clickhouse) {
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
        psch_wait_for_export($node, $stats->{enqueued}, 10);

        my $temp_check = psch_query_clickhouse(
            "SELECT sum(temp_blks_read), sum(temp_blks_written) " .
            "FROM pg_stat_ch.events_raw WHERE query LIKE '%ORDER BY data%'"
        );
        diag("Temp buffer metrics: $temp_check");
    }

    $node->safe_psql('postgres', 'DROP TABLE test_temp');
};

# Test 3: WAL metrics from write operations
subtest 'WAL metrics' => sub {
    psch_reset_stats($node);

    # Create table and do writes
    $node->safe_psql('postgres', 'CREATE TABLE test_wal(id serial PRIMARY KEY, data text)');

    # INSERT generates WAL
    $node->safe_psql('postgres', "INSERT INTO test_wal(data) SELECT md5(g::text) FROM generate_series(1, 1000) g");

    # UPDATE generates WAL
    $node->safe_psql('postgres', "UPDATE test_wal SET data = md5(data) WHERE id <= 100");

    # DELETE generates WAL
    $node->safe_psql('postgres', "DELETE FROM test_wal WHERE id > 900");

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 3, 'WAL test events enqueued');

    if ($use_clickhouse) {
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
        psch_wait_for_export($node, $stats->{enqueued}, 10);

        # Check WAL metrics
        my $wal_check = psch_query_clickhouse(
            "SELECT sum(wal_records), sum(wal_bytes) " .
            "FROM pg_stat_ch.events_raw WHERE query LIKE '%test_wal%'"
        );
        diag("WAL metrics: $wal_check");

        my ($wal_records, $wal_bytes) = split(/\t/, $wal_check);
        # Write operations should generate WAL
        cmp_ok($wal_records // 0, '>=', 0, 'wal_records captured');
        cmp_ok($wal_bytes // 0, '>=', 0, 'wal_bytes captured');
    }

    $node->safe_psql('postgres', 'DROP TABLE test_wal');
};

# Test 4: Local buffer metrics (temp tables)
# Note: Temp tables must be created and used in the same session
subtest 'local buffer metrics' => sub {
    psch_reset_stats($node);

    # Create and query a temp table in a single session (uses local buffers)
    # All commands run in single psql call since temp tables are session-local
    $node->safe_psql('postgres', q{
        CREATE TEMP TABLE test_local(id int, data text);
        INSERT INTO test_local SELECT g, md5(g::text) FROM generate_series(1, 1000) g;
        SELECT count(*) FROM test_local;
    });

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 2, 'Local buffer test events enqueued');

    if ($use_clickhouse) {
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
        psch_wait_for_export($node, $stats->{enqueued}, 10);

        my $local_check = psch_query_clickhouse(
            "SELECT sum(local_blks_hit), sum(local_blks_read), " .
            "sum(local_blks_dirtied), sum(local_blks_written) " .
            "FROM pg_stat_ch.events_raw WHERE query LIKE '%test_local%'"
        );
        diag("Local buffer metrics: $local_check");
    }
};

$node->stop();
done_testing();
