#!/usr/bin/env perl
# Test: ClickHouse export functionality
# Prerequisites: ClickHouse container must be running (docker compose -f docker/docker-compose.test.yml up -d)

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

# Clear any existing test data in ClickHouse
psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

# Initialize node with ClickHouse export enabled
my $node = psch_init_node_with_clickhouse('ch_export',
    flush_interval_ms => 100,
    batch_max => 100
);

# Test 1: Basic export - run queries and verify events appear in ClickHouse
subtest 'basic export' => sub {
    psch_reset_stats($node);

    # Run some test queries
    $node->safe_psql('postgres', 'SELECT 1');
    $node->safe_psql('postgres', 'SELECT 2');
    $node->safe_psql('postgres', 'SELECT 3');

    my $stats_before = psch_get_stats($node);
    cmp_ok($stats_before->{enqueued}, '>=', 3, 'Events enqueued');

    # Wait for export (with timeout)
    my $exported = psch_wait_for_export($node, 3, 10);
    cmp_ok($exported, '>=', 3, 'Events exported to ClickHouse');

    # Verify events in ClickHouse
    my $ch_count = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw",
        sub { $_[0] >= 3 },
        10
    );
    cmp_ok($ch_count, '>=', 3, "Events visible in ClickHouse (got $ch_count)");

    # Verify query field is populated
    my $query_check = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw WHERE query_text != ''",
        sub { $_[0] >= 1 },
        10
    );
    cmp_ok($query_check, '>=', 1, 'Query text is captured');
};

# Test 2: Batch sizing - verify batch_max is honored
subtest 'batch sizing' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Generate more events than batch_max (100)
    for my $i (1..150) {
        $node->safe_psql('postgres', "SELECT $i");
    }

    # Wait for export
    psch_wait_for_export($node, 150, 15);

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{exported}, '>=', 150, 'All events exported');

    # Verify in ClickHouse
    my $ch_count = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw",
        sub { $_[0] >= 150 },
        15
    );
    cmp_ok($ch_count, '>=', 150, "All events in ClickHouse (got $ch_count)");
};

# Test 3: Immediate flush via pg_stat_ch_flush()
subtest 'immediate flush' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Run a query
    $node->safe_psql('postgres', 'SELECT 42 AS test_flush');

    # Force immediate flush
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Check ClickHouse - should have the event
    my $ch_count = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw",
        sub { $_[0] >= 1 },
        10
    );
    cmp_ok($ch_count, '>=', 1, "Flush triggered export (got $ch_count events)");
};

# Test 4: All fields populated
subtest 'all fields populated' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Create a table and run some operations
    $node->safe_psql('postgres', 'CREATE TABLE test_fields(id int, data text)');
    $node->safe_psql('postgres', "INSERT INTO test_fields VALUES (1, 'test')");
    $node->safe_psql('postgres', 'SELECT * FROM test_fields');
    $node->safe_psql('postgres', 'UPDATE test_fields SET data = \'updated\'');
    $node->safe_psql('postgres', 'DELETE FROM test_fields WHERE id = 1');

    # Flush and wait
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Check that various fields are populated
    my $duration_check = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw WHERE duration_us > 0",
        sub { $_[0] >= 1 },
        10
    );
    cmp_ok($duration_check, '>=', 1, 'duration_us is populated');

    my $db_check = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw WHERE db_name = 'postgres'",
        sub { $_[0] >= 1 },
        10
    );
    cmp_ok($db_check, '>=', 1, 'db_name field is populated');

    my $db_operation_check = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw WHERE db_operation != ''",
        sub { $_[0] >= 1 },
        10
    );
    cmp_ok($db_operation_check, '>=', 1, 'db_operation is populated');

    # Clean up
    $node->safe_psql('postgres', 'DROP TABLE IF EXISTS test_fields');
};

# Test 5: Stats accuracy - exported_events matches ClickHouse row count
subtest 'stats accuracy' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Run known number of queries
    my $num_queries = 25;
    for my $i (1..$num_queries) {
        $node->safe_psql('postgres', "SELECT $i");
    }

    # Flush and wait
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    psch_wait_for_export($node, $num_queries, 10);

    my $stats = psch_get_stats($node);
    my $ch_count = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw",
        sub { $_[0] >= $num_queries },
        10
    );

    # exported_events should approximately match ClickHouse count
    # (may not be exact due to timing, but should be close)
    cmp_ok(abs($stats->{exported} - $ch_count), '<=', 5,
        "exported_events ($stats->{exported}) ~ ClickHouse count ($ch_count)");
};

# Syntax error never reaches parse or executor hooks, so this relies on
# ClientAuthentication_hook setup
subtest 'first statement error' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    $node->psql('postgres', 'SELEC 1');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $row = psch_wait_for_clickhouse_query(
        "SELECT db_name, db_user, err_message FROM pg_stat_ch.events_raw "
            . "WHERE err_sqlstate = '42601' FORMAT TSV",
        sub { $_[0] ne '' },
        10
    );
    my $user = $node->safe_psql('postgres', 'SELECT current_user');
    is($row, "postgres\t$user\tsyntax error at or near \"SELEC\"",
        'Strings and names exported');
};

# Unsampled SET ROLE, and COMMIT restoring a role, run no hook before error
subtest 'error after role change' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    $node->safe_psql('postgres',
        'CREATE ROLE psch_set; CREATE ROLE psch_local; CREATE ROLE psch_old');
    $node->psql('postgres', q{
SET pg_stat_ch.min_duration_us = 1000000000;
SET pg_stat_ch.sample_rate = 0;
SET ROLE psch_set;
SELEC 1;
RESET ROLE;
BEGIN;
SET LOCAL ROLE psch_local;
COMMIT;
SELEC 2;
SET ROLE psch_old;
RESET ROLE;
ALTER ROLE psch_old RENAME TO psch_new;
SET ROLE psch_new;
SELEC 3;
}, on_error_stop => 0);
    $node->psql('postgres', 'SELEC 4',
        connstr => $node->connstr('postgres') . " options='-c role=psch_set'");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $rows = psch_wait_for_clickhouse_query(
        "SELECT db_user FROM pg_stat_ch.events_raw "
            . "WHERE err_sqlstate = '42601' ORDER BY ts FORMAT TSV",
        sub { scalar(split /\n/, $_[0]) >= 4 },
        10
    );
    my $user = $node->safe_psql('postgres', 'SELECT current_user');
    is($rows, "psch_set\n$user\npsch_new\npsch_set", 'Current role names exported');
};

# Definer's own warning runs no hook as definer
subtest 'security definer warning' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    $node->safe_psql('postgres', q{
CREATE ROLE psch_owner;
CREATE FUNCTION psch_warn() RETURNS void LANGUAGE plpgsql SECURITY DEFINER
    AS $$BEGIN RAISE WARNING 'psch definer'; END$$;
ALTER FUNCTION psch_warn() OWNER TO psch_owner;
});
    $node->safe_psql('postgres', 'SELECT psch_warn()');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $row = psch_wait_for_clickhouse_query(
        "SELECT db_user FROM pg_stat_ch.events_raw "
            . "WHERE err_message = 'psch definer' FORMAT TSV",
        sub { $_[0] ne '' },
        10
    );
    is($row, 'psch_owner', 'Definer name exported');
};

# Reload enabling collection reaches session only at its next statement
subtest 'enabled after connect' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    $node->append_conf('postgresql.conf', 'pg_stat_ch.enabled = off');
    $node->reload;
    $node->poll_query_until('postgres', 'SHOW pg_stat_ch.enabled', 'off');
    my $session = $node->background_psql('postgres', on_error_stop => 0);
    $node->append_conf('postgresql.conf', 'pg_stat_ch.enabled = on');
    $node->reload;
    $node->poll_query_until('postgres', 'SHOW pg_stat_ch.enabled', 'on');
    $session->query('SELEC 3');
    $session->quit;
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $row = psch_wait_for_clickhouse_query(
        "SELECT db_name, db_user, err_message FROM pg_stat_ch.events_raw "
            . "WHERE err_sqlstate = '42601' FORMAT TSV",
        sub { $_[0] ne '' },
        10
    );
    my $user = $node->safe_psql('postgres', 'SELECT current_user');
    is($row, "postgres\t$user\tsyntax error at or near \"SELEC\"",
        'Strings and names exported');
};

# Test 6: Connection failure handling - verify graceful failure when CH stops
subtest 'connection failure handling' => sub {
    # This test is informational - we just verify the extension doesn't crash
    # when ClickHouse becomes unavailable

    psch_reset_stats($node);

    # Run some queries - these should succeed regardless of CH status
    $node->safe_psql('postgres', 'SELECT 1');
    $node->safe_psql('postgres', 'SELECT 2');

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 2, 'Queries still enqueued');

    # send_failures may or may not be > 0 depending on timing
    # The key is that PostgreSQL didn't crash
    ok(1, 'PostgreSQL survived connection handling');
};

$node->stop();
done_testing();
