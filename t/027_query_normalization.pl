#!/usr/bin/env perl
# Test: Query normalization — literal constants replaced with $N placeholders
# Prerequisites: ClickHouse container must be running

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Skip if Docker/ClickHouse not available
if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping normalization tests';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running';
}

psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

my $node = psch_init_node_with_clickhouse('normalize',
    flush_interval_ms => 100,
    batch_max => 100
);

# Helper: run a query, flush, and return the captured query text from ClickHouse.
# Uses queryid to identify the specific query.
sub get_captured_query {
    my ($node, $sql) = @_;

    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', $sql);
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    psch_wait_for_export($node, 1, 10);

    # Small delay for ClickHouse to process the insert
    sleep(1);

    # Get the most recent non-extension query (skip pg_stat_ch internal queries)
    my $captured = psch_query_clickhouse(
        "SELECT query FROM pg_stat_ch.events_raw " .
        "WHERE query NOT LIKE '%pg_stat_ch%' " .
        "AND query NOT LIKE '%pg_extension%' " .
        "AND query != '' " .
        "ORDER BY ts_start DESC LIMIT 1"
    );
    chomp($captured);
    return $captured;
}

# ---------------------------------------------------------------------------
# Test 1: String literal normalization
# ---------------------------------------------------------------------------
subtest 'string literal replaced with placeholder' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM pg_class WHERE relname = 'pg_type'");
    like($q, qr/\$1/, 'String literal replaced with $N');
    unlike($q, qr/'pg_type'/, 'Original string value not present');
    like($q, qr/relname/, 'Column name preserved');
    like($q, qr/pg_class/, 'Table name preserved');
};

# ---------------------------------------------------------------------------
# Test 2: Numeric literal normalization
# ---------------------------------------------------------------------------
subtest 'numeric literals replaced' => sub {
    $node->safe_psql('postgres', 'CREATE TABLE test_norm(id int, val text)');
    $node->safe_psql('postgres', "INSERT INTO test_norm VALUES (1, 'a')");

    my $q = get_captured_query($node,
        "SELECT * FROM test_norm WHERE id = 42");
    like($q, qr/\$1/, 'Numeric literal replaced with $N');
    unlike($q, qr/\b42\b/, 'Original numeric value not present');
    like($q, qr/test_norm/, 'Table name preserved');
};

# ---------------------------------------------------------------------------
# Test 3: Multiple constants get sequential $N
# ---------------------------------------------------------------------------
subtest 'multiple constants get sequential placeholders' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_norm WHERE id > 10 AND val = 'secret'");
    like($q, qr/\$1/, 'First constant replaced');
    like($q, qr/\$2/, 'Second constant replaced');
    unlike($q, qr/\b10\b/, 'Numeric value 10 not present');
    unlike($q, qr/'secret'/, 'String value not present');
};

# ---------------------------------------------------------------------------
# Test 4: Negative number normalization
# ---------------------------------------------------------------------------
subtest 'negative number normalized' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_norm WHERE id = -99");
    like($q, qr/\$1/, 'Negative number replaced');
    unlike($q, qr/-99/, 'Negative value not present');
};

# ---------------------------------------------------------------------------
# Test 5: INSERT with multiple values
# ---------------------------------------------------------------------------
subtest 'INSERT values normalized' => sub {
    my $q = get_captured_query($node,
        "INSERT INTO test_norm VALUES (100, 'sensitive_data')");
    like($q, qr/\$1/, 'First value replaced');
    like($q, qr/\$2/, 'Second value replaced');
    unlike($q, qr/100/, 'Numeric value not present');
    unlike($q, qr/sensitive_data/, 'Sensitive string not present');
    like($q, qr/INSERT INTO test_norm/, 'Table name and keyword preserved');
};

# ---------------------------------------------------------------------------
# Test 6: UPDATE with SET and WHERE constants
# ---------------------------------------------------------------------------
subtest 'UPDATE constants normalized' => sub {
    my $q = get_captured_query($node,
        "UPDATE test_norm SET val = 'new_password' WHERE id = 7");
    like($q, qr/\$1/, 'SET value replaced');
    like($q, qr/\$2/, 'WHERE value replaced');
    unlike($q, qr/new_password/, 'Sensitive value not present');
    unlike($q, qr/\b7\b/, 'WHERE numeric value not present');
};

# ---------------------------------------------------------------------------
# Test 7: Query without constants passes through unchanged
# ---------------------------------------------------------------------------
subtest 'query without constants unchanged' => sub {
    my $q = get_captured_query($node,
        "SELECT id, val FROM test_norm ORDER BY id");
    like($q, qr/SELECT id, val FROM test_norm ORDER BY id/,
        'Query without constants is preserved');
    unlike($q, qr/\$\d/, 'No placeholders when no constants');
};

# ---------------------------------------------------------------------------
# Test 8: Float literal normalization
# ---------------------------------------------------------------------------
subtest 'float literal normalized' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_norm WHERE id > 3.14");
    like($q, qr/\$1/, 'Float literal replaced');
    unlike($q, qr/3\.14/, 'Float value not present');
};

# ---------------------------------------------------------------------------
# Test 9: Boolean-like constant
# ---------------------------------------------------------------------------
subtest 'boolean constant in expression' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_norm WHERE id = 1 AND val IS NOT NULL");
    like($q, qr/\$1/, 'Constant replaced');
    like($q, qr/IS NOT NULL/, 'IS NOT NULL preserved');
};

# ---------------------------------------------------------------------------
# Test 10: Multi-statement: each statement normalized independently
# ---------------------------------------------------------------------------
subtest 'multi-statement normalization' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Run two separate statements that each have constants
    $node->safe_psql('postgres', "SELECT * FROM test_norm WHERE id = 77");
    $node->safe_psql('postgres', "SELECT * FROM test_norm WHERE val = 'multi_test'");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    psch_wait_for_export($node, 2, 10);
    sleep(1);

    my $all_queries = psch_query_clickhouse(
        "SELECT query FROM pg_stat_ch.events_raw " .
        "WHERE query LIKE '%test_norm%' " .
        "AND query NOT LIKE '%pg_stat_ch%' " .
        "ORDER BY ts_start DESC"
    );
    unlike($all_queries, qr/\b77\b/, 'First statement constant normalized');
    unlike($all_queries, qr/multi_test/, 'Second statement constant normalized');
};

# ---------------------------------------------------------------------------
# Test 11: IN list normalization
# ---------------------------------------------------------------------------
subtest 'IN list constants normalized' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_norm WHERE id IN (1, 2, 3, 4, 5)");
    like($q, qr/\$/, 'IN list values replaced with placeholders');
    # PG18+ squashes IN lists: IN ($1 /*, ... */)
    # Verify the raw values (2,3,4,5) are gone; $1 contains "1" so skip that
    unlike($q, qr/\b[2-5]\b/, 'Individual IN list values not present');
    unlike($q, qr/,\s*\d/, 'No comma-separated raw numbers in IN list');
    like($q, qr/IN/, 'IN keyword preserved');
};

# ---------------------------------------------------------------------------
# Test 12: DELETE with string constant
# ---------------------------------------------------------------------------
subtest 'DELETE constant normalized' => sub {
    my $q = get_captured_query($node,
        "DELETE FROM test_norm WHERE val = 'drop_me'");
    like($q, qr/\$1/, 'DELETE WHERE value replaced');
    unlike($q, qr/drop_me/, 'Sensitive value not present');
};

# ---------------------------------------------------------------------------
# Test 13: Subquery with constants
# ---------------------------------------------------------------------------
subtest 'subquery constants normalized' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_norm WHERE id IN (SELECT 42)");
    unlike($q, qr/\b42\b/, 'Subquery constant not present');
    like($q, qr/\$/, 'Placeholder present');
};

# ---------------------------------------------------------------------------
# Test 14: LIKE pattern with string constant
# ---------------------------------------------------------------------------
subtest 'LIKE pattern normalized' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_norm WHERE val LIKE '%password%'");
    like($q, qr/\$1/, 'LIKE pattern replaced');
    unlike($q, qr/password/, 'LIKE pattern value not present');
    like($q, qr/LIKE/, 'LIKE keyword preserved');
};

# ---------------------------------------------------------------------------
# Test 15: BETWEEN with constants
# ---------------------------------------------------------------------------
subtest 'BETWEEN constants normalized' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_norm WHERE id BETWEEN 10 AND 20");
    unlike($q, qr/\b10\b/, 'BETWEEN lower bound not present');
    unlike($q, qr/\b20\b/, 'BETWEEN upper bound not present');
    like($q, qr/BETWEEN/, 'BETWEEN keyword preserved');
};

# ---------------------------------------------------------------------------
# Test 16: Schema and table names preserved in complex query
# ---------------------------------------------------------------------------
subtest 'schema qualified names preserved' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM pg_catalog.pg_class WHERE oid = 1234");
    like($q, qr/pg_catalog\.pg_class/, 'Schema.table preserved');
    unlike($q, qr/\b1234\b/, 'OID constant not present');
};

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', 'DROP TABLE IF EXISTS test_norm');
$node->stop();
done_testing();
