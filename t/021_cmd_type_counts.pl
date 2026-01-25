#!/usr/bin/env perl
# Test: Command type count verification in ClickHouse
# Verifies that each SQL command type (SELECT, INSERT, UPDATE, DELETE, UTILITY)
# is correctly classified and counted in ClickHouse events_raw table.

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Skip if Docker/ClickHouse not available
if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running';
}

my $node = psch_init_node_with_clickhouse('cmd_type_counts',
    flush_interval_ms => 100,
    batch_max => 100
);

# Helper: parse cmd_type counts from ClickHouse
sub get_cmd_type_counts {
    my $result = psch_query_clickhouse(
        "SELECT cmd_type, count() FROM pg_stat_ch.events_raw GROUP BY cmd_type FORMAT TabSeparated"
    );
    my %counts;
    for my $line (split /\n/, $result) {
        next unless $line;
        my ($type, $cnt) = split /\t/, $line;
        $counts{$type} = $cnt if defined $type && defined $cnt;
    }
    return %counts;
}

# Subtest 1: DML command types (SELECT, INSERT, UPDATE, DELETE)
subtest 'DML command type counts' => sub {
    # Setup and reset BEFORE truncating ClickHouse - these events won't be counted
    $node->safe_psql('postgres', 'CREATE TABLE IF NOT EXISTS dml_test(id int, val text)');
    $node->safe_psql('postgres', 'TRUNCATE dml_test');
    psch_reset_stats($node);

    # Flush any pending events and wait for export
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(1);

    # NOW truncate ClickHouse - start clean measurement window
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");

    # Execute exact counts of each DML type
    $node->safe_psql('postgres', 'SELECT 1');
    $node->safe_psql('postgres', 'SELECT 2');
    $node->safe_psql('postgres', 'SELECT 3');  # 3 SELECTs

    $node->safe_psql('postgres', "INSERT INTO dml_test VALUES (1, 'a')");
    $node->safe_psql('postgres', "INSERT INTO dml_test VALUES (2, 'b')");  # 2 INSERTs

    $node->safe_psql('postgres', "UPDATE dml_test SET val = 'x' WHERE id = 1");  # 1 UPDATE

    $node->safe_psql('postgres', "DELETE FROM dml_test WHERE id = 2");  # 1 DELETE

    # Flush to export (flush itself adds 1 SELECT)
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(2);  # Wait for export without polling (polling adds SELECTs)

    my %counts = get_cmd_type_counts();

    # Verify exact counts (flush adds 1 SELECT, so expect 4)
    is($counts{SELECT} // 0, 4, 'SELECT count = 4 (3 + flush)');
    is($counts{INSERT} // 0, 2, 'INSERT count = 2');
    is($counts{UPDATE} // 0, 1, 'UPDATE count = 1');
    is($counts{DELETE} // 0, 1, 'DELETE count = 1');

    diag("DML counts: SELECT=$counts{SELECT}, INSERT=$counts{INSERT}, UPDATE=$counts{UPDATE}, DELETE=$counts{DELETE}");

    $node->safe_psql('postgres', 'DROP TABLE dml_test');
};

# Subtest 2: DDL/Utility command types
subtest 'DDL/Utility command type counts' => sub {
    # Reset and flush before truncating ClickHouse
    psch_reset_stats($node);
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(1);

    # NOW truncate ClickHouse - start clean measurement window
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");

    # Execute various DDL operations - all should be UTILITY
    $node->safe_psql('postgres', 'CREATE TABLE ddl_test(id int)');           # 1
    $node->safe_psql('postgres', 'ALTER TABLE ddl_test ADD COLUMN name text'); # 2
    $node->safe_psql('postgres', 'CREATE INDEX ddl_idx ON ddl_test(id)');     # 3
    $node->safe_psql('postgres', 'DROP INDEX ddl_idx');                       # 4
    $node->safe_psql('postgres', 'TRUNCATE ddl_test');                        # 5
    $node->safe_psql('postgres', 'VACUUM ddl_test');                          # 6
    $node->safe_psql('postgres', 'ANALYZE ddl_test');                         # 7
    $node->safe_psql('postgres', 'DROP TABLE ddl_test');                      # 8

    # Flush to export (flush adds 1 SELECT)
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(2);  # Wait for export without polling

    my %counts = get_cmd_type_counts();

    is($counts{UTILITY} // 0, 8, 'UTILITY count = 8 DDL statements');
    is($counts{SELECT} // 0, 1, 'SELECT count = 1 (flush only)');

    diag("DDL counts: UTILITY=$counts{UTILITY}, SELECT=$counts{SELECT}");
};

# Subtest 3: Mixed workload with exact verification
subtest 'mixed workload counts' => sub {
    # Reset and flush before truncating ClickHouse
    psch_reset_stats($node);
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(1);

    # NOW truncate ClickHouse - start clean measurement window
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");

    # Create table (1 UTILITY)
    $node->safe_psql('postgres', 'CREATE TABLE mixed_test(id int, val text)');

    # INSERT 3 rows (3 INSERTs)
    $node->safe_psql('postgres', "INSERT INTO mixed_test VALUES (1, 'a')");
    $node->safe_psql('postgres', "INSERT INTO mixed_test VALUES (2, 'b')");
    $node->safe_psql('postgres', "INSERT INTO mixed_test VALUES (3, 'c')");

    # SELECT 2 times (2 SELECTs)
    $node->safe_psql('postgres', 'SELECT * FROM mixed_test');
    $node->safe_psql('postgres', 'SELECT count(*) FROM mixed_test');

    # UPDATE 1 time (1 UPDATE)
    $node->safe_psql('postgres', "UPDATE mixed_test SET val = 'updated' WHERE id = 1");

    # DELETE 1 time (1 DELETE)
    $node->safe_psql('postgres', 'DELETE FROM mixed_test WHERE id = 3');

    # More DDL (2 UTILITY)
    $node->safe_psql('postgres', 'CREATE INDEX mixed_idx ON mixed_test(id)');
    $node->safe_psql('postgres', 'DROP INDEX mixed_idx');

    # Cleanup table (1 UTILITY)
    $node->safe_psql('postgres', 'DROP TABLE mixed_test');

    # Flush to export (flush adds 1 SELECT)
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(2);  # Wait for export without polling

    my %counts = get_cmd_type_counts();

    # Verify exact counts
    # SELECT: 2 queries + 1 flush = 3
    # INSERT: 3
    # UPDATE: 1
    # DELETE: 1
    # UTILITY: CREATE TABLE + CREATE INDEX + DROP INDEX + DROP TABLE = 4
    is($counts{SELECT} // 0, 3, 'SELECT count = 3 (2 queries + flush)');
    is($counts{INSERT} // 0, 3, 'INSERT count = 3');
    is($counts{UPDATE} // 0, 1, 'UPDATE count = 1');
    is($counts{DELETE} // 0, 1, 'DELETE count = 1');
    is($counts{UTILITY} // 0, 4, 'UTILITY count = 4 (DDL statements)');

    diag("Mixed workload counts:");
    for my $type (sort keys %counts) {
        diag("  $type: $counts{$type}");
    }
};

# Subtest 4: Comprehensive DDL coverage - various object types
subtest 'comprehensive DDL coverage' => sub {
    # Reset and flush before truncating ClickHouse
    psch_reset_stats($node);
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(2);  # Extra sleep to ensure all previous events are exported

    # NOW truncate ClickHouse - start clean measurement window
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");

    # Test comprehensive DDL coverage - 11 UTILITY statements
    # Note: PRIMARY KEY creates implicit index, generating an extra UTILITY event
    $node->safe_psql('postgres', 'CREATE SCHEMA test_schema');                            # 1
    $node->safe_psql('postgres', 'CREATE TABLE test_schema.t1(id int)');                  # 2
    $node->safe_psql('postgres', 'CREATE SEQUENCE test_seq');                             # 3
    $node->safe_psql('postgres', 'CREATE VIEW test_view AS SELECT 1 AS x');               # 4
    $node->safe_psql('postgres', 'CREATE TYPE test_enum AS ENUM (\'a\', \'b\')');         # 5
    $node->safe_psql('postgres', 'COMMENT ON TABLE test_schema.t1 IS \'test comment\'');  # 6

    # Cleanup - 5 more UTILITY statements
    $node->safe_psql('postgres', 'DROP TYPE test_enum');        # 7
    $node->safe_psql('postgres', 'DROP VIEW test_view');        # 8
    $node->safe_psql('postgres', 'DROP SEQUENCE test_seq');     # 9
    $node->safe_psql('postgres', 'DROP TABLE test_schema.t1');  # 10
    $node->safe_psql('postgres', 'DROP SCHEMA test_schema');    # 11

    # Flush to export (flush adds 1 SELECT)
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    sleep(2);  # Wait for export without polling

    my %counts = get_cmd_type_counts();

    # All DDL should be UTILITY - exactly 11
    is($counts{UTILITY} // 0, 11,
        'UTILITY count = 11 (SCHEMA, TABLE, SEQUENCE, VIEW, TYPE, COMMENT)');
    is($counts{SELECT} // 0, 1, 'SELECT count = 1 (flush only)');

    diag("Comprehensive DDL: UTILITY=$counts{UTILITY}, SELECT=$counts{SELECT}");
};

$node->stop();
done_testing();
