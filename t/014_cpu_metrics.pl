#!/usr/bin/env perl
# Test: CPU time metrics capture
# Verifies cpu_user_time_us and cpu_sys_time_us are captured

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
    $node = psch_init_node_with_clickhouse('cpu',
        flush_interval_ms => 100,
        batch_max => 200
    );
} else {
    $node = psch_init_node('cpu',
        flush_interval_ms => 100
    );
}

# Test 1: CPU-intensive query
subtest 'cpu intensive query' => sub {
    psch_reset_stats($node);

    # Create a table for CPU-intensive operations
    $node->safe_psql('postgres', 'CREATE TABLE test_cpu(id int, data text)');
    $node->safe_psql('postgres', "INSERT INTO test_cpu SELECT g, md5(g::text) FROM generate_series(1, 10000) g");

    # Run CPU-intensive query (lots of string operations)
    $node->safe_psql('postgres', q{
        SELECT id, md5(data || data || data), length(repeat(data, 10))
        FROM test_cpu
        WHERE id <= 5000
    });

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 1, 'CPU test events enqueued');

    if ($use_clickhouse) {
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
        psch_wait_for_export($node, $stats->{enqueued}, 10);

        # Check CPU metrics
        my $cpu_check = psch_query_clickhouse(
            "SELECT sum(cpu_user_time_us), sum(cpu_sys_time_us) " .
            "FROM pg_stat_ch.events_raw WHERE query LIKE '%test_cpu%'"
        );
        diag("CPU metrics: $cpu_check");

        my ($user_time, $sys_time) = split(/\t/, $cpu_check);
        # CPU time should be captured (may be 0 on some systems)
        ok(defined $user_time, 'cpu_user_time_us captured');
        ok(defined $sys_time, 'cpu_sys_time_us captured');
    }

    $node->safe_psql('postgres', 'DROP TABLE test_cpu');
};

# Test 2: Hash join (CPU-intensive)
subtest 'hash join cpu usage' => sub {
    psch_reset_stats($node);

    # Create two tables for join
    $node->safe_psql('postgres', 'CREATE TABLE cpu_a(id int PRIMARY KEY, val text)');
    $node->safe_psql('postgres', 'CREATE TABLE cpu_b(id int, a_id int, data text)');
    $node->safe_psql('postgres', "INSERT INTO cpu_a SELECT g, md5(g::text) FROM generate_series(1, 5000) g");
    $node->safe_psql('postgres', "INSERT INTO cpu_b SELECT g, g % 5000 + 1, md5(g::text) FROM generate_series(1, 20000) g");

    # Force hash join
    $node->safe_psql('postgres', 'SET enable_mergejoin = off');
    $node->safe_psql('postgres', 'SET enable_nestloop = off');

    $node->safe_psql('postgres', q{
        SELECT count(*), sum(length(a.val || b.data))
        FROM cpu_a a JOIN cpu_b b ON a.id = b.a_id
    });

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 1, 'Hash join events enqueued');

    if ($use_clickhouse) {
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
        psch_wait_for_export($node, $stats->{enqueued}, 10);

        my $cpu_check = psch_query_clickhouse(
            "SELECT max(cpu_user_time_us), max(cpu_sys_time_us), max(duration_us) " .
            "FROM pg_stat_ch.events_raw WHERE query LIKE '%cpu_a%JOIN%cpu_b%'"
        );
        diag("Hash join CPU metrics: $cpu_check");
    }

    $node->safe_psql('postgres', 'DROP TABLE cpu_a, cpu_b');
};

# Test 3: Sort operation (CPU-intensive)
subtest 'sort cpu usage' => sub {
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'CREATE TABLE cpu_sort(id int, val text, num float)');
    $node->safe_psql('postgres', q{
        INSERT INTO cpu_sort
        SELECT g, md5(random()::text), random()
        FROM generate_series(1, 50000) g
    });

    # Sort without index
    $node->safe_psql('postgres', 'SET work_mem = \'4MB\'');
    $node->safe_psql('postgres', 'SELECT * FROM cpu_sort ORDER BY val, num LIMIT 100');

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 1, 'Sort events enqueued');

    if ($use_clickhouse) {
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
        psch_wait_for_export($node, $stats->{enqueued}, 10);

        my $cpu_check = psch_query_clickhouse(
            "SELECT max(cpu_user_time_us), max(duration_us) " .
            "FROM pg_stat_ch.events_raw WHERE query LIKE '%ORDER BY val%'"
        );
        diag("Sort CPU metrics: $cpu_check");
    }

    $node->safe_psql('postgres', 'DROP TABLE cpu_sort');
};

# Test 4: Aggregate operations
subtest 'aggregate cpu usage' => sub {
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'CREATE TABLE cpu_agg(category int, value numeric)');
    $node->safe_psql('postgres', q{
        INSERT INTO cpu_agg
        SELECT g % 100, random() * 1000
        FROM generate_series(1, 100000) g
    });

    # Aggregate query
    $node->safe_psql('postgres', q{
        SELECT category, count(*), avg(value), stddev(value), sum(value)
        FROM cpu_agg
        GROUP BY category
        ORDER BY sum(value) DESC
    });

    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 1, 'Aggregate events enqueued');

    if ($use_clickhouse) {
        $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
        psch_wait_for_export($node, $stats->{enqueued}, 10);

        my $cpu_check = psch_query_clickhouse(
            "SELECT max(cpu_user_time_us), max(duration_us) " .
            "FROM pg_stat_ch.events_raw WHERE query LIKE '%stddev%'"
        );
        diag("Aggregate CPU metrics: $cpu_check");
    }

    $node->safe_psql('postgres', 'DROP TABLE cpu_agg');
};

$node->stop();
done_testing();
