#!/usr/bin/env perl
# Test: Normalize cache — LRU eviction and query-length clamping
# Prerequisites: ClickHouse container must be running

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
    plan skip_all => 'Docker not available, skipping normalize cache tests';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running';
}

psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

# ---------------------------------------------------------------------------
# Test 1: LRU eviction — cache bounded to 4 entries, run 8 distinct queries
#
# With normalize_cache_max = 4, entries are evicted after the 4th unique
# queryId. Every query should still export normalized text because
# Remember → Lookup happens within the same query lifecycle.
# ---------------------------------------------------------------------------
subtest 'LRU eviction with small cache still normalizes all queries' => sub {
    # Set up a node with a tiny normalize cache.
    my $node = PostgreSQL::Test::Cluster->new('eviction');
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.queue_capacity = 65536
pg_stat_ch.flush_interval_ms = 100
pg_stat_ch.batch_max = 100
pg_stat_ch.clickhouse_host = 'localhost'
pg_stat_ch.clickhouse_port = 19000
pg_stat_ch.clickhouse_database = 'pg_stat_ch'
pg_stat_ch.normalize_cache_max = 64
});
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Each query targets a different system catalog table → different queryId.
    # The constant in each query must be normalized to $1.
    my @queries = (
        "SELECT * FROM pg_class WHERE oid = 100",
        "SELECT * FROM pg_namespace WHERE oid = 200",
        "SELECT * FROM pg_type WHERE oid = 300",
        "SELECT * FROM pg_attribute WHERE attrelid = 400",
        "SELECT * FROM pg_index WHERE indexrelid = 500",
        "SELECT * FROM pg_proc WHERE oid = 600",
        "SELECT * FROM pg_operator WHERE oid = 700",
        "SELECT * FROM pg_am WHERE oid = 800",
    );

    my $session = $node->background_psql('postgres', on_error_stop => 1);
    my ($stdout, $ret) = $session->query('SELECT pg_backend_pid()');
    is($ret, 0, 'Can determine backend pid');
    my ($pid) = $stdout =~ /(\d+)/;
    ok(defined $pid, 'Captured backend pid');

    for my $sql (@queries) {
        ($stdout, $ret) = $session->query($sql);
        is($ret, 0, "Query succeeded: $sql");
    }

    ($stdout, $ret) = $session->query('SELECT pg_stat_ch_flush()');
    is($ret, 0, 'Flush succeeds');
    $session->quit();

    psch_wait_for_export($node, scalar(@queries), 10);

    # Collect all exported query texts from this backend.
    my $all_queries = psch_wait_for_clickhouse_query(
        "SELECT groupArray(query) FROM pg_stat_ch.events_raw " .
        "WHERE pid = $pid " .
        "AND query NOT LIKE '%pg_stat_ch%' " .
        "AND query NOT LIKE '%pg_extension%' " .
        "AND query != ''",
        sub { $_[0] ne '' },
        10
    );

    # None of the raw constant values should appear — all normalized to $N.
    for my $val (100, 200, 300, 400, 500, 600, 700, 800) {
        unlike($all_queries, qr/\b$val\b/,
            "Raw constant $val not present (normalized to \$N)");
    }

    # All queries should contain a $1 placeholder.
    like($all_queries, qr/\$1/, 'Placeholder $1 present in exported queries');

    # Verify we captured all 8 queries.
    my $count = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw " .
        "WHERE pid = $pid " .
        "AND query NOT LIKE '%pg_stat_ch%' " .
        "AND query NOT LIKE '%pg_extension%' " .
        "AND query != ''",
        sub { $_[0] >= scalar(@queries) },
        10
    );
    cmp_ok($count, '>=', scalar(@queries),
        "All " . scalar(@queries) . " queries were captured");

    $node->stop();
};

# ---------------------------------------------------------------------------
# Test 2: Query-length clamping — queries > 2048 bytes get truncated
#
# The cache clamps stored text to PSCH_MAX_CACHED_QUERY_LEN - 1 (2047)
# bytes. Generate a normalized query > 2048 bytes and verify the exported
# text is truncated.
# ---------------------------------------------------------------------------
subtest 'long normalized query is clamped to max export length' => sub {
    my $node = psch_init_node_with_clickhouse('clamp',
        flush_interval_ms => 100,
        batch_max => 100
    );

    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Build a query whose normalized form exceeds 2048 bytes.
    # Use many WHERE conditions; each "relname != $N" is ~16 chars.
    my @conditions = map { "relname != 'table_$_'" } (1..200);
    my $long_sql = "SELECT * FROM pg_class WHERE " . join(" AND ", @conditions);

    # Sanity: the original SQL is well over 2048 bytes.
    ok(length($long_sql) > 2048,
        "Generated SQL is " . length($long_sql) . " bytes (> 2048)");

    my $session = $node->background_psql('postgres', on_error_stop => 1);
    my ($stdout, $ret) = $session->query('SELECT pg_backend_pid()');
    is($ret, 0, 'Can determine backend pid');
    my ($pid) = $stdout =~ /(\d+)/;
    ok(defined $pid, 'Captured backend pid');

    ($stdout, $ret) = $session->query($long_sql);
    is($ret, 0, 'Long query executes successfully');

    ($stdout, $ret) = $session->query('SELECT pg_stat_ch_flush()');
    is($ret, 0, 'Flush succeeds');
    $session->quit();

    psch_wait_for_export($node, 1, 10);

    my $exported_query = psch_wait_for_clickhouse_query(
        "SELECT query FROM pg_stat_ch.events_raw " .
        "WHERE pid = $pid " .
        "AND query LIKE 'SELECT%pg_class%' " .
        "ORDER BY ts_start DESC LIMIT 1",
        sub { $_[0] ne '' },
        10
    );

    ok(length($exported_query) > 0, 'Exported query is non-empty');
    cmp_ok(length($exported_query), '<=', 2047,
        "Exported query length (" . length($exported_query) . ") <= 2047");
    like($exported_query, qr/^SELECT \* FROM pg_class WHERE relname != \$1/,
        'Exported query starts with expected prefix');

    # The last placeholder ($200) should NOT appear — it's past the truncation point.
    unlike($exported_query, qr/\$200/,
        'Last placeholder $200 not present (query was truncated)');

    $node->stop();
};

# ---------------------------------------------------------------------------
# Test 3: Forced eviction — EXECUTE after cache overflow gets empty query text
#
# PREPARE caches the inner SELECT's normalized text via post_parse_analyze.
# EXECUTE reuses the cached plan and skips post_parse_analyze, so it relies
# entirely on the cache for query text.  If we overflow the cache between
# two EXECUTEs, the second one should produce an event with empty query text,
# proving the entry was actually evicted.
# ---------------------------------------------------------------------------
subtest 'evicted entry produces empty query text on EXECUTE' => sub {
    my $node = PostgreSQL::Test::Cluster->new('force_evict');
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.queue_capacity = 65536
pg_stat_ch.flush_interval_ms = 100
pg_stat_ch.batch_max = 100
pg_stat_ch.clickhouse_host = 'localhost'
pg_stat_ch.clickhouse_port = 19000
pg_stat_ch.clickhouse_database = 'pg_stat_ch'
pg_stat_ch.normalize_cache_max = 64
});
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    # Create 70 filler tables — each will produce a distinct queryId when
    # queried, enough to overflow a 64-entry cache.
    for my $i (1..70) {
        $node->safe_psql('postgres', "CREATE TABLE filler_$i (id int)");
    }

    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    my $session = $node->background_psql('postgres', on_error_stop => 1);
    my ($stdout, $ret) = $session->query('SELECT pg_backend_pid()');
    is($ret, 0, 'Can determine backend pid');
    my ($pid) = $stdout =~ /(\d+)/;
    ok(defined $pid, 'Captured backend pid');

    # Step 1: PREPARE and EXECUTE once — normalized text should be present.
    ($stdout, $ret) = $session->query(
        "PREPARE evict_test AS SELECT * FROM pg_class WHERE oid = 42");
    is($ret, 0, 'PREPARE succeeds');

    ($stdout, $ret) = $session->query("EXECUTE evict_test");
    is($ret, 0, 'First EXECUTE succeeds');

    ($stdout, $ret) = $session->query('SELECT pg_stat_ch_flush()');
    is($ret, 0, 'First flush succeeds');

    psch_wait_for_export($node, 1, 10);

    my $first = psch_wait_for_clickhouse_query(
        "SELECT query FROM pg_stat_ch.events_raw " .
        "WHERE pid = $pid " .
        "AND query LIKE '%pg_class%' " .
        "AND query LIKE '%\$1%' " .
        "LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($first, qr/\$1/,
        'First EXECUTE exports normalized text with $1 placeholder');

    # Step 2: Overflow the cache with 70 distinct SPI queries via DO block.
    # Each PERFORM targets a different table → different queryId.
    my $do_sql = "DO \$\$ BEGIN ";
    for my $i (1..70) {
        $do_sql .= "PERFORM * FROM filler_$i WHERE id = $i; ";
    }
    $do_sql .= "END; \$\$";
    ($stdout, $ret) = $session->query($do_sql);
    is($ret, 0, 'DO block with 70 SPI queries succeeds');

    # Step 3: EXECUTE again after eviction — the inner SELECT's cache entry
    # is gone, and EXECUTE skips post_parse_analyze (reuses cached plan),
    # so the exported query text should be empty.
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    ($stdout, $ret) = $session->query("EXECUTE evict_test");
    is($ret, 0, 'Second EXECUTE succeeds');

    ($stdout, $ret) = $session->query('SELECT pg_stat_ch_flush()');
    is($ret, 0, 'Second flush succeeds');
    $session->quit();

    psch_wait_for_export($node, 1, 10);

    # Wait for events to arrive, then check for an empty-query event.
    psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw WHERE pid = $pid",
        sub { $_[0] >= 1 },
        10
    );

    my $empty_count = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.events_raw " .
        "WHERE pid = $pid AND query = ''"
    );
    cmp_ok($empty_count, '>=', 1,
        'Evicted cache entry produces empty query text on re-EXECUTE');

    $node->stop();
};

done_testing();
