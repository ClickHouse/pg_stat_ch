#!/usr/bin/env perl
# Test: nested executions of the same normalized SPI query keep distinct state

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::BackgroundPsql;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping nested normalization test';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running';
}

psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

my $node = psch_init_node_with_clickhouse('nested_normalize',
    flush_interval_ms => 100,
    batch_max => 100
);

$node->safe_psql('postgres', q{
    CREATE OR REPLACE FUNCTION nested_normalize_same_sql(depth int)
    RETURNS int
    LANGUAGE plpgsql
    AS $$
    DECLARE
        result int;
    BEGIN
        IF depth <= 0 THEN
            RETURN 0;
        END IF;

        SELECT nested_normalize_same_sql(depth - 1) + 42
          INTO result
         WHERE 7 = 7;

        RETURN result;
    END;
    $$;
});

subtest 'recursive SPI executions of the same statement stay normalized' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    my $session = $node->background_psql('postgres', on_error_stop => 1);
    my ($stdout, $ret) = $session->query('SELECT pg_backend_pid()');
    is($ret, 0, 'Can determine backend pid');
    my ($pid) = $stdout =~ /(\d+)/;
    ok(defined $pid, 'Captured backend pid');

    ($stdout, $ret) = $session->query('SELECT nested_normalize_same_sql(3)');
    is($ret, 0, 'Recursive function call succeeds');

    ($stdout, $ret) = $session->query('SELECT pg_stat_ch_flush()');
    is($ret, 0, 'Flush succeeds');
    $session->quit();

    my $nested_count = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw " .
        "WHERE pid = $pid " .
        "AND query LIKE '%WHERE%' " .
        "AND query LIKE '%nested_normalize_same_sql%'",
        sub { $_[0] >= 3 },
        10
    );
    cmp_ok($nested_count, '>=', 3,
        'Captured recursive nested executions of the same SPI statement');

    my $queries = psch_wait_for_clickhouse_query(
        "SELECT groupArray(query) FROM pg_stat_ch.events_raw " .
        "WHERE pid = $pid " .
        "AND query LIKE '%WHERE%' " .
        "AND query LIKE '%nested_normalize_same_sql%'",
        sub { $_[0] ne '' },
        10
    );

    like($queries, qr/\$\d/,
        'Nested SPI queries retain normalized placeholders');
    unlike($queries, qr/\b42\b/,
        'Nested SPI queries do not fall back to raw constant 42');
    unlike($queries, qr/\b7\s*=\s*7\b/,
        'Nested SPI queries do not fall back to raw WHERE constant expression');
};

$node->stop();
done_testing();
