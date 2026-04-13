#!/usr/bin/env perl
# Test: Query labels via sqlcommenter comments → ClickHouse JSON column
# Prerequisites: ClickHouse container must be running
#   docker compose -f docker/docker-compose.test.yml up -d

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Skip if Docker/ClickHouse not available
if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping ClickHouse labels tests';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all =>
        'ClickHouse container not running. Start with: docker compose -f docker/docker-compose.test.yml up -d';
}

psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

my $node = psch_init_node_with_clickhouse('ch_labels',
    flush_interval_ms => 100,
    batch_max         => 100,
);

# Test 1: Query with sqlcommenter labels appears with JSON labels in CH
subtest 'basic labels export' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres',
        "SELECT 1 /* controller='users',action='show' */");

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $labels = psch_wait_for_clickhouse_query(
        "SELECT labels FROM pg_stat_ch.events_raw "
        . "WHERE query LIKE '%controller%' LIMIT 1",
        sub { $_[0] ne '' && $_[0] ne '{}' },
        10
    );

    like($labels, qr/controller/, 'labels JSON contains controller key');
    like($labels, qr/users/,      'labels JSON contains users value');
    like($labels, qr/action/,     'labels JSON contains action key');
    like($labels, qr/show/,       'labels JSON contains show value');
};

# Test 2: Query without comment gets empty labels
subtest 'no comment produces empty labels' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'CREATE TABLE nocomment_test(id int)');
    $node->safe_psql('postgres', 'DROP TABLE nocomment_test');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $labels = psch_wait_for_clickhouse_query(
        "SELECT labels FROM pg_stat_ch.events_raw "
        . "WHERE query LIKE '%nocomment_test%' LIMIT 1",
        sub { defined $_[0] && $_[0] ne '' },
        10
    );

    like($labels, qr/^\{\}$/, 'no-comment query has empty {} labels');
};

# Test 3: Multiple label keys
subtest 'multiple labels' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres',
        "SELECT 1 /* controller='orders',action='index',framework='rails',db_driver='pg' */");

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Query individual label values via JSONExtractString
    my $controller = psch_wait_for_clickhouse_query(
        "SELECT JSONExtractString(labels, 'controller') FROM pg_stat_ch.events_raw "
        . "WHERE query LIKE '%orders%' LIMIT 1",
        sub { $_[0] =~ /orders/ },
        10
    );
    like($controller, qr/orders/, 'labels.controller = orders');

    my $framework = psch_query_clickhouse(
        "SELECT JSONExtractString(labels, 'framework') FROM pg_stat_ch.events_raw "
        . "WHERE query LIKE '%orders%' LIMIT 1");
    like($framework, qr/rails/, 'labels.framework = rails');
};

# Test 4: track_labels = off produces empty labels
subtest 'track_labels off' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres',
        "ALTER SYSTEM SET pg_stat_ch.track_labels = off");
    $node->reload();
    sleep(1);

    $node->safe_psql('postgres',
        "SELECT 1 /* controller='ignored' */");

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $labels = psch_wait_for_clickhouse_query(
        "SELECT labels FROM pg_stat_ch.events_raw "
        . "WHERE query LIKE '%ignored%' LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
    like($labels, qr/^\{\}$/, 'track_labels=off produces empty labels');

    # Re-enable
    $node->safe_psql('postgres',
        "ALTER SYSTEM SET pg_stat_ch.track_labels = on");
    $node->reload();
    sleep(1);
};

# Test 5: URL-encoded values are decoded
subtest 'url encoded values' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres',
        "SELECT 1 /* route='/api/users%3Fid%3D1' */");

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $labels = psch_wait_for_clickhouse_query(
        "SELECT labels FROM pg_stat_ch.events_raw "
        . "WHERE query LIKE '%route%' LIMIT 1",
        sub { $_[0] ne '' && $_[0] ne '{}' },
        10
    );

    like($labels, qr{/api/users\?id=1}, 'URL-encoded value decoded correctly');
};

# Test 6: Only last comment is extracted
subtest 'multiple comments takes last' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres',
        "SELECT /* first='ignored' */ 1 /* second='captured' */");

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $labels = psch_wait_for_clickhouse_query(
        "SELECT labels FROM pg_stat_ch.events_raw "
        . "WHERE query LIKE '%second%' LIMIT 1",
        sub { $_[0] ne '' && $_[0] ne '{}' },
        10
    );

    like($labels, qr/captured/, 'last comment labels exported');
    unlike($labels, qr/ignored/, 'first comment labels not exported');
};

$node->stop();
done_testing();
