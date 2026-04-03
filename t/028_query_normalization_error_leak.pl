#!/usr/bin/env perl
# Test: failed normalized query must not leak into the next successful query
# Prerequisites: ClickHouse container must be running

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::BackgroundPsql;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping normalization leak test';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running';
}

psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

my $node = psch_init_node_with_clickhouse('normalize_error_leak',
    flush_interval_ms => 100,
    batch_max => 100
);

subtest 'failed constant-bearing query does not poison next constant-free query' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    my $session = $node->background_psql('postgres', on_error_stop => 0);
    my ($stdout, $ret) = $session->query('SELECT 1/0');
    is($ret, 1, 'Division by zero fails as expected in the same backend session');
    $session->{stderr} = '';

    ($stdout, $ret) = $session->query('SELECT current_database()');
    is($ret, 0, 'Follow-up constant-free statement succeeds in the same backend session');

    ($stdout, $ret) = $session->query('SELECT pg_stat_ch_flush()');
    is($ret, 0, 'Flush succeeds in the same backend session');
    $session->quit();

    my $captured_count = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw " .
        "WHERE query NOT LIKE '%pg_stat_ch%' " .
        "AND query NOT LIKE '%pg_extension%' " .
        "AND query != ''",
        sub { $_[0] >= 2 },
        10
    );
    cmp_ok($captured_count, '>=', 2,
        'Captured both the error event and the following successful statement');

    my $q = psch_wait_for_clickhouse_query(
        "SELECT query FROM pg_stat_ch.events_raw " .
        "WHERE err_message = '' " .
        "AND query NOT LIKE '%pg_stat_ch%' " .
        "AND query NOT LIKE '%pg_extension%' " .
        "AND query != '' " .
        "ORDER BY ts_start DESC LIMIT 1",
        sub { $_[0] ne '' },
        10
    );

    like($q, qr/SELECT current_database\(\)/,
        'Next successful statement should be captured as itself');
    unlike($q, qr/\$\d/,
        'Next successful statement should not reuse placeholders from failed query');
};

$node->stop();
done_testing();
