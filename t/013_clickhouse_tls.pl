#!/usr/bin/env perl
# Test: ClickHouse export over TLS
# Prerequisites: TLS ClickHouse container must be running:
#   ./docker/tls/generate-certs.sh
#   docker compose -f docker/docker-compose.test.yml -f docker/docker-compose.tls.yml up -d --wait
# Exercises TlsConnect (SNI, handshake), the OpenSSL I/O path, and a full
# INSERT over an encrypted native connection with skip_tls_verify (self-signed).

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

use psch;

my $tls_port = 29440;

sub ch_tls_ready {
    return system("docker exec psch-clickhouse clickhouse-client --secure --host localhost --port 9440 --accept-invalid-certificate -q 'SELECT 1' >/dev/null 2>&1") == 0;
}

if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping ClickHouse TLS tests';
}

# Confirm the TLS container is up and serving
if (!ch_tls_ready()) {
    plan skip_all => "TLS ClickHouse container not running. Start with: "
        . "./docker/tls/generate-certs.sh && "
        . "docker compose -f docker/docker-compose.test.yml -f docker/docker-compose.tls.yml up -d --wait";
}

psch_query_clickhouse('TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw');

my $node = psch_init_node_with_clickhouse('ch_tls',
    flush_interval_ms => 100,
    batch_max => 100,
    clickhouse_port => $tls_port,
    use_tls => 1,
    skip_tls_verify => 1,
);

subtest 'export over TLS' => sub {
    psch_reset_stats($node);

    my $num_queries = 10;
    $node->safe_psql('postgres', "SELECT $_") for (1 .. $num_queries);
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $exported = psch_wait_for_export($node, $num_queries, 15);
    cmp_ok($exported, '>=', $num_queries, "events exported over TLS (got $exported)");

    my $stats = psch_get_stats($node);
    is($stats->{send_failures}, 0, 'no send failures over TLS');

    my $ch_count = psch_wait_for_clickhouse_query(
        'SELECT count() FROM pg_stat_ch.events_raw', sub { $_[0] >= $num_queries }, 15);
    cmp_ok($ch_count, '>=', $num_queries,
        "events visible in TLS ClickHouse (got $ch_count)");

    my $query_check = psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw WHERE query != ''", sub { $_[0] >= 1 }, 10);
    cmp_ok($query_check, '>=', 1, 'query text captured over TLS');
};

# Reconnect path: handshake must succeed again after the connection drops
subtest 'reconnect over TLS' => sub {
    psch_query_clickhouse('TRUNCATE TABLE pg_stat_ch.events_raw');
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'SELECT 100');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    my $first = psch_wait_for_clickhouse_query(
        'SELECT count() FROM pg_stat_ch.events_raw', sub { $_[0] >= 1 }, 15);
    cmp_ok($first, '>=', 1, 'first batch exported over TLS');

    # Bounce the server so the next export forces a fresh TLS handshake
    system("docker restart psch-clickhouse >/dev/null 2>&1");
    for my $i (1 .. 30) {
        last if ch_tls_ready();
        sleep(1);
    }
    psch_query_clickhouse('TRUNCATE TABLE pg_stat_ch.events_raw');

    $node->safe_psql('postgres', 'SELECT 200');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
    my $after = psch_wait_for_clickhouse_query(
        'SELECT count() FROM pg_stat_ch.events_raw', sub { $_[0] >= 1 }, 30);
    cmp_ok($after, '>=', 1, "export resumed over TLS after reconnect (got $after)");
};

$node->stop();
done_testing();
