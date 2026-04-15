#!/usr/bin/env perl

use strict;
use warnings;
use lib 't';

use File::Glob qw(bsd_glob);
use File::Temp qw(tempdir);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping Arrow export test';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all =>
        'ClickHouse container not running. Start with: docker compose -f docker/docker-compose.test.yml up -d';
}

sub ensure_query_logs_table {
    is(
        psch_query_clickhouse('CREATE DATABASE IF NOT EXISTS pg_stat_ch'),
        '',
        'ClickHouse database exists'
    );
    is(
        system(
            'curl',
            '-sf',
            'http://localhost:18123/',
            '--data-binary',
            '@docker/init/01-query-logs.sql'
        ),
        0,
        'ClickHouse query_logs schema installed'
    );
    is(
        psch_query_clickhouse('EXISTS TABLE pg_stat_ch.query_logs'),
        '1',
        'ClickHouse query_logs table exists'
    );
}

psch_query_clickhouse('TRUNCATE TABLE IF EXISTS pg_stat_ch.query_logs');

my $dump_dir = tempdir(CLEANUP => 1);
my $node = psch_init_node_with_otel(
    'arrow_export',
    flush_interval_ms      => 100,
    batch_max              => 100,
    otel_endpoint          => 'localhost:65535',
    hostname               => 'arrow-test-host',
    otel_arrow_passthrough => 'on',
    otel_max_block_bytes   => 65536,
    extra_attributes       =>
        'instance_ubid:inst-123;server_ubid:server-456;server_role:primary;region:us-east-1;cell:cell-01;host_id:host-01;pod_name:pod-01',
    debug_arrow_dump_dir => $dump_dir,
);

sub wait_for_arrow_dump {
    my ($timeout_secs) = @_;
    my $deadline = time() + $timeout_secs;
    while (time() < $deadline) {
        my @files = sort bsd_glob("$dump_dir/*.ipc");
        return $files[-1] if @files && -s $files[-1];
        select undef, undef, undef, 0.1;
    }
    return undef;
}

subtest 'arrow ipc round trip into clickhouse' => sub {
    psch_reset_stats($node);
    ensure_query_logs_table();
    psch_query_clickhouse('TRUNCATE TABLE pg_stat_ch.query_logs');

    $node->safe_psql('postgres', q{
        SELECT 4242 AS arrow_passthrough_test
    });
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $ipc_file = wait_for_arrow_dump(10);
    ok(defined $ipc_file, 'Arrow IPC dump file was written');
    ok(defined $ipc_file && -s $ipc_file, 'Arrow IPC dump file is non-empty');

    if (defined $ipc_file) {
        my $insert_url =
            'http://localhost:18123/?query=INSERT%20INTO%20pg_stat_ch.query_logs%20FORMAT%20ArrowStream';
        is(system('curl', '-sf', $insert_url, '--data-binary', '@' . $ipc_file), 0,
            'ClickHouse accepted ArrowStream payload');
    }

    my $row_count_raw = psch_wait_for_clickhouse_query(
        'SELECT count() FROM pg_stat_ch.query_logs',
        sub { $_[0] =~ /^\d+$/ && $_[0] >= 1 },
        10
    );
    my $row_count = ($row_count_raw =~ /^\d+$/) ? 0 + $row_count_raw : 0;
    cmp_ok($row_count, '>=', 1, 'Arrow rows ingested into query_logs');

    SKIP: {
        skip 'Arrow row not available in ClickHouse', 8 if $row_count < 1;

        is(psch_query_clickhouse(q{
            SELECT db_name FROM pg_stat_ch.query_logs
            WHERE position(query_text, 'arrow_passthrough_test') > 0
            LIMIT 1
        }), 'postgres', 'db_name round-tripped');

        is(psch_query_clickhouse(q{
            SELECT db_operation FROM pg_stat_ch.query_logs
            WHERE position(query_text, 'arrow_passthrough_test') > 0
            LIMIT 1
        }), 'SELECT', 'db_operation round-tripped');

        like(psch_query_clickhouse(q{
            SELECT query_text FROM pg_stat_ch.query_logs
            WHERE position(query_text, 'arrow_passthrough_test') > 0
            LIMIT 1
        }), qr/\barrow_passthrough_test\b/, 'query_text round-tripped');

        is(psch_query_clickhouse(q{
            SELECT instance_ubid FROM pg_stat_ch.query_logs
            WHERE position(query_text, 'arrow_passthrough_test') > 0
            LIMIT 1
        }), 'inst-123', 'instance_ubid populated from extra_attributes');

        is(psch_query_clickhouse(q{
            SELECT server_role FROM pg_stat_ch.query_logs
            WHERE position(query_text, 'arrow_passthrough_test') > 0
            LIMIT 1
        }), 'primary', 'server_role populated from extra_attributes');

        is(psch_query_clickhouse(q{
            SELECT region FROM pg_stat_ch.query_logs
            WHERE position(query_text, 'arrow_passthrough_test') > 0
            LIMIT 1
        }), 'us-east-1', 'region populated from extra_attributes');

        is(psch_query_clickhouse(q{
            SELECT host_id FROM pg_stat_ch.query_logs
            WHERE position(query_text, 'arrow_passthrough_test') > 0
            LIMIT 1
        }), 'host-01', 'host_id populated from extra_attributes');

        cmp_ok(psch_query_clickhouse(q{
            SELECT count() FROM pg_stat_ch.query_logs
            WHERE position(query_text, 'arrow_passthrough_test') > 0
              AND service_version != ''
        }), '>=', 1, 'service_version populated');

        cmp_ok(psch_query_clickhouse(q{
            SELECT count() FROM pg_stat_ch.query_logs
            WHERE position(query_text, 'arrow_passthrough_test') > 0
              AND duration_us >= 0
        }), '>=', 1, 'duration_us present');
    }
};

$node->stop();
done_testing();
