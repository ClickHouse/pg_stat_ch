#!/usr/bin/env perl
# Test: GUC validation edge cases
# Tests boundary values, invalid settings, and SIGHUP reloads

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Test 1: Invalid queue_capacity (not power of 2)
subtest 'invalid queue_capacity' => sub {
    my $node = PostgreSQL::Test::Cluster->new('guc_invalid_cap');
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.queue_capacity = 1000
});

    # Server should start but use default capacity
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    my $capacity = $node->safe_psql('postgres', 'SELECT queue_capacity FROM pg_stat_ch_stats()');
    is($capacity, '65536', 'Invalid capacity 1000 falls back to default 65536');

    $node->stop();
};

# Test 2: Minimum valid queue_capacity (power of 2)
subtest 'minimum queue_capacity' => sub {
    my $node = PostgreSQL::Test::Cluster->new('guc_min_cap');
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.queue_capacity = 1024
});
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    my $capacity = $node->safe_psql('postgres', 'SELECT queue_capacity FROM pg_stat_ch_stats()');
    is($capacity, '1024', 'Minimum valid capacity 1024 accepted');

    # Verify the queue works at minimum capacity
    psch_reset_stats($node);
    $node->safe_psql('postgres', 'SELECT 1');
    my $stats = psch_get_stats($node);
    cmp_ok($stats->{enqueued}, '>=', 1, 'Queue works at minimum capacity');

    $node->stop();
};

# Test 3: Large queue_capacity
subtest 'large queue_capacity' => sub {
    my $node = PostgreSQL::Test::Cluster->new('guc_large_cap');
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.queue_capacity = 262144
});
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    my $capacity = $node->safe_psql('postgres', 'SELECT queue_capacity FROM pg_stat_ch_stats()');
    is($capacity, '262144', 'Large capacity 262144 accepted');

    $node->stop();
};

# Test 4: Boundary values for flush_interval_ms
subtest 'flush_interval_ms boundaries' => sub {
    # Minimum value
    my $node_min = PostgreSQL::Test::Cluster->new('guc_flush_min');
    $node_min->init();
    $node_min->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.flush_interval_ms = 100
});
    $node_min->start();
    $node_min->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    my $interval_min = $node_min->safe_psql('postgres', 'SHOW pg_stat_ch.flush_interval_ms');
    # PostgreSQL 18+ may show '100ms' instead of '100'
    like($interval_min, qr/^100(ms)?$/, 'Minimum flush_interval_ms 100 accepted');
    $node_min->stop();

    # Maximum reasonable value
    my $node_max = PostgreSQL::Test::Cluster->new('guc_flush_max');
    $node_max->init();
    $node_max->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.flush_interval_ms = 60000
});
    $node_max->start();
    $node_max->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    my $interval_max = $node_max->safe_psql('postgres', 'SHOW pg_stat_ch.flush_interval_ms');
    # PostgreSQL 18+ may show '1min' instead of '60000'
    like($interval_max, qr/^(60000|1min)$/, 'Large flush_interval_ms 60000 accepted');
    $node_max->stop();
};

# Test 5: Boundary values for batch_max
subtest 'batch_max boundaries' => sub {
    # Minimum value
    my $node_min = PostgreSQL::Test::Cluster->new('guc_batch_min');
    $node_min->init();
    $node_min->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.batch_max = 1
});
    $node_min->start();
    $node_min->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    my $batch_min = $node_min->safe_psql('postgres', 'SHOW pg_stat_ch.batch_max');
    is($batch_min, '1', 'Minimum batch_max 1 accepted');

    # Verify it still works
    psch_reset_stats($node_min);
    $node_min->safe_psql('postgres', 'SELECT 1');
    my $stats = psch_get_stats($node_min);
    cmp_ok($stats->{enqueued}, '>=', 1, 'Queue works with batch_max=1');
    $node_min->stop();

    # Large value
    my $node_max = PostgreSQL::Test::Cluster->new('guc_batch_max');
    $node_max->init();
    $node_max->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.batch_max = 100000
});
    $node_max->start();
    $node_max->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    my $batch_max = $node_max->safe_psql('postgres', 'SHOW pg_stat_ch.batch_max');
    is($batch_max, '100000', 'Large batch_max 100000 accepted');
    $node_max->stop();
};

# Test 6: SIGHUP reload of enabled setting
subtest 'sighup reload enabled' => sub {
    my $node = psch_init_node('guc_reload');

    # Verify initially enabled
    my $enabled = $node->safe_psql('postgres', 'SHOW pg_stat_ch.enabled');
    is($enabled, 'on', 'Initially enabled');

    # Disable via SIGHUP
    $node->append_conf('postgresql.conf', 'pg_stat_ch.enabled = off');
    $node->safe_psql('postgres', 'SELECT pg_reload_conf()');
    sleep(1);

    $enabled = $node->safe_psql('postgres', 'SHOW pg_stat_ch.enabled');
    is($enabled, 'off', 'Disabled after reload');

    # Re-enable
    $node->append_conf('postgresql.conf', 'pg_stat_ch.enabled = on');
    $node->safe_psql('postgres', 'SELECT pg_reload_conf()');
    sleep(1);

    $enabled = $node->safe_psql('postgres', 'SHOW pg_stat_ch.enabled');
    is($enabled, 'on', 'Re-enabled after reload');

    $node->stop();
};

# Test 7: SIGHUP reload of flush_interval_ms
subtest 'sighup reload flush_interval_ms' => sub {
    my $node = PostgreSQL::Test::Cluster->new('guc_reload_flush');
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.flush_interval_ms = 1000
});
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    my $interval = $node->safe_psql('postgres', 'SHOW pg_stat_ch.flush_interval_ms');
    # PostgreSQL 18+ may show '1s' instead of '1000'
    like($interval, qr/^(1000|1s)$/, 'Initial flush_interval_ms is 1000');

    # Change via SIGHUP
    $node->append_conf('postgresql.conf', 'pg_stat_ch.flush_interval_ms = 500');
    $node->safe_psql('postgres', 'SELECT pg_reload_conf()');
    sleep(1);

    $interval = $node->safe_psql('postgres', 'SHOW pg_stat_ch.flush_interval_ms');
    # PostgreSQL 18+ may show '500ms' instead of '500'
    like($interval, qr/^500(ms)?$/, 'flush_interval_ms changed to 500 after reload');

    $node->stop();
};

# Test 8: SIGHUP reload of batch_max
subtest 'sighup reload batch_max' => sub {
    my $node = PostgreSQL::Test::Cluster->new('guc_reload_batch');
    $node->init();
    $node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.batch_max = 1000
});
    $node->start();
    $node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    my $batch = $node->safe_psql('postgres', 'SHOW pg_stat_ch.batch_max');
    is($batch, '1000', 'Initial batch_max is 1000');

    # Change via SIGHUP
    $node->append_conf('postgresql.conf', 'pg_stat_ch.batch_max = 500');
    $node->safe_psql('postgres', 'SELECT pg_reload_conf()');
    sleep(1);

    $batch = $node->safe_psql('postgres', 'SHOW pg_stat_ch.batch_max');
    is($batch, '500', 'batch_max changed to 500 after reload');

    $node->stop();
};

# Test 9: Port boundary values
subtest 'port boundaries' => sub {
    # Minimum port
    my $node_min = PostgreSQL::Test::Cluster->new('guc_port_min');
    $node_min->init();
    $node_min->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.clickhouse_port = 1
});
    $node_min->start();
    $node_min->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    my $port = $node_min->safe_psql('postgres', 'SHOW pg_stat_ch.clickhouse_port');
    is($port, '1', 'Minimum port 1 accepted');
    $node_min->stop();

    # Maximum port
    my $node_max = PostgreSQL::Test::Cluster->new('guc_port_max');
    $node_max->init();
    $node_max->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.clickhouse_port = 65535
});
    $node_max->start();
    $node_max->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

    $port = $node_max->safe_psql('postgres', 'SHOW pg_stat_ch.clickhouse_port');
    is($port, '65535', 'Maximum port 65535 accepted');
    $node_max->stop();
};

done_testing();
