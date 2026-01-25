#!/usr/bin/env perl
# GUC settings: verify all settings exist with correct types and contexts

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

my $node = psch_init_node('settings');

# Get all pg_stat_ch settings
my $result = $node->safe_psql('postgres',
    "SELECT name, vartype, context FROM pg_settings " .
    "WHERE name LIKE 'pg_stat_ch.%' ORDER BY name");

# Count settings (should be 9)
my @lines = split /\n/, $result;
cmp_ok(scalar(@lines), '>=', 9, 'At least 9 GUC settings exist');

# Verify each setting exists with correct type and context
like($result, qr/pg_stat_ch\.enabled\|bool\|sighup/,
    'enabled: bool, sighup context');
like($result, qr/pg_stat_ch\.clickhouse_host\|string\|postmaster/,
    'clickhouse_host: string, postmaster context');
like($result, qr/pg_stat_ch\.clickhouse_port\|integer\|postmaster/,
    'clickhouse_port: integer, postmaster context');
like($result, qr/pg_stat_ch\.clickhouse_user\|string\|postmaster/,
    'clickhouse_user: string, postmaster context');
like($result, qr/pg_stat_ch\.clickhouse_password\|string\|postmaster/,
    'clickhouse_password: string, postmaster context');
like($result, qr/pg_stat_ch\.clickhouse_database\|string\|postmaster/,
    'clickhouse_database: string, postmaster context');
like($result, qr/pg_stat_ch\.queue_capacity\|integer\|postmaster/,
    'queue_capacity: integer, postmaster context');
like($result, qr/pg_stat_ch\.flush_interval_ms\|integer\|sighup/,
    'flush_interval_ms: integer, sighup context');
like($result, qr/pg_stat_ch\.batch_max\|integer\|sighup/,
    'batch_max: integer, sighup context');

# Verify default values
my $enabled = $node->safe_psql('postgres', "SHOW pg_stat_ch.enabled");
is($enabled, 'on', 'enabled default is on');

my $host = $node->safe_psql('postgres', "SHOW pg_stat_ch.clickhouse_host");
is($host, 'localhost', 'clickhouse_host default is localhost');

my $port = $node->safe_psql('postgres', "SHOW pg_stat_ch.clickhouse_port");
is($port, '9000', 'clickhouse_port default is 9000');

my $database = $node->safe_psql('postgres', "SHOW pg_stat_ch.clickhouse_database");
is($database, 'pg_stat_ch', 'clickhouse_database default is pg_stat_ch');

# Test invalid queue_capacity (non power of 2)
# PostgreSQL doesn't fail to start but logs a warning and uses the default
my $node2 = PostgreSQL::Test::Cluster->new('settings_invalid');
$node2->init();
$node2->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.queue_capacity = 1000
});

# Server starts but should have logged a warning and used default
$node2->start();
$node2->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

# The actual capacity should be the default (65536), not 1000
my $actual_cap = $node2->safe_psql('postgres', "SHOW pg_stat_ch.queue_capacity");
is($actual_cap, '65536', 'Invalid queue_capacity (1000) falls back to default (65536)');

$node2->stop();

diag("All settings found:");
diag($result);

$node->stop();
done_testing();
