#!/usr/bin/env perl
# Basic extension lifecycle: create, verify, drop

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('lifecycle');
$node->init();
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'pg_stat_ch'");
$node->start();

# Test CREATE EXTENSION
$node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');
pass('CREATE EXTENSION succeeds');

# Verify extension exists with correct version
my $result = $node->safe_psql('postgres',
    "SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_stat_ch'");
like($result, qr/pg_stat_ch\|/, 'Extension registered in pg_extension');

# Test version function returns a version string starting with semver
$result = $node->safe_psql('postgres', 'SELECT pg_stat_ch_version()');
like($result, qr/^v?\d+\.\d+\.\d+/, 'pg_stat_ch_version() returns version string');

# Verify all expected functions exist
$result = $node->safe_psql('postgres',
    "SELECT proname FROM pg_proc WHERE proname LIKE 'pg_stat_ch%' ORDER BY proname");
like($result, qr/pg_stat_ch_reset/, 'pg_stat_ch_reset function exists');
like($result, qr/pg_stat_ch_stats/, 'pg_stat_ch_stats function exists');
like($result, qr/pg_stat_ch_version/, 'pg_stat_ch_version function exists');

# Test DROP EXTENSION
$node->safe_psql('postgres', 'DROP EXTENSION pg_stat_ch');
pass('DROP EXTENSION succeeds');

# Verify extension is fully removed
$result = $node->safe_psql('postgres',
    "SELECT count(*) FROM pg_extension WHERE extname = 'pg_stat_ch'");
is($result, '0', 'Extension fully removed from pg_extension');

$node->stop();
done_testing();
