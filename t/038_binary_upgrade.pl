#!/usr/bin/env perl
# Verify pg_upgrade succeeds with pg_stat_ch in shared_preload_libraries.
#
# Regression test for: pg_stat_ch's background worker connects to the
# "postgres" database unconditionally in PschBgworkerMain(), including when
# the postmaster is started by pg_upgrade in -b (binary upgrade) mode. That
# open connection blocks pg_restore's `DROP DATABASE "postgres"` step with
# "database is being accessed by other users", failing the upgrade outright.
#
# This does a same-version self-upgrade (old bindir == new bindir), which is
# sufficient to exercise the -b startup path pg_upgrade uses internally —
# it does not require two installed PG majors.

use strict;
use warnings;
use lib 't';

use Cwd qw(abs_path);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $old = PostgreSQL::Test::Cluster->new('old');
$old->init();
$old->append_conf('postgresql.conf', "shared_preload_libraries = 'pg_stat_ch'\n");
$old->start();
$old->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');
$old->stop();

my $new = PostgreSQL::Test::Cluster->new('new');
$new->init();
$new->append_conf('postgresql.conf', "shared_preload_libraries = 'pg_stat_ch'\n");

# Resolve these to absolute paths before chdir-ing below -- $old/$new's
# data_dir accessors return the relative tmp_check/... path they were
# constructed with, which stops resolving once cwd moves.
my $old_datadir = abs_path($old->data_dir);
my $new_datadir = abs_path($new->data_dir);
my $old_bindir  = $old->config_data('--bindir');
my $new_bindir  = $new->config_data('--bindir');

my $orig_cwd = abs_path('.');
chdir($new_datadir);

command_ok(
    [
        'pg_upgrade',
        '--old-datadir', $old_datadir,
        '--new-datadir', $new_datadir,
        '--old-bindir',  $old_bindir,
        '--new-bindir',  $new_bindir,
    ],
    'pg_upgrade succeeds with pg_stat_ch in shared_preload_libraries'
);

# $new->start() below uses the relative data_dir it was constructed with,
# which only resolves from the original cwd.
chdir($orig_cwd);

$new->start();
my $version = $new->safe_psql('postgres', 'SELECT pg_stat_ch_version()');
ok(length($version) > 0, 'pg_stat_ch functions after pg_upgrade');
$new->stop();

done_testing();
