#!/usr/bin/env perl
# Standby export: verify bgworker runs on standbys (read replica support)
#
# pg_stat_ch uses BgWorkerStart_ConsistentState so the exporter bgworker
# starts on standbys in recovery, enabling telemetry export from read replicas.

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# --- Set up primary ---
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.queue_capacity = 65536
pg_stat_ch.flush_interval_ms = 1000
});
$primary->start();
$primary->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

# Test 1: bgworker runs on primary
my $primary_bgw = $primary->safe_psql('postgres', q{
    SELECT count(*) FROM pg_stat_activity
    WHERE backend_type = 'pg_stat_ch exporter'
});
is($primary_bgw, '1', 'bgworker runs on primary');

# --- Set up standby ---
$primary->backup('backup1');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'backup1', has_streaming => 1);
$standby->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.queue_capacity = 65536
pg_stat_ch.flush_interval_ms = 1000
});
$standby->start();

# Wait for standby to catch up
$primary->wait_for_catchup($standby, 'replay');

# Test 2: standby is actually in recovery
my $in_recovery = $standby->safe_psql('postgres', 'SELECT pg_is_in_recovery()');
is($in_recovery, 't', 'standby is in recovery mode');

# Test 3: bgworker runs on standby (the key behavioral change)
my $standby_bgw = $standby->safe_psql('postgres', q{
    SELECT count(*) FROM pg_stat_activity
    WHERE backend_type = 'pg_stat_ch exporter'
});
is($standby_bgw, '1', 'bgworker runs on standby in recovery');

# Test 4: standby can enqueue events from read queries
my $stats_before = psch_get_stats($standby);
$standby->safe_psql('postgres', 'SELECT 1');
$standby->safe_psql('postgres', 'SELECT 2');
$standby->safe_psql('postgres', 'SELECT 3');
my $stats_after = psch_get_stats($standby);

cmp_ok($stats_after->{enqueued}, '>=', $stats_before->{enqueued} + 3,
    'standby enqueues events from read queries');

# Test 5: pg_stat_ch_stats() works on standby
my $standby_stats = $standby->safe_psql('postgres', q{
    SELECT
        enqueued_events IS NOT NULL AND enqueued_events >= 0 AS enq_ok,
        queue_capacity IS NOT NULL AND queue_capacity > 0 AS cap_ok
    FROM pg_stat_ch_stats()
});
is($standby_stats, 't|t', 'pg_stat_ch_stats() returns valid data on standby');

# Test 6: primary bgworker still running after standby joined
my $primary_bgw_after = $primary->safe_psql('postgres', q{
    SELECT count(*) FROM pg_stat_activity
    WHERE backend_type = 'pg_stat_ch exporter'
});
is($primary_bgw_after, '1', 'primary bgworker still running after standby joined');

diag("Primary enqueued: " . psch_get_stats($primary)->{enqueued});
diag("Standby enqueued: $stats_after->{enqueued}");

$standby->stop();
$primary->stop();
done_testing();
