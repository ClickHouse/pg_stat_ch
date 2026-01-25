#!/usr/bin/env perl
# Background worker lifecycle: verify bgworker is running and survives reloads

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

my $node = psch_init_node('bgworker');

# Test 1: Verify bgworker is running
my $bgworker_count = $node->safe_psql('postgres', q{
    SELECT count(*) FROM pg_stat_activity
    WHERE backend_type = 'pg_stat_ch exporter'
});
is($bgworker_count, '1', 'pg_stat_ch exporter background worker is running');

# Test 2: Verify pg_stat_ch_stats() returns valid data
my $stats_result = $node->safe_psql('postgres', q{
    SELECT
        enqueued_events IS NOT NULL AND enqueued_events >= 0 AS enq_ok,
        dropped_events IS NOT NULL AND dropped_events >= 0 AS drop_ok,
        exported_events IS NOT NULL AND exported_events >= 0 AS exp_ok,
        send_failures IS NOT NULL AND send_failures >= 0 AS fail_ok,
        queue_size IS NOT NULL AND queue_size >= 0 AS size_ok,
        queue_capacity IS NOT NULL AND queue_capacity > 0 AS cap_ok
    FROM pg_stat_ch_stats()
});
is($stats_result, 't|t|t|t|t|t', 'pg_stat_ch_stats() returns valid data');

# Test 3: Generate some queries and verify enqueued count increases
my $stats_before = psch_get_stats($node);
$node->safe_psql('postgres', 'SELECT 1');
$node->safe_psql('postgres', 'SELECT 2');
$node->safe_psql('postgres', 'SELECT 3');
my $stats_after = psch_get_stats($node);

cmp_ok($stats_after->{enqueued}, '>=', $stats_before->{enqueued} + 3,
    'Enqueued count increases after queries');

# Test 4: Verify bgworker survives pg_reload_conf()
$node->safe_psql('postgres', 'SELECT pg_reload_conf()');

# Wait a moment for reload to complete
sleep(1);

$bgworker_count = $node->safe_psql('postgres', q{
    SELECT count(*) FROM pg_stat_activity
    WHERE backend_type = 'pg_stat_ch exporter'
});
is($bgworker_count, '1', 'Background worker survives pg_reload_conf()');

# Test 5: Verify stats function still works after reload
my $stats_post_reload = psch_get_stats($node);
cmp_ok($stats_post_reload->{enqueued}, '>=', $stats_after->{enqueued},
    'Stats still accessible after reload');

# Test 6: Verify queue capacity matches GUC
my $capacity = $node->safe_psql('postgres', q{
    SELECT queue_capacity FROM pg_stat_ch_stats()
});
is($capacity, '65536', 'Queue capacity matches default GUC value');

diag("Background worker test results:");
diag("  Enqueued: $stats_post_reload->{enqueued}");
diag("  Dropped:  $stats_post_reload->{dropped}");
diag("  Exported: $stats_post_reload->{exported}");
diag("  Queue capacity: $capacity");

$node->stop();
done_testing();
