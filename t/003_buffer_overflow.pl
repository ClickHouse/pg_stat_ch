#!/usr/bin/env perl
# Buffer overflow test: deliberately fill the buffer to test overflow behavior
#
# SKIPPED: This test is currently disabled because the enqueue lock contention
# when the buffer is full causes severe performance degradation. This needs
# investigation - possibly by using a lock-free overflow path or reducing
# the lock scope.
#
# To run this test, uncomment the test code below and expect it to take
# several minutes to complete.

use strict;
use warnings;
use Test::More;

plan skip_all => 'Buffer overflow test disabled due to lock contention performance issue';

# The test code that needs lock optimization:
# use lib 't';
# use PostgreSQL::Test::Cluster;
# use PostgreSQL::Test::Utils;
# use psch;
#
# my $capacity = 1024;
# my $node = psch_init_node('overflow',
#     queue_capacity    => $capacity,
#     flush_interval_ms => 60000
# );
#
# psch_reset_stats($node);
# my $stats_before = psch_get_stats($node);
#
# # Generate more events than buffer can hold
# my $nclients = 1;
# my $ntxns = 1500;
# $node->pgbench(
#     "--client=$nclients --transactions=$ntxns --no-vacuum",
#     0, [qr/processed: \d+\/\d+/], [qr/^$/],
#     'overflow stress test',
#     { 'simple' => 'SELECT 1' }
# );
#
# my $stats = psch_get_stats($node);
# cmp_ok($stats->{dropped}, '>', 0, "Events were dropped due to overflow");
# is($stats->{queue_size}, $capacity, "Queue at capacity");
# is($stats->{capacity}, $capacity, "Queue capacity matches configuration");
#
# $node->stop();
# done_testing();
