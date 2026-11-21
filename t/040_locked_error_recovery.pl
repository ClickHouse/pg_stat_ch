#!/usr/bin/env perl
# ERROR raised under queue lock must leave backend usable. Forced error fires
# twice: once from SET's own utility enqueue (propagates, abort reverts SET)
# and once from emit_log_hook capture of that error (swallowed by hook). Both
# unwinds must release queue lock and leave InterruptHoldoffCount balanced,
# else statement_timeout never fires (release) or RESUME_INTERRUPTS asserts

use strict;
use warnings;
use lib 't';
use Test::More;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use psch;

my $node = psch_init_node('locked_error', flush_interval_ms => 60000);

# Enqueue count added by one captured ERROR, measured in given session.
# Stats read enqueues its own event, so compare against fresh backend baseline
sub error_capture_delta {
    my ($session) = @_;
    my $stats_sql = 'SELECT enqueued_events FROM pg_stat_ch_stats()';
    my $before = $session->query_safe($stats_sql);
    $session->query('SELECT 1/0');
    $session->{stderr} = '';
    my $after = $session->query_safe($stats_sql);
    return $after - $before;
}

my $fresh = $node->background_psql('postgres', on_error_stop => 0);
my $baseline = error_capture_delta($fresh);
$fresh->quit;
cmp_ok($baseline, '>=', 2, 'baseline captures division by zero error');

my $session = $node->background_psql('postgres', on_error_stop => 0);
my (undef, $failed) = $session->query('SET pg_stat_ch.debug_force_locked_error = on');
ok($failed, 'forced error raised under queue lock');
like($session->{stderr}, qr/forced error under queue lock/, 'forced error reported');
$session->{stderr} = '';

$session->query_safe("SET statement_timeout = '100ms'");
$session->query('SELECT pg_sleep(5)');
like($session->{stderr}, qr/statement timeout/, 'statement_timeout fires after recovery');
$session->{stderr} = '';
$session->query_safe('RESET statement_timeout');

is(error_capture_delta($session), $baseline, 'error capture resumes in same backend');

my ($ret) = $node->psql('postgres', 'SELECT 1', timeout => 10);
is($ret, 0, 'other backend acquires queue lock');

$session->quit;
$node->stop();
done_testing();
