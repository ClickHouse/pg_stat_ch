#!/usr/bin/env perl
# PSCH-01: an ERROR raised inside emit_log_hook capture must not return into
# the report being emitted. Run 4's trigger: with queue full, reset re-arms
# overflow WARNING, a pending self-cancel fires at its CHECK_FOR_INTERRUPTS
# while advisory unlock WARNING is being captured

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

my $node = psch_init_node('nested_error',
    queue_capacity    => 1024,
    flush_interval_ms => 60000,  # Keep queue full
);
$node->safe_psql('postgres', 'CREATE ROLE psch_unpriv LOGIN');

my $start_time = $node->safe_psql('postgres', 'SELECT pg_postmaster_start_time()');

$node->safe_psql('postgres', q{
DO $$ BEGIN
  FOR i IN 1..2048 LOOP EXECUTE 'SELECT 1'; END LOOP;
END $$;
});
cmp_ok(psch_get_stats($node)->{dropped}, '>', 0, 'Queue full');

my $trigger = 'SELECT pg_stat_ch_reset(), pg_cancel_backend(pg_backend_pid()), '
    . "pg_advisory_unlock(42);\n";
my ($ret, $stdout, $stderr) = $node->psql('postgres',
    ($trigger x 5) . "SELECT 'alive';",
    extra_params  => ['-U', 'psch_unpriv'],
    on_error_stop => 0);

like($stderr, qr/canceling statement due to user request/, 'Cancel delivered');
like($stdout, qr/alive/, 'Session survives');
is($node->safe_psql('postgres', 'SELECT pg_postmaster_start_time()'),
    $start_time, 'No crash restart');
unlike(slurp_file($node->logfile), qr/terminated by signal|TRAP:/, 'No backend crash');

$node->stop();
done_testing();
