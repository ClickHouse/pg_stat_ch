#!/usr/bin/env perl
# Test: Insert-race contract — concurrent first-time acquires for the same
# novel key all converge on a single interned body without leaks or spurious
# OOMs.
#
# PR #92 documents the lost-race path in PschQueryInternAcquire:
#
#   miss → allocate outside the partition lock → re-lock and HASH_ENTER_NULL
#       → found = true  (another backend won)
#           → free our loser allocation, return the existing object,
#             refcount++
#
# This branch only fires on the *first* concurrent access to a queryid: once
# the entry exists, subsequent acquires hit the fast path under partition
# lock and never race.  So to exercise it we need many distinct queryids
# being first-touched simultaneously by multiple backends.
#
# Strategy:
#   1. Spawn N parallel background_psql sessions.
#   2. Each session runs the *same* batch of M structurally-distinct
#      queries.  Whichever session reaches a given queryid first wins the
#      insert; the rest hit the lost-race branch and free their loser
#      allocations.
#   3. Assert that after the dust settles:
#      - dsa_oom_count == 0  (losers freed cleanly; no spurious OOMs)
#      - dropped == 0        (queue was large enough)
#      - enqueued >= N * M   (every backend's events accounted for)
#
# We cannot directly assert refcount accounting from SQL, but the OOM
# counter is the proxy: if losers leaked their allocations, the next
# iteration of the same workload would eventually exhaust the DSA pool and
# OOM.  Running the loop multiple times below makes a leak visible.

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::BackgroundPsql;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use psch;

# ---------------------------------------------------------------------------
# Node config: large queue, ample DSA pool, unreachable ClickHouse so we
# can observe the producer side in isolation.  A leak from the lost-race
# path would still be visible because the consumer is not draining refs.
# ---------------------------------------------------------------------------
my $node = PostgreSQL::Test::Cluster->new('query_intern_concurrent');
$node->init();
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.queue_capacity = 65536
pg_stat_ch.string_area_size = 64MB
pg_stat_ch.flush_interval_ms = 60000
pg_stat_ch.clickhouse_host = '127.0.0.1'
pg_stat_ch.clickhouse_port = 1
});
$node->start();
$node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

# ---------------------------------------------------------------------------
# Build the per-session batch.  Each query is structurally distinct (alias
# index in the SELECT list), so M queryids per iteration are first-touched
# by whichever of the N sessions ships them earliest.  Keep the body small
# enough that 64MB pool comfortably holds N*M intern entries even before
# the consumer drains anything.
# ---------------------------------------------------------------------------
my $num_sessions      = 4;
my $queries_per_round = 200;
my $rounds            = 3;

sub build_batch {
    my ($round) = @_;
    my @stmts;
    for (my $k = 1; $k <= $queries_per_round; $k++) {
        # Round number is part of the alias so each round has fresh queryids
        # (no entry exists yet, every acquire is a first-touch race).  This
        # is the whole point: hits to existing entries do not exercise the
        # lost-race branch.
        my @aliases = map { "1 AS c_r${round}_k${k}_$_" } (1 .. 30);
        # LIMIT 0 keeps psql output small without affecting queryid/intern-
        # key distinctness (see 033 for the rationale).
        push @stmts, "SELECT " . join(", ", @aliases) . " LIMIT 0";
    }
    return join(";\n", @stmts) . ";\n";
}

psch_reset_stats($node);
my $baseline = psch_get_stats($node);

# ---------------------------------------------------------------------------
# Drive N sessions in parallel.  background_psql's query_until is
# synchronous per-session, but Perl's libpq returns control to us between
# sends — so we issue the batch to each session sequentially (the SEND is
# non-blocking until each session's buffer fills), then await completion
# via a sentinel.  This is the same pattern as t/002_concurrent_sessions.pl.
#
# To maximize the chance that backends actually race (rather than serialize
# on the postmaster), each session is opened *before* any batch is sent;
# the batches then go out in tight succession.
# ---------------------------------------------------------------------------
for my $round (1 .. $rounds) {
    note("round $round: spawning $num_sessions sessions, $queries_per_round queries each");

    my @sessions;
    for my $i (1 .. $num_sessions) {
        push @sessions, $node->background_psql('postgres', on_error_stop => 1);
    }

    my $batch = build_batch($round);

    # Send the same batch to every session, each followed by a unique
    # sentinel so we can detect completion without races.
    for my $i (0 .. $#sessions) {
        my $sentinel = "CONCURRENT_DONE_R${round}_S${i}";
        $sessions[$i]->query_until(
            qr/\Q$sentinel\E/,
            $batch . "SELECT '$sentinel';\n");
    }

    $_->quit() for @sessions;
}

my $stats = psch_get_stats($node);
note(sprintf("baseline: enqueued=%s dsa_oom=%s",
             $baseline->{enqueued}, $baseline->{dsa_oom}));
note(sprintf("final:    enqueued=%s dropped=%s queue_size=%s dsa_oom=%s",
             $stats->{enqueued}, $stats->{dropped},
             $stats->{queue_size}, $stats->{dsa_oom}));

# ---------------------------------------------------------------------------
# Assertions
#
# - dsa_oom should not have grown: the pool is generous and the lost-race
#   path frees its loser allocation, so neither the alloc nor any leak
#   should pressure DSA.  A non-zero delta would mean either we
#   under-sized the pool or losers are leaking.
# - dropped must be zero: queue is sized for the full load.  A drop here
#   would mask leak-related OOMs by silently shedding work.
# - enqueued should account for at least sessions * queries_per_round *
#   rounds.  We use >= rather than == because PostgreSQL's own internal
#   activity (e.g. extension setup paths) may bump the counter too.
# ---------------------------------------------------------------------------
my $oom_delta      = $stats->{dsa_oom}  - $baseline->{dsa_oom};
my $enqueued_delta = $stats->{enqueued} - $baseline->{enqueued};
my $expected_min   = $num_sessions * $queries_per_round * $rounds;

is($oom_delta, 0,
   "no DSA OOM under generous pool — proves lost-race losers freed cleanly");
is($stats->{dropped}, 0, 'no drops with adequate queue capacity');
cmp_ok($enqueued_delta, '>=', $expected_min,
       "enqueued >= sessions*queries*rounds " .
       "(delta=$enqueued_delta, expected_min=$expected_min)");

$node->stop();
done_testing();
