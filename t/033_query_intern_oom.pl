#!/usr/bin/env perl
# Test: Query-text interner degrades gracefully under DSA exhaustion.
#
# Companion to t/032_query_intern.pl, which proves the *deduplication* path:
# repeated identical normalized queries collapse onto a single DSA body so a
# tight pool does not OOM.  This test exercises the *failure* path: many
# structurally-distinct queries cannot dedupe at all, so the DSA string area
# must run out, and we want to confirm the contract documented in PR #92:
#
#   - dsa_oom_count is incremented on each failed intern alloc
#   - events continue to enqueue (numeric telemetry preserved)
#   - the slot's query_len is set to 0 so the consumer renders empty query text
#     rather than wrong/stale SQL.  The round-trip "lands in ClickHouse as
#     query=''" assertion lives in t/034_query_intern_oom_export.pl.
#
# Strategy:
#   1. Configure an unreachable ClickHouse so the bgworker cannot drain.
#   2. Set string_area_size = 8MB (the minimum) so the DSA pool is tight.
#   3. Send N structurally-distinct long queries.  Each query embeds the loop
#      index K in *output column aliases*; pg_stat_ch normalizes literals to
#      $N but preserves identifiers (see t/027_query_normalization.pl), so
#      each K gets a distinct normalized text and a distinct queryid.  At
#      ~2KB clamped per body, 6000 distinct queries push ~12MB through an
#      8MB pool — guaranteed exhaustion.
#   4. Assert dsa_oom_count grew and the queue still gained events.

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::BackgroundPsql;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use psch;

# ---------------------------------------------------------------------------
# Node config: tight DSA pool, large queue, unreachable ClickHouse.
# ---------------------------------------------------------------------------
my $node = PostgreSQL::Test::Cluster->new('query_intern_oom');
$node->init();
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.queue_capacity = 8192
pg_stat_ch.string_area_size = 8MB
pg_stat_ch.flush_interval_ms = 60000
pg_stat_ch.batch_max = 1000
pg_stat_ch.clickhouse_host = '127.0.0.1'
pg_stat_ch.clickhouse_port = 1
});
$node->start();
$node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

psch_reset_stats($node);
my $baseline = psch_get_stats($node);

# ---------------------------------------------------------------------------
# Build N structurally-distinct ~2KB queries.
#
# Each query has the form:
#   SELECT 1 AS c_K_1, 1 AS c_K_2, ..., 1 AS c_K_100
#
# The literal `1` normalizes to `$N`, but the alias identifier `c_K_M` is
# preserved verbatim in the normalized form, so distinct K produces both a
# distinct queryid (parse tree differs) and a distinct query_hash (bytes
# differ).  100 aliases of average ~16 bytes plus boilerplate clamps each
# normalized form near the 2047-byte PSCH_MAX_QUERY_LEN ceiling.
# ---------------------------------------------------------------------------
my $distinct_count   = 6000;
my $aliases_per_query = 100;

my $session = $node->background_psql('postgres', on_error_stop => 1);

# Chunk multi-statement SQL so the wire buffer stays manageable.  Same
# pattern as 032 — psql tolerates large multi-statement sends but smaller
# chunks make any error attributable to a smaller window.
my $chunk_size = 250;
my $chunk      = '';
my $sent       = 0;
my $ret        = 0;

for (my $k = 1; $k <= $distinct_count; $k++) {
    my @aliases = map { "1 AS c_${k}_$_" } (1 .. $aliases_per_query);
    # LIMIT 0 returns no rows so background_psql does not echo a 100-column
    # row per query.  The optimizer still parses/plans the full statement
    # (executor hooks fire, queryid is computed, intern path runs), but the
    # test output stays sane.  LIMIT 0 itself is a literal that normalizes to
    # $N so it does not affect distinctness across loop iterations.
    $chunk .= "SELECT " . join(", ", @aliases) . " LIMIT 0;\n";

    if ($k % $chunk_size == 0) {
        (undef, $ret) = $session->query($chunk);
        last if $ret != 0;
        $chunk = '';
        $sent  = $k;
    }
}
if ($chunk ne '' && $ret == 0) {
    (undef, $ret) = $session->query($chunk);
    $sent = $distinct_count if $ret == 0;
}
is($ret, 0, "distinct queries dispatched ($sent of $distinct_count)");
$session->quit();

my $stats = psch_get_stats($node);
note(sprintf("baseline: enqueued=%s dsa_oom=%s",
             $baseline->{enqueued}, $baseline->{dsa_oom}));
note(sprintf("final:    enqueued=%s dropped=%s queue_size=%s dsa_oom=%s",
             $stats->{enqueued}, $stats->{dropped},
             $stats->{queue_size}, $stats->{dsa_oom}));

# ---------------------------------------------------------------------------
# Assertions
#
# - dsa_oom_count must have *grown* during the run: distinct-text workload
#   cannot be absorbed by interning, so the DSA pool has to refuse some
#   allocations.  Baseline is non-zero only in case earlier extension setup
#   ran something we did not anticipate; checking the delta keeps the test
#   robust to that.
# - enqueued count must have grown by most of $sent: the contract is that
#   intern failures degrade gracefully — the event still goes through with
#   numeric telemetry intact, just with empty query_text.  We do not require
#   exact equality because the queue can also fill (queue_capacity=8192 vs
#   $sent=6000 leaves headroom but is not infinite under producer back-
#   pressure).  Allow up to 25% loss before flagging.
# ---------------------------------------------------------------------------
my $oom_delta      = $stats->{dsa_oom}   - $baseline->{dsa_oom};
my $enqueued_delta = $stats->{enqueued}  - $baseline->{enqueued};

cmp_ok($oom_delta, '>', 0,
       "dsa_oom_count grew under distinct-text overload (delta=$oom_delta)");
cmp_ok($enqueued_delta, '>=', $sent * 0.75,
       "events still enqueued despite intern failures " .
       "(delta=$enqueued_delta, sent=$sent)");

$node->stop();
done_testing();
