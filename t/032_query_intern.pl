#!/usr/bin/env perl
# Test: Query-text interner deduplicates DSA storage across queued events.
#
# Without interning, every queued event owns a private DSA copy of the
# normalized query text — `queued_events * query_len` total live bytes.  A
# tightly-bounded DSA pool exhausts well before the queue itself fills.
#
# With interning, repeated identical normalized queries share a single DSA
# body and live DSA usage collapses to `distinct_live_query_texts * query_len`.
#
# Strategy:
#   1. Configure an unreachable ClickHouse so the bgworker cannot drain the
#      queue.  Events accumulate.
#   2. Set `string_area_size = 8MB` (the minimum allowed) so the DSA pool is
#      tight.  6000 × ~2KB unique copies would be ~12MB.
#   3. PREPARE/EXECUTE a single long normalized query that clamps near the
#      2047-byte truncation limit.
#   4. After many EXECUTEs, assert:
#      - the queue actually filled with events (proves the path under test ran)
#      - dsa_oom_count == 0 (proves storage was deduplicated, not duplicated)

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
my $node = PostgreSQL::Test::Cluster->new('query_intern');
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

# ---------------------------------------------------------------------------
# Build a normalized query whose cached text clamps near 2047 bytes.
#
# Each "relname != 'tN'" condition is roughly 18 bytes after normalization
# (the literal becomes $N).  ~140 conditions takes the normalized form well
# past 2KB so the cache clamps to PSCH_MAX_QUERY_LEN-1 = 2047 bytes — the
# worst case for per-event DSA cost.
# ---------------------------------------------------------------------------
my @conditions = map { "relname != 'table_$_'" } (1..140);
my $long_sql   = "SELECT count(*) FROM pg_class WHERE " .
                 join(" AND ", @conditions);

# Prepare the statement and execute it many times in one persistent backend.
my $exec_count = 6000;
my $session = $node->background_psql('postgres', on_error_stop => 1);
my (undef, $ret) = $session->query("PREPARE intern_test AS $long_sql");
is($ret, 0, 'PREPARE long normalized query succeeds');

# Drive the EXECUTEs in chunks of multi-statement SQL.  Sending all 6000 in
# one giant string is fine for psql, but chunking keeps the protocol output
# small enough to digest if anything goes wrong.  Build each chunk as a single
# scalar (the `x` operator on a scalar repeats it) — passing a list to query()
# would only use the first element.
my $chunk_size = 1000;
die "exec_count must be a multiple of chunk_size" if $exec_count % $chunk_size;
my $chunk_sql  = "EXECUTE intern_test;\n" x $chunk_size;
my $chunks     = int($exec_count / $chunk_size);
for (my $i = 0; $i < $chunks; $i++) {
    (undef, $ret) = $session->query($chunk_sql);
    last if $ret != 0;
}
is($ret, 0, "EXECUTE intern_test x $exec_count succeeds");
$session->quit();

my $stats = psch_get_stats($node);
note(sprintf("stats: enqueued=%s dropped=%s queue_size=%s dsa_oom=%s",
             $stats->{enqueued}, $stats->{dropped},
             $stats->{queue_size}, $stats->{dsa_oom}));

# ---------------------------------------------------------------------------
# Assertions
#
# - We expect at least 5000 enqueued events, proving EXECUTE actually drove
#   the producer path (rather than every call being filtered out).
# - With interning in place, dsa_oom_count must be 0: 6000 events sharing a
#   single ~2KB interned body fits comfortably in 8MB.  Without interning,
#   this same workload would push 12MB through an 8MB pool and OOM long
#   before the queue filled.
# ---------------------------------------------------------------------------
cmp_ok($stats->{enqueued}, '>=', 5000,
       'queue captured the bulk of repeated EXECUTEs');
is($stats->{dsa_oom}, 0,
   'interned repeated query text avoids DSA OOM under tight 8MB pool');

$node->stop();
done_testing();
