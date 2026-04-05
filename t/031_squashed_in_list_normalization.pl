#!/usr/bin/env perl
# Test: Squashed IN-list normalization (PG18+ only)
#
# PG18 introduced "squashed" constant lists: SELECT * FROM t WHERE id IN (1,2,3)
# gets a single LocationLen entry with squashed=true that covers the entire list.
# The normalized output should be: ... IN ($1 /*, ... */)
#
# This test verifies that FillInConstantLengths correctly computes the token
# length for squashed constants, producing clean normalized output.

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::BackgroundPsql;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Skip if Docker/ClickHouse not available
if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running';
}

psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

my $node = psch_init_node_with_clickhouse('squashed_in',
    flush_interval_ms => 100,
    batch_max => 100
);

# Detect PG version — squashed IN lists are PG18+ only
my $pg_version = $node->safe_psql('postgres',
    'SHOW server_version_num');
chomp($pg_version);
if ($pg_version < 180000) {
    plan skip_all => "Squashed IN-list normalization requires PG18+ (have $pg_version)";
}

$node->safe_psql('postgres', 'CREATE TABLE test_squash(id int, val text)');
$node->safe_psql('postgres',
    "INSERT INTO test_squash SELECT g, 'v' || g FROM generate_series(1,20) g");

# Helper: run a query, flush, and return the captured query text.
sub get_captured_query {
    my ($node, $sql) = @_;

    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    my $session = $node->background_psql('postgres', on_error_stop => 1);
    my ($stdout, $ret) = $session->query('SELECT pg_backend_pid()');
    die "failed to get backend pid" unless $ret == 0;
    my ($pid) = $stdout =~ /(\d+)/;
    die "backend pid missing" unless defined $pid;

    ($stdout, $ret) = $session->query($sql);
    die "query failed: $sql" unless $ret == 0;

    ($stdout, $ret) = $session->query('SELECT pg_stat_ch_flush()');
    die "flush failed" unless $ret == 0;
    $session->quit();

    psch_wait_for_export($node, 1, 10);

    return psch_wait_for_clickhouse_query(
        "SELECT query FROM pg_stat_ch.events_raw " .
        "WHERE pid = $pid " .
        "AND query NOT LIKE '%pg_stat_ch%' " .
        "AND query NOT LIKE '%pg_extension%' " .
        "AND query != '' " .
        "ORDER BY ts_start DESC LIMIT 1",
        sub { $_[0] ne '' },
        10
    );
}

# ---------------------------------------------------------------------------
# Test 1: Basic IN list with numeric constants gets squashed
# ---------------------------------------------------------------------------
subtest 'IN list with numerics is squashed' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_squash WHERE id IN (1, 2, 3, 4, 5)");

    like($q, qr/\$\d/, 'IN list has placeholder');
    # The squashed form should collapse the entire list into one placeholder
    like($q, qr{/\*, \.\.\. \*/}, 'Squashed comment marker present');
    # The original comma-separated list should be gone
    unlike($q, qr/\d\s*,\s*\d/, 'No comma-separated raw numbers');
    like($q, qr/IN\s*\(/, 'IN keyword and opening paren preserved');
    # The full squashed form: IN ($N /*, ... */) with nothing else inside parens
    like($q, qr/IN\s*\(\s*\$\d+\s+\/\*,\s*\.\.\.\s*\*\/\s*\)/,
        'IN list fully squashed to single placeholder with comment');
};

# ---------------------------------------------------------------------------
# Test 2: IN list with string constants gets squashed
# ---------------------------------------------------------------------------
subtest 'IN list with strings is squashed' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_squash WHERE val IN ('alpha', 'beta', 'gamma')");

    like($q, qr/\$\d/, 'IN list has placeholder');
    like($q, qr{/\*, \.\.\. \*/}, 'Squashed comment marker present');
    unlike($q, qr/alpha/, 'String constant alpha removed');
    unlike($q, qr/beta/, 'String constant beta removed');
    unlike($q, qr/gamma/, 'String constant gamma removed');
};

# ---------------------------------------------------------------------------
# Test 3: Mixed query — IN list squashed AND other constants normalized
# ---------------------------------------------------------------------------
subtest 'IN list squashed alongside regular constant' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_squash WHERE id IN (10, 20, 30) AND val = 'secret'");

    like($q, qr{/\*, \.\.\. \*/}, 'Squashed comment marker present for IN list');
    unlike($q, qr/\b10\b/, 'IN constant 10 removed');
    unlike($q, qr/\b20\b/, 'IN constant 20 removed');
    unlike($q, qr/\b30\b/, 'IN constant 30 removed');
    unlike($q, qr/secret/, 'Standalone string constant removed');
    # Should have at least two distinct placeholders ($N for IN, $M for val)
    like($q, qr/\$1/, 'First placeholder present');
    like($q, qr/\$2/, 'Second placeholder present');
};

# ---------------------------------------------------------------------------
# Test 4: Large IN list to ensure full list coverage
# ---------------------------------------------------------------------------
subtest 'large IN list fully squashed' => sub {
    my @nums = (100 .. 119);
    my $in_list = join(', ', @nums);
    my $q = get_captured_query($node,
        "SELECT * FROM test_squash WHERE id IN ($in_list)");

    like($q, qr{/\*, \.\.\. \*/}, 'Squashed comment marker present');
    # Verify no raw numbers from the list survive
    for my $n (@nums) {
        unlike($q, qr/\b$n\b/, "IN constant $n removed");
    }
    # The normalized form should be compact, not contain 20 separate placeholders
    unlike($q, qr/\$5/, 'No excessive placeholders (list is squashed, not expanded)');
};

# ---------------------------------------------------------------------------
# Test 5: NOT IN list also squashed
# ---------------------------------------------------------------------------
subtest 'NOT IN list squashed' => sub {
    my $q = get_captured_query($node,
        "SELECT * FROM test_squash WHERE id NOT IN (7, 8, 9)");

    like($q, qr{/\*, \.\.\. \*/}, 'Squashed comment marker present for NOT IN');
    unlike($q, qr/\b[7-9]\b/, 'NOT IN constants removed');
};

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', 'DROP TABLE IF EXISTS test_squash');
$node->stop();
done_testing();
