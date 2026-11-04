#!/usr/bin/env perl
# Test: err_message exports the message format string, never argument values
# Prerequisites: ClickHouse container must be running

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
    plan skip_all => 'Docker not available, skipping error message tests';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse container not running';
}

psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

my $node = psch_init_node_with_clickhouse('err_redaction',
    flush_interval_ms => 100,
    batch_max => 100
);

# Helper: run setup statements and then a failing statement in one backend,
# flush, and return "<err_sqlstate>\t<err_message>" of the error it produced.
sub get_captured_error {
    my ($node, $sql, @setup) = @_;

    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    my $session = $node->background_psql('postgres', on_error_stop => 0);
    my ($stdout, $ret) = $session->query('SELECT pg_backend_pid()');
    die "failed to get backend pid" unless $ret == 0;
    my ($pid) = $stdout =~ /(\d+)/;
    die "backend pid missing from psql output: $stdout" unless defined $pid;

    for my $stmt (@setup) {
        ($stdout, $ret) = $session->query($stmt);
        die "setup failed unexpectedly: $stmt" unless $ret == 0;
    }

    ($stdout, $ret) = $session->query($sql);
    die "statement succeeded unexpectedly: $sql" unless $ret == 1;
    $session->{stderr} = '';

    ($stdout, $ret) = $session->query('SELECT pg_stat_ch_flush()');
    die "flush failed unexpectedly" unless $ret == 0;
    $session->quit();

    psch_wait_for_export($node, 1, 10);

    return psch_wait_for_clickhouse_query(
        "SELECT err_sqlstate, err_message FROM pg_stat_ch.events_raw " .
        "WHERE pid = $pid AND err_elevel >= 21 " .
        "ORDER BY ts DESC LIMIT 1 FORMAT TSVRaw",
        sub { $_[0] ne '' },
        10
    );
}

$node->safe_psql('postgres', q{
    CREATE TABLE redaction_users(email text UNIQUE);
    INSERT INTO redaction_users VALUES ('secret_key@example.com');
    CREATE TABLE redaction_parallel(val text);
    INSERT INTO redaction_parallel VALUES ('secret_parallel_val');
});

# ---------------------------------------------------------------------------
# Test 1: Input value of a failed type cast
# ---------------------------------------------------------------------------
subtest 'invalid input value not exported' => sub {
    my $row = get_captured_error($node, "SELECT 'secret_input_val'::numeric");
    is($row, qq{22P02\tinvalid input syntax for type %s: "%s"},
        'Format string exported without the input value');
};

# ---------------------------------------------------------------------------
# Test 2: Query fragment of a syntax error
# ---------------------------------------------------------------------------
subtest 'syntax error fragment not exported' => sub {
    my $row = get_captured_error($node, "SELECT 1 'secret_syntax_val'");
    is($row, qq{42601\t%s at or near "%s"},
        'Format string exported without the query fragment');
};

# ---------------------------------------------------------------------------
# Test 3: Constraint name in a unique violation
# ---------------------------------------------------------------------------
subtest 'unique violation exports format string' => sub {
    my $row = get_captured_error($node,
        "INSERT INTO redaction_users VALUES ('secret_key\@example.com')");
    is($row, qq{23505\tduplicate key value violates unique constraint "%s"},
        'Format string exported without the constraint name');
};

# ---------------------------------------------------------------------------
# Test 4: PL/pgSQL RAISE passes its whole text through "%s"
# ---------------------------------------------------------------------------
subtest 'RAISE message not exported' => sub {
    my $row = get_captured_error($node,
        q{DO $$ BEGIN RAISE EXCEPTION 'order % failed', 'secret_raise_val'; END $$});
    is($row, "P0001\t%s", 'Format string exported without the RAISE text');
};

# ---------------------------------------------------------------------------
# Test 5: Errors rethrown from a parallel worker carry no format string
# ---------------------------------------------------------------------------
subtest 'parallel worker error message not exported' => sub {
    my $row = get_captured_error($node,
        'SELECT val::numeric FROM redaction_parallel',
        'SET debug_parallel_query = on');
    is($row, "22P02\t", 'Rethrown worker error exports no message');
};

$node->stop();
done_testing();
