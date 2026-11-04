#!/usr/bin/env perl
# Test: Query labels via sqlcommenter comments → ClickHouse labels column
# Prerequisites: ClickHouse container must be running
#   docker compose -f docker/docker-compose.test.yml up -d

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

# Skip if Docker/ClickHouse not available
if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker not available, skipping ClickHouse labels tests';
}

my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all =>
        'ClickHouse container not running. Start with: docker compose -f docker/docker-compose.test.yml up -d';
}

psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.events_raw");

my $node = psch_init_node_with_clickhouse('ch_labels',
    flush_interval_ms => 100,
    batch_max         => 100,
);
# track_labels is off by default and PGC_POSTMASTER.
$node->append_conf('postgresql.conf', 'pg_stat_ch.track_labels = on');
$node->restart();

# Returns the labels of the first event whose query text contains $marker,
# after waiting for that event to reach ClickHouse.
sub labels_for_marker {
    my ($marker) = @_;
    psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw WHERE query_text LIKE '%$marker%'",
        sub { $_[0] >= 1 },
        10
    );
    return psch_query_clickhouse(
        "SELECT labels FROM pg_stat_ch.events_raw "
        . "WHERE query_text LIKE '%$marker%' LIMIT 1");
}

# Test 1: Query with sqlcommenter labels appears with JSON labels in CH
subtest 'basic labels export' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres',
        "SELECT 1 /* controller='users',action='show' */");

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $labels = psch_wait_for_clickhouse_query(
        "SELECT labels FROM pg_stat_ch.events_raw "
        . "WHERE query_text LIKE '%controller%' LIMIT 1",
        sub { $_[0] ne '' && $_[0] ne '{}' },
        10
    );

    like($labels, qr/controller/, 'labels JSON contains controller key');
    like($labels, qr/users/,      'labels JSON contains users value');
    like($labels, qr/action/,     'labels JSON contains action key');
    like($labels, qr/show/,       'labels JSON contains show value');
};

# Test 2: Query without comment gets empty labels
subtest 'no comment produces empty labels' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', 'CREATE TABLE nocomment_test(id int)');
    $node->safe_psql('postgres', 'DROP TABLE nocomment_test');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $labels = psch_wait_for_clickhouse_query(
        "SELECT labels FROM pg_stat_ch.events_raw "
        . "WHERE query_text LIKE '%nocomment_test%' LIMIT 1",
        sub { defined $_[0] && $_[0] ne '' },
        10
    );

    like($labels, qr/^\{\}$/, 'no-comment query has empty {} labels');
};

# Test 3: Multiple label keys
subtest 'multiple labels' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres',
        "SELECT 1 /* controller='orders',action='index',framework='rails',db_driver='pg' */");
    $node->safe_psql('postgres', "SELECT 1 /* dup_marker='x',a='1',b='2',a='3' */");

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Query individual label values via JSONExtractString
    my $controller = psch_wait_for_clickhouse_query(
        "SELECT JSONExtractString(labels, 'controller') FROM pg_stat_ch.events_raw "
        . "WHERE query_text LIKE '%orders%' LIMIT 1",
        sub { $_[0] =~ /orders/ },
        10
    );
    like($controller, qr/orders/, 'controller label = orders');

    my $framework = psch_query_clickhouse(
        "SELECT JSONExtractString(labels, 'framework') FROM pg_stat_ch.events_raw "
        . "WHERE query_text LIKE '%orders%' LIMIT 1");
    like($framework, qr/rails/, 'framework label = rails');

    like(labels_for_marker('dup_marker'), qr/^\{"dup_marker":"x","a":"3","b":"2"\}$/,
        'repeated key keeps its first position and the last value');
};

# Test 4: track_labels = off produces empty labels
subtest 'track_labels off' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    # Off omits the column from inserts, so ClickHouse fills its '{}' default.
    $node->safe_psql('postgres',
        "ALTER SYSTEM SET pg_stat_ch.track_labels = off");
    $node->restart();

    $node->safe_psql('postgres',
        "CREATE TABLE labels_off_test(id int) /* controller='off' */");
    $node->safe_psql('postgres',
        "DROP TABLE labels_off_test /* controller='off' */");

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Wait for rows to arrive in ClickHouse first
    psch_wait_for_clickhouse_query(
        "SELECT count() FROM pg_stat_ch.events_raw WHERE query_text LIKE '%labels_off_test%'",
        sub { $_[0] >= 1 },
        10
    );

    my $labels = psch_query_clickhouse(
        "SELECT labels FROM pg_stat_ch.events_raw "
        . "WHERE query_text LIKE '%labels_off_test%' LIMIT 1");
    like($labels, qr/^\{\}$/, 'track_labels=off produces empty labels');

    # Re-enable
    $node->safe_psql('postgres',
        "ALTER SYSTEM SET pg_stat_ch.track_labels = on");
    $node->restart();
};

# Test 5: URL-encoded values are decoded
subtest 'url encoded values' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres',
        "SELECT 1 /* route='%2Fapi%2Fusers%2F%3Aid' */");

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $labels = psch_wait_for_clickhouse_query(
        "SELECT labels FROM pg_stat_ch.events_raw "
        . "WHERE query_text LIKE '%route%' LIMIT 1",
        sub { $_[0] ne '' && $_[0] ne '{}' },
        10
    );

    like($labels, qr{^\{"route":"/api/users/:id"\}$}, 'URL-encoded value decoded correctly');
};

# Test 6: Only last comment is extracted
subtest 'multiple comments takes last' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres',
        "SELECT /* first='ignored' */ 1 /* second='captured' */");

    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    my $labels = psch_wait_for_clickhouse_query(
        "SELECT labels FROM pg_stat_ch.events_raw "
        . "WHERE query_text LIKE '%second%' LIMIT 1",
        sub { $_[0] ne '' && $_[0] ne '{}' },
        10
    );

    like($labels, qr/captured/, 'last comment labels exported');
    unlike($labels, qr/ignored/, 'first comment labels not exported');
};

# Test 7: Only a comment that ends the query is parsed
subtest 'comment inside query ignored' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', "SELECT /* mid_marker='x' */ 1");
    # Each ends in "*/" that closes something other than the last "/*".
    $node->safe_psql('postgres',
        "SELECT 1 /* nested_marker /* controller='users',close='*/' */");
    $node->safe_psql('postgres', "SELECT 1 /* split_marker='x',bad='*/--' */");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    like(labels_for_marker('mid_marker'), qr/^\{\}$/,
        'comment before the end of the query produces empty labels');
    like(labels_for_marker('nested_marker'), qr/^\{\}$/,
        'inner block of a nested comment produces empty labels');
    like(labels_for_marker('split_marker'), qr/^\{\}$/,
        'block closed early, then a line comment, produces empty labels');
};

# Test 8: A malformed pair rejects the whole comment
subtest 'malformed comment rejected' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', "SELECT 1 /* bad_marker='x',oops */");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    like(labels_for_marker('bad_marker'), qr/^\{\}$/,
        'malformed comment produces empty labels');
};

# Test 9: A pair with a quote is dropped; an encoded comma is decoded
subtest 'quoted value dropped, encoded comma kept' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres',
        "SELECT 1 /* esc_marker='x',quoted='it\\'s',list='a%2Cb' */");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    like(labels_for_marker('esc_marker'), qr/^\{"esc_marker":"x","list":"a,b"\}$/,
        'escaped quote is scanned but not a tag character');
};

# Test 10: No cap on the number of labels; a malformed pair still rejects all
subtest 'many labels' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    my $many = join(',', map { "manyk$_='v'" } 0 .. 19);
    my $malformed = join(',', map { "overk$_='v'" } 0 .. 19) . ',oops';
    $node->safe_psql('postgres', "SELECT 1 /* $many */");
    $node->safe_psql('postgres', "SELECT 1 /* $malformed */");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    labels_for_marker('manyk0=');
    my $count = psch_query_clickhouse(
        "SELECT JSONLength(labels) FROM pg_stat_ch.events_raw "
        . "WHERE query_text LIKE '%manyk0=%' LIMIT 1");
    is($count, '20', 'all 20 labels are kept');

    like(labels_for_marker('overk0='), qr/^\{\}$/,
        'malformed pair after many valid ones rejects the comment');
};

# Test 11: tag rules drop a pair, not the comment
subtest 'tag rules' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    my $key65 = 'k' x 65;
    my $key66 = 'j' x 66;
    $node->safe_psql('postgres',
        "SELECT 1 /* rulemark='x',$key65='v',$key66='v',sp='a b',empty='',pct='50%' */");
    $node->safe_psql('postgres',
        "SELECT 1 /* valmark='x',big='" . ('a' x 1024) . "' */");
    $node->safe_psql('postgres',
        "SELECT 1 /* val2mark='x',big='" . ('a' x 1025) . "' */");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    like(labels_for_marker('rulemark'), qr/^\{"rulemark":"x","$key65":"v"\}$/,
        'long keys and disallowed characters are dropped');
    labels_for_marker('valmark');
    my $len = psch_query_clickhouse(
        "SELECT length(JSONExtractString(labels, 'big')) FROM pg_stat_ch.events_raw "
        . "WHERE query_text LIKE '%valmark%' LIMIT 1");
    is($len, '1024', 'a 1024-byte value is kept');
    like(labels_for_marker('val2mark'), qr/^\{"val2mark":"x"\}$/,
        'a 1025-byte value is dropped');
};

# Test 12: Text cut at the length cap gets no labels, even when the cut lands
# right after a comment from the middle of the query
subtest 'text at the length cap' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    my $head = 'SELECT /*';
    my $tail = "*/ /* truncmark='x' */";
    my $pad = 'p' x (2047 - length($head) - length($tail));
    $node->safe_psql('postgres', "$head$pad$tail current_user");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    like(labels_for_marker('truncmark'), qr/^\{\}$/,
        'comment exposed by the cut produces empty labels');
    my $len = psch_query_clickhouse(
        "SELECT length(query_text) FROM pg_stat_ch.events_raw "
        . "WHERE query_text LIKE '%truncmark%' LIMIT 1");
    is($len, '2047', 'stored text was cut right after the comment');
};

# Test 13: A block comment inside a trailing line comment is not parsed
subtest 'block comment inside line comment ignored' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', "SELECT 1 -- /* line_marker='x' */");
    # The "/*" is inside a quoted identifier; the query ends in a line comment.
    $node->safe_psql('postgres',
        q{SELECT current_user AS "/* qid_marker='users',ignored"--='x' */});
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    like(labels_for_marker('line_marker'), qr/^\{\}$/,
        'block comment after -- produces empty labels');
    like(labels_for_marker('qid_marker'), qr/^\{\}$/,
        'quoted /* followed by a line comment produces empty labels');
};

# Test 14: CR and CRLF end a line comment, as in the PostgreSQL scanner
subtest 'CR and CRLF end a line comment' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.events_raw");
    psch_reset_stats($node);

    $node->safe_psql('postgres', "SELECT 1 -- note\r/* crmark='x' */");
    $node->safe_psql('postgres', "SELECT 1 -- note\r\n/* crlfmark='x' */");
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    like(labels_for_marker('crmark'), qr/^\{"crmark":"x"\}$/,
        'labels after a CR-terminated line comment are exported');
    like(labels_for_marker('crlfmark'), qr/^\{"crlfmark":"x"\}$/,
        'labels after a CRLF-terminated line comment are exported');
};

$node->stop();
done_testing();
