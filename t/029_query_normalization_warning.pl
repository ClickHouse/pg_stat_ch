#!/usr/bin/env perl
# Test: query normalization must not emit duplicate escape-string warnings

use strict;
use warnings;
use lib 't';

use PostgreSQL::Test::BackgroundPsql;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

my $node = psch_init_node('normalize_warning');

my $session = $node->background_psql('postgres', on_error_stop => 1);
my ($stdout, $ret) = $session->query('SET client_min_messages = warning');
is($ret, 0, 'Can lower client message threshold');

($stdout, $ret) = $session->query('SET standard_conforming_strings = off');
is($ret, 0, 'Can disable standard_conforming_strings');

($stdout, $ret) = $session->query('SET escape_string_warning = on');
is($ret, 0, 'Can enable escape_string_warning');

$session->{stderr} = '';
($stdout, $ret) = $session->query(q{SELECT 'a\\b'});
ok(length($stdout) > 0, 'Query with backslash string literal returns output');

my $warning_count = () =
    ($session->{stderr} // '') =~ /nonstandard use of (?:escape|\\\\) in a string literal/g;
is($warning_count, 1, 'Normalization does not emit a duplicate escape string warning');

$session->quit();
$node->stop();
done_testing();
