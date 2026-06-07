#!/usr/bin/env perl
# Parent query id linkage, verified through the OTel/Arrow export path
# (the production export pathway).  No ClickHouse or OTel collector is
# needed: pg_stat_ch.debug_arrow_dump_dir captures each Arrow IPC batch
# to disk before the gRPC send (which we deliberately point at a
# non-existent collector so it fails harmlessly).  See t/026_arrow_dump.pl
# for the same trick.
#
# What this test guards:
#   1. The Arrow IPC schema actually contains parent_query_id.  An earlier
#      version of #95 added the field to PschEvent and the
#      ClickHouse-native exporter but missed arrow_batch.cc.  That gap
#      escaped because the only existing parent_query_id test
#      (t/028, ClickHouse-native) didn't exercise the Arrow path.
#   2. Top-level queries report parent_query_id = 0.
#   3. Nested SPI queries report parent_query_id matching the outer's
#      query_id.
#   4. Log events captured by emit_log_hook while a nested SPI query is on
#      the stack carry query_id of the running (inner) statement and
#      parent_query_id of the outer caller — catches the CaptureLogEvent
#      off-by-one (would otherwise read the wrong slot).  We use RAISE
#      WARNING for this: emit_log_hook does not fire from errfinish for
#      ERROR-level events (errfinish PG_RE_THROWs without calling
#      EmitErrorReport; emit_log_hook only fires later in PostgresMain's
#      top-level catch, after all frames have been popped — or never, for
#      caught-in-EXCEPTION errors).  WARNING goes through EmitErrorReport
#      directly so the frame stack is intact when our hook runs.

use strict;
use warnings;
use lib 't';
use File::Temp qw(tempdir);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use psch;

if (system("uv --version >/dev/null 2>&1") != 0) {
    plan skip_all => 'uv not installed (needed for inline pyarrow validation)';
}

my $dump_dir = tempdir('psch_pqid_arrow_XXXX', TMPDIR => 1, CLEANUP => 1);

my $node = PostgreSQL::Test::Cluster->new('pqid_arrow');
$node->init();
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.queue_capacity = 65536
pg_stat_ch.flush_interval_ms = 100
pg_stat_ch.batch_max = 100
pg_stat_ch.use_otel = on
pg_stat_ch.otel_endpoint = 'localhost:14317'
pg_stat_ch.otel_arrow_passthrough = on
pg_stat_ch.debug_arrow_dump_dir = '$dump_dir'
pg_stat_ch.hostname = 'test-pqid-arrow-host'
});
$node->start();
$node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

# Fixtures.  Distinctive table/function names so we can filter on them
# in the Arrow dumps — names survive query normalization where literals
# do not.
$node->safe_psql('postgres', q{
    CREATE TABLE pqid_top_marker(x int);

    CREATE TABLE pqid_inner_marker(x int);
    INSERT INTO pqid_inner_marker VALUES (1);
    CREATE FUNCTION pqid_outer_caller() RETURNS int
    LANGUAGE plpgsql AS $$
    DECLARE v int;
    BEGIN
        SELECT x INTO v FROM pqid_inner_marker;
        RETURN v;
    END$$;

    CREATE TABLE pqid_warn_tbl(x int);
    INSERT INTO pqid_warn_tbl VALUES (1);
    CREATE FUNCTION pqid_emit_warn(x int) RETURNS int
    LANGUAGE plpgsql AS $$
    BEGIN
        RAISE WARNING 'pqid_warn_marker' USING ERRCODE = '01001';
        RETURN x;
    END$$;
    CREATE FUNCTION pqid_warn_outer() RETURNS int
    LANGUAGE plpgsql AS $$
    DECLARE v int;
    BEGIN
        SELECT pqid_emit_warn(x) INTO v FROM pqid_warn_tbl;
        RETURN v;
    END$$;
});

# Wait briefly for setup events to flush, then clear so the test queries
# we drive next are the only ones in the dump set.
$node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');
select(undef, undef, undef, 0.5);
unlink glob("$dump_dir/*.ipc");
psch_reset_stats($node);

# Drive the test queries.
$node->safe_psql('postgres', q{
    SELECT * FROM pqid_top_marker;
    SELECT count(*) FROM pqid_top_marker;
    SELECT pqid_outer_caller();
    SELECT pqid_warn_outer();
    SELECT pg_stat_ch_flush();
});

# Wait for dump files.
my @ipc_files;
my $deadline = time() + 10;
while (time() < $deadline) {
    @ipc_files = glob("$dump_dir/*.ipc");
    last if @ipc_files > 0;
    select(undef, undef, undef, 0.2);
}
cmp_ok(scalar @ipc_files, '>=', 1, 'arrow IPC dumps were produced')
    or BAIL_OUT('No Arrow IPC dumps; cannot verify parent_query_id');

# Inline pyarrow validator: parses all .ipc files, computes the
# assertions, and emits one KEY=VALUE line per result.  Keeping the
# logic in one script avoids parsing Arrow IPC from Perl directly.
my $py = <<'PYEOF';
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyarrow"]
# ///
import glob, os, sys
import pyarrow.ipc as ipc

dump_dir = sys.argv[1]
rows = []
schema_has_pqid = False
for p in sorted(glob.glob(os.path.join(dump_dir, "*.ipc"))):
    with open(p, "rb") as f:
        reader = ipc.open_stream(f)
        if reader.schema.get_field_index("parent_query_id") != -1:
            schema_has_pqid = True
        rows.extend(reader.read_all().to_pylist())

print(f"schema_has_parent_query_id={'1' if schema_has_pqid else '0'}")

def has(text):
    return [r for r in rows if text in (r.get("query_text") or "")]

# Arrow dict-encodes query_id and parent_query_id as decimal *strings*;
# "0" means top-level / no parent.  Compare against strings, not ints.
def is_zero(v):
    return v is None or v == "" or v == "0"

# Test 1: top-level
top = has("pqid_top_marker")
print(f"top_rows={len(top)}")
print(f"top_nonzero_parent={sum(1 for r in top if not is_zero(r.get('parent_query_id')))}")

# Test 2: nested SPI.  Filter down to the SELECT op so that setup CREATE
# TABLE / INSERT / DROP utility statements (which also mention the table)
# don't pollute the "no orphans" check.
outer = [r for r in has("pqid_outer_caller") if r.get("db_operation") == "SELECT"]
inner = [r for r in has("pqid_inner_marker") if r.get("db_operation") == "SELECT"]
outer_qids = {o.get('query_id') for o in outer if not is_zero(o.get('query_id'))}
inner_linked = sum(1 for r in inner if r.get('parent_query_id') in outer_qids)
print(f"outer_rows={len(outer)}")
print(f"inner_rows={len(inner)}")
print(f"inner_linked_to_outer={inner_linked}")
print(f"inner_zero_parent={sum(1 for r in inner if is_zero(r.get('parent_query_id')))}")
print(f"outer_nonzero_parent={sum(1 for r in outer if not is_zero(r.get('parent_query_id')))}")

# Test 3: log event captured inside nested SPI.  The warning event itself
# carries no query_text (CaptureLogEvent leaves it empty), so we identify
# it via err_sqlstate '01001' (our RAISE WARNING's custom code).
warn_outer = [r for r in has("pqid_warn_outer") if r.get("db_operation") == "SELECT"]
warn_inner = [r for r in has("pqid_emit_warn") if r.get("db_operation") == "SELECT"]
warn_outer_qids = {o.get('query_id') for o in warn_outer if not is_zero(o.get('query_id'))}
warn_inner_qids = {i.get('query_id') for i in warn_inner if not is_zero(i.get('query_id'))}
warn = [r for r in rows
        if r.get("err_sqlstate") == "01001" and r.get("db_operation") in (None, "", "UNKNOWN")]
warn_linked_to_outer = sum(1 for r in warn
                           if r.get('parent_query_id') in warn_outer_qids)
warn_qid_is_inner = sum(1 for r in warn if r.get('query_id') in warn_inner_qids)
warn_qid_is_outer = sum(1 for r in warn if r.get('query_id') in warn_outer_qids)
warn_self_parent = sum(1 for r in warn
                       if not is_zero(r.get('query_id'))
                       and r.get('query_id') == r.get('parent_query_id'))
print(f"warn_rows={len(warn)}")
print(f"warn_linked_to_outer={warn_linked_to_outer}")
print(f"warn_qid_is_inner={warn_qid_is_inner}")
print(f"warn_qid_is_outer={warn_qid_is_outer}")
print(f"warn_self_parent={warn_self_parent}")
PYEOF

my $script_path = "$dump_dir/_validate.py";
open(my $fh, '>', $script_path) or die "Cannot write $script_path: $!";
print $fh $py;
close $fh;

my $raw = `uv run --quiet '$script_path' '$dump_dir' 2>&1`;
my %r;
for my $line (split /\n/, $raw) {
    $r{$1} = $2 if $line =~ /^(\w+)=(.*)$/;
}
diag("pyarrow stdout:\n$raw") if $ENV{TEST_VERBOSE};

# Regression check: the schema itself must include parent_query_id.
is($r{schema_has_parent_query_id}, '1',
   'arrow_batch.cc schema includes parent_query_id column');

subtest 'top-level parent_query_id is 0' => sub {
    cmp_ok($r{top_rows}, '>=', 1, 'top-level marker rows landed');
    is($r{top_nonzero_parent}, '0', 'top-level rows report parent_query_id = 0');
};

subtest 'nested SPI parent_query_id links to outer' => sub {
    cmp_ok($r{outer_rows}, '>=', 1, 'outer rows landed');
    cmp_ok($r{inner_rows}, '>=', 1, 'inner rows landed');
    cmp_ok($r{inner_linked_to_outer}, '>=', 1,
           'inner row joins outer via parent_query_id');
    is($r{inner_zero_parent}, '0',
       'nested SPI is not reported as top-level');
    is($r{outer_nonzero_parent}, '0',
       'outer call still reports parent_query_id = 0');
};

subtest 'log event inside nested SPI links queryid -> outer' => sub {
    cmp_ok($r{warn_rows}, '>=', 1, 'RAISE WARNING log event landed');
    cmp_ok($r{warn_linked_to_outer}, '>=', 1,
           "log event parent_query_id = outer caller's query_id");
    cmp_ok($r{warn_qid_is_inner}, '>=', 1,
           'log event query_id = inner SPI statement (the running query)');
    is($r{warn_qid_is_outer}, '0',
       'log event query_id is NOT the outer caller (off-by-one regression)');
    is($r{warn_self_parent}, '0',
       'log event query_id != parent_query_id (no self-parent)');
};

$node->stop();
done_testing();
