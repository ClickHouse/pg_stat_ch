# /// script
# requires-python = ">=3.9"
# dependencies = ["pyarrow"]
# ///
"""Validates pg_stat_ch Arrow IPC payloads produced by arrow_batch_harness.c.

Two layers:
  1. Structural: stdlib-only V5 framing + flatbuffer walk — message sequence
     (schema, 13 dictionary batches with ids 0..12, record batch, EOS),
     BodyCompression{ZSTD}, null_count=0, 8-byte alignment, ZSTD frame magic.
  2. Decoded (pyarrow): schema equality against the documented 56-field
     schema and per-cell value equality against the synthetic event recipe
     mirrored from arrow_batch_harness.c (FillEvent).

Usage: validate.py [--structural-only] payload1.bin payload2.bin
"""
import struct
import sys

# ---------------------------------------------------------------------------
# Expected data (mirror of FillEvent in arrow_batch_harness.c)
# ---------------------------------------------------------------------------

CMD = ["UNKNOWN", "SELECT", "UPDATE", "INSERT", "DELETE", "MERGE", "UTILITY", "NOTHING"]

U64_COLS = [
    "duration_us", "rows", "shared_blks_hit", "shared_blks_read", "shared_blks_written",
    "shared_blks_dirtied", "shared_blk_read_time_us", "shared_blk_write_time_us",
    "local_blks_hit", "local_blks_read", "local_blks_written", "local_blks_dirtied",
    "local_blk_read_time_us", "local_blk_write_time_us", "temp_blks_read", "temp_blks_written",
    "temp_blk_read_time_us", "temp_blk_write_time_us", "wal_records", "wal_bytes", "wal_fpi",
    "cpu_user_time_us", "cpu_sys_time_us", "jit_functions", "jit_generation_time_us",
    "jit_inlining_time_us", "jit_optimization_time_us", "jit_emission_time_us",
    "jit_deform_time_us",
]

NUM_DICT_COLS = 13


def expected_row(i, region):
    return {
        "ts": (1000000 + i * 1000 + 946684800000000) * 1000,
        "severity": "",
        "body": "",
        "trace_id": "",
        "span_id": "",
        "query_id": str(2**64 - 1 if i == 3 else (i % 7) * 1234567890123),
        "db_name": "x" * 63 if i == 19 else f"db_{i % 3}",
        "db_user": f"user_{i % 2}",
        "db_operation": CMD[i % 8],
        "app": "a" * 63 if i == 7 else "app",
        "client_addr": "" if i % 4 == 0 else ("c" * 45 if i == 9 else f"10.0.0.{i}"),
        "query_text": "q" * 2048 if i == 11 else f"SELECT {i}",
        "pid": "2147483647" if i == 5 else str(100000 + i),
        "err_message": "e" * 2048 if i == 13 else ("" if i % 5 == 0 else f"error {i}"),
        "err_sqlstate": "" if i % 6 == 0 else "42P01",
        "err_elevel": 0 if i % 3 == 0 else 21,
        "duration_us": i * 10 + 1,
        "rows": i * 100,
        "shared_blks_hit": 0 if i == 17 else i * 2,
        "shared_blks_read": i * 3,
        "shared_blks_written": i * 7,
        "shared_blks_dirtied": i * 5,
        "shared_blk_read_time_us": i * 31,
        "shared_blk_write_time_us": i * 37,
        "local_blks_hit": i * 11,
        "local_blks_read": i * 13,
        "local_blks_written": i * 19,
        "local_blks_dirtied": i * 17,
        "local_blk_read_time_us": i * 41,
        "local_blk_write_time_us": i * 43,
        "temp_blks_read": i * 23,
        "temp_blks_written": i * 29,
        "temp_blk_read_time_us": i * 47,
        "temp_blk_write_time_us": i * 53,
        "wal_records": i * 59,
        "wal_bytes": i * 67,
        "wal_fpi": i * 61,
        "cpu_user_time_us": i * 71,
        "cpu_sys_time_us": i * 73,
        "jit_functions": 0 if i == 17 else i % 50,
        "jit_generation_time_us": i * 3 + 1,
        "jit_inlining_time_us": i * 7 + 3,
        "jit_optimization_time_us": i * 11 + 4,
        "jit_emission_time_us": i * 13 + 5,
        "jit_deform_time_us": i * 5 + 2,
        "parallel_workers_planned": 0 if i == 17 else i % 10,
        "parallel_workers_launched": (i + 1) % 10,
        "instance_ubid": "inst-1",
        "server_ubid": "srv-9",
        "server_role": "primary",
        "read_replica_type": "",
        "region": region,
        "cell": "cell-7",
        "service_version": "0.4.0-test",
        "host_id": "host-abc",
        "pod_name": "pod-xyz",
    }


# ---------------------------------------------------------------------------
# Minimal flatbuffer walker (little-endian)
# ---------------------------------------------------------------------------


def _i8(b, o):
    return struct.unpack_from("<b", b, o)[0]


def _u8(b, o):
    return struct.unpack_from("<B", b, o)[0]


def _i16(b, o):
    return struct.unpack_from("<h", b, o)[0]


def _i32(b, o):
    return struct.unpack_from("<i", b, o)[0]


def _i64(b, o):
    return struct.unpack_from("<q", b, o)[0]


def _table(b, o):
    vt = o - _i32(b, o)
    n = (_i16(b, vt) - 4) // 2
    return {f: o + _i16(b, vt + 4 + 2 * f) for f in range(n) if _i16(b, vt + 4 + 2 * f) != 0}


def _indirect(b, slot):
    return slot + _i32(b, slot)


def parse_recordbatch(meta, o):
    t = _table(meta, o)
    out = {"length": _i64(meta, t[0]) if 0 in t else 0, "nodes": [], "buffers": []}
    if 1 in t:
        vo = _indirect(meta, t[1])
        out["nodes"] = [
            (_i64(meta, vo + 4 + 16 * k), _i64(meta, vo + 4 + 16 * k + 8))
            for k in range(_i32(meta, vo))
        ]
    if 2 in t:
        vo = _indirect(meta, t[2])
        out["buffers"] = [
            (_i64(meta, vo + 4 + 16 * k), _i64(meta, vo + 4 + 16 * k + 8))
            for k in range(_i32(meta, vo))
        ]
    if 3 in t:
        ct = _table(meta, _indirect(meta, t[3]))
        out["codec"] = _i8(meta, ct[0]) if 0 in ct else 0
        out["method"] = _i8(meta, ct[1]) if 1 in ct else 0
    else:
        out["codec"] = None
    return out


def parse_message(meta):
    t = _table(meta, _i32(meta, 0))
    m = {
        "version": _i16(meta, t[0]) if 0 in t else 0,
        "header_type": _u8(meta, t[1]) if 1 in t else 0,
        "bodyLength": _i64(meta, t[3]) if 3 in t else 0,
    }
    if 2 in t:
        ho = _indirect(meta, t[2])
        if m["header_type"] == 2:  # DictionaryBatch
            dt = _table(meta, ho)
            m["dict_id"] = _i64(meta, dt[0]) if 0 in dt else 0
            m["is_delta"] = _u8(meta, dt[2]) if 2 in dt else 0
            m["batch"] = parse_recordbatch(meta, _indirect(meta, dt[1]))
        elif m["header_type"] == 3:  # RecordBatch
            m["batch"] = parse_recordbatch(meta, ho)
    return m


def parse_stream(buf):
    msgs = []
    pos = 0
    while True:
        assert pos + 8 <= len(buf), "truncated stream"
        cont, mlen = struct.unpack_from("<iI", buf, pos)
        assert cont == -1, f"missing continuation marker at {pos}"
        if mlen == 0:
            assert pos + 8 == len(buf), "EOS not at end of stream"
            return msgs
        assert mlen % 8 == 0, f"metadata length {mlen} not 8-aligned"
        meta = buf[pos + 8 : pos + 8 + mlen]
        m = parse_message(meta)
        body_start = pos + 8 + mlen
        m["body"] = buf[body_start : body_start + m["bodyLength"]]
        msgs.append(m)
        pos = body_start + m["bodyLength"]


def check_batch_structural(m, expect_rows, expect_nodes, what):
    rb = m["batch"]
    assert m["version"] == 4, f"{what}: not V5"
    assert rb["codec"] == 1, f"{what}: BodyCompression codec != ZSTD"
    assert rb.get("method", 0) == 0, f"{what}: method != BUFFER"
    assert rb["length"] == expect_rows, f"{what}: length {rb['length']} != {expect_rows}"
    assert len(rb["nodes"]) == expect_nodes, f"{what}: node count"
    body = m["body"]
    assert m["bodyLength"] % 8 == 0, f"{what}: bodyLength not padded"
    for length, null_count in rb["nodes"]:
        assert null_count == 0, f"{what}: null_count != 0"
        assert length == expect_rows, f"{what}: node length"
    end = 0
    for off, ln in rb["buffers"]:
        assert off % 8 == 0, f"{what}: buffer offset {off} not 8-aligned"
        assert off + ln <= len(body), f"{what}: buffer beyond body"
        if ln > 0:
            assert ln >= 9, f"{what}: compressed buffer too short"
            unc = struct.unpack_from("<q", body, off)[0]
            assert unc >= 0, f"{what}: -1 raw escape emitted"
            assert body[off + 8 : off + 12] == b"\x28\xb5\x2f\xfd", f"{what}: no ZSTD magic"
            end = max(end, off + ln)
    assert m["bodyLength"] == (end + 7) & ~7, f"{what}: bodyLength mismatch"


def validate_structural(buf, num_rows):
    msgs = parse_stream(buf)
    assert len(msgs) == 1 + NUM_DICT_COLS + 1, f"expected 15 messages, got {len(msgs)}"
    assert msgs[0]["header_type"] == 1 and msgs[0]["version"] == 4, "first message not V5 schema"
    assert msgs[0]["bodyLength"] == 0, "schema message has a body"
    for k in range(NUM_DICT_COLS):
        m = msgs[1 + k]
        assert m["header_type"] == 2, f"message {1 + k} not a dictionary batch"
        assert m["dict_id"] == k, f"dictionary ids out of order: {m['dict_id']} != {k}"
        assert m["is_delta"] == 0, "isDelta set"
        ndict = m["batch"]["length"]
        assert 1 <= ndict <= num_rows, "dictionary cardinality out of range"
        check_batch_structural(m, ndict, 1, f"dict {k}")
        assert len(m["batch"]["buffers"]) == 3, "dict batch buffer count"
    rb = msgs[1 + NUM_DICT_COLS]
    assert rb["header_type"] == 3, "missing record batch"
    check_batch_structural(rb, num_rows, 56, "record batch")
    assert len(rb["batch"]["buffers"]) == 122, "record batch buffer count"


# ---------------------------------------------------------------------------
# Decoded validation (pyarrow)
# ---------------------------------------------------------------------------


def expected_schema(pa):
    d = pa.dictionary(pa.int32(), pa.utf8())
    f = [
        ("ts", pa.timestamp("ns", tz="UTC")), ("severity", d), ("body", pa.utf8()),
        ("trace_id", pa.utf8()), ("span_id", pa.utf8()), ("query_id", d), ("db_name", d),
        ("db_user", d), ("db_operation", d), ("app", d), ("client_addr", d),
        ("query_text", pa.utf8()), ("pid", pa.utf8()), ("err_message", pa.utf8()),
        ("err_sqlstate", d), ("err_elevel", pa.int32()),
    ]
    f += [(name, pa.uint64()) for name in U64_COLS]
    f += [
        ("parallel_workers_planned", pa.uint32()), ("parallel_workers_launched", pa.uint32()),
        ("instance_ubid", pa.utf8()), ("server_ubid", pa.utf8()), ("server_role", d),
        ("read_replica_type", d), ("region", d), ("cell", d), ("service_version", d),
        ("host_id", pa.utf8()), ("pod_name", pa.utf8()),
    ]
    return pa.schema([pa.field(n, t) for n, t in f])


def validate_decoded(pa, buf, indices, region):
    reader = pa.ipc.open_stream(buf)
    schema = expected_schema(pa)
    assert reader.schema.equals(schema), (
        f"schema mismatch:\n--- got ---\n{reader.schema}\n--- want ---\n{schema}"
    )
    table = reader.read_all()
    assert table.num_rows == len(indices), "row count"

    rows = [expected_row(i, region) for i in indices]
    for name in schema.names:
        want = [r[name] for r in rows]
        col = table.column(name)
        if name == "ts":
            got = col.cast(pa.int64()).to_pylist()
        else:
            got = col.to_pylist()
        assert got == want, f"column {name}: {got[:5]}... != {want[:5]}..."
    # the 13 dictionary-encoded columns must arrive dictionary-encoded
    for name in schema.names:
        if pa.types.is_dictionary(schema.field(name).type):
            chunk = table.column(name).chunk(0)
            assert pa.types.is_dictionary(chunk.type), f"{name} not dictionary-encoded"
            assert chunk.type.value_type.equals(pa.utf8()), f"{name} dict value type"


def main():
    args = sys.argv[1:]
    structural_only = False
    if args and args[0] == "--structural-only":
        structural_only = True
        args = args[1:]
    assert len(args) == 2, "usage: validate.py [--structural-only] payload1 payload2"

    with open(args[0], "rb") as fh:
        p1 = fh.read()
    with open(args[1], "rb") as fh:
        p2 = fh.read()

    validate_structural(p1, 50)
    validate_structural(p2, 5)
    print("structural checks OK")

    if structural_only:
        print("SKIP decoded checks (pyarrow unavailable)")
        return

    import pyarrow as pa

    validate_decoded(pa, p1, list(range(50)), "us-east-1")
    validate_decoded(pa, p2, list(range(100, 105)), "eu-west-1")
    print(f"decoded checks OK (pyarrow {pa.__version__})")


if __name__ == "__main__":
    main()
