---
name: quickstart-validate
description: Validate pg_stat_ch behavior end-to-end against the local quickstart Docker stack via the OTel/Arrow export path (with debug_arrow_dump_dir capturing Arrow IPC batches). Use when a change affects emitted event content (parent_query_id, cmd_type, err_sqlstate, buffer usage, etc.) and you want to confirm it round-trips through the production export pathway before pushing — i.e., the kind of verification the TAP test gives you but without needing a TAP-enabled local Postgres build.
---

# Quickstart-based PR validation (OTel/Arrow path)

Drive pg_stat_ch through its production export path (OTel + Arrow IPC) against the local quickstart Postgres container, capture Arrow batches via `debug_arrow_dump_dir`, and assert on the contents with pyarrow.

## Steps

1. **Install `uv`** (one-time, for inline pyarrow):
   ```bash
   brew install uv      # macOS
   pip install uv       # otherwise
   ```

2. **Bring up the OTel/Arrow quickstart**:
   ```bash
   ./scripts/quickstart.sh up                                                # builds image once, ~10-15 min cold
   docker compose -f docker/quickstart/docker-compose.otel.yml up -d --wait  # arrow-dump variant
   ```
   The `docker-compose.otel.yml` postgres container is configured with
   `use_otel=on`, `otel_arrow_passthrough=on`, and
   `debug_arrow_dump_dir=/var/lib/postgresql/arrow-dump` (host-mounted to
   `docker/quickstart/arrow-dump/`).  The `otel_endpoint` points at a
   non-existent collector — the gRPC send fails, but
   `MaybeDumpArrowBatch()` writes the IPC file *before* the send, so we
   still get fully-formed Arrow batches on the host.

3. **Pick the validation script for the topic under test.** Convention:
   - `scripts/quickstart-validate-parent-query-id.sh` — exercises parent_query_id linkage and the off-by-one in CaptureLogEvent.
   - …add more as new PRs need them.

   If none matches, write one (see "Adding a new script" below).

4. **Run it**:
   ```bash
   ./scripts/quickstart-validate-parent-query-id.sh
   ```
   Auto-creates fixtures, runs queries, flushes, parses the Arrow IPC dumps with pyarrow, prints per-assertion PASS/FAIL, exits non-zero on any failure.

5. **On failure, inspect the dumps directly**:
   ```bash
   ls docker/quickstart/arrow-dump/*.ipc
   uv run --with pyarrow python -c "
   import pyarrow.ipc, sys
   with open(sys.argv[1], 'rb') as f:
       t = pyarrow.ipc.open_stream(f).read_all()
   print(t.to_pandas().to_string())
   " docker/quickstart/arrow-dump/<file>.ipc
   ```

6. **Tear down when done** (optional):
   ```bash
   docker compose -f docker/quickstart/docker-compose.otel.yml down
   ```

## Why the OTel/Arrow path and not the native ClickHouse path

`docker/quickstart/docker-compose.yml` (the ClickHouse-native variant)
currently can't be used end-to-end: clickhouse-cpp's LZ4-compressed
block format trips a checksum-mismatch on ClickHouse 26.1 server
(separate pre-existing bug, not pg_stat_ch's fault).  The OTel/Arrow
path uses ZSTD via Arrow IPC and is unaffected, plus it mirrors the
export pathway actually used in production.

## When to use this skill vs alternatives

- **Quickstart-validate** (this skill): cheap, fast iteration on event-content semantics via the production export path.  Best for "did my change produce the right rows downstream?"
- **`./scripts/run-tests.sh 18 regress`**: PG regression suite.  Best for SQL-level functionality (does `pg_stat_ch_stats()` return the right shape, does the extension load, etc.).  Doesn't exercise the export side.
- **`./scripts/run-tests.sh ../postgres/install_tap tap`**: TAP harness, which CI uses.  Most thorough but requires a TAP-enabled local Postgres and a fresh local build.

## Adding a new script

Use `scripts/quickstart-validate-parent-query-id.sh` as a template. Each script should:

1. `ensure_stack_up`: idempotently bring up the OTel/Arrow quickstart compose if it isn't running.
2. **Set up fixtures** with `pg_exec <<SQL`, dropping anything left over from a prior run so the script is re-runnable.
3. **Clear the dump directory** (`rm -f $DUMP_DIR/*.ipc`) and reset state via `SELECT pg_stat_ch_reset()`.
4. **Drive the queries** that exercise the behavior. Use distinctive table/function names as markers — they survive query normalization where literals don't.
5. **`SELECT pg_stat_ch_flush()`** to force an export instead of waiting on the flush timer.
6. **Wait briefly** for `.ipc` files to land in `$DUMP_DIR`.
7. **Parse** with `uv run --with pyarrow` and a Python heredoc that reads each IPC stream and emits one `KEY=VALUE` summary line per assertion result.
8. **Assert** via the `expect` helper.
9. **Drop fixtures** before exiting so leftover state doesn't poison the next run.

Keep each script focused on one PR's worth of behavior; don't try to be a generic regression suite. That's what the TAP and regression harnesses are for.
