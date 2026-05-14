---
name: quickstart-validate
description: Validate pg_stat_ch behavior end-to-end against the local quickstart Docker stack (Postgres + ClickHouse). Use when a change affects emitted event content (parent_query_id, cmd_type, err_sqlstate, buffer usage, etc.) and you want to confirm it round-trips through to ClickHouse before pushing — i.e., the kind of verification the TAP test gives you but without needing a TAP-enabled local Postgres build.
---

# Quickstart-based PR validation

Use the quickstart stack (`docker/quickstart/docker-compose.yml`) plus a per-PR validation script to confirm a change reaches ClickHouse with the expected event content.

## Steps

1. **Bring up the quickstart stack** (idempotent):
   ```bash
   ./scripts/quickstart.sh up
   ```
   First run takes ~10–15 min because the postgres container builds `pg_stat_ch` from scratch via vcpkg + cmake. Cached subsequent runs are seconds.

2. **Pick the validation script for the topic under test.** The convention is `scripts/quickstart-validate-<topic>.sh`:
   - `scripts/quickstart-validate-parent-query-id.sh` — exercises parent_query_id linkage and the off-by-one in CaptureLogEvent.
   - …add more as new PRs need them.

   If no script matches the change, write one (see "Adding a new script" below).

3. **Run it**:
   ```bash
   ./scripts/quickstart-validate-<topic>.sh
   ```
   Exits 0 on full pass; per-assertion PASS/FAIL printed to stderr. Output ends with a `Result: N passed, M failed` summary.

4. **On failure, inspect raw events**:
   ```bash
   ./scripts/quickstart.sh ch -q "SELECT * FROM pg_stat_ch.events_raw ORDER BY ts_start DESC LIMIT 20 FORMAT Pretty"
   ./scripts/quickstart.sh pg     # open psql for ad-hoc reproduction
   ```

5. **Tear down when done** (optional — leaving it up speeds up next iteration):
   ```bash
   ./scripts/quickstart.sh down
   ```

## When to use this skill vs alternatives

- **Quickstart-validate**: cheap, fast iteration on event-content semantics. Best for "did my change produce the right rows in ClickHouse?"
- **`./scripts/run-tests.sh 18 regress`**: PG regression suite. Best for SQL-level functionality (does `pg_stat_ch_stats()` return the right shape, does the extension load, etc.). Doesn't exercise the ClickHouse side.
- **`./scripts/run-tests.sh ../postgres/install_tap tap`**: TAP harness, which the CI uses. Most thorough but requires a TAP-enabled local Postgres and a fresh local build. Use before opening the PR, or when iterating on `t/*.pl` files directly.

## Adding a new script

Use `scripts/quickstart-validate-parent-query-id.sh` as a template. Each script should:

1. `ensure_stack_up`: idempotently bring up the quickstart compose if it isn't running.
2. **Set up fixtures** with `pg_exec <<SQL`, dropping anything left over from a prior run so the script is re-runnable.
3. **Reset state** with `TRUNCATE pg_stat_ch.events_raw` on the ClickHouse side and `SELECT pg_stat_ch_reset()` on the PG side.
4. **Drive the queries** that exercise the behavior. Use distinctive table/function names as markers — they survive query normalization where literals don't.
5. **`SELECT pg_stat_ch_flush()`** to force an export instead of waiting on the flush timer.
6. **Wait briefly** (typically <5s) for ClickHouse to receive the batch.
7. **Assert** by querying `pg_stat_ch.events_raw` directly. Self-joins are useful for verifying linkage (e.g. `inner.parent_query_id = outer.query_id`).
8. **Print PASS/FAIL per assertion** and a summary; exit non-zero on any failure.
9. **Drop fixtures** before exiting so leftover state doesn't poison the next run.

Keep each script focused on one PR's worth of behavior; don't try to be a generic regression suite. That's what the TAP and regression harnesses are for.
