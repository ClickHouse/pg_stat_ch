# PostgreSQL Hooks Architecture

pg_stat_ch uses PostgreSQL's extensibility hooks to capture query telemetry without modifying the database server. This document explains each hook, what data it captures, and how the capture pipeline works.

## Hook Overview

| Hook | Purpose | When Called |
|------|---------|-------------|
| `ExecutorStart_hook` | Initialize instrumentation | Before query execution begins |
| `ExecutorRun_hook` | Track nesting level | During query execution |
| `ExecutorFinish_hook` | Track nesting level | After execution, before cleanup |
| `ExecutorEnd_hook` | Capture metrics and enqueue event | After query completes |
| `ProcessUtility_hook` | Capture DDL/utility statements | For non-optimizable statements |
| `emit_log_hook` | Capture errors and warnings | When PostgreSQL logs a message |

## Executor Hooks

The executor hooks work together to capture metrics for optimizable queries (SELECT, INSERT, UPDATE, DELETE, MERGE).

### ExecutorStart_hook

**Called:** Before the executor begins processing a query.

**What pg_stat_ch does:**

1. Skips parallel workers (to avoid double-counting)
2. Records if this is a top-level query (nesting_level == 0)
3. Captures the query start timestamp
4. Initializes CPU time baseline via `getrusage()`
5. Sets up instrumentation (`InstrAlloc`) to collect buffer/timing stats

### ExecutorRun_hook

**Called:** When the executor actually runs the query plan.

**What pg_stat_ch does:**

1. Increments `nesting_level` to track nested queries
2. Calls the actual executor
3. Decrements `nesting_level` in PG_FINALLY (even on error)

This hook is used purely for nesting level tracking. The actual metric capture happens in `ExecutorEnd_hook`.

### ExecutorFinish_hook

**Called:** After execution completes but before cleanup (handles AFTER triggers).

**What pg_stat_ch does:** Same as ExecutorRun - tracks nesting level only.

### ExecutorEnd_hook

**Called:** After query execution is complete, during cleanup.

**What pg_stat_ch does:**

1. Finalizes instrumentation (`InstrEndLoop`)
2. Computes CPU time delta from `getrusage()`
3. Extracts all metrics from `QueryDesc`
4. Builds a `PschEvent` and enqueues it to shared memory

## Metrics Captured from QueryDesc

The `QueryDesc` structure provides access to all query execution information:

### From `query_desc->totaltime` (Instrumentation)

| Field | Description | Source |
|-------|-------------|--------|
| `total` | Total execution time | `InstrEndLoop()` result |
| `bufusage.shared_blks_hit` | Shared buffer cache hits | Buffer manager |
| `bufusage.shared_blks_read` | Shared blocks read from disk | Buffer manager |
| `bufusage.shared_blks_dirtied` | Shared blocks dirtied | Buffer manager |
| `bufusage.shared_blks_written` | Shared blocks written | Buffer manager |
| `bufusage.local_blks_*` | Local buffer stats | Buffer manager |
| `bufusage.temp_blks_*` | Temp buffer stats | Buffer manager |
| `bufusage.shared_blk_read_time` | Time spent reading (PG17+) | `track_io_timing` |
| `walusage.wal_records` | WAL records generated | WAL writer |
| `walusage.wal_fpi` | Full page images | WAL writer |
| `walusage.wal_bytes` | WAL bytes generated | WAL writer |

### From `query_desc->estate` (Executor State)

| Field | Description | PG Version |
|-------|-------------|------------|
| `es_processed` | Rows affected/returned | All |
| `es_jit->instr` | JIT compilation stats | PG15+ |
| `es_parallel_workers_to_launch` | Planned parallel workers | PG18+ |
| `es_parallel_workers_launched` | Actually launched workers | PG18+ |

### From `query_desc->plannedstmt`

| Field | Description |
|-------|-------------|
| `queryId` | Query fingerprint hash (for grouping similar queries) |

## ProcessUtility_hook

**Called:** For utility (non-optimizable) statements like DDL.

**What pg_stat_ch captures:**

- CREATE/ALTER/DROP (tables, indexes, etc.)
- COPY
- VACUUM, ANALYZE
- GRANT, REVOKE
- SET, SHOW
- Transaction control (BEGIN, COMMIT, ROLLBACK)

**Skipped statements** (to avoid double-counting):
- EXECUTE (prepared statement execution - counted via executor hooks)
- PREPARE (preparation only, not execution)
- DEALLOCATE

## emit_log_hook

**Called:** When PostgreSQL emits a log message (before sending to log destination).

**What pg_stat_ch captures:** Messages at the configured minimum level and above (default: WARNING). The minimum level is controlled by the `pg_stat_ch.log_min_elevel` GUC parameter.

See [version-compatibility.md](version-compatibility.md#error-fields) for the complete list of error levels and their numeric values.

**ErrorData fields captured:**

| Field | Description | Event Field |
|-------|-------------|-------------|
| `sqlerrcode` | SQLSTATE code (packed) | `err_sqlstate` (unpacked to 5-char) |
| `elevel` | Error severity | `err_elevel` |

**Example SQLSTATE codes:**
- `42P01` - Undefined table
- `23505` - Unique violation
- `42601` - Syntax error
- `40001` - Serialization failure

**Deadlock prevention:** The hook sets `disable_error_capture = true` before calling `PschEnqueueEvent()` to prevent recursive calls if enqueueing itself triggers an error.

## Hook Chaining

PostgreSQL hooks use a chaining pattern - each extension saves the previous hook value and calls it. This ensures pg_stat_ch works alongside other extensions like `pg_stat_statements`, `auto_explain`, etc.

## Nesting Level Tracking

Queries can be nested (e.g., triggers, functions calling queries). pg_stat_ch tracks nesting level to:

1. **Identify top-level queries** - Only top-level queries start CPU time tracking
2. **Avoid double-counting** - Nested queries are captured separately

```
nesting_level = 0  ->  Top-level SELECT
nesting_level = 1  ->  Trigger fires INSERT
nesting_level = 2  ->  INSERT trigger calls a function with SELECT
```

The `top_level` flag in events indicates whether the query was top-level.

## Parallel Worker Handling

Parallel workers execute portions of a query plan. pg_stat_ch:

1. **Skips parallel workers** via `IsParallelWorker()` check
2. **Captures aggregate stats** from the leader backend
3. **Reports worker counts** (PG18+) via `es_parallel_workers_*`

## Data Flow Summary

```
┌─────────────────────────────────────────────────────────────────┐
│                     PostgreSQL Backend                          │
├─────────────────────────────────────────────────────────────────┤
│  ExecutorStart_hook                                             │
│    ├─ Record start time, CPU baseline                          │
│    └─ Enable instrumentation                                    │
│                                                                 │
│  ExecutorRun_hook / ExecutorFinish_hook                        │
│    └─ Track nesting level                                       │
│                                                                 │
│  ExecutorEnd_hook                                               │
│    ├─ Finalize instrumentation                                  │
│    ├─ Compute CPU delta                                         │
│    ├─ Extract metrics from QueryDesc                           │
│    └─ Enqueue PschEvent to shared memory                       │
│                                                                 │
│  ProcessUtility_hook                                            │
│    ├─ Capture buffer/WAL/CPU baselines                         │
│    ├─ Execute utility                                           │
│    ├─ Compute deltas                                            │
│    └─ Enqueue PschEvent                                         │
│                                                                 │
│  emit_log_hook                                                  │
│    ├─ Check error level >= configured minimum                  │
│    ├─ Extract SQLSTATE, error level                            │
│    └─ Enqueue PschEvent                                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Shared Memory Ring Buffer                     │
│                     (MPSC: Multi-Producer, Single-Consumer)    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Background Worker                           │
│                  Batch export to ClickHouse                     │
└─────────────────────────────────────────────────────────────────┘
```

## References

- [PostgreSQL Documentation: Writing Hooks](https://www.postgresql.org/docs/current/xfunc-c.html)
- [pg_stat_statements source](https://github.com/postgres/postgres/blob/master/contrib/pg_stat_statements/pg_stat_statements.c)
- [pg_stat_monitor source](https://github.com/percona/pg_stat_monitor)
