# PostgreSQL Version Compatibility

This document details the features available in each PostgreSQL version supported by pg_stat_ch.

## Version Feature Matrix

All versions capture core telemetry (timing, buffer usage, WAL, CPU time, errors, client context). Newer versions add additional instrumentation:

| Feature | PG 16 | PG 17 | PG 18 |
|---------|:-----:|:-----:|:-----:|
| **Core Metrics** |
| Query timing (duration_us) | ✓ | ✓ | ✓ |
| Row counts | ✓ | ✓ | ✓ |
| Query ID | ✓ | ✓ | ✓ |
| Command type (SELECT/INSERT/UPDATE/DELETE/UTILITY) | ✓ | ✓ | ✓ |
| Query text | ✓ | ✓ | ✓ |
| **Buffer Usage** |
| Shared blocks (hit/read/dirtied/written) | ✓ | ✓ | ✓ |
| Local blocks (hit/read/dirtied/written) | ✓ | ✓ | ✓ |
| Temp blocks (read/written) | ✓ | ✓ | ✓ |
| **I/O Timing** |
| Shared block read/write time | ✓ | ✓ | ✓ |
| Local block read/write time | - | ✓ | ✓ |
| Temp block read/write time | ✓ | ✓ | ✓ |
| **WAL Usage** |
| WAL records/FPI/bytes | ✓ | ✓ | ✓ |
| **CPU Time** |
| User/system CPU time (via getrusage) | ✓ | ✓ | ✓ |
| **JIT Instrumentation** |
| JIT function count | ✓ | ✓ | ✓ |
| JIT generation/inlining/optimization/emission time | ✓ | ✓ | ✓ |
| JIT deform time | - | ✓ | ✓ |
| **Parallel Query** |
| Parallel workers planned/launched | - | - | ✓ |
| **Command Types** |
| MERGE command | ✓ | ✓ | ✓ |
| **Client Context** |
| Application name | ✓ | ✓ | ✓ |
| Client IP address | ✓ | ✓ | ✓ |
| **Error Capture** |
| SQLSTATE code | ✓ | ✓ | ✓ |
| Error level | ✓ | ✓ | ✓ |
| **Observability** |
| Custom wait event name in pg_stat_activity | - | ✓ | ✓ |

## Feature Details by Version

### PostgreSQL 18

#### Parallel Worker Statistics

Fields: `parallel_workers_planned`, `parallel_workers_launched`

Tracks how many parallel workers were planned for a query vs how many were actually launched. This helps identify resource contention - if `planned > launched`, the system may be hitting `max_parallel_workers` limits.

```sql
-- Find queries where parallel workers couldn't be launched
SELECT query_id, cmd_type,
       parallel_workers_planned,
       parallel_workers_launched,
       parallel_workers_planned - parallel_workers_launched AS workers_missed
FROM pg_stat_ch.events_raw
WHERE parallel_workers_planned > parallel_workers_launched
ORDER BY workers_missed DESC;
```

#### ExecutorRun Signature Change

The `execute_once` parameter was removed from the ExecutorRun hook in PG18. pg_stat_ch handles this transparently via compile-time version checks.

### PostgreSQL 17

#### Separate Local/Shared Block I/O Timing

Fields: `local_blk_read_time_us`, `local_blk_write_time_us`

PG16 only tracked combined `blk_read_time`/`blk_write_time` for shared buffers. PG17 separates local and shared buffer timing, enabling better analysis of temporary table performance.

```sql
-- Queries with high local buffer I/O (temporary tables)
SELECT query_id,
       local_blk_read_time_us + local_blk_write_time_us AS local_io_us,
       shared_blk_read_time_us + shared_blk_write_time_us AS shared_io_us
FROM pg_stat_ch.events_raw
WHERE local_blk_read_time_us > 0 OR local_blk_write_time_us > 0;
```

#### JIT Deform Time

Field: `jit_deform_time_us`

Time spent JIT-compiling tuple deforming (extracting column values from heap tuples). High deform time may indicate queries touching many columns.

```sql
-- Queries with significant JIT deform overhead
SELECT query_id, jit_functions, jit_deform_time_us,
       jit_generation_time_us + jit_inlining_time_us +
       jit_optimization_time_us + jit_emission_time_us AS other_jit_us
FROM pg_stat_ch.events_raw
WHERE jit_deform_time_us > 1000;  -- > 1ms
```

#### Custom Wait Event Name

The background worker appears as "PgStatChExporter" in `pg_stat_activity.wait_event` instead of generic "Extension". This makes it easier to identify pg_stat_ch activity in monitoring.

#### Efficient Backend Status Lookup

Uses `pgstat_get_beentry_by_proc_number()` for O(1) backend status lookup instead of iterating all backends (internal optimization, no user-visible change).

### PostgreSQL 16 (Baseline)

All core features are available:

- **Buffer Usage**: Tracks all block access patterns (shared, local, temp) with hit/read/dirtied/written counts
- **Shared I/O Timing**: Read/write time for shared buffers (requires `track_io_timing = on`)
- **Temp I/O Timing**: Read/write time for temp blocks
- **WAL Usage**: Records generated, full page images, total bytes
- **CPU Time**: User and system CPU time via `getrusage(RUSAGE_SELF)`
- **JIT Instrumentation**: Function count and timing for generation/inlining/optimization/emission phases
- **MERGE Command**: Full support for SQL MERGE statements (introduced in PG15)
- **Error Capture**: SQLSTATE codes and error levels via `emit_log_hook`
- **Client Context**: Application name and client IP address

## Field Reference

### Timing Fields

| Field | Type | Description |
|-------|------|-------------|
| `ts_start` | DateTime64(6) | Query start timestamp (microsecond precision) |
| `duration_us` | UInt64 | Total execution time in microseconds |

### Identity Fields

| Field | Type | Description |
|-------|------|-------------|
| `db` | String | Database name |
| `username` | String | User name who executed the query |
| `pid` | Int32 | Backend process ID |
| `query_id` | Int64 | Query identifier (hash of normalized query) |
| `cmd_type` | String | Command type: SELECT, INSERT, UPDATE, DELETE, MERGE, UTILITY, NOTHING, UNKNOWN |

### Buffer Usage Fields

| Field | Type | Description |
|-------|------|-------------|
| `shared_blks_hit` | Int64 | Shared buffer cache hits |
| `shared_blks_read` | Int64 | Shared blocks read from disk |
| `shared_blks_dirtied` | Int64 | Shared blocks dirtied by query |
| `shared_blks_written` | Int64 | Shared blocks written to disk |
| `local_blks_hit` | Int64 | Local buffer cache hits (temp tables) |
| `local_blks_read` | Int64 | Local blocks read |
| `local_blks_dirtied` | Int64 | Local blocks dirtied |
| `local_blks_written` | Int64 | Local blocks written |
| `temp_blks_read` | Int64 | Temp blocks read (sorts, hashes spilling to disk) |
| `temp_blks_written` | Int64 | Temp blocks written |

### I/O Timing Fields

Requires `track_io_timing = on` in postgresql.conf.

| Field | Type | PG Version | Description |
|-------|------|------------|-------------|
| `shared_blk_read_time_us` | Int64 | 16+ | Time reading shared blocks (μs) |
| `shared_blk_write_time_us` | Int64 | 16+ | Time writing shared blocks (μs) |
| `local_blk_read_time_us` | Int64 | 17+ | Time reading local blocks (μs) |
| `local_blk_write_time_us` | Int64 | 17+ | Time writing local blocks (μs) |
| `temp_blk_read_time_us` | Int64 | 16+ | Time reading temp blocks (μs) |
| `temp_blk_write_time_us` | Int64 | 16+ | Time writing temp blocks (μs) |

### WAL Usage Fields

| Field | Type | Description |
|-------|------|-------------|
| `wal_records` | Int64 | Number of WAL records generated |
| `wal_fpi` | Int64 | Number of WAL full page images |
| `wal_bytes` | UInt64 | Total WAL bytes generated |

### CPU Time Fields

| Field | Type | Description |
|-------|------|-------------|
| `cpu_user_time_us` | Int64 | User CPU time in microseconds |
| `cpu_sys_time_us` | Int64 | System CPU time in microseconds |

### JIT Fields

JIT compilation is triggered for expensive queries when `jit = on`.

| Field | Type | PG Version | Description |
|-------|------|------------|-------------|
| `jit_functions` | Int32 | 16+ | Number of functions JIT-compiled |
| `jit_generation_time_us` | Int32 | 16+ | Time generating JIT code (μs) |
| `jit_deform_time_us` | Int32 | 17+ | Time JIT-compiling tuple deforming (μs) |
| `jit_inlining_time_us` | Int32 | 16+ | Time inlining functions (μs) |
| `jit_optimization_time_us` | Int32 | 16+ | Time optimizing JIT code (μs) |
| `jit_emission_time_us` | Int32 | 16+ | Time emitting machine code (μs) |

### Parallel Query Fields

| Field | Type | PG Version | Description |
|-------|------|------------|-------------|
| `parallel_workers_planned` | Int16 | 18+ | Workers planned by query optimizer |
| `parallel_workers_launched` | Int16 | 18+ | Workers actually launched |

### Error Fields

| Field | Type | Description |
|-------|------|-------------|
| `err_sqlstate` | FixedString(5) | SQLSTATE error code (e.g., "42P01" for undefined table) |
| `err_elevel` | UInt8 | Error level: 0=success, 19=WARNING, 20=ERROR, 21=FATAL |

### Client Context Fields

| Field | Type | Description |
|-------|------|-------------|
| `app` | String | Application name (from `application_name` GUC) |
| `client_addr` | String | Client IP address |

### Query Text

| Field | Type | Description |
|-------|------|-------------|
| `query` | String | Query text (truncated to 2KB) |
