---
title: Version compatibility
description: Feature matrix and version-specific behavior for PostgreSQL 16, 17, and 18
---

pg_stat_ch supports PostgreSQL 16, 17, and 18. All versions capture core telemetry. Newer versions expose additional metrics.

## Feature matrix

| Feature | PG 16 | PG 17 | PG 18 |
|---|:---:|:---:|:---:|
| **Core metrics** |
| Query timing (`duration_us`) | Yes | Yes | Yes |
| Row counts | Yes | Yes | Yes |
| Query ID | Yes | Yes | Yes |
| Command type (SELECT/INSERT/UPDATE/DELETE/MERGE/UTILITY) | Yes | Yes | Yes |
| Normalized query text | Yes | Yes | Yes |
| **Buffer usage** |
| Shared blocks (hit/read/dirtied/written) | Yes | Yes | Yes |
| Local blocks (hit/read/dirtied/written) | Yes | Yes | Yes |
| Temp blocks (read/written) | Yes | Yes | Yes |
| **I/O timing** |
| Shared block read/write time | Yes | Yes | Yes |
| Local block read/write time | -- | Yes | Yes |
| Temp block read/write time | Yes | Yes | Yes |
| **WAL** |
| WAL records/FPI/bytes | Yes | Yes | Yes |
| **CPU time** |
| User/system CPU time (`getrusage`) | Yes | Yes | Yes |
| **JIT** |
| JIT function count and compilation times | Yes | Yes | Yes |
| JIT deform time | -- | Yes | Yes |
| **Parallel query** |
| Workers planned/launched | -- | -- | Yes |
| **Client context** |
| Application name, client IP | Yes | Yes | Yes |
| **Error capture** |
| SQLSTATE code, error level, message | Yes | Yes | Yes |
| **Observability** |
| Custom wait event in `pg_stat_activity` | -- | Yes | Yes |

## PostgreSQL 18

### Parallel worker statistics

Fields: `parallel_workers_planned`, `parallel_workers_launched`

Tracks how many parallel workers the optimizer planned vs how many were actually launched. If `launched < planned`, the system may be hitting `max_parallel_workers` limits.

```sql
SELECT query_id, cmd_type,
       parallel_workers_planned,
       parallel_workers_launched,
       parallel_workers_planned - parallel_workers_launched AS workers_missed
FROM pg_stat_ch.events_raw
WHERE parallel_workers_planned > parallel_workers_launched
ORDER BY workers_missed DESC;
```

## PostgreSQL 17

### Separate local block I/O timing

Fields: `local_blk_read_time_us`, `local_blk_write_time_us`

PG 16 only tracked I/O timing for shared buffers. PG 17 adds separate timing for local buffers (temporary tables), enabling analysis of temp table performance.

```sql
SELECT query_id,
       local_blk_read_time_us + local_blk_write_time_us AS local_io_us,
       shared_blk_read_time_us + shared_blk_write_time_us AS shared_io_us
FROM pg_stat_ch.events_raw
WHERE local_blk_read_time_us > 0 OR local_blk_write_time_us > 0;
```

### JIT deform time

Field: `jit_deform_time_us`

Time spent JIT-compiling tuple deform functions. High deform time may indicate queries touching many columns.

```sql
SELECT query_id, jit_functions, jit_deform_time_us,
       jit_generation_time_us + jit_inlining_time_us +
       jit_optimization_time_us + jit_emission_time_us AS other_jit_us
FROM pg_stat_ch.events_raw
WHERE jit_deform_time_us > 1000;  -- > 1ms
```

### Custom wait event name

The background worker appears as `PgStatChExporter` in `pg_stat_activity.wait_event` instead of generic `Extension`. This makes it easier to identify pg_stat_ch activity in monitoring tools.

## Field reference

For the complete list of all fields with types, descriptions, and tuning guidance, see the [events schema reference](/reference/events-schema).
