# ClickHouse Setup & Schema Guide

## Quick Start (Docker)

```bash
docker compose -f docker/docker-compose.test.yml up -d

# Or via mise
mise run clickhouse:start
```

This creates a ClickHouse server on ports 19000 (native) and 18123 (HTTP) with the `pg_stat_ch` database, `events_raw` table, and all materialized views pre-created.

## Manual Setup

For production deployments, apply the canonical schema directly:

```bash
clickhouse-client < docker/init/00-schema.sql
```

The schema file ([`docker/init/00-schema.sql`](../docker/init/00-schema.sql)) is the single source of truth and includes:
- Full `events_raw` table with all columns documented (what metrics mean, when values are HIGH/LOW)
- 4 materialized views for common analytics patterns
- Column comments explaining how to interpret each metric
- Example queries for dashboards and debugging

## Schema Overview

### `events_raw` Table

The primary table stores one row per query execution. Events are exported in batches by the pg_stat_ch background worker. Key column groups:

| Category | Columns | Notes |
|----------|---------|-------|
| **Identity & Timing** | `ts_start`, `duration_us`, `db`, `username`, `pid`, `query_id` | Core fields for every event |
| **Query** | `cmd_type`, `rows`, `query` | Command classification and text |
| **Shared Buffers** | `shared_blks_hit/read/dirtied/written` | Cache hit ratio = hit / (hit + read) |
| **Local Buffers** | `local_blks_hit/read/dirtied/written` | Temp table I/O |
| **Temp Files** | `temp_blks_read/written` | Non-zero = work_mem pressure |
| **I/O Timing** | `shared/local/temp_blk_read/write_time_us` | Requires `track_io_timing=on` |
| **WAL** | `wal_records`, `wal_fpi`, `wal_bytes` | Write-ahead log metrics |
| **CPU** | `cpu_user_time_us`, `cpu_sys_time_us` | User vs kernel time |
| **JIT** (PG15+) | `jit_functions`, `jit_*_time_us` | JIT compilation overhead |
| **Parallel** (PG18+) | `parallel_workers_planned/launched` | Worker efficiency |
| **Errors** | `err_sqlstate`, `err_elevel`, `err_message` | Error tracking |
| **Client** | `app`, `client_addr` | Load attribution |

See the schema file for detailed COMMENT annotations on each column explaining what HIGH/LOW values mean and tuning guidance.

### Materialized Views

The schema includes 4 materialized views. All aggregation happens in ClickHouse, not in PostgreSQL.

#### 1. `events_recent_1h` — Real-time Debugging

A copy of `events_raw` with a 1-hour TTL for fast access to recent events.

**Use cases:** Real-time dashboards, "what just happened?" debugging, sub-second refresh monitoring.

#### 2. `query_stats_5m` — Query Performance Dashboard

Pre-aggregated query statistics in 5-minute buckets using ClickHouse AggregateFunction columns.

**Use cases:** QPS trends, latency percentiles (p95/p99), identifying slow queries, capacity planning.

**Querying aggregate states:** This MV uses `-State` / `-Merge` functions:

```sql
SELECT
    query_id,
    cmd_type,
    countMerge(calls_state) AS calls,
    round(sumMerge(duration_sum_state) / countMerge(calls_state) / 1000, 2) AS avg_ms,
    round(quantilesTDigestMerge(0.95, 0.99)(duration_q_state)[1] / 1000, 2) AS p95_ms,
    round(quantilesTDigestMerge(0.95, 0.99)(duration_q_state)[2] / 1000, 2) AS p99_ms
FROM pg_stat_ch.query_stats_5m
WHERE bucket >= now() - INTERVAL 1 HOUR
GROUP BY query_id, cmd_type
ORDER BY p99_ms DESC
LIMIT 10;
```

#### 3. `db_app_user_1m` — Load by Application/User

1-minute buckets grouped by database, application, and user with error counts.

**Use cases:** Identifying load sources, per-tenant chargeback, spotting misbehaving applications.

#### 4. `errors_recent` — Error Investigation

Recent errors with a 7-day TTL, filtered from `events_raw` where `err_elevel > 0`.

**Use cases:** Incident investigation, error rate monitoring, recurring error patterns.

## Example Queries

### Recent Slow Queries (>100ms)

```sql
SELECT ts_start, db, duration_us/1000 AS duration_ms, query
FROM pg_stat_ch.events_raw
WHERE duration_us > 100000
ORDER BY ts_start DESC
LIMIT 20;
```

### Query Statistics by query_id

```sql
SELECT
    query_id,
    cmd_type,
    count() AS calls,
    avg(duration_us) AS avg_us,
    quantile(0.95)(duration_us) AS p95_us,
    sum(rows) AS total_rows
FROM pg_stat_ch.events_raw
WHERE ts_start > now() - INTERVAL 1 HOUR
GROUP BY query_id, cmd_type
ORDER BY p95_us DESC;
```

### Errors by SQLSTATE

```sql
SELECT
    err_sqlstate,
    count() AS errors,
    any(query) AS sample_query
FROM pg_stat_ch.events_raw
WHERE err_elevel >= 21  -- ERROR level and above
GROUP BY err_sqlstate
ORDER BY errors DESC;
```

### Cache Hit Ratio by Query

```sql
SELECT
    query_id,
    sumMerge(shared_hit_sum_state) AS hits,
    sumMerge(shared_read_sum_state) AS reads,
    round(100 * sumMerge(shared_hit_sum_state) /
          (sumMerge(shared_hit_sum_state) + sumMerge(shared_read_sum_state) + 0.001), 2) AS hit_pct
FROM pg_stat_ch.query_stats_5m
WHERE bucket >= now() - INTERVAL 1 HOUR
GROUP BY query_id
HAVING reads > 1000
ORDER BY hit_pct ASC
LIMIT 20;
```

### QPS Over Time

```sql
SELECT
    bucket,
    countMerge(calls_state) / 300 AS qps  -- 300 seconds = 5 minutes
FROM pg_stat_ch.query_stats_5m
WHERE bucket >= now() - INTERVAL 24 HOUR
GROUP BY bucket
ORDER BY bucket;
```

### Load by Application

```sql
SELECT
    app,
    countMerge(calls_state) AS total_queries,
    round(sumMerge(duration_sum_state) / 1000000, 2) AS total_seconds,
    round(quantilesTDigestMerge(0.95, 0.99)(duration_q_state)[2] / 1000, 2) AS p99_ms,
    sumMerge(errors_sum_state) AS errors
FROM pg_stat_ch.db_app_user_1m
WHERE bucket >= now() - INTERVAL 30 MINUTE
GROUP BY app
ORDER BY total_seconds DESC;
```

### JIT Overhead Analysis

```sql
SELECT
    query_id,
    count() AS calls,
    avg(jit_functions) AS avg_functions,
    avg(jit_generation_time_us + jit_inlining_time_us + jit_optimization_time_us + jit_emission_time_us) AS avg_jit_us,
    avg(duration_us) AS avg_duration_us,
    round(100 * avg(jit_generation_time_us + jit_inlining_time_us + jit_optimization_time_us + jit_emission_time_us) / avg(duration_us), 1) AS jit_overhead_pct
FROM pg_stat_ch.events_raw
WHERE jit_functions > 0
  AND ts_start > now() - INTERVAL 1 HOUR
GROUP BY query_id
HAVING calls >= 10
ORDER BY jit_overhead_pct DESC
LIMIT 20;
```

For more example queries, see the comments in [`docker/init/00-schema.sql`](../docker/init/00-schema.sql).
