# ClickHouse Setup & Schema Guide

## Quick Start (Docker)

From the repo root:

```bash
./scripts/quickstart.sh up
```

This brings up local PostgreSQL + ClickHouse with the `pg_stat_ch` schema preloaded. For endpoints and stack details, see [docker/quickstart/README.md](../docker/quickstart/README.md).

## Manual Setup

For production deployments, apply the canonical schema directly (from the repo root):

```bash
clickhouse-client < docker/init/00-schema.sql
```

The schema file ([`docker/init/00-schema.sql`](/docker/init/00-schema.sql)) is the single source of truth and includes:
- Full `events_raw` table with all columns documented (what metrics mean, when values are HIGH/LOW)
- 4 materialized views for common analytics patterns
- Column comments explaining how to interpret each metric
- Canonical table/materialized-view DDL used by deployments

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

Queries follow a typical workflow: find problems with MVs, then drill into raw events.

### Find Slowest Queries (MV)

Identify worst tail latency from the pre-aggregated `query_stats_5m` view. The `-State`/`-Merge` pattern is how ClickHouse finalizes pre-aggregated columns.

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

### Latency Trend for a Specific Query

After finding a slow `query_id` above, see how its latency changes over time. Impossible with pg_stat_statements since it only stores cumulative aggregates.

```sql
SELECT
    toStartOfFiveMinutes(ts_start) AS bucket,
    count() AS calls,
    quantile(0.95)(duration_us) / 1000 AS p95_ms
FROM pg_stat_ch.events_raw
WHERE query_id = 14460383662181259114  -- from the query above
  AND ts_start > now() - INTERVAL 24 HOUR
GROUP BY bucket
ORDER BY bucket;
```

### Cache Miss Outliers

Find individual executions that read the most from disk.

```sql
SELECT
    ts_start,
    query_id,
    shared_blks_read,
    shared_blks_hit,
    round(100 * shared_blks_read / (shared_blks_hit + shared_blks_read), 2) AS miss_pct,
    duration_us / 1000 AS duration_ms,
    query
FROM pg_stat_ch.events_raw
WHERE shared_blks_read > 100
  AND ts_start > now() - INTERVAL 1 HOUR
ORDER BY shared_blks_read DESC
LIMIT 20;
```

### Errors by SQLSTATE

Find which error types are most frequent. Filters on `err_elevel >= 21` (ERROR and above) to skip warnings.

```sql
SELECT
    err_sqlstate,
    count() AS errors,
    any(query) AS sample_query
FROM pg_stat_ch.events_raw
WHERE err_elevel >= 21
  AND ts_start > now() - INTERVAL 24 HOUR
GROUP BY err_sqlstate
ORDER BY errors DESC;
```

### QPS Over Time (MV)

Time-series throughput from the pre-aggregated view. Each bucket is 5 minutes, so divide by 300 for per-second rate.

```sql
SELECT
    bucket,
    countMerge(calls_state) / 300 AS qps
FROM pg_stat_ch.query_stats_5m
WHERE bucket >= now() - INTERVAL 24 HOUR
GROUP BY bucket
ORDER BY bucket;
```

### Load by Application (MV)

Rank applications by total query time to find the heaviest consumers.

```sql
SELECT
    app,
    countMerge(calls_state) AS total_queries,
    round(sumMerge(duration_sum_state) / 1000000, 2) AS total_seconds,
    round(quantilesTDigestMerge(0.95, 0.99)(duration_q_state)[2] / 1000, 2) AS p99_ms,
    sumMerge(errors_sum_state) AS errors
FROM pg_stat_ch.db_app_user_1m
WHERE bucket >= now() - INTERVAL 24 HOUR
GROUP BY app
ORDER BY total_seconds DESC;
```

### WAL and Full Page Images Over Time

Shows the checkpoint cycle - FPIs spike right after each checkpoint then drop until the next one. This sawtooth pattern is invisible in pg_stat_statements.

```sql
SELECT
    toStartOfMinute(ts_start) AS bucket,
    sum(wal_fpi) AS total_fpi,
    sum(wal_bytes) AS total_wal_bytes
FROM pg_stat_ch.events_raw
WHERE cmd_type IN ('INSERT', 'UPDATE', 'DELETE')
  AND ts_start > now() - INTERVAL 24 HOUR
GROUP BY bucket
ORDER BY bucket;
```

### Dirty Blocks Over Time

Buffer write pressure by block type. Spikes in shared blocks indicate write-heavy batches; non-zero local/temp indicates temp table or work_mem spill activity.

```sql
SELECT
    toStartOfMinute(ts_start) AS bucket,
    sum(shared_blks_dirtied) AS shared_dirtied,
    sum(local_blks_dirtied) AS local_dirtied,
    sum(temp_blks_written) AS temp_written
FROM pg_stat_ch.events_raw
WHERE ts_start > now() - INTERVAL 24 HOUR
GROUP BY bucket
ORDER BY bucket;
```
