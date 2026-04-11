---
title: ClickHouse setup
description: Set up the ClickHouse backend for pg_stat_ch
---

## Quick start with Docker

From the repository root:

```bash
./scripts/quickstart.sh up
```

This starts PostgreSQL (with pg_stat_ch pre-loaded) and ClickHouse with the full schema applied. See `docker/quickstart/` for stack details.

To stop the stack:

```bash
./scripts/quickstart.sh down
```

## Production setup

For production, apply the schema directly to your ClickHouse instance:

```bash
clickhouse-client < docker/init/00-schema.sql
```

The schema file ([`docker/init/00-schema.sql`](https://github.com/ClickHouse/pg_stat_ch/blob/main/docker/init/00-schema.sql)) is the single source of truth. It creates:

- The `pg_stat_ch` database
- The `events_raw` table with all columns
- Four materialized views for common analytics

### Configure PostgreSQL to connect

Set the ClickHouse connection parameters in `postgresql.conf`:

```ini
pg_stat_ch.clickhouse_host = 'clickhouse.internal'
pg_stat_ch.clickhouse_port = 9000
pg_stat_ch.clickhouse_user = 'default'
pg_stat_ch.clickhouse_password = 'your-password'
pg_stat_ch.clickhouse_database = 'pg_stat_ch'
```

These parameters require a PostgreSQL restart. See the [configuration reference](/reference/configuration#clickhouse-connection) for all connection options including TLS.

### Verify data flow

After connecting, check that events are being exported:

```sql
-- In PostgreSQL
SELECT * FROM pg_stat_ch_stats();
```

`exported_events` should increase as queries run. If `send_failures` is non-zero, check `last_error_text` for the error message.

```sql
-- In ClickHouse
SELECT count() FROM pg_stat_ch.events_raw;
```

## Schema overview

The `events_raw` table stores one row per query execution with 50+ columns covering timing, buffer usage, WAL, CPU, JIT, errors, and client context.

For the complete column reference, see [events schema](/reference/events-schema).

Four materialized views provide pre-aggregated analytics:

| View | Purpose | Retention |
|---|---|---|
| `events_recent_1h` | Real-time debugging | 1-hour TTL |
| `query_stats_5m` | Query performance dashboards (p95/p99) | Unbounded |
| `db_app_user_1m` | Load by application/user | Unbounded |
| `errors_recent` | Error investigation | 7-day TTL |

For view schemas, query patterns, and the `-State`/`-Merge` aggregation pattern, see [materialized views](/reference/materialized-views).

## Data retention

The `events_raw` table has no TTL by default. To limit storage, add a TTL:

```sql
ALTER TABLE pg_stat_ch.events_raw
MODIFY TTL toDateTime(ts_start) + INTERVAL 30 DAY DELETE;
```

The materialized views with TTL (`events_recent_1h`, `errors_recent`) clean up automatically. For the unbounded views (`query_stats_5m`, `db_app_user_1m`), add TTLs based on your retention needs:

```sql
ALTER TABLE pg_stat_ch.query_stats_5m
MODIFY TTL toDateTime(bucket) + INTERVAL 90 DAY DELETE;
```

## ClickHouse sizing

pg_stat_ch events compress well in ClickHouse. Rough estimates:

| QPS | Events/day | Raw size/day | Compressed/day |
|---|---|---|---|
| 100 | 8.6M | ~39 GB | ~2-4 GB |
| 1,000 | 86M | ~390 GB | ~20-40 GB |
| 10,000 | 864M | ~3.9 TB | ~200-400 GB |

Actual compression depends on query diversity. Workloads with many similar queries compress better due to ClickHouse's column-oriented storage and LZ4 compression.
