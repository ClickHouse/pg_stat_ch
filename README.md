# pg_stat_ch: PostgreSQL Query Telemetry Exporter to ClickHouse

A PostgreSQL extension that captures per-query execution telemetry and exports it to ClickHouse in real-time. Unlike pg_stat_statements which aggregates statistics in PostgreSQL, pg_stat_ch exports **raw events** to ClickHouse where aggregation happens via ClickHouse's powerful analytical engine.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [Hooks](#hooks)
- [Features](#features)
- [Supported Versions](#supported-versions)
- [Building from Source](#building-from-source)
- [Installation](#installation)
- [Configuration](#configuration)
- [SQL API](#sql-api)
- [ClickHouse Setup](#clickhouse-setup)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Overview

pg_stat_ch captures detailed telemetry for every query executed in PostgreSQL and exports it to ClickHouse via a single data pipeline:

```
PostgreSQL Hooks (foreground) → Shared Memory Queue → Background Worker → ClickHouse
```

**Key design principles:**
- **Zero network I/O on query path** - Events are queued in shared memory, not sent synchronously
- **Raw events, not aggregates** - All aggregation (p50/p95/p99, top queries, errors) happens in ClickHouse
- **Bounded memory** - Fixed-size ring buffer with overflow counters; dropped events don't block queries
- **Minimal overhead** - ~5μs p99 overhead per captured statement

## Architecture

### Data Flow

1. **PostgreSQL Hooks** capture query start/end with full instrumentation
2. **Shared Memory Ring Buffer** stores events (MPSC: multi-producer, single-consumer)
3. **Background Worker** dequeues batches and exports to ClickHouse
4. **ClickHouse** stores raw events in `events_raw` table; views/MVs provide aggregates

### Hooks

pg_stat_ch uses PostgreSQL's executor and utility hooks to capture telemetry:

| Hook | Purpose |
|------|---------|
| `ExecutorStart_hook` | Initialize instrumentation, capture start time and CPU baseline |
| `ExecutorRun/Finish_hook` | Track query nesting level |
| `ExecutorEnd_hook` | Extract metrics from QueryDesc, enqueue event |
| `ProcessUtility_hook` | Capture DDL and utility statements (CREATE, COPY, VACUUM, etc.) |
| `emit_log_hook` | Capture errors and warnings (SQLSTATE, error level) |

**Key design decisions:**
- Parallel workers are skipped (metrics aggregated by leader)
- Nesting level tracked to identify top-level vs nested queries
- Hooks chain to previous values for compatibility with other extensions
- Error capture uses deadlock prevention to avoid recursive calls

See [docs/hooks.md](docs/hooks.md) for detailed documentation on each hook, captured metrics, and code examples.

### Source Structure

```
src/
├── pg_stat_ch.cc              # Entry point, _PG_init(), SQL functions
├── config/
│   └── guc.cc                 # GUC variable definitions
├── hooks/
│   └── hooks.cc               # PostgreSQL executor/utility hooks
├── queue/
│   ├── event.h                # Event structure (~2KB fixed size)
│   └── shmem.cc               # Shared memory ring buffer
├── worker/
│   └── bgworker.cc            # Background worker main loop
└── export/
    └── clickhouse_exporter.cc # ClickHouse client integration
```

## Features

- **Full Query Telemetry**: Timing, row counts, buffer usage, WAL usage, CPU time
- **All Statement Types**: DML (SELECT/INSERT/UPDATE/DELETE/MERGE), DDL, utility statements
- **Error Capture**: SQLSTATE codes and error levels via emit_log_hook
- **JIT Instrumentation** (PG15+): Function count, generation/inlining/optimization/emission time
- **Parallel Worker Stats** (PG18+): Planned vs launched workers
- **Client Context**: Application name, client IP address
- **Query Text**: Captured with truncation (2KB max)
- **Graceful Degradation**: Queue overflow drops events with counters; ClickHouse unavailability doesn't block PostgreSQL

## Supported Versions

| PostgreSQL | Status | New Features |
|------------|--------|--------------|
| 18 | Full support | Parallel worker stats (`parallel_workers_planned/launched`) |
| 17 | Full support | JIT deform time, separate local/shared I/O timing, custom wait event name |
| 16 | Full support | Baseline: all core metrics |

See [docs/version-compatibility.md](docs/version-compatibility.md) for detailed feature matrix, field descriptions, and example queries.

## Building from Source

### Prerequisites

- CMake 3.16+
- C++17 compiler (GCC 9+, Clang 10+)
- PostgreSQL 16+ development headers
- [mise](https://mise.jdx.dev/) (recommended) or manual PostgreSQL installation

### Build Commands

Using mise (recommended):

```bash
mise run build              # Debug build (PG 18)
mise run build:release      # Release build
mise run build:16           # Build for PostgreSQL 16
mise run build:17           # Build for PostgreSQL 17
mise run build:18           # Build for PostgreSQL 18
mise run install            # Install the extension
```

Manual build:

```bash
cmake -B build -G Ninja -DPG_CONFIG=/path/to/pg_config
cmake --build build
cmake --install build
```

### Dependencies

The [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) library is vendored as a git submodule and **statically linked** into the extension. This means the final `pg_stat_ch.so` is self-contained with no external runtime dependencies on clickhouse-cpp.

```bash
git submodule update --init --recursive
```

## Installation

### 1. Configure PostgreSQL

Add to `postgresql.conf`:

```ini
shared_preload_libraries = 'pg_stat_ch'

# ClickHouse connection (change for your setup)
pg_stat_ch.clickhouse_host = 'localhost'
pg_stat_ch.clickhouse_port = 9000
pg_stat_ch.clickhouse_database = 'pg_stat_ch'
```

### 2. Restart PostgreSQL

```bash
sudo systemctl restart postgresql
```

### 3. Create the Extension

```sql
CREATE EXTENSION pg_stat_ch;
```

### 4. Verify

```sql
SELECT pg_stat_ch_version();
SELECT * FROM pg_stat_ch_stats();
```

## Configuration

### GUC Variables

| Parameter | Type | Default | Reload | Description |
|-----------|------|---------|--------|-------------|
| `pg_stat_ch.enabled` | bool | `on` | SIGHUP | Enable/disable telemetry collection |
| `pg_stat_ch.clickhouse_host` | string | `localhost` | Restart | ClickHouse server hostname |
| `pg_stat_ch.clickhouse_port` | int | `9000` | Restart | ClickHouse native protocol port |
| `pg_stat_ch.clickhouse_user` | string | `default` | Restart | ClickHouse username |
| `pg_stat_ch.clickhouse_password` | string | `""` | Restart | ClickHouse password |
| `pg_stat_ch.clickhouse_database` | string | `pg_stat_ch` | Restart | ClickHouse database name |
| `pg_stat_ch.queue_capacity` | int | `65536` | Restart | Ring buffer size (must be power of 2) |
| `pg_stat_ch.flush_interval_ms` | int | `1000` | SIGHUP | Export batch interval in milliseconds |
| `pg_stat_ch.batch_max` | int | `10000` | SIGHUP | Maximum events per ClickHouse insert |
| `pg_stat_ch.log_min_elevel` | enum | `warning` | Superuser | Minimum error level to capture (debug5..panic) |

See [Error Level Values](docs/version-compatibility.md#error-fields) for the complete list of error levels and their numeric values in ClickHouse.

## SQL API

### `pg_stat_ch_version()`

Returns the extension version string.

```sql
SELECT pg_stat_ch_version();
-- Returns: '0.1.0' (or git commit hash)
```

### `pg_stat_ch_stats()`

Returns queue and exporter statistics:

```sql
SELECT * FROM pg_stat_ch_stats();
```

| Column | Type | Description |
|--------|------|-------------|
| `enqueued_events` | bigint | Total events added to queue |
| `dropped_events` | bigint | Events dropped due to full queue |
| `exported_events` | bigint | Events successfully sent to ClickHouse |
| `send_failures` | bigint | Failed export attempts |
| `last_success_ts` | timestamptz | Last successful export timestamp |
| `last_error_text` | text | Most recent error message |
| `last_error_ts` | timestamptz | Most recent error timestamp |
| `queue_size` | int | Current events in queue |
| `queue_capacity` | int | Maximum queue capacity |
| `queue_usage_pct` | float | Queue utilization percentage |

### `pg_stat_ch_reset()`

Resets all queue counters to zero.

```sql
SELECT pg_stat_ch_reset();
```

### `pg_stat_ch_flush()`

Triggers an immediate flush of queued events to ClickHouse (signals the background worker).

```sql
SELECT pg_stat_ch_flush();
```

## ClickHouse Setup

```bash
# Start ClickHouse with schema pre-created
docker compose -f docker/docker-compose.test.yml up -d

# Or via mise
mise run clickhouse:start
```

For production, apply the canonical schema directly:

```bash
clickhouse-client < docker/init/00-schema.sql
```

The schema creates `events_raw` plus 4 materialized views for dashboards (recent events, query stats with percentiles, load by app/user, error tracking).

See [docs/clickhouse.md](docs/clickhouse.md) for the full setup guide, schema overview, materialized view explanations, and example queries.

## Testing

```bash
mise run test:all                          # Run all tests
mise run test:regress                      # SQL regression tests only
./scripts/run-tests.sh 18 all             # Specific PG version
./scripts/run-tests.sh ../postgres/install_tap tap  # TAP tests with local PG build
```

See [docs/testing.md](docs/testing.md) for test types, TAP test setup, and a full listing of test files.

## Troubleshooting

Common issues: extension not loading (check `shared_preload_libraries`), events not appearing (check `pg_stat_ch_stats()` for errors), high queue usage or dropped events (tune `pg_stat_ch.queue_capacity`, `pg_stat_ch.flush_interval_ms`, `pg_stat_ch.batch_max`).

See [docs/troubleshooting.md](docs/troubleshooting.md) for detailed solutions.

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE.md](LICENSE.md) for the full license text.
