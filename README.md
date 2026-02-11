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
- [Telemetry Data](#telemetry-data)
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
| `pg_stat_ch.log_min_elevel` | enum | `warning` | Superuser | Minimum error level to capture (debug5..panic). Note: only filters messages that PostgreSQL already emits; PostgreSQL's `log_min_messages` must be set to the same or lower level for messages to reach the hook. |

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

### Schema Reference

For a production-ready ClickHouse schema with comprehensive documentation, see [`docker/init/00-schema.sql`](docker/init/00-schema.sql).

This schema includes:
- Full `events_raw` table with all columns documented (what metrics mean, when values are HIGH/LOW)
- 4 materialized views for common analytics patterns (recent events, query stats, load by app/user, errors)
- Column comments explaining how to interpret each metric
- Example queries for dashboards and debugging

### Start ClickHouse (Docker)

For testing, a Docker Compose setup is provided:

```bash
# Start ClickHouse with schema pre-created
docker compose -f docker/docker-compose.test.yml up -d

# Via mise
mise run clickhouse:start
```

This creates:
- ClickHouse server on ports 19000 (native) and 18123 (HTTP)
- Database `pg_stat_ch` with `events_raw` table and materialized views

### Manual Schema Setup

For production deployments, use the full documented schema:

```bash
clickhouse-client < docker/init/00-schema.sql
```

Or if running your own ClickHouse instance with a minimal schema:

```sql
CREATE DATABASE IF NOT EXISTS pg_stat_ch;

CREATE TABLE IF NOT EXISTS pg_stat_ch.events_raw (
    ts_start DateTime64(6),
    duration_us UInt64,
    db String,
    username String,
    pid Int32,
    query_id Int64,
    cmd_type String,
    rows UInt64,
    query String,

    -- Buffer usage
    shared_blks_hit Int64,
    shared_blks_read Int64,
    shared_blks_dirtied Int64,
    shared_blks_written Int64,
    local_blks_hit Int64,
    local_blks_read Int64,
    local_blks_dirtied Int64,
    local_blks_written Int64,
    temp_blks_read Int64,
    temp_blks_written Int64,

    -- I/O timing (microseconds)
    shared_blk_read_time_us Int64,
    shared_blk_write_time_us Int64,
    local_blk_read_time_us Int64,
    local_blk_write_time_us Int64,
    temp_blk_read_time_us Int64,
    temp_blk_write_time_us Int64,

    -- WAL usage
    wal_records Int64,
    wal_fpi Int64,
    wal_bytes UInt64,

    -- CPU time
    cpu_user_time_us Int64,
    cpu_sys_time_us Int64,

    -- JIT
    jit_functions Int32,
    jit_generation_time_us Int32,
    jit_deform_time_us Int32,
    jit_inlining_time_us Int32,
    jit_optimization_time_us Int32,
    jit_emission_time_us Int32,

    -- Parallel workers
    parallel_workers_planned Int16,
    parallel_workers_launched Int16,

    -- Error info
    err_sqlstate FixedString(5),
    err_elevel UInt8,

    -- Client context
    app String,
    client_addr String
) ENGINE = MergeTree()
ORDER BY (ts_start, db, query_id)
PARTITION BY toYYYYMMDD(ts_start);
```

### Example Queries

```sql
-- Recent slow queries (>100ms)
SELECT ts_start, db, duration_us/1000 AS duration_ms, query
FROM pg_stat_ch.events_raw
WHERE duration_us > 100000
ORDER BY ts_start DESC
LIMIT 20;

-- Query statistics by query_id
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

-- Errors by SQLSTATE
SELECT
    err_sqlstate,
    count() AS errors,
    any(query) AS sample_query
FROM pg_stat_ch.events_raw
WHERE err_elevel >= 21  -- ERROR level and above
GROUP BY err_sqlstate
ORDER BY errors DESC;
```

## Testing

### Test Types

| Type | Description | Command |
|------|-------------|---------|
| `regress` | SQL regression tests | `mise run test:regress` |
| `tap` | Perl TAP tests (stress, concurrent, lifecycle) | `mise run test:tap` |
| `isolation` | Race condition tests | `mise run test:isolation` |
| `stress` | High-load stress test with pgbench | `mise run test:stress` |
| `clickhouse` | ClickHouse integration tests | `mise run test:clickhouse` |
| `all` | Run all tests | `mise run test:all` |

### Running Tests

```bash
# Run all tests (mise)
mise run test:all

# Via script with specific PG version
./scripts/run-tests.sh 18 all
./scripts/run-tests.sh 17 regress

# ClickHouse integration tests (requires Docker)
mise run clickhouse:start
mise run test:clickhouse
mise run clickhouse:stop
```

### TAP Tests with Local PostgreSQL Build

TAP tests require PostgreSQL built with `--enable-tap-tests`. Mise-installed versions don't include TAP modules. To run TAP tests:

1. Build PostgreSQL with TAP support:
   ```bash
   cd ../postgres
   meson setup build_tap --prefix=$(pwd)/install_tap -Dtap_tests=enabled
   ninja -C build_tap -j$(nproc)
   ninja -C build_tap install
   ```

2. Build pg_stat_ch against it:
   ```bash
   cmake -B build -G Ninja -DPG_CONFIG=../postgres/install_tap/bin/pg_config
   cmake --build build && cmake --install build
   ```

3. Run TAP tests:
   ```bash
   ./scripts/run-tests.sh ../postgres/install_tap tap
   ```

### Test Files

**Regression tests** (`test/regression/sql/`):
- `basic.sql` - Extension CREATE/DROP
- `version.sql` - Version function
- `guc.sql` - GUC parameter validation
- `stats.sql` - Stats function output
- `utility.sql` - DDL/utility statement tracking
- `buffers.sql` - Buffer usage tracking
- `cmd_type.sql` - Command type classification
- `client_info.sql` - Application name and client address
- `error_capture.sql` - Error capture via emit_log_hook

**TAP tests** (`t/`):
- `001_stress_test.pl` - High-load stress test with pgbench
- `002_concurrent_sessions.pl` - Multiple concurrent sessions
- `003_buffer_overflow.pl` - Queue overflow handling
- `004_basic_lifecycle.pl` - Extension lifecycle
- `005_settings.pl` - GUC settings verification
- `006_query_capture.pl` - Query capture via executor hooks
- `007_utility_tracking.pl` - DDL/utility statement tracking
- `008_error_capture.pl` - Error capture tests
- `009_bgworker.pl` - Background worker lifecycle
- `010_clickhouse_export.pl` - ClickHouse export integration
- `011_clickhouse_reconnect.pl` - Reconnection after ClickHouse restart
- `012_timing_accuracy.pl` - Timing measurement accuracy
- `013_buffer_metrics.pl` - Buffer usage metrics
- `014_cpu_metrics.pl` - CPU time tracking
- `015_guc_validation.pl` - GUC validation tests

## Telemetry Data

### Captured Metrics

| Category | Metrics |
|----------|---------|
| **Timing** | Start timestamp, duration (μs) |
| **Identity** | Database, user, PID, query ID |
| **Query** | Command type (SELECT/INSERT/UPDATE/DELETE/MERGE/UTILITY), query text |
| **Buffer Usage** | Shared/local/temp blocks: hit, read, dirtied, written |
| **I/O Timing** | Shared/local/temp block read/write time (μs) |
| **WAL Usage** | Records, full page images, bytes |
| **CPU Time** | User time, system time (μs) |
| **JIT** (PG15+) | Functions, generation/deform/inlining/optimization/emission time |
| **Parallel** (PG18+) | Workers planned, workers launched |
| **Errors** | SQLSTATE code, error level |
| **Client** | Application name, client IP address |

### Event Structure

Events are stored in a fixed-size (~2KB) structure in shared memory. This design enables:
- Simple ring buffer math without fragmentation
- Lock-free consumer reads
- Predictable memory usage: `capacity × sizeof(PschEvent)`
- Fast memcpy (~20ns for 2KB on modern CPUs)

## Troubleshooting

### Extension won't load

```
WARNING:  pg_stat_ch must be loaded via shared_preload_libraries
```

Add `shared_preload_libraries = 'pg_stat_ch'` to `postgresql.conf` and restart PostgreSQL.

### Events not appearing in ClickHouse

1. Check connection settings:
   ```sql
   SHOW pg_stat_ch.clickhouse_host;
   SHOW pg_stat_ch.clickhouse_port;
   ```

2. Check stats for errors:
   ```sql
   SELECT * FROM pg_stat_ch_stats();
   ```

3. Check PostgreSQL logs for connection errors.

### High queue usage

If `queue_usage_pct` is consistently high:
- Increase `pg_stat_ch.queue_capacity` (restart required)
- Decrease `pg_stat_ch.flush_interval_ms`
- Increase `pg_stat_ch.batch_max`
- Ensure ClickHouse is healthy and reachable

### Dropped events

Check the `dropped_events` counter:
```sql
SELECT dropped_events FROM pg_stat_ch_stats();
```

Dropped events indicate the queue filled faster than the background worker could export. This is safe (queries continue unaffected) but means some telemetry is lost.

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE.md](LICENSE.md) for the full license text.
