---
title: Testing Guide
description: Running regression, TAP, isolation, and stress tests
---

## Test Types

| Type | Description | Prerequisites | Command |
|------|-------------|---------------|---------|
| `regress` | SQL regression tests | None | `mise run test:regress` |
| `tap` | Perl TAP tests (stress, concurrent, lifecycle) | PG built with `-Dtap_tests=enabled` ([see below](#tap-tests)) | `./scripts/run-tests.sh <pg_install> tap` |
| `isolation` | Race condition tests | None | `mise run test:isolation` |
| `stress` | High-load stress test with pgbench | None | `mise run test:stress` |
| `clickhouse` | ClickHouse integration tests | Docker | `mise run test:clickhouse` |
| `all` | Run all tests | None (skips TAP if unavailable) | `mise run test:all` |

## Running Tests

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

## TAP Tests

TAP tests require PostgreSQL compiled with `-Dtap_tests=enabled` (Meson). Mise-installed PostgreSQL versions don't include the Perl TAP modules, so you must build PostgreSQL from source.

### 1. Build PostgreSQL with TAP support

```bash
cd ../postgres
meson setup build_tap --prefix=$(pwd)/install_tap -Dtap_tests=enabled
ninja -C build_tap -j$(nproc)
ninja -C build_tap install
```

### 2. Build pg_stat_ch against it

```bash
cmake -B build -G Ninja -DPG_CONFIG=../postgres/install_tap/bin/pg_config
cmake --build build && cmake --install build
```

### 3. Run TAP tests

```bash
./scripts/run-tests.sh ../postgres/install_tap tap
```

## Test Files

### Regression Tests (`test/regression/sql/`)

| File | Description |
|------|-------------|
| `basic.sql` | Extension CREATE/DROP |
| `version.sql` | Version function |
| `guc.sql` | GUC parameter validation |
| `stats.sql` | Stats function output |
| `utility.sql` | DDL/utility statement tracking |
| `buffers.sql` | Buffer usage tracking |
| `cmd_type.sql` | Command type classification |
| `client_info.sql` | Application name and client address |
| `error_capture.sql` | Error capture via emit_log_hook |

### TAP Tests (`t/`)

| File | Description |
|------|-------------|
| `001_stress_test.pl` | High-load stress test with pgbench |
| `002_concurrent_sessions.pl` | Multiple concurrent sessions |
| `004_basic_lifecycle.pl` | Extension lifecycle |
| `005_settings.pl` | GUC settings verification |
| `006_query_capture.pl` | Query capture via executor hooks |
| `007_utility_tracking.pl` | DDL/utility statement tracking |
| `008_error_capture.pl` | Error capture tests |
| `009_bgworker.pl` | Background worker lifecycle |
| `010_clickhouse_export.pl` | ClickHouse export integration |
| `011_clickhouse_reconnect.pl` | Reconnection after ClickHouse restart |
| `012_timing_accuracy.pl` | Timing measurement accuracy |
| `015_guc_validation.pl` | GUC validation tests |
