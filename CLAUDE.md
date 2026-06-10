# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_stat_ch is a PostgreSQL 16+ extension written in C that captures query execution telemetry via server hooks and exports it to ClickHouse (native protocol) or to any OTLP/HTTP collector. The architecture is:
`hooks (foreground) → shared-memory queue → bgworker exporter → ClickHouse events_raw / OTLP collector`

All aggregation (p50/p95/p99, top queries, errors) happens in ClickHouse via materialized views, not in the extension.

The export path is a from-scratch C rewrite of an earlier C++ implementation, motivated by a production SIGABRT under memory pressure. **`OTEL_REWRITE_DESIGN.md` is the authoritative reference** for the exporter architecture, the preallocated memory model, the OTLP/Arrow wire contracts, and the failure semantics — read it before touching `src/export/` or `src/config/`.

## Dependencies

Third-party C libraries (OpenSSL, lz4, zstd) are managed via **vcpkg** (manifest mode) so release artifacts stay statically linked and version-pinned. The vcpkg submodule lives at `third_party/vcpkg`; the manifest is `vcpkg.json`.

The ClickHouse client is **clickhouse-c** (`https://github.com/ClickHouse/clickhouse-c`), a header-only C library vendored as the `third_party/clickhouse-c` submodule. Its sole `CHC_IMPLEMENTATION` unit is `src/export/clickhouse_c_impl.c`; the exporter includes the headers for declarations only.

Arrow IPC support uses **nanoarrow**, vendored as a checked-in amalgamation (NOT a submodule) under `third_party/nanoarrow/`, with the upstream tag/commit pinned in `third_party/nanoarrow/VERSION`. Never patch the amalgamation files; write-side ZSTD buffer compression and dictionary-batch emission live in `src/export/` on top of it. To upgrade, regenerate from an `apache/arrow-nanoarrow` checkout at the new tag and update `VERSION`:

```bash
python3 ci/scripts/bundle.py --output-dir <dest> --with-ipc --with-flatcc --symbol-namespace PgStatCh
```

**First-time setup:**
```bash
git submodule update --init  # clone vcpkg + clickhouse-c
third_party/vcpkg/bootstrap-vcpkg.sh -disableMetrics
```

mise.toml sets `VCPKG_ROOT` automatically when using `mise run` tasks.

## Build Commands

```bash
mise run build              # Debug build (current pg_config)
mise run build:release      # Release build
mise run build:16           # Build for PostgreSQL 16
mise run build:17           # Build for PostgreSQL 17
mise run build:18           # Build for PostgreSQL 18
mise run build:all          # Build for all PG versions
mise run install            # Install the extension
mise run clean              # Clean build artifacts
```

## Development Commands

```bash
mise run format             # Format code with clang-format
mise run lint               # Run clang-tidy linting
mise run compdb             # Copy compile_commands.json to root (for IDE)
mise run configure          # Configure with CMake (debug)
```

## Testing

```bash
# Via mise (uses PG 18)
mise run test:all           # Run all tests
mise run test:regress       # SQL regression tests
mise run test:tap           # TAP tests (stress, concurrent, overflow)
mise run test:isolation     # Isolation tests (race conditions)

# Via script (specify PG version)
./scripts/run-tests.sh 18 all
./scripts/run-tests.sh 17 regress
```

**Test types:**
- `regress` - SQL regression tests in `test/regression/`
- `tap` - Perl TAP tests in `t/` (stress, concurrent sessions, overflow)
- `isolation` - Race condition tests in `specs/`

**Note:** TAP tests require PostgreSQL built with `--enable-tap-tests`. Mise-installed versions don't include the Perl test modules; the script skips TAP tests gracefully when unavailable.

```bash
# we have a local build of PostgreSQL with tap tests enabled
# so we can run the tap tests against it
./scripts/run-tests.sh ../postgres/install_tap tap
```

## Code Style

- C (gnu17; `C_STANDARD 17` + `C_EXTENSIONS ON` are pinned in CMake) with Google-derived formatting (`.clang-format` — `Language: Cpp` stays because clang-format has no separate C mode)
- Column limit: 100, 2-space indent
- `postgres.h` must be included first in any `.c` file; project headers assume it was already included
- Naming: CamelCase for functions (`PschFooBar`), lower_case for variables, kCamelCase for file-local constants, UPPER_CASE for macros; `static` for internal linkage
- Error handling: error-code returns with goto-style cleanup; every early-exit path releases what it acquired (no RAII exists to save you)
- **No longjmp on export/commit paths:** never call `ereport(ERROR)`/`elog(ERROR)` (or any PG function that can throw) from export or ring-commit code — WARNING/LOG/DEBUG1 only. `ereport(FATAL)` is allowed only in bgworker init paths, where a clean worker restart is the correct outcome. Assume a longjmp may still arrive from elsewhere: reset state on entry and never hold non-reclaimable resources across PG calls.
- **Zero heap allocation on steady-state export paths:** all buffers are preallocated at create/init; allocation failure at init returns an error (never aborts)
- No heap-allocating helpers in shared memory; use Postgres allocators and shmem APIs

## Architecture

**Source files:**
- `src/pg_stat_ch.c` - Main entry point with `_PG_init()` and SQL functions
- `src/export/` - Exporter driver (`stats_exporter.c`), backends (`clickhouse_exporter.c`, `otel_exporter.c`), OTLP protobuf encoder (`otlp_encode.c`), Arrow IPC builder (`arrow_batch.c`); interface contract in `src/export/exporter.h`
- `src/config/` - GUC definitions and the `memory_limit` budget resolution (`memory_budget.h`)
- `include/pg_stat_ch/pg_stat_ch.h` - Public header with version macro and declarations
- `sql/pg_stat_ch--0.1.sql` - SQL function definitions

**Build system:**
- CMake with presets (default=debug, release, release-arm64); `project(LANGUAGES C)` — any `.cc/.cxx/.cpp` under `src/` is a configure-time FATAL_ERROR
- vcpkg manifest mode (`vcpkg.json`, 3 deps: openssl/lz4/zstd) with custom triplets in `triplets/`
- `cmake/FindPostgreSQLServer.cmake` - Finds PostgreSQL via pg_config
- `cmake/CompilerWarnings.cmake` - Strict warning flags
- `cmake/GitVersion.cmake` - Version extraction from git

**Key PostgreSQL hooks to implement** (see `pg_stat_ch.md` for details):
- `shmem_request_hook` / `shmem_startup_hook` - Shared memory setup
- `post_parse_analyze_hook` - Capture queryId early
- `ExecutorStart/Run/Finish/End_hook` - Track execution
- `ProcessUtility_hook` - Handle DDL/utility statements
- `emit_log_hook` - Capture errors

## Version Compatibility

Use `#if PG_VERSION_NUM >= XXXXX` for version-specific code:
- PG 18+: `execute_once` removed from ExecutorRun
- PG 17+: Unified nesting_level, separate block timing
- PG 15+: JIT instrumentation, temp_blk timing

## Versioning

Two independent versions exist:

| Version | Location | Purpose |
|---------|----------|---------|
| Git tag (e.g., `v0.1.5`) | Release workflow | Build/release artifacts |
| `default_version` | `pg_stat_ch.control` | PostgreSQL extension schema |

- **Release workflow** uses `git describe --tags` for artifact naming
- **Extension version** in `.control` must match SQL filename (e.g., `0.1` → `pg_stat_ch--0.1.sql`)
- Only bump `default_version` when SQL interface changes (new functions, types, etc.)
- Schema changes require a migration script: `pg_stat_ch--OLD--NEW.sql`

## Reference Projects

These projects are available in the workspace as references:

- **`../pg_stat_monitor`** - Primary reference for PostgreSQL hook patterns, shared memory management, and query statistics collection. Our hook implementations are based on patterns from this project.
- **`~/s/pg_clickhouse`** - Sibling PG extension that embeds clickhouse-c; reference for the connection setup, `CHC_IMPLEMENTATION` TU, and INSERT packet loop (`src/binary/connection.c`, `insert.c`)
- **`../pg_duckdb`** - Another PostgreSQL extension (C++); occasionally useful for comparing hook usage and build patterns
- **`../postgres`** - PostgreSQL source code for understanding internal APIs
