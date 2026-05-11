# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_stat_ch is a PostgreSQL 16+ extension written in C++ that captures query execution telemetry via server hooks and exports it to ClickHouse. The architecture is:
`hooks (foreground) → shared-memory queue → bgworker exporter → ClickHouse events_raw`

All aggregation (p50/p95/p99, top queries, errors) happens in ClickHouse via materialized views, not in the extension.

## Dependencies

Third-party dependencies are managed via **vcpkg** (manifest mode). The vcpkg submodule lives at `third_party/vcpkg`; the manifest is `vcpkg.json`.

**First-time setup:**
```bash
git submodule update --init  # clone vcpkg
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

- C++17 with Google style guide (`.clang-format`)
- Column limit: 100, 2-space indent
- `postgres.h` must be included first in any source file
- Use `extern "C"` blocks for PostgreSQL C ABI compatibility
- Naming: CamelCase for classes/functions, lower_case for variables, kCamelCase for constants
- No STL in shared memory; use Postgres allocators and shmem APIs

## Architecture

**Source files:**
- `src/pg_stat_ch.cc` - Main entry point with `_PG_init()` and SQL functions
- `include/pg_stat_ch/pg_stat_ch.h` - Public header with version macro and declarations
- `sql/pg_stat_ch--0.1.sql` - SQL function definitions

**Build system:**
- CMake with presets (default=debug, release, release-arm64)
- vcpkg manifest mode (`vcpkg.json`) with custom triplets in `triplets/`
- `cmake/FindPostgreSQLServer.cmake` - Finds PostgreSQL via pg_config
- `cmake/FindClickHouseCpp.cmake` - Finds clickhouse-cpp (vcpkg port has no cmake config)
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

## ClickHouse Schema

Committed schema files in `docker/init/` are **immutable once released**. Never modify `00-schema.sql` or previously released migration files. All schema changes must be done via new numbered migration files (e.g., `02-*.sql`). Migrations must work both for upgrades (applied on top of existing schema) and fresh installs (all files run sequentially).

## Reference Projects

When you need to check PostgreSQL internals, pg_stat_monitor patterns, or other reference source code, ask the user to clone the relevant project locally (e.g., `git clone --depth 1 https://github.com/postgres/postgres.git`). Never attempt to access files outside this repository directory.

## Useful Skills

- `/cpp-review` - Review C++ code against Google Style Guide
- `/cpp-naming-check` - Check naming conventions
