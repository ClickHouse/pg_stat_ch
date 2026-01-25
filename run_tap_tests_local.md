# Running TAP Tests with Local PostgreSQL Build

TAP tests require PostgreSQL built with `--enable-tap-tests` (or `-Dtap_tests=enabled` for meson). The mise-installed PostgreSQL doesn't include TAP test modules, so you need a local build.

## Prerequisites

Install the required Perl module:
```bash
sudo pacman -S perl-ipc-run  # Arch Linux
```

## Building PostgreSQL with TAP Tests

From the `postgres` source directory:

```bash
cd ../postgres

# Configure with meson (enable TAP tests)
meson setup build_tap --prefix=$(pwd)/install_tap -Dtap_tests=enabled

# Build and install
ninja -C build_tap -j$(nproc)
ninja -C build_tap install
```

## Building pg_stat_ch Against Local PostgreSQL

```bash
# From pg_stat_ch directory
rm -rf build && \
cmake -B build -G Ninja -DPG_CONFIG=../postgres/install_tap/bin/pg_config && \
cmake --build build && \
cmake --install build
```

Extension files installed to:
- `../postgres/install_tap/lib/pg_stat_ch.so`
- `../postgres/install_tap/share/extension/pg_stat_ch.control`
- `../postgres/install_tap/share/extension/pg_stat_ch--0.1.0.sql`

## Running TAP Tests

### Via Script (Recommended)
```bash
./scripts/run-tests.sh ../postgres/install_tap tap
```

### Manual Method
```bash
rm -rf tmp_check && \
PG_DIR="../postgres/install_tap" && \
export PATH="${PG_DIR}/bin:$PATH" && \
export PERL5LIB="${PG_DIR}/lib/pgxs/src/test/perl:t" && \
export PG_REGRESS="${PG_DIR}/lib/pgxs/src/test/regress/pg_regress" && \
prove -v t/004_basic_lifecycle.pl
```

### Run a single test
```bash
./scripts/run-tests.sh ../postgres/install_tap stress   # Just stress test
```

Or manually:
```bash
rm -rf tmp_check && \
PG_DIR="../postgres/install_tap" && \
PATH="${PG_DIR}/bin:$PATH" \
PERL5LIB="${PG_DIR}/lib/pgxs/src/test/perl:t" \
PG_REGRESS="${PG_DIR}/lib/pgxs/src/test/regress/pg_regress" \
prove -v t/001_stress_test.pl
```

## Key Environment Variables

| Variable | Description |
|----------|-------------|
| `PATH` | Must include PostgreSQL bin directory |
| `PERL5LIB` | Must include the TAP test perl modules |
| `PG_REGRESS` | **Required** - Path to pg_regress binary (for auth config) |
| `PG_TEST_NOCLEAN` | Set to 1 to keep test data directories for debugging |
| `PG_TEST_TIMEOUT_DEFAULT` | Increase timeout for slow hosts (e.g., 300) |

## Test Files

- `t/001_stress_test.pl` - High-load stress test with pgbench
- `t/002_concurrent_sessions.pl` - Multiple concurrent sessions
- `t/003_buffer_overflow.pl` - Buffer overflow handling
- `t/004_basic_lifecycle.pl` - Extension CREATE/DROP lifecycle
- `t/005_settings.pl` - GUC settings verification
- `t/006_query_capture.pl` - Query capture via executor hooks
- `t/007_utility_tracking.pl` - DDL/utility statement tracking

## Troubleshooting

### Tests fail with exit code 255 after "initializing database system"

**Cause:** Missing `PG_REGRESS` environment variable.

The `PostgreSQL::Test::Cluster->init()` method calls `$ENV{PG_REGRESS}` (line 707 of Cluster.pm) to configure authentication, but this variable is normally set by the Makefile.

**Fix:** Set `PG_REGRESS` to point to the pg_regress binary:
```bash
export PG_REGRESS="${PG_DIR}/lib/pgxs/src/test/regress/pg_regress"
```

### "could not create data directory: File exists"

**Cause:** Stale `tmp_check` directory from a previous test run.

**Fix:** Remove the directory before running tests:
```bash
rm -rf tmp_check
```

### IPC::Run not found

**Fix:** Install the Perl module:
```bash
sudo pacman -S perl-ipc-run  # Arch Linux
sudo apt install libipc-run-perl  # Debian/Ubuntu
```
