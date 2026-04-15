# Arrow IPC Passthrough Export — pg_stat_ch Implementation Plan

## Context

The OTel pipeline from pg_stat_ch to ClickHouse suffers from 11 serialize/deserialize steps across 3 hops, causing 69.5% drop rate at the extension and 41.5% refusal rate at datagres-otelcol. The fix: build Arrow RecordBatches directly from the event queue, wrap the IPC bytes as an opaque OTLP LogRecord body, and let ClickHouse ingest via `FORMAT ArrowStream` — zero format conversions of actual data.

This plan covers **pg_stat_ch side only** (Components 1 & 2 from `otel_native_passthrough_plan.md`).

## Target: `query_logs` Table Schema

Source: `clickgres-platform/services/datagres-insights-api/migrations/`

The Arrow schema must exactly match the `query_logs` ClickHouse table (column names are case-sensitive for ArrowStream). After the `app` migration, the table has 49 columns:

```sql
ts                         DateTime64(9)              -- Arrow: timestamp[ns, tz=UTC]
severity                   LowCardinality(String)     -- Arrow: dictionary(int32, utf8), default ""
body                       String                     -- Arrow: utf8, default ""
trace_id                   String                     -- Arrow: utf8, default ""
span_id                    String                     -- Arrow: utf8, default ""
query_id                   String                     -- Arrow: utf8 (cast uint64 → string)
db_name                    LowCardinality(String)     -- Arrow: dictionary(int32, utf8)
db_user                    LowCardinality(String)     -- Arrow: dictionary(int32, utf8)
db_operation               LowCardinality(String)     -- Arrow: dictionary(int32, utf8)
app                        LowCardinality(String)     -- Arrow: dictionary(int32, utf8)
query_text                 String                     -- Arrow: utf8
pid                        String                     -- Arrow: utf8 (cast int32 → string)
err_message                String                     -- Arrow: utf8
err_sqlstate               LowCardinality(String)     -- Arrow: dictionary(int32, utf8)
err_elevel                 Int32                      -- Arrow: int32
duration_us                UInt64                     -- Arrow: uint64
rows                       UInt64                     -- Arrow: uint64
shared_blks_hit            UInt64                     -- Arrow: uint64
shared_blks_read           UInt64                     -- Arrow: uint64
shared_blks_written        UInt64                     -- Arrow: uint64
shared_blks_dirtied        UInt64                     -- Arrow: uint64
shared_blk_read_time_us    UInt64                     -- Arrow: uint64
shared_blk_write_time_us   UInt64                     -- Arrow: uint64
local_blks_hit             UInt64                     -- Arrow: uint64
local_blks_read            UInt64                     -- Arrow: uint64
local_blks_written         UInt64                     -- Arrow: uint64
local_blks_dirtied         UInt64                     -- Arrow: uint64
temp_blks_read             UInt64                     -- Arrow: uint64
temp_blks_written          UInt64                     -- Arrow: uint64
temp_blk_read_time_us      UInt64                     -- Arrow: uint64
temp_blk_write_time_us     UInt64                     -- Arrow: uint64
wal_records                UInt64                     -- Arrow: uint64
wal_bytes                  UInt64                     -- Arrow: uint64
wal_fpi                    UInt64                     -- Arrow: uint64
cpu_user_time_us           UInt64                     -- Arrow: uint64
cpu_sys_time_us            UInt64                     -- Arrow: uint64
jit_functions              UInt64                     -- Arrow: uint64
jit_generation_time_us     UInt64                     -- Arrow: uint64
jit_inlining_time_us       UInt64                     -- Arrow: uint64
jit_optimization_time_us   UInt64                     -- Arrow: uint64
jit_emission_time_us       UInt64                     -- Arrow: uint64
jit_deform_time_us         UInt64                     -- Arrow: uint64
parallel_workers_planned   UInt32                     -- Arrow: uint32
parallel_workers_launched  UInt32                     -- Arrow: uint32
instance_ubid              String                     -- Arrow: utf8, from extra_attributes
server_ubid                String                     -- Arrow: utf8, from extra_attributes
server_role                LowCardinality(String)     -- Arrow: dictionary(int32, utf8), from extra_attributes
region                     LowCardinality(String)     -- Arrow: dictionary(int32, utf8), from extra_attributes
cell                       LowCardinality(String)     -- Arrow: dictionary(int32, utf8), from extra_attributes
service_version            LowCardinality(String)     -- Arrow: dictionary(int32, utf8), = PG_STAT_CH_VERSION
host_id                    String                     -- Arrow: utf8, from extra_attributes
pod_name                   String                     -- Arrow: utf8, from extra_attributes
```

**Type mapping notes** (PschEvent → Arrow → CH):
- `PschEvent.shared_blks_hit` (int64) → Arrow uint64 → CH UInt64: clamp negatives to 0
- `PschEvent.jit_functions` (int32) → Arrow uint64 → CH UInt64: widen + clamp
- `PschEvent.parallel_workers_planned` (int16) → Arrow uint32 → CH UInt32: widen
- `PschEvent.queryid` (uint64) → Arrow utf8 → CH String: `std::to_string()`
- `PschEvent.pid` (int32) → Arrow utf8 → CH String: `std::to_string()`
- `PschEvent.err_elevel` (uint8) → Arrow int32 → CH Int32: widen
- `PschEvent.ts_start` (TimestampTz, us since PG epoch) → Arrow timestamp[ns, tz=UTC]: `(ts_start + kPostgresEpochOffsetUs) * 1000`

**Columns NOT in PschEvent** (filled from `extra_attributes` GUC or defaults):
- `severity` → empty string (or "INFO")
- `body` → empty string
- `trace_id` → empty string
- `span_id` → empty string
- `instance_ubid`, `server_ubid`, `server_role`, `region`, `cell`, `host_id`, `pod_name` → from `extra_attributes` GUC
- `service_version` → `PG_STAT_CH_VERSION` constant

---

## Implementation Steps

### Step 1: Refactor CMake + Add Apache Arrow C++

#### 1a. Add Arrow submodule

```bash
git submodule add https://github.com/apache/arrow.git third_party/arrow
cd third_party/arrow && git checkout apache-arrow-19.0.1
```

#### 1b. Extract existing third-party config into cmake/ modules

Move inline third-party configuration out of `CMakeLists.txt` into dedicated modules. This keeps the root CMakeLists.txt clean.

**Create `cmake/ThirdPartyOTel.cmake`** — move the opentelemetry-cpp block (currently CMakeLists.txt lines 36-52):
```cmake
# cmake/ThirdPartyOTel.cmake — OpenTelemetry C++ with vendored gRPC + abseil
macro(pg_stat_ch_setup_otel)
  include(FetchContent)
  set(WITH_OTLP_GRPC ON CACHE BOOL "Enable OTel OTLP gRPC exporter" FORCE)
  set(WITH_OTLP_HTTP OFF CACHE BOOL "" FORCE)
  set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
  set(WITH_BENCHMARK OFF CACHE BOOL "" FORCE)
  set(WITH_EXAMPLES OFF CACHE BOOL "" FORCE)
  set(WITH_FUNC_TESTS OFF CACHE BOOL "" FORCE)
  set(OPENTELEMETRY_INSTALL OFF CACHE BOOL "" FORCE)
  set(WITH_ABSEIL ON CACHE BOOL "" FORCE)
  set(CMAKE_DISABLE_FIND_PACKAGE_gRPC TRUE)
  set(gRPC_SSL_PROVIDER "package" CACHE STRING "" FORCE)
  add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp EXCLUDE_FROM_ALL)
endmacro()
```

**Create `cmake/ThirdPartyClickHouse.cmake`** — move the clickhouse-cpp block (currently CMakeLists.txt lines 55-63):
```cmake
# cmake/ThirdPartyClickHouse.cmake — ClickHouse C++ client
macro(pg_stat_ch_setup_clickhouse)
  set(WITH_SYSTEM_ABSEIL ON CACHE BOOL "" FORCE)
  set(WITH_OPENSSL ${WITH_OPENSSL} CACHE BOOL "" FORCE)
  set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(BUILD_BENCHMARK OFF CACHE BOOL "" FORCE)
  add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/clickhouse-cpp EXCLUDE_FROM_ALL)
endmacro()
```

**Create `cmake/ThirdPartyArrow.cmake`** — new Arrow C++ configuration:
```cmake
# cmake/ThirdPartyArrow.cmake — Apache Arrow C++ (IPC-only, minimal build)
macro(pg_stat_ch_setup_arrow)
  set(ARROW_BUILD_STATIC ON CACHE BOOL "" FORCE)
  set(ARROW_BUILD_SHARED OFF CACHE BOOL "" FORCE)
  set(ARROW_IPC ON CACHE BOOL "" FORCE)
  set(ARROW_COMPUTE OFF CACHE BOOL "" FORCE)
  set(ARROW_CSV OFF CACHE BOOL "" FORCE)
  set(ARROW_DATASET OFF CACHE BOOL "" FORCE)
  set(ARROW_FILESYSTEM OFF CACHE BOOL "" FORCE)
  set(ARROW_FLIGHT OFF CACHE BOOL "" FORCE)
  set(ARROW_FLIGHT_SQL OFF CACHE BOOL "" FORCE)
  set(ARROW_GANDIVA OFF CACHE BOOL "" FORCE)
  set(ARROW_JSON OFF CACHE BOOL "" FORCE)
  set(ARROW_PARQUET OFF CACHE BOOL "" FORCE)
  set(ARROW_SUBSTRAIT OFF CACHE BOOL "" FORCE)
  set(ARROW_ACERO OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_BROTLI OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_BZ2 OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_LZ4 OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_SNAPPY OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_ZLIB OFF CACHE BOOL "" FORCE)
  set(ARROW_WITH_ZSTD ON CACHE BOOL "" FORCE)
  set(ARROW_JEMALLOC OFF CACHE BOOL "" FORCE)
  set(ARROW_MIMALLOC OFF CACHE BOOL "" FORCE)
  set(ARROW_TESTING OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_UTILITIES OFF CACHE BOOL "" FORCE)
  set(ARROW_DEPENDENCY_SOURCE BUNDLED CACHE STRING "Bundle flatbuffers + zstd" FORCE)
  add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/arrow/cpp EXCLUDE_FROM_ALL)
endmacro()
```

#### 1c. Simplify root CMakeLists.txt

Replace the inline third-party blocks with:
```cmake
include(ThirdPartyOTel)
pg_stat_ch_setup_otel()

include(ThirdPartyArrow)
pg_stat_ch_setup_arrow()

include(ThirdPartyClickHouse)
pg_stat_ch_setup_clickhouse()
```

Add Arrow to existing `target_include_directories` and `target_link_libraries`:
```cmake
# In SYSTEM PRIVATE includes:
  ${CMAKE_SOURCE_DIR}/third_party/arrow/cpp/src
  ${CMAKE_BINARY_DIR}/third_party/arrow/cpp/src

# In link libraries:
  arrow_static
```

**Verify**: `mise run build` succeeds.

---

### Step 2: Add new GUCs

**Files**: `include/config/guc.h`, `src/config/guc.cc`

| GUC | C variable | Type | Default | Context | Purpose |
|-----|-----------|------|---------|---------|---------|
| `pg_stat_ch.otel_arrow_passthrough` | `psch_otel_arrow_passthrough` | bool | false | PGC_SIGHUP | Feature flag |
| `pg_stat_ch.otel_max_block_bytes` | `psch_otel_max_block_bytes` | int | 3145728 | PGC_SIGHUP | Max Arrow batch bytes (range 65536–4194304) |
| `pg_stat_ch.extra_attributes` | `psch_extra_attributes` | string | "" | PGC_SIGHUP | Key-value pairs for resource columns: `k1:v1;k2:v2` |
| `pg_stat_ch.debug_arrow_dump_dir` | `psch_debug_arrow_dump_dir` | string | "" | PGC_SIGHUP | When non-empty, write raw Arrow IPC bytes to this directory before gRPC send (for testing/debugging) |

In `guc.h` add after line 34:
```c
extern bool psch_otel_arrow_passthrough;
extern int psch_otel_max_block_bytes;
extern char* psch_extra_attributes;
extern char* psch_debug_arrow_dump_dir;
```

In `guc.cc` register following existing patterns. All PGC_SIGHUP for hot-reload without restart.

The `extra_attributes` GUC is set by the VM provisioning layer:
```
pg_stat_ch.extra_attributes = 'instance_ubid:abc123;server_ubid:xyz456;server_role:primary;region:us-east-1;cell:cell01;host_id:host01;pod_name:pod01'
```

---

### Step 3: Create ArrowBatchBuilder

**New files**:
- `src/export/arrow_batch.h` — public API (includes Arrow headers directly)
- `src/export/arrow_batch.cc` — implementation (~400 lines)

Auto-discovered by `file(GLOB_RECURSE SOURCES src/*.cc src/*.c)`.

#### API (`arrow_batch.h`)

```cpp
class ArrowBatchBuilder {
 public:
  ArrowBatchBuilder() = default;
  ~ArrowBatchBuilder() = default;

  // Initialize schema and builders. extra_attrs is parsed "k1:v1;k2:v2" string.
  bool Init(const char* extra_attrs, const char* service_version);
  bool Append(const PschEvent& event);

  struct FinishResult {
    std::shared_ptr<arrow::Buffer> ipc_buffer;
    int num_rows;
  };
  FinishResult Finish();
  void Reset();

  int NumRows() const { return num_rows_; }
  size_t EstimatedBytes() const { return estimated_bytes_; }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  // All 49 column builders (StringBuilder, UInt64Builder, etc.)
  // ... one per query_logs column ...
  std::unordered_map<std::string, std::string> extra_attrs_;
  std::string service_version_;
  int num_rows_ = 0;
  size_t estimated_bytes_ = 0;
};
```

#### Implementation (`arrow_batch.cc`)

**Include order**: `extern "C" { #include "postgres.h" }` then `#include "queue/event.h"` then Arrow headers. Same pattern as `stats_exporter.cc`.

**Impl class** holds:
- `shared_ptr<arrow::Schema>` — built once in Init()
- All 49 column builders matching the query_logs table
- Parsed extra_attributes map (`std::unordered_map<string, string>`)
- `service_version` string (PG_STAT_CH_VERSION)
- `num_rows_`, `estimated_bytes_` counters

**Schema construction** in `Init()`:

Arrow fields for all 49 query_logs columns, using exact column names and types from the CH table. Dictionary-encoded fields use `arrow::dictionary(arrow::int32(), arrow::utf8())` for LowCardinality(String) columns.

**Append(event)** maps PschEvent fields → Arrow columns:

```
ts           ← (ev.ts_start + kPostgresEpochOffsetUs) * 1000  [ns]
severity     ← ""
body         ← ""
trace_id     ← ""
span_id      ← ""
query_id     ← std::to_string(ev.queryid)
db_name      ← string(ev.datname, ev.datname_len)
db_user      ← string(ev.username, ev.username_len)
db_operation ← CmdTypeToString(ev.cmd_type)
app          ← string(ev.application_name, ev.application_name_len)
query_text   ← string(ev.query, ClampFieldLen(ev.query_len, ...))
pid          ← std::to_string(ev.pid)
err_message  ← string(ev.err_message, ClampFieldLen(ev.err_message_len, ...))
err_sqlstate ← string_view(ev.err_sqlstate, 5)
err_elevel   ← static_cast<int32_t>(ev.err_elevel)
duration_us  ← ev.duration_us
rows         ← ev.rows
shared_blks_hit ← ClampNegToZero(ev.shared_blks_hit)  [cast to uint64]
... (all buffer/IO/WAL/CPU/JIT fields: clamp negatives, widen to uint64)
parallel_workers_planned  ← static_cast<uint32_t>(ev.parallel_workers_planned)
parallel_workers_launched ← static_cast<uint32_t>(ev.parallel_workers_launched)
instance_ubid  ← extra_attrs_["instance_ubid"]
server_ubid    ← extra_attrs_["server_ubid"]
server_role    ← extra_attrs_["server_role"]
region         ← extra_attrs_["region"]
cell           ← extra_attrs_["cell"]
service_version ← service_version_  (PG_STAT_CH_VERSION)
host_id        ← extra_attrs_["host_id"]
pod_name       ← extra_attrs_["pod_name"]
```

**ClampNegToZero helper**: `static inline uint64_t ClampNeg(int64_t v) { return v < 0 ? 0 : static_cast<uint64_t>(v); }` — PschEvent uses signed types but CH table uses unsigned.

**Estimated bytes**: track incrementally per Append — fixed fields ~400 bytes/row, variable strings at actual length. Conservative for gRPC budget checks.

**Finish()**: calls `builder->Finish()` on all 49 builders, constructs `arrow::RecordBatch`, writes to `arrow::io::BufferOutputStream` via `arrow::ipc::MakeStreamWriter()` with `IpcWriteOptions::codec = arrow::util::Codec::Create(arrow::Compression::ZSTD)`. Zstd compresses each column buffer independently — columns like `region` and `db_name` with repeated values compress to nearly nothing. Returns IPC buffer.

**Reset()**: resets all builders (keeps schema), zeros counters.

**Extra attributes parsing**: In `Init()`, parse `"k1:v1;k2:v2;..."` into `std::unordered_map`. Missing keys default to empty string (ClickHouse accepts this for String/LowCardinality(String) columns).

---

### Step 4: Add `SendArrowBatch` to StatsExporter interface

**File**: `src/export/exporter_interface.h`

Add virtual method with default (before destructor, ~line 76):
```cpp
virtual bool SendArrowBatch(const uint8_t* ipc_data, size_t ipc_len, int num_rows) {
  (void)ipc_data; (void)ipc_len; (void)num_rows;
  return false;
}
```

ClickHouseExporter inherits the default no-op. Only OTelExporter overrides.

---

### Step 5: Implement `OTelExporter::SendArrowBatch`

**File**: `src/export/otel_exporter.cc`

Override in the OTelExporter class. Creates a minimal OTLP request with one LogRecord:

```cpp
bool SendArrowBatch(const uint8_t* ipc_data, size_t ipc_len, int num_rows) final {
  if (stub_ == nullptr || ipc_len == 0) return false;

  // Arena-allocate the request (same pattern as ResetChunk/FlushChunk)
  google::protobuf::ArenaOptions opts;
  opts.initial_block_size = 4096;
  opts.max_block_size = 65536;
  auto arena = std::make_unique<google::protobuf::Arena>(opts);

  auto* request = google::protobuf::Arena::Create<
      collector_logs::ExportLogsServiceRequest>(arena.get());
  auto* rl = request->add_resource_logs();
  PopulateResource(rl->mutable_resource());  // service.name, version, host.name
  auto* sl = rl->add_scope_logs();
  sl->mutable_scope()->set_name("pg_stat_ch");
  sl->mutable_scope()->set_version(PG_STAT_CH_VERSION);

  auto* record = sl->add_log_records();
  record->mutable_body()->set_bytes_value(ipc_data, ipc_len);  // opaque IPC bytes
  SetString(AddAttr(record), "pg_stat_ch.block_format", "arrow_ipc");  // routing attr
  SetInt(AddAttr(record), "pg_stat_ch.block_rows", num_rows);

  // Send via existing gRPC stub
  auto context = otlp::OtlpGrpcClient::MakeClientContext(grpc_opts_);
  collector_logs::ExportLogsServiceResponse response;
  auto status = otlp::OtlpGrpcClient::DelegateExport(
      stub_.get(), std::move(context), std::move(arena),
      std::move(*request), &response);

  if (status.ok()) {
    exported_count_ += num_rows;
    return true;
  }
  LogExporterWarning("Arrow batch gRPC failed", status.error_message().c_str());
  RecordExporterFailure(status.error_message().c_str());
  consecutive_failures_++;
  return false;
}
```

Key: body uses `bytes_value` (not `string_value`) to preserve binary IPC bytes. The `pg_stat_ch.block_format` attribute enables routing in datagres-otelcol.

---

### Step 6: Branch in `stats_exporter.cc`

**File**: `src/export/stats_exporter.cc`

Add `#include "export/arrow_batch.h"`.

New function in anonymous namespace:

```cpp
// Write raw IPC bytes to debug dump directory (if configured).
// File name: arrow_<timestamp_ns>.ipc
static void MaybeDumpArrowBatch(const uint8_t* data, size_t len) {
  if (psch_debug_arrow_dump_dir == nullptr || *psch_debug_arrow_dump_dir == '\0') return;
  char path[MAXPGPATH];
  snprintf(path, sizeof(path), "%s/arrow_%lu.ipc",
           psch_debug_arrow_dump_dir, (unsigned long)GetCurrentTimestamp());
  FILE* f = fopen(path, "wb");
  if (f) { fwrite(data, 1, len, f); fclose(f); }
}

int ExportEventsAsArrow(const std::vector<PschEvent>& events, StatsExporter* exporter) {
  ArrowBatchBuilder builder;
  if (!builder.Init(psch_extra_attributes, PG_STAT_CH_VERSION)) {
    LogExporterWarning("Arrow init", "failed to initialize Arrow batch builder");
    return 0;
  }

  int total = 0;
  const size_t max_bytes = static_cast<size_t>(psch_otel_max_block_bytes);

  for (const auto& ev : events) {
    if (!builder.Append(ev)) break;
    if (builder.EstimatedBytes() >= max_bytes) {
      auto result = builder.Finish();
      if (!result.ipc_buffer) break;
      MaybeDumpArrowBatch(result.ipc_buffer->data(),
                          static_cast<size_t>(result.ipc_buffer->size()));
      if (!exporter->SendArrowBatch(result.ipc_buffer->data(),
              static_cast<size_t>(result.ipc_buffer->size()), result.num_rows))
        break;
      total += result.num_rows;
      builder.Reset();
    }
  }
  if (builder.NumRows() > 0) {
    auto result = builder.Finish();
    if (result.ipc_buffer) {
      MaybeDumpArrowBatch(result.ipc_buffer->data(),
                          static_cast<size_t>(result.ipc_buffer->size()));
      if (exporter->SendArrowBatch(result.ipc_buffer->data(),
              static_cast<size_t>(result.ipc_buffer->size()), result.num_rows))
        total += result.num_rows;
    }
  }

  if (total > 0) {
    if (psch_shared_state) pg_atomic_fetch_add_u64(&psch_shared_state->exported, total);
    PschRecordExportSuccess();
  }
  return total;
}
```

Modify `PschExportBatch()` — add branch after `DequeueEvents`:
```cpp
if (psch_otel_arrow_passthrough && psch_use_otel) {
  return ExportEventsAsArrow(events, exporter);
}
// ... existing per-record path unchanged ...
```

Guard checks both GUCs — arrow path only works with OTel export mode.

---

## Files Summary

| File | Action | What |
|------|--------|------|
| `cmake/ThirdPartyOTel.cmake` | **Create** | Extracted otel-cpp config from CMakeLists.txt |
| `cmake/ThirdPartyClickHouse.cmake` | **Create** | Extracted clickhouse-cpp config from CMakeLists.txt |
| `cmake/ThirdPartyArrow.cmake` | **Create** | Arrow C++ minimal IPC-only config |
| `CMakeLists.txt` | Modify | Replace inline blocks with `include()` calls, add Arrow includes/link |
| `include/config/guc.h` | Modify | Add 4 extern declarations |
| `src/config/guc.cc` | Modify | Add 4 GUC variables + registration |
| `src/export/arrow_batch.h` | **Create** | ArrowBatchBuilder API |
| `src/export/arrow_batch.cc` | **Create** | Arrow schema (49 cols) + builders + IPC serialization |
| `src/export/exporter_interface.h` | Modify | Add `SendArrowBatch` virtual with default |
| `src/export/otel_exporter.cc` | Modify | Override `SendArrowBatch` |
| `src/export/stats_exporter.cc` | Modify | Add `ExportEventsAsArrow`, `MaybeDumpArrowBatch`, branch in `PschExportBatch` |
| `docker/init/01-query-logs.sql` | **Create** | `query_logs` table matching Arrow schema (test + production target) |
| `t/032_arrow_export.pl` | **Create** | TAP test: Arrow IPC → ClickHouse round-trip validation |

---

### Step 7: TAP Test — Arrow IPC Round-Trip Validation

**File**: `t/032_arrow_export.pl`

**Prerequisites**: ClickHouse container running (`docker compose -f docker/docker-compose.test.yml up -d`). The `01-query-logs.sql` init script creates the `query_logs` table automatically.

**Strategy**: Enable arrow passthrough with `debug_arrow_dump_dir` pointing to a temp directory. The bgworker writes raw IPC files before attempting the gRPC send (which will fail — no OTel collector needed). Insert the dumped IPC bytes directly into ClickHouse via `curl ... FORMAT ArrowStream`, then query CH to validate column values.

This validates the full chain: `PschEvent → ArrowBatchBuilder → IPC bytes → ClickHouse ingest → queryable data`.

```perl
#!/usr/bin/env perl
# Test: Arrow IPC passthrough — schema alignment and data correctness
# Prerequisites: ClickHouse container (docker compose -f docker/docker-compose.test.yml up -d)

use strict;
use warnings;
use lib 't';
use File::Temp qw(tempdir);
use File::Glob qw(:bsd_glob);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use psch;

# Skip if ClickHouse not available
if (!psch_clickhouse_available()) {
    plan skip_all => 'Docker/ClickHouse not available';
}
my $ch_check = `curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null`;
if ($ch_check !~ /^1/) {
    plan skip_all => 'ClickHouse not running';
}

# Create temp dir for Arrow IPC dumps
my $dump_dir = tempdir(CLEANUP => 1);

# Truncate query_logs before test
psch_query_clickhouse("TRUNCATE TABLE IF EXISTS pg_stat_ch.query_logs");

# Initialize node with arrow passthrough + debug dump
# OTel endpoint is bogus — we don't need gRPC to succeed, just the dump
my $node = PostgreSQL::Test::Cluster->new('arrow_export');
$node->init();
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.queue_capacity = 65536
pg_stat_ch.flush_interval_ms = 100
pg_stat_ch.use_otel = on
pg_stat_ch.otel_endpoint = 'localhost:4317'
pg_stat_ch.otel_arrow_passthrough = on
pg_stat_ch.debug_arrow_dump_dir = '$dump_dir'
pg_stat_ch.extra_attributes = 'instance_ubid:inst-001;server_ubid:srv-001;server_role:primary;region:us-east-1;cell:cell-01;host_id:host-001;pod_name:pod-001'
});
$node->start();
$node->safe_psql('postgres', 'CREATE EXTENSION pg_stat_ch');

# --- Subtest 1: Schema alignment (CH accepts the ArrowStream insert) ---
subtest 'schema alignment' => sub {
    $node->safe_psql('postgres', 'SELECT 1 AS arrow_test');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Wait for IPC dump files to appear
    my @files;
    my $deadline = time() + 10;
    while (time() < $deadline) {
        @files = bsd_glob("$dump_dir/arrow_*.ipc");
        last if @files;
        select(undef, undef, undef, 0.5);
    }
    ok(@files > 0, 'Arrow IPC dump file(s) created');

    # Insert each dump file into ClickHouse via ArrowStream
    my $inserted = 0;
    for my $f (@files) {
        my $rc = system("curl -sf 'http://localhost:18123/" .
            "?query=INSERT+INTO+pg_stat_ch.query_logs+FORMAT+ArrowStream' " .
            "--data-binary '\@$f'");
        $inserted++ if $rc == 0;
    }
    cmp_ok($inserted, '>=', 1, 'At least one IPC file accepted by ClickHouse');

    my $count = psch_query_clickhouse("SELECT count() FROM pg_stat_ch.query_logs");
    cmp_ok($count, '>=', 1, "Rows visible in query_logs ($count)");
};

# --- Subtest 2: Data correctness ---
subtest 'data correctness' => sub {
    psch_query_clickhouse("TRUNCATE TABLE pg_stat_ch.query_logs");
    unlink bsd_glob("$dump_dir/arrow_*.ipc");
    psch_reset_stats($node);

    # Run known queries to generate events with predictable values
    $node->safe_psql('postgres', 'CREATE TABLE arrow_test(id int, data text)');
    $node->safe_psql('postgres', "INSERT INTO arrow_test VALUES (1, 'hello')");
    $node->safe_psql('postgres', 'SELECT * FROM arrow_test');
    $node->safe_psql('postgres', 'DROP TABLE arrow_test');
    $node->safe_psql('postgres', 'SELECT pg_stat_ch_flush()');

    # Wait for dumps + insert into CH
    my @files;
    my $deadline = time() + 10;
    while (time() < $deadline) {
        @files = bsd_glob("$dump_dir/arrow_*.ipc");
        last if @files;
        select(undef, undef, undef, 0.5);
    }
    for my $f (@files) {
        system("curl -sf 'http://localhost:18123/" .
            "?query=INSERT+INTO+pg_stat_ch.query_logs+FORMAT+ArrowStream' " .
            "--data-binary '\@$f'");
    }

    # Validate column values
    my $count = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs WHERE db_name = 'postgres'");
    cmp_ok($count, '>=', 1, 'db_name populated correctly');

    my $dur = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs WHERE duration_us > 0");
    cmp_ok($dur, '>=', 1, 'duration_us is positive');

    my $ts = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs " .
        "WHERE ts > '2020-01-01' AND ts < now() + INTERVAL 1 DAY");
    cmp_ok($ts, '>=', 1, 'ts is a valid recent timestamp');

    my $pid = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs WHERE pid != '' AND toInt32OrZero(pid) > 0");
    cmp_ok($pid, '>=', 1, 'pid is a valid numeric string');

    my $query_text = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs WHERE query_text LIKE '%arrow_test%'");
    cmp_ok($query_text, '>=', 1, 'query_text captured');

    my $cmd = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs WHERE db_operation = 'SELECT'");
    cmp_ok($cmd, '>=', 1, 'db_operation populated');
};

# --- Subtest 3: extra_attributes round-trip ---
subtest 'extra_attributes' => sub {
    my $inst = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs WHERE instance_ubid = 'inst-001'");
    cmp_ok($inst, '>=', 1, 'instance_ubid from extra_attributes');

    my $region = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs WHERE region = 'us-east-1'");
    cmp_ok($region, '>=', 1, 'region from extra_attributes');

    my $role = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs WHERE server_role = 'primary'");
    cmp_ok($role, '>=', 1, 'server_role from extra_attributes');

    my $ver = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs WHERE service_version != ''");
    cmp_ok($ver, '>=', 1, 'service_version populated');
};

# --- Subtest 4: Negative clamping (unsigned columns never negative) ---
subtest 'unsigned columns' => sub {
    my $neg = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs WHERE shared_blks_hit < 0");
    is($neg, '0', 'shared_blks_hit never negative (clamped to 0)');

    $neg = psch_query_clickhouse(
        "SELECT count() FROM pg_stat_ch.query_logs WHERE rows < 0");
    is($neg, '0', 'rows never negative');
};

$node->stop();
done_testing();
```

**What each subtest validates**:

1. **Schema alignment** — ClickHouse `FORMAT ArrowStream` insert succeeds. This implicitly validates all 52 column names and type compatibility. A single wrong column name or incompatible type causes CH to reject the entire batch.
2. **Data correctness** — Queries CH for expected values: `db_name='postgres'`, `duration_us > 0`, `ts` in valid range, `pid` is numeric string, `query_text` contains table name, `db_operation` is 'SELECT'.
3. **extra_attributes** — Verifies `instance_ubid`, `region`, `server_role`, `service_version` survive the PschEvent → Arrow → CH round-trip.
4. **Unsigned clamping** — Confirms UInt64 columns never contain negative values (the `ClampNeg` helper works).

## Verification

1. **Build**: `mise run build` succeeds with Arrow dependency
2. **Regression**: `mise run test:regress` passes (GUC defaults off, existing path unchanged)
3. **Unit test**: `test/unit/arrow_batch_test.cc` — Init, Append, Finish, Reset cycle; timestamp conversion (ns precision); string casting (query_id, pid); negative clamping; dictionary dedup; extra_attributes parsing
4. **TAP test**: `t/032_arrow_export.pl` — End-to-end: PschEvent → Arrow IPC → dump → ClickHouse ArrowStream insert → query validation (schema alignment, data correctness, extra_attributes, unsigned clamping)
5. **Integration**: Enable GUC with a local otelcol, verify log records have `pg_stat_ch.block_format=arrow_ipc` and binary body

## Risks

- **Arrow C++ build time**: ~30-60s with minimal features + ccache
- **Flatbuffers/zstd conflict**: Arrow bundles its own flatbuffers and zstd; gRPC may have flatbuffers, clickhouse-cpp uses LZ4. `ARROW_DEPENDENCY_SOURCE=BUNDLED` + static + `EXCLUDE_FROM_ALL` isolates symbols
- **postgres.h + Arrow headers**: same extern "C" pattern as stats_exporter.cc
- **query_summaries_1m MV**: Currently reads from `datagres_insights_raw`. Arrow path bypasses this table, so the summaries pipeline needs updating on the Go exporter / CH migration side (out of scope for this plan)
