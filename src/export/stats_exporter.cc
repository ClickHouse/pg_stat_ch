// pg_stat_ch statistics exporter implementation

extern "C" {
#include "postgres.h"

#include "utils/timestamp.h"
}

#include <chrono>
#include <cstdio>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include "config/guc.h"
#include "export/arrow_batch.h"
#include "export/clickhouse_exporter.h"
#include "export/exporter_interface.h"
#include "export/otel_arrow_exporter.h"
#include "export/otel_exporter.h"
#include "export/stats_exporter.h"
#include "queue/event.h"
#include "queue/shmem.h"

namespace {

// PostgreSQL epoch is 2000-01-01, Unix epoch is 1970-01-01
// Difference is 946684800 seconds = 946684800000000 microseconds
constexpr int64_t kPostgresEpochOffsetUs = 946684800000000LL;

// Exponential backoff constants
constexpr int kBaseDelayMs = 1000;           // 1 second
constexpr int kMaxDelayMs = 60000;           // 60 seconds
constexpr int kMaxConsecutiveFailures = 10;  // Cap for exponential growth

// Exporter state - encapsulates all bgworker-local state
struct ExporterState {
  std::unique_ptr<StatsExporter> exporter;
};

// [[clang::no_destroy]] (Clang only — GCC has no equivalent).  Suppresses
// the implicit exit-time destructor on namespace-scope variables that need
// one.  PschExporterShutdown (registered with on_proc_exit) resets
// g_exporter.exporter before the C++ atexit chain ever runs, so the
// implicit ~ExporterState would be a no-op anyway.  Making the suppression
// explicit satisfies -Werror=global-constructors (Clang's
// "namespace-scope variable with non-trivial dtor" diagnostic).
#if __has_cpp_attribute(clang::no_destroy)
#define PSCH_NO_DESTROY [[clang::no_destroy]]
#else
#define PSCH_NO_DESTROY
#endif

// Bgworker-local exporter state (no locking needed).
PSCH_NO_DESTROY ExporterState g_exporter;

// Catch-all for C++ exceptions that escape every explicit barrier in the
// extension.  The default std::terminate calls abort() -> SIGABRT, which the
// postmaster treats as a backend crash and uses to trigger DB-wide crash
// recovery (every connection severed, WAL replayed).  Routing through
// ereport(FATAL) instead gives us a clean proc_exit(1): postmaster respawns
// just our bgworker on its bgw_restart_time.  The explicit catches elsewhere
// in this file still run first when they apply — this handler is only the
// backstop for whatever they miss.
//
// The handler is installed inside PschExporterInit (below) rather than from
// _PG_init: after the C99 port of the plugin layer, _PG_init lives in C and
// cannot call std::set_terminate, and PschExporterInit is the first C++ code
// that runs in the bgworker — the only process that ever executes our C++
// exception-throwing code paths.
[[noreturn]] void PschTerminateHandler() noexcept {
  if (auto eptr = std::current_exception()) {
    try {
      std::rethrow_exception(eptr);
    } catch (const std::bad_alloc&) {
      ereport(FATAL, (errmsg("pg_stat_ch: uncaught std::bad_alloc")));
    } catch (const std::exception& e) {
      ereport(FATAL, (errmsg("pg_stat_ch: uncaught C++ exception: %s", e.what())));
    } catch (...) {
      ereport(FATAL, (errmsg("pg_stat_ch: uncaught non-standard C++ exception")));
    }
  } else {
    ereport(FATAL, (errmsg("pg_stat_ch: std::terminate called without an active exception")));
  }
  pg_unreachable();
}

// Clamp a field length to its buffer maximum, warning on overflow.
template <typename LenT>
LenT ClampFieldLen(LenT len, LenT max, const char* field_name) {
  if (len <= max)
    return len;
  elog(WARNING, "pg_stat_ch: invalid %s %u, clamping", field_name, static_cast<unsigned>(len));
  return max;
}

// Dequeue events from the shared memory queue
std::vector<PschEvent> DequeueEvents(int max_events) {
  std::vector<PschEvent> events;
  events.reserve(max_events);

  PschEvent event;
  while (events.size() < static_cast<size_t>(max_events) && PschDequeueEvent(&event)) {
    events.push_back(event);
  }
  return events;
}

void MaybeDumpArrowBatch(const uint8_t* data, size_t len) {
  if (data == nullptr || len == 0 || psch_debug_arrow_dump_dir == nullptr ||
      *psch_debug_arrow_dump_dir == '\0') {
    return;
  }

  const auto unix_now_ns =
      static_cast<unsigned long long>((GetCurrentTimestamp() + kPostgresEpochOffsetUs) * 1000LL);
  char path[MAXPGPATH];
  char tmp_path[MAXPGPATH];
  snprintf(path, sizeof(path), "%s/arrow_%llu.ipc", psch_debug_arrow_dump_dir, unix_now_ns);
  snprintf(tmp_path, sizeof(tmp_path), "%s/arrow_%llu.ipc.tmp", psch_debug_arrow_dump_dir,
           unix_now_ns);

  FILE* file = fopen(tmp_path, "wb");
  if (file == nullptr) {
    elog(WARNING, "pg_stat_ch: failed to open Arrow dump file '%s'", tmp_path);
    return;
  }

  const size_t written = fwrite(data, 1, len, file);
  fclose(file);
  if (written != len) {
    remove(tmp_path);
    elog(WARNING, "pg_stat_ch: short Arrow dump write to '%s' (%zu/%zu bytes)", tmp_path, written,
         len);
    return;
  }

  if (rename(tmp_path, path) != 0) {
    remove(tmp_path);
    elog(WARNING, "pg_stat_ch: failed to finalize Arrow dump file '%s'", path);
  }
}

int ExportEventsAsArrowInternal(const std::vector<PschEvent>& events, StatsExporter* exporter) {
  ArrowBatchBuilder builder;
  if (!builder.Init(psch_extra_attributes, PG_STAT_CH_VERSION)) {
    LogExporterWarning("Arrow init", "failed to initialize Arrow batch builder");
    RecordExporterFailure("Arrow batch builder init failed");
    return 0;
  }

  const size_t max_block_bytes =
      std::max<size_t>(65536, static_cast<size_t>(psch_otel_max_block_bytes));
  int total_exported = 0;
  bool export_failed = false;

  auto flush_builder = [&]() -> bool {
    ArrowBatchBuilder::FinishResult result = builder.Finish();
    if (result.ipc_buffer == nullptr || result.num_rows <= 0) {
      return false;
    }

    const auto ipc_len = static_cast<size_t>(result.ipc_buffer->size());
    MaybeDumpArrowBatch(result.ipc_buffer->data(), ipc_len);
    // Legacy ArrowBatchBuilder path writes the query_logs_arrow column set;
    // the central OTel collector's routingconnector matches "arrow_ipc" to
    // dispatch to the legacy datagres-arrow-exporter target table.
    if (!exporter->SendArrowBatch(result.ipc_buffer->data(), ipc_len, result.num_rows,
                                  "arrow_ipc")) {
      return false;
    }

    total_exported += result.num_rows;
    return true;
  };

  for (const auto& event : events) {
    if (!builder.Append(event)) {
      export_failed = true;
      break;
    }
    if (builder.EstimatedBytes() >= max_block_bytes) {
      if (!flush_builder()) {
        export_failed = true;
        break;
      }
      builder.Reset();
    }
  }

  if (!export_failed && builder.NumRows() > 0 && !flush_builder()) {
    export_failed = true;
  }

  if (export_failed) {
    RecordExporterFailure("Arrow batch build/send failed");
  }

  // INVARIANT: `builder` (and any other C++ RAII object) is still live below.
  // The PG calls that follow — pg_atomic_fetch_add_u64, PschRecordExportSuccess,
  // RecordExporterFailure — must not longjmp, or builder's Arrow/protobuf/ZSTD
  // allocations leak (heap, not palloc) when destructors are skipped.  None
  // longjmp today (LWLock acquire is not interruptible; atomics are pure),
  // but if you add a PG call here that can longjmp (elog(ERROR), table_open,
  // SPI, etc.), wrap the builder ops above in a nested {} block first so it
  // destructs before this point.
  if (!export_failed && total_exported > 0) {
    if (psch_shared_state != nullptr) {
      pg_atomic_fetch_add_u64(&psch_shared_state->exported, total_exported);
    }
    PschRecordExportSuccess();
  }

  return total_exported;
}

// Exception barrier: Arrow + protobuf + ZSTD allocate via ::operator new and
// throw std::bad_alloc on OOM. Catching here keeps the bgworker from unwinding
// across PostgreSQL's PG_TRY/longjmp frames.
int ExportEventsAsArrow(const std::vector<PschEvent>& events, StatsExporter* exporter) {
  try {
    return ExportEventsAsArrowInternal(events, exporter);
  } catch (const std::bad_alloc&) {
    LogExporterWarning("Arrow export", "out of memory");
    RecordExporterFailure("Arrow export OOM");
    return 0;
  } catch (const std::exception& e) {
    LogExporterWarning("Arrow export exception", e.what());
    RecordExporterFailure(e.what());
    return 0;
  }
}

// Build and export stats (records, metrics, ClickHouse rows) from events
void ExportEventStatsInternal(const std::vector<PschEvent>& events, StatsExporter* exporter) {
  elog(DEBUG1, "pg_stat_ch: ExportEventStatsInternal() called with %zu events", events.size());

  exporter->BeginBatch();

  auto col_ts = exporter->StatTimestamp("ts");
  auto col_duration_us = exporter->DbDurationColumn();
  auto col_db_name = exporter->DbNameColumn();
  auto col_db_user = exporter->DbUserColumn();
  auto col_pid = exporter->StatLCInt32("pid");
  auto col_query_id = exporter->StatHCInt64("query_id");
  auto col_db_operation = exporter->DbOperationColumn();
  auto col_rows = exporter->StatHCUInt64("rows");
  auto col_query_text = exporter->DbQueryTextColumn();

  auto col_shared_blks_hit = exporter->StatHCInt64("shared_blks_hit");
  auto col_shared_blks_read = exporter->StatHCInt64("shared_blks_read");
  auto col_shared_blks_dirtied = exporter->StatHCInt64("shared_blks_dirtied");
  auto col_shared_blks_written = exporter->StatHCInt64("shared_blks_written");
  auto col_local_blks_hit = exporter->StatHCInt64("local_blks_hit");
  auto col_local_blks_read = exporter->StatHCInt64("local_blks_read");
  auto col_local_blks_dirtied = exporter->StatHCInt64("local_blks_dirtied");
  auto col_local_blks_written = exporter->StatHCInt64("local_blks_written");
  auto col_temp_blks_read = exporter->StatHCInt64("temp_blks_read");
  auto col_temp_blks_written = exporter->StatHCInt64("temp_blks_written");

  auto col_shared_blk_read_time_us = exporter->StatHCInt64("shared_blk_read_time_us");
  auto col_shared_blk_write_time_us = exporter->StatHCInt64("shared_blk_write_time_us");
  auto col_local_blk_read_time_us = exporter->StatHCInt64("local_blk_read_time_us");
  auto col_local_blk_write_time_us = exporter->StatHCInt64("local_blk_write_time_us");
  auto col_temp_blk_read_time_us = exporter->StatHCInt64("temp_blk_read_time_us");
  auto col_temp_blk_write_time_us = exporter->StatHCInt64("temp_blk_write_time_us");

  auto col_wal_records = exporter->StatHCInt64("wal_records");
  auto col_wal_fpi = exporter->StatHCInt64("wal_fpi");
  auto col_wal_bytes = exporter->StatHCUInt64("wal_bytes");

  auto col_cpu_user_time_us = exporter->StatHCInt64("cpu_user_time_us");
  auto col_cpu_sys_time_us = exporter->StatHCInt64("cpu_sys_time_us");

  auto col_jit_functions = exporter->StatLCInt32("jit_functions");
  auto col_jit_generation_time_us = exporter->StatLCInt32("jit_generation_time_us");
  auto col_jit_deform_time_us = exporter->StatLCInt32("jit_deform_time_us");
  auto col_jit_inlining_time_us = exporter->StatLCInt32("jit_inlining_time_us");
  auto col_jit_optimization_time_us = exporter->StatLCInt32("jit_optimization_time_us");
  auto col_jit_emission_time_us = exporter->StatLCInt32("jit_emission_time_us");

  auto col_parallel_workers_planned = exporter->StatLCInt16("parallel_workers_planned");
  auto col_parallel_workers_launched = exporter->StatLCInt16("parallel_workers_launched");

  auto col_err_sqlstate = exporter->StatLCString("err_sqlstate");
  auto col_err_elevel = exporter->StatLCUInt8("err_elevel");
  auto col_err_message = exporter->StatHCString("err_message");

  auto col_app = exporter->StatLCString("app");
  auto col_client_addr = exporter->StatHCString("client_addr");

  for (const auto& ev : events) {
    exporter->BeginRow();

    col_ts->Append(ev.ts_start + kPostgresEpochOffsetUs);
    col_duration_us->Append(ev.duration_us);
    col_db_name->Append(std::string(ev.datname, ev.datname_len));
    col_db_user->Append(std::string(ev.username, ev.username_len));
    col_pid->Append(ev.pid);
    col_query_id->Append(static_cast<int64_t>(ev.queryid));
    col_db_operation->Append(PschCmdTypeToString(ev.cmd_type));
    col_rows->Append(ev.rows);

    auto qlen = ClampFieldLen(ev.query_len, static_cast<uint16>(PSCH_MAX_QUERY_LEN), "query_len");
    col_query_text->Append(std::string(ev.query, qlen));

    col_shared_blks_hit->Append(ev.shared_blks_hit);
    col_shared_blks_read->Append(ev.shared_blks_read);
    col_shared_blks_dirtied->Append(ev.shared_blks_dirtied);
    col_shared_blks_written->Append(ev.shared_blks_written);
    col_local_blks_hit->Append(ev.local_blks_hit);
    col_local_blks_read->Append(ev.local_blks_read);
    col_local_blks_dirtied->Append(ev.local_blks_dirtied);
    col_local_blks_written->Append(ev.local_blks_written);
    col_temp_blks_read->Append(ev.temp_blks_read);
    col_temp_blks_written->Append(ev.temp_blks_written);

    col_shared_blk_read_time_us->Append(ev.shared_blk_read_time_us);
    col_shared_blk_write_time_us->Append(ev.shared_blk_write_time_us);
    col_local_blk_read_time_us->Append(ev.local_blk_read_time_us);
    col_local_blk_write_time_us->Append(ev.local_blk_write_time_us);
    col_temp_blk_read_time_us->Append(ev.temp_blk_read_time_us);
    col_temp_blk_write_time_us->Append(ev.temp_blk_write_time_us);

    col_wal_records->Append(ev.wal_records);
    col_wal_fpi->Append(ev.wal_fpi);
    col_wal_bytes->Append(ev.wal_bytes);

    col_cpu_user_time_us->Append(ev.cpu_user_time_us);
    col_cpu_sys_time_us->Append(ev.cpu_sys_time_us);

    col_jit_functions->Append(ev.jit_functions);
    col_jit_generation_time_us->Append(ev.jit_generation_time_us);
    col_jit_deform_time_us->Append(ev.jit_deform_time_us);
    col_jit_inlining_time_us->Append(ev.jit_inlining_time_us);
    col_jit_optimization_time_us->Append(ev.jit_optimization_time_us);
    col_jit_emission_time_us->Append(ev.jit_emission_time_us);

    col_parallel_workers_planned->Append(ev.parallel_workers_planned);
    col_parallel_workers_launched->Append(ev.parallel_workers_launched);

    col_err_sqlstate->Append(
        std::string(ev.err_sqlstate, strnlen(ev.err_sqlstate, sizeof(ev.err_sqlstate) - 1)));
    col_err_elevel->Append(ev.err_elevel);
    auto elen = ClampFieldLen(ev.err_message_len, static_cast<uint16>(PSCH_MAX_ERR_MSG_LEN),
                              "err_message_len");
    col_err_message->Append(std::string(ev.err_message, elen));

    auto alen = ClampFieldLen(ev.application_name_len, static_cast<uint8>(PSCH_MAX_APP_NAME_LEN),
                              "app_name_len");
    auto clen = ClampFieldLen(ev.client_addr_len, static_cast<uint8>(PSCH_MAX_CLIENT_ADDR_LEN),
                              "client_addr_len");
    col_app->Append(std::string(ev.application_name, alen));
    col_client_addr->Append(std::string(ev.client_addr, clen));
  }
  elog(DEBUG1, "pg_stat_ch: finished processing %zu events", events.size());
}

// Exception barrier: protobuf arena, column factories, and per-row appends can
// throw std::bad_alloc. Catching here prevents the throw from crossing the
// bgworker's PG_TRY frame. Returns false on failure so the caller skips
// CommitBatch and avoids flushing partial / stale exporter state.
bool ExportEventStats(const std::vector<PschEvent>& events, StatsExporter* exporter) {
  try {
    ExportEventStatsInternal(events, exporter);
    return true;
  } catch (const std::bad_alloc&) {
    LogExporterWarning("event stats export", "out of memory");
    RecordExporterFailure("event stats OOM");
    return false;
  } catch (const std::exception& e) {
    LogExporterWarning("event stats exception", e.what());
    RecordExporterFailure(e.what());
    return false;
  }
}

}  // namespace

void LogExporterWarning(const char* context, const char* message) {
  ereport(WARNING, errmsg("pg_stat_ch: %s: %s", context, message));
}

void RecordExporterFailure(const char* message) {
  PschRecordExportFailure(message);
}

// Used to report negative values, which are not supported by OTel.
void LogNegativeValue(const std::string& column_name, int64_t value) {
  static std::chrono::steady_clock::time_point last_log = {};
  auto now = std::chrono::steady_clock::now();
  if (now - last_log > std::chrono::seconds(1)) {
    elog(WARNING, "pg_stat_ch: Negative value " INT64_FORMAT " clamped to 0 for column `%s`", value,
         column_name.c_str());
    last_log = now;
  }
}

extern "C" {

// Exception barrier: factory and connection setup allocate via std::make_unique
// and can throw std::bad_alloc.  An uncaught throw would unwind out of this
// extern "C" entry point through PostgreSQL's bgworker startup (which has no
// C++ handler), terminating the process via std::terminate -> SIGABRT.  The
// postmaster would treat that as a backend crash and trigger DB-wide crash
// recovery.  Instead, log FATAL so PostgreSQL records the cause and exits via
// proc_exit; the postmaster respawns just our bgworker on its bgw_restart_time.
// The exporter is the only reason this extension runs, so there is nothing
// useful to do without one — graceful exit is strictly better than limping.
bool PschExporterInit(void) {
  // Install the C++ terminate handler before any other C++ work runs in this
  // process.  See PschTerminateHandler above for the rationale.  Idempotent —
  // a second call in the same process would just reinstall the same handler.
  std::set_terminate(PschTerminateHandler);

  try {
    if (psch_use_otel && psch_otel_arrow_passthrough && psch_use_unified_arrow_exporter) {
      // New unified path: typed Arrow column wrappers driving the
      // StatsExporter interface end-to-end, writing the events_raw schema.
      g_exporter.exporter = MakeUnifiedArrowExporter();
    } else if (psch_use_otel) {
      g_exporter.exporter = MakeOpenTelemetryExporter();
    } else {
      g_exporter.exporter = MakeClickHouseExporter();
    }
    return g_exporter.exporter->EstablishNewConnection();
  } catch (const std::bad_alloc&) {
    ereport(FATAL, (errmsg("pg_stat_ch: out of memory constructing exporter; "
                           "bgworker will be restarted")));
    pg_unreachable();
  } catch (const std::exception& e) {
    ereport(FATAL, (errmsg("pg_stat_ch: exception constructing exporter: %s", e.what())));
    pg_unreachable();
  }
}

// Exception barrier: DequeueEvents reserves a std::vector sized for
// psch_batch_max events and can throw std::bad_alloc. Catching here prevents
// C++ exceptions from escaping this extern "C" entry point or crossing
// PostgreSQL C frames.
int PschExportBatch(void) {
  try {
    elog(DEBUG1, "pg_stat_ch: PschExportBatch() called");
    StatsExporter* exporter = g_exporter.exporter.get();

    if (!exporter->IsConnected()) {
      elog(DEBUG1, "pg_stat_ch: client is null, initializing");
      if (!exporter->EstablishNewConnection()) {
        PschRecordExportFailure("Failed to connect to exporter backend");
        return 0;
      }
    }

    elog(DEBUG1, "pg_stat_ch: dequeuing events (max=%d)", psch_batch_max);
    std::vector<PschEvent> events = DequeueEvents(psch_batch_max);
    if (events.empty()) {
      elog(DEBUG1, "pg_stat_ch: no events to export");
      return 0;
    }

    // Legacy Arrow path bypasses the StatsExporter column interface entirely
    // — ArrowBatchBuilder owns its own column shape, the exporter is reached
    // only for SendArrowBatch (transport).  The new unified exporter goes
    // through the interface like CH-native and OTel column-emission do, so
    // when its GUC is on we skip the bypass and fall through to
    // ExportEventStats.
    if (psch_use_otel && psch_otel_arrow_passthrough && !psch_use_unified_arrow_exporter) {
      elog(DEBUG1, "pg_stat_ch: exporting batch of %zu events as Arrow IPC (legacy)",
           events.size());
      return ExportEventsAsArrow(events, exporter);
    }

    elog(DEBUG1, "pg_stat_ch: exporting batch of %zu events", events.size());
    if (!ExportEventStats(events, exporter)) {
      return 0;
    }

    // INVARIANT: `events` (std::vector<PschEvent>) and any other C++ RAII
    // object in this scope must not be live across a PG call that can longjmp.
    // CommitBatch, pg_atomic_fetch_add_u64, and PschRecordExportSuccess do not
    // longjmp today, but if you add a PG call (e.g. elog(ERROR), table_open)
    // here, narrow the scope of `events` to a nested {} block first or move
    // the call below this point.  A longjmp through these frames skips
    // destructors and leaks the events buffer (heap, not palloc).
    if (exporter->CommitBatch()) {
      if (psch_shared_state != nullptr) {
        pg_atomic_fetch_add_u64(&psch_shared_state->exported, exporter->NumExported());
      }
      PschRecordExportSuccess();
    }

    return exporter->NumExported();
  } catch (const std::bad_alloc&) {
    LogExporterWarning("export batch", "out of memory");
    RecordExporterFailure("export batch OOM");
    return 0;
  } catch (const std::exception& e) {
    LogExporterWarning("export batch exception", e.what());
    RecordExporterFailure(e.what());
    return 0;
  }
}

// Exception barrier: NumConsecutiveFailures / ResetFailures are trivial
// `return int_;` / `member = 0` today, but they are virtual and could grow.
// Match the rest of the C-ABI surface so a future override that allocates
// cannot escape this entry point.
void PschResetRetryState(void) {
  try {
    if (g_exporter.exporter)
      g_exporter.exporter->ResetFailures();
  } catch (const std::exception& e) {
    LogExporterWarning("reset retry state exception", e.what());
  }
}

int PschGetRetryDelayMs(void) {
  try {
    StatsExporter* exporter = g_exporter.exporter.get();
    if (!exporter) {
      return 0;
    }
    int failures = exporter->NumConsecutiveFailures();
    if (failures <= 0) {
      return 0;
    }
    // Exponential backoff: base * 2^(failures-1), capped at max
    int capped_failures = (failures > kMaxConsecutiveFailures) ? kMaxConsecutiveFailures : failures;
    int delay = kBaseDelayMs * (1 << (capped_failures - 1));
    return (delay > kMaxDelayMs) ? kMaxDelayMs : delay;
  } catch (const std::exception& e) {
    LogExporterWarning("retry delay exception", e.what());
    return 0;
  }
}

int PschGetConsecutiveFailures(void) {
  try {
    return g_exporter.exporter ? g_exporter.exporter->NumConsecutiveFailures() : 0;
  } catch (const std::exception& e) {
    LogExporterWarning("get failures exception", e.what());
    return 0;
  }
}

// Exception barrier: the OTel exporter's destructors (gRPC stub teardown,
// protobuf arena release) can throw. Catching here prevents the throw from
// crossing the on_proc_exit chain.
void PschExporterShutdown(void) {
  try {
    g_exporter.exporter.reset();
  } catch (const std::bad_alloc&) {
    LogExporterWarning("exporter shutdown", "out of memory");
  } catch (const std::exception& e) {
    LogExporterWarning("exporter shutdown exception", e.what());
  }
  elog(LOG, "pg_stat_ch: statistics exporter shutdown");
}

}  // extern "C"
