// pg_stat_ch exporter library entry points

#include "export/psch_exporter.h"

#include <algorithm>
#include <exception>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>

#include "export/arrow_batch.h"
#include "export/arrow_dump.h"
#include "export/clickhouse_exporter.h"
#include "export/diagnostics.h"
#include "export/exporter_config.h"
#include "export/exporter_interface.h"
#include "export/otel_arrow_exporter.h"
#include "export/otel_exporter.h"

namespace {

std::unique_ptr<StatsExporter> MakeBackend(const ExporterConfig* config, Diagnostics* diag) {
  switch (config->mode) {
    case PSCH_EXPORTER_CLICKHOUSE:
      return MakeClickHouseExporter(config, diag);
    case PSCH_EXPORTER_OTEL:
    case PSCH_EXPORTER_OTEL_ARROW_LEGACY:
      return MakeOpenTelemetryExporter(config, diag);
    case PSCH_EXPORTER_OTEL_ARROW_UNIFIED:
      return MakeUnifiedArrowExporter(config, diag);
  }
  return nullptr;
}

// Legacy Arrow shares OTelExporter transport with OTLP log records, so those
// two modes may swap at runtime. Every other change needs a new backend
bool SameBackend(PschExporterMode a, PschExporterMode b) {
  auto backend = [](PschExporterMode m) {
    return m == PSCH_EXPORTER_OTEL_ARROW_LEGACY ? PSCH_EXPORTER_OTEL : m;
  };
  return backend(a) == backend(b);
}

std::string_view View(PschExportString s) {
  return {s.data, s.len};
}

const char* ValidateEvents(const PschExportEvent* events, size_t count) {
  if (count > 0 && events == nullptr)
    return "events is NULL";
  for (size_t i = 0; i < count; ++i) {
    const PschExportEvent& ev = events[i];
    const struct {
      PschExportString s;
      uint32_t max;
    } strings[] = {
        {ev.db_name, PSCH_EXPORT_NAME_MAX},          {ev.db_user, PSCH_EXPORT_NAME_MAX},
        {ev.db_operation, PSCH_EXPORT_NAME_MAX},     {ev.query_text, PSCH_EXPORT_QUERY_MAX},
        {ev.app_name, PSCH_EXPORT_NAME_MAX},         {ev.client_addr, PSCH_EXPORT_CLIENT_ADDR_MAX},
        {ev.err_sqlstate, PSCH_EXPORT_SQLSTATE_MAX}, {ev.err_message, PSCH_EXPORT_ERR_MESSAGE_MAX},
    };
    for (const auto& [s, max] : strings) {
      if (s.len > max || (s.len > 0 && s.data == nullptr))
        return "event string violates length contract";
    }
  }
  return nullptr;
}

}  // namespace

struct PschExporter {
  explicit PschExporter(const PschExporterConfig& c)
      : config(c), backend(MakeBackend(&config, &diag)) {}

  ExporterConfig config;
  Diagnostics diag;
  std::unique_ptr<StatsExporter> backend;
};

namespace {

bool Connect(PschExporter& ex) {
  return ex.backend->IsConnected() || ex.backend->EstablishNewConnection();
}

// Column emission shared by ClickHouse, OTLP log record, and unified Arrow backends
bool ExportColumns(PschExporter& ex, const PschExportEvent* events, size_t count) {
  StatsExporter& exporter = *ex.backend;
  exporter.BeginBatch();

  auto col_ts = exporter.StatTimestamp("ts");
  auto col_duration_us = exporter.DbDurationColumn();
  auto col_db_name = exporter.DbNameColumn();
  auto col_db_user = exporter.DbUserColumn();
  auto col_pid = exporter.StatLCInt32("pid");
  auto col_query_id = exporter.StatHCInt64("query_id");
  auto col_db_operation = exporter.DbOperationColumn();
  auto col_rows = exporter.StatHCUInt64("rows");
  auto col_query_text = exporter.DbQueryTextColumn();

  auto col_shared_blks_hit = exporter.StatHCInt64("shared_blks_hit");
  auto col_shared_blks_read = exporter.StatHCInt64("shared_blks_read");
  auto col_shared_blks_dirtied = exporter.StatHCInt64("shared_blks_dirtied");
  auto col_shared_blks_written = exporter.StatHCInt64("shared_blks_written");
  auto col_local_blks_hit = exporter.StatHCInt64("local_blks_hit");
  auto col_local_blks_read = exporter.StatHCInt64("local_blks_read");
  auto col_local_blks_dirtied = exporter.StatHCInt64("local_blks_dirtied");
  auto col_local_blks_written = exporter.StatHCInt64("local_blks_written");
  auto col_temp_blks_read = exporter.StatHCInt64("temp_blks_read");
  auto col_temp_blks_written = exporter.StatHCInt64("temp_blks_written");

  auto col_shared_blk_read_time_us = exporter.StatHCInt64("shared_blk_read_time_us");
  auto col_shared_blk_write_time_us = exporter.StatHCInt64("shared_blk_write_time_us");
  auto col_local_blk_read_time_us = exporter.StatHCInt64("local_blk_read_time_us");
  auto col_local_blk_write_time_us = exporter.StatHCInt64("local_blk_write_time_us");
  auto col_temp_blk_read_time_us = exporter.StatHCInt64("temp_blk_read_time_us");
  auto col_temp_blk_write_time_us = exporter.StatHCInt64("temp_blk_write_time_us");

  auto col_wal_records = exporter.StatHCInt64("wal_records");
  auto col_wal_fpi = exporter.StatHCInt64("wal_fpi");
  auto col_wal_bytes = exporter.StatHCUInt64("wal_bytes");

  auto col_cpu_user_time_us = exporter.StatHCInt64("cpu_user_time_us");
  auto col_cpu_sys_time_us = exporter.StatHCInt64("cpu_sys_time_us");

  auto col_jit_functions = exporter.StatLCInt32("jit_functions");
  auto col_jit_generation_time_us = exporter.StatLCInt32("jit_generation_time_us");
  auto col_jit_deform_time_us = exporter.StatLCInt32("jit_deform_time_us");
  auto col_jit_inlining_time_us = exporter.StatLCInt32("jit_inlining_time_us");
  auto col_jit_optimization_time_us = exporter.StatLCInt32("jit_optimization_time_us");
  auto col_jit_emission_time_us = exporter.StatLCInt32("jit_emission_time_us");

  auto col_parallel_workers_planned = exporter.StatLCInt16("parallel_workers_planned");
  auto col_parallel_workers_launched = exporter.StatLCInt16("parallel_workers_launched");

  auto col_err_sqlstate = exporter.StatLCString("err_sqlstate");
  auto col_err_elevel = exporter.StatLCUInt8("err_elevel");
  auto col_err_message = exporter.StatHCString("err_message");

  auto col_app = exporter.StatLCString("app");
  auto col_client_addr = exporter.StatHCString("client_addr");

  for (size_t i = 0; i < count; ++i) {
    const PschExportEvent& ev = events[i];
    exporter.BeginRow();

    col_ts->Append(ev.ts_unix_us);
    col_duration_us->Append(ev.duration_us);
    col_db_name->Append(std::string(View(ev.db_name)));
    col_db_user->Append(std::string(View(ev.db_user)));
    col_pid->Append(ev.pid);
    col_query_id->Append(ev.query_id);
    col_db_operation->Append(std::string(View(ev.db_operation)));
    col_rows->Append(ev.rows);
    col_query_text->Append(View(ev.query_text));

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

    col_err_sqlstate->Append(std::string(View(ev.err_sqlstate)));
    col_err_elevel->Append(ev.err_elevel);
    col_err_message->Append(View(ev.err_message));

    col_app->Append(std::string(View(ev.app_name)));
    col_client_addr->Append(View(ev.client_addr));
  }

  return exporter.CommitBatch();
}

// query_logs_arrow column set. Collector routing matches block_format
// "arrow_ipc" to that legacy destination
bool ExportLegacyArrow(PschExporter& ex, const PschExportEvent* events, size_t count) {
  ArrowBatchBuilder builder(&ex.diag);
  builder.Init(ex.config.extra_attributes.c_str(), ex.config.service_version.c_str());

  const size_t max_block_bytes = std::max<size_t>(65536, ex.config.arrow_max_block_bytes);

  auto flush = [&]() -> bool {
    ArrowBatchBuilder::FinishResult result = builder.Finish();
    if (result.ipc_buffer == nullptr || result.num_rows <= 0)
      return false;
    const auto ipc_len = static_cast<size_t>(result.ipc_buffer->size());
    MaybeDumpArrowBatch(ex.config.arrow_dump_dir, result.ipc_buffer->data(), ipc_len, ex.diag);
    return ex.backend->SendArrowBatch(result.ipc_buffer->data(), ipc_len, result.num_rows,
                                      "arrow_ipc");
  };

  for (size_t i = 0; i < count; ++i) {
    if (!builder.Append(events[i]))
      return false;
    if (builder.EstimatedBytes() >= max_block_bytes) {
      if (!flush())
        return false;
      builder.Reset();
    }
  }
  return builder.NumRows() == 0 || flush();
}

bool Export(PschExporter& ex, const PschExportEvent* events, size_t count) {
  if (ex.config.mode == PSCH_EXPORTER_OTEL_ARROW_LEGACY)
    return ExportLegacyArrow(ex, events, count);
  return ExportColumns(ex, events, count);
}

// Exception barrier for every fallible entry point
template <typename Fn>
PschExporterStatus Guarded(Diagnostics& diag, PschExporterResult* result, std::string_view op,
                           const StatsExporter* backend, Fn&& fn) noexcept {
  if (result == nullptr)
    return PSCH_EXPORTER_INVALID_ARGUMENT;
  diag.Begin(result);
  try {
    if (!fn() && !diag.Failed())
      diag.Fail(op, "failed without detail");
  } catch (const std::bad_alloc&) {
    diag.Fail(op, "out of memory");
  } catch (const std::exception& e) {
    diag.Fail(op, e.what());
  } catch (...) {
    diag.Fail(op, "unknown C++ exception");
  }
  result->connected = backend != nullptr && backend->IsConnected();
  return diag.End();
}

// Fill result even when no handle supplies Diagnostics
PschExporterStatus RejectNullExporter(PschExporterResult* result, std::string_view op) noexcept {
  Diagnostics diag;
  return Guarded(diag, result, op, nullptr, [&] {
    diag.Reject(op, "exporter must not be NULL");
    return false;
  });
}

}  // namespace

extern "C" {

PschExporterStatus PschExporterCreate(const PschExporterConfig* config, PschExporter** exporter,
                                      PschExporterResult* result) noexcept {
  if (exporter != nullptr)
    *exporter = nullptr;
  Diagnostics diag;
  return Guarded(diag, result, "create exporter", nullptr, [&] {
    if (config == nullptr || exporter == nullptr) {
      diag.Reject("create exporter", "config and exporter must not be NULL");
      return false;
    }
    auto created = std::make_unique<PschExporter>(*config);
    if (created->backend == nullptr) {
      diag.Reject("create exporter", "unknown exporter mode");
      return false;
    }
    *exporter = created.release();
    return true;
  });
}

PschExporterStatus PschExporterConfigure(PschExporter* exporter, const PschExporterConfig* config,
                                         PschExporterResult* result) noexcept {
  if (exporter == nullptr)
    return RejectNullExporter(result, "configure exporter");
  Diagnostics& diag = exporter->diag;
  return Guarded(diag, result, "configure exporter", exporter->backend.get(), [&] {
    if (config == nullptr) {
      diag.Reject("configure exporter", "config must not be NULL");
      return false;
    }
    if (!SameBackend(exporter->config.mode, config->mode)) {
      diag.Reject("configure exporter", "exporter mode cannot change after creation");
      return false;
    }
    ExporterConfig next(*config);
    exporter->config = std::move(next);
    return true;
  });
}

PschExporterStatus PschExporterConnect(PschExporter* exporter,
                                       PschExporterResult* result) noexcept {
  if (exporter == nullptr)
    return RejectNullExporter(result, "connect");
  return Guarded(exporter->diag, result, "connect", exporter->backend.get(),
                 [&] { return Connect(*exporter); });
}

PschExporterStatus PschExporterExport(PschExporter* exporter, const PschExportEvent* events,
                                      size_t count, PschExporterResult* result) noexcept {
  if (exporter == nullptr)
    return RejectNullExporter(result, "export");
  Diagnostics& diag = exporter->diag;
  return Guarded(diag, result, "export", exporter->backend.get(), [&] {
    if (const char* problem = ValidateEvents(events, count)) {
      diag.Reject("export", problem);
      return false;
    }
    return count == 0 || Export(*exporter, events, count);
  });
}

void PschExporterDestroy(PschExporter* exporter) noexcept {
  delete exporter;
}

}  // extern "C"
