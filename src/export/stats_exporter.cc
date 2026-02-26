// pg_stat_ch statistics exporter implementation

extern "C" {
#include "postgres.h"
}

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <clickhouse/client.h>

#include "config/guc.h"
#include "export/stats_exporter.h"
#include "export/exporter_interface.h"
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

// ClickHouse-flavored Stats Exporter
// Builds a clickhouse::Block and uploads it
class ClickHouseExporter : public StatsExporter {
 public:
  // Tags in CH are just ordinary columns; aggregation happens in queries
  shared_ptr<Column<string>> TagString(string_view name) final {
    return Wrap<clickhouse::ColumnString, string>(name);
  }

  // Metrics are also all ordinary columns.
  shared_ptr<Column<int16_t>> MetricInt16(string_view name) final {
    return Wrap<clickhouse::ColumnInt16>(name);
  }
  shared_ptr<Column<int32_t>> MetricInt32(string_view name) final {
    return Wrap<clickhouse::ColumnInt32>(name);
  }
  shared_ptr<Column<int64_t>> MetricInt64(string_view name) final {
    return Wrap<clickhouse::ColumnInt64>(name);
  }
  shared_ptr<Column<uint8_t>> MetricUInt8(string_view name) final {
    return Wrap<clickhouse::ColumnUInt8>(name);
  }
  shared_ptr<Column<uint64_t>> MetricUInt64(string_view name) final {
    return Wrap<clickhouse::ColumnUInt64>(name);
  }
  shared_ptr<Column<string_view>> MetricFixedString(int len, string_view name) final {
    return Wrap<clickhouse::ColumnFixedString, string_view>(name, len);
  }

  // Records... you guessed it
  shared_ptr<Column<int32_t>> RecordInt32(string_view name) final {
    return Wrap<clickhouse::ColumnInt32>(name);
  }
  shared_ptr<Column<int64_t>> RecordInt64(string_view name) final {
    return Wrap<clickhouse::ColumnInt64>(name);
  }
  shared_ptr<Column<int64_t>> RecordDateTime(string_view name) final {
    return Wrap<clickhouse::ColumnDateTime64, int64_t>(name, 6);
  }
  shared_ptr<Column<string_view>> RecordString(string_view name) final {
    return Wrap<clickhouse::ColumnString, string_view>(name);
  }

  void BeginBatch() final {
    block = std::make_unique<clickhouse::Block>();
    columns.clear();
    exported_count = 0;
  }
  void BeginRow() final { ++exported_count; }
  bool CommitBatch() final;

  bool EstablishNewConnection() final;
  bool IsConnected() const final { return (bool)client; }
  int NumConsecutiveFailures() const final { return consecutive_failures; }
  void ResetFailures() final { consecutive_failures = 0; }
  int NumExported() const final { return exported_count; }

 private:
  template<typename T, typename U> class ClickHouseColumn : public Column<T> {
   public:
    template<typename... CH_Args>
    ClickHouseColumn(ClickHouseExporter *exporter_, std::string_view name_, CH_Args&&... args):
        exporter(exporter_), name(name_), ch_column(std::make_shared<U>(args...)) {}

    void Append(const T &t) final { ch_column->Append(t); }
    void Crunch() final { exporter->block->AppendColumn(name, ch_column); }

   private:
    ClickHouseExporter *const exporter;
    std::string name;
    const shared_ptr<U> ch_column;
  };

  template<class T, typename U = typename T::DataType, typename... Args>
  shared_ptr<ClickHouseColumn<U, T>> Wrap(std::string_view name, Args&&... args) {
    auto col = std::make_shared<ClickHouseColumn<U, T>>(this, name, args...);
    columns.push_back(col);
    return col;
  }

  std::unique_ptr<clickhouse::Client> client;
  std::unique_ptr<clickhouse::Block> block;
  std::vector<shared_ptr<BasicColumn>> columns;
  int consecutive_failures = 0;
  int exported_count = 0;
};

bool ClickHouseExporter::CommitBatch() {
  try {
    if (!block) {
      elog(WARNING, "pg_stat_ch: Logic error: Block not built");
      return false;
    }
    for (const auto &col : columns) {
      col->Crunch();
    }

    if (!client && (!EstablishNewConnection() || !client)) {
      elog(WARNING, "pg_stat_ch: Connection not established; bailing.");
      return false;
    }

    elog(DEBUG1, "pg_stat_ch: Inserting Block to ClickHouse");
    client->Insert("events_raw", *block);
    elog(DEBUG1, "pg_stat_ch: insert completed");

    // Success: reset retry state and record success timestamp
    consecutive_failures = 0;
    elog(DEBUG1, "pg_stat_ch: exported %d events to ClickHouse", exported_count);
    return true;

  } catch (const std::exception& ex) {
    std::string err_msg = ex.what();
    elog(WARNING, "pg_stat_ch: failed to insert to ClickHouse: %s", err_msg.c_str());

    // Failure: increment counter, record error, reset client for reconnect
    consecutive_failures++;
    PschRecordExportFailure(err_msg.c_str());
    client.reset();
    return false;
  }
}

// Exporter state - encapsulates all bgworker-local state
struct ExporterState {
  std::unique_ptr<StatsExporter> exporter;
};

// Bgworker-local exporter state (no locking needed)
ExporterState g_exporter;

// Convert PschCmdType to string
const char* CmdTypeToString(PschCmdType cmd) {
  switch (cmd) {
    case PSCH_CMD_SELECT:
      return "SELECT";
    case PSCH_CMD_UPDATE:
      return "UPDATE";
    case PSCH_CMD_INSERT:
      return "INSERT";
    case PSCH_CMD_DELETE:
      return "DELETE";
    case PSCH_CMD_MERGE:
      return "MERGE";
    case PSCH_CMD_UTILITY:
      return "UTILITY";
    case PSCH_CMD_NOTHING:
      return "NOTHING";
    default:
      return "UNKNOWN";
  }
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

// Build and export stats (records, metrics, ClickHouse rows) from events
void ExportEventStats(const std::vector<PschEvent>& events, StatsExporter *exporter) {
  elog(DEBUG1, "pg_stat_ch: ExportEventStats() called with %zu events", events.size());

  exporter->BeginBatch();

  elog(DEBUG2, "pg_stat_ch: creating column objects");

  // Basic columns
  elog(DEBUG3, "pg_stat_ch: creating col_ts_start");
  auto col_ts_start = exporter->RecordDateTime("ts_start");
  elog(DEBUG3, "pg_stat_ch: col_ts_start created");
  auto col_duration_us = exporter->MetricUInt64("duration_us");
  // Use pre-resolved names from event (resolved at capture time in hooks)
  auto col_db = exporter->TagString("db");
  auto col_username = exporter->TagString("username");
  elog(DEBUG3, "pg_stat_ch: basic columns created");
  auto col_pid = exporter->RecordInt32("pid");
  auto col_query_id = exporter->RecordInt64("query_id");
  auto col_cmd_type = exporter->RecordString("cmd_type");
  auto col_rows = exporter->MetricUInt64("rows");
  auto col_query = exporter->RecordString("query");
  elog(DEBUG3, "pg_stat_ch: all basic columns created");

  // Buffer usage columns
  auto col_shared_blks_hit = exporter->MetricInt64("shared_blks_hit");
  auto col_shared_blks_read = exporter->MetricInt64("shared_blks_read");
  auto col_shared_blks_dirtied = exporter->MetricInt64("shared_blks_dirtied");
  auto col_shared_blks_written = exporter->MetricInt64("shared_blks_written");
  auto col_local_blks_hit = exporter->MetricInt64("local_blks_hit");
  auto col_local_blks_read = exporter->MetricInt64("local_blks_read");
  auto col_local_blks_dirtied = exporter->MetricInt64("local_blks_dirtied");
  auto col_local_blks_written = exporter->MetricInt64("local_blks_written");
  auto col_temp_blks_read = exporter->MetricInt64("temp_blks_read");
  auto col_temp_blks_written = exporter->MetricInt64("temp_blks_written");

  // I/O timing columns
  auto col_shared_blk_read_time_us = exporter->MetricInt64("shared_blk_read_time_us");
  auto col_shared_blk_write_time_us = exporter->MetricInt64("shared_blk_write_time_us");
  auto col_local_blk_read_time_us = exporter->MetricInt64("local_blk_read_time_us");
  auto col_local_blk_write_time_us = exporter->MetricInt64("local_blk_write_time_us");
  auto col_temp_blk_read_time_us = exporter->MetricInt64("temp_blk_read_time_us");
  auto col_temp_blk_write_time_us = exporter->MetricInt64("temp_blk_write_time_us");

  // WAL usage columns
  auto col_wal_records = exporter->MetricInt64("wal_records");
  auto col_wal_fpi = exporter->MetricInt64("wal_fpi");
  auto col_wal_bytes = exporter->MetricUInt64("wal_bytes");

  // CPU time columns
  auto col_cpu_user_time_us = exporter->MetricInt64("cpu_user_time_us");
  auto col_cpu_sys_time_us = exporter->MetricInt64("cpu_sys_time_us");

  // JIT columns
  auto col_jit_functions = exporter->MetricInt32("jit_functions");
  auto col_jit_generation_time_us = exporter->MetricInt32("jit_generation_time_us");
  auto col_jit_deform_time_us = exporter->MetricInt32("jit_deform_time_us");
  auto col_jit_inlining_time_us = exporter->MetricInt32("jit_inlining_time_us");
  auto col_jit_optimization_time_us = exporter->MetricInt32("jit_optimization_time_us");
  auto col_jit_emission_time_us = exporter->MetricInt32("jit_emission_time_us");

  // Parallel worker columns
  auto col_parallel_workers_planned = exporter->MetricInt16("parallel_workers_planned");
  auto col_parallel_workers_launched = exporter->MetricInt16("parallel_workers_launched");

  elog(DEBUG3, "pg_stat_ch: creating error columns");
  // Error columns
  auto col_err_sqlstate = exporter->MetricFixedString(5, "err_sqlstate");
  auto col_err_elevel = exporter->MetricUInt8("err_elevel");
  auto col_err_message = exporter->RecordString("err_message");
  elog(DEBUG3, "pg_stat_ch: error columns created");

  // Client context columns; records rather than tags (no histogram in OTel)
  auto col_app = exporter->RecordString("app");
  auto col_client_addr = exporter->RecordString("client_addr");

  elog(DEBUG2, "pg_stat_ch: all columns created, starting event loop");
  size_t event_idx = 0;
  for (const auto& ev : events) {
    elog(DEBUG2, "pg_stat_ch: processing event %zu: pid=%d, query_len=%u", event_idx, ev.pid,
         ev.query_len);
    exporter->BeginRow();

    int64_t unix_us = ev.ts_start + kPostgresEpochOffsetUs;
    col_ts_start->Append(unix_us);
    col_duration_us->Append(ev.duration_us);

    // Use pre-resolved names from event (resolved at capture time in hooks)
    col_db->Append(std::string(ev.datname, ev.datname_len));
    col_username->Append(std::string(ev.username, ev.username_len));

    col_pid->Append(ev.pid);
    col_query_id->Append(static_cast<int64_t>(ev.queryid));
    col_cmd_type->Append(CmdTypeToString(ev.cmd_type));
    col_rows->Append(ev.rows);

    // Validate query_len before using it
    uint16 safe_query_len = ev.query_len;
    if (safe_query_len > PSCH_MAX_QUERY_LEN) {
      elog(WARNING, "pg_stat_ch: event %zu has invalid query_len %u, clamping", event_idx,
           safe_query_len);
      safe_query_len = PSCH_MAX_QUERY_LEN;
    }
    col_query->Append(std::string(ev.query, safe_query_len));

    elog(DEBUG3, "pg_stat_ch: event %zu - buffer usage", event_idx);
    // Buffer usage
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

    elog(DEBUG3, "pg_stat_ch: event %zu - I/O timing", event_idx);
    // I/O timing
    col_shared_blk_read_time_us->Append(ev.shared_blk_read_time_us);
    col_shared_blk_write_time_us->Append(ev.shared_blk_write_time_us);
    col_local_blk_read_time_us->Append(ev.local_blk_read_time_us);
    col_local_blk_write_time_us->Append(ev.local_blk_write_time_us);
    col_temp_blk_read_time_us->Append(ev.temp_blk_read_time_us);
    col_temp_blk_write_time_us->Append(ev.temp_blk_write_time_us);

    elog(DEBUG3, "pg_stat_ch: event %zu - WAL usage", event_idx);
    // WAL usage
    col_wal_records->Append(ev.wal_records);
    col_wal_fpi->Append(ev.wal_fpi);
    col_wal_bytes->Append(ev.wal_bytes);

    elog(DEBUG3, "pg_stat_ch: event %zu - CPU time", event_idx);
    // CPU time
    col_cpu_user_time_us->Append(ev.cpu_user_time_us);
    col_cpu_sys_time_us->Append(ev.cpu_sys_time_us);

    elog(DEBUG3, "pg_stat_ch: event %zu - JIT", event_idx);
    // JIT
    col_jit_functions->Append(ev.jit_functions);
    col_jit_generation_time_us->Append(ev.jit_generation_time_us);
    col_jit_deform_time_us->Append(ev.jit_deform_time_us);
    col_jit_inlining_time_us->Append(ev.jit_inlining_time_us);
    col_jit_optimization_time_us->Append(ev.jit_optimization_time_us);
    col_jit_emission_time_us->Append(ev.jit_emission_time_us);

    elog(DEBUG3, "pg_stat_ch: event %zu - parallel workers", event_idx);
    // Parallel workers
    col_parallel_workers_planned->Append(ev.parallel_workers_planned);
    col_parallel_workers_launched->Append(ev.parallel_workers_launched);

    elog(DEBUG3, "pg_stat_ch: event %zu - error info", event_idx);
    // Error info (5-char SQLSTATE, trimmed)
    col_err_sqlstate->Append(std::string_view(ev.err_sqlstate, 5));
    col_err_elevel->Append(ev.err_elevel);
    // Error message (validate length)
    uint16 safe_err_msg_len = ev.err_message_len;
    if (safe_err_msg_len > PSCH_MAX_ERR_MSG_LEN) {
      elog(WARNING, "pg_stat_ch: event %zu has invalid err_message_len %u, clamping", event_idx,
           safe_err_msg_len);
      safe_err_msg_len = PSCH_MAX_ERR_MSG_LEN;
    }
    col_err_message->Append(std::string(ev.err_message, safe_err_msg_len));

    elog(DEBUG3, "pg_stat_ch: event %zu - client context (app_len=%u, addr_len=%u)", event_idx,
         ev.application_name_len, ev.client_addr_len);
    // Client context - validate lengths
    uint8 safe_app_len = ev.application_name_len;
    if (safe_app_len > 63) {
      elog(WARNING, "pg_stat_ch: event %zu has invalid app_name_len %u, clamping", event_idx,
           safe_app_len);
      safe_app_len = 63;
    }
    uint8 safe_addr_len = ev.client_addr_len;
    if (safe_addr_len > 45) {
      elog(WARNING, "pg_stat_ch: event %zu has invalid client_addr_len %u, clamping", event_idx,
           safe_addr_len);
      safe_addr_len = 45;
    }
    col_app->Append(std::string(ev.application_name, safe_app_len));
    col_client_addr->Append(std::string(ev.client_addr, safe_addr_len));

    event_idx++;
  }
  elog(DEBUG1, "pg_stat_ch: finished processing %zu events", event_idx);
}


bool ClickHouseExporter::EstablishNewConnection() {
  try {
    clickhouse::ClientOptions options;

    // Socket timeouts prevent indefinite blocking during network I/O. Without them,
    // the bgworker can't respond to PostgreSQL signals (e.g., DROP DATABASE waits
    // for ProcSignalBarrier acknowledgment). 30 seconds balances reliability on
    // slow networks against signal responsiveness. See bgworker.cc for details.
    constexpr auto kSocketTimeout = std::chrono::seconds(30);

    options.SetHost(psch_clickhouse_host != nullptr ? psch_clickhouse_host : "localhost")
        .SetPort(psch_clickhouse_port)
        .SetUser(psch_clickhouse_user != nullptr ? psch_clickhouse_user : "default")
        .SetPassword(psch_clickhouse_password != nullptr ? psch_clickhouse_password : "")
        .SetDefaultDatabase(psch_clickhouse_database != nullptr ? psch_clickhouse_database
                                                                : "pg_stat_ch")
        .SetCompressionMethod(clickhouse::CompressionMethod::LZ4)
        .SetPingBeforeQuery(true)
        .SetSendRetries(3)
        .SetRetryTimeout(std::chrono::seconds(5))
        .SetConnectionConnectTimeout(kSocketTimeout)
        .SetConnectionRecvTimeout(kSocketTimeout)
        .SetConnectionSendTimeout(kSocketTimeout);

    if (psch_clickhouse_use_tls) {
      clickhouse::ClientOptions::SSLOptions ssl_opts;
      ssl_opts.SetUseDefaultCALocations(true).SetUseSNI(true).SetSkipVerification(
          psch_clickhouse_skip_tls_verify);
      options.SetSSLOptions(ssl_opts);
      elog(LOG, "pg_stat_ch: TLS enabled for ClickHouse connection%s",
           psch_clickhouse_skip_tls_verify ? " (verification skipped)" : "");
    }

    client = std::make_unique<clickhouse::Client>(options);

    const char* host = psch_clickhouse_host != nullptr ? psch_clickhouse_host : "localhost";
    elog(LOG, "pg_stat_ch: connected to ClickHouse at %s:%d%s", host, psch_clickhouse_port,
         psch_clickhouse_use_tls ? " (TLS)" : "");

    return true;
  } catch (const std::exception& ex) {
    std::string err_msg = ex.what();
    elog(WARNING, "pg_stat_ch: failed to connect to ClickHouse: %s", err_msg.c_str());
    client.reset();
    return false;
  }
}

}  // namespace

extern "C" {

bool PschExporterInit(void) {
  g_exporter.exporter = std::make_unique<ClickHouseExporter>();
  return g_exporter.exporter->EstablishNewConnection();
}

int PschExportBatch(void) {
  elog(DEBUG1, "pg_stat_ch: PschExportBatch() called");
  StatsExporter *exporter = g_exporter.exporter.get();

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

  elog(DEBUG1, "pg_stat_ch: exporting batch of %zu events", events.size());
  ExportEventStats(events, exporter);

  if (exporter->CommitBatch()) {
    if (psch_shared_state != nullptr) {
      pg_atomic_fetch_add_u64(&psch_shared_state->exported, exporter->NumExported());
    }
    PschRecordExportSuccess();
  }

  return exporter->NumExported();
}

void PschResetRetryState(void) {
  if (g_exporter.exporter)
    g_exporter.exporter->ResetFailures();
}

int PschGetRetryDelayMs(void) {
  StatsExporter *exporter = g_exporter.exporter.get();
  if (!exporter || exporter->NumConsecutiveFailures() <= 0) {
    return 0;
  }
  // Exponential backoff: base * 2^(failures-1), capped at max
  int capped_failures = (exporter->NumConsecutiveFailures() > kMaxConsecutiveFailures)
                            ? kMaxConsecutiveFailures
                            : exporter->NumConsecutiveFailures();
  int delay = kBaseDelayMs * (1 << (capped_failures - 1));
  return (delay > kMaxDelayMs) ? kMaxDelayMs : delay;
}

int PschGetConsecutiveFailures(void) {
  return g_exporter.exporter->NumConsecutiveFailures();
}

void PschExporterShutdown(void) {
  g_exporter.exporter.reset();
  elog(LOG, "pg_stat_ch: statistics exporter shutdown");
}

}  // extern "C"
