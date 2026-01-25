// pg_stat_ch ClickHouse exporter implementation

extern "C" {
#include "postgres.h"
}

#include <memory>
#include <string>
#include <vector>

#include <clickhouse/client.h>

#include "config/guc.h"
#include "export/clickhouse_exporter.h"
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
  std::unique_ptr<clickhouse::Client> client;
  int consecutive_failures = 0;
  bool initialized = false;
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

// Build a ClickHouse block from events
clickhouse::Block BuildClickHouseBlock(const std::vector<PschEvent>& events) {
  elog(DEBUG1, "pg_stat_ch: BuildClickHouseBlock() called with %zu events", events.size());

  elog(DEBUG2, "pg_stat_ch: creating column objects");
  clickhouse::Block block;

  // Basic columns
  elog(DEBUG3, "pg_stat_ch: creating col_ts_start");
  auto col_ts_start = std::make_shared<clickhouse::ColumnDateTime64>(6);
  elog(DEBUG3, "pg_stat_ch: col_ts_start created");
  auto col_duration_us = std::make_shared<clickhouse::ColumnUInt64>();
  // Use pre-resolved names from event (resolved at capture time in hooks)
  auto col_db = std::make_shared<clickhouse::ColumnString>();
  auto col_username = std::make_shared<clickhouse::ColumnString>();
  elog(DEBUG3, "pg_stat_ch: basic columns created");
  auto col_pid = std::make_shared<clickhouse::ColumnInt32>();
  auto col_query_id = std::make_shared<clickhouse::ColumnInt64>();
  auto col_cmd_type = std::make_shared<clickhouse::ColumnString>();
  auto col_rows = std::make_shared<clickhouse::ColumnUInt64>();
  auto col_query = std::make_shared<clickhouse::ColumnString>();
  elog(DEBUG3, "pg_stat_ch: all basic columns created");

  // Buffer usage columns
  auto col_shared_blks_hit = std::make_shared<clickhouse::ColumnInt64>();
  auto col_shared_blks_read = std::make_shared<clickhouse::ColumnInt64>();
  auto col_shared_blks_dirtied = std::make_shared<clickhouse::ColumnInt64>();
  auto col_shared_blks_written = std::make_shared<clickhouse::ColumnInt64>();
  auto col_local_blks_hit = std::make_shared<clickhouse::ColumnInt64>();
  auto col_local_blks_read = std::make_shared<clickhouse::ColumnInt64>();
  auto col_local_blks_dirtied = std::make_shared<clickhouse::ColumnInt64>();
  auto col_local_blks_written = std::make_shared<clickhouse::ColumnInt64>();
  auto col_temp_blks_read = std::make_shared<clickhouse::ColumnInt64>();
  auto col_temp_blks_written = std::make_shared<clickhouse::ColumnInt64>();

  // I/O timing columns
  auto col_shared_blk_read_time_us = std::make_shared<clickhouse::ColumnInt64>();
  auto col_shared_blk_write_time_us = std::make_shared<clickhouse::ColumnInt64>();
  auto col_local_blk_read_time_us = std::make_shared<clickhouse::ColumnInt64>();
  auto col_local_blk_write_time_us = std::make_shared<clickhouse::ColumnInt64>();
  auto col_temp_blk_read_time_us = std::make_shared<clickhouse::ColumnInt64>();
  auto col_temp_blk_write_time_us = std::make_shared<clickhouse::ColumnInt64>();

  // WAL usage columns
  auto col_wal_records = std::make_shared<clickhouse::ColumnInt64>();
  auto col_wal_fpi = std::make_shared<clickhouse::ColumnInt64>();
  auto col_wal_bytes = std::make_shared<clickhouse::ColumnUInt64>();

  // CPU time columns
  auto col_cpu_user_time_us = std::make_shared<clickhouse::ColumnInt64>();
  auto col_cpu_sys_time_us = std::make_shared<clickhouse::ColumnInt64>();

  // JIT columns
  auto col_jit_functions = std::make_shared<clickhouse::ColumnInt32>();
  auto col_jit_generation_time_us = std::make_shared<clickhouse::ColumnInt32>();
  auto col_jit_deform_time_us = std::make_shared<clickhouse::ColumnInt32>();
  auto col_jit_inlining_time_us = std::make_shared<clickhouse::ColumnInt32>();
  auto col_jit_optimization_time_us = std::make_shared<clickhouse::ColumnInt32>();
  auto col_jit_emission_time_us = std::make_shared<clickhouse::ColumnInt32>();

  // Parallel worker columns
  auto col_parallel_workers_planned = std::make_shared<clickhouse::ColumnInt16>();
  auto col_parallel_workers_launched = std::make_shared<clickhouse::ColumnInt16>();

  elog(DEBUG3, "pg_stat_ch: creating error columns");
  // Error columns
  auto col_err_sqlstate = std::make_shared<clickhouse::ColumnFixedString>(5);
  auto col_err_elevel = std::make_shared<clickhouse::ColumnUInt8>();
  elog(DEBUG3, "pg_stat_ch: error columns created");

  // Client context columns
  auto col_app = std::make_shared<clickhouse::ColumnString>();
  auto col_client_addr = std::make_shared<clickhouse::ColumnString>();

  elog(DEBUG2, "pg_stat_ch: all columns created, starting event loop");
  size_t event_idx = 0;
  for (const auto& ev : events) {
    elog(DEBUG2, "pg_stat_ch: processing event %zu: pid=%d, query_len=%u", event_idx, ev.pid,
         ev.query_len);

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

  // Basic columns
  block.AppendColumn("ts_start", col_ts_start);
  block.AppendColumn("duration_us", col_duration_us);
  block.AppendColumn("db", col_db);
  block.AppendColumn("username", col_username);
  block.AppendColumn("pid", col_pid);
  block.AppendColumn("query_id", col_query_id);
  block.AppendColumn("cmd_type", col_cmd_type);
  block.AppendColumn("rows", col_rows);
  block.AppendColumn("query", col_query);

  // Buffer usage columns
  block.AppendColumn("shared_blks_hit", col_shared_blks_hit);
  block.AppendColumn("shared_blks_read", col_shared_blks_read);
  block.AppendColumn("shared_blks_dirtied", col_shared_blks_dirtied);
  block.AppendColumn("shared_blks_written", col_shared_blks_written);
  block.AppendColumn("local_blks_hit", col_local_blks_hit);
  block.AppendColumn("local_blks_read", col_local_blks_read);
  block.AppendColumn("local_blks_dirtied", col_local_blks_dirtied);
  block.AppendColumn("local_blks_written", col_local_blks_written);
  block.AppendColumn("temp_blks_read", col_temp_blks_read);
  block.AppendColumn("temp_blks_written", col_temp_blks_written);

  // I/O timing columns
  block.AppendColumn("shared_blk_read_time_us", col_shared_blk_read_time_us);
  block.AppendColumn("shared_blk_write_time_us", col_shared_blk_write_time_us);
  block.AppendColumn("local_blk_read_time_us", col_local_blk_read_time_us);
  block.AppendColumn("local_blk_write_time_us", col_local_blk_write_time_us);
  block.AppendColumn("temp_blk_read_time_us", col_temp_blk_read_time_us);
  block.AppendColumn("temp_blk_write_time_us", col_temp_blk_write_time_us);

  // WAL usage columns
  block.AppendColumn("wal_records", col_wal_records);
  block.AppendColumn("wal_fpi", col_wal_fpi);
  block.AppendColumn("wal_bytes", col_wal_bytes);

  // CPU time columns
  block.AppendColumn("cpu_user_time_us", col_cpu_user_time_us);
  block.AppendColumn("cpu_sys_time_us", col_cpu_sys_time_us);

  // JIT columns
  block.AppendColumn("jit_functions", col_jit_functions);
  block.AppendColumn("jit_generation_time_us", col_jit_generation_time_us);
  block.AppendColumn("jit_deform_time_us", col_jit_deform_time_us);
  block.AppendColumn("jit_inlining_time_us", col_jit_inlining_time_us);
  block.AppendColumn("jit_optimization_time_us", col_jit_optimization_time_us);
  block.AppendColumn("jit_emission_time_us", col_jit_emission_time_us);

  // Parallel worker columns
  block.AppendColumn("parallel_workers_planned", col_parallel_workers_planned);
  block.AppendColumn("parallel_workers_launched", col_parallel_workers_launched);

  // Error columns
  block.AppendColumn("err_sqlstate", col_err_sqlstate);
  block.AppendColumn("err_elevel", col_err_elevel);

  // Client context columns
  block.AppendColumn("app", col_app);
  block.AppendColumn("client_addr", col_client_addr);

  return block;
}

}  // namespace

extern "C" {

bool PschExporterInit(void) {
  try {
    clickhouse::ClientOptions options;
    options.SetHost(psch_clickhouse_host != nullptr ? psch_clickhouse_host : "localhost")
        .SetPort(psch_clickhouse_port)
        .SetUser(psch_clickhouse_user != nullptr ? psch_clickhouse_user : "default")
        .SetPassword(psch_clickhouse_password != nullptr ? psch_clickhouse_password : "")
        .SetDefaultDatabase(psch_clickhouse_database != nullptr ? psch_clickhouse_database
                                                                : "pg_stat_ch")
        .SetCompressionMethod(clickhouse::CompressionMethod::LZ4)
        .SetPingBeforeQuery(true)
        .SetSendRetries(3)
        .SetRetryTimeout(std::chrono::seconds(5));

    if (psch_clickhouse_use_tls) {
      clickhouse::ClientOptions::SSLOptions ssl_opts;
      ssl_opts.SetUseDefaultCALocations(true)
          .SetUseSNI(true)
          .SetSkipVerification(psch_clickhouse_skip_tls_verify);
      options.SetSSLOptions(ssl_opts);
      elog(LOG, "pg_stat_ch: TLS enabled for ClickHouse connection%s",
           psch_clickhouse_skip_tls_verify ? " (verification skipped)" : "");
    }

    g_exporter.client = std::make_unique<clickhouse::Client>(options);
    g_exporter.initialized = true;

    const char* host = psch_clickhouse_host != nullptr ? psch_clickhouse_host : "localhost";
    elog(LOG, "pg_stat_ch: connected to ClickHouse at %s:%d%s", host, psch_clickhouse_port,
         psch_clickhouse_use_tls ? " (TLS)" : "");

    return true;
  } catch (const std::exception& ex) {
    std::string err_msg = ex.what();
    elog(WARNING, "pg_stat_ch: failed to connect to ClickHouse: %s", err_msg.c_str());
    g_exporter.client.reset();
    return false;
  }
}

void PschExportBatch(void) {
  elog(DEBUG1, "pg_stat_ch: PschExportBatch() called");

  if (g_exporter.client == nullptr) {
    elog(DEBUG1, "pg_stat_ch: client is null, initializing");
    if (!PschExporterInit()) {
      g_exporter.consecutive_failures++;
      PschRecordExportFailure("Failed to connect to ClickHouse");
      return;
    }
  }

  elog(DEBUG1, "pg_stat_ch: dequeuing events (max=%d)", psch_batch_max);
  std::vector<PschEvent> events = DequeueEvents(psch_batch_max);
  if (events.empty()) {
    elog(DEBUG1, "pg_stat_ch: no events to export");
    return;
  }

  elog(DEBUG1, "pg_stat_ch: building ClickHouse block with %zu events", events.size());

  try {
    clickhouse::Block block = BuildClickHouseBlock(events);
    elog(DEBUG1, "pg_stat_ch: block built, inserting to ClickHouse");
    g_exporter.client->Insert("events_raw", block);
    elog(DEBUG1, "pg_stat_ch: insert completed");

    if (psch_shared_state != nullptr) {
      pg_atomic_fetch_add_u64(&psch_shared_state->exported, events.size());
    }

    // Success: reset retry state and record success timestamp
    g_exporter.consecutive_failures = 0;
    PschRecordExportSuccess();

    elog(DEBUG1, "pg_stat_ch: exported %zu events to ClickHouse", events.size());

  } catch (const std::exception& ex) {
    std::string err_msg = ex.what();
    elog(WARNING, "pg_stat_ch: failed to insert to ClickHouse: %s", err_msg.c_str());

    // Failure: increment counter, record error, reset client for reconnect
    g_exporter.consecutive_failures++;
    PschRecordExportFailure(err_msg.c_str());
    g_exporter.client.reset();
  }
}

void PschResetRetryState(void) {
  g_exporter.consecutive_failures = 0;
}

int PschGetRetryDelayMs(void) {
  if (g_exporter.consecutive_failures <= 0) {
    return 0;
  }
  // Exponential backoff: base * 2^(failures-1), capped at max
  int capped_failures = (g_exporter.consecutive_failures > kMaxConsecutiveFailures)
                            ? kMaxConsecutiveFailures
                            : g_exporter.consecutive_failures;
  int delay = kBaseDelayMs * (1 << (capped_failures - 1));
  return (delay > kMaxDelayMs) ? kMaxDelayMs : delay;
}

int PschGetConsecutiveFailures(void) {
  return g_exporter.consecutive_failures;
}

void PschExporterShutdown(void) {
  g_exporter.client.reset();
  g_exporter.consecutive_failures = 0;
  g_exporter.initialized = false;
  elog(LOG, "pg_stat_ch: ClickHouse exporter shutdown");
}

}  // extern "C"
