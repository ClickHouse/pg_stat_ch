// pg_stat_ch ClickHouse exporter implementation

extern "C" {
#include "postgres.h"

#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"
}

#include <memory>
#include <string>
#include <vector>

#include <clickhouse/client.h>

#include "export/clickhouse_exporter.h"
#include "config/guc.h"
#include "queue/event.h"
#include "queue/shmem.h"

namespace {

// PostgreSQL epoch is 2000-01-01, Unix epoch is 1970-01-01
// Difference is 946684800 seconds = 946684800000000 microseconds
constexpr int64_t kPostgresEpochOffsetUs = 946684800000000LL;

// Exponential backoff constants
constexpr int kBaseDelayMs = 1000;      // 1 second
constexpr int kMaxDelayMs = 60000;      // 60 seconds
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

// Resolve database OID to name
std::string ResolveDatabaseName(Oid dbid) {
  if (dbid == InvalidOid) {
    return "unknown";
  }

  char* name = get_database_name(dbid);
  if (name != nullptr) {
    std::string result(name);
    pfree(name);
    return result;
  }
  return "unknown";
}

// Resolve user OID to name
std::string ResolveUserName(Oid userid) {
  if (userid == InvalidOid) {
    return "unknown";
  }

  char* name = GetUserNameFromId(userid, true);
  if (name != nullptr) {
    return std::string(name);
  }
  return "unknown";
}

// Dequeue events from the shared memory queue
std::vector<PschEvent> DequeueEvents(int max_events) {
  std::vector<PschEvent> events;
  events.reserve(max_events);

  PschEvent event;
  while (events.size() < static_cast<size_t>(max_events) &&
         PschDequeueEvent(&event)) {
    events.push_back(event);
  }
  return events;
}

// Build a ClickHouse block from events
clickhouse::Block BuildClickHouseBlock(const std::vector<PschEvent>& events) {
  clickhouse::Block block;

  // Basic columns
  auto col_ts_start = std::make_shared<clickhouse::ColumnDateTime64>(6);
  auto col_duration_us = std::make_shared<clickhouse::ColumnUInt64>();
  auto col_db = std::make_shared<clickhouse::ColumnString>();
  auto col_user = std::make_shared<clickhouse::ColumnString>();
  auto col_pid = std::make_shared<clickhouse::ColumnUInt32>();
  auto col_query_id = std::make_shared<clickhouse::ColumnUInt64>();
  auto col_top_level = std::make_shared<clickhouse::ColumnUInt8>();
  auto col_cmd_type = std::make_shared<clickhouse::ColumnString>();
  auto col_rows = std::make_shared<clickhouse::ColumnUInt64>();
  auto col_query = std::make_shared<clickhouse::ColumnString>();

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
  auto col_shared_blk_write_time_us =
      std::make_shared<clickhouse::ColumnInt64>();
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

  // Error columns
  auto col_err_sqlstate = std::make_shared<clickhouse::ColumnFixedString>(5);
  auto col_err_elevel = std::make_shared<clickhouse::ColumnUInt8>();

  // Client context columns
  auto col_app = std::make_shared<clickhouse::ColumnString>();
  auto col_client_addr = std::make_shared<clickhouse::ColumnString>();

  for (const auto& ev : events) {
    int64_t unix_us = ev.ts_start + kPostgresEpochOffsetUs;
    col_ts_start->Append(unix_us);
    col_duration_us->Append(ev.duration_us);
    col_db->Append(ResolveDatabaseName(ev.dbid));
    col_user->Append(ResolveUserName(ev.userid));
    col_pid->Append(static_cast<uint32_t>(ev.pid));
    col_query_id->Append(ev.queryid);
    col_top_level->Append(ev.top_level ? 1 : 0);
    col_cmd_type->Append(CmdTypeToString(ev.cmd_type));
    col_rows->Append(ev.rows);
    col_query->Append(std::string(ev.query, ev.query_len));

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

    // I/O timing
    col_shared_blk_read_time_us->Append(ev.shared_blk_read_time_us);
    col_shared_blk_write_time_us->Append(ev.shared_blk_write_time_us);
    col_local_blk_read_time_us->Append(ev.local_blk_read_time_us);
    col_local_blk_write_time_us->Append(ev.local_blk_write_time_us);
    col_temp_blk_read_time_us->Append(ev.temp_blk_read_time_us);
    col_temp_blk_write_time_us->Append(ev.temp_blk_write_time_us);

    // WAL usage
    col_wal_records->Append(ev.wal_records);
    col_wal_fpi->Append(ev.wal_fpi);
    col_wal_bytes->Append(ev.wal_bytes);

    // CPU time
    col_cpu_user_time_us->Append(ev.cpu_user_time_us);
    col_cpu_sys_time_us->Append(ev.cpu_sys_time_us);

    // JIT
    col_jit_functions->Append(ev.jit_functions);
    col_jit_generation_time_us->Append(ev.jit_generation_time_us);
    col_jit_deform_time_us->Append(ev.jit_deform_time_us);
    col_jit_inlining_time_us->Append(ev.jit_inlining_time_us);
    col_jit_optimization_time_us->Append(ev.jit_optimization_time_us);
    col_jit_emission_time_us->Append(ev.jit_emission_time_us);

    // Parallel workers
    col_parallel_workers_planned->Append(ev.parallel_workers_planned);
    col_parallel_workers_launched->Append(ev.parallel_workers_launched);

    // Error info (5-char SQLSTATE, trimmed)
    col_err_sqlstate->Append(std::string_view(ev.err_sqlstate, 5));
    col_err_elevel->Append(ev.err_elevel);

    // Client context
    col_app->Append(std::string(ev.application_name, ev.application_name_len));
    col_client_addr->Append(std::string(ev.client_addr, ev.client_addr_len));
  }

  // Basic columns
  block.AppendColumn("ts_start", col_ts_start);
  block.AppendColumn("duration_us", col_duration_us);
  block.AppendColumn("db", col_db);
  block.AppendColumn("user", col_user);
  block.AppendColumn("pid", col_pid);
  block.AppendColumn("query_id", col_query_id);
  block.AppendColumn("top_level", col_top_level);
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
    options
        .SetHost(psch_clickhouse_host != nullptr ? psch_clickhouse_host
                                                 : "localhost")
        .SetPort(psch_clickhouse_port)
        .SetUser(psch_clickhouse_user != nullptr ? psch_clickhouse_user
                                                 : "default")
        .SetPassword(psch_clickhouse_password != nullptr
                         ? psch_clickhouse_password
                         : "")
        .SetDefaultDatabase(psch_clickhouse_database != nullptr
                                ? psch_clickhouse_database
                                : "pg_stat_ch")
        .SetCompressionMethod(clickhouse::CompressionMethod::LZ4)
        .SetPingBeforeQuery(true)
        .SetSendRetries(3)
        .SetRetryTimeout(std::chrono::seconds(5));

    g_exporter.client = std::make_unique<clickhouse::Client>(options);
    g_exporter.initialized = true;

    const char* host =
        psch_clickhouse_host != nullptr ? psch_clickhouse_host : "localhost";
    elog(LOG, "pg_stat_ch: connected to ClickHouse at %s:%d", host,
         psch_clickhouse_port);

    return true;
  } catch (const std::exception& ex) {
    std::string err_msg = ex.what();
    elog(WARNING, "pg_stat_ch: failed to connect to ClickHouse: %s",
         err_msg.c_str());
    g_exporter.client.reset();
    return false;
  }
}

void PschExportBatch(void) {
  if (g_exporter.client == nullptr) {
    if (!PschExporterInit()) {
      g_exporter.consecutive_failures++;
      PschRecordExportFailure("Failed to connect to ClickHouse");
      return;
    }
  }

  std::vector<PschEvent> events = DequeueEvents(psch_batch_max);
  if (events.empty()) {
    return;
  }

  try {
    clickhouse::Block block = BuildClickHouseBlock(events);
    g_exporter.client->Insert("events_raw", block);

    if (psch_shared_state != nullptr) {
      pg_atomic_fetch_add_u64(&psch_shared_state->exported, events.size());
    }

    // Success: reset retry state and record success timestamp
    g_exporter.consecutive_failures = 0;
    PschRecordExportSuccess();

    elog(DEBUG1, "pg_stat_ch: exported %zu events to ClickHouse",
         events.size());

  } catch (const std::exception& ex) {
    std::string err_msg = ex.what();
    elog(WARNING, "pg_stat_ch: failed to insert to ClickHouse: %s",
         err_msg.c_str());

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
