// pg_stat_ch ClickHouse exporter implementation

extern "C" {
#include "postgres.h"

#include "commands/dbcommands.h"
#include "miscadmin.h"
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

// ClickHouse client instance (lives for the lifetime of the bgworker)
std::unique_ptr<clickhouse::Client> g_ch_client;

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
  }

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

    g_ch_client = std::make_unique<clickhouse::Client>(options);

    const char* host =
        psch_clickhouse_host != nullptr ? psch_clickhouse_host : "localhost";
    elog(LOG, "pg_stat_ch: connected to ClickHouse at %s:%d", host,
         psch_clickhouse_port);

    return true;
  } catch (const std::exception& ex) {
    std::string err_msg = ex.what();
    elog(WARNING, "pg_stat_ch: failed to connect to ClickHouse: %s",
         err_msg.c_str());
    g_ch_client.reset();
    return false;
  }
}

void PschExportBatch(void) {
  if (g_ch_client == nullptr) {
    if (!PschExporterInit()) {
      return;
    }
  }

  std::vector<PschEvent> events = DequeueEvents(psch_batch_max);
  if (events.empty()) {
    return;
  }

  try {
    clickhouse::Block block = BuildClickHouseBlock(events);
    g_ch_client->Insert("events_raw", block);

    if (psch_shared_state != nullptr) {
      pg_atomic_fetch_add_u64(&psch_shared_state->exported, events.size());
    }

    elog(DEBUG1, "pg_stat_ch: exported %zu events to ClickHouse",
         events.size());

  } catch (const std::exception& ex) {
    std::string err_msg = ex.what();
    elog(WARNING, "pg_stat_ch: failed to insert to ClickHouse: %s",
         err_msg.c_str());
    g_ch_client.reset();
  }
}

void PschExporterShutdown(void) {
  g_ch_client.reset();
  elog(LOG, "pg_stat_ch: ClickHouse exporter shutdown");
}

}  // extern "C"
