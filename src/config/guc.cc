// pg_stat_ch GUC (Grand Unified Configuration) implementation

extern "C" {
#include "postgres.h"

#include "utils/guc.h"
}

#include "config/guc.h"

// GUC variable storage
bool psch_enabled = true;
char* psch_clickhouse_host = nullptr;
int psch_clickhouse_port = 9000;
char* psch_clickhouse_user = nullptr;
char* psch_clickhouse_password = nullptr;
char* psch_clickhouse_database = nullptr;
int psch_queue_capacity = 65536;
int psch_flush_interval_ms = 1000;
int psch_batch_max = 1000;

extern "C" {

// Check hook to ensure queue_capacity is a power of 2
// NOLINTNEXTLINE(readability-identifier-naming) - PostgreSQL GUC hook naming convention
static bool check_psch_queue_capacity(int* newval, void** extra, GucSource source) {
  (void)extra;   // Unused parameter
  (void)source;  // Unused parameter
  
  // Check if value is positive and a power of 2
  if (*newval <= 0) {
    GUC_check_errdetail("pg_stat_ch.queue_capacity must be positive.");
    return false;
  }
  
  // Check if power of 2: value & (value - 1) == 0
  if ((*newval & (*newval - 1)) != 0) {
    GUC_check_errdetail("pg_stat_ch.queue_capacity must be a power of 2 (e.g., 1024, 2048, 4096, 8192, 16384, 32768, 65536).");
    return false;
  }
  
  return true;
}

void PschInitGuc(void) {
  DefineCustomBoolVariable(
      "pg_stat_ch.enabled",
      "Enable or disable pg_stat_ch query telemetry collection.",
      nullptr,
      &psch_enabled,
      true,
      PGC_SIGHUP,
      0,
      nullptr,
      nullptr,
      nullptr);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_host",
      "ClickHouse server hostname.",
      nullptr,
      &psch_clickhouse_host,
      "localhost",
      PGC_POSTMASTER,
      0,
      nullptr,
      nullptr,
      nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.clickhouse_port",
      "ClickHouse server native protocol port.",
      nullptr,
      &psch_clickhouse_port,
      9000,
      1,
      65535,
      PGC_POSTMASTER,
      0,
      nullptr,
      nullptr,
      nullptr);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_user",
      "ClickHouse user name.",
      nullptr,
      &psch_clickhouse_user,
      "default",
      PGC_POSTMASTER,
      0,
      nullptr,
      nullptr,
      nullptr);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_password",
      "ClickHouse user password.",
      nullptr,
      &psch_clickhouse_password,
      "",
      PGC_POSTMASTER,
      0,
      nullptr,
      nullptr,
      nullptr);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_database",
      "ClickHouse database name for telemetry storage.",
      nullptr,
      &psch_clickhouse_database,
      "pg_stat_ch",
      PGC_POSTMASTER,
      0,
      nullptr,
      nullptr,
      nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.queue_capacity",
      "Maximum number of events in the shared memory queue (must be a power of 2).",
      nullptr,
      &psch_queue_capacity,
      65536,
      1024,
      1048576,
      PGC_POSTMASTER,
      0,
      check_psch_queue_capacity,
      nullptr,
      nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.flush_interval_ms",
      "Interval in milliseconds between ClickHouse export batches.",
      nullptr,
      &psch_flush_interval_ms,
      1000,
      100,
      60000,
      PGC_SIGHUP,
      GUC_UNIT_MS,
      nullptr,
      nullptr,
      nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.batch_max",
      "Maximum number of events per ClickHouse insert batch.",
      nullptr,
      &psch_batch_max,
      1000,
      1,
      100000,
      PGC_SIGHUP,
      0,
      nullptr,
      nullptr,
      nullptr);

  EmitWarningsOnPlaceholders("pg_stat_ch");
}

}  // extern "C"
