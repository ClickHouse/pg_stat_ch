// pg_stat_ch GUC (Grand Unified Configuration) implementation

extern "C" {
#include "postgres.h"

#include "utils/guc.h"
}

#include <array>

#include "config/guc.h"

// GUC variable storage
bool psch_enabled = true;
bool psch_use_otel = false;
char* psch_clickhouse_host = nullptr;
int psch_clickhouse_port = 9000;
char* psch_clickhouse_user = nullptr;
char* psch_clickhouse_password = nullptr;
char* psch_clickhouse_database = nullptr;
bool psch_clickhouse_use_tls = false;
bool psch_clickhouse_skip_tls_verify = false;
char* psch_otel_endpoint = nullptr;
char* psch_hostname = nullptr;
int psch_queue_capacity = 131072;
int psch_flush_interval_ms = 200;
int psch_batch_max = 200000;
int psch_log_min_elevel = WARNING;
int psch_otel_log_queue_size = 65536;
int psch_otel_log_batch_size = 8192;
int psch_otel_log_max_bytes = 3 * 1024 * 1024;  // 3 MiB: gRPC default max is 4 MiB
int psch_otel_log_delay_ms = 100;
int psch_otel_metric_interval_ms = 5000;
bool psch_debug_force_locked_overflow = false;

// Log level options (matches PostgreSQL's server_message_level_options pattern)
// clang-format off
static const std::array<config_enum_entry, 13> log_elevel_options = {{
    {"debug5",  DEBUG5,  false},
    {"debug4",  DEBUG4,  false},
    {"debug3",  DEBUG3,  false},
    {"debug2",  DEBUG2,  false},
    {"debug1",  DEBUG1,  false},
    {"log",     LOG,     false},
    {"info",    INFO,    false},
    {"notice",  NOTICE,  false},
    {"warning", WARNING, false},
    {"error",   ERROR,   false},
    {"fatal",   FATAL,   false},
    {"panic",   PANIC,   false},
    {nullptr,   0,       false},
}};
// clang-format on

extern "C" {

// Check hook to ensure queue_capacity is a power of 2.
// Parameters follow PostgreSQL GUC check hook signature.
static bool check_psch_queue_capacity(int* newval, void** extra [[maybe_unused]],
                                      GucSource source [[maybe_unused]]) {
  // Check if value is positive and a power of 2
  if (*newval <= 0) {
    GUC_check_errdetail("pg_stat_ch.queue_capacity must be positive.");
    return false;
  }

  // Check if power of 2: value & (value - 1) == 0
  if ((*newval & (*newval - 1)) != 0) {
    GUC_check_errdetail(
        "pg_stat_ch.queue_capacity must be a power of 2 "
        "(e.g., 1024, 2048, 4096, 8192, 16384, 32768, 65536).");
    return false;
  }

  return true;
}

void PschInitGuc(void) {
  // clang-format off
  DefineCustomBoolVariable(
      "pg_stat_ch.enabled",                                   // name
      "Enable or disable pg_stat_ch query telemetry collection.",  // short_desc
      nullptr,                                                // long_desc
      &psch_enabled,                                          // valueAddr
      true,                                                   // bootValue
      PGC_SIGHUP,                                             // context
      0,                                                      // flags
      nullptr, nullptr, nullptr);                             // hooks

  DefineCustomBoolVariable(
      "pg_stat_ch.use_otel",
      "Send metrics through OpenTelemetry instead of ClickHouse.",
      "When enabled, stats will be sent to an OTel endpoint instead of ClickHouse.",
      &psch_use_otel,
      false,
      PGC_POSTMASTER,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_host",
      "ClickHouse server hostname.",
      nullptr,
      &psch_clickhouse_host,
      "localhost",
      PGC_POSTMASTER,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.clickhouse_port",
      "ClickHouse server native protocol port.",
      nullptr,
      &psch_clickhouse_port,
      9000,           // bootValue
      1, 65535,       // min, max
      PGC_POSTMASTER,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_user",
      "ClickHouse user name.",
      nullptr,
      &psch_clickhouse_user,
      "default",
      PGC_POSTMASTER,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_password",
      "ClickHouse user password.",
      nullptr,
      &psch_clickhouse_password,
      "",
      PGC_POSTMASTER,
      GUC_SUPERUSER_ONLY,
      nullptr, nullptr, nullptr);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_database",
      "ClickHouse database name for telemetry storage.",
      nullptr,
      &psch_clickhouse_database,
      "pg_stat_ch",
      PGC_POSTMASTER,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomBoolVariable(
      "pg_stat_ch.clickhouse_use_tls",
      "Enable TLS for ClickHouse connections.",
      nullptr,
      &psch_clickhouse_use_tls,
      false,
      PGC_POSTMASTER,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomBoolVariable(
      "pg_stat_ch.clickhouse_skip_tls_verify",
      "Skip TLS certificate verification (insecure, for testing only).",
      nullptr,
      &psch_clickhouse_skip_tls_verify,
      false,
      PGC_POSTMASTER,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomStringVariable(
      "pg_stat_ch.otel_endpoint",
      "OpenTelemetry gRPC endpoint (host:port).",
      nullptr,
      &psch_otel_endpoint,
      "localhost:4317",
      PGC_POSTMASTER,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomStringVariable(
      "pg_stat_ch.hostname",
      "Override the hostname of the current machine.",
      nullptr,
      &psch_hostname,
      "",
      PGC_POSTMASTER,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.queue_capacity",
      "Maximum number of events in the shared memory queue (must be a power of 2).",
      nullptr,
      &psch_queue_capacity,
      131072,             // bootValue
      1024, 4194304,      // min, max
      PGC_POSTMASTER,
      0,
      check_psch_queue_capacity, nullptr, nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.flush_interval_ms",
      "Interval in milliseconds between ClickHouse export batches.",
      nullptr,
      &psch_flush_interval_ms,
      200,            // bootValue
      100, 60000,     // min, max
      PGC_SIGHUP,
      GUC_UNIT_MS,
      nullptr, nullptr, nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.batch_max",
      "Maximum number of events per ClickHouse insert batch.",
      nullptr,
      &psch_batch_max,
      200000,           // bootValue
      1, 1000000,       // min, max
      PGC_SIGHUP,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.otel_log_queue_size",
      "OTel batch log processor queue size.",
      "Maximum number of log records buffered before dropping. "
      "Only used when use_otel is enabled.",
      &psch_otel_log_queue_size,
      65536,              // bootValue
      512, 1048576,       // min, max
      PGC_POSTMASTER,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.otel_log_batch_size",
      "OTel batch log processor export batch size.",
      "Number of log records per gRPC export call. "
      "Only used when use_otel is enabled.",
      &psch_otel_log_batch_size,
      8192,               // bootValue
      1, 131072,          // min, max
      PGC_POSTMASTER,
      0,
      nullptr, nullptr, nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.otel_log_max_bytes",
      "Maximum gRPC message size (bytes) for OTel log export.",
      "Each gRPC ExportLogs call is capped at this many serialized bytes. "
      "The gRPC default is 4 MiB; this default leaves a safety margin. "
      "Only used when use_otel is enabled.",
      &psch_otel_log_max_bytes,
      3 * 1024 * 1024,        // bootValue: 3 MiB
      65536, 64 * 1024 * 1024,  // min: 64 KiB, max: 64 MiB
      PGC_POSTMASTER,
      GUC_UNIT_BYTE,
      nullptr, nullptr, nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.otel_log_delay_ms",
      "OTel batch log processor schedule delay in milliseconds.",
      "Time between batch export attempts. "
      "Only used when use_otel is enabled.",
      &psch_otel_log_delay_ms,
      100,                // bootValue
      10, 60000,          // min, max
      PGC_POSTMASTER,
      GUC_UNIT_MS,
      nullptr, nullptr, nullptr);

  DefineCustomIntVariable(
      "pg_stat_ch.otel_metric_interval_ms",
      "OTel metric export interval in milliseconds.",
      "How often the metrics reader exports aggregated histograms via gRPC. "
      "Metrics export is asynchronous and does not block the bgworker.",
      &psch_otel_metric_interval_ms,
      5000,               // bootValue
      100, 300000,         // min, max (100ms to 5min)
      PGC_POSTMASTER,
      GUC_UNIT_MS,
      nullptr, nullptr, nullptr);

  DefineCustomEnumVariable(
      "pg_stat_ch.log_min_elevel",
      "Minimum error level to capture via emit_log_hook.",
      "Set to 'warning' (default) to capture warnings and errors, "
      "'error' for errors only, or 'debug5' for all messages.",
      &psch_log_min_elevel,
      WARNING,
      log_elevel_options.data(),
      PGC_SUSET,
      0,
      nullptr, nullptr, nullptr);
  DefineCustomBoolVariable(
      "pg_stat_ch.debug_force_locked_overflow",
      "Force HandleOverflow in locked path (debug/test only).",
      "When enabled, TryEnqueueLocked always calls HandleOverflow regardless of "
      "queue state. Used to deterministically test the overflow-under-lock deadlock fix.",
      &psch_debug_force_locked_overflow,
      false,
      PGC_SUSET,
      0,
      nullptr, nullptr, nullptr);
  // clang-format on

  EmitWarningsOnPlaceholders("pg_stat_ch");
}

}  // extern "C"
