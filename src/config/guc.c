// pg_stat_ch GUC (Grand Unified Configuration) implementation
//
// Memory surface (OTEL_REWRITE_DESIGN.md §6): pg_stat_ch.memory_limit is the
// one operator knob; queue_capacity / string_area_size / export_buffer_size
// are -1=auto expert overrides.  Resolution and the SetConfigOption
// write-back of effective values live in src/config/memory_budget.c.
//
// Deleted outright: pg_stat_ch.otel_log_queue_size and
// pg_stat_ch.otel_metric_interval_ms.  Because MarkGUCPrefixReserved makes
// the prefix reserved, SET/ALTER SYSTEM of the deleted names now errors —
// release notes must flag the fleet-template / postgresql.auto.conf scrub.
//
// Five legacy knobs survive one release as hidden bridges (see the bridge
// section at the bottom of PschInitGuc).

#include "postgres.h"

#include <limits.h>

#include "utils/guc.h"

#include "config/guc.h"

// GUC variable storage.  Initializers mirror the boot values so the globals
// are sane even when _PG_init bails out before registration (extension loaded
// without shared_preload_libraries).
bool psch_enabled = true;
bool psch_use_otel = false;
char* psch_clickhouse_host = NULL;
int psch_clickhouse_port = 9000;
char* psch_clickhouse_user = NULL;
char* psch_clickhouse_password = NULL;
char* psch_clickhouse_database = NULL;
bool psch_clickhouse_use_tls = false;
bool psch_clickhouse_skip_tls_verify = false;
char* psch_otel_endpoint = NULL;
char* psch_otel_headers = NULL;
char* psch_otel_ca_file = NULL;
char* psch_hostname = NULL;
int psch_memory_limit_mb = 160;
int psch_queue_capacity = -1;
int psch_string_area_size = -1;
int psch_export_buffer_size_mb = -1;
int psch_flush_interval_ms = 500;
int psch_export_timeout_ms = 1000;
int psch_log_min_elevel = WARNING;
int psch_min_duration_us = 0;
int psch_normalize_cache_max = 32768;
double psch_sample_rate = 1.0;
bool psch_otel_arrow_passthrough = false;
char* psch_extra_attributes = NULL;
char* psch_debug_arrow_dump_dir = NULL;
bool psch_debug_force_locked_overflow = false;

// Hidden one-release compat bridges (-1 = unset; see guc.h).
int psch_bridge_batch_max = -1;
int psch_bridge_otel_max_block_bytes = -1;
int psch_bridge_otel_log_max_bytes = -1;
int psch_bridge_otel_log_batch_size = -1;
int psch_bridge_otel_log_delay_ms = -1;

// Log level options (matches PostgreSQL's server_message_level_options pattern)
// clang-format off
static const struct config_enum_entry log_elevel_options[] = {
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
    {NULL,      0,       false},
};
// clang-format on

// The declared GUC range is -1..4096 so -1 (= auto from shared_buffers) can
// pass the built-in range check; the real lower bound for explicit values is
// enforced here.
static bool CheckMemoryLimit(int* newval, void** extra pg_attribute_unused(),
                             GucSource source pg_attribute_unused()) {
  if (*newval == -1) {
    return true;
  }
  // 32 MB is the smallest budget the component floors actually fit within
  // (ring floor ~2 MB + intern + export-arena floor 8 MB + DSA floor 8 MB).  A
  // value below this would always be silently auto-raised, so reject it as a
  // configuration error rather than honor it dishonestly.
  if (*newval < 32) {
    GUC_check_errdetail(
        "pg_stat_ch.memory_limit must be at least 32 MB, "
        "or -1 for automatic sizing from shared_buffers.");
    return false;
  }
  return true;
}

// -1 = auto.  Explicit values are rounded UP to the next power of 2 in place
// (replaces the old must-be-pow2 startup error).  The declared maximum
// (4194304 = 2^22) is itself a power of 2 and range checking runs before this
// hook, so rounding cannot exceed the maximum.
static bool CheckQueueCapacity(int* newval, void** extra pg_attribute_unused(),
                               GucSource source pg_attribute_unused()) {
  uint32 pow2;

  if (*newval == -1) {
    return true;
  }
  if (*newval <= 0) {
    GUC_check_errdetail("pg_stat_ch.queue_capacity must be positive, or -1 for automatic sizing.");
    return false;
  }

  pow2 = 1;
  while (pow2 < (uint32)*newval) {
    pow2 <<= 1;
  }
  *newval = (int)pow2;
  return true;
}

static bool CheckStringAreaSize(int* newval, void** extra pg_attribute_unused(),
                                GucSource source pg_attribute_unused()) {
  if (*newval == -1) {
    return true;
  }
  if (*newval < 8) {
    GUC_check_errdetail(
        "pg_stat_ch.string_area_size must be between 8 and 1024 MB, "
        "or -1 for automatic sizing.");
    return false;
  }
  return true;
}

static bool CheckExportBufferSize(int* newval, void** extra pg_attribute_unused(),
                                  GucSource source pg_attribute_unused()) {
  if (*newval == -1) {
    return true;
  }
  if (*newval < 8) {
    GUC_check_errdetail(
        "pg_stat_ch.export_buffer_size must be between 8 and 512 MB, "
        "or -1 for automatic sizing.");
    return false;
  }
  return true;
}

// One-time deprecation WARNING for a bridge GUC explicitly set by the
// operator.  PGC_S_DEFAULT covers definition time; PGC_S_DYNAMIC_DEFAULT
// covers our own write-backs — neither is operator intent.
static bool BridgeDeprecationCheck(const int* newval, GucSource source, bool* warned,
                                   const char* old_name, const char* successor) {
  if (*newval == -1 || source <= PGC_S_DYNAMIC_DEFAULT || *warned) {
    return true;
  }
  *warned = true;
  ereport(WARNING,
          (errmsg("pg_stat_ch.%s is deprecated and will be removed in the next release", old_name),
           errhint("Use pg_stat_ch.%s instead.", successor)));
  return true;
}

static bool CheckBridgeBatchMax(int* newval, void** extra pg_attribute_unused(), GucSource source) {
  static bool warned = false;
  return BridgeDeprecationCheck(newval, source, &warned, "batch_max", "export_buffer_size");
}

static bool CheckBridgeOtelMaxBlockBytes(int* newval, void** extra pg_attribute_unused(),
                                         GucSource source) {
  static bool warned = false;
  return BridgeDeprecationCheck(newval, source, &warned, "otel_max_block_bytes",
                                "export_buffer_size");
}

static bool CheckBridgeOtelLogMaxBytes(int* newval, void** extra pg_attribute_unused(),
                                       GucSource source) {
  static bool warned = false;
  return BridgeDeprecationCheck(newval, source, &warned, "otel_log_max_bytes",
                                "export_buffer_size");
}

static bool CheckBridgeOtelLogBatchSize(int* newval, void** extra pg_attribute_unused(),
                                        GucSource source) {
  static bool warned = false;
  return BridgeDeprecationCheck(newval, source, &warned, "otel_log_batch_size",
                                "export_buffer_size");
}

static bool CheckBridgeOtelLogDelayMs(int* newval, void** extra pg_attribute_unused(),
                                      GucSource source) {
  static bool warned = false;
  return BridgeDeprecationCheck(newval, source, &warned, "otel_log_delay_ms", "export_timeout_ms");
}

// When adding a GUC here, also update test/regression/expected/guc.out.
void PschInitGuc(void) {
  // clang-format off
  DefineCustomBoolVariable(
      "pg_stat_ch.enabled",                                   // name
      "Enable or disable pg_stat_ch query telemetry collection.",  // short_desc
      NULL,                                                   // long_desc
      &psch_enabled,                                          // valueAddr
      true,                                                   // bootValue
      PGC_SIGHUP,                                             // context
      0,                                                      // flags
      NULL, NULL, NULL);                                      // hooks

  DefineCustomBoolVariable(
      "pg_stat_ch.use_otel",
      "Send telemetry through OpenTelemetry instead of ClickHouse.",
      "When enabled, events are sent to an OTLP/HTTP endpoint instead of ClickHouse.",
      &psch_use_otel,
      false,
      PGC_POSTMASTER,
      0,
      NULL, NULL, NULL);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_host",
      "ClickHouse server hostname.",
      NULL,
      &psch_clickhouse_host,
      "localhost",
      PGC_POSTMASTER,
      0,
      NULL, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.clickhouse_port",
      "ClickHouse server native protocol port.",
      NULL,
      &psch_clickhouse_port,
      9000,           // bootValue
      1, 65535,       // min, max
      PGC_POSTMASTER,
      0,
      NULL, NULL, NULL);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_user",
      "ClickHouse user name.",
      NULL,
      &psch_clickhouse_user,
      "default",
      PGC_POSTMASTER,
      0,
      NULL, NULL, NULL);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_password",
      "ClickHouse user password.",
      NULL,
      &psch_clickhouse_password,
      "",
      PGC_POSTMASTER,
      GUC_SUPERUSER_ONLY,
      NULL, NULL, NULL);

  DefineCustomStringVariable(
      "pg_stat_ch.clickhouse_database",
      "ClickHouse database name for telemetry storage.",
      NULL,
      &psch_clickhouse_database,
      "pg_stat_ch",
      PGC_POSTMASTER,
      0,
      NULL, NULL, NULL);

  DefineCustomBoolVariable(
      "pg_stat_ch.clickhouse_use_tls",
      "Enable TLS for ClickHouse connections.",
      NULL,
      &psch_clickhouse_use_tls,
      false,
      PGC_POSTMASTER,
      0,
      NULL, NULL, NULL);

  DefineCustomBoolVariable(
      "pg_stat_ch.clickhouse_skip_tls_verify",
      "Skip TLS certificate verification (insecure, for testing only).",
      NULL,
      &psch_clickhouse_skip_tls_verify,
      false,
      PGC_POSTMASTER,
      0,
      NULL, NULL, NULL);

  DefineCustomStringVariable(
      "pg_stat_ch.otel_endpoint",
      "OTLP/HTTP endpoint URL for telemetry export.",
      "The scheme decides TLS (http or https). A bare host:port is treated as "
      "http://host:port; the request path defaults to /v1/logs when absent.",
      &psch_otel_endpoint,
      "http://localhost:4318",
      PGC_POSTMASTER,
      0,
      NULL, NULL, NULL);

  DefineCustomStringVariable(
      "pg_stat_ch.otel_headers",
      "Static HTTP headers added to every OTLP request.",
      "Newline-separated \"Name: value\" pairs, typically used for authentication "
      "(parity with OTEL_EXPORTER_OTLP_HEADERS).",
      &psch_otel_headers,
      "",
      PGC_SIGHUP,
      GUC_SUPERUSER_ONLY,
      NULL, NULL, NULL);

  DefineCustomStringVariable(
      "pg_stat_ch.otel_ca_file",
      "PEM CA bundle used to verify https OTLP endpoints.",
      "Empty means the system default trust store.",
      &psch_otel_ca_file,
      "",
      PGC_SIGHUP,
      0,
      NULL, NULL, NULL);

  DefineCustomStringVariable(
      "pg_stat_ch.hostname",
      "Override the hostname of the current machine.",
      NULL,
      &psch_hostname,
      "",
      PGC_POSTMASTER,
      0,
      NULL, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.memory_limit",
      "Total memory budget for pg_stat_ch (ring queue + intern table + string area + export arena).",
      "The default equals the footprint of the pre-0.4 defaults. -1 sizes the budget "
      "automatically as shared_buffers/16 clamped to [48 MB, 256 MB]. Component overrides "
      "raise the effective budget with a WARNING; resolved sizes are reported by "
      "pg_stat_ch_memory().",
      &psch_memory_limit_mb,
      160,             // bootValue
      -1, 4096,        // min (-1 = auto; explicit minimum 16 via check hook), max
      PGC_POSTMASTER,
      GUC_UNIT_MB,
      CheckMemoryLimit, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.queue_capacity",
      "Number of slots in the shared memory event queue.",
      "-1 derives the largest power of 2 whose ring + intern-table cost fits half of "
      "pg_stat_ch.memory_limit. Explicit values are rounded up to the next power of 2.",
      &psch_queue_capacity,
      -1,              // bootValue: auto
      -1, 4194304,     // min (-1 = auto), max (2^22)
      PGC_POSTMASTER,
      0,
      CheckQueueCapacity, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.string_area_size",
      "Size in MB of the DSA area for variable-length string storage.",
      "Query text and error messages are stored in a DSA (Dynamic Shared Memory Area) "
      "rather than inline in ring buffer slots. -1 lets the area absorb whatever remains "
      "of pg_stat_ch.memory_limit after the other components are sized.",
      &psch_string_area_size,
      -1,              // bootValue: auto
      -1, 1024,        // min (-1 = auto; explicit minimum 8 via check hook), max
      PGC_POSTMASTER,
      GUC_UNIT_MB,
      CheckStringAreaSize, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.export_buffer_size",
      "Size in MB of the bgworker export arena.",
      "Covers the event staging chunk, Arrow build scratch, OTLP encode buffer and "
      "network buffers. -1 = 1/8 of pg_stat_ch.memory_limit clamped to [8 MB, 64 MB]. "
      "Replaces batch_max, otel_max_block_bytes, otel_log_max_bytes and otel_log_batch_size.",
      &psch_export_buffer_size_mb,
      -1,              // bootValue: auto
      -1, 512,         // min (-1 = auto; explicit minimum 8 via check hook), max
      PGC_POSTMASTER,
      GUC_UNIT_MB,
      CheckExportBufferSize, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.flush_interval_ms",
      "Interval in milliseconds between export batches.",
      NULL,
      &psch_flush_interval_ms,
      500,            // bootValue
      100, 60000,     // min, max
      PGC_SIGHUP,
      GUC_UNIT_MS,
      NULL, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.export_timeout_ms",
      "Deadline in milliseconds for a single export request.",
      "Bounds connect plus request/response per export call so the bgworker cannot "
      "block indefinitely on a slow collector. Renames otel_log_delay_ms; the default "
      "leaves room for a TLS reconnect inside one request.",
      &psch_export_timeout_ms,
      1000,               // bootValue
      10, 60000,          // min, max
      PGC_SIGHUP,
      GUC_UNIT_MS,
      NULL, NULL, NULL);

  DefineCustomEnumVariable(
      "pg_stat_ch.log_min_elevel",
      "Minimum error level to capture via emit_log_hook.",
      "Set to 'warning' (default) to capture warnings and errors, "
      "'error' for errors only, or 'debug5' for all messages.",
      &psch_log_min_elevel,
      WARNING,
      log_elevel_options,
      PGC_SUSET,
      0,
      NULL, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.min_duration_us",
      "Minimum query duration in microseconds to always capture.",
      "Queries faster than this threshold are sampled at pg_stat_ch.sample_rate "
      "instead of being captured unconditionally. Set to 0 to capture all queries.",
      &psch_min_duration_us,
      0,              // bootValue
      0, INT_MAX,     // min, max
      PGC_SUSET,
      0,
      NULL, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.normalize_cache_max",
      "Maximum entries in the per-backend normalized query cache.",
      "Controls the LRU cache that bridges parse-time normalization to "
      "executor-time event export. Takes effect at first query after backend start. "
      "Per-backend memory, deliberately outside pg_stat_ch.memory_limit.",
      &psch_normalize_cache_max,
      32768,          // bootValue
      64, 65536,      // min, max
      PGC_SUSET,
      0,
      NULL, NULL, NULL);

  DefineCustomRealVariable(
      "pg_stat_ch.sample_rate",
      "Sampling rate for queries below min_duration_us.",
      "Fraction of sub-threshold queries to capture (0.0 = none, 1.0 = all). "
      "Queries at or above min_duration_us are always captured regardless.",
      &psch_sample_rate,
      1.0,            // bootValue
      0.0, 1.0,       // min, max
      PGC_SUSET,
      0,
      NULL, NULL, NULL);

  DefineCustomBoolVariable(
      "pg_stat_ch.otel_arrow_passthrough",
      "Send Arrow IPC batches via OTel instead of per-record proto.",
      "When enabled together with use_otel, the bgworker builds Arrow record batches "
      "from the event queue and sends them as opaque OTLP LogRecord bodies.",
      &psch_otel_arrow_passthrough,
      false,
      PGC_SIGHUP,
      0,
      NULL, NULL, NULL);

  DefineCustomStringVariable(
      "pg_stat_ch.extra_attributes",
      "Key-value pairs appended to exported Arrow batches.",
      "Semicolon-separated k:v pairs for resource columns: "
      "'instance_ubid:abc;server_role:primary;read_replica_type:regional;region:us-east-1'.",
      &psch_extra_attributes,
      "",
      PGC_SIGHUP,
      0,
      NULL, NULL, NULL);

  DefineCustomStringVariable(
      "pg_stat_ch.debug_arrow_dump_dir",
      "Directory for dumping raw Arrow IPC payloads before send.",
      "When non-empty, each Arrow IPC payload is written to this directory before "
      "being sent through OTLP. Intended for test validation and debugging only.",
      &psch_debug_arrow_dump_dir,
      "",
      PGC_SIGHUP,
      0,
      NULL, NULL, NULL);

  DefineCustomBoolVariable(
      "pg_stat_ch.debug_force_locked_overflow",
      "Force HandleOverflow in locked path (debug/test only).",
      "When enabled, TryEnqueueLocked always calls HandleOverflow regardless of "
      "queue state. Used to deterministically test the overflow-under-lock deadlock fix.",
      &psch_debug_force_locked_overflow,
      false,
      PGC_SUSET,
      0,
      NULL, NULL, NULL);

  // --- Deprecated one-release bridges --------------------------------------
  // Hidden (GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE), default -1 = unset; explicit
  // use draws a one-time deprecation WARNING naming the successor.  Only
  // memory_budget.c may read these; it folds them in iff the successor GUC is
  // still -1/at default:
  //   batch_max, otel_log_batch_size -> export_buffer_size: arena sized so its
  //       staging share holds that many PschEvents.
  //   otel_max_block_bytes -> export_buffer_size: arena sized so its Arrow
  //       scratch share covers the requested block bytes.
  //   otel_log_max_bytes   -> export_buffer_size: arena sized so its encode
  //       share covers it (the 16 MiB encode ceiling still applies).
  //   otel_log_delay_ms    -> export_timeout_ms via a dynamic-default
  //       write-back in PschMemoryBudgetWriteBack().
  // Bridge-derived arena sizes are clamped to [8 MB, 64 MB] (the old 200000
  // event batch_max default implied ~880 MB of transient staging — that is the
  // bug this rewrite deletes, deliberately not honored).  Contexts are
  // POSTMASTER (batch_max and otel_max_block_bytes were SIGHUP) because
  // folding happens once at budget resolution.

  DefineCustomIntVariable(
      "pg_stat_ch.batch_max",
      "DEPRECATED: use pg_stat_ch.export_buffer_size.",
      "Maximum number of events per export batch. Folded into the export arena "
      "staging size when export_buffer_size is -1.",
      &psch_bridge_batch_max,
      -1,                 // bootValue: unset
      -1, 1000000,        // min, max
      PGC_POSTMASTER,
      GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
      CheckBridgeBatchMax, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.otel_max_block_bytes",
      "DEPRECATED: use pg_stat_ch.export_buffer_size.",
      "Soft byte budget for a single Arrow IPC batch. Folded into the export arena "
      "Arrow scratch size when export_buffer_size is -1.",
      &psch_bridge_otel_max_block_bytes,
      -1,                        // bootValue: unset
      -1, 16 * 1024 * 1024,      // min, max: 16 MiB
      PGC_POSTMASTER,
      GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_UNIT_BYTE,
      CheckBridgeOtelMaxBlockBytes, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.otel_log_max_bytes",
      "DEPRECATED: use pg_stat_ch.export_buffer_size.",
      "Soft byte budget for a single OTLP export request. Folded into the export "
      "arena encode buffer size when export_buffer_size is -1.",
      &psch_bridge_otel_log_max_bytes,
      -1,                        // bootValue: unset
      -1, 64 * 1024 * 1024,      // min, max: 64 MiB
      PGC_POSTMASTER,
      GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_UNIT_BYTE,
      CheckBridgeOtelLogMaxBytes, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.otel_log_batch_size",
      "DEPRECATED: use pg_stat_ch.export_buffer_size.",
      "Maximum records per OTLP export call. Folded into the export arena staging "
      "size when export_buffer_size is -1.",
      &psch_bridge_otel_log_batch_size,
      -1,                 // bootValue: unset
      -1, 131072,         // min, max
      PGC_POSTMASTER,
      GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
      CheckBridgeOtelLogBatchSize, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_ch.otel_log_delay_ms",
      "DEPRECATED: use pg_stat_ch.export_timeout_ms.",
      "Deadline in milliseconds for a single export call. Becomes the dynamic "
      "default of export_timeout_ms when that GUC is not set explicitly.",
      &psch_bridge_otel_log_delay_ms,
      -1,                 // bootValue: unset
      -1, 60000,          // min, max
      PGC_POSTMASTER,
      GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_UNIT_MS,
      CheckBridgeOtelLogDelayMs, NULL, NULL);
  // clang-format on

  MarkGUCPrefixReserved("pg_stat_ch");
}
