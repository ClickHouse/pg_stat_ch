// PostgreSQL-independent C interface, translate C++ exceptions into results
#ifndef PG_STAT_CH_SRC_EXPORT_PSCH_EXPORTER_H_
#define PG_STAT_CH_SRC_EXPORT_PSCH_EXPORTER_H_

#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
#define PSCH_EXPORTER_NOEXCEPT noexcept
extern "C" {
#else
#define PSCH_EXPORTER_NOEXCEPT
#endif

// Input contract for string lengths, checked at entry
#define PSCH_EXPORT_NAME_MAX        63
#define PSCH_EXPORT_CLIENT_ADDR_MAX 45
#define PSCH_EXPORT_SQLSTATE_MAX    5
#define PSCH_EXPORT_QUERY_MAX       2048
#define PSCH_EXPORT_ERR_MESSAGE_MAX 2048

#define PSCH_EXPORTER_TEXT_MAX     256
#define PSCH_EXPORTER_WARNINGS_MAX 4

typedef struct PschExporter PschExporter;

typedef enum PschExporterMode {
  PSCH_EXPORTER_CLICKHOUSE = 0,
  PSCH_EXPORTER_OTEL = 1,               // OTLP log records over gRPC
  PSCH_EXPORTER_OTEL_ARROW_LEGACY = 2,  // query_logs_arrow Arrow IPC over OTLP
  PSCH_EXPORTER_OTEL_ARROW_UNIFIED = 3  // events_raw Arrow IPC over OTLP
} PschExporterMode;

// Borrow strings until return, copy retained values, NULL means empty
typedef struct PschExporterConfig {
  PschExporterMode mode;
  const char* service_version;

  const char* clickhouse_host;
  int clickhouse_port;
  const char* clickhouse_user;
  const char* clickhouse_password;
  const char* clickhouse_database;
  bool clickhouse_use_tls;
  bool clickhouse_skip_tls_verify;

  const char* otel_endpoint;
  const char* hostname;
  int otel_log_batch_size;
  int otel_log_max_bytes;
  int otel_log_delay_ms;

  size_t arrow_max_block_bytes;
  const char* extra_attributes;  // "key:value;key:value"
  const char* arrow_dump_dir;    // empty disables IPC dumps

  // Polled during blocking ClickHouse I/O, nonzero aborts the operation.
  // Optional, NULL disables cancellation
  const volatile sig_atomic_t* cancel_flag;
} PschExporterConfig;

typedef struct PschExportString {
  const char* data;
  uint32_t len;
} PschExportString;

// Borrowed view of one event, valid until PschExporterExport returns
typedef struct PschExportEvent {
  int64_t ts_unix_us;
  uint64_t duration_us;
  int32_t pid;
  int64_t query_id;
  uint64_t rows;

  PschExportString db_name;       // <= PSCH_EXPORT_NAME_MAX
  PschExportString db_user;       // <= PSCH_EXPORT_NAME_MAX
  PschExportString db_operation;  // <= PSCH_EXPORT_NAME_MAX
  PschExportString query_text;    // <= PSCH_EXPORT_QUERY_MAX
  PschExportString app_name;      // <= PSCH_EXPORT_NAME_MAX
  PschExportString client_addr;   // <= PSCH_EXPORT_CLIENT_ADDR_MAX
  PschExportString err_sqlstate;  // <= PSCH_EXPORT_SQLSTATE_MAX
  PschExportString err_message;   // <= PSCH_EXPORT_ERR_MESSAGE_MAX
  uint8_t err_elevel;

  int64_t shared_blks_hit;
  int64_t shared_blks_read;
  int64_t shared_blks_dirtied;
  int64_t shared_blks_written;
  int64_t local_blks_hit;
  int64_t local_blks_read;
  int64_t local_blks_dirtied;
  int64_t local_blks_written;
  int64_t temp_blks_read;
  int64_t temp_blks_written;

  int64_t shared_blk_read_time_us;
  int64_t shared_blk_write_time_us;
  int64_t local_blk_read_time_us;
  int64_t local_blk_write_time_us;
  int64_t temp_blk_read_time_us;
  int64_t temp_blk_write_time_us;

  int64_t wal_records;
  int64_t wal_fpi;
  uint64_t wal_bytes;

  int64_t cpu_user_time_us;
  int64_t cpu_sys_time_us;

  int32_t jit_functions;
  int32_t jit_generation_time_us;
  int32_t jit_deform_time_us;
  int32_t jit_inlining_time_us;
  int32_t jit_optimization_time_us;
  int32_t jit_emission_time_us;

  int16_t parallel_workers_planned;
  int16_t parallel_workers_launched;
} PschExportEvent;

typedef enum PschExporterStatus {
  PSCH_EXPORTER_OK = 0,
  PSCH_EXPORTER_INVALID_ARGUMENT = 1,  // caller contract violated, state unchanged
  PSCH_EXPORTER_FAILED = 2             // see error text, handle stays usable
} PschExporterStatus;

// Filled by every entry point before it returns, no allocation
typedef struct PschExporterResult {
  PschExporterStatus status;
  bool connected;                      // destination connection state after this call
  uint32_t exported;                   // events accepted by destination during this call
  uint32_t warning_count;              // warnings raised, may exceed retained count
  char error[PSCH_EXPORTER_TEXT_MAX];  // empty when status is PSCH_EXPORTER_OK
  char warnings[PSCH_EXPORTER_WARNINGS_MAX][PSCH_EXPORTER_TEXT_MAX];
} PschExporterResult;

// *exporter is NULL unless status is PSCH_EXPORTER_OK
PschExporterStatus PschExporterCreate(const PschExporterConfig* config, PschExporter** exporter,
                                      PschExporterResult* result) PSCH_EXPORTER_NOEXCEPT;

// Retain previous configuration on failure, allow switching OTLP logs and legacy Arrow
PschExporterStatus PschExporterConfigure(PschExporter* exporter, const PschExporterConfig* config,
                                         PschExporterResult* result) PSCH_EXPORTER_NOEXCEPT;

// Establish destination connection if not already connected
PschExporterStatus PschExporterConnect(PschExporter* exporter,
                                       PschExporterResult* result) PSCH_EXPORTER_NOEXCEPT;

// Report partial acceptance in result->exported, never replay failed batches
PschExporterStatus PschExporterExport(PschExporter* exporter, const PschExportEvent* events,
                                      size_t count,
                                      PschExporterResult* result) PSCH_EXPORTER_NOEXCEPT;

// Safe on NULL
void PschExporterDestroy(PschExporter* exporter) PSCH_EXPORTER_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_EXPORT_PSCH_EXPORTER_H_
