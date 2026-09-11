// Translation between PostgreSQL types and exporter library interface

#include "postgres.h"

#include "datatype/timestamp.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"

#include <string.h>

#include "pg_stat_ch/pg_stat_ch.h"
#include "config/guc.h"
#include "worker/exporter_bridge.h"

StaticAssertDecl(PSCH_MAX_QUERY_LEN == PSCH_EXPORT_QUERY_MAX, "query length contract mismatch");
StaticAssertDecl(PSCH_MAX_ERR_MSG_LEN == PSCH_EXPORT_ERR_MESSAGE_MAX,
                 "error message length contract mismatch");
StaticAssertDecl(PSCH_MAX_APP_NAME_LEN == PSCH_EXPORT_NAME_MAX, "name length contract mismatch");
StaticAssertDecl(PSCH_MAX_CLIENT_ADDR_LEN == PSCH_EXPORT_CLIENT_ADDR_MAX,
                 "client address length contract mismatch");

static const int64 kPostgresEpochOffsetUs =
    (int64)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * USECS_PER_SEC;

static PschExporterMode ExporterMode(void) {
  if (!psch_use_otel)
    return PSCH_EXPORTER_CLICKHOUSE;
  if (!psch_otel_arrow_passthrough)
    return PSCH_EXPORTER_OTEL;
  return psch_use_unified_arrow_exporter ? PSCH_EXPORTER_OTEL_ARROW_UNIFIED
                                         : PSCH_EXPORTER_OTEL_ARROW_LEGACY;
}

void PschBuildExporterConfig(PschExporterConfig* config) {
  *config = (PschExporterConfig){
      .mode = ExporterMode(),
      .service_version = PG_STAT_CH_VERSION,
      .clickhouse_host = psch_clickhouse_host,
      .clickhouse_port = psch_clickhouse_port,
      .clickhouse_user = psch_clickhouse_user,
      .clickhouse_password = psch_clickhouse_password,
      .clickhouse_database = psch_clickhouse_database,
      .clickhouse_use_tls = psch_clickhouse_use_tls,
      .clickhouse_skip_tls_verify = psch_clickhouse_skip_tls_verify,
      .otel_endpoint = psch_otel_endpoint,
      .hostname = psch_hostname,
      .otel_log_batch_size = psch_otel_log_batch_size,
      .otel_log_max_bytes = psch_otel_log_max_bytes,
      .otel_log_delay_ms = psch_otel_log_delay_ms,
      .arrow_max_block_bytes = (size_t)psch_otel_max_block_bytes,
      .extra_attributes = psch_extra_attributes,
      .arrow_dump_dir = psch_debug_arrow_dump_dir,
      .cancel_flag = &ProcDiePending,
  };
}

static PschExportString ClampedString(const char* data, uint32 len, uint32 max, const char* field) {
  if (len > max) {
    elog(WARNING, "pg_stat_ch: invalid %s %u, clamping", field, len);
    len = max;
  }
  return (PschExportString){data, len};
}

void PschToExportEvent(const PschEvent* ev, PschExportEvent* out) {
  const char* op = PschCmdTypeToString(ev->cmd_type);

  *out = (PschExportEvent){
      .ts_unix_us = ev->ts_start + kPostgresEpochOffsetUs,
      .duration_us = ev->duration_us,
      .pid = ev->pid,
      .query_id = (int64)ev->queryid,
      .rows = ev->rows,

      .db_name = ClampedString(ev->datname, ev->datname_len, PSCH_EXPORT_NAME_MAX, "datname_len"),
      .db_user =
          ClampedString(ev->username, ev->username_len, PSCH_EXPORT_NAME_MAX, "username_len"),
      .db_operation = {op, (uint32)strlen(op)},
      .query_text = ClampedString(ev->query, ev->query_len, PSCH_EXPORT_QUERY_MAX, "query_len"),
      .app_name = ClampedString(ev->application_name, ev->application_name_len,
                                PSCH_EXPORT_NAME_MAX, "app_name_len"),
      .client_addr = ClampedString(ev->client_addr, ev->client_addr_len,
                                   PSCH_EXPORT_CLIENT_ADDR_MAX, "client_addr_len"),
      .err_sqlstate = {ev->err_sqlstate,
                       (uint32)strnlen(ev->err_sqlstate, PSCH_EXPORT_SQLSTATE_MAX)},
      .err_message = ClampedString(ev->err_message, ev->err_message_len,
                                   PSCH_EXPORT_ERR_MESSAGE_MAX, "err_message_len"),
      .err_elevel = ev->err_elevel,

      .shared_blks_hit = ev->shared_blks_hit,
      .shared_blks_read = ev->shared_blks_read,
      .shared_blks_dirtied = ev->shared_blks_dirtied,
      .shared_blks_written = ev->shared_blks_written,
      .local_blks_hit = ev->local_blks_hit,
      .local_blks_read = ev->local_blks_read,
      .local_blks_dirtied = ev->local_blks_dirtied,
      .local_blks_written = ev->local_blks_written,
      .temp_blks_read = ev->temp_blks_read,
      .temp_blks_written = ev->temp_blks_written,

      .shared_blk_read_time_us = ev->shared_blk_read_time_us,
      .shared_blk_write_time_us = ev->shared_blk_write_time_us,
      .local_blk_read_time_us = ev->local_blk_read_time_us,
      .local_blk_write_time_us = ev->local_blk_write_time_us,
      .temp_blk_read_time_us = ev->temp_blk_read_time_us,
      .temp_blk_write_time_us = ev->temp_blk_write_time_us,

      .wal_records = ev->wal_records,
      .wal_fpi = ev->wal_fpi,
      .wal_bytes = ev->wal_bytes,

      .cpu_user_time_us = ev->cpu_user_time_us,
      .cpu_sys_time_us = ev->cpu_sys_time_us,

      .jit_functions = ev->jit_functions,
      .jit_generation_time_us = ev->jit_generation_time_us,
      .jit_deform_time_us = ev->jit_deform_time_us,
      .jit_inlining_time_us = ev->jit_inlining_time_us,
      .jit_optimization_time_us = ev->jit_optimization_time_us,
      .jit_emission_time_us = ev->jit_emission_time_us,

      .parallel_workers_planned = ev->parallel_workers_planned,
      .parallel_workers_launched = ev->parallel_workers_launched,
  };
}

// Truncation inside a multibyte character would make text unconvertible
static void ClipToEncoding(char* s) {
  int len = (int)strlen(s);
  s[pg_mbcliplen(s, len, len)] = '\0';
}

void PschLogExporterResult(const char* operation, PschExporterResult* result) {
  uint32 retained = Min(result->warning_count, PSCH_EXPORTER_WARNINGS_MAX);

  ClipToEncoding(result->error);
  for (uint32 i = 0; i < retained; i++) {
    ClipToEncoding(result->warnings[i]);
    ereport(WARNING, (errmsg("pg_stat_ch: %s: %s", operation, result->warnings[i])));
  }
  if (result->warning_count > retained) {
    ereport(WARNING, (errmsg("pg_stat_ch: %s: %u more warnings suppressed", operation,
                             result->warning_count - retained)));
  }
  if (result->status != PSCH_EXPORTER_OK) {
    ereport(WARNING, (errmsg("pg_stat_ch: %s failed: %s", operation, result->error)));
  }
}
