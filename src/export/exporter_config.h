// Owned snapshot of exporter settings, copied from caller-borrowed C config
#ifndef PG_STAT_CH_SRC_EXPORT_EXPORTER_CONFIG_H_
#define PG_STAT_CH_SRC_EXPORT_EXPORTER_CONFIG_H_

#include <csignal>
#include <cstddef>
#include <string>

#include "export/psch_exporter.h"

struct ExporterConfig {
  // Copies every string, may throw std::bad_alloc
  explicit ExporterConfig(const PschExporterConfig& c)
      : mode(c.mode),
        service_version(Str(c.service_version)),
        clickhouse_host(Str(c.clickhouse_host)),
        clickhouse_port(c.clickhouse_port),
        clickhouse_user(Str(c.clickhouse_user)),
        clickhouse_password(Str(c.clickhouse_password)),
        clickhouse_database(Str(c.clickhouse_database)),
        clickhouse_use_tls(c.clickhouse_use_tls),
        clickhouse_skip_tls_verify(c.clickhouse_skip_tls_verify),
        otel_endpoint(Str(c.otel_endpoint)),
        hostname(Str(c.hostname)),
        otel_log_batch_size(c.otel_log_batch_size),
        otel_log_max_bytes(c.otel_log_max_bytes),
        otel_log_delay_ms(c.otel_log_delay_ms),
        arrow_max_block_bytes(c.arrow_max_block_bytes),
        extra_attributes(Str(c.extra_attributes)),
        arrow_dump_dir(Str(c.arrow_dump_dir)),
        cancel_flag(c.cancel_flag) {}

  PschExporterMode mode;
  std::string service_version;

  std::string clickhouse_host;
  int clickhouse_port;
  std::string clickhouse_user;
  std::string clickhouse_password;
  std::string clickhouse_database;
  bool clickhouse_use_tls;
  bool clickhouse_skip_tls_verify;

  std::string otel_endpoint;
  std::string hostname;
  int otel_log_batch_size;
  int otel_log_max_bytes;
  int otel_log_delay_ms;

  size_t arrow_max_block_bytes;
  std::string extra_attributes;
  std::string arrow_dump_dir;

  const volatile sig_atomic_t* cancel_flag;

 private:
  static std::string Str(const char* s) { return s != nullptr ? std::string(s) : std::string(); }
};

#endif  // PG_STAT_CH_SRC_EXPORT_EXPORTER_CONFIG_H_
