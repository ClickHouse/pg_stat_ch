// pg_stat_ch GUC (Grand Unified Configuration) declarations

#ifndef PG_STAT_CH_GUC_H
#define PG_STAT_CH_GUC_H

// Exporter backend enum values
#define PSCH_EXPORTER_CLICKHOUSE    0
#define PSCH_EXPORTER_OPENTELEMETRY 1

extern bool psch_enabled;
extern int psch_exporter_backend;
extern char* psch_clickhouse_host;
extern int psch_clickhouse_port;
extern char* psch_clickhouse_user;
extern char* psch_clickhouse_password;
extern char* psch_clickhouse_database;
extern bool psch_clickhouse_use_tls;
extern bool psch_clickhouse_skip_tls_verify;
extern char* psch_otel_endpoint;
extern int psch_queue_capacity;
extern int psch_flush_interval_ms;
extern int psch_batch_max;
extern int psch_log_min_elevel;
extern bool psch_debug_force_locked_overflow;

#endif  // PG_STAT_CH_GUC_H
