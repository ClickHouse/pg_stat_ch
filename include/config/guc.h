// pg_stat_ch GUC (Grand Unified Configuration) declarations

#ifndef PG_STAT_CH_GUC_H
#define PG_STAT_CH_GUC_H

#ifdef __cplusplus
extern "C" {
#endif

extern bool psch_enabled;
extern bool psch_use_otel;
extern char* psch_clickhouse_host;
extern int psch_clickhouse_port;
extern char* psch_clickhouse_user;
extern char* psch_clickhouse_password;
extern char* psch_clickhouse_database;
extern bool psch_clickhouse_use_tls;
extern bool psch_clickhouse_skip_tls_verify;
extern char* psch_otel_endpoint;
extern char* psch_hostname;
extern int psch_queue_capacity;
extern int psch_string_area_size;
extern int psch_flush_interval_ms;
extern int psch_batch_max;
extern int psch_log_min_elevel;
extern int psch_otel_log_queue_size;
extern int psch_otel_log_batch_size;
extern int psch_otel_log_max_bytes;
extern int psch_otel_log_delay_ms;
extern int psch_otel_metric_interval_ms;
extern bool psch_debug_force_locked_overflow;

// Initialize GUC variables
void PschInitGuc(void);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_GUC_H
