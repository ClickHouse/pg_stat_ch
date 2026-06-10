// pg_stat_ch GUC (Grand Unified Configuration) declarations
//
// Memory surface (OTEL_REWRITE_DESIGN.md §6): one operator knob
// (memory_limit) + three -1=auto expert overrides (queue_capacity,
// string_area_size, export_buffer_size).  Resolution lives in
// src/config/memory_budget.h.  Legacy knobs kept as hidden one-release
// bridges are marked below; deleted knobs: otel_log_queue_size,
// otel_metric_interval_ms.

#ifndef PG_STAT_CH_GUC_H
#define PG_STAT_CH_GUC_H

#ifdef __cplusplus
extern "C" {
#endif

extern bool psch_enabled;
extern bool psch_use_otel;

// ClickHouse native-protocol connection (unchanged)
extern char* psch_clickhouse_host;
extern int psch_clickhouse_port;
extern char* psch_clickhouse_user;
extern char* psch_clickhouse_password;
extern char* psch_clickhouse_database;
extern bool psch_clickhouse_use_tls;
extern bool psch_clickhouse_skip_tls_verify;

// OTLP/HTTP endpoint configuration.  otel_endpoint is now a URL: scheme
// decides TLS (http/https); a bare "host:port" is accepted and treated as
// http://host:port; the path defaults to /v1/logs when absent.
// Default: "http://localhost:4318".
extern char* psch_otel_endpoint;
extern char* psch_otel_headers;  // NEW: "Name: value\nName2: value2" static headers (auth)
extern char* psch_otel_ca_file;  // NEW: PEM CA bundle path for https endpoints ("" = system)
extern char* psch_hostname;

// Memory budget: the operator knob + auto-defaulted expert overrides.
extern int psch_memory_limit_mb;        // default 160; -1 = auto from shared_buffers
extern int psch_queue_capacity;         // slots; default -1 = auto; explicit values pow2-rounded
extern int psch_string_area_size;       // MB;    default -1 = auto
extern int psch_export_buffer_size_mb;  // MB;    default -1 = auto  (NEW)

// Export timing
extern int psch_flush_interval_ms;
extern int
    psch_export_timeout_ms;  // per-request deadline; default 1000 (renames otel_log_delay_ms)

// Capture behavior (unchanged)
extern int psch_log_min_elevel;
extern int psch_min_duration_us;
extern int psch_normalize_cache_max;  // per-backend; deliberately OUTSIDE memory_limit
extern double psch_sample_rate;
extern bool psch_otel_arrow_passthrough;
extern char* psch_extra_attributes;

// Debug (unchanged)
extern char* psch_debug_arrow_dump_dir;
extern bool psch_debug_force_locked_overflow;

// --- Hidden one-release compat bridges (GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE).
// Honored iff the successor GUC is -1/unset; explicit use logs a deprecation
// WARNING.  guc.c folds them into the resolved values; no other module may
// read these.
extern int psch_bridge_batch_max;             // -> export_buffer_size (staging)
extern int psch_bridge_otel_max_block_bytes;  // -> export_buffer_size (arrow/ipc budget)
extern int psch_bridge_otel_log_max_bytes;    // -> export_buffer_size (encode)
extern int psch_bridge_otel_log_batch_size;   // -> export_buffer_size (encode)
extern int psch_bridge_otel_log_delay_ms;     // -> export_timeout

// Initialize GUC variables
void PschInitGuc(void);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_GUC_H
