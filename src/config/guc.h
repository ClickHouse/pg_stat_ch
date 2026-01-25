// pg_stat_ch GUC (Grand Unified Configuration) definitions
#ifndef PG_STAT_CH_SRC_CONFIG_GUC_H_
#define PG_STAT_CH_SRC_CONFIG_GUC_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

// GUC variables (defined in guc.cc)
extern bool psch_enabled;
extern char* psch_clickhouse_host;
extern int psch_clickhouse_port;
extern char* psch_clickhouse_user;
extern char* psch_clickhouse_password;
extern char* psch_clickhouse_database;
extern int psch_queue_capacity;
extern int psch_flush_interval_ms;
extern int psch_batch_max;

// Initialize GUC variables
void PschInitGuc(void);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_CONFIG_GUC_H_
