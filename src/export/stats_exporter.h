// pg_stat_ch statistics exporter
#ifndef PG_STAT_CH_SRC_EXPORT_STATS_EXPORTER_H_
#define PG_STAT_CH_SRC_EXPORT_STATS_EXPORTER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

// Initialize the ClickHouse exporter (called once at bgworker startup)
bool PschExporterInit(void);

// Export one batch. Returns number of events exported (0 = queue empty or error).
int PschExportBatch(void);

// Shutdown the exporter and close connection
void PschExporterShutdown(void);

// Retry state management for exponential backoff
void PschResetRetryState(void);
int PschGetRetryDelayMs(void);
int PschGetConsecutiveFailures(void);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_EXPORT_STATS_EXPORTER_H_
