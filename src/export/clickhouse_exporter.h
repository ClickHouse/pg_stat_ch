// pg_stat_ch ClickHouse exporter
#ifndef PG_STAT_CH_SRC_EXPORT_CLICKHOUSE_EXPORTER_H_
#define PG_STAT_CH_SRC_EXPORT_CLICKHOUSE_EXPORTER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

// Initialize the ClickHouse exporter (called once at bgworker startup)
bool PschExporterInit(void);

// Export a batch of events to ClickHouse
void PschExportBatch(void);

// Shutdown the exporter and close connection
void PschExporterShutdown(void);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_EXPORT_CLICKHOUSE_EXPORTER_H_
