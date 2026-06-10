// pg_stat_ch export driver — the C ABI surface bgworker.c drives.
//
// Contract (OTEL_REWRITE_DESIGN.md §5a):
//  * PschExporterInit preallocates everything the export path needs; the only
//    failure mode is ereport(FATAL) (a worker without its preallocation is
//    useless; FATAL -> proc_exit(1) -> clean bgworker-only restart).  A false
//    return means "created but not yet connected" — events stay queued and
//    the first export retries the connect.
//  * PschExportBatch never ereport(ERROR)s and performs zero heap allocation.
//    Returns the number of events exported; 0 means queue empty OR failure —
//    load-bearing for the bgworker drain loop, which must break (and let
//    consecutive-failure backoff engage) rather than spin.
#ifndef PG_STAT_CH_SRC_EXPORT_STATS_EXPORTER_H_
#define PG_STAT_CH_SRC_EXPORT_STATS_EXPORTER_H_

// Initialize the statistics exporter (called once at bgworker startup).
// Returns true when the initial backend connect succeeded.
bool PschExporterInit(void);

// Export one staged chunk. Returns number of events exported (0 = queue empty
// or failure).
int PschExportBatch(void);

// Shutdown the exporter and release preallocations.  Idempotent; runs from
// the on_proc_exit chain (possibly after a FATAL mid-init), so it tolerates
// half-initialized state and never ereport(ERROR)s.
void PschExporterShutdown(void);

// Retry state for exponential backoff (read by the bgworker sleep logic).
int PschGetRetryDelayMs(void);
int PschGetConsecutiveFailures(void);

#endif  // PG_STAT_CH_SRC_EXPORT_STATS_EXPORTER_H_
