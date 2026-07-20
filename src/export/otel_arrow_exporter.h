#ifndef PG_STAT_CH_SRC_EXPORT_OTEL_ARROW_EXPORTER_H_
#define PG_STAT_CH_SRC_EXPORT_OTEL_ARROW_EXPORTER_H_

#include "export/exporter_interface.h"

#include <memory>

// Build an Arrow-IPC-emitting StatsExporter that ships through a held
// OTelExporter for transport. Implements the StatLC/StatHC interface end-to-
// end with typed Arrow column wrappers (LC -> StringDictionary32Builder,
// HC -> plain typed builder), so column type choices stay in one place
// instead of being duplicated across the column-emission path and a
// separate IPC builder.
//
// Gated by pg_stat_ch.use_unified_arrow_exporter (default off). The legacy
// arrow_batch.cc path stays alive when the GUC is off — wire shape there
// targets clickgres-platform's query_logs_arrow, which is a different
// table from events_raw and retains the sprintf-decimal id encoding.
std::unique_ptr<StatsExporter> MakeUnifiedArrowExporter();

#endif  // PG_STAT_CH_SRC_EXPORT_OTEL_ARROW_EXPORTER_H_
