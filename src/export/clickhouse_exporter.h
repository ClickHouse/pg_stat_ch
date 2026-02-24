// pg_stat_ch ClickHouse exporter
#ifndef PG_STAT_CH_SRC_EXPORT_CLICKHOUSE_EXPORTER_H_
#define PG_STAT_CH_SRC_EXPORT_CLICKHOUSE_EXPORTER_H_

#include "exporter_interface.h"
#include <memory>

std::unique_ptr<StatsExporter> MakeClickHouseExporter();

#endif  // PG_STAT_CH_SRC_EXPORT_CLICKHOUSE_EXPORTER_H_
