#ifndef PG_STAT_CH_SRC_EXPORT_OTEL_EXPORTER_H_
#define PG_STAT_CH_SRC_EXPORT_OTEL_EXPORTER_H_

#include <memory>

#include "exporter_interface.h"

std::unique_ptr<StatsExporter> MakeOpenTelemetryExporter();

#endif  // PG_STAT_CH_SRC_EXPORT_OTEL_EXPORTER_H_
