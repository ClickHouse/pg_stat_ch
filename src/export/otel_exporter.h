#ifndef PG_STAT_CH_SRC_EXPORT_OTEL_EXPORTER_H_
#define PG_STAT_CH_SRC_EXPORT_OTEL_EXPORTER_H_

#include "export/exporter_interface.h"

#include <memory>

std::unique_ptr<StatsExporter> MakeOpenTelemetryExporter();

#endif  // PG_STAT_CH_SRC_EXPORT_OTEL_EXPORTER_H_
