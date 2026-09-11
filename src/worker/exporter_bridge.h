// Translation between PostgreSQL types and exporter library interface
#ifndef PG_STAT_CH_SRC_WORKER_EXPORTER_BRIDGE_H_
#define PG_STAT_CH_SRC_WORKER_EXPORTER_BRIDGE_H_

#include "postgres.h"

#include "export/psch_exporter.h"
#include "queue/event.h"

// Snapshot exporter settings from GUCs, strings borrowed from GUC storage
void PschBuildExporterConfig(PschExporterConfig* config);

// View one dequeued event as exporter input, borrowing its inline strings
void PschToExportEvent(const PschEvent* event, PschExportEvent* out);

// Clip diagnostic text to encoding boundaries in place, then emit retained
// warnings and error text as WARNING
void PschLogExporterResult(const char* operation, PschExporterResult* result);

#endif  // PG_STAT_CH_SRC_WORKER_EXPORTER_BRIDGE_H_
