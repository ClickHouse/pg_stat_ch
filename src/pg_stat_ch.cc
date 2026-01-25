// pg_stat_ch - Query telemetry exporter to ClickHouse
//
// This is the main entry point for the pg_stat_ch extension.

extern "C" {
#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
}

#include "pg_stat_ch/pg_stat_ch.h"

extern "C" {

PG_MODULE_MAGIC;

// Extension initialization - called when shared library is loaded
void _PG_init(void) {
  elog(LOG, "pg_stat_ch %s: initializing", PG_STAT_CH_VERSION);

  // TODO: Register hooks
  // TODO: Request shared memory
  // TODO: Register background worker
}

// SQL function: pg_stat_ch_version()
// Returns the extension version string
PG_FUNCTION_INFO_V1(pg_stat_ch_version);
Datum pg_stat_ch_version(PG_FUNCTION_ARGS) {
  PG_RETURN_TEXT_P(cstring_to_text(PG_STAT_CH_VERSION));
}

}  // extern "C"
