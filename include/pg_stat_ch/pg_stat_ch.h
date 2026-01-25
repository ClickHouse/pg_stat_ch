#ifndef PG_STAT_CH_H
#define PG_STAT_CH_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "fmgr.h"

// Extension version (set at compile time)
#ifndef PG_STAT_CH_VERSION
#define PG_STAT_CH_VERSION "unknown"
#endif

// SQL-callable functions
PGDLLEXPORT Datum pg_stat_ch_version(PG_FUNCTION_ARGS);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_H
