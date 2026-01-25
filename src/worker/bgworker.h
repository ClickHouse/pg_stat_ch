// pg_stat_ch background worker
#ifndef PG_STAT_CH_SRC_WORKER_BGWORKER_H_
#define PG_STAT_CH_SRC_WORKER_BGWORKER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

// Register background worker (called from _PG_init)
void PschRegisterBgworker(void);

// Background worker entry point (called by PostgreSQL)
PGDLLEXPORT void PschBgworkerMain(Datum main_arg);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_WORKER_BGWORKER_H_
