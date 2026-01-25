// pg_stat_ch executor hooks
#ifndef PG_STAT_CH_SRC_HOOKS_HOOKS_H_
#define PG_STAT_CH_SRC_HOOKS_HOOKS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

// Install executor hooks (called from _PG_init)
void PschInstallHooks(void);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_HOOKS_HOOKS_H_
