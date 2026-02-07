// pg_stat_ch executor hooks
#ifndef PG_STAT_CH_SRC_HOOKS_HOOKS_H_
#define PG_STAT_CH_SRC_HOOKS_HOOKS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

// Install executor hooks (called from _PG_init)
void PschInstallHooks(void);

// Suppress error capture to prevent deadlock during enqueue.
// When true, emit_log_hook will not re-enter PschEnqueueEvent.
void PschSuppressErrorCapture(bool suppress);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_HOOKS_HOOKS_H_
