#ifndef PG_STAT_CH_INCLUDE_HOOKS_RELATIONS_BUDGET_H_
#define PG_STAT_CH_INCLUDE_HOOKS_RELATIONS_BUDGET_H_

// [truncated:u8][count:u8] -- the GUC's minimum must never go below this.
#define PSCH_RELATIONS_HEADER_LEN 2

// Sentinel relations_len for "blob lost to DSA OOM" -- distinct from 0 (no
// relations) and real blob lengths (>= PSCH_RELATIONS_HEADER_LEN).
#define PSCH_RELATIONS_OOM_LEN 1

#endif  // PG_STAT_CH_INCLUDE_HOOKS_RELATIONS_BUDGET_H_
