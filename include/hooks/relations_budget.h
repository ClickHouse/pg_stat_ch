// Pure C, no PostgreSQL headers -- kept separate from hooks.c so it's
// unit-testable in isolation (see test/unit/relations_budget_test.cc).
#ifndef PG_STAT_CH_INCLUDE_HOOKS_RELATIONS_BUDGET_H_
#define PG_STAT_CH_INCLUDE_HOOKS_RELATIONS_BUDGET_H_

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// [truncated:u8][count:u8] -- the GUC's minimum must never go below this.
#define PSCH_RELATIONS_HEADER_LEN 2

// True if appending an entry of entry_len bytes to a buffer that already
// holds current_len bytes would exceed budget. No separator byte to
// account for -- every entry is self-delimiting (length-prefixed).
static inline bool PschRelationsWouldExceedBudget(int current_len, int entry_len, int budget) {
  return (current_len + entry_len) > budget;
}

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_INCLUDE_HOOKS_RELATIONS_BUDGET_H_
