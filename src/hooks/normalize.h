// Query normalization: replace constants with $1, $2, ... placeholders.
#ifndef PG_STAT_CH_SRC_HOOKS_NORMALIZE_H_
#define PG_STAT_CH_SRC_HOOKS_NORMALIZE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "nodes/queryjumble.h"

// Normalize query by replacing constants with $N parameters.
// Returns palloc'd string (freed when the memory context resets).
// Returns NULL if jstate is NULL or has no constants to replace.
char* PschNormalizeQuery(JumbleState* jstate, const char* query, int query_loc, int* query_len_p);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_HOOKS_NORMALIZE_H_
