// Statement-scoped normalized query registry.
//
// Flow:
// 1. post_parse_analyze_hook has access to JumbleState and can normalize
//    literals, so it calls PschRememberNormalizedQuery().
// 2. ExecutorEnd / ProcessUtility later need the same statement text for the
//    exported event, but they no longer have JumbleState, so they call
//    PschCopyNormalizedQueryForStatement().
// 3. Cleanup paths call PschForgetNormalizedQueryForStatement() when execution
//    exits without producing a normal event.
//
// We intentionally keep this registry scoped to executor/utility capture only.
// emit_log_hook does not get stmt_location/stmt_len, so reconstructing a
// normalized statement there required fuzzy matching and more backend-local
// state. Error events now omit query text instead of trying to recover it from
// this registry.
//
// Example:
//   SELECT 1; SELECT 2;
// post_parse_analyze_hook runs once per statement and stores two entries that
// share one source string but have different stmt_location/stm_len values.
//
// Example:
//   A plpgsql function executes the same SPI statement multiple times.
// We keep one reusable normalized entry for that exact statement identity so
// each execution can export "SELECT ... WHERE id = $1" instead of falling back
// to the raw SQL text on later executions.
#ifndef PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_
#define PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_

#include "postgres.h"

// Exact statement identity for the normalization registry.
//
// The hash is a fast prefilter for executor/utility lookups. We still fall
// back to bytewise source-text comparison after the hash/offset checks, so
// collisions cannot attach the wrong normalized query text.
typedef struct PschStatementKey {
  const char* source_text;
  size_t source_text_len;
  size_t statement_hash;
  int stmt_location;
  int stmt_len;
} PschStatementKey;

// Build a statement key from PostgreSQL's source string plus statement slice.
PschStatementKey PschMakeStatementKey(const char* source_text, int stmt_location, int stmt_len);

typedef struct PschNormalizedQueryEntry PschNormalizedQueryEntry;

typedef struct PschNormalizedQueryState {
  PschNormalizedQueryEntry* head;
} PschNormalizedQueryState;

// Stash normalized SQL text for one parsed statement.
void PschRememberNormalizedQuery(PschNormalizedQueryState* state, PschStatementKey key,
                                 char* normalized_query, int normalized_len);

// Copy normalized text for one exact statement match.
bool PschCopyNormalizedQueryForStatement(PschNormalizedQueryState* state, char* dst,
                                         size_t dst_size, uint16* out_len,
                                         PschStatementKey key, bool consume);

// Forget one pending normalized entry by exact statement identity.
void PschForgetNormalizedQueryForStatement(PschNormalizedQueryState* state, PschStatementKey key);

#endif  // PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_
