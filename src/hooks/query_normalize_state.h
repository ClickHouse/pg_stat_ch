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
#ifndef PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_
#define PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_

#include <cstddef>

extern "C" {
#include "postgres.h"

#include "utils/hsearch.h"
}

// Exact statement identity for the normalization registry.
//
// statement_hash is a 64-bit hash of (source_text content, stmt_location,
// stmt_len) computed via absl::HashOf.  Together with stmt_location and
// stmt_len, the triple forms the HTAB key — a collision requires matching
// all three fields, making false positives negligible.
struct PschStatementKey {
  const char* source_text;
  size_t source_text_len;
  size_t statement_hash;
  int stmt_location;
  int stmt_len;
};

// Build a statement key from PostgreSQL's source string plus statement slice.
PschStatementKey PschMakeStatementKey(const char* source_text, int stmt_location, int stmt_len);

struct PschNormalizedQueryState {
  HTAB* htab;
};

// Stash normalized SQL text for one parsed statement.
//
// Called from post_parse_analyze_hook immediately after PschNormalizeQuery()
// succeeds. We have to save the normalized text here because JumbleState only
// exists at parse time, but the actual event is built later in ExecutorEnd or
// ProcessUtility.
//
// The key is "statement identity", not just query text:
// - source_text identifies which SQL string we parsed
// - stmt_location / stmt_len identify which statement inside that string
//
// Example:
//   source_text = "SELECT 1; SELECT 2"
//   stmt_location/stm_len distinguish the first SELECT from the second.
void PschRememberNormalizedQuery(PschNormalizedQueryState* state, const PschStatementKey& key,
                                 char* normalized_query, int normalized_len);

// Copy normalized text for one exact statement match and remove the entry.
//
// Called from ExecutorEnd and ProcessUtility when building the final event.
// The entry is always consumed: for simple-query mode the source text is
// unique per execution, and for cached plans post_parse_analyze_hook
// re-creates the entry on the next parse cycle.
bool PschCopyNormalizedQueryForStatement(PschNormalizedQueryState* state, char* dst,
                                         size_t dst_size, uint16* out_len,
                                         const PschStatementKey& key);

// Forget one pending normalized entry by exact statement identity.
//
// Called on executor / utility early-return paths that skipped normal event
// construction. This is the "we are done with this statement, do not reuse its
// normalized text by accident" cleanup path.
void PschForgetNormalizedQueryForStatement(PschNormalizedQueryState* state,
                                           const PschStatementKey& key);

#endif  // PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_
