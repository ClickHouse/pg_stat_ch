// Statement-scoped normalized query registry.
//
// Flow:
// 1. post_parse_analyze_hook has access to JumbleState and can normalize
//    literals, so it calls PschRememberNormalizedQuery().
// 2. ExecutorEnd / ProcessUtility later need the same statement text for the
//    exported event, but they no longer have JumbleState, so they call
//    PschCopyNormalizedQueryForStatement().
// 3. emit_log_hook only has debug_query_string plus cursor position, so error
//    capture falls back to PschCopyNormalizedQueryForLog().
// 4. Cleanup paths call the Forget helpers so normalized text cannot leak into
//    an unrelated later statement on the same backend.
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

#include <cstddef>

extern "C" {
#include "postgres.h"
}

struct PschNormalizedQueryEntry;

struct PschNormalizedQueryState {
  PschNormalizedQueryEntry* head;
};

// Stash normalized SQL text for one parsed statement.
//
// Called from post_parse_analyze_hook immediately after PschNormalizeQuery()
// succeeds. We have to save the normalized text here because JumbleState only
// exists at parse time.
//
// The key is "statement identity", not just query text:
// - source_text identifies which SQL string we parsed
// - stmt_location / stmt_len identify which statement inside that string
// - query_id helps error-log matching when we only have debug_query_string
//
// Example:
//   source_text = "SELECT 1; SELECT 2"
//   stmt_location/stm_len distinguish the first SELECT from the second.
void PschRememberNormalizedQuery(PschNormalizedQueryState* state, const char* source_text,
                                 int64 query_id, int stmt_location, int stmt_len,
                                 char* normalized_query, int normalized_len);

// Copy normalized text for one exact statement match.
//
// Called from ExecutorEnd and ProcessUtility when building the final event.
// These paths know the original source_text plus stmt_location/stm_len, so they
// can do an exact lookup.
//
// When consume is true, remove the matched entry after copying it. That mode is
// useful for one-shot statements where a later reuse would indicate stale state.
// When consume is false, keep the entry for cached-plan or SPI cases where the
// same statement identity may execute again.
//
// Example:
//   A plpgsql function caches "SELECT child(depth - 1) + 42 WHERE 7 = 7".
//   The first and third executions should both see the same normalized form.
bool PschCopyNormalizedQueryForStatement(PschNormalizedQueryState* state, char* dst,
                                         size_t dst_size, uint16* out_len, const char* source_text,
                                         int stmt_location, int stmt_len, bool consume);

// Best-effort lookup for emit_log_hook.
//
// Error capture does not get stmt_location/stm_len directly. Instead it has the
// full source string, current queryId, and sometimes a cursor position inside
// the failing statement. This helper first tries to match by cursor position
// within the source text, then falls back to a unique query_id/source_text
// match.
//
// Example:
//   debug_query_string = "SELECT 1/0"
//   cursorpos points at the "/" token, so we can still recover the normalized
//   "SELECT $1/$2" text for the error event.
bool PschCopyNormalizedQueryForLog(PschNormalizedQueryState* state, char* dst, size_t dst_size,
                                   uint16* out_len, const char* source_text, int64 query_id,
                                   int cursorpos);

// Forget one pending normalized entry by exact statement identity.
//
// Called on executor / utility early-return paths that skipped normal event
// construction. This is the "we are done with this statement, do not reuse its
// normalized text by accident" cleanup path.
void PschForgetNormalizedQueryForStatement(PschNormalizedQueryState* state, const char* source_text,
                                           int stmt_location, int stmt_len);

// Forget every pending normalized entry for one source string.
//
// Called after an aborting error. If a statement errors out before ExecutorEnd
// or ProcessUtility consumes the entry, the backend may go on to execute a new
// query in the same session. Clearing everything for that source string prevents
// stale placeholders from being attached to the next event.
//
// Example:
//   "SELECT 1/0" errors, then the client sends "SELECT current_database()".
//   The old normalized entry must be gone before the follow-up statement runs.
void PschForgetNormalizedQueriesForSource(PschNormalizedQueryState* state, const char* source_text);

#endif  // PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_
