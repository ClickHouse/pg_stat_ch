// Statement-scoped normalized query registry
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

// Stash normalized SQL text for a parsed statement until executor, utility, or
// error logging can attach it to an event.
void PschRememberNormalizedQuery(PschNormalizedQueryState* state, const char* source_text,
                                 int64 query_id, int stmt_location, int stmt_len,
                                 char* normalized_query, int normalized_len);

// Copy the normalized text for one exact statement match. When consume is true,
// remove the entry after copying so it cannot be reused by a later statement.
bool PschCopyNormalizedQueryForStatement(PschNormalizedQueryState* state, char* dst,
                                         size_t dst_size, uint16* out_len, const char* source_text,
                                         int stmt_location, int stmt_len, bool consume);

// Best-effort lookup for emit_log_hook, which only has the full source string,
// queryId, and cursor position rather than stmt_location / stmt_len.
bool PschCopyNormalizedQueryForLog(PschNormalizedQueryState* state, char* dst, size_t dst_size,
                                   uint16* out_len, const char* source_text, int64 query_id,
                                   int cursorpos);

// Forget one pending normalized statement entry by exact source/location match.
void PschForgetNormalizedQueryForStatement(PschNormalizedQueryState* state, const char* source_text,
                                           int stmt_location, int stmt_len);

// Forget every pending normalized entry for one source string. Used after an
// aborting error so leftover normalized text cannot leak into a later query.
void PschForgetNormalizedQueriesForSource(PschNormalizedQueryState* state, const char* source_text);

#endif  // PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_
