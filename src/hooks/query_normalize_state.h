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

void PschRememberNormalizedQuery(PschNormalizedQueryState* state, const char* source_text,
                                 int64 query_id, int stmt_location, int stmt_len,
                                 char* normalized_query, int normalized_len);

bool PschCopyNormalizedQueryForStatement(PschNormalizedQueryState* state, char* dst,
                                         size_t dst_size, uint16* out_len, const char* source_text,
                                         int stmt_location, int stmt_len, bool consume);

bool PschCopyNormalizedQueryForLog(PschNormalizedQueryState* state, char* dst, size_t dst_size,
                                   uint16* out_len, const char* source_text, int64 query_id,
                                   int cursorpos);

void PschForgetNormalizedQueryForStatement(PschNormalizedQueryState* state, const char* source_text,
                                           int stmt_location, int stmt_len);

void PschForgetNormalizedQueriesForSource(PschNormalizedQueryState* state, const char* source_text);

#endif  // PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_
