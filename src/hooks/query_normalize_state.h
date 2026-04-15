// Backend-local LRU cache of normalized query text, keyed by queryId.
//
// Flow:
// 1. post_parse_analyze_hook calls PschRememberNormalizedQuery() to stash the
//    normalized (or trimmed) query text, keyed by the queryId that the jumbler
//    assigned.
// 2. ExecutorEnd / ProcessUtility call PschLookupNormalizedQuery() to retrieve
//    the text when building the exported event.
//
// Keying by queryId (instead of source-text hash) solves two problems at once:
// - Cached/SPI plans that execute many times without re-parsing always find
//   the entry (same queryId across executions).
// - Distinct constant-bearing queries that share a query shape (e.g. SELECT 1,
//   SELECT 2) map to the same queryId, so only one entry is stored instead of
//   one per unique SQL string.
//
// This intentionally canonicalizes query text by query shape, matching the
// tradeoff PostgreSQL itself makes for queryId-based aggregation.
//
// The cache is bounded by a configurable max entry count. When full, the
// least-recently-used entry is evicted via a dclist threaded through the HTAB
// entries.
//
#ifndef PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_
#define PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_

#include "postgres.h"

#include "lib/ilist.h"
#include "utils/hsearch.h"

// Default and limits for the LRU cache size (number of entries).
#define PSCH_NORMALIZE_CACHE_DEFAULT_MAX 32768
#define PSCH_NORMALIZE_CACHE_MIN_MAX     64
#define PSCH_NORMALIZE_CACHE_MAX_MAX     65536

typedef struct PschNormalizedQueryCache {
  HTAB* htab;
  dclist_head lru;     // HEAD = most recently used, TAIL = eviction candidate
  MemoryContext mcxt;  // dedicated context for HTAB + query text allocations
  int max_entries;
} PschNormalizedQueryCache;

// Initialize the cache. Call once per backend (e.g. on first use).
void PschInitNormalizedQueryCache(PschNormalizedQueryCache* cache, int max_entries);

// Stash normalized query text for a queryId.
//
// If the queryId already exists, updates the text and promotes the entry to
// MRU. If the cache is full, evicts the LRU entry first.
//
// Copies normalized_query into the cache's own memory context. The caller's
// allocation is not consumed — the caller may pfree it or let it be cleaned
// up by the current memory context reset.
void PschRememberNormalizedQuery(PschNormalizedQueryCache* cache, uint64 query_id,
                                 const char* normalized_query, int normalized_len);

// Copy cached normalized text for a queryId into dst.
//
// On hit, promotes the entry to MRU and returns true.
// On miss, returns false and leaves dst unchanged.
bool PschLookupNormalizedQuery(PschNormalizedQueryCache* cache, uint64 query_id, char* dst,
                               Size dst_size, uint16* out_len);

#endif  // PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_STATE_H_
