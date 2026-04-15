// Backend-local LRU cache of normalized query text, keyed by queryId.
//
// Uses PostgreSQL's dynahash (HTAB) for O(1) lookup/insert and dclist for
// O(1) LRU promotion and eviction.

#include "postgres.h"

#include <string.h>

#include "lib/ilist.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "config/guc.h"
#include "hooks/query_normalize_state.h"

// Maximum query length exported in events (must match PSCH_MAX_QUERY_LEN in
// queue/event.h).  Duplicated here to avoid pulling event.h into pure-C code.
#define PSCH_MAX_CACHED_QUERY_LEN 2048

// HTAB key — just the queryId.
typedef struct PschNormalizeCacheKey
{
  uint64 query_id;
} PschNormalizeCacheKey;

// Entry stored in HTAB.  key must be the first member so hash_search can
// cast between key pointer and entry pointer.
typedef struct PschNormalizeCacheEntry
{
  PschNormalizeCacheKey key;
  dlist_node lru_node;
  char *normalized_query;  // palloc'd in cache->mcxt
  int normalized_len;
} PschNormalizeCacheEntry;

static PschNormalizeCacheKey
MakeCacheKey(uint64 query_id)
{
  PschNormalizeCacheKey key;

  key.query_id = query_id;
  return key;
}

static void
EnsureCacheInitialized(PschNormalizedQueryCache *cache)
{
  int max_entries;

  if (cache->htab != NULL)
    return;

  max_entries = cache->max_entries > 0 ? cache->max_entries
                                       : psch_normalize_cache_max;
  PschInitNormalizedQueryCache(cache, max_entries);
}

static int
ClampCachedQueryLen(int normalized_len)
{
  if (normalized_len >= PSCH_MAX_CACHED_QUERY_LEN)
    return PSCH_MAX_CACHED_QUERY_LEN - 1;

  return normalized_len;
}

static void
PushEntryToMru(PschNormalizedQueryCache *cache, PschNormalizeCacheEntry *entry)
{
  dclist_push_head(&cache->lru, &entry->lru_node);
}

static void
PromoteEntryToMru(PschNormalizedQueryCache *cache, PschNormalizeCacheEntry *entry)
{
  dclist_delete_from(&cache->lru, &entry->lru_node);
  PushEntryToMru(cache, entry);
}

static char *
CopyQueryText(PschNormalizedQueryCache *cache, const char *normalized_query,
              int normalized_len)
{
  char *copy;

  copy = (char *) MemoryContextAlloc(cache->mcxt, normalized_len + 1);
  memcpy(copy, normalized_query, normalized_len);
  copy[normalized_len] = '\0';
  return copy;
}

static uint16
CopyEntryText(const PschNormalizeCacheEntry *entry, char *dst, Size dst_size)
{
  Size len;

  len = Min((Size) entry->normalized_len, dst_size - 1);
  memcpy(dst, entry->normalized_query, len);
  dst[len] = '\0';
  return (uint16) len;
}

void
PschInitNormalizedQueryCache(PschNormalizedQueryCache *cache, int max_entries)
{
  HASHCTL info;

  Assert(cache != NULL);
  Assert(max_entries >= PSCH_NORMALIZE_CACHE_MIN_MAX);

  cache->mcxt = AllocSetContextCreate(TopMemoryContext,
                                      "pg_stat_ch normalize cache",
                                      ALLOCSET_DEFAULT_SIZES);

  MemSet(&info, 0, sizeof(info));
  info.keysize = sizeof(PschNormalizeCacheKey);
  info.entrysize = sizeof(PschNormalizeCacheEntry);
  info.hcxt = cache->mcxt;
  cache->htab = hash_create("pg_stat_ch normalize cache",
                            Min(max_entries, 256), &info,
                            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
  dclist_init(&cache->lru);
  cache->max_entries = max_entries;
}

// Evict the least-recently-used entry (tail of LRU list).
static void
EvictLru(PschNormalizedQueryCache *cache)
{
  dlist_node *tail;
  PschNormalizeCacheEntry *victim;
  PschNormalizeCacheKey victim_key;

  Assert(!dclist_is_empty(&cache->lru));

  tail = dclist_tail_node(&cache->lru);
  victim = dlist_container(PschNormalizeCacheEntry, lru_node, tail);

  dclist_delete_from(&cache->lru, &victim->lru_node);
  pfree(victim->normalized_query);

  victim_key = victim->key;
  hash_search(cache->htab, &victim_key, HASH_REMOVE, NULL);
}

void
PschRememberNormalizedQuery(PschNormalizedQueryCache *cache, uint64 query_id,
                            const char *normalized_query, int normalized_len)
{
  PschNormalizeCacheKey hkey;
  PschNormalizeCacheEntry *entry;
  bool found;

  if (cache == NULL || normalized_query == NULL || query_id == UINT64CONST(0))
    return;

  EnsureCacheInitialized(cache);

  hkey = MakeCacheKey(query_id);
  entry = (PschNormalizeCacheEntry *) hash_search(cache->htab, &hkey,
                                                  HASH_ENTER, &found);

  if (found)
  {
    // Update existing entry — free old text and requeue it as MRU below.
    pfree(entry->normalized_query);
    dclist_delete_from(&cache->lru, &entry->lru_node);
  }
  else
  {
    // New entry — evict LRU if at capacity.
    if ((int) dclist_count(&cache->lru) >= cache->max_entries)
      EvictLru(cache);
  }

  // Clamp to the maximum length that events ever export — caching more than
  // that wastes memory in the long-lived cache context.
  normalized_len = ClampCachedQueryLen(normalized_len);

  // Copy text into the cache's own memory context.
  entry->normalized_query = CopyQueryText(cache, normalized_query,
                                          normalized_len);
  entry->normalized_len = normalized_len;
  PushEntryToMru(cache, entry);
}

bool
PschLookupNormalizedQuery(PschNormalizedQueryCache *cache, uint64 query_id,
                          char *dst, Size dst_size, uint16 *out_len)
{
  PschNormalizeCacheKey hkey;
  PschNormalizeCacheEntry *entry;

  if (cache == NULL || cache->htab == NULL || dst == NULL || dst_size == 0 ||
      out_len == NULL || query_id == UINT64CONST(0))
    return false;

  hkey = MakeCacheKey(query_id);
  entry = (PschNormalizeCacheEntry *) hash_search(cache->htab, &hkey,
                                                  HASH_FIND, NULL);
  if (entry == NULL)
    return false;

  // Promote to MRU.
  PromoteEntryToMru(cache, entry);

  // Copy text to destination.
  *out_len = CopyEntryText(entry, dst, dst_size);
  return true;
}
