// pg_stat_ch shared query-text interner.
//
// See query_intern.h for the architectural overview.  This file implements
// the acquire/release lifecycle around a shared HTAB and the existing DSA
// area.  The pattern mirrors pg_stat_statements (shared HTAB sized via
// hash_estimate_size and ShmemInitHash) and pgstat_shmem (HTAB entries point
// at variable-size DSA bodies, refcounts protect lifetime, the DSA body is
// freed only after the HTAB entry is removed).

#include "postgres.h"

#include <string.h>

#include "common/hashfn.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"

#include "config/guc.h"
#include "queue/psch_dsa.h"
#include "queue/query_intern.h"

// Magic number stamped into every DSA-allocated query object.  Used purely
// as a sanity check when resolving a dsa_pointer that came from a ring slot.
#define PSCH_QUERY_INTERN_MAGIC 0x51544348u  // "QTCH"

// Hash-table key.  Combining (dbid, queryid, query_hash, query_len) makes
// accidental collisions exceedingly rare without requiring full text in the
// key.  PostgreSQL usually assigns the same query_id to same-shaped queries
// so most lookups hit by query_id alone; the extra fields harden the key.
typedef struct PschQueryInternKey {
  Oid dbid;
  uint64 queryid;
  uint64 query_hash;
  uint16 query_len;
} PschQueryInternKey;

// Hash-table entry stored in the shared HTAB.  `key` must be first so dynahash
// can locate it from the entry pointer.  The variable-size query bytes live
// in the DSA area; `object` points at a PschQueryInternObject there.
typedef struct PschQueryInternEntry {
  PschQueryInternKey key;
  dsa_pointer object;
  uint32 refcount;
} PschQueryInternEntry;

// DSA-resident body.  Stamped with the magic + a copy of the key so the
// bgworker can recover the lock partition from a bare dsa_pointer.
typedef struct PschQueryInternObject {
  PschQueryInternKey key;
  uint32 magic;
  char query[FLEXIBLE_ARRAY_MEMBER];
} PschQueryInternObject;

// Shared HTAB handle (process-local pointer to a struct that lives in shmem).
static HTAB* psch_query_intern_htab = NULL;

// Base of the partition LWLock array (index 0 .. PSCH_QUERY_INTERN_PARTITIONS-1).
// Lives in MainLWLockArray inside the pg_stat_ch named tranche.  Entries are
// cache-line padded (LWLockPadded), so iteration must go through the padded
// type rather than `LWLock *` arithmetic.
static LWLockPadded* psch_query_intern_locks = NULL;

static long PschQueryInternMaxEntries(void) {
  // There can never be more distinct live interned objects than there are
  // queue slots: every interned entry must be referenced by at least one
  // queued event (refcount >= 1).  Sizing the HTAB at the queue capacity
  // gives a tight, predictable upper bound.
  return (long)psch_queue_capacity;
}

Size PschQueryInternShmemSize(void) {
  return hash_estimate_size(PschQueryInternMaxEntries(), sizeof(PschQueryInternEntry));
}

int PschQueryInternLockCount(void) {
  return PSCH_QUERY_INTERN_PARTITIONS;
}

void PschQueryInternShmemInit(void* lwlock_base) {
  HASHCTL info;

  psch_query_intern_locks = (LWLockPadded*)lwlock_base;

  MemSet(&info, 0, sizeof(info));
  info.keysize = sizeof(PschQueryInternKey);
  info.entrysize = sizeof(PschQueryInternEntry);

  psch_query_intern_htab =
      ShmemInitHash("pg_stat_ch query intern", PschQueryInternMaxEntries(),
                    PschQueryInternMaxEntries(), &info, HASH_ELEM | HASH_BLOBS);
}

static void MakeKey(PschQueryInternKey* key, Oid dbid, uint64 queryid, const char* query,
                    uint16 query_len) {
  // Zero pad bytes so HASH_BLOBS hashing is deterministic.
  MemSet(key, 0, sizeof(*key));
  key->dbid = dbid;
  key->queryid = queryid;
  key->query_hash = hash_any_extended((const unsigned char*)query, query_len, 0);
  key->query_len = query_len;
}

static LWLock* PartitionLockFor(uint64 query_hash) {
  uint32 idx = (uint32)(query_hash & (PSCH_QUERY_INTERN_PARTITIONS - 1));
  return &psch_query_intern_locks[idx].lock;
}

// Allocate a fresh DSA object holding the query text and key.
// Returns InvalidDsaPointer on DSA OOM (and bumps the OOM counter, mirroring
// PschDsaAllocString).
static dsa_pointer AllocInternObject(dsa_area* dsa, const PschQueryInternKey* key,
                                     const char* query, uint16 query_len) {
  Size alloc_size;
  dsa_pointer dp;
  PschQueryInternObject* obj;

  alloc_size = offsetof(PschQueryInternObject, query) + query_len + 1;
  dp = dsa_allocate_extended(dsa, alloc_size, DSA_ALLOC_NO_OOM);
  if (!DsaPointerIsValid(dp)) {
    pg_atomic_fetch_add_u64(&psch_shared_state->dsa_oom_count, 1);
    return InvalidDsaPointer;
  }

  obj = (PschQueryInternObject*)dsa_get_address(dsa, dp);
  obj->key = *key;
  obj->magic = PSCH_QUERY_INTERN_MAGIC;
  memcpy(obj->query, query, query_len);
  obj->query[query_len] = '\0';
  return dp;
}

// Compare a query text against the bytes already stored in an interned object.
// Returns true if the existing object holds exactly the same text.  A false
// here on a hash hit is a (dbid, queryid, query_hash, query_len) collision —
// extremely unlikely but the caller must handle it.
static bool ObjectMatches(dsa_area* dsa, dsa_pointer dp, const char* query, uint16 query_len) {
  PschQueryInternObject* obj = (PschQueryInternObject*)dsa_get_address(dsa, dp);

  if (obj->magic != PSCH_QUERY_INTERN_MAGIC) {
    return false;
  }
  if (obj->key.query_len != query_len) {
    return false;
  }
  return memcmp(obj->query, query, query_len) == 0;
}

dsa_pointer PschQueryInternAcquire(Oid dbid, uint64 queryid, const char* query, uint16 query_len) {
  PschQueryInternKey key;
  LWLock* partition;
  PschQueryInternEntry* entry;
  dsa_area* dsa;
  dsa_pointer new_dp;
  bool found;

  if (query_len == 0 || psch_query_intern_htab == NULL) {
    return InvalidDsaPointer;
  }

  dsa = PschDsaGetArea();
  if (dsa == NULL) {
    return InvalidDsaPointer;
  }

  MakeKey(&key, dbid, queryid, query, query_len);
  partition = PartitionLockFor(key.query_hash);

  // First lookup: fast path for a hit.
  LWLockAcquire(partition, LW_EXCLUSIVE);
  entry = (PschQueryInternEntry*)hash_search(psch_query_intern_htab, &key, HASH_FIND, NULL);
  if (entry != NULL && ObjectMatches(dsa, entry->object, query, query_len)) {
    entry->refcount++;
    LWLockRelease(partition);
    return entry->object;
  }
  if (entry != NULL) {
    // Hash hit but bytes differ — collision.  Treat as a miss; the safe
    // fallback below will attempt to install our own entry, which can't
    // happen because the slot is taken.  Return InvalidDsaPointer so the
    // caller exports empty query text rather than wrong SQL.
    LWLockRelease(partition);
    return InvalidDsaPointer;
  }
  LWLockRelease(partition);

  // Miss: allocate the new object outside the partition lock so we don't
  // hold it across DSA work.
  new_dp = AllocInternObject(dsa, &key, query, query_len);
  if (!DsaPointerIsValid(new_dp)) {
    return InvalidDsaPointer;
  }

  // Re-lock partition and re-check.  Another backend may have inserted the
  // same key while we were allocating.
  LWLockAcquire(partition, LW_EXCLUSIVE);
  entry = (PschQueryInternEntry*)hash_search(psch_query_intern_htab, &key, HASH_ENTER_NULL, &found);
  if (entry == NULL) {
    // Hash table is full — back out the loser allocation and report miss.
    LWLockRelease(partition);
    dsa_free(dsa, new_dp);
    return InvalidDsaPointer;
  }

  if (found) {
    dsa_pointer existing = entry->object;

    if (ObjectMatches(dsa, existing, query, query_len)) {
      entry->refcount++;
      LWLockRelease(partition);
      dsa_free(dsa, new_dp);
      return existing;
    }

    // Lost the race AND the winner stored different bytes (collision).
    // Don't disturb the winner; back out and report miss.
    LWLockRelease(partition);
    dsa_free(dsa, new_dp);
    return InvalidDsaPointer;
  }

  entry->object = new_dp;
  entry->refcount = 1;
  LWLockRelease(partition);
  return new_dp;
}

// Drop one reference to `ref`.  Frees the DSA object when refcount hits zero.
// Caller has already copied any data it needs out of the object.
static void ReleaseRef(dsa_pointer ref) {
  dsa_area* dsa;
  PschQueryInternObject* obj;
  PschQueryInternKey key;
  LWLock* partition;
  PschQueryInternEntry* entry;
  dsa_pointer freed_dp = InvalidDsaPointer;

  if (!DsaPointerIsValid(ref) || psch_query_intern_htab == NULL) {
    return;
  }

  dsa = PschDsaGetArea();
  if (dsa == NULL) {
    return;
  }

  obj = (PschQueryInternObject*)dsa_get_address(dsa, ref);
  if (obj->magic != PSCH_QUERY_INTERN_MAGIC) {
    // Object was never an interned body (or was already freed).  Don't
    // touch the HTAB; just leak the pointer — better than corrupting it.
    return;
  }

  key = obj->key;
  partition = PartitionLockFor(key.query_hash);

  LWLockAcquire(partition, LW_EXCLUSIVE);
  entry = (PschQueryInternEntry*)hash_search(psch_query_intern_htab, &key, HASH_FIND, NULL);
  if (entry != NULL && entry->object == ref) {
    Assert(entry->refcount > 0);
    entry->refcount--;
    if (entry->refcount == 0) {
      freed_dp = entry->object;
      hash_search(psch_query_intern_htab, &key, HASH_REMOVE, NULL);
    }
  }
  LWLockRelease(partition);

  // Free outside the partition lock so we don't hold it across DSA work.
  if (DsaPointerIsValid(freed_dp)) {
    dsa_free(dsa, freed_dp);
  }
}

void PschQueryInternResolveAndRelease(dsa_pointer ref, char* dst, uint16 dst_size,
                                      uint16* out_len) {
  dsa_area* dsa;
  PschQueryInternObject* obj;
  uint16 copy_len;

  if (!DsaPointerIsValid(ref) || dst == NULL || dst_size == 0 || out_len == NULL) {
    if (dst != NULL && dst_size > 0) {
      dst[0] = '\0';
    }
    if (out_len != NULL) {
      *out_len = 0;
    }
    return;
  }

  dsa = PschDsaGetArea();
  if (dsa == NULL) {
    dst[0] = '\0';
    *out_len = 0;
    return;
  }

  obj = (PschQueryInternObject*)dsa_get_address(dsa, ref);
  if (obj->magic != PSCH_QUERY_INTERN_MAGIC) {
    dst[0] = '\0';
    *out_len = 0;
    return;
  }

  // We hold a reference (the caller's slot), so the object cannot be freed
  // underneath us during the copy.  No partition lock is needed for the
  // read itself.
  copy_len = obj->key.query_len;
  if (copy_len >= dst_size) {
    copy_len = dst_size - 1;
  }
  memcpy(dst, obj->query, copy_len);
  dst[copy_len] = '\0';
  *out_len = copy_len;

  ReleaseRef(ref);
}

void PschQueryInternRelease(dsa_pointer ref) {
  ReleaseRef(ref);
}
