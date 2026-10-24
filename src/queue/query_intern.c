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

#if PG_VERSION_NUM >= 170000
#include "common/hashfn_unstable.h"
#else
#include "common/hashfn.h"
#endif
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"

#include "compat.h"
#include "config/guc.h"
#include "queue/psch_dsa.h"
#include "queue/query_intern.h"

// Hash-table key, derived from text alone so identical text dedupes across
// databases and queryids.  Hits are verified with memcmp, so a 64-bit hash
// collision drops the text rather than exporting wrong SQL.
typedef struct PschQueryInternKey {
  uint64 query_hash;
  uint16 query_len;
} PschQueryInternKey;

// Hash-table entry stored in the shared HTAB.  `key` must be first so dynahash
// can locate it from the entry pointer.  `object` points at the raw query
// bytes in the DSA area, without header or terminator, so a 2047-byte text
// fits DSA's 2048-byte size class.
typedef struct PschQueryInternEntry {
  PschQueryInternKey key;
  dsa_pointer object;
  uint32 refcount;
} PschQueryInternEntry;

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

void PschQueryInternShmemInit(LWLockPadded* lwlock_base) {
  HASHCTL info;

  psch_query_intern_locks = lwlock_base;

  MemSet(&info, 0, sizeof(info));
  info.keysize = sizeof(PschQueryInternKey);
  info.entrysize = sizeof(PschQueryInternEntry);
  info.num_partitions = PSCH_QUERY_INTERN_PARTITIONS;

  // HASH_PARTITION is required when external partition locks guard concurrent
  // mutation: it disables on-the-fly bucket splits and gives each partition
  // its own spinlock-protected freelist.  Our external partition lock for a
  // given key is derived from dynahash's own hashcode (get_hash_value) so the
  // external and internal partition agree, matching the LockTagHashCode /
  // LockHashPartitionLock pattern in src/backend/storage/lmgr/lock.c.
  psch_query_intern_htab = PschShmemInitHash("pg_stat_ch query intern", PschQueryInternMaxEntries(),
                                             &info, HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}

static void MakeKey(PschQueryInternKey* key, const char* query, uint16 query_len) {
  // Zero pad bytes so HASH_BLOBS hashing is deterministic.
  MemSet(key, 0, sizeof(*key));
#if PG_VERSION_NUM >= 170000
  key->query_hash = fasthash64(query, query_len, 0);
#else
  key->query_hash = hash_any_extended((const unsigned char*)query, query_len, 0);
#endif
  key->query_len = query_len;
}

// Partition lock index must come from dynahash's own hashcode (the low-order
// bits, per dynahash.c "we expect callers to use the low-order bits of a
// lookup key's hash value as a partition number").  Using a different hash
// (e.g. our PschQueryInternKey.query_hash field) would mis-align the external
// lock with dynahash's internal partition and risk freelist/bucket corruption.
static LWLock* PartitionLockFor(uint32 hashcode) {
  uint32 idx = hashcode & (PSCH_QUERY_INTERN_PARTITIONS - 1);
  return &psch_query_intern_locks[idx].lock;
}

// Allocate a fresh DSA object holding the query text.
// Returns InvalidDsaPointer on DSA OOM (and bumps the OOM counter, mirroring
// PschDsaAllocString).
static dsa_pointer AllocInternObject(dsa_area* dsa, const char* query, uint16 query_len) {
  dsa_pointer dp = dsa_allocate_extended(dsa, query_len, DSA_ALLOC_NO_OOM);
  if (!DsaPointerIsValid(dp)) {
    pg_atomic_fetch_add_u64(&psch_shared_state->dsa_oom_count, 1);
    return InvalidDsaPointer;
  }
  memcpy(dsa_get_address(dsa, dp), query, query_len);
  return dp;
}

// Caller has matched the key, so lengths are equal.  A false here is a
// (query_hash, query_len) collision, extremely unlikely but must be handled.
static bool ObjectMatches(dsa_area* dsa, dsa_pointer dp, const char* query, uint16 query_len) {
  return memcmp(dsa_get_address(dsa, dp), query, query_len) == 0;
}

dsa_pointer PschQueryInternAcquire(const char* query, uint16 query_len) {
  PschQueryInternKey key;
  uint32 hashcode;
  LWLock* partition;
  PschQueryInternEntry* entry;
  dsa_area* dsa;
  dsa_pointer new_dp;
  bool found;

  if (query_len == 0 || psch_query_intern_htab == NULL) {
    return InvalidDsaPointer;
  }

  dsa = PschDsaAttach();
  if (dsa == NULL) {
    return InvalidDsaPointer;
  }

  MakeKey(&key, query, query_len);
  hashcode = get_hash_value(psch_query_intern_htab, &key);
  partition = PartitionLockFor(hashcode);

  // First lookup: fast path for a hit.
  LWLockAcquire(partition, LW_EXCLUSIVE);
  entry = (PschQueryInternEntry*)hash_search_with_hash_value(psch_query_intern_htab, &key, hashcode,
                                                             HASH_FIND, NULL);
  if (entry != NULL && ObjectMatches(dsa, entry->object, query, query_len)) {
    entry->refcount++;
    LWLockRelease(partition);
    return entry->object;
  }
  if (entry != NULL) {
    // Hash hit but bytes differ — collision. Don't attempt to insert a second
    // entry for the same key; return InvalidDsaPointer so the caller exports
    // empty query text rather than wrong SQL.
    LWLockRelease(partition);
    return InvalidDsaPointer;
  }
  LWLockRelease(partition);

  // Miss: allocate the new object outside the partition lock so we don't
  // hold it across DSA work.
  new_dp = AllocInternObject(dsa, query, query_len);
  if (!DsaPointerIsValid(new_dp)) {
    return InvalidDsaPointer;
  }

  // Re-lock partition and re-check.  Another backend may have inserted the
  // same key while we were allocating.
  LWLockAcquire(partition, LW_EXCLUSIVE);
  entry = (PschQueryInternEntry*)hash_search_with_hash_value(psch_query_intern_htab, &key, hashcode,
                                                             HASH_ENTER_NULL, &found);
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
static void ReleaseRef(dsa_pointer ref, uint16 query_len) {
  dsa_area* dsa;
  PschQueryInternKey key;
  uint32 hashcode;
  LWLock* partition;
  PschQueryInternEntry* entry;
  dsa_pointer freed_dp = InvalidDsaPointer;

  if (!DsaPointerIsValid(ref) || psch_query_intern_htab == NULL) {
    return;
  }

  dsa = PschDsaAttach();
  if (dsa == NULL) {
    return;
  }

  // Rehash the bytes to rebuild the key.  A mismatched length misses below
  // and leaks the object rather than corrupting the HTAB.
  MakeKey(&key, dsa_get_address(dsa, ref), query_len);
  hashcode = get_hash_value(psch_query_intern_htab, &key);
  partition = PartitionLockFor(hashcode);

  LWLockAcquire(partition, LW_EXCLUSIVE);
  entry = (PschQueryInternEntry*)hash_search_with_hash_value(psch_query_intern_htab, &key, hashcode,
                                                             HASH_FIND, NULL);
  if (entry != NULL && entry->object == ref) {
    Assert(entry->refcount > 0);
    entry->refcount--;
    if (entry->refcount == 0) {
      freed_dp = entry->object;
      hash_search_with_hash_value(psch_query_intern_htab, &key, hashcode, HASH_REMOVE, NULL);
    }
  }
  LWLockRelease(partition);

  // Free outside the partition lock so we don't hold it across DSA work.
  if (DsaPointerIsValid(freed_dp)) {
    dsa_free(dsa, freed_dp);
  }
}

// Copy the resolved query bytes into the caller's buffer.  Pure read; does
// not touch the refcount.  No-op when ref is invalid, output args are
// unusable, or the DSA area is gone (caller's buffer is left as-is — the
// public entry point zeroes it first).
//
// We hold a reference (the caller's slot), so the object cannot be freed
// underneath us during the copy.  No partition lock is needed for the read.
static void ResolveInto(dsa_pointer ref, uint16 query_len, char* dst, uint16 dst_size,
                        uint16* out_len) {
  dsa_area* dsa;
  uint16 copy_len;

  if (!DsaPointerIsValid(ref) || dst == NULL || dst_size == 0 || out_len == NULL) {
    return;
  }

  dsa = PschDsaAttach();
  if (dsa == NULL) {
    return;
  }

  copy_len = Min(query_len, (uint16)(dst_size - 1));
  memcpy(dst, dsa_get_address(dsa, ref), copy_len);
  dst[copy_len] = '\0';
  *out_len = copy_len;
}

void PschQueryInternResolveAndRelease(dsa_pointer ref, uint16 query_len, char* dst, uint16 dst_size,
                                      uint16* out_len) {
  // Zero outputs first so callers see a consistent empty result on any
  // failure inside ResolveInto.  The release is independent of the resolve:
  // ReleaseRef always runs and has its own guards for invalid ref / no DSA,
  // so passing degenerate output args still drops the reference.
  if (out_len != NULL) {
    *out_len = 0;
  }
  if (dst != NULL && dst_size > 0) {
    dst[0] = '\0';
  }

  ResolveInto(ref, query_len, dst, dst_size, out_len);
  ReleaseRef(ref, query_len);
}
