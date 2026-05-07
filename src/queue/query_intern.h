// pg_stat_ch shared query-text interner.
//
// Without interning, every queued event owns a private DSA copy of the
// normalized query text.  For repeated long-running queries the live DSA
// footprint is `queued_events * query_len`, which exhausts the bounded DSA
// pool well before the queue fills.
//
// The interner deduplicates query text across queued events: each distinct
// (dbid, queryid, query_hash, query_len) tuple maps to a single DSA-allocated
// `PschQueryInternObject`.  Queue slots store a `dsa_pointer` to that object
// and a refcount-managed shared HTAB tracks lifetime.  Live DSA usage drops to
// `distinct_live_query_texts * query_len`.
//
// Layout in shared memory:
//   - HTAB allocated from extension shmem (sized via hash_estimate_size).
//   - Variable-length query bodies live inside the existing pg_stat_ch DSA
//     area (same pool used for err_message strings).
//
// Concurrency: a small set of partitioned LWLocks (PSCH_QUERY_INTERN_PARTITIONS)
// protects the HTAB and per-entry refcount.  The partition is selected from
// the query_hash so unrelated query texts contend on different locks.
#ifndef PG_STAT_CH_SRC_QUEUE_QUERY_INTERN_H_
#define PG_STAT_CH_SRC_QUEUE_QUERY_INTERN_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "utils/dsa.h"

// Number of LWLock partitions guarding the interner HTAB.  Power of two so
// the partition index can be derived from query_hash with a bitmask.
#define PSCH_QUERY_INTERN_PARTITIONS 32

// Add the HTAB size requirement to the running total of extension shmem.
// Caller passes the shared memory size accumulator; this function increases
// it to include hash_estimate_size for the interner.
Size PschQueryInternShmemSize(void);

// Reserve the partition LWLocks (caller must already have requested the main
// pg_stat_ch tranche; this function adds to that single tranche request).
//
// Returns the number of additional LWLocks required.
int PschQueryInternLockCount(void);

// Initialize the shared HTAB and bind the partition LWLocks.  Must be called
// from PschShmemStartupHook with AddinShmemInitLock held, after the main
// PschSharedState has been initialized (so `psch_shared_state` is valid).
//
// `lwlock_base` must point at the first interner partition lock inside the
// pg_stat_ch named tranche (i.e. immediately after the main queue lock).
void PschQueryInternShmemInit(void* lwlock_base);

// Acquire (or create) an intern reference for the given query text.
//
// On hit: increments the entry's refcount and returns the existing DSA
// pointer.  On miss: allocates a new DSA object, inserts it with refcount=1,
// and returns the new DSA pointer.  Returns InvalidDsaPointer if `len == 0`,
// the DSA handle is unavailable, DSA allocation fails, the shared HTAB is
// full, or a hash collision is detected against a different query text
// (collisions are treated as a miss with no insert — exporting empty query
// text is preferable to exporting the wrong SQL).
//
// Must be called by a backend that has already attached to the DSA area.
dsa_pointer PschQueryInternAcquire(Oid dbid, uint64 queryid, const char* query, uint16 query_len);

// Resolve `ref` into the caller's buffer and drop one reference.
//
// Copies up to `dst_size - 1` bytes of the interned query into `dst` and
// null-terminates.  Sets `*out_len` to the number of bytes copied.  Then
// decrements the refcount; if the refcount reaches zero the entry is removed
// from the HTAB and the DSA object is freed.
//
// If `ref` is InvalidDsaPointer, sets `dst[0] = '\0'` and `*out_len = 0`.
//
// Called by the bgworker (single consumer) on every dequeued event.
void PschQueryInternResolveAndRelease(dsa_pointer ref, char* dst, uint16 dst_size, uint16* out_len);

// Drop one reference without resolving the bytes.  Used when an enqueue path
// has acquired a reference but decides not to publish the ring slot.
void PschQueryInternRelease(dsa_pointer ref);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_QUEUE_QUERY_INTERN_H_
