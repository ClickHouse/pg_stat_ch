// pg_stat_ch DSA (Dynamic Shared Memory Area) string storage
//
// Variable-length query text and error messages are stored in a DSA area
// rather than inline in ring buffer slots.  This reduces per-slot size from
// ~4.5KB (with 2×2KB char arrays) to ~500 bytes (with 2×8-byte dsa_pointer).
//
// SHARED MEMORY LAYOUT
// ====================
//
//  ┌───────────────────────────────────────────────────────────────────┐
//  │  PschSharedState                                                 │
//  │  (lock, capacity, raw_dsa_area, atomics, exporter stats, ...)    │
//  ├───────────────────────────────────────────────────────────────────┤
//  │  PschRingEntry[0]   (~500 bytes)                                 │
//  │  PschRingEntry[1]                                                │
//  │  ...                                                             │
//  │  PschRingEntry[capacity-1]                                       │
//  ├─── MAXALIGN boundary ─────────────────────────────────────────────┤
//  │  DSA area  (pg_stat_ch.string_area_size MB)                      │
//  │  ┌─────────────────────────────────────────────────────────────┐  │
//  │  │ DSA control structures (freelists, size classes, locks)     │  │
//  │  │ Superblock: "SELECT * FR..." ◄── ring[3].query_dsa         │  │
//  │  │ Superblock: "division by..." ◄── ring[7].err_message_dsa   │  │
//  │  │ (free)                                                     │  │
//  │  │ ...                                                        │  │
//  │  └─────────────────────────────────────────────────────────────┘  │
//  └───────────────────────────────────────────────────────────────────┘
//
// LIFECYCLE
// =========
//  1. Postmaster: PschDsaInit()  — dsa_create_in_place, dsa_pin, dsa_detach
//  2. Backends:   PschDsaAllocString() calls PschDsaAttach() lazily on first use
//  3. Bgworker:   PschDsaAttach() eagerly at startup, then PschDsaResolveString()
//                 on each dequeue (resolves pointer + frees DSA memory)
//
// CONCURRENCY
// ===========
//  Producers call PschDsaAllocString() under the ring buffer LWLock, so DSA
//  allocation is serialized with slot writes.  The consumer (bgworker) calls
//  PschDsaResolveString() lock-free.  DSA's internal per-size-class LWLocks
//  handle producer-vs-consumer contention on the allocator itself.
#ifndef PG_STAT_CH_SRC_QUEUE_PSCH_DSA_H_
#define PG_STAT_CH_SRC_QUEUE_PSCH_DSA_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "datatype/timestamp.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"

// Shared state in shared memory.
//
// Fields are grouped by writer and separated with cache-line padding to avoid
// false sharing.  See shmem.cc for the concurrency model.
//
// Ring buffer slots (PschRingEntry[capacity]) follow immediately after this
// struct in the shared memory segment.
struct PschSharedState {
  // --- Init-time fields (written once by postmaster) -----------------------
  LWLock* lock;
  void* raw_dsa_area;
  pg_atomic_uint64 dsa_oom_count;
  uint32 capacity;  // must be power of 2

  char pad1[PG_CACHE_LINE_SIZE -
            (sizeof(LWLock*) + sizeof(void*) + sizeof(pg_atomic_uint64) + sizeof(uint32))];

  // --- Producer hotpath (written by many backends under lock) --------------
  pg_atomic_uint64 head;
  pg_atomic_uint64 enqueued;
  pg_atomic_uint64 dropped;
  pg_atomic_flag overflow_logged;

  char pad2[PG_CACHE_LINE_SIZE - (3 * sizeof(pg_atomic_uint64) + sizeof(pg_atomic_flag))];

  // --- Consumer hotpath (written only by bgworker) -------------------------
  pg_atomic_uint64 tail;
  pg_atomic_uint64 exported;

  char pad3[PG_CACHE_LINE_SIZE - (2 * sizeof(pg_atomic_uint64))];

  // --- Exporter stats (written by bgworker, read by stats function) --------
  pg_atomic_uint64 send_failures;
  TimestampTz last_success_ts;
  TimestampTz last_error_ts;
  char last_error_text[256];
  pg_atomic_uint32 bgworker_pid;
};

// Global pointer to shared state (set in shmem startup)
extern PschSharedState* psch_shared_state;

// Compute the DSA portion of shared memory size (bytes).
Size PschDsaShmemSize(void);

// Create the DSA area inside the shared memory segment.
// Called once by the postmaster during InitializeSharedState().
// `dsa_place` must point to a MAXALIGN'd address with at least
// PschDsaShmemSize() bytes available.
void PschDsaInit(struct PschSharedState* state, void* dsa_place);

// Attach to the DSA area (lazy, idempotent).
// Must be called before PschDsaAllocString / PschDsaResolveString.
// Backends attach lazily on first enqueue; bgworker attaches eagerly at startup.
void PschDsaAttach(void);

// Get the process-local DSA handle, attaching lazily if needed.
// Returns nullptr if shared state / raw_dsa_area is unavailable.
dsa_area* PschDsaGetArea(void);

// Allocate a DSA string from an inline buffer.
// Returns InvalidDsaPointer on zero-length, unavailable DSA handle, or allocation
// failure.
// On OOM, increments state->dsa_oom_count atomically.
dsa_pointer PschDsaAllocString(const char* src, uint16 len, uint16 max_len);

// Resolve a DSA string into a caller-owned buffer and free the DSA memory.
// If dp is InvalidDsaPointer, or the DSA handle is unavailable, sets dst_buf to
// "" and *out_len to 0. If the DSA handle is unavailable, the shared allocation
// is left in place because it can't be safely resolved/freed.
void PschDsaResolveString(dsa_pointer dp, uint16 src_len, char* dst_buf, uint16 max_len,
                          uint16* out_len);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_QUEUE_PSCH_DSA_H_
