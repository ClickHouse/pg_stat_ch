// pg_stat_ch shared memory ring buffer implementation
//
// This implements a multi-producer, single-consumer (MPSC) ring buffer for query
// telemetry events. Design patterns are borrowed from PostgreSQL core (shm_mq.c,
// sinvaladt.c) and pg_stat_monitor for robustness.
//
// KEY DESIGN DECISIONS:
//
// 1. LOCK-FREE CONSUMER (pattern from shm_mq.c):
//    The bgworker consumer reads without locks using atomic operations and memory
//    barriers. This prevents producer contention from blocking the consumer.
//
// 2. POWER-OF-2 CAPACITY (pattern from sinvaladt.c):
//    Ring buffer size must be power-of-2 to allow fast modulo via bitmask:
//    slot_index = position & (capacity - 1)  // Fast!
//    Instead of: position % capacity          // Slow division
//
// 3. MEMORY BARRIERS (pattern from shm_mq.c):
//    - pg_memory_barrier() ensures data is written before updating head/tail
//    - pg_read_barrier() ensures counters are read before accessing data
//    - pg_write_barrier() ensures data is copied before updating tail
//    Critical for correctness on weakly-ordered architectures (ARM, PowerPC)
//
// 4. ERROR-SAFE LOCKING (pattern from pg_stat_monitor):
//    PG_TRY/PG_FINALLY blocks ensure locks are released even if an error occurs
//    during enqueue, preventing deadlocks.
//
// 5. GRACEFUL OVERFLOW (pattern from pg_stat_monitor):
//    When queue is full, we log a warning once (overflow_logged flag) and drop
//    events. This prevents log spam while alerting operators to capacity issues.

#include "postgres.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "config/guc.h"
#include "config/memory_budget.h"
#include "hooks/hooks.h"
#include "queue/local_batch.h"
#include "queue/psch_dsa.h"
#include "queue/query_intern.h"
#include "queue/ring_entry.h"
#include "queue/shmem.h"

PschSharedState* psch_shared_state = NULL;

// Previous hook values for chaining
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif

static inline PschRingEntry* GetRingBuffer(void) {
  return (PschRingEntry*)((char*)psch_shared_state + sizeof(PschSharedState));
}

// Handle queue overflow: increment dropped counter and log warning once
//
// DROP-ON-OVERFLOW STRATEGY: When the queue is full, we drop events rather than
// blocking producers. This is intentional:
// - Blocking would add latency to user queries, defeating the purpose of telemetry
// - Telemetry is "best effort" - missing some events during spikes is acceptable
// - The dropped counter lets operators detect and address capacity issues
//
// SIZING GUIDANCE: The ring is sized from pg_stat_ch.memory_limit (see
// src/config/memory_budget.h). Monitor pg_stat_ch_stats().queue_size and
// .dropped to tune the budget.
//
// DETECTING OVERFLOW: Check pg_stat_ch_stats().dropped > 0 or look for the WARNING
// in PostgreSQL logs. The overflow_logged flag prevents log spam by warning only once
// until stats are reset.
static void HandleOverflow(void) {
  pg_atomic_fetch_add_u64(&psch_shared_state->dropped, 1);

  // Log overflow warning once to avoid log spam (pg_stat_monitor pattern)
  if (pg_atomic_test_set_flag(&psch_shared_state->overflow_logged)) {
    ereport(WARNING, (errmsg("pg_stat_ch: queue overflow, events being dropped"),
                      errhint("Raise pg_stat_ch.memory_limit (requires restart), or reduce capture "
                              "volume with pg_stat_ch.sample_rate / pg_stat_ch.min_duration_us "
                              "(no restart).")));
  }
}

// Size of the fixed-field prefix shared between PschEvent and PschRingEntry.
// Both structs have identical layout from offset 0 through query_len — the
// last field before the variable-length data diverges (char arrays vs
// dsa_pointers).  Verified by static assert in ring_entry.h.
static const size_t kFixedPrefixSize = offsetof(PschRingEntry, err_message_dsa);

// Check queue fullness and enqueue event if space available.
// Called with lock held.  Returns true if event was enqueued.
//
// DSA STRING STORAGE: The ring buffer stores PschRingEntry (compact ~500-byte
// slots) rather than PschEvent (~4.5KB).  Query text and error messages are
// allocated in a DSA area and referenced by dsa_pointer.  On DSA OOM the
// event is still enqueued with numeric data intact; only the string is lost.
static bool TryEnqueueLocked(const PschEvent* event, uint32 capacity) {
  uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
  uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);

  if (head - tail >= capacity || psch_debug_force_locked_overflow) {
    HandleOverflow();
    return false;
  }

  // Fast modulo via bitmask (requires power-of-2 capacity, enforced by GUC check)
  uint32 mask = capacity - 1;
  PschRingEntry* slot = &GetRingBuffer()[head & mask];

  // 1. Copy the entire fixed-field prefix (all numeric fields, app_name,
  //    client_addr, lengths — everything before the variable-length data).
  memcpy(slot, event, kFixedPrefixSize);

  // 2. Allocate DSA for err_message and query text
  slot->err_message_dsa =
      PschDsaAllocString(event->err_message, event->err_message_len, PSCH_MAX_ERR_MSG_LEN);
  if (event->err_message_len > 0 && !DsaPointerIsValid(slot->err_message_dsa)) {
    slot->err_message_len = 0;  // Lost string on OOM — numeric data preserved
  }

  // Query text goes through the shared interner so repeated identical
  // normalized queries share a single DSA-allocated body.  See query_intern.h
  // for the design rationale.  On miss + DSA OOM, miss + hash-full, or hash
  // collision we drop the query bytes (numeric data is preserved).
  if (event->query_len > 0) {
    // Clamp the input length to what we'd have stored anyway, so the intern
    // key doesn't include trailing bytes the consumer would have truncated.
    uint16 clamped_len = Min(event->query_len, (uint16)(PSCH_MAX_QUERY_LEN - 1));
    slot->query_dsa =
        PschQueryInternAcquire(event->dbid, event->queryid, event->query, clamped_len);
    if (!DsaPointerIsValid(slot->query_dsa)) {
      slot->query_len = 0;
    } else {
      slot->query_len = clamped_len;
    }
  } else {
    slot->query_dsa = InvalidDsaPointer;
  }

  // CRITICAL: Memory barrier ensures the event data is written to shared memory
  // before we update head. Without this, the consumer might read stale data on
  // weakly-ordered architectures (ARM, PowerPC). Pattern from shm_mq.c.
  pg_memory_barrier();
  pg_atomic_write_u64(&psch_shared_state->head, head + 1);
  pg_atomic_fetch_add_u64(&psch_shared_state->enqueued, 1);

  return true;
}

static void PschShmemShutdown(int code pg_attribute_unused(), Datum arg pg_attribute_unused()) {
  if (psch_shared_state != NULL) {
    elog(LOG,
         "pg_stat_ch: shutdown (enqueued=" UINT64_FORMAT ", dropped=" UINT64_FORMAT
         ", exported=" UINT64_FORMAT ", export_dropped=" UINT64_FORMAT ")",
         pg_atomic_read_u64(&psch_shared_state->enqueued),
         pg_atomic_read_u64(&psch_shared_state->dropped),
         pg_atomic_read_u64(&psch_shared_state->exported),
         pg_atomic_read_u64(&psch_shared_state->export_dropped));
  }
}

// Size of the contiguous shmem block that ShmemInitStruct("pg_stat_ch", ...)
// allocates.  Layout: [PschSharedState] [PschRingEntry × capacity] [DSA area].
//
// All components are sized from the resolved memory budget (memory_budget.h),
// not from raw GUC reads — the budget is the single authority that charges
// ring + intern + DSA + export arena against pg_stat_ch.memory_limit.
static Size PschSharedBlockSize(void) {
  const PschMemoryBudget* budget = PschMemoryBudgetGet();
  Size ring_end =
      add_size(sizeof(PschSharedState), mul_size(budget->ring_slots, sizeof(PschRingEntry)));
  Size dsa_offset = MAXALIGN(ring_end);
  // PschDsaInit() still sizes the in-place area from psch_string_area_size
  // (written back by the budget).  Reserve the max of the two so the created
  // area can never overrun the reservation, even if write-back rounds the MB
  // value up.
  Size dsa_size = Max((Size)budget->dsa_bytes, PschDsaShmemSize());
  Size total = add_size(dsa_offset, dsa_size);
  return MAXALIGN(total);
}

Size PschShmemSize(void) {
  // Total shmem requested from the postmaster: the shared block plus the
  // interner HTAB (which ShmemInitHash carves out of the same pool).
  return add_size(PschSharedBlockSize(), PschQueryInternShmemSize());
}

static void RequestSharedResources(void) {
  // Resolve the budget and write the component values back to their GUCs
  // BEFORE any size computation: PschQueryInternShmemSize() still reads
  // psch_queue_capacity, which only matches budget->ring_slots after the
  // write-back.  SetConfigOption is legal here — this runs in the postmaster
  // (shmem_request_hook on PG15+, _PG_init below) before shmem creation, the
  // wal_buffers/XLOGShmemSize idiom.
  PschMemoryBudgetWriteBack();
  PschMemoryBudgetLogStartup();

  RequestAddinShmemSpace(PschShmemSize());
  // 1 main queue lock + N partition locks for the query-text interner, all
  // in the single "pg_stat_ch" named tranche so we read them as a contiguous
  // LWLockPadded[] from GetNamedLWLockTranche().
  RequestNamedLWLockTranche("pg_stat_ch", 1 + PschQueryInternLockCount());
}

#if PG_VERSION_NUM >= 150000
static void PschShmemRequestHook(void) {
  if (prev_shmem_request_hook != NULL) {
    prev_shmem_request_hook();
  }
  RequestSharedResources();
}
#endif

// Initialize shared state fields on first-time setup.
// Called with AddinShmemInitLock held.
static void InitializeSharedState(void) {
  const PschMemoryBudget* budget = PschMemoryBudgetGet();

  // The pg_stat_ch named tranche owns 1 main queue lock at index 0 and
  // PSCH_QUERY_INTERN_PARTITIONS partition locks at indices 1..N for the
  // query-text interner (see PschQueryInternShmemInit).
  LWLockPadded* lwlocks = GetNamedLWLockTranche("pg_stat_ch");
  psch_shared_state->lock = &lwlocks[0].lock;
  psch_shared_state->capacity = budget->ring_slots;
  pg_atomic_init_u64(&psch_shared_state->head, 0);
  pg_atomic_init_u64(&psch_shared_state->enqueued, 0);
  pg_atomic_init_u64(&psch_shared_state->dropped, 0);
  pg_atomic_init_flag(&psch_shared_state->overflow_logged);
  pg_atomic_init_u64(&psch_shared_state->tail, 0);
  pg_atomic_init_u64(&psch_shared_state->exported, 0);

  pg_atomic_init_u64(&psch_shared_state->send_failures, 0);
  pg_atomic_init_u64(&psch_shared_state->export_dropped, 0);
  psch_shared_state->last_success_ts = 0;
  psch_shared_state->last_error_ts = 0;
  MemSet(psch_shared_state->last_error_text, 0, sizeof(psch_shared_state->last_error_text));
  pg_atomic_init_u32(&psch_shared_state->bgworker_pid, 0);
  pg_atomic_init_u64(&psch_shared_state->dsa_oom_count, 0);

  // Zero ring buffer (InvalidDsaPointer == 0, so dsa fields are implicitly invalid)
  MemSet(GetRingBuffer(), 0, (Size)budget->ring_slots * sizeof(PschRingEntry));

  // Create DSA area for variable-length string storage.
  // See psch_dsa.h for the shared memory layout diagram.
  char* dsa_place =
      (char*)psch_shared_state +
      MAXALIGN(sizeof(PschSharedState) + (Size)budget->ring_slots * sizeof(PschRingEntry));
  PschDsaInit(psch_shared_state, dsa_place);

  elog(LOG, "pg_stat_ch: initialized shared memory (capacity=%u, ring=%zuKB, dsa=%zuMB, total=%zu)",
       budget->ring_slots, ((Size)budget->ring_slots * sizeof(PschRingEntry)) / 1024,
       (Size)budget->dsa_bytes / (1024 * 1024), PschShmemSize());
}

static void PschShmemStartupHook(void) {
  bool found;

  if (prev_shmem_startup_hook != NULL) {
    prev_shmem_startup_hook();
  }

  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

  // ShmemInitStruct gets just the contiguous shared block (state + ring + DSA);
  // the interner HTAB lives in a separately-named shmem segment allocated by
  // PschQueryInternShmemInit -> ShmemInitHash, drawing from the same pool that
  // RequestAddinShmemSpace(PschShmemSize()) reserved.
  psch_shared_state =
      (PschSharedState*)ShmemInitStruct("pg_stat_ch", PschSharedBlockSize(), &found);

  if (psch_shared_state == NULL) {
    LWLockRelease(AddinShmemInitLock);
    ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("pg_stat_ch: could not create shared memory segment")));
  }

  if (!found) {
    InitializeSharedState();
  }

  // Initialize (or attach to) the query intern HTAB. All backends get same handle.
  LWLockPadded* lwlocks = GetNamedLWLockTranche("pg_stat_ch");
  PschQueryInternShmemInit(&lwlocks[1]);

  LWLockRelease(AddinShmemInitLock);

  on_shmem_exit(PschShmemShutdown, 0);
}

void PschInstallShmemHooks(void) {
#if PG_VERSION_NUM >= 150000
  prev_shmem_request_hook = shmem_request_hook;
  shmem_request_hook = PschShmemRequestHook;
#else
  RequestSharedResources();
#endif

  prev_shmem_startup_hook = shmem_startup_hook;
  shmem_startup_hook = PschShmemStartupHook;
}

// Enqueue an event to the ring buffer (multi-producer, called from executor hooks)
//
// CONCURRENCY: Multiple backends can call this simultaneously. We use a
// double-checked locking pattern with a lock-free fast path for overflow:
//
// 1. LOCK-FREE FAST PATH: Check if queue is full using atomic reads. If full,
//    handle overflow entirely with atomics - NO LOCK ACQUIRED. This eliminates
//    lock contention during overflow conditions.
//
// 2. SLOW PATH: If not full, acquire lock, re-check (TOCTOU handling), enqueue.
//
// This pattern is critical for performance under overflow: without it, every
// producer contends for an exclusive lock just to increment an atomic counter.
//
// ERROR HANDLING: PG_TRY/PG_CATCH ensures the lock is released even if an error
// occurs (e.g., out of memory during memcpy, or signal interrupt). Without this,
// a backend crash could leave the lock held indefinitely, blocking all producers.
//
// OVERFLOW STRATEGY: When full, we drop the event and increment the 'dropped'
// counter. The overflow_logged flag prevents log spam - we warn once, then silently
// drop until stats are reset. This is better than blocking producers or growing
// the queue unboundedly.
bool PschEnqueueEvent(const PschEvent* event) {
  if (psch_shared_state == NULL || !psch_enabled) {
    return false;
  }

  // Suppress error capture for the duration of this function to prevent deadlock.
  // HandleOverflow() calls ereport(WARNING) which triggers emit_log_hook, which
  // would re-enter PschEnqueueEvent() and deadlock on LWLockAcquire.
  PschSuppressErrorCapture(true);

  uint32 capacity = psch_shared_state->capacity;

  // === LOCK-FREE FAST PATH: Check for overflow without acquiring lock ===
  // This eliminates lock contention during overflow conditions.
  uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
  pg_read_barrier();  // Ensure head is read before tail for correct ordering
  uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);

  if (head - tail >= capacity) {
    HandleOverflow();
    PschSuppressErrorCapture(false);
    return false;
  }

  // === TRY-LOCK PATH: Attempt non-blocking lock ===
  if (LWLockConditionalAcquire(psch_shared_state->lock, LW_EXCLUSIVE)) {
    bool result = false;
    PG_TRY();
    { result = TryEnqueueLocked(event, capacity); }
    PG_FINALLY();
    { LWLockRelease(psch_shared_state->lock); }
    PG_END_TRY();
    PschSuppressErrorCapture(false);
    return result;
  }

  // === CONTENDED PATH: Buffer locally, flush at transaction end ===
  PschSuppressErrorCapture(false);
  PschLocalBatchAdd(event);
  return true;
}

// Enqueue a batch of events under a single lock acquisition.
// Called by PschLocalBatchFlush to amortize lock overhead across multiple events.
int PschEnqueueBatch(const PschEvent* events, int count) {
  if (psch_shared_state == NULL || !psch_enabled || count == 0)
    return 0;

  PschSuppressErrorCapture(true);
  uint32 capacity = psch_shared_state->capacity;

  uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
  pg_read_barrier();
  uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);
  if (head - tail >= capacity) {
    for (int i = 0; i < count; i++)
      HandleOverflow();
    PschSuppressErrorCapture(false);
    return 0;
  }

  int enqueued = 0;
  LWLockAcquire(psch_shared_state->lock, LW_EXCLUSIVE);
  PG_TRY();
  {
    for (int i = 0; i < count; i++) {
      if (TryEnqueueLocked(&events[i], capacity))
        enqueued++;
    }
  }
  PG_FINALLY();
  { LWLockRelease(psch_shared_state->lock); }
  PG_END_TRY();

  PschSuppressErrorCapture(false);
  return enqueued;
}

// Read-only resolve of a per-event DSA string (err_message).  Mirrors
// PschDsaResolveString but never frees: peek must leave the ring slot's
// allocations intact so a failed export can retry the same events.
static void DsaPeekString(dsa_pointer dp, uint16 src_len, char* dst_buf, uint16 max_len,
                          uint16* out_len) {
  dsa_area* dsa;
  char* src;
  uint16 len;

  dst_buf[0] = '\0';
  *out_len = 0;

  if (!DsaPointerIsValid(dp)) {
    return;
  }
  dsa = PschDsaGetArea();
  if (dsa == NULL) {
    return;
  }
  src = (char*)dsa_get_address(dsa, dp);
  len = Min(src_len, (uint16)(max_len - 1));
  memcpy(dst_buf, src, len);
  dst_buf[len] = '\0';
  *out_len = len;
}

// Two-phase consume, phase 1 (single-consumer, called by bgworker).
//
// LOCK-FREE DESIGN: Runs lock-free because there's only one consumer (the
// bgworker). Atomic reads + memory barriers instead of locks, pattern from
// PostgreSQL's shm_mq.c: pg_read_barrier() after reading head ensures the
// producer's slot writes are visible before we copy.
//
// The tail does NOT advance and no DSA memory is freed: producers cannot
// reuse the peeked slots (they check head - tail against capacity), so the
// copies stay valid and a failed export can peek the same events again.
// Interned query objects stay referenced by the slots, so the read-only
// resolve cannot race a free.
//
// WRAPAROUND SAFETY: uint64 head/tail wrap at the same point (2^64), so
// (head - tail) arithmetic stays correct (~584 years at 1M events/sec).
int PschPeekEvents(PschEvent* buf, int max) {
  uint64 head;
  uint64 tail;
  uint64 avail;
  uint32 mask;
  int n;
  int i;

  if (psch_shared_state == NULL || buf == NULL || max <= 0) {
    return 0;
  }

  head = pg_atomic_read_u64(&psch_shared_state->head);
  pg_read_barrier();
  tail = pg_atomic_read_u64(&psch_shared_state->tail);

  avail = head - tail;
  if (avail == 0) {
    return 0;
  }
  n = (avail < (uint64)max) ? (int)avail : max;
  mask = psch_shared_state->capacity - 1;

  for (i = 0; i < n; i++) {
    PschRingEntry* slot = &GetRingBuffer()[(tail + (uint64)i) & mask];
    PschEvent* event = &buf[i];

    // Copy the fixed-field prefix, then resolve the DSA-backed strings into
    // the event's inline buffers without freeing or releasing anything.
    memcpy(event, slot, kFixedPrefixSize);
    DsaPeekString(slot->err_message_dsa, slot->err_message_len, event->err_message,
                  PSCH_MAX_ERR_MSG_LEN, &event->err_message_len);
    PschQueryInternResolve(slot->query_dsa, event->query, PSCH_MAX_QUERY_LEN, &event->query_len);
  }

  return n;
}

// Two-phase consume, phase 2: free the DSA strings of the first n queued
// events and advance the tail past them.  Slot dsa_pointers are cleared as
// they are freed so that a longjmp landing mid-loop cannot cause a double
// free on a later retry (the not-yet-advanced tail would re-expose the
// slots).  The write barrier ensures all frees complete before producers can
// reuse the slots.
void PschConsumeEvents(int n) {
  uint64 head;
  uint64 tail;
  uint32 mask;
  dsa_area* dsa;
  int i;

  if (psch_shared_state == NULL || n <= 0) {
    return;
  }

  head = pg_atomic_read_u64(&psch_shared_state->head);
  pg_read_barrier();
  tail = pg_atomic_read_u64(&psch_shared_state->tail);

  if ((uint64)n > head - tail) {
    elog(WARNING, "pg_stat_ch: consume of %d events exceeds " UINT64_FORMAT " queued; clamping", n,
         head - tail);
    n = (int)(head - tail);
  }

  dsa = PschDsaGetArea();
  mask = psch_shared_state->capacity - 1;

  for (i = 0; i < n; i++) {
    PschRingEntry* slot = &GetRingBuffer()[(tail + (uint64)i) & mask];
    dsa_pointer err_dsa = slot->err_message_dsa;
    dsa_pointer query_dsa = slot->query_dsa;

    // Clear the slot pointers BEFORE freeing.  dsa_free / the interner release
    // can ereport(ERROR) on a damaged DSA, and PschExportBatch runs inside the
    // drain loop's PG_TRY/PG_CATCH (bgworker.c): a longjmp out of a free leaves
    // the tail un-advanced, so the slot would be re-resolved (use-after-free)
    // and re-freed (double free) on the next cycle.  Clearing first means such
    // a longjmp merely leaks the allocation — the documented safe policy
    // (matching PschDsaResolveString) — instead of corrupting the DSA.
    slot->err_message_dsa = InvalidDsaPointer;
    slot->query_dsa = InvalidDsaPointer;

    if (DsaPointerIsValid(err_dsa) && dsa != NULL) {
      dsa_free(dsa, err_dsa);
    }

    // NULL output args make this a pure refcount release (see query_intern.h).
    PschQueryInternResolveAndRelease(query_dsa, NULL, 0, NULL);
  }

  pg_write_barrier();
  pg_atomic_write_u64(&psch_shared_state->tail, tail + (uint64)n);
}

// Validate ring invariants at bgworker start.  The SIGABRT backstop exits
// with _exit(1), which skips shmem reinit — if the abort happened while ring
// state was being mutated, the indices could be inconsistent.  Producers
// mutate head only under the main lock, so holding it exclusively makes the
// paired reset race-free; lock-free fast-path readers may observe one
// spurious overflow during the reset, which is acceptable for a corrupt-only
// worker-start path.
void PschRingSanityCheck(void) {
  const PschMemoryBudget* budget;
  uint64 head;
  uint64 tail;
  uint32 capacity;
  bool corrupt = false;

  if (psch_shared_state == NULL) {
    return;
  }
  budget = PschMemoryBudgetGet();

  LWLockAcquire(psch_shared_state->lock, LW_EXCLUSIVE);
  capacity = psch_shared_state->capacity;
  head = pg_atomic_read_u64(&psch_shared_state->head);
  tail = pg_atomic_read_u64(&psch_shared_state->tail);

  if (capacity == 0 || (capacity & (capacity - 1)) != 0 || capacity != budget->ring_slots) {
    psch_shared_state->capacity = budget->ring_slots;
    corrupt = true;
  }
  if (head - tail > (uint64)psch_shared_state->capacity) {
    corrupt = true;
  }

  if (corrupt) {
    // Discards queued events: their slot mapping / DSA pointers can no
    // longer be trusted, so referenced DSA strings are leaked, not freed.
    // Tail is reset before head: fast-path readers load head first, so a
    // torn pair can never underflow (head - tail); at worst one producer
    // sees a stale-full ring and overflow-drops a single event.
    pg_atomic_write_u64(&psch_shared_state->tail, 0);
    pg_write_barrier();
    pg_atomic_write_u64(&psch_shared_state->head, 0);
    elog(LOG,
         "pg_stat_ch: ring state corrupt at worker start (capacity=%u head=" UINT64_FORMAT
         " tail=" UINT64_FORMAT "); indices reset, queued events discarded",
         capacity, head, tail);
  }
  LWLockRelease(psch_shared_state->lock);
}

// Get queue statistics (called by SQL function pg_stat_ch_stats())
//
// MEMORY BARRIER: We insert a full barrier between reading cumulative counters
// (enqueued, dropped, exported) and reading positions (head, tail). This ensures
// we get a consistent snapshot where the counters reflect the same or earlier state
// than the positions. Without this, on weakly-ordered CPUs, we might read:
// - old counter values with new positions, or
// - new counter values with old positions
// causing temporary inconsistencies in the reported queue_size vs counters.
void PschGetStats(uint64* enqueued, uint64* dropped, uint64* exported, uint32* queue_size,
                  uint32* queue_capacity, uint64* send_failures, TimestampTz* last_success_ts,
                  char* last_error_buf, size_t last_error_buf_size, TimestampTz* last_error_ts,
                  uint64* dsa_oom_count) {
  if (psch_shared_state == NULL) {
    *enqueued = 0;
    *dropped = 0;
    *exported = 0;
    *queue_size = 0;
    *queue_capacity = 0;
    *send_failures = 0;
    *last_success_ts = 0;
    if (last_error_buf_size > 0)
      last_error_buf[0] = '\0';
    *last_error_ts = 0;
    *dsa_oom_count = 0;
    return;
  }

  *enqueued = pg_atomic_read_u64(&psch_shared_state->enqueued);
  *dropped = pg_atomic_read_u64(&psch_shared_state->dropped);
  *exported = pg_atomic_read_u64(&psch_shared_state->exported);
  *send_failures = pg_atomic_read_u64(&psch_shared_state->send_failures);
  *dsa_oom_count = pg_atomic_read_u64(&psch_shared_state->dsa_oom_count);

  // Full barrier for consistent snapshot (counters before positions)
  pg_memory_barrier();

  uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
  uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);
  *queue_size = (uint32)(head - tail);
  *queue_capacity = psch_shared_state->capacity;

  // Copy exporter timestamps and error text under lock to prevent torn reads.
  // The error text is copied into the caller's buffer so it remains valid after
  // the lock is released (returning a pointer into shmem would allow torn reads).
  LWLockAcquire(psch_shared_state->lock, LW_SHARED);
  *last_success_ts = psch_shared_state->last_success_ts;
  *last_error_ts = psch_shared_state->last_error_ts;
  strlcpy(last_error_buf, psch_shared_state->last_error_text, last_error_buf_size);
  LWLockRelease(psch_shared_state->lock);
}

// Reset statistics counters (called by SQL function pg_stat_ch_reset())
//
// LOCKING: We use the same lock as enqueue to prevent race conditions. Without
// this, a concurrent enqueue could increment a counter after we zero it but
// before we return, losing that count permanently.
//
// OVERFLOW FLAG RESET: Clearing overflow_logged allows the warning to be logged
// again on the next overflow, which is useful after capacity has been increased.
void PschResetStats(void) {
  if (psch_shared_state == NULL) {
    return;
  }

  LWLockAcquire(psch_shared_state->lock, LW_EXCLUSIVE);
  pg_atomic_write_u64(&psch_shared_state->enqueued, 0);
  pg_atomic_write_u64(&psch_shared_state->dropped, 0);
  pg_atomic_write_u64(&psch_shared_state->exported, 0);
  pg_atomic_write_u64(&psch_shared_state->send_failures, 0);
  pg_atomic_write_u64(&psch_shared_state->export_dropped, 0);
  pg_atomic_clear_flag(&psch_shared_state->overflow_logged);
  pg_atomic_write_u64(&psch_shared_state->dsa_oom_count, 0);
  psch_shared_state->last_success_ts = 0;
  psch_shared_state->last_error_ts = 0;
  MemSet(psch_shared_state->last_error_text, 0, sizeof(psch_shared_state->last_error_text));
  LWLockRelease(psch_shared_state->lock);
}

// Record a successful export (updates timestamp)
// Lock protects last_success_ts from torn reads by PschGetStats/PschResetStats.
void PschRecordExportSuccess(void) {
  if (psch_shared_state == NULL) {
    return;
  }
  LWLockAcquire(psch_shared_state->lock, LW_EXCLUSIVE);
  psch_shared_state->last_success_ts = GetCurrentTimestamp();
  LWLockRelease(psch_shared_state->lock);
}

// Record an export failure (updates counter, timestamp, and error text)
// Lock protects last_error_ts and last_error_text from torn reads by
// PschGetStats/PschResetStats. send_failures is atomic so it's safe outside the lock.
void PschRecordExportFailure(const char* error_msg) {
  if (psch_shared_state == NULL) {
    return;
  }
  pg_atomic_fetch_add_u64(&psch_shared_state->send_failures, 1);

  LWLockAcquire(psch_shared_state->lock, LW_EXCLUSIVE);
  psch_shared_state->last_error_ts = GetCurrentTimestamp();
  if (error_msg != NULL) {
    strlcpy(psch_shared_state->last_error_text, error_msg,
            sizeof(psch_shared_state->last_error_text));
  }
  LWLockRelease(psch_shared_state->lock);
}

// Count events destructively consumed after an export failure (poison-batch
// valve, internal error, or exporter OOM).  Distinct from the enqueue-overflow
// 'dropped' counter so operators can tell capture loss from export loss.
void PschRecordExportDrop(int n) {
  if (psch_shared_state == NULL || n <= 0) {
    return;
  }
  pg_atomic_fetch_add_u64(&psch_shared_state->export_dropped, (uint64)n);
}

uint64 PschGetExportDropped(void) {
  if (psch_shared_state == NULL) {
    return 0;
  }
  return pg_atomic_read_u64(&psch_shared_state->export_dropped);
}

pid_t PschGetBgworkerPid(void) {
  if (psch_shared_state == NULL) {
    return 0;
  }
  return (pid_t)pg_atomic_read_u32(&psch_shared_state->bgworker_pid);
}

void PschSetBgworkerPid(pid_t pid) {
  if (psch_shared_state == NULL) {
    return;
  }
  pg_atomic_write_u32(&psch_shared_state->bgworker_pid, (uint32)pid);
}
