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

extern "C" {
#include "postgres.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
}

#include "config/guc.h"
#include "hooks/hooks.h"
#include "queue/local_batch.h"
#include "queue/shmem.h"

PschSharedState* psch_shared_state = nullptr;

// Previous hook values for chaining
static shmem_startup_hook_type prev_shmem_startup_hook = nullptr;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = nullptr;
#endif

static inline PschEvent* GetRingBuffer(void) {
  return reinterpret_cast<PschEvent*>(reinterpret_cast<char*>(psch_shared_state) +
                                      sizeof(PschSharedState));
}

// Handle queue overflow: increment dropped counter and log warning once
//
// DROP-ON-OVERFLOW STRATEGY: When the queue is full, we drop events rather than
// blocking producers. This is intentional:
// - Blocking would add latency to user queries, defeating the purpose of telemetry
// - Telemetry is "best effort" - missing some events during spikes is acceptable
// - The dropped counter lets operators detect and address capacity issues
//
// SIZING GUIDANCE: The queue should be sized to handle burst query loads plus some
// headroom for bgworker export delays. Monitor pg_stat_ch_stats().queue_size and
// .dropped to tune capacity. A good starting point is 1024-4096 events.
//
// DETECTING OVERFLOW: Check pg_stat_ch_stats().dropped > 0 or look for the WARNING
// in PostgreSQL logs. The overflow_logged flag prevents log spam by warning only once
// until stats are reset.
static void HandleOverflow() {
  pg_atomic_fetch_add_u64(&psch_shared_state->dropped, 1);

  // Log overflow warning once to avoid log spam (pg_stat_monitor pattern)
  if (pg_atomic_test_set_flag(&psch_shared_state->overflow_logged)) {
    ereport(WARNING,
            (errmsg("pg_stat_ch: queue overflow, events being dropped"),
             errhint("Consider increasing pg_stat_ch.queue_capacity or reducing query load.")));
  }
}

// Check queue fullness and enqueue event if space available.
// Called with lock held. Returns true if event was enqueued.
//
// SMART COPY: Instead of copying the full ~4.5KB PschEvent, we copy only the bytes
// that contain actual data. For a typical pgbench query (~50 bytes, no error), this
// copies ~600 bytes instead of ~4,500 bytes — 7.5x less data under the exclusive lock.
// The two large buffers (err_message: 2KB, query: 2KB) are mostly empty; we copy
// only their actual content based on the length fields.
static bool TryEnqueueLocked(const PschEvent* event, uint32 capacity) {
  uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
  uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);

  if (head - tail >= capacity || psch_debug_force_locked_overflow) {
    HandleOverflow();
    return false;
  }

  // Fast modulo via bitmask (requires power-of-2 capacity, enforced by GUC check)
  uint32 mask = capacity - 1;
  PschEvent* slot = &GetRingBuffer()[head & mask];

  // 1. Copy header: all fixed fields up to err_message (includes err_message_len)
  const size_t header_size = offsetof(PschEvent, err_message);
  memcpy(slot, event, header_size);

  // 2. Copy actual err_message content (usually 0 bytes for non-error events)
  if (event->err_message_len > 0) {
    uint16 len = Min(event->err_message_len, (uint16)(PSCH_MAX_ERR_MSG_LEN - 1));
    memcpy(slot->err_message, event->err_message, len);
    slot->err_message[len] = '\0';
  } else {
    slot->err_message[0] = '\0';
  }

  // 3. Copy mid section: application_name, client_addr, query_len
  const size_t mid_offset = offsetof(PschEvent, application_name);
  const size_t mid_size = offsetof(PschEvent, query) - mid_offset;
  memcpy(reinterpret_cast<char*>(slot) + mid_offset,
         reinterpret_cast<const char*>(event) + mid_offset, mid_size);

  // 4. Copy actual query content (typically 20-200 bytes vs 2KB buffer)
  if (event->query_len > 0) {
    uint16 len = Min(event->query_len, (uint16)(PSCH_MAX_QUERY_LEN - 1));
    memcpy(slot->query, event->query, len);
    slot->query[len] = '\0';
  } else {
    slot->query[0] = '\0';
  }

  // CRITICAL: Memory barrier ensures the event data is written to shared memory
  // before we update head. Without this, the consumer might read stale data on
  // weakly-ordered architectures (ARM, PowerPC). Pattern from shm_mq.c.
  pg_memory_barrier();
  pg_atomic_write_u64(&psch_shared_state->head, head + 1);
  pg_atomic_fetch_add_u64(&psch_shared_state->enqueued, 1);

  return true;
}

static void PschShmemShutdown([[maybe_unused]] int code, [[maybe_unused]] Datum arg) {
  if (psch_shared_state != nullptr) {
    elog(LOG, "pg_stat_ch: shutdown (enqueued=" UINT64_FORMAT ", dropped=" UINT64_FORMAT
             ", exported=" UINT64_FORMAT ")",
         pg_atomic_read_u64(&psch_shared_state->enqueued),
         pg_atomic_read_u64(&psch_shared_state->dropped),
         pg_atomic_read_u64(&psch_shared_state->exported));
  }
}

extern "C" {

Size PschShmemSize(void) {
  Size size = sizeof(PschSharedState);
  size = add_size(size, mul_size(psch_queue_capacity, sizeof(PschEvent)));
  return MAXALIGN(size);
}

static void RequestSharedResources(void) {
  RequestAddinShmemSpace(PschShmemSize());
  RequestNamedLWLockTranche("pg_stat_ch", 1);
}

#if PG_VERSION_NUM >= 150000
static void PschShmemRequestHook(void) {
  if (prev_shmem_request_hook != nullptr) {
    prev_shmem_request_hook();
  }
  RequestSharedResources();
}
#endif

// Initialize shared state fields on first-time setup.
// Called with AddinShmemInitLock held.
static void InitializeSharedState(void) {
  psch_shared_state->lock = &(GetNamedLWLockTranche("pg_stat_ch"))->lock;
  psch_shared_state->capacity = psch_queue_capacity;
  pg_atomic_init_u64(&psch_shared_state->head, 0);
  pg_atomic_init_u64(&psch_shared_state->enqueued, 0);
  pg_atomic_init_u64(&psch_shared_state->dropped, 0);
  pg_atomic_init_flag(&psch_shared_state->overflow_logged);
  pg_atomic_init_u64(&psch_shared_state->tail, 0);
  pg_atomic_init_u64(&psch_shared_state->exported, 0);

  pg_atomic_init_u64(&psch_shared_state->send_failures, 0);
  psch_shared_state->last_success_ts = 0;
  psch_shared_state->last_error_ts = 0;
  MemSet(psch_shared_state->last_error_text, 0, sizeof(psch_shared_state->last_error_text));
  pg_atomic_init_u32(&psch_shared_state->bgworker_pid, 0);

  MemSet(GetRingBuffer(), 0, psch_queue_capacity * sizeof(PschEvent));

  elog(LOG, "pg_stat_ch: initialized shared memory (capacity=%d, size=%zu)", psch_queue_capacity,
       PschShmemSize());
}

static void PschShmemStartupHook(void) {
  bool found;

  if (prev_shmem_startup_hook != nullptr) {
    prev_shmem_startup_hook();
  }

  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

  psch_shared_state =
      static_cast<PschSharedState*>(ShmemInitStruct("pg_stat_ch", PschShmemSize(), &found));

  if (psch_shared_state == nullptr) {
    LWLockRelease(AddinShmemInitLock);
    ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("pg_stat_ch: could not create shared memory segment")));
  }

  if (!found) {
    InitializeSharedState();
  }

  LWLockRelease(AddinShmemInitLock);

  on_shmem_exit(PschShmemShutdown, 0);
}

void PschShmemRequest(void) {
#if PG_VERSION_NUM < 150000
  RequestSharedResources();
#endif
}

void PschShmemStartup(void) {}

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
  if (psch_shared_state == nullptr || !psch_enabled) {
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
  if (psch_shared_state == nullptr || !psch_enabled || count == 0)
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

// Dequeue an event from the ring buffer (single-consumer, called by bgworker)
//
// LOCK-FREE DESIGN: This function runs lock-free because there's only one consumer
// (the bgworker). We use atomic reads and memory barriers instead of locks. This
// prevents producer contention from affecting the consumer's ability to drain the
// queue. Pattern from PostgreSQL's shm_mq.c.
//
// MEMORY BARRIERS: Critical for correctness on weakly-ordered CPUs:
// 1. pg_read_barrier() after reading head ensures we see the producer's writes
// 2. pg_write_barrier() before updating tail ensures our copy completes first
// Without these, we could read stale event data or signal completion too early.
//
// WRAPAROUND SAFETY: Using uint64 for head/tail means wraparound takes ~584 years
// at 1M events/sec. Even after wraparound, (head - tail) arithmetic works correctly
// because both wrap at the same point (2^64).
bool PschDequeueEvent(PschEvent* event) {
  if (psch_shared_state == nullptr) {
    return false;
  }

  // Read head with acquire semantics - we must see producer's latest writes
  uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
  // pg_read_barrier() is sufficient here (vs full pg_memory_barrier() in GetStats)
  // because this is a read-only path - we're not writing anything that needs to be
  // visible to producers before we complete the read.
  pg_read_barrier();
  uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);

  if (head == tail) {
    return false;  // Queue is empty
  }

  // Fast modulo via bitmask (requires power-of-2 capacity)
  uint32 capacity = psch_shared_state->capacity;
  uint32 mask = capacity - 1;
  PschEvent* slot = &GetRingBuffer()[tail & mask];
  memcpy(event, slot, sizeof(PschEvent));

  // CRITICAL: Write barrier ensures memcpy completes before we update tail.
  // Otherwise, a producer might reuse this slot before we finish copying,
  // corrupting the event we're reading. Pattern from shm_mq.c.
  pg_write_barrier();
  pg_atomic_write_u64(&psch_shared_state->tail, tail + 1);
  return true;
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
                  char* last_error_buf, size_t last_error_buf_size, TimestampTz* last_error_ts) {
  if (psch_shared_state == nullptr) {
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
    return;
  }

  *enqueued = pg_atomic_read_u64(&psch_shared_state->enqueued);
  *dropped = pg_atomic_read_u64(&psch_shared_state->dropped);
  *exported = pg_atomic_read_u64(&psch_shared_state->exported);
  *send_failures = pg_atomic_read_u64(&psch_shared_state->send_failures);

  // Full barrier for consistent snapshot (counters before positions)
  pg_memory_barrier();

  uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
  uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);
  *queue_size = static_cast<uint32>(head - tail);
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
  if (psch_shared_state == nullptr) {
    return;
  }

  LWLockAcquire(psch_shared_state->lock, LW_EXCLUSIVE);
  pg_atomic_write_u64(&psch_shared_state->enqueued, 0);
  pg_atomic_write_u64(&psch_shared_state->dropped, 0);
  pg_atomic_write_u64(&psch_shared_state->exported, 0);
  pg_atomic_write_u64(&psch_shared_state->send_failures, 0);
  pg_atomic_clear_flag(&psch_shared_state->overflow_logged);
  psch_shared_state->last_success_ts = 0;
  psch_shared_state->last_error_ts = 0;
  MemSet(psch_shared_state->last_error_text, 0, sizeof(psch_shared_state->last_error_text));
  LWLockRelease(psch_shared_state->lock);
}

// Record a successful export (updates timestamp)
// Lock protects last_success_ts from torn reads by PschGetStats/PschResetStats.
void PschRecordExportSuccess(void) {
  if (psch_shared_state == nullptr) {
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
  if (psch_shared_state == nullptr) {
    return;
  }
  pg_atomic_fetch_add_u64(&psch_shared_state->send_failures, 1);

  LWLockAcquire(psch_shared_state->lock, LW_EXCLUSIVE);
  psch_shared_state->last_error_ts = GetCurrentTimestamp();
  if (error_msg != nullptr) {
    strlcpy(psch_shared_state->last_error_text, error_msg,
            sizeof(psch_shared_state->last_error_text));
  }
  LWLockRelease(psch_shared_state->lock);
}

int PschGetBgworkerPid(void) {
  if (psch_shared_state == nullptr) {
    return 0;
  }
  return static_cast<int>(pg_atomic_read_u32(&psch_shared_state->bgworker_pid));
}

void PschSetBgworkerPid(int pid) {
  if (psch_shared_state == nullptr) {
    return;
  }
  pg_atomic_write_u32(&psch_shared_state->bgworker_pid, static_cast<uint32>(pid));
}

}  // extern "C"
