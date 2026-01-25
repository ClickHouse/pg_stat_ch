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
//    PG_TRY/PG_CATCH blocks ensure locks are released even if an error occurs
//    during enqueue or stats reset, preventing deadlocks.
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
}

#include "queue/shmem.h"
#include "config/guc.h"

// Shared memory state
PschSharedState* psch_shared_state = nullptr;

// Previous hook values for chaining
static shmem_startup_hook_type prev_shmem_startup_hook = nullptr;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = nullptr;
#endif

// Get pointer to the ring buffer array (immediately follows the shared state)
static inline PschEvent* GetRingBuffer(void) {
  return reinterpret_cast<PschEvent*>(
      reinterpret_cast<char*>(psch_shared_state) + sizeof(PschSharedState));
}

// Shutdown callback to log final stats
static void PschShmemShutdown(int code, Datum arg) {
  (void)code;  // Unused parameter
  (void)arg;   // Unused parameter
  
  if (psch_shared_state != nullptr) {
    elog(LOG,
         "pg_stat_ch: shutdown (enqueued=%lu, dropped=%lu, exported=%lu)",
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

static void PschShmemStartupHook(void) {
  bool found;

  if (prev_shmem_startup_hook != nullptr) {
    prev_shmem_startup_hook();
  }

  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

  psch_shared_state = static_cast<PschSharedState*>(
      ShmemInitStruct("pg_stat_ch", PschShmemSize(), &found));

  if (psch_shared_state == nullptr) {
    LWLockRelease(AddinShmemInitLock);
    ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_MEMORY),
             errmsg("pg_stat_ch: could not create shared memory segment")));
  }

  if (!found) {
    // First time initialization
    psch_shared_state->lock = &(GetNamedLWLockTranche("pg_stat_ch"))->lock;
    psch_shared_state->capacity = psch_queue_capacity;
    pg_atomic_init_u64(&psch_shared_state->head, 0);
    pg_atomic_init_u64(&psch_shared_state->enqueued, 0);
    pg_atomic_init_u64(&psch_shared_state->dropped, 0);
    pg_atomic_init_flag(&psch_shared_state->overflow_logged);
    pg_atomic_init_u64(&psch_shared_state->tail, 0);
    pg_atomic_init_u64(&psch_shared_state->exported, 0);

    // Zero-initialize the ring buffer
    MemSet(GetRingBuffer(), 0, psch_queue_capacity * sizeof(PschEvent));

    elog(LOG,
         "pg_stat_ch: initialized shared memory (capacity=%d, size=%zu)",
         psch_queue_capacity, PschShmemSize());
  }

  LWLockRelease(AddinShmemInitLock);

  // Register shutdown callback
  on_shmem_exit(PschShmemShutdown, 0);
}

void PschShmemRequest(void) {
#if PG_VERSION_NUM < 150000
  // For PG < 15, request resources directly
  RequestSharedResources();
#endif
}

void PschShmemStartup(void) {
  // This is called by PschShmemStartupHook
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
// CONCURRENCY: Multiple backends can call this simultaneously. We use an exclusive
// LWLock to serialize producers. This is acceptable because enqueue is fast (~100ns)
// and happens at query end, not during query execution.
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

  bool result = false;

  LWLockAcquire(psch_shared_state->lock, LW_EXCLUSIVE);
  PG_TRY();
  {
    uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
    uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);
    uint32 capacity = psch_shared_state->capacity;

    // Check if queue is full (head - tail handles 64-bit wraparound correctly)
    if (head - tail >= capacity) {
      pg_atomic_fetch_add_u64(&psch_shared_state->dropped, 1);
      
      // Log overflow warning once to avoid log spam (pg_stat_monitor pattern)
      if (!pg_atomic_test_set_flag(&psch_shared_state->overflow_logged)) {
        ereport(WARNING,
                (errmsg("pg_stat_ch: queue overflow, events being dropped"),
                 errhint("Consider increasing pg_stat_ch.queue_capacity or reducing query load.")));
      }
      
      result = false;
    } else {
      // Fast modulo via bitmask (requires power-of-2 capacity, enforced by GUC check)
      uint32 mask = capacity - 1;
      PschEvent* slot = &GetRingBuffer()[head & mask];
      memcpy(slot, event, sizeof(PschEvent));

      // CRITICAL: Memory barrier ensures the event data is written to shared memory
      // before we update head. Without this, the consumer might read stale data on
      // weakly-ordered architectures (ARM, PowerPC). Pattern from shm_mq.c.
      pg_memory_barrier();
      pg_atomic_write_u64(&psch_shared_state->head, head + 1);
      pg_atomic_fetch_add_u64(&psch_shared_state->enqueued, 1);

      result = true;
    }
  }
  PG_CATCH();
  {
    // Release lock before re-throwing error (prevents deadlock)
    LWLockRelease(psch_shared_state->lock);
    PG_RE_THROW();
  }
  PG_END_TRY();

  LWLockRelease(psch_shared_state->lock);
  return result;
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
  pg_read_barrier();  // Ensure head read completes before tail read and data access
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
void PschGetStats(uint64* enqueued, uint64* dropped, uint64* exported,
                  uint32* queue_size, uint32* queue_capacity) {
  if (psch_shared_state == nullptr) {
    *enqueued = 0;
    *dropped = 0;
    *exported = 0;
    *queue_size = 0;
    *queue_capacity = 0;
    return;
  }

  // Read cumulative counters first
  *enqueued = pg_atomic_read_u64(&psch_shared_state->enqueued);
  *dropped = pg_atomic_read_u64(&psch_shared_state->dropped);
  *exported = pg_atomic_read_u64(&psch_shared_state->exported);

  // Full barrier for consistent snapshot (counters before positions)
  pg_memory_barrier();

  // Read current positions to calculate queue occupancy
  uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
  uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);
  *queue_size = static_cast<uint32>(head - tail);
  *queue_capacity = psch_shared_state->capacity;
}

// Reset statistics counters (called by SQL function pg_stat_ch_reset())
//
// LOCKING: We use the same lock as enqueue to prevent race conditions. Without this,
// a concurrent enqueue could increment a counter after we zero it but before we return,
// losing that count permanently.
//
// ERROR HANDLING: PG_TRY/PG_CATCH ensures lock release on error. This is critical
// because stats reset might be called interactively by a DBA, and we don't want a
// keyboard interrupt (Ctrl-C) to leave the lock held.
//
// OVERFLOW FLAG RESET: Clearing overflow_logged allows the warning to be logged
// again on the next overflow, which is useful after capacity has been increased.
void PschResetStats(void) {
  if (psch_shared_state == nullptr) {
    return;
  }

  LWLockAcquire(psch_shared_state->lock, LW_EXCLUSIVE);
  PG_TRY();
  {
    pg_atomic_write_u64(&psch_shared_state->enqueued, 0);
    pg_atomic_write_u64(&psch_shared_state->dropped, 0);
    pg_atomic_write_u64(&psch_shared_state->exported, 0);
    pg_atomic_clear_flag(&psch_shared_state->overflow_logged);  // Allow warning again
  }
  PG_CATCH();
  {
    LWLockRelease(psch_shared_state->lock);
    PG_RE_THROW();
  }
  PG_END_TRY();
  LWLockRelease(psch_shared_state->lock);
}

}  // extern "C"
