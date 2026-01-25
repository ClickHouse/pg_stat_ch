// pg_stat_ch shared memory ring buffer implementation

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

  if (!found) {
    // First time initialization
    psch_shared_state->lock = &(GetNamedLWLockTranche("pg_stat_ch"))->lock;
    pg_atomic_init_u64(&psch_shared_state->head, 0);
    pg_atomic_init_u64(&psch_shared_state->tail, 0);
    pg_atomic_init_u64(&psch_shared_state->enqueued, 0);
    pg_atomic_init_u64(&psch_shared_state->dropped, 0);
    pg_atomic_init_u64(&psch_shared_state->exported, 0);
    psch_shared_state->capacity = psch_queue_capacity;

    // Zero-initialize the ring buffer
    MemSet(GetRingBuffer(), 0, psch_queue_capacity * sizeof(PschEvent));

    elog(LOG,
         "pg_stat_ch: initialized shared memory (capacity=%d, size=%zu)",
         psch_queue_capacity, PschShmemSize());
  }

  LWLockRelease(AddinShmemInitLock);
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

bool PschEnqueueEvent(const PschEvent* event) {
  if (psch_shared_state == nullptr || !psch_enabled) {
    return false;
  }

  LWLockAcquire(psch_shared_state->lock, LW_EXCLUSIVE);

  uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
  uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);
  uint32 capacity = psch_shared_state->capacity;

  // Check if queue is full
  if (head - tail >= capacity) {
    pg_atomic_fetch_add_u64(&psch_shared_state->dropped, 1);
    LWLockRelease(psch_shared_state->lock);
    return false;
  }

  // Copy event to ring buffer
  PschEvent* slot = &GetRingBuffer()[head % capacity];
  memcpy(slot, event, sizeof(PschEvent));

  pg_atomic_write_u64(&psch_shared_state->head, head + 1);
  pg_atomic_fetch_add_u64(&psch_shared_state->enqueued, 1);

  LWLockRelease(psch_shared_state->lock);
  return true;
}

bool PschDequeueEvent(PschEvent* event) {
  if (psch_shared_state == nullptr) {
    return false;
  }

  // Single consumer, no lock needed for reading
  uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
  uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);

  if (head == tail) {
    return false;  // Queue is empty
  }

  uint32 capacity = psch_shared_state->capacity;
  PschEvent* slot = &GetRingBuffer()[tail % capacity];
  memcpy(event, slot, sizeof(PschEvent));

  pg_atomic_write_u64(&psch_shared_state->tail, tail + 1);
  return true;
}

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

  *enqueued = pg_atomic_read_u64(&psch_shared_state->enqueued);
  *dropped = pg_atomic_read_u64(&psch_shared_state->dropped);
  *exported = pg_atomic_read_u64(&psch_shared_state->exported);

  uint64 head = pg_atomic_read_u64(&psch_shared_state->head);
  uint64 tail = pg_atomic_read_u64(&psch_shared_state->tail);
  *queue_size = static_cast<uint32>(head - tail);
  *queue_capacity = psch_shared_state->capacity;
}

void PschResetStats(void) {
  if (psch_shared_state == nullptr) {
    return;
  }

  LWLockAcquire(psch_shared_state->lock, LW_EXCLUSIVE);
  pg_atomic_write_u64(&psch_shared_state->enqueued, 0);
  pg_atomic_write_u64(&psch_shared_state->dropped, 0);
  pg_atomic_write_u64(&psch_shared_state->exported, 0);
  LWLockRelease(psch_shared_state->lock);
}

}  // extern "C"
