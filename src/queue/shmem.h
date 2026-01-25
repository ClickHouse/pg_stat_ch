// pg_stat_ch shared memory ring buffer
#ifndef PG_STAT_CH_SRC_QUEUE_SHMEM_H_
#define PG_STAT_CH_SRC_QUEUE_SHMEM_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "storage/lwlock.h"

#include "queue/event.h"

// Shared state structure in shared memory
struct PschSharedState {
  LWLock* lock;              // Protects writes to the ring buffer
  pg_atomic_uint64 head;     // Write position (producer)
  pg_atomic_uint64 tail;     // Read position (consumer)
  pg_atomic_uint64 enqueued; // Total events enqueued
  pg_atomic_uint64 dropped;  // Events dropped due to full queue
  pg_atomic_uint64 exported; // Events successfully exported to ClickHouse
  uint32 capacity;           // Maximum number of events in queue
  // Ring buffer follows immediately after this struct
};

// Global pointer to shared state (set in shmem startup)
extern PschSharedState* psch_shared_state;

// Calculate total shared memory size needed
Size PschShmemSize(void);

// Request shared memory allocation (called from _PG_init)
void PschShmemRequest(void);

// Initialize shared memory structures (called from shmem_startup_hook)
void PschShmemStartup(void);

// Install shmem hooks (called from _PG_init)
void PschInstallShmemHooks(void);

// Enqueue an event to the ring buffer (returns true if successful, false if dropped)
bool PschEnqueueEvent(const PschEvent* event);

// Dequeue an event from the ring buffer (returns true if event was available)
bool PschDequeueEvent(PschEvent* event);

// Get current queue statistics
void PschGetStats(uint64* enqueued, uint64* dropped, uint64* exported,
                  uint32* queue_size, uint32* queue_capacity);

// Reset all queue statistics to zero
void PschResetStats(void);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_QUEUE_SHMEM_H_
