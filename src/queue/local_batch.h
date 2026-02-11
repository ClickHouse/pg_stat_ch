// pg_stat_ch local batch buffer for try-lock contention avoidance
//
// When the ring buffer's LWLock is contended, events are stashed in a
// process-local array instead of blocking. The buffer is flushed:
// 1. On transaction COMMIT/ABORT/PREPARE (via XactCallback)
// 2. When the local buffer fills up (capacity = 8)
// 3. On backend shutdown (via on_shmem_exit)
//
// This reduces lock acquisitions from ~N per transaction to ~1 per transaction
// under contention, while adding zero overhead when the lock is uncontended.
#ifndef PG_STAT_CH_SRC_QUEUE_LOCAL_BATCH_H_
#define PG_STAT_CH_SRC_QUEUE_LOCAL_BATCH_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "queue/event.h"

// Buffer an event locally (flushes automatically if buffer is full).
// Lazily registers XactCallback and on_shmem_exit on first call.
void PschLocalBatchAdd(const PschEvent* event);

// Flush all buffered events to the shared ring buffer via PschEnqueueBatch.
// Returns the number of events successfully enqueued.
int PschLocalBatchFlush(void);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_QUEUE_LOCAL_BATCH_H_
