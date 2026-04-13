// pg_stat_ch shared memory ring buffer
#ifndef PG_STAT_CH_SRC_QUEUE_SHMEM_H_
#define PG_STAT_CH_SRC_QUEUE_SHMEM_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "queue/event.h"
#include "queue/psch_dsa.h"

// Calculate total shared memory size needed
Size PschShmemSize(void);

// Request shared memory allocation (called from _PG_init)
void PschShmemRequest(void);

// Initialize shared memory structures (called from shmem_startup_hook)
void PschShmemStartup(void);

// Install shmem hooks (called from _PG_init)
void PschInstallShmemHooks(void);

// Enqueue an event to the ring buffer. Returns true if the event was enqueued or
// buffered locally for deferred flush; false if dropped due to overflow.
bool PschEnqueueEvent(const PschEvent* event);

// Enqueue a batch of events under a single lock acquisition.
// Returns the number of events successfully enqueued (rest are dropped via overflow).
int PschEnqueueBatch(const PschEvent* events, int count);

// Dequeue an event from the ring buffer (returns true if event was available)
bool PschDequeueEvent(PschEvent* event);

// Get current queue statistics.
// last_error_buf must point to a caller-owned buffer of last_error_buf_size bytes;
// the error text is copied into it under the lock to prevent torn reads.
void PschGetStats(uint64* enqueued, uint64* dropped, uint64* exported, uint32* queue_size,
                  uint32* queue_capacity, uint64* send_failures, TimestampTz* last_success_ts,
                  char* last_error_buf, size_t last_error_buf_size, TimestampTz* last_error_ts,
                  uint64* dsa_oom_count);

// Reset all queue statistics to zero
void PschResetStats(void);

// Record export success (called by bgworker after successful export)
void PschRecordExportSuccess(void);

// Record export failure (called by bgworker on export error)
void PschRecordExportFailure(const char* error_msg);

// Get the background worker PID (for signaling)
int PschGetBgworkerPid(void);

// Set the background worker PID (called at bgworker startup)
void PschSetBgworkerPid(int pid);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_QUEUE_SHMEM_H_
