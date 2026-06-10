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

// Install shmem hooks (called from _PG_init)
void PschInstallShmemHooks(void);

// Enqueue an event to the ring buffer. Returns true if the event was enqueued or
// buffered locally for deferred flush; false if dropped due to overflow.
bool PschEnqueueEvent(const PschEvent* event);

// Enqueue a batch of events under a single lock acquisition.
// Returns the number of events successfully enqueued (rest are dropped via overflow).
int PschEnqueueBatch(const PschEvent* events, int count);

// Two-phase consume (single consumer: the bgworker).  See OTEL_REWRITE_DESIGN.md
// §5a — peek/consume replaces the destructive dequeue so a failed export can
// leave events queued for retry.
//
// PschPeekEvents copies up to `max` events from the tail into `buf`, resolving
// DSA-backed strings into the copies' inline buffers WITHOUT freeing them and
// WITHOUT advancing the tail.  Read-only with respect to the ring; safe to
// call repeatedly (retry path) before a consume.  Returns the number copied.
int PschPeekEvents(PschEvent* buf, int max);

// PschConsumeEvents frees the DSA strings of the first `n` queued events and
// advances the tail past them.  Must only follow a PschPeekEvents that
// returned >= n.
void PschConsumeEvents(int n);

// Validate ring invariants (capacity power-of-2 and matching the resolved
// budget; head-tail distance within capacity).  On corruption: LOG and reset
// the indices, discarding queued events.  Called once at bgworker start to
// compensate for the SIGABRT handler's _exit(1) skipping shmem reinit.
void PschRingSanityCheck(void);

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

// Count n events destructively consumed after an export failure (poison-batch
// valve or internal/OOM failure).  Distinct from the enqueue-overflow
// `dropped` counter.
void PschRecordExportDrop(int n);

// Read the export-drop counter (consumed by pg_stat_ch_memory()/stats views).
uint64 PschGetExportDropped(void);

// Get the background worker PID (for signaling)
pid_t PschGetBgworkerPid(void);

// Set the background worker PID (called at bgworker startup)
void PschSetBgworkerPid(pid_t pid);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_QUEUE_SHMEM_H_
