// pg_stat_ch shared memory ring buffer
#ifndef PG_STAT_CH_SRC_QUEUE_SHMEM_H_
#define PG_STAT_CH_SRC_QUEUE_SHMEM_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "datatype/timestamp.h"
#include "storage/lwlock.h"

#include "queue/event.h"

// Shared state structure in shared memory
//
// CACHE-LINE ALIGNMENT STRATEGY (pattern from PostgreSQL's shmem.c):
// This structure is carefully laid out to avoid "false sharing" - a performance
// problem where multiple CPU cores unnecessarily bounce cache lines back and forth
// even though they're accessing different fields. By separating producer-written
// and consumer-written fields with padding, we ensure they live on different cache
// lines (typically 64 bytes on modern CPUs).
//
// CONCURRENCY MODEL:
// - Multiple producers (backends) write events via PschEnqueueEvent() using LWLock
// - Single consumer (bgworker) reads events via PschDequeueEvent() lock-free
// - head/tail use atomic operations with memory barriers (see shm_mq.c pattern)
// - capacity must be power-of-2 for fast modulo via bitmask (capacity - 1)
//
// MEMORY LAYOUT:
// [lock, capacity, pad1] → [head, enqueued, dropped, overflow_logged, pad2] → [tail, exported] →
// [ring buffer]
//    rarely changed              producer cache line                             consumer cache
//    line
struct PschSharedState {
  // === Rarely-changed fields (initialization only) ===
  LWLock* lock;     // Protects writes to the ring buffer (multi-producer)
  uint32 capacity;  // Ring buffer size (must be power of 2 for bitmask)

  // Padding to separate initialization fields from hot producer fields
  char pad1[PG_CACHE_LINE_SIZE - (sizeof(LWLock*) + sizeof(uint32))];

  // === Producer-written atomics (hot, written by many backends) ===
  pg_atomic_uint64 head;           // Write position (incremented by producers)
  pg_atomic_uint64 enqueued;       // Total events successfully enqueued (stats)
  pg_atomic_uint64 dropped;        // Total events dropped due to full queue (stats)
  pg_atomic_flag overflow_logged;  // Set once on first overflow (prevents log spam)

  // Padding to separate producer fields from consumer fields (critical for perf)
  char pad2[PG_CACHE_LINE_SIZE - (3 * sizeof(pg_atomic_uint64) + sizeof(pg_atomic_flag))];

  // === Consumer-written atomics (hot, written only by bgworker) ===
  pg_atomic_uint64 tail;      // Read position (incremented by single consumer)
  pg_atomic_uint64 exported;  // Total events exported to ClickHouse (stats)

  // Padding to separate consumer fields from exporter stats
  char pad3[PG_CACHE_LINE_SIZE - (2 * sizeof(pg_atomic_uint64))];

  // === Exporter stats (written by bgworker, read by stats function) ===
  pg_atomic_uint32 export_error;   // Non-zero when exporter has a hard error (hooks disable)
  pg_atomic_uint64 send_failures;  // Total failed send attempts
  TimestampTz last_success_ts;     // Last successful export timestamp
  TimestampTz last_error_ts;       // Last error timestamp
  char last_error_text[256];       // Last error message (truncated)
  int bgworker_pid;                // Background worker PID for signaling

  // Ring buffer array follows immediately after this struct (flexible array member)
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
void PschGetStats(uint64* enqueued, uint64* dropped, uint64* exported, uint32* queue_size,
                  uint32* queue_capacity, uint64* send_failures, TimestampTz* last_success_ts,
                  const char** last_error_text, TimestampTz* last_error_ts);

// Reset all queue statistics to zero
void PschResetStats(void);

// Record export success (called by bgworker after successful export)
void PschRecordExportSuccess(void);

// Record export failure (called by bgworker on export error)
void PschRecordExportFailure(const char* error_msg);

// Set/get export error flag (disables hooks when exporter can't connect)
void PschSetExportError(bool error);
bool PschGetExportError(void);

// Get the background worker PID (for signaling)
int PschGetBgworkerPid(void);

// Set the background worker PID (called at bgworker startup)
void PschSetBgworkerPid(int pid);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_QUEUE_SHMEM_H_
