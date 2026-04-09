// pg_stat_ch local batch buffer implementation
//
// Process-local static storage (BSS segment) — no shared memory changes needed.
// At 8 events × ~4.6KB = ~37KB per backend, negligible even at 100 connections.
//
// CALLBACK ORDERING (LIFO):
// on_shmem_exit callbacks run in reverse registration order.
// PschShmemShutdown (logs final stats) is registered in PschShmemStartupHook (early).
// PschLocalBatchShutdown is registered later (on first Add), so it runs FIRST —
// flushing buffered events before stats are logged.

#include "postgres.h"

#include "access/xact.h"
#include "storage/ipc.h"

#include "queue/local_batch.h"
#include "queue/shmem.h"

#define PSCH_LOCAL_BATCH_CAPACITY 8

static PschEvent local_batch[PSCH_LOCAL_BATCH_CAPACITY];
static int local_batch_count = 0;
static bool local_batch_initialized = false;

// Forward declarations
static void PschXactCallback(XactEvent event, void* arg);
static void PschLocalBatchShutdown(int code, Datum arg);

// Register callbacks lazily on first use.
static void PschLocalBatchInit(void) {
  RegisterXactCallback(PschXactCallback, NULL);
  on_shmem_exit(PschLocalBatchShutdown, 0);
  local_batch_initialized = true;
}

int PschLocalBatchFlush(void) {
  if (local_batch_count == 0) {
    return 0;
  }

  int count = local_batch_count;
  local_batch_count = 0;
  return PschEnqueueBatch(local_batch, count);
}

void PschLocalBatchAdd(const PschEvent* event) {
  if (!local_batch_initialized) {
    PschLocalBatchInit();
  }

  // If buffer is full, flush first
  if (local_batch_count >= PSCH_LOCAL_BATCH_CAPACITY) {
    PschLocalBatchFlush();
  }

  memcpy(&local_batch[local_batch_count], event, sizeof(PschEvent));
  local_batch_count++;
}

// Flush on transaction end (COMMIT, ABORT, PREPARE)
static void PschXactCallback(XactEvent event, void* arg) {
  (void)arg;
  if (local_batch_count == 0) {
    return;
  }

  switch (event) {
    case XACT_EVENT_COMMIT:
    case XACT_EVENT_PARALLEL_COMMIT:
    case XACT_EVENT_ABORT:
    case XACT_EVENT_PARALLEL_ABORT:
    case XACT_EVENT_PREPARE:
      PschLocalBatchFlush();
      break;
    default:
      break;
  }
}

// Flush on backend shutdown to avoid losing buffered events
static void PschLocalBatchShutdown(int code, Datum arg) {
  (void)code;
  (void)arg;
  PschLocalBatchFlush();
}
