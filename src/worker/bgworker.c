// pg_stat_ch background worker implementation
//
// Signal handling notes:
//
// PostgreSQL uses SIGUSR1 for inter-process signaling via the "procsignal"
// mechanism (storage/procsignal.h). Operations like DROP DATABASE use
// ProcSignalBarrier to coordinate across all backends - they send SIGUSR1
// and wait for each backend to acknowledge by calling ProcessProcSignalBarrier().
//
// We MUST use procsignal_sigusr1_handler for SIGUSR1. A previous bug used a
// custom handler which prevented barrier acknowledgment, causing DROP DATABASE
// to hang indefinitely. The fix:
//   1. Use procsignal_sigusr1_handler for SIGUSR1 (handles barriers)
//   2. Use SIGUSR2 for extension-specific immediate flush requests
//   3. Add socket timeouts to backend connections as a safety net
//
// Signal assignments:
//   SIGHUP  -> SignalHandlerForConfigReload (reload postgresql.conf)
//   SIGTERM -> die() (graceful shutdown)
//   SIGUSR1 -> procsignal_sigusr1_handler (PostgreSQL internal - barriers, etc.)
//   SIGUSR2 -> HandleFlushSignal (extension-specific - immediate flush)
//   SIGPIPE -> SIG_IGN (ignore broken pipe from network)
//   SIGABRT -> HandleAbortSignal (bgworker-only restart instead of cluster crash)

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

#include "queue/psch_dsa.h"
#include "queue/shmem.h"

#include <signal.h>
#include <unistd.h>

#include "config/guc.h"
#include "export/stats_exporter.h"
#include "worker/bgworker.h"

// Upper bound on staged chunks drained per export cycle.  One PschExportBatch
// call processes at most one staging chunk and returns 0 on failure or empty
// queue, so under sustained load the loop would otherwise run until the ring
// drains; 16 chunks bounds the time between WaitLatch returns so procsignal
// barriers (DROP DATABASE), SIGTERM, and config reloads stay responsive.
static const int kMaxChunksPerCycle = 16;

// Custom wait event for pg_stat_activity visibility
static uint32 psch_wait_event_main = 0;

// SIGABRT backstop.  The C++ terminate handler that used to convert stray
// aborts into ereport(FATAL) is gone with the C++ runtime, but residual
// abort() sources remain (libc heap-corruption checks, assert() inside
// linked C libraries).  Exit-status semantics make this handler load-bearing:
//
//   * exit status 0 or 1  -> postmaster restarts ONLY this bgworker after
//     bgw_restart_time (10 s; see PschRegisterBgworker below)
//   * death by signal, or any other exit status -> postmaster assumes shared
//     memory may be corrupt and crash-recovers the WHOLE cluster (every
//     connection severed, WAL replayed) — the 2026-06-10 production incident.
//
// _exit(1) therefore converts an abort into a clean bgworker-only restart.
// Only async-signal-safe work is allowed here: never ereport/elog (palloc
// into ErrorContext from a signal handler corrupts memory), never proc_exit
// (runs arbitrary callbacks).  The handler must not return either: abort()
// re-raises SIGABRT with SIG_DFL if the handler returns, restoring the
// cluster-crash path.  _exit skips the on_proc_exit chain, leaving a stale
// bgworker_pid in shmem — PschSignalFlush's ESRCH compare-exchange recovery
// (below) cleans that up on the next flush attempt.  SIGSEGV/SIGBUS are
// deliberately left untouched: for those, shared memory really may be
// corrupt, so full crash-recovery semantics are correct.
static void HandleAbortSignal(SIGNAL_ARGS) {
  static const char msg[] =
      "pg_stat_ch: bgworker caught SIGABRT; exiting with status 1 for bgworker-only restart\n";
  ssize_t rc;

  (void)postgres_signal_arg;
  rc = write(STDERR_FILENO, msg, sizeof(msg) - 1);
  (void)rc;
  _exit(1);
}

// SIGUSR2 handler: wake the worker for immediate flush.
// Note: SIGUSR1 is reserved for PostgreSQL's procsignal mechanism.
static void HandleFlushSignal(SIGNAL_ARGS) {
  (void)postgres_signal_arg;
  int save_errno = errno;
  SetLatch(MyLatch);
  errno = save_errno;
}

// Set up signal handlers before unblocking signals.
// Pattern from worker_spi.c:158-163 and autovacuum.c.
//
// CRITICAL: We MUST use procsignal_sigusr1_handler for SIGUSR1.
// This handler processes PostgreSQL's internal signals including:
//   - PROCSIG_BARRIER: Global barrier for DROP DATABASE, DROP TABLESPACE, etc.
//   - PROCSIG_CATCHUP_INTERRUPT: Shared invalidation catchup
//   - PROCSIG_NOTIFY_INTERRUPT: LISTEN/NOTIFY
//   - PROCSIG_LOG_MEMORY_CONTEXT: Memory context logging
//   - PROCSIG_RECOVERY_CONFLICT_*: Standby recovery conflicts
//
// If we don't use this handler, operations that require barrier acknowledgment
// (like DROP DATABASE) will hang indefinitely waiting for this worker.
static void SetupSignalHandlers(void) {
  pqsignal(SIGHUP, SignalHandlerForConfigReload);
  pqsignal(SIGTERM, die);
  pqsignal(SIGUSR1, procsignal_sigusr1_handler);  // REQUIRED for barriers
  pqsignal(SIGUSR2, HandleFlushSignal);           // Extension-specific flush
  pqsignal(SIGPIPE, SIG_IGN);
}

// Handle SIGHUP config reload
static void HandleConfigReload(void) {
  if (ConfigReloadPending != 0) {
    ConfigReloadPending = 0;
    ProcessConfigFile(PGC_SIGHUP);
    elog(DEBUG1, "pg_stat_ch: configuration reloaded");
  }
}

// Callback for bgworker process exit (registered via on_proc_exit).
// Clear bgworker_pid before exporter teardown so a concurrent
// pg_stat_ch_flush() cannot race a SIGUSR2 to a recycled PID.
static void PschBgworkerShutdown(int code pg_attribute_unused(), Datum arg pg_attribute_unused()) {
  PschSetBgworkerPid(0);
  PschExporterShutdown();
}

// Process pending signals: barriers, interrupts (SIGTERM/SIGINT), config reload.
// Called after WaitLatch wakes and between batches in the drain loop.
static void ProcessPendingSignals(void) {
  // Barriers first: ProcSignalBarrierPending is set when operations like
  // DROP DATABASE need all backends to acknowledge. Failing to process
  // causes those operations to hang.
  if (ProcSignalBarrierPending) {
    ProcessProcSignalBarrier();
  }
  CHECK_FOR_INTERRUPTS();
  HandleConfigReload();
}

// Drain the queue, at most kMaxChunksPerCycle staged chunks per cycle.  Each
// chunk gets its own PG_TRY/PG_CATCH so an error on chunk N+1 doesn't lose
// chunks 1..N.  PschExportBatch returns 0 on failure or empty queue (the
// chunk stays in the ring on requeueable failures), so 0 always breaks the
// loop and lets CalculateSleepMs apply consecutive-failure backoff instead
// of spinning.  Signals are processed between chunks to stay responsive to
// SIGTERM, barriers, and config reload.
static void ExportBatchWithRecovery(void) {
  int chunk;

  pgstat_report_activity(STATE_RUNNING, "exporting telemetry events");

  for (chunk = 0; chunk < kMaxChunksPerCycle; chunk++) {
    // volatile: required because PG_TRY/PG_CATCH uses setjmp/longjmp.
    // Without it, the compiler may keep 'exported' in a register that
    // gets clobbered on longjmp, making the value undefined in PG_CATCH.
    volatile int exported = 0;

    PG_TRY();
    { exported = PschExportBatch(); }
    PG_CATCH();
    {
      EmitErrorReport();
      FlushErrorState();
      elog(WARNING, "pg_stat_ch: export error, will retry");
    }
    PG_END_TRY();

    if (exported <= 0) {
      break;
    }

    ProcessPendingSignals();
    if (!psch_enabled) {
      break;
    }
  }

  pgstat_report_activity(STATE_IDLE, NULL);
}

// Initialize wait event for pg_stat_activity visibility
static uint32 InitializeWaitEvent(void) {
#if PG_VERSION_NUM >= 170000
  return WaitEventExtensionNew("PgStatChExporter");
#else
  return PG_WAIT_EXTENSION;
#endif
}

// Calculate sleep time with exponential backoff on failures
static int CalculateSleepMs(void) {
  int sleep_ms = psch_flush_interval_ms;
  int failures = PschGetConsecutiveFailures();
  if (failures > 0) {
    int backoff_ms = PschGetRetryDelayMs();
    sleep_ms = (backoff_ms > sleep_ms) ? backoff_ms : sleep_ms;
    elog(DEBUG1, "pg_stat_ch: %d consecutive failures, sleeping %d ms", failures, sleep_ms);
  }
  return sleep_ms;
}

// Run one export cycle: sleep, process signals, then drain queue if enabled.
//
// Note on blocking: If we're blocked in backend network I/O when a barrier
// signal arrives, we can't process it until the I/O completes. The socket
// deadlines configured by the export backends (pg_stat_ch.export_timeout)
// bound this delay.
static void RunExportCycle(uint32 wait_event) {
  int sleep_ms = CalculateSleepMs();
  (void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, sleep_ms, wait_event);
  ResetLatch(MyLatch);

  ProcessPendingSignals();

  if (psch_enabled) {
    ExportBatchWithRecovery();
  }
}

void PschBgworkerMain(Datum main_arg pg_attribute_unused()) {
  // Install the SIGABRT backstop before anything else: bgworkers start with
  // all signals blocked, so installing it before
  // BackgroundWorkerUnblockSignals leaves no window where an abort would
  // crash-recover the whole cluster.
  pqsignal(SIGABRT, HandleAbortSignal);

  SetupSignalHandlers();
  BackgroundWorkerUnblockSignals();
  BackgroundWorkerInitializeConnection("postgres", NULL, 0);

  // Register cleanup callback for graceful shutdown
  on_proc_exit(PschBgworkerShutdown, 0);

  // Store our PID for signaling (used by pg_stat_ch_flush())
  PschSetBgworkerPid(MyProcPid);

  // Attach to DSA area eagerly so the first dequeue doesn't hit lazy init
  PschDsaAttach();

  // The SIGABRT handler's _exit(1) skips shmem-corruption reinit; verify the
  // ring invariants before trusting head/tail from a previous incarnation.
  PschRingSanityCheck();

  elog(LOG, "pg_stat_ch: background worker started (pid=%d)", MyProcPid);

  // Register custom wait event for pg_stat_activity visibility
  if (psch_wait_event_main == 0) {
    psch_wait_event_main = InitializeWaitEvent();
  }

  // Initialize the export backend and verify connectivity
  pgstat_report_activity(STATE_RUNNING, "initializing telemetry exporter");
  if (PschExporterInit()) {
    elog(LOG, "pg_stat_ch: exporter connectivity verified on startup");
  } else {
    elog(WARNING,
         "pg_stat_ch: failed to connect to telemetry backend on startup, "
         "will retry on first export");
  }
  pgstat_report_activity(STATE_IDLE, NULL);

  // Main loop (pattern from worker_spi.c:206-290)
  for (;;) {
    RunExportCycle(psch_wait_event_main);
  }
}

// Signal the background worker to flush immediately.
// Called from pg_stat_ch_flush() SQL function.
// Uses SIGUSR2 since SIGUSR1 is reserved for PostgreSQL's procsignal mechanism.
void PschSignalFlush(void) {
  pid_t bgworker_pid = PschGetBgworkerPid();
  if (bgworker_pid == 0) {
    ereport(WARNING, (errmsg("pg_stat_ch: background worker not running")));
  } else if (kill(bgworker_pid, SIGUSR2) != 0) {
    // Stale pid: worker died without running on_proc_exit (kill -9, OOM
    // killer, or the SIGABRT handler's _exit(1)). Clear shmem so we don't
    // keep signaling a recycled pid; postmaster restart will repopulate.
    // Only clear when shmem still holds the stale pid we observed.
    if (errno == ESRCH) {
      uint32 expected = (uint32)bgworker_pid;
      pg_atomic_compare_exchange_u32(&psch_shared_state->bgworker_pid, &expected, 0);
    }
    ereport(WARNING, (errmsg("pg_stat_ch: failed to signal background worker: %m")));
  }
}

void PschRegisterBgworker(void) {
  BackgroundWorker worker;

  MemSet(&worker, 0, sizeof(worker));
  strlcpy(worker.bgw_name, "pg_stat_ch exporter", BGW_MAXLEN);
  strlcpy(worker.bgw_type, "pg_stat_ch exporter", BGW_MAXLEN);
  worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
  worker.bgw_start_time = BgWorkerStart_ConsistentState;
  worker.bgw_restart_time = 10;  // Restart after 10 seconds on crash
  strlcpy(worker.bgw_library_name, "pg_stat_ch", BGW_MAXLEN);
  strlcpy(worker.bgw_function_name, "PschBgworkerMain", BGW_MAXLEN);
  worker.bgw_main_arg = (Datum)0;
  worker.bgw_notify_pid = 0;

  RegisterBackgroundWorker(&worker);
}
