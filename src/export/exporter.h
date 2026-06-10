// pg_stat_ch C exporter interface — replaces the C++ exporter_interface.h.
//
// Contract highlights (see OTEL_REWRITE_DESIGN.md §3, §5a):
//  * Backends consume PschEvent arrays directly; there is no column-handle
//    abstraction and no per-row heap allocation.
//  * All buffers a backend needs are preallocated in its create function;
//    steady-state export performs zero heap allocation.  Allocation failure
//    at create time returns NULL (caller decides FATAL); after create, OOM
//    is impossible by construction on the hot path.
//  * No function in this interface may ereport(ERROR) or longjmp.  WARNING/
//    LOG/DEBUG elevels only.  Backends must tolerate a longjmp having
//    interrupted the previous call (reset batch state on entry; force
//    reconnect if a wire protocol exchange was in flight).
//  * Every failed connect/export_events/send_arrow MUST increment the
//    backend's consecutive-failure counter (drives bgworker backoff); every
//    success resets it.  This is what prevents the 2/sec retry flood.
#ifndef PG_STAT_CH_EXPORTER_H
#define PG_STAT_CH_EXPORTER_H

#include "queue/event.h"

typedef enum PschExportStatus {
  PSCH_EXPORT_OK = 0,
  PSCH_EXPORT_ERR_NOMEM,     // checked allocation failed (should be init-only; poison-drop if hot)
  PSCH_EXPORT_ERR_CONN,      // could not (re)connect — events must NOT be consumed
  PSCH_EXPORT_ERR_SEND,      // wire send/response failure — events must NOT be consumed
  PSCH_EXPORT_ERR_INTERNAL,  // encode/build failure — poison batch, consume + count as export-drop
} PschExportStatus;

typedef struct PschExporter PschExporter;

typedef struct PschExporterOps {
  // (Re)establish the connection.  Bounded by psch_export_timeout_ms.
  // On failure: write a NUL-terminated reason into errbuf, return false.
  bool (*connect)(PschExporter* self, char* errbuf, size_t errlen);
  bool (*is_connected)(const PschExporter* self);

  // Export a batch of events (per-record path).  Sets *exported_out to the
  // number of events confirmed sent (0 on failure).  Must be all-or-nothing
  // per call from the caller's consume perspective: the driver consumes the
  // staged chunk iff PSCH_EXPORT_OK (or poison-drops on INTERNAL/NOMEM).
  PschExportStatus (*export_events)(PschExporter* self, const PschEvent* events, int nevents,
                                    int* exported_out);

  // Send a finished Arrow IPC payload (arrow passthrough path).
  // NULL when the backend does not support it (ClickHouse backend).
  PschExportStatus (*send_arrow)(PschExporter* self, const uint8* ipc_data, size_t ipc_len,
                                 int num_rows);

  int (*consecutive_failures)(const PschExporter* self);
  void (*reset_failures)(PschExporter* self);

  // Total bytes this backend preallocated (observability; pg_stat_ch_memory).
  uint64 (*mem_used)(const PschExporter* self);

  // Idempotent; must tolerate half-initialized state (on_proc_exit may run
  // it during FATAL-from-init).  Must not ereport(ERROR).
  void (*destroy)(PschExporter* self);
} PschExporterOps;

// Backends embed this as their first member and downcast.
struct PschExporter {
  const PschExporterOps* ops;
};

// Backend factories.  Preallocate everything per the arena plan; on failure
// write a reason into errbuf and return NULL (caller ereport(FATAL)s — a
// worker that cannot preallocate is useless and a clean restart is correct).
extern PschExporter* PschClickHouseExporterCreate(char* errbuf, size_t errlen);
extern PschExporter* PschOtelExporterCreate(char* errbuf, size_t errlen);

// ---------------------------------------------------------------------------
// Export arena split.  The GUC layer resolves a single export_arena_bytes
// budget (src/config/memory_budget.h); this is the one place its internal
// split is defined so the driver (staging) and the OTel backend (encode/net/
// arrow scratch) cannot disagree.  Both export paths are preallocated
// simultaneously because otel_arrow_passthrough is SIGHUP-switchable.
// ---------------------------------------------------------------------------
typedef struct PschExportArenaPlan {
  int staging_events;          // PschEvent staging chunk (driver-owned)
  size_t staging_bytes;        // == staging_events * sizeof(PschEvent)
  size_t encode_buf_bytes;     // OTLP request encode buffer, UNCOMPRESSED payload cap
  size_t net_buf_bytes;        // HTTP response / scratch buffer
  size_t arrow_scratch_bytes;  // nanoarrow build buffers + IPC emit + ZSTD workspace
} PschExportArenaPlan;

// Encode ceiling: otelcol max_request_body_size defaults to 20 MiB and is
// enforced on the DECOMPRESSED body — the cap must bound uncompressed bytes.
#define PSCH_EXPORT_ENCODE_CEILING ((size_t)16 * 1024 * 1024)

static inline void PschExportArenaSplit(uint64 arena_bytes, PschExportArenaPlan* plan) {
  uint64 staging = arena_bytes * 45 / 100;
  uint64 arrow = arena_bytes * 30 / 100;
  uint64 encode = arena_bytes * 20 / 100;
  uint64 net = arena_bytes - staging - arrow - encode;

  if (staging < 256 * sizeof(PschEvent))
    staging = 256 * sizeof(PschEvent);
  if (encode < 256 * 1024)
    encode = 256 * 1024;
  if (encode > PSCH_EXPORT_ENCODE_CEILING)
    encode = PSCH_EXPORT_ENCODE_CEILING;
  if (net < 64 * 1024)
    net = 64 * 1024;

  plan->staging_events = (int)(staging / sizeof(PschEvent));
  plan->staging_bytes = (size_t)plan->staging_events * sizeof(PschEvent);
  plan->encode_buf_bytes = (size_t)encode;
  plan->net_buf_bytes = (size_t)net;
  plan->arrow_scratch_bytes = (size_t)arrow;
}

#endif  // PG_STAT_CH_EXPORTER_H
