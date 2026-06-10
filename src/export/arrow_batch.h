// Arrow IPC batch builder — C replacement for the Arrow C++ ArrowBatchBuilder.
//
// Produces byte-equivalent Arrow IPC *streaming* payloads to the previous
// implementation (see OTEL_REWRITE_DESIGN.md §2/§4 and /tmp/wf1-code-arrow-batch.md
// for the exact 56-field schema): V5 little-endian, schema message, 13
// dictionary batches (ids 0-12, isDelta=false, fresh per flush), one record
// batch with BodyCompression{ZSTD, BUFFER} (every buffer compressed, int64-LE
// uncompressed-length prefix, no -1 raw escape), 8-byte EOS.
//
// Memory model: ALL buffers (column builders, dictionary memo tables, flatcc
// builder pages, ZSTD CCtx via static-alloc, IPC output) are preallocated in
// Create() against cfg->scratch_budget_bytes; Append/Finish/Reset perform
// zero heap allocation.  Allocation failure is only possible in Create()/
// SetAttributes(), which return an error.
#ifndef PG_STAT_CH_ARROW_BATCH_H
#define PG_STAT_CH_ARROW_BATCH_H

#include "queue/event.h"

typedef struct PschArrowBuilder PschArrowBuilder;

typedef struct PschArrowBuilderConfig {
  size_t scratch_budget_bytes;   // PschExportArenaPlan.arrow_scratch_bytes
  size_t ipc_budget_bytes;       // pre-compression flush threshold (estimated)
  int max_rows;                  // upper bound on rows per batch (sizes memo tables)
  const char* extra_attributes;  // GUC pg_stat_ch.extra_attributes ("k:v;k:v")
  const char* service_version;   // PG_STAT_CH_VERSION
} PschArrowBuilderConfig;

typedef enum PschArrowAppendResult {
  PSCH_ARROW_APPEND_OK = 0,
  PSCH_ARROW_APPEND_FULL,  // batch reached budget/max_rows: Finish + send + Reset, then retry
  PSCH_ARROW_APPEND_ERR,
} PschArrowAppendResult;

// Preallocates everything; returns NULL with a reason in errbuf on failure.
extern PschArrowBuilder* PschArrowBuilderCreate(const PschArrowBuilderConfig* cfg, char* errbuf,
                                                size_t errlen);
extern void PschArrowBuilderDestroy(PschArrowBuilder* b);

// Re-parse extra_attributes after SIGHUP (cheap; reuses preallocated storage;
// values longer than the preallocated bound are truncated with a WARNING).
extern bool PschArrowBuilderSetAttributes(PschArrowBuilder* b, const char* extra_attributes);

extern PschArrowAppendResult PschArrowBuilderAppend(PschArrowBuilder* b, const PschEvent* ev);

// Serialize the accumulated rows.  *data_out points into builder-owned memory,
// valid until the next Append/Finish/Reset/Destroy.  Returns false on encode
// failure (caller records failure and decides whether rows are dropped).
extern bool PschArrowBuilderFinish(PschArrowBuilder* b, const uint8** data_out, size_t* len_out,
                                   int* rows_out);

extern void PschArrowBuilderReset(PschArrowBuilder* b);

extern int PschArrowBuilderNumRows(const PschArrowBuilder* b);
extern size_t PschArrowBuilderEstimatedBytes(const PschArrowBuilder* b);
extern uint64 PschArrowBuilderMemUsed(const PschArrowBuilder* b);

#endif  // PG_STAT_CH_ARROW_BATCH_H
