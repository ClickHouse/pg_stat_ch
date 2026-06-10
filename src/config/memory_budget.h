// Resolution of pg_stat_ch.memory_limit into component budgets.
// See OTEL_REWRITE_DESIGN.md §6.  Pure computation + GUC write-back; the
// consumers are src/queue/shmem.c (ring slots, DSA size, intern HTAB) and
// src/export/stats_exporter.c (export arena), plus pg_stat_ch_memory().
//
// Policy (critique-hardened):
//  * memory_limit default 160 MB == verified equivalence point of the old
//    defaults (131072 slots x 520 B ring + intern HTAB + 64 MB DSA + arena).
//    -1 = opt-in auto: clamp(shared_buffers/16, 48 MB, 256 MB).
//  * Per-slot ring cost = sizeof(PschRingEntry) + intern HTAB estimate
//    (hash_estimate_size(cap, entry) / cap) — the intern table is sized by
//    queue_capacity and MUST be charged to the budget.
//  * Explicit overrides win and AUTO-RAISE the total with a WARNING; never
//    FATAL, never starve sibling components.  DSA absorbs the power-of-2
//    rounding remainder.
//  * Resolved values are written back via SetConfigOption(PGC_S_DYNAMIC_DEFAULT)
//    so SHOW reports effective sizes (wal_buffers idiom).
#ifndef PG_STAT_CH_MEMORY_BUDGET_H
#define PG_STAT_CH_MEMORY_BUDGET_H

typedef enum PschBudgetSource {
  PSCH_BUDGET_AUTO = 0,  // derived from memory_limit
  PSCH_BUDGET_OVERRIDE,  // operator set the component GUC explicitly
} PschBudgetSource;

typedef struct PschMemoryBudget {
  uint64 total_bytes;  // effective total (>= configured if overrides raised it)
  bool raised;         // true when overrides pushed past configured memory_limit

  uint32 ring_slots;  // power of 2
  uint64 ring_bytes;  // ring_slots * sizeof(PschRingEntry)
  uint64 intern_bytes;
  PschBudgetSource ring_source;

  uint64 dsa_bytes;
  PschBudgetSource dsa_source;

  uint64 export_arena_bytes;
  PschBudgetSource arena_source;
} PschMemoryBudget;

// Resolve once per process (idempotent, cached).  Safe to call from
// shmem_request_hook (shared_buffers is final there) and later.
extern const PschMemoryBudget* PschMemoryBudgetGet(void);

// SetConfigOption(PGC_S_DYNAMIC_DEFAULT) write-back of resolved component
// values + one WARNING if `raised`.  Call once from shmem_request_hook.
extern void PschMemoryBudgetWriteBack(void);

// One startup LOG line with the full resolved derivation table.
extern void PschMemoryBudgetLogStartup(void);

#endif  // PG_STAT_CH_MEMORY_BUDGET_H
