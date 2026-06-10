// Resolution of pg_stat_ch.memory_limit into per-component budgets.
// Policy in src/config/memory_budget.h and OTEL_REWRITE_DESIGN.md §6.
//
// Resolution order: total -> ring slots (+ charged intern HTAB) -> export
// arena -> DSA string area (absorbs the remainder).  Explicit overrides win
// and auto-raise the total with one WARNING; nothing here is ever FATAL.
//
// All entry points are init-path only (postmaster shmem_request_hook, or a
// backend running pg_stat_ch_memory()); the result is cached after the first
// call.  On fork-based platforms backends inherit the postmaster's cache, so
// source attribution survives the write-back; under EXEC_BACKEND a backend
// re-resolves against written-back values, which yields identical sizes but
// may report "override" for auto-derived components.

#include "postgres.h"

#include "storage/bufmgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"

#include "config/guc.h"
#include "config/memory_budget.h"
#include "export/exporter.h"
#include "queue/ring_entry.h"

static const uint64 kMegabyte = 1024 * 1024;

// sizeof(PschQueryInternEntry) in src/queue/query_intern.c.  The struct is
// private to that file: key {Oid + pad, uint64, uint64, uint16 + pad} = 32,
// dsa_pointer = 8, uint32 refcount + tail pad = 8.  Keep in sync if the
// interner key/entry changes (panel-verified figure: 48).
static const Size kInternEntrySize = 48;

// Ring floor per design §6; max mirrors the pg_stat_ch.queue_capacity range.
static const uint32 kMinRingSlots = 4096;
static const uint32 kMaxRingSlots = 4194304;  // 2^22

static const uint64 kMinArenaBytes = 8 * 1024 * 1024;
static const uint64 kMaxArenaBytes = 64 * 1024 * 1024;

static PschMemoryBudget budget_cache;
static bool budget_cache_valid = false;

static uint64 ClampU64(uint64 v, uint64 lo, uint64 hi) {
  if (v < lo) {
    return lo;
  }
  if (v > hi) {
    return hi;
  }
  return v;
}

static uint64 RoundUpToMb(uint64 v) {
  return ((v + kMegabyte - 1) / kMegabyte) * kMegabyte;
}

static uint32 RoundUpPow2(uint32 v) {
  uint32 pow2 = 1;

  // The queue_capacity GUC range caps v at 2^22, so the shift cannot overflow.
  while (pow2 < v) {
    pow2 <<= 1;
  }
  return pow2;
}

// Full per-capacity cost charged to the budget: ring slots plus the intern
// HTAB, which is sized at queue_capacity entries (query_intern.c) and must
// not ride for free.
static uint64 RingPlusInternBytes(uint32 slots) {
  return (uint64)slots * sizeof(PschRingEntry) +
         (uint64)hash_estimate_size((long)slots, kInternEntrySize);
}

// Largest power of 2 whose ring+intern cost fits half the total, floor 4096.
static uint32 AutoRingSlots(uint64 total_bytes) {
  uint64 half = total_bytes / 2;
  uint32 best = kMinRingSlots;
  uint32 slots = kMinRingSlots;

  for (;;) {
    if (RingPlusInternBytes(slots) <= half) {
      best = slots;
    } else {
      break;
    }
    if (slots == kMaxRingSlots) {
      break;
    }
    slots <<= 1;
  }
  return best;
}

// Smallest whole-MB arena in [8 MB, 64 MB] whose PschExportArenaSplit plan
// satisfies every explicitly-set byte/count bridge GUC.  Using the real split
// function keeps the bridge mapping drift-proof against share changes in
// exporter.h.  Requests beyond what 64 MB can honor are clamped — the old
// batch_max=200000 default implied ~880 MB of staging, which is exactly the
// transient-heap bug this rewrite deletes.
static uint64 BridgeArenaBytes(void) {
  uint64 arena;

  for (arena = kMinArenaBytes; arena <= kMaxArenaBytes; arena += kMegabyte) {
    PschExportArenaPlan plan;

    PschExportArenaSplit(arena, &plan);
    if (psch_bridge_batch_max > 0 && plan.staging_events < psch_bridge_batch_max) {
      continue;
    }
    if (psch_bridge_otel_log_batch_size > 0 &&
        plan.staging_events < psch_bridge_otel_log_batch_size) {
      continue;
    }
    if (psch_bridge_otel_max_block_bytes > 0 &&
        plan.arrow_scratch_bytes < (size_t)psch_bridge_otel_max_block_bytes) {
      continue;
    }
    if (psch_bridge_otel_log_max_bytes > 0 &&
        plan.encode_buf_bytes <
            Min((size_t)psch_bridge_otel_log_max_bytes, PSCH_EXPORT_ENCODE_CEILING)) {
      continue;
    }
    return arena;
  }
  return kMaxArenaBytes;
}

static bool AnyByteBridgeSet(void) {
  return psch_bridge_batch_max > 0 || psch_bridge_otel_max_block_bytes > 0 ||
         psch_bridge_otel_log_max_bytes > 0 || psch_bridge_otel_log_batch_size > 0;
}

static void Resolve(PschMemoryBudget* budget) {
  uint64 total;
  uint64 component_sum;

  MemSet(budget, 0, sizeof(*budget));

  if (psch_memory_limit_mb == -1) {
    // Opt-in auto: clamp(shared_buffers/16, 48 MB, 256 MB).  NBuffers is in
    // BLCKSZ pages and is final by shmem_request_hook time.
    uint64 shared_buffers_bytes = (uint64)NBuffers * BLCKSZ;

    total = ClampU64(shared_buffers_bytes / 16, 48 * kMegabyte, 256 * kMegabyte);
  } else {
    total = (uint64)psch_memory_limit_mb * kMegabyte;
  }

  if (psch_queue_capacity > 0) {
    // The check hook already rounded; round again defensively for the
    // not-via-GUC path (extension loaded without registration).  Honor the
    // same kMinRingSlots floor as the auto path so an explicit tiny value
    // cannot produce a uselessly small ring.
    budget->ring_slots = Max(RoundUpPow2((uint32)psch_queue_capacity), kMinRingSlots);
    budget->ring_source = PSCH_BUDGET_OVERRIDE;
  } else {
    budget->ring_slots = AutoRingSlots(total);
    budget->ring_source = PSCH_BUDGET_AUTO;
  }
  budget->ring_bytes = (uint64)budget->ring_slots * sizeof(PschRingEntry);
  budget->intern_bytes = (uint64)hash_estimate_size((long)budget->ring_slots, kInternEntrySize);

  if (psch_export_buffer_size_mb > 0) {
    budget->export_arena_bytes = (uint64)psch_export_buffer_size_mb * kMegabyte;
    budget->arena_source = PSCH_BUDGET_OVERRIDE;
  } else if (AnyByteBridgeSet()) {
    // Bridge-derived counts as an override: the operator asked for specific
    // capacities, just via deprecated knobs.
    budget->export_arena_bytes = BridgeArenaBytes();
    budget->arena_source = PSCH_BUDGET_OVERRIDE;
  } else {
    budget->export_arena_bytes = RoundUpToMb(ClampU64(total / 8, kMinArenaBytes, kMaxArenaBytes));
    budget->arena_source = PSCH_BUDGET_AUTO;
  }

  if (psch_string_area_size > 0) {
    budget->dsa_bytes = (uint64)psch_string_area_size * kMegabyte;
    budget->dsa_source = PSCH_BUDGET_OVERRIDE;
  } else {
    // DSA absorbs the remainder (pow2 rounding loss becomes string capacity).
    // Truncated to whole MB so the value round-trips through the GUC
    // write-back (psch_dsa.c sizes the area from the GUC), and clamped to the
    // 8..1024 MB GUC range.
    uint64 used = budget->ring_bytes + budget->intern_bytes + budget->export_arena_bytes;
    uint64 remainder = total > used ? total - used : 0;

    remainder -= remainder % kMegabyte;
    budget->dsa_bytes = ClampU64(remainder, 8 * kMegabyte, 1024 * kMegabyte);
    budget->dsa_source = PSCH_BUDGET_AUTO;
  }

  // Overrides (and component floors) never starve siblings: the total gives
  // way instead.  Never FATAL — old fleet templates must keep booting.
  component_sum =
      budget->ring_bytes + budget->intern_bytes + budget->export_arena_bytes + budget->dsa_bytes;
  if (component_sum > total) {
    total = component_sum;
    budget->raised = true;
  }
  budget->total_bytes = total;
}

const PschMemoryBudget* PschMemoryBudgetGet(void) {
  if (!budget_cache_valid) {
    Resolve(&budget_cache);
    budget_cache_valid = true;
  }
  return &budget_cache;
}

static const char* SourceText(PschBudgetSource source) {
  return source == PSCH_BUDGET_OVERRIDE ? "override" : "auto";
}

static const char* TotalSourceText(const PschMemoryBudget* budget) {
  if (budget->raised) {
    return "raised";
  }
  return psch_memory_limit_mb == -1 ? "auto" : "configured";
}

void PschMemoryBudgetWriteBack(void) {
  const PschMemoryBudget* budget = PschMemoryBudgetGet();
  static bool done = false;
  char value[32];

  if (done) {
    return;
  }
  done = true;

  // wal_buffers idiom: PGC_S_DYNAMIC_DEFAULT loses to explicit settings, so
  // operator-set components keep their (check-hook-normalized) values and
  // auto components start SHOWing their effective sizes.
  snprintf(value, sizeof(value), "%u", budget->ring_slots);
  SetConfigOption("pg_stat_ch.queue_capacity", value, PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);

  snprintf(value, sizeof(value), UINT64_FORMAT, budget->dsa_bytes / kMegabyte);
  SetConfigOption("pg_stat_ch.string_area_size", value, PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);

  snprintf(value, sizeof(value), UINT64_FORMAT, budget->export_arena_bytes / kMegabyte);
  SetConfigOption("pg_stat_ch.export_buffer_size", value, PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);

  // otel_log_delay_ms bridge: becomes the dynamic default of its successor;
  // ignored automatically when export_timeout_ms was set explicitly.
  if (psch_bridge_otel_log_delay_ms > 0) {
    snprintf(value, sizeof(value), "%d", psch_bridge_otel_log_delay_ms);
    SetConfigOption("pg_stat_ch.export_timeout_ms", value, PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);
  }

  if (budget->raised) {
    uint64 raised_mb = (budget->total_bytes + kMegabyte - 1) / kMegabyte;

    // Component maxima keep the sum under the 4096 MB GUC max, but clamp so a
    // future range change cannot turn the write-back into a boot failure.
    snprintf(value, sizeof(value), UINT64_FORMAT, Min(raised_mb, (uint64)4096));
    SetConfigOption("pg_stat_ch.memory_limit", value, PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);

    ereport(WARNING,
            (errmsg("pg_stat_ch: effective memory_limit raised to " UINT64_FORMAT
                    " MB to fit configured components",
                    raised_mb),
             errdetail("ring_queue=%u slots/" UINT64_FORMAT " kB (%s), intern_table=" UINT64_FORMAT
                       " kB (derived), string_area=" UINT64_FORMAT " kB (%s), "
                       "export_arena=" UINT64_FORMAT " kB (%s).",
                       budget->ring_slots, budget->ring_bytes / 1024,
                       SourceText(budget->ring_source), budget->intern_bytes / 1024,
                       budget->dsa_bytes / 1024, SourceText(budget->dsa_source),
                       budget->export_arena_bytes / 1024, SourceText(budget->arena_source)),
             errhint("Raise pg_stat_ch.memory_limit or remove the component overrides.")));
  }
}

void PschMemoryBudgetLogStartup(void) {
  const PschMemoryBudget* budget = PschMemoryBudgetGet();

  ereport(
      LOG,
      (errmsg("pg_stat_ch: memory budget total=" UINT64_FORMAT
              " kB (%s): ring_queue=%u slots/" UINT64_FORMAT " kB (%s), intern_table=" UINT64_FORMAT
              " kB (derived), string_area=" UINT64_FORMAT " kB (%s), export_arena=" UINT64_FORMAT
              " kB (%s)",
              budget->total_bytes / 1024, TotalSourceText(budget), budget->ring_slots,
              budget->ring_bytes / 1024, SourceText(budget->ring_source),
              budget->intern_bytes / 1024, budget->dsa_bytes / 1024, SourceText(budget->dsa_source),
              budget->export_arena_bytes / 1024, SourceText(budget->arena_source))));
}
