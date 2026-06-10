// Fixed page-pool allocator for flatcc's emitter, force-included into the
// vendored third_party/nanoarrow/src/flatcc.c translation unit (see
// CMakeLists.txt).  See OTEL_REWRITE_DESIGN.md §4 and the review finding
// "flatcc-emitter-page-malloc-hot-path".
//
// Why this exists: flatcc's default emitter malloc/free's its ~3 KB pages, and
// flatcc_emitter_reset's "used_average" heuristic frees pages when usage drops.
// Across our per-flush lifecycle (one large schema once, then 13 tiny dict
// messages + one record-batch message per Arrow flush) that heuristic frees a
// page on every reset and re-mallocs it building the next record batch — i.e.
// heap allocation on the steady-state export path, the exact behavior the
// rewrite forbids.  A NULL malloc there would drop the batch under
// vm.overcommit_memory=2.
//
// This routes FLATCC_EMITTER_ALLOC/FREE through a fixed pool of pre-reserved,
// 64-byte-aligned page slots (matching flatcc's aligned_alloc(64, ...)
// default).  Alloc/free become O(1) free-list ops that never touch the system
// allocator in steady state; flatcc's page-shrink logic is unchanged but now
// only cycles pages in and out of the pool.  If the pool is ever exhausted the
// allocator falls back to malloc so correctness is preserved (only the
// zero-allocation guarantee degrades — the pool is sized well above the
// worst-case concurrent page count).  The bgworker is the sole flatcc user, so
// no locking is needed.
#ifndef PG_STAT_CH_FLATCC_EMITTER_ALLOC_H
#define PG_STAT_CH_FLATCC_EMITTER_ALLOC_H

#include <stddef.h>

void* PschFlatccEmitterAlloc(size_t n);
void PschFlatccEmitterFree(void* p);

// Consumed by flatcc_emitter.h's #ifndef guards (this header is force-included
// before it when compiling flatcc.c).
#define FLATCC_EMITTER_ALLOC(n) PschFlatccEmitterAlloc((size_t)(n))
#define FLATCC_EMITTER_FREE(p)  PschFlatccEmitterFree(p)

#endif  // PG_STAT_CH_FLATCC_EMITTER_ALLOC_H
