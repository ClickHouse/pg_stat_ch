// Implementation of the flatcc emitter page pool (see flatcc_emitter_alloc.h).
//
// The pool is a fixed array of 64-byte-aligned slots carved into a free list on
// first use.  flatcc requests pages of a fixed size (sizeof(flatcc_emitter_page_t),
// ~3 KB); kSlotBytes is comfortably above that.  kSlotCount covers the
// worst-case concurrent page count (the one-time schema message needs the most,
// ~6 native pages; steady-state flushes need only 2-3) with wide margin.

#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "export/flatcc_emitter_alloc.h"

// Larger than sizeof(flatcc_emitter_page_t) (FLATCC_EMITTER_PAGE_SIZE 2944 +
// list pointers) and 64-aligned.  A request above this falls back to malloc.
#define PSCH_FLATCC_SLOT_BYTES 4096
#define PSCH_FLATCC_SLOT_COUNT 16

typedef union PschFlatccSlot {
  // next is valid only while the slot is on the free list; the page bytes
  // overlay it once handed out.
  union PschFlatccSlot* next;
  pg_attribute_aligned(64) char page[PSCH_FLATCC_SLOT_BYTES];
} PschFlatccSlot;

static pg_attribute_aligned(64) PschFlatccSlot g_pool[PSCH_FLATCC_SLOT_COUNT];
static PschFlatccSlot* g_free_list = NULL;
static bool g_pool_initialized = false;

static void PschFlatccPoolInit(void) {
  g_free_list = NULL;
  for (int i = 0; i < PSCH_FLATCC_SLOT_COUNT; i++) {
    g_pool[i].next = g_free_list;
    g_free_list = &g_pool[i];
  }
  g_pool_initialized = true;
}

static inline bool PschFlatccFromPool(const void* p) {
  return (const char*)p >= (const char*)g_pool &&
         (const char*)p < (const char*)g_pool + sizeof(g_pool);
}

void* PschFlatccEmitterAlloc(size_t n) {
  if (!g_pool_initialized) {
    PschFlatccPoolInit();
  }
  // Pool slots are fixed-size; an out-of-range request or an exhausted pool
  // falls back to the system allocator (correctness over the zero-alloc
  // guarantee — sizing keeps this off the steady-state path).
  if (n <= PSCH_FLATCC_SLOT_BYTES && g_free_list != NULL) {
    PschFlatccSlot* slot = g_free_list;
    g_free_list = slot->next;
    return slot;
  }
  return malloc(n);
}

void PschFlatccEmitterFree(void* p) {
  if (p == NULL) {
    return;
  }
  if (PschFlatccFromPool(p)) {
    PschFlatccSlot* slot = (PschFlatccSlot*)p;
    slot->next = g_free_list;
    g_free_list = slot;
    return;
  }
  free(p);
}
