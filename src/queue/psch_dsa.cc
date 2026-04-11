// pg_stat_ch DSA string storage implementation
//
// See psch_dsa.h for the shared memory layout diagram and lifecycle overview.

extern "C" {
#include "postgres.h"

#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
}

#include "config/guc.h"
#include "queue/psch_dsa.h"
#include "queue/shmem.h"

// Process-local DSA handle.  Each backend/bgworker gets its own via
// dsa_attach_in_place().  This is NOT shared state — it contains
// process-local page tables, free-list caches, etc.
static dsa_area* psch_dsa = nullptr;

extern "C" {

Size PschDsaShmemSize(void) {
  return (Size)psch_string_area_size * 1024 * 1024;
}

void PschDsaInit(PschSharedState* state, void* dsa_place) {
  Size dsa_size = PschDsaShmemSize();
  state->raw_dsa_area = dsa_place;

  int tranche_id = LWLockNewTrancheId();
  LWLockRegisterTranche(tranche_id, "pg_stat_ch_dsa");

  dsa_area* dsa = dsa_create_in_place(dsa_place, dsa_size, tranche_id, NULL);
  dsa_pin(dsa);
  dsa_set_size_limit(dsa, dsa_size);
  dsa_detach(dsa);  // Postmaster detaches; backends/bgworker re-attach
}

void PschDsaAttach(void) {
  if (psch_dsa != nullptr) {
    return;
  }
  if (psch_shared_state == nullptr || psch_shared_state->raw_dsa_area == nullptr) {
    return;
  }
  // Attach in TopMemoryContext so the dsa_area handle survives transaction
  // boundaries.  Without this, the palloc'd dsa_area struct would be freed
  // when a per-transaction context is destroyed, leaving psch_dsa dangling.
  // Pattern from pg_stat_monitor's pgsm_attach_shmem().
  MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
  psch_dsa = dsa_attach_in_place(psch_shared_state->raw_dsa_area, NULL);
  MemoryContextSwitchTo(oldctx);
  dsa_pin_mapping(psch_dsa);
}

dsa_area* PschDsaGetArea(void) {
  if (psch_dsa == nullptr) {
    PschDsaAttach();
  }
  return psch_dsa;
}

dsa_pointer PschDsaAllocString(const char* src, uint16 len, uint16 max_len) {
  if (len == 0) {
    return InvalidDsaPointer;
  }
  dsa_area* dsa = PschDsaGetArea();
  uint16 clamped = Min(len, static_cast<uint16>(max_len - 1));
  dsa_pointer dp = dsa_allocate_extended(dsa, clamped + 1, DSA_ALLOC_NO_OOM);
  if (DsaPointerIsValid(dp)) {
    char* dst = static_cast<char*>(dsa_get_address(dsa, dp));
    memcpy(dst, src, clamped);
    dst[clamped] = '\0';
  } else {
    pg_atomic_fetch_add_u64(&psch_shared_state->dsa_oom_count, 1);
  }
  return dp;
}

void PschDsaResolveString(dsa_pointer dp, uint16 src_len,
                          char* dst_buf, uint16 max_len, uint16* out_len) {
  if (DsaPointerIsValid(dp)) {
    dsa_area* dsa = PschDsaGetArea();
    char* src = static_cast<char*>(dsa_get_address(dsa, dp));
    uint16 len = Min(src_len, static_cast<uint16>(max_len - 1));
    memcpy(dst_buf, src, len);
    dst_buf[len] = '\0';
    *out_len = len;
    dsa_free(dsa, dp);
  } else {
    dst_buf[0] = '\0';
    *out_len = 0;
  }
}

}  // extern "C"
