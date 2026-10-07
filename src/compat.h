// Shims over PostgreSQL API changes between supported major versions
#ifndef PG_STAT_CH_SRC_COMPAT_H_
#define PG_STAT_CH_SRC_COMPAT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "executor/execdesc.h"
#include "executor/instrument.h"
#include "nodes/queryjumble.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

#ifndef PG_SIG_IGN
#define PG_SIG_IGN SIG_IGN
#endif

// post_parse_analyze_hook passes const JumbleState on PG19+
#if PG_VERSION_NUM >= 190000
typedef const JumbleState PschJumbleState;
#else
typedef JumbleState PschJumbleState;
#endif

static inline int PschLWLockNewTrancheId(const char* name) {
#if PG_VERSION_NUM >= 190000
  return LWLockNewTrancheId(name);
#else
  int tranche_id = LWLockNewTrancheId();
  LWLockRegisterTranche(tranche_id, name);
  return tranche_id;
#endif
}

static inline HTAB* PschShmemInitHash(const char* name, int64 nelems, HASHCTL* info, int flags) {
#if PG_VERSION_NUM >= 190000
  return ShmemInitHash(name, nelems, info, flags);
#else
  return ShmemInitHash(name, nelems, nelems, info, flags);
#endif
}

// Query-level instrumentation, NULL unless requested in ExecutorStart
static inline Instrumentation* PschQueryInstr(QueryDesc* query_desc) {
#if PG_VERSION_NUM >= 190000
  return query_desc->query_instr;
#else
  return query_desc->totaltime;
#endif
}

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_COMPAT_H_
