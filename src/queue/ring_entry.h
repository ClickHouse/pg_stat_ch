// Compact ring buffer entry for shared memory storage.
//
// PschRingEntry mirrors PschEvent's numeric fields identically but replaces
// the two large inline char arrays (query[2048] and err_message[2048]) with
// dsa_pointer values that reference variable-length strings allocated in a
// DSA (Dynamic Shared Memory Area).
//
// This shrinks each ring buffer slot from ~4.5KB to ~500 bytes while keeping
// the public API type (PschEvent) unchanged.  The conversion happens
// transparently inside PschEnqueueEvent / PschDequeueEvent.
#ifndef PG_STAT_CH_SRC_QUEUE_RING_ENTRY_H_
#define PG_STAT_CH_SRC_QUEUE_RING_ENTRY_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "datatype/timestamp.h"
#include "utils/dsa.h"

#include "queue/event.h"

// Compact ring buffer slot.  All numeric fields through _padding3 have the
// same types and order as PschEvent so the prefix can be block-copied.  The
// two 2KB char arrays are replaced by 8-byte dsa_pointer values.
struct PschRingEntry {
  // === Timing ===
  TimestampTz ts_start;
  uint64 duration_us;

  // === Identity ===
  Oid dbid;
  Oid userid;
  char datname[64];
  uint8 datname_len;
  char username[64];
  uint8 username_len;
  int32 pid;
  uint64 queryid;
  bool top_level;
  PschCmdType cmd_type;

  // === Results ===
  uint64 rows;

  // === Buffer usage ===
  int64 shared_blks_hit;
  int64 shared_blks_read;
  int64 shared_blks_dirtied;
  int64 shared_blks_written;
  int64 local_blks_hit;
  int64 local_blks_read;
  int64 local_blks_dirtied;
  int64 local_blks_written;
  int64 temp_blks_read;
  int64 temp_blks_written;

  // === I/O timing (microseconds) ===
  int64 shared_blk_read_time_us;
  int64 shared_blk_write_time_us;
  int64 local_blk_read_time_us;
  int64 local_blk_write_time_us;
  int64 temp_blk_read_time_us;
  int64 temp_blk_write_time_us;

  // === WAL usage ===
  int64 wal_records;
  int64 wal_fpi;
  uint64 wal_bytes;

  // === CPU time (microseconds) ===
  int64 cpu_user_time_us;
  int64 cpu_sys_time_us;

  // === JIT instrumentation ===
  int32 jit_functions;
  int32 jit_generation_time_us;
  int32 jit_deform_time_us;
  int32 jit_inlining_time_us;
  int32 jit_optimization_time_us;
  int32 jit_emission_time_us;

  // === Parallel workers ===
  int16 parallel_workers_planned;
  int16 parallel_workers_launched;

  // === Error info ===
  char err_sqlstate[6];
  uint8 err_elevel;
  uint8 _padding3;
  uint16 err_message_len;

  // === Client context (kept inline — small, always populated) ===
  char application_name[64];
  uint8 application_name_len;
  char client_addr[46];
  uint8 client_addr_len;
  uint16 query_len;

  // --- DSA pointers replace the two trailing char arrays in PschEvent ---
  dsa_pointer err_message_dsa;  // was: char err_message[PSCH_MAX_ERR_MSG_LEN]
  dsa_pointer query_dsa;        // was: char query[PSCH_MAX_QUERY_LEN]
};

#ifdef __cplusplus
}

// Verify that the fixed prefix of PschRingEntry has the same binary layout as
// PschEvent up to (and including) query_len — the last field before the two
// variable-length char arrays / dsa_pointers diverge.  If a field is added to
// PschEvent without updating PschRingEntry, these will fire at compile time.
static_assert(offsetof(PschEvent, ts_start) == offsetof(PschRingEntry, ts_start),
              "ts_start offset mismatch");
static_assert(offsetof(PschEvent, queryid) == offsetof(PschRingEntry, queryid),
              "queryid offset mismatch");
static_assert(offsetof(PschEvent, rows) == offsetof(PschRingEntry, rows), "rows offset mismatch");
static_assert(offsetof(PschEvent, shared_blks_hit) == offsetof(PschRingEntry, shared_blks_hit),
              "shared_blks_hit offset mismatch");
static_assert(offsetof(PschEvent, wal_records) == offsetof(PschRingEntry, wal_records),
              "wal_records offset mismatch");
static_assert(offsetof(PschEvent, err_sqlstate) == offsetof(PschRingEntry, err_sqlstate),
              "err_sqlstate offset mismatch");
static_assert(offsetof(PschEvent, err_message_len) == offsetof(PschRingEntry, err_message_len),
              "err_message_len offset mismatch");
static_assert(offsetof(PschEvent, application_name) == offsetof(PschRingEntry, application_name),
              "application_name offset mismatch");
static_assert(offsetof(PschEvent, query_len) == offsetof(PschRingEntry, query_len),
              "query_len offset mismatch");
// The fields after query_len diverge: PschEvent has char arrays, PschRingEntry
// has dsa_pointers.  This is intentional — the fixed prefix up to query_len is
// block-copied with a single memcpy.
static_assert(offsetof(PschEvent, err_message) == offsetof(PschRingEntry, err_message_dsa),
              "err_message/err_message_dsa start offset mismatch");

#endif

#endif  // PG_STAT_CH_SRC_QUEUE_RING_ENTRY_H_
