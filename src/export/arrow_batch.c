// Arrow IPC batch builder — C replacement for the Arrow C++ ArrowBatchBuilder.
//
// Behavior oracle: the previous arrow_batch.cc (see /tmp/orig-export/) and
// /tmp/wf1-code-arrow-batch.md.  The 56-column schema (order traps:
// shared_blks_written BEFORE shared_blks_dirtied; jit_deform_time_us last
// among jit), all value transforms (epoch shift, decimal query_id/pid,
// PschCmdTypeToString, length clamps, negative->0 with rate-limited
// WARNING, extra_attributes parsing with trim + first-wins), and the
// estimated-bytes formula are preserved exactly.
//
// Storage: one malloc'd slab carved into per-column fixed-capacity arrays;
// dictionary columns use fixed-capacity open-addressing memo tables.  All
// sizing happens in Create() against cfg->scratch_budget_bytes, degrading
// rows-per-batch (with a LOG) until the plan fits.  Append/Finish/Reset
// never allocate; Append reports FULL instead of growing.
#include "postgres.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "export/arrow_batch.h"
#include "export/arrow_ipc_emit.h"
#include "queue/event.h"

#define PSCH_ARROW_ATTR_MAX 128

static const int64 kPostgresEpochOffsetUs = INT64CONST(946684800000000);
static const size_t kFixedBytesPerRow = 512;   // estimate term, matches old impl
static const size_t kIpcEnvelopeBytes = 1024;  // estimate seed, matches old impl
static const size_t kDefaultIpcBudgetBytes = 3 * 1024 * 1024;
static const size_t kMinIpcBudgetBytes = 65536;
static const size_t kMaxIpcBudgetBytes = 16 * 1024 * 1024;
static const int kMinDegradeRows = 16;
static const int kMaxRowsCap = 65536;

// ---------------------------------------------------------------------------
// The schema, single-sourced.  Expansion order == wire column order.
// PSCH_COL_<KIND>(name) macros are (re)defined at each expansion site.
// ---------------------------------------------------------------------------
#define PSCH_ARROW_COLUMNS(X)       \
  X(TS, ts)                         \
  X(DICT, severity)                 \
  X(UTF8, body)                     \
  X(UTF8, trace_id)                 \
  X(UTF8, span_id)                  \
  X(DICT, query_id)                 \
  X(DICT, db_name)                  \
  X(DICT, db_user)                  \
  X(DICT, db_operation)             \
  X(DICT, app)                      \
  X(DICT, client_addr)              \
  X(UTF8, query_text)               \
  X(UTF8, pid)                      \
  X(UTF8, err_message)              \
  X(DICT, err_sqlstate)             \
  X(I32, err_elevel)                \
  X(U64, duration_us)               \
  X(U64, rows)                      \
  X(U64, shared_blks_hit)           \
  X(U64, shared_blks_read)          \
  X(U64, shared_blks_written)       \
  X(U64, shared_blks_dirtied)       \
  X(U64, shared_blk_read_time_us)   \
  X(U64, shared_blk_write_time_us)  \
  X(U64, local_blks_hit)            \
  X(U64, local_blks_read)           \
  X(U64, local_blks_written)        \
  X(U64, local_blks_dirtied)        \
  X(U64, local_blk_read_time_us)    \
  X(U64, local_blk_write_time_us)   \
  X(U64, temp_blks_read)            \
  X(U64, temp_blks_written)         \
  X(U64, temp_blk_read_time_us)     \
  X(U64, temp_blk_write_time_us)    \
  X(U64, wal_records)               \
  X(U64, wal_bytes)                 \
  X(U64, wal_fpi)                   \
  X(U64, cpu_user_time_us)          \
  X(U64, cpu_sys_time_us)           \
  X(U64, jit_functions)             \
  X(U64, jit_generation_time_us)    \
  X(U64, jit_inlining_time_us)      \
  X(U64, jit_optimization_time_us)  \
  X(U64, jit_emission_time_us)      \
  X(U64, jit_deform_time_us)        \
  X(U32, parallel_workers_planned)  \
  X(U32, parallel_workers_launched) \
  X(UTF8, instance_ubid)            \
  X(UTF8, server_ubid)              \
  X(DICT, server_role)              \
  X(DICT, read_replica_type)        \
  X(DICT, region)                   \
  X(DICT, cell)                     \
  X(DICT, service_version)          \
  X(UTF8, host_id)                  \
  X(UTF8, pod_name)

#define PSCH_COL(K, n) PSCH_COL_##K(n)

// Slot enums per storage kind, derived from the same list.
#define PSCH_COL_TS(n)
#define PSCH_COL_I32(n)
#define PSCH_COL_U32(n)
#define PSCH_COL_U64(n) PSCH_U64_##n,
#define PSCH_COL_UTF8(n)
#define PSCH_COL_DICT(n)
typedef enum PschU64Slot { PSCH_ARROW_COLUMNS(PSCH_COL) PSCH_U64_NUM } PschU64Slot;
#undef PSCH_COL_U64
#undef PSCH_COL_U32
#define PSCH_COL_U64(n)
#define PSCH_COL_U32(n) PSCH_U32_##n,
typedef enum PschU32Slot { PSCH_ARROW_COLUMNS(PSCH_COL) PSCH_U32_NUM } PschU32Slot;
#undef PSCH_COL_U32
#undef PSCH_COL_UTF8
#define PSCH_COL_U32(n)
#define PSCH_COL_UTF8(n) PSCH_U8_##n,
typedef enum PschUtf8Slot { PSCH_ARROW_COLUMNS(PSCH_COL) PSCH_U8_NUM } PschUtf8Slot;
#undef PSCH_COL_UTF8
#undef PSCH_COL_DICT
#define PSCH_COL_UTF8(n)
#define PSCH_COL_DICT(n) PSCH_DICT_##n,
typedef enum PschDictSlot { PSCH_ARROW_COLUMNS(PSCH_COL) PSCH_DICT_NUM } PschDictSlot;
#undef PSCH_COL_TS
#undef PSCH_COL_I32
#undef PSCH_COL_U32
#undef PSCH_COL_U64
#undef PSCH_COL_UTF8
#undef PSCH_COL_DICT

#define PSCH_COL_TS(n)   +1
#define PSCH_COL_I32(n)  +1
#define PSCH_COL_U32(n)  +1
#define PSCH_COL_U64(n)  +1
#define PSCH_COL_UTF8(n) +1
#define PSCH_COL_DICT(n) +1
enum { PSCH_ARROW_NUM_COLS = 0 PSCH_ARROW_COLUMNS(PSCH_COL) };
#undef PSCH_COL_TS
#undef PSCH_COL_I32
#undef PSCH_COL_U32
#undef PSCH_COL_U64
#undef PSCH_COL_UTF8
#undef PSCH_COL_DICT

StaticAssertDecl(PSCH_ARROW_NUM_COLS == 56, "wire schema must have exactly 56 columns");

// Resource attributes replicated into every row (extra_attributes GUC keys).
typedef enum PschAttrSlot {
  PSCH_ATTR_INSTANCE_UBID = 0,
  PSCH_ATTR_SERVER_UBID,
  PSCH_ATTR_SERVER_ROLE,
  PSCH_ATTR_READ_REPLICA_TYPE,
  PSCH_ATTR_REGION,
  PSCH_ATTR_CELL,
  PSCH_ATTR_HOST_ID,
  PSCH_ATTR_POD_NAME,
  PSCH_ATTR_NUM,
} PschAttrSlot;

static const char* const kAttrKeys[PSCH_ATTR_NUM] = {
    "instance_ubid", "server_ubid", "server_role", "read_replica_type",
    "region",        "cell",        "host_id",     "pod_name",
};

typedef struct PschUtf8Col {
  int32* offsets;  // max_rows + 1 entries; offsets[0] == 0
  char* data;
  size_t data_cap;
} PschUtf8Col;

typedef struct PschDictCol {
  int32* indices;      // max_rows entries
  int32* val_offsets;  // max_distinct + 1 entries; val_offsets[0] == 0
  char* val_data;
  size_t val_cap;
  int32* memo;  // memo_cap slots; value = dict index + 1; 0 = empty
  uint32 memo_cap;
  int32 max_distinct;
  int32 count;
} PschDictCol;

struct PschArrowBuilder {
  int max_rows;
  size_t ipc_budget_bytes;
  int num_rows;
  size_t estimated_bytes;

  int64* ts;
  int32* err_elevel;
  uint32* u32[PSCH_U32_NUM];
  uint64* u64[PSCH_U64_NUM];
  PschUtf8Col utf8[PSCH_U8_NUM];
  PschDictCol dict[PSCH_DICT_NUM];

  char attr_val[PSCH_ATTR_NUM][PSCH_ARROW_ATTR_MAX + 1];
  int attr_len[PSCH_ATTR_NUM];
  char service_version[PSCH_ARROW_ATTR_MAX + 1];
  int service_version_len;

  uint8* slab;
  size_t slab_bytes;
  PschIpcEmitter* emitter;
  uint64 mem_used;
};

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

static size_t AlignUp8(size_t n) {
  return (n + 7) & ~(size_t)7;
}

static void SetErr(char* errbuf, size_t errlen, const char* msg) {
  if (errbuf != NULL && errlen > 0)
    snprintf(errbuf, errlen, "%s", msg);
}

// Matches the old LogNegativeValue (stats_exporter.cc): at most one WARNING
// per second across all columns.
static void LogNegativeValue(const char* column_name, int64 value) {
  static time_t last_log = 0;
  time_t now = time(NULL);

  if (now - last_log >= 1) {
    elog(WARNING, "pg_stat_ch: Negative value " INT64_FORMAT " clamped to 0 for column `%s`", value,
         column_name);
    last_log = now;
  }
}

static uint64 ClampSignedToUint64(int64 value, const char* column_name) {
  if (value < 0) {
    LogNegativeValue(column_name, value);
    return 0;
  }
  return (uint64)value;
}

static uint32 ClampSignedToUint32(int32 value, const char* column_name) {
  if (value < 0) {
    LogNegativeValue(column_name, value);
    return 0;
  }
  return (uint32)value;
}

static int ClampFieldLen(unsigned len, unsigned max, const char* field_name) {
  if (len <= max)
    return (int)len;
  // Rate-limit to at most one WARNING/sec (like LogNegativeValue): a corrupt or
  // oversized field could otherwise flood the log at the event rate.
  static time_t last_log = 0;
  time_t now = time(NULL);
  if (now - last_log >= 1) {
    elog(WARNING, "pg_stat_ch: invalid %s %u, clamping", field_name, len);
    last_log = now;
  }
  return (int)max;
}

static uint32 HashBytes(const char* s, int len) {
  uint32 h = 2166136261u;  // FNV-1a

  for (int i = 0; i < len; i++) {
    h ^= (uint8)s[i];
    h *= 16777619u;
  }
  return h;
}

// Returns the dictionary index if present, else -1 with *ins_slot set to the
// free memo slot.  Load factor <= 0.5 guarantees termination.
static int32 DictLookup(const PschDictCol* d, const char* s, int len, uint32* ins_slot) {
  uint32 mask = d->memo_cap - 1;
  uint32 idx = HashBytes(s, len) & mask;

  for (;;) {
    int32 v = d->memo[idx];

    if (v == 0) {
      *ins_slot = idx;
      return -1;
    }
    {
      int32 cand = v - 1;
      int32 off = d->val_offsets[cand];
      int32 clen = d->val_offsets[cand + 1] - off;

      if (clen == len && memcmp(d->val_data + off, s, (size_t)len) == 0)
        return cand;
    }
    idx = (idx + 1) & mask;
  }
}

static int32 DictInsert(PschDictCol* d, uint32 slot, const char* s, int len) {
  int32 idx = d->count;
  int32 off = d->val_offsets[idx];

  memcpy(d->val_data + off, s, (size_t)len);
  d->val_offsets[idx + 1] = off + len;
  d->memo[slot] = idx + 1;
  d->count++;
  return idx;
}

static void Utf8Append(PschUtf8Col* c, int row, const char* s, int len) {
  int32 off = c->offsets[row];

  memcpy(c->data + off, s, (size_t)len);
  c->offsets[row + 1] = off + len;
}

// ---------------------------------------------------------------------------
// Capacity policy (single source for specs + slab carving)
// ---------------------------------------------------------------------------

static size_t Utf8DataCapFor(int slot, int rows, size_t ipc_budget) {
  size_t per_row;

  switch (slot) {
    case PSCH_U8_body:
    case PSCH_U8_trace_id:
    case PSCH_U8_span_id:
      return 8;  // always ""
    case PSCH_U8_query_text:
      per_row = PSCH_MAX_QUERY_LEN;
      break;
    case PSCH_U8_err_message:
      per_row = PSCH_MAX_ERR_MSG_LEN;
      break;
    case PSCH_U8_pid:
      return (size_t)12 * (size_t)rows;
    default:  // attr-backed: instance_ubid, server_ubid, host_id, pod_name
      per_row = PSCH_ARROW_ATTR_MAX;
      break;
  }
  // The estimate-driven flush bounds total var bytes near ipc_budget; one
  // max-size row of overshoot is possible (flush check is post-append).
  return Min(per_row * (size_t)rows, ipc_budget + per_row);
}

static int32 DictMaxDistinctFor(int slot, int rows) {
  switch (slot) {
    case PSCH_DICT_severity:     // always ""
    case PSCH_DICT_server_role:  // constant per batch (extra_attributes)
    case PSCH_DICT_read_replica_type:
    case PSCH_DICT_region:
    case PSCH_DICT_cell:
    case PSCH_DICT_service_version:
      return 2;
    case PSCH_DICT_db_operation:
      return 8;  // PschCmdType domain
    default:     // query_id, db_name, db_user, app, client_addr, err_sqlstate
      return rows;
  }
}

static size_t DictValMaxLenFor(int slot) {
  switch (slot) {
    case PSCH_DICT_severity:
      return 1;
    case PSCH_DICT_query_id:
      return 21;  // uint64 decimal
    case PSCH_DICT_db_operation:
      return 8;  // "UNKNOWN"
    case PSCH_DICT_client_addr:
      return PSCH_MAX_CLIENT_ADDR_LEN;
    case PSCH_DICT_err_sqlstate:
      return 5;
    case PSCH_DICT_db_name:
    case PSCH_DICT_db_user:
    case PSCH_DICT_app:
      return 63;
    default:  // attr-backed + service_version
      return PSCH_ARROW_ATTR_MAX;
  }
}

static uint32 MemoCapFor(int32 max_distinct) {
  uint32 cap = 8;

  while (cap < 2u * (uint32)max_distinct)
    cap <<= 1;
  return cap;
}

static void SpecFixed(PschIpcColSpec* specs, int* i, const char* name, PschIpcFixedType type,
                      size_t width, int rows) {
  specs[*i] = (PschIpcColSpec){.name = name,
                               .kind = PSCH_IPC_COL_FIXED,
                               .fixed_type = type,
                               .data_cap = width * (size_t)rows};
  (*i)++;
}

static void SpecUtf8(PschIpcColSpec* specs, int* i, const char* name, int slot, int rows,
                     size_t ipc_budget) {
  specs[*i] = (PschIpcColSpec){.name = name,
                               .kind = PSCH_IPC_COL_UTF8,
                               .offsets_cap = ((size_t)rows + 1) * 4,
                               .var_cap = Utf8DataCapFor(slot, rows, ipc_budget)};
  (*i)++;
}

static void SpecDict(PschIpcColSpec* specs, int* i, const char* name, int slot, int rows) {
  int32 md = DictMaxDistinctFor(slot, rows);

  specs[*i] = (PschIpcColSpec){.name = name,
                               .kind = PSCH_IPC_COL_DICT,
                               .data_cap = (size_t)rows * 4,
                               .offsets_cap = ((size_t)md + 1) * 4,
                               .var_cap = Max(8, (size_t)md * DictValMaxLenFor(slot))};
  (*i)++;
}

static void BuildSpecs(PschIpcColSpec* specs, int rows, size_t ipc_budget) {
  int i = 0;

#define PSCH_COL_TS(n)   SpecFixed(specs, &i, #n, PSCH_IPC_TS_NS_UTC, 8, rows);
#define PSCH_COL_I32(n)  SpecFixed(specs, &i, #n, PSCH_IPC_INT32, 4, rows);
#define PSCH_COL_U32(n)  SpecFixed(specs, &i, #n, PSCH_IPC_UINT32, 4, rows);
#define PSCH_COL_U64(n)  SpecFixed(specs, &i, #n, PSCH_IPC_UINT64, 8, rows);
#define PSCH_COL_UTF8(n) SpecUtf8(specs, &i, #n, PSCH_U8_##n, rows, ipc_budget);
#define PSCH_COL_DICT(n) SpecDict(specs, &i, #n, PSCH_DICT_##n, rows);
  PSCH_ARROW_COLUMNS(PSCH_COL)
#undef PSCH_COL_TS
#undef PSCH_COL_I32
#undef PSCH_COL_U32
#undef PSCH_COL_U64
#undef PSCH_COL_UTF8
#undef PSCH_COL_DICT
}

static uint8* CarvePtr(uint8* base, size_t* off, size_t nbytes) {
  uint8* p = base != NULL ? base + *off : NULL;

  *off += AlignUp8(nbytes);
  return p;
}

static void CarveUtf8(PschUtf8Col* c, int slot, uint8* base, size_t* off, int rows,
                      size_t ipc_budget) {
  c->offsets = (int32*)CarvePtr(base, off, ((size_t)rows + 1) * 4);
  c->data_cap = Utf8DataCapFor(slot, rows, ipc_budget);
  c->data = (char*)CarvePtr(base, off, c->data_cap);
}

static void CarveDict(PschDictCol* d, int slot, uint8* base, size_t* off, int rows) {
  d->max_distinct = DictMaxDistinctFor(slot, rows);
  d->memo_cap = MemoCapFor(d->max_distinct);
  d->val_cap = Max(8, (size_t)d->max_distinct * DictValMaxLenFor(slot));
  d->indices = (int32*)CarvePtr(base, off, (size_t)rows * 4);
  d->val_offsets = (int32*)CarvePtr(base, off, ((size_t)d->max_distinct + 1) * 4);
  d->val_data = (char*)CarvePtr(base, off, d->val_cap);
  d->memo = (int32*)CarvePtr(base, off, (size_t)d->memo_cap * 4);
}

// One pass assigns all column pointers and returns the slab size; called
// with base=NULL for sizing, then with the real slab.
static size_t CarveAll(PschArrowBuilder* b, uint8* base, int rows, size_t ipc_budget) {
  size_t off = 0;

#define PSCH_COL_TS(n)  b->ts = (int64*)CarvePtr(base, &off, (size_t)rows * 8);
#define PSCH_COL_I32(n) b->err_elevel = (int32*)CarvePtr(base, &off, (size_t)rows * 4);
#define PSCH_COL_U32(n) b->u32[PSCH_U32_##n] = (uint32*)CarvePtr(base, &off, (size_t)rows * 4);
#define PSCH_COL_U64(n) b->u64[PSCH_U64_##n] = (uint64*)CarvePtr(base, &off, (size_t)rows * 8);
#define PSCH_COL_UTF8(n) \
  CarveUtf8(&b->utf8[PSCH_U8_##n], PSCH_U8_##n, base, &off, rows, ipc_budget);
#define PSCH_COL_DICT(n) CarveDict(&b->dict[PSCH_DICT_##n], PSCH_DICT_##n, base, &off, rows);
  PSCH_ARROW_COLUMNS(PSCH_COL)
#undef PSCH_COL_TS
#undef PSCH_COL_I32
#undef PSCH_COL_U32
#undef PSCH_COL_U64
#undef PSCH_COL_UTF8
#undef PSCH_COL_DICT
  return off;
}

static void DataFixed(PschIpcColData* cols, int* i, const void* p, size_t len) {
  cols[*i] = (PschIpcColData){.data = p, .data_len = len};
  (*i)++;
}

static void DataUtf8(PschIpcColData* cols, int* i, const PschUtf8Col* c, int rows) {
  cols[*i] = (PschIpcColData){
      .offsets = c->offsets, .var_data = c->data, .var_len = (size_t)c->offsets[rows]};
  (*i)++;
}

static void DataDict(PschIpcColData* cols, int* i, const PschDictCol* d, int rows) {
  cols[*i] = (PschIpcColData){.data = d->indices,
                              .data_len = (size_t)rows * 4,
                              .offsets = d->val_offsets,
                              .var_data = d->val_data,
                              .var_len = (size_t)d->val_offsets[d->count],
                              .dict_count = d->count};
  (*i)++;
}

static void FillColData(PschArrowBuilder* b, PschIpcColData* cols) {
  int i = 0;
  int rows = b->num_rows;

#define PSCH_COL_TS(n)   DataFixed(cols, &i, b->ts, (size_t)rows * 8);
#define PSCH_COL_I32(n)  DataFixed(cols, &i, b->err_elevel, (size_t)rows * 4);
#define PSCH_COL_U32(n)  DataFixed(cols, &i, b->u32[PSCH_U32_##n], (size_t)rows * 4);
#define PSCH_COL_U64(n)  DataFixed(cols, &i, b->u64[PSCH_U64_##n], (size_t)rows * 8);
#define PSCH_COL_UTF8(n) DataUtf8(cols, &i, &b->utf8[PSCH_U8_##n], rows);
#define PSCH_COL_DICT(n) DataDict(cols, &i, &b->dict[PSCH_DICT_##n], rows);
  PSCH_ARROW_COLUMNS(PSCH_COL)
#undef PSCH_COL_TS
#undef PSCH_COL_I32
#undef PSCH_COL_U32
#undef PSCH_COL_U64
#undef PSCH_COL_UTF8
#undef PSCH_COL_DICT
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

PschArrowBuilder* PschArrowBuilderCreate(const PschArrowBuilderConfig* cfg, char* errbuf,
                                         size_t errlen) {
  PschIpcColSpec specs[PSCH_ARROW_NUM_COLS];
  PschArrowBuilder* b;
  size_t ipc_budget;
  size_t budget;
  int rows, r0;

  if (cfg == NULL) {
    SetErr(errbuf, errlen, "arrow builder: NULL config");
    return NULL;
  }
  b = calloc(1, sizeof(PschArrowBuilder));
  if (b == NULL) {
    SetErr(errbuf, errlen, "arrow builder: out of memory");
    return NULL;
  }

  ipc_budget = cfg->ipc_budget_bytes != 0 ? cfg->ipc_budget_bytes : kDefaultIpcBudgetBytes;
  if (ipc_budget < kMinIpcBudgetBytes)
    ipc_budget = kMinIpcBudgetBytes;
  if (ipc_budget > kMaxIpcBudgetBytes)
    ipc_budget = kMaxIpcBudgetBytes;
  b->ipc_budget_bytes = ipc_budget;

  // Rows are additionally bounded by the estimate-driven flush: each row
  // contributes at least kFixedBytesPerRow to the estimate.
  r0 = cfg->max_rows > 0 ? cfg->max_rows : kMaxRowsCap;
  if ((size_t)r0 > ipc_budget / kFixedBytesPerRow + 1)
    r0 = (int)(ipc_budget / kFixedBytesPerRow + 1);
  if (r0 > kMaxRowsCap)
    r0 = kMaxRowsCap;
  if (r0 < 1)
    r0 = 1;

  budget = cfg->scratch_budget_bytes;  // 0 = no budget check
  rows = r0;
  for (;;) {
    size_t slab_bytes, need;

    BuildSpecs(specs, rows, ipc_budget);
    slab_bytes = CarveAll(b, NULL, rows, ipc_budget);
    need = slab_bytes + PschIpcEmitterEstimateBytes(specs, PSCH_ARROW_NUM_COLS) +
           sizeof(PschArrowBuilder);
    if (budget == 0 || need <= budget) {
      b->slab_bytes = slab_bytes;
      break;
    }
    if (rows <= kMinDegradeRows) {
      if (errbuf != NULL && errlen > 0)
        snprintf(errbuf, errlen,
                 "arrow builder: scratch budget %zu too small (need %zu even at %d rows/batch)",
                 budget, need, rows);
      free(b);
      return NULL;
    }
    rows = rows / 2 > kMinDegradeRows ? rows / 2 : kMinDegradeRows;
  }
  if (rows < r0)
    elog(LOG, "pg_stat_ch: arrow builder degraded to %d rows/batch to fit %zu-byte budget", rows,
         budget);
  b->max_rows = rows;

  b->slab = malloc(b->slab_bytes);
  if (b->slab == NULL) {
    SetErr(errbuf, errlen, "arrow builder: out of memory (column slab)");
    free(b);
    return NULL;
  }
  CarveAll(b, b->slab, rows, ipc_budget);

  b->emitter = PschIpcEmitterCreate(specs, PSCH_ARROW_NUM_COLS, errbuf, errlen);
  if (b->emitter == NULL) {
    free(b->slab);
    free(b);
    return NULL;
  }

  if (cfg->service_version != NULL) {
    size_t len = strlen(cfg->service_version);

    if (len > PSCH_ARROW_ATTR_MAX) {
      elog(WARNING, "pg_stat_ch: service_version truncated to %d bytes", PSCH_ARROW_ATTR_MAX);
      len = PSCH_ARROW_ATTR_MAX;
    }
    memcpy(b->service_version, cfg->service_version, len);
    b->service_version[len] = '\0';
    b->service_version_len = (int)len;
  }
  (void)PschArrowBuilderSetAttributes(b, cfg->extra_attributes);

  b->mem_used = sizeof(PschArrowBuilder) + b->slab_bytes + PschIpcEmitterMemUsed(b->emitter);
  PschArrowBuilderReset(b);
  return b;
}

void PschArrowBuilderDestroy(PschArrowBuilder* b) {
  if (b == NULL)
    return;
  PschIpcEmitterDestroy(b->emitter);
  free(b->slab);
  free(b);
}

bool PschArrowBuilderSetAttributes(PschArrowBuilder* b, const char* extra_attributes) {
  bool seen[PSCH_ATTR_NUM] = {false};
  const char* p;

  if (b == NULL)
    return false;
  for (int k = 0; k < PSCH_ATTR_NUM; k++) {
    b->attr_val[k][0] = '\0';
    b->attr_len[k] = 0;
  }

  p = extra_attributes != NULL ? extra_attributes : "";
  while (*p != '\0') {
    const char* end = strchr(p, ';');
    size_t tok_len = end != NULL ? (size_t)(end - p) : strlen(p);
    const char* colon = memchr(p, ':', tok_len);

    if (colon != NULL) {
      const char* key = p;
      size_t key_len = (size_t)(colon - p);
      const char* val = colon + 1;
      size_t val_len = tok_len - key_len - 1;

      while (key_len > 0 && isspace((unsigned char)key[0]))
        key++, key_len--;
      while (key_len > 0 && isspace((unsigned char)key[key_len - 1]))
        key_len--;
      while (val_len > 0 && isspace((unsigned char)val[0]))
        val++, val_len--;
      while (val_len > 0 && isspace((unsigned char)val[val_len - 1]))
        val_len--;

      if (key_len > 0) {
        for (int k = 0; k < PSCH_ATTR_NUM; k++) {
          if (seen[k] || strlen(kAttrKeys[k]) != key_len || memcmp(kAttrKeys[k], key, key_len) != 0)
            continue;
          if (val_len > PSCH_ARROW_ATTR_MAX) {
            elog(WARNING, "pg_stat_ch: extra_attributes value for \"%s\" truncated to %d bytes",
                 kAttrKeys[k], PSCH_ARROW_ATTR_MAX);
            val_len = PSCH_ARROW_ATTR_MAX;
          }
          memcpy(b->attr_val[k], val, val_len);
          b->attr_val[k][val_len] = '\0';
          b->attr_len[k] = (int)val_len;
          seen[k] = true;  // first occurrence wins, like the old map emplace
          break;
        }
      }
    }
    p = end != NULL ? end + 1 : p + tok_len;
  }
  return true;
}

PschArrowAppendResult PschArrowBuilderAppend(PschArrowBuilder* b, const PschEvent* ev) {
  const char* dval[PSCH_DICT_NUM];
  const char* uval[PSCH_U8_NUM];
  int dlen[PSCH_DICT_NUM];
  int ulen[PSCH_U8_NUM];
  int32 dfound[PSCH_DICT_NUM];
  uint32 dins[PSCH_DICT_NUM];
  char queryid_buf[24];
  char pid_buf[16];
  int row;

  if (b == NULL || ev == NULL)
    return PSCH_ARROW_APPEND_ERR;
  if (b->num_rows >= b->max_rows)
    return PSCH_ARROW_APPEND_FULL;
  // Post-append flush semantics of the old implementation: the row that
  // crosses the budget stays in the batch; the next append flushes.
  if (b->num_rows > 0 && b->estimated_bytes >= b->ipc_budget_bytes)
    return PSCH_ARROW_APPEND_FULL;

  {
    int datname_len =
        ClampFieldLen(ev->datname_len, (unsigned)sizeof(ev->datname) - 1, "datname_len");
    int username_len =
        ClampFieldLen(ev->username_len, (unsigned)sizeof(ev->username) - 1, "username_len");
    int app_len =
        ClampFieldLen(ev->application_name_len, PSCH_MAX_APP_NAME_LEN, "application_name_len");
    int client_addr_len =
        ClampFieldLen(ev->client_addr_len, PSCH_MAX_CLIENT_ADDR_LEN, "client_addr_len");
    int query_len = ClampFieldLen(ev->query_len, PSCH_MAX_QUERY_LEN, "query_len");
    int err_message_len =
        ClampFieldLen(ev->err_message_len, PSCH_MAX_ERR_MSG_LEN, "err_message_len");
    int sqlstate_len = (int)strnlen(ev->err_sqlstate, sizeof(ev->err_sqlstate) - 1);

    snprintf(queryid_buf, sizeof(queryid_buf), UINT64_FORMAT, ev->queryid);
    snprintf(pid_buf, sizeof(pid_buf), "%d", ev->pid);

    dval[PSCH_DICT_severity] = "";
    dlen[PSCH_DICT_severity] = 0;
    dval[PSCH_DICT_query_id] = queryid_buf;
    dlen[PSCH_DICT_query_id] = (int)strlen(queryid_buf);
    dval[PSCH_DICT_db_name] = ev->datname;
    dlen[PSCH_DICT_db_name] = datname_len;
    dval[PSCH_DICT_db_user] = ev->username;
    dlen[PSCH_DICT_db_user] = username_len;
    dval[PSCH_DICT_db_operation] = PschCmdTypeToString(ev->cmd_type);
    dlen[PSCH_DICT_db_operation] = (int)strlen(dval[PSCH_DICT_db_operation]);
    dval[PSCH_DICT_app] = ev->application_name;
    dlen[PSCH_DICT_app] = app_len;
    dval[PSCH_DICT_client_addr] = ev->client_addr;
    dlen[PSCH_DICT_client_addr] = client_addr_len;
    dval[PSCH_DICT_err_sqlstate] = ev->err_sqlstate;
    dlen[PSCH_DICT_err_sqlstate] = sqlstate_len;
    dval[PSCH_DICT_server_role] = b->attr_val[PSCH_ATTR_SERVER_ROLE];
    dlen[PSCH_DICT_server_role] = b->attr_len[PSCH_ATTR_SERVER_ROLE];
    dval[PSCH_DICT_read_replica_type] = b->attr_val[PSCH_ATTR_READ_REPLICA_TYPE];
    dlen[PSCH_DICT_read_replica_type] = b->attr_len[PSCH_ATTR_READ_REPLICA_TYPE];
    dval[PSCH_DICT_region] = b->attr_val[PSCH_ATTR_REGION];
    dlen[PSCH_DICT_region] = b->attr_len[PSCH_ATTR_REGION];
    dval[PSCH_DICT_cell] = b->attr_val[PSCH_ATTR_CELL];
    dlen[PSCH_DICT_cell] = b->attr_len[PSCH_ATTR_CELL];
    dval[PSCH_DICT_service_version] = b->service_version;
    dlen[PSCH_DICT_service_version] = b->service_version_len;

    uval[PSCH_U8_body] = "";
    ulen[PSCH_U8_body] = 0;
    uval[PSCH_U8_trace_id] = "";
    ulen[PSCH_U8_trace_id] = 0;
    uval[PSCH_U8_span_id] = "";
    ulen[PSCH_U8_span_id] = 0;
    uval[PSCH_U8_query_text] = ev->query;
    ulen[PSCH_U8_query_text] = query_len;
    uval[PSCH_U8_pid] = pid_buf;
    ulen[PSCH_U8_pid] = (int)strlen(pid_buf);
    uval[PSCH_U8_err_message] = ev->err_message;
    ulen[PSCH_U8_err_message] = err_message_len;
    uval[PSCH_U8_instance_ubid] = b->attr_val[PSCH_ATTR_INSTANCE_UBID];
    ulen[PSCH_U8_instance_ubid] = b->attr_len[PSCH_ATTR_INSTANCE_UBID];
    uval[PSCH_U8_server_ubid] = b->attr_val[PSCH_ATTR_SERVER_UBID];
    ulen[PSCH_U8_server_ubid] = b->attr_len[PSCH_ATTR_SERVER_UBID];
    uval[PSCH_U8_host_id] = b->attr_val[PSCH_ATTR_HOST_ID];
    ulen[PSCH_U8_host_id] = b->attr_len[PSCH_ATTR_HOST_ID];
    uval[PSCH_U8_pod_name] = b->attr_val[PSCH_ATTR_POD_NAME];
    ulen[PSCH_U8_pod_name] = b->attr_len[PSCH_ATTR_POD_NAME];

    // Capacity checks before any mutation: a row is appended atomically or
    // not at all (the old builder could leave ragged columns on failure).
    for (int u = 0; u < PSCH_U8_NUM; u++) {
      const PschUtf8Col* c = &b->utf8[u];

      if ((size_t)c->offsets[b->num_rows] + (size_t)ulen[u] > c->data_cap)
        goto full;
    }
    for (int k = 0; k < PSCH_DICT_NUM; k++) {
      const PschDictCol* d = &b->dict[k];

      dfound[k] = DictLookup(d, dval[k], dlen[k], &dins[k]);
      if (dfound[k] < 0) {
        if (d->count >= d->max_distinct)
          goto full;
        if ((size_t)d->val_offsets[d->count] + (size_t)dlen[k] > d->val_cap)
          goto full;
      }
    }

    row = b->num_rows;
    b->ts[row] = (ev->ts_start + kPostgresEpochOffsetUs) * 1000;
    b->err_elevel[row] = (int32)ev->err_elevel;

    b->u64[PSCH_U64_duration_us][row] = ev->duration_us;
    b->u64[PSCH_U64_rows][row] = ev->rows;
    b->u64[PSCH_U64_wal_bytes][row] = ev->wal_bytes;
#define PSCH_APPEND_C64(f) b->u64[PSCH_U64_##f][row] = ClampSignedToUint64(ev->f, #f)
    PSCH_APPEND_C64(shared_blks_hit);
    PSCH_APPEND_C64(shared_blks_read);
    PSCH_APPEND_C64(shared_blks_written);
    PSCH_APPEND_C64(shared_blks_dirtied);
    PSCH_APPEND_C64(shared_blk_read_time_us);
    PSCH_APPEND_C64(shared_blk_write_time_us);
    PSCH_APPEND_C64(local_blks_hit);
    PSCH_APPEND_C64(local_blks_read);
    PSCH_APPEND_C64(local_blks_written);
    PSCH_APPEND_C64(local_blks_dirtied);
    PSCH_APPEND_C64(local_blk_read_time_us);
    PSCH_APPEND_C64(local_blk_write_time_us);
    PSCH_APPEND_C64(temp_blks_read);
    PSCH_APPEND_C64(temp_blks_written);
    PSCH_APPEND_C64(temp_blk_read_time_us);
    PSCH_APPEND_C64(temp_blk_write_time_us);
    PSCH_APPEND_C64(wal_records);
    PSCH_APPEND_C64(wal_fpi);
    PSCH_APPEND_C64(cpu_user_time_us);
    PSCH_APPEND_C64(cpu_sys_time_us);
    PSCH_APPEND_C64(jit_functions);
    PSCH_APPEND_C64(jit_generation_time_us);
    PSCH_APPEND_C64(jit_inlining_time_us);
    PSCH_APPEND_C64(jit_optimization_time_us);
    PSCH_APPEND_C64(jit_emission_time_us);
    PSCH_APPEND_C64(jit_deform_time_us);
#undef PSCH_APPEND_C64
    b->u32[PSCH_U32_parallel_workers_planned][row] =
        ClampSignedToUint32(ev->parallel_workers_planned, "parallel_workers_planned");
    b->u32[PSCH_U32_parallel_workers_launched][row] =
        ClampSignedToUint32(ev->parallel_workers_launched, "parallel_workers_launched");

    for (int u = 0; u < PSCH_U8_NUM; u++)
      Utf8Append(&b->utf8[u], row, uval[u], ulen[u]);
    for (int k = 0; k < PSCH_DICT_NUM; k++) {
      PschDictCol* d = &b->dict[k];
      int32 idx = dfound[k] >= 0 ? dfound[k] : DictInsert(d, dins[k], dval[k], dlen[k]);

      d->indices[row] = idx;
    }

    // Exact estimate formula of the old implementation.
    b->estimated_bytes += kFixedBytesPerRow + (size_t)datname_len + (size_t)username_len +
                          (size_t)app_len + (size_t)client_addr_len + (size_t)query_len +
                          (size_t)err_message_len + (size_t)sqlstate_len +
                          (size_t)b->service_version_len;
    for (int k = 0; k < PSCH_ATTR_NUM; k++)
      b->estimated_bytes += (size_t)b->attr_len[k];
    b->num_rows++;
    return PSCH_ARROW_APPEND_OK;
  }

full:
  if (b->num_rows == 0) {
    // Cannot happen with capacities >= one max-size row; guard anyway so a
    // single oversized event cannot wedge the flush loop.
    elog(WARNING, "pg_stat_ch: event exceeds arrow batch capacity, dropping");
    return PSCH_ARROW_APPEND_ERR;
  }
  return PSCH_ARROW_APPEND_FULL;
}

bool PschArrowBuilderFinish(PschArrowBuilder* b, const uint8** data_out, size_t* len_out,
                            int* rows_out) {
  PschIpcColData cols[PSCH_ARROW_NUM_COLS];

  if (data_out != NULL)
    *data_out = NULL;
  if (len_out != NULL)
    *len_out = 0;
  if (rows_out != NULL)
    *rows_out = 0;
  if (b == NULL || data_out == NULL || len_out == NULL || rows_out == NULL)
    return false;
  if (b->num_rows == 0)
    return false;  // nothing to serialize

  FillColData(b, cols);
  if (!PschIpcEmitterEmit(b->emitter, cols, PSCH_ARROW_NUM_COLS, b->num_rows, data_out, len_out))
    return false;
  *rows_out = b->num_rows;
  return true;
}

void PschArrowBuilderReset(PschArrowBuilder* b) {
  if (b == NULL)
    return;
  b->num_rows = 0;
  b->estimated_bytes = kIpcEnvelopeBytes;
  for (int u = 0; u < PSCH_U8_NUM; u++)
    b->utf8[u].offsets[0] = 0;
  for (int k = 0; k < PSCH_DICT_NUM; k++) {
    PschDictCol* d = &b->dict[k];

    // Full reset: every payload carries fresh, self-contained dictionaries
    // (the old builders called ResetFull()).
    d->count = 0;
    d->val_offsets[0] = 0;
    memset(d->memo, 0, (size_t)d->memo_cap * 4);
  }
}

int PschArrowBuilderNumRows(const PschArrowBuilder* b) {
  return b != NULL ? b->num_rows : 0;
}

size_t PschArrowBuilderEstimatedBytes(const PschArrowBuilder* b) {
  return b != NULL ? b->estimated_bytes : 0;
}

uint64 PschArrowBuilderMemUsed(const PschArrowBuilder* b) {
  return b != NULL ? b->mem_used : 0;
}
