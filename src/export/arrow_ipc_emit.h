// Internal Arrow IPC streaming-format emitter used by arrow_batch.c.
//
// nanoarrow's ArrowIpcEncoder can encode neither dictionary batches nor
// compressed buffers (see /tmp/wf1-research-nanoarrow.md), so this module
// emits the encapsulated messages itself: flatbuffer metadata is built with
// the flatcc runtime bundled with the vendored nanoarrow
// (third_party/nanoarrow/src/flatcc.c), bodies are ZSTD-framed per buffer
// (int64-LE uncompressed-length prefix + ZSTD frame, level 1, no -1 raw
// escape; zero-length buffers stay raw, matching Arrow C++).
//
// Memory model: Create() preallocates the output buffer, the ZSTD CCtx
// (static workspace), and warms up the flatcc builder pages by building
// worst-case template messages.  Emit() performs zero heap allocation.
// PschIpcEmitterEstimateBytes() is the pure sizing oracle Create() also
// allocates by, so callers can budget-fit specs before creating.
#ifndef PG_STAT_CH_ARROW_IPC_EMIT_H
#define PG_STAT_CH_ARROW_IPC_EMIT_H

typedef enum PschIpcColKind {
  PSCH_IPC_COL_FIXED = 0,  // body: validity + fixed-width data
  PSCH_IPC_COL_UTF8,       // body: validity + int32 offsets + data
  PSCH_IPC_COL_DICT,       // body: validity + int32 indices; plus one dictionary batch
} PschIpcColKind;

typedef enum PschIpcFixedType {
  PSCH_IPC_TS_NS_UTC = 0,  // timestamp[ns, tz="UTC"], 8 bytes
  PSCH_IPC_INT32,
  PSCH_IPC_UINT32,
  PSCH_IPC_UINT64,
} PschIpcFixedType;

// Capacities are worst-case uncompressed byte counts; they size the output
// buffer and the ZSTD workspace.  `name` must outlive the emitter (string
// literals in practice).
typedef struct PschIpcColSpec {
  const char* name;
  PschIpcColKind kind;
  PschIpcFixedType fixed_type;  // FIXED only
  size_t data_cap;              // FIXED: value bytes; DICT: int32 index bytes
  size_t offsets_cap;           // UTF8: row offsets bytes; DICT: value offsets bytes
  size_t var_cap;               // UTF8: data bytes; DICT: dictionary value bytes
} PschIpcColSpec;

// Per-column views for one Emit() call.  Lengths must not exceed the
// corresponding spec capacities.
typedef struct PschIpcColData {
  const void* data;      // FIXED values / DICT indices
  size_t data_len;       // bytes
  const int32* offsets;  // UTF8: rows+1 entries; DICT: dict_count+1 entries
  const void* var_data;  // UTF8 data / DICT value bytes
  size_t var_len;        // bytes
  int64 dict_count;      // DICT only: number of dictionary values
} PschIpcColData;

typedef struct PschIpcEmitter PschIpcEmitter;

// Total bytes Create() will preallocate for these specs (output buffer +
// ZSTD workspace + schema message + flatcc page slack).  Pure function.
extern size_t PschIpcEmitterEstimateBytes(const PschIpcColSpec* specs, int ncols);

// Preallocates everything (including the prebuilt schema message and
// metadata-size templates).  Returns NULL with a reason in errbuf on
// failure.  Dictionary ids are assigned 0..N-1 to DICT columns in spec
// order, both in the schema and in the emitted dictionary batches.
extern PschIpcEmitter* PschIpcEmitterCreate(const PschIpcColSpec* specs, int ncols, char* errbuf,
                                            size_t errlen);
extern void PschIpcEmitterDestroy(PschIpcEmitter* e);

extern uint64 PschIpcEmitterMemUsed(const PschIpcEmitter* e);

// Serializes one complete IPC stream payload: schema message, one
// dictionary batch per DICT column (isDelta=false), one record batch with
// BodyCompression{ZSTD, BUFFER}, 8-byte EOS.  *out_data points into
// emitter-owned memory, valid until the next Emit/Destroy.  Returns false
// on encode failure (logs WARNING; never ereport(ERROR)).
extern bool PschIpcEmitterEmit(PschIpcEmitter* e, const PschIpcColData* cols, int ncols,
                               int64 num_rows, const uint8** out_data, size_t* out_len);

#endif  // PG_STAT_CH_ARROW_IPC_EMIT_H
