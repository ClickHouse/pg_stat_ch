// Arrow IPC streaming emitter: flatcc-built metadata + ZSTD-framed bodies.
//
// Wire format (verified against Arrow C++ / pyarrow output, see
// /tmp/wf1-code-arrow-batch.md):
//  * Every message: 0xFFFFFFFF continuation, int32 LE metadata length
//    (multiple of 8, includes padding), flatbuffer, zero padding, body.
//  * Schema message first (Message.bodyLength omitted, like Arrow C++),
//    then one dictionary batch per DICT column (ids 0..N-1 in field order,
//    isDelta omitted=false), one record batch, 8-byte EOS.
//  * BodyCompression{codec=ZSTD}; method omitted (=BUFFER default).  Every
//    body buffer with size > 0 is unconditionally framed as int64 LE
//    uncompressed length + ZSTD frame (level 1); zero-length buffers are
//    stored raw with length 0.  Buffers start 8-byte aligned, zero padded;
//    bodyLength includes the final padding.
//  * All fields nullable=true, null_count=0, no custom metadata anywhere.
//
// Deviation from Arrow C++ (semantically transparent to flatbuffers
// readers): metadata regions are padded up to a per-message-type constant
// slot so bodies can be compressed directly into the output buffer before
// the metadata (with its buffer lengths) is built; default-valued scalars
// we always store (DictionaryEncoding.id=0, Message.bodyLength) are
// explicitly written.
#include "postgres.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include "flatcc/flatcc_builder.h"

#include "export/arrow_ipc_emit.h"

#ifdef WORDS_BIGENDIAN
#error "pg_stat_ch arrow_ipc_emit assumes a little-endian host"
#endif

// MetadataVersion / MessageHeader / Type-union / TimeUnit / CompressionType
// values verified against the flatcc-generated header bundled in
// third_party/nanoarrow/src/nanoarrow_ipc.c.
#define PSCH_FB_METADATA_V5             ((int16)4)
#define PSCH_FB_HEADER_SCHEMA           ((uint8)1)
#define PSCH_FB_HEADER_DICTIONARY_BATCH ((uint8)2)
#define PSCH_FB_HEADER_RECORD_BATCH     ((uint8)3)
#define PSCH_FB_TYPE_INT                ((uint8)2)
#define PSCH_FB_TYPE_UTF8               ((uint8)5)
#define PSCH_FB_TYPE_TIMESTAMP          ((uint8)10)
#define PSCH_FB_TIMEUNIT_NANOSECOND     ((int16)3)
#define PSCH_FB_COMPRESSION_ZSTD        ((int8)1)

static const size_t kSchemaMsgCap = 16384;
static const size_t kDictMetaCap = 768;
static const size_t kMetaSlack = 64;
static const size_t kVecMaxCount = 1 << 20;
// flatcc builder/emitter pages are warm-up allocated and reused; this is the
// accounting allowance used by both EstimateBytes and MemUsed.
static const size_t kFlatccSlackBytes = 256 * 1024;

typedef struct PschIpcNode {
  int64 length;
  int64 null_count;
} PschIpcNode;

typedef struct PschIpcBufEnt {
  int64 offset;
  int64 length;
} PschIpcBufEnt;

StaticAssertDecl(sizeof(PschIpcNode) == 16, "FieldNode wire struct must be 16 bytes");
StaticAssertDecl(sizeof(PschIpcBufEnt) == 16, "Buffer wire struct must be 16 bytes");

struct PschIpcEmitter {
  int ncols;
  int ndict;
  PschIpcColSpec* specs;

  flatcc_builder_t fb;
  bool fb_ready;

  ZSTD_CCtx* cctx;  // points into cctx_ws (ZSTD_initStaticCCtx)
  void* cctx_ws;
  size_t cctx_ws_bytes;

  uint8* schema_msg;  // complete prebuilt schema message (framing included)
  size_t schema_msg_bytes;

  size_t dict_meta_slot;  // padded metadata region per dictionary message
  size_t rec_meta_slot;   // padded metadata region of the record batch message

  uint8* out;
  size_t out_cap;

  PschIpcNode* nodes;   // scratch: ncols entries
  PschIpcBufEnt* ents;  // scratch: 3 * ncols entries

  uint64 mem_used;
};

static size_t Align8(size_t n) {
  return (n + 7) & ~(size_t)7;
}

static void EmitErr(char* errbuf, size_t errlen, const char* fmt, const char* detail) {
  if (errbuf != NULL && errlen > 0)
    snprintf(errbuf, errlen, fmt, detail);
}

// ---------------------------------------------------------------------------
// Sizing (shared by EstimateBytes and Create so they can never disagree)
// ---------------------------------------------------------------------------

static size_t RecMetaCap(int ncols) {
  return 1024 + (size_t)ncols * 128;
}

static size_t BodyBufCap(size_t src_cap) {
  return src_cap > 0 ? Align8(8 + ZSTD_compressBound(src_cap)) : 0;
}

static size_t DictBodyCap(const PschIpcColSpec* s) {
  return BodyBufCap(s->offsets_cap) + BodyBufCap(s->var_cap);
}

static size_t RecBodyCapForCol(const PschIpcColSpec* s) {
  switch (s->kind) {
    case PSCH_IPC_COL_UTF8:
      return BodyBufCap(s->offsets_cap) + BodyBufCap(s->var_cap);
    case PSCH_IPC_COL_FIXED:
    case PSCH_IPC_COL_DICT:
    default:
      return BodyBufCap(s->data_cap);
  }
}

static size_t MaxCompressSrcCap(const PschIpcColSpec* specs, int ncols) {
  size_t max = 1;
  for (int i = 0; i < ncols; i++) {
    if (specs[i].data_cap > max)
      max = specs[i].data_cap;
    if (specs[i].offsets_cap > max)
      max = specs[i].offsets_cap;
    if (specs[i].var_cap > max)
      max = specs[i].var_cap;
  }
  return max;
}

static size_t OutCapacity(const PschIpcColSpec* specs, int ncols) {
  size_t cap = kSchemaMsgCap;
  for (int i = 0; i < ncols; i++) {
    if (specs[i].kind == PSCH_IPC_COL_DICT)
      cap += 8 + kDictMetaCap + DictBodyCap(&specs[i]);
    cap += RecBodyCapForCol(&specs[i]);
  }
  cap += 8 + RecMetaCap(ncols);
  cap += 8 + 64;  // EOS + slack
  return cap;
}

static size_t ZstdWorkspaceBytes(size_t max_src) {
  ZSTD_compressionParameters cp = ZSTD_getCParams(1, (unsigned long long)max_src, 0);
  size_t est = ZSTD_estimateCCtxSize_usingCParams(cp);
  if (ZSTD_isError(est))
    est = 4 * 1024 * 1024;
  return est + est / 4 + 65536;
}

size_t PschIpcEmitterEstimateBytes(const PschIpcColSpec* specs, int ncols) {
  size_t scratch =
      (size_t)ncols * (sizeof(PschIpcColSpec) + sizeof(PschIpcNode) + 3 * sizeof(PschIpcBufEnt));
  return OutCapacity(specs, ncols) + ZstdWorkspaceBytes(MaxCompressSrcCap(specs, ncols)) +
         kSchemaMsgCap + kFlatccSlackBytes + scratch + 4096;
}

// ---------------------------------------------------------------------------
// flatbuffer metadata construction (flatcc runtime, hand-rolled tables)
// ---------------------------------------------------------------------------

static bool AddScalar(flatcc_builder_t* B, int id, const void* src, size_t size) {
  void* p = flatcc_builder_table_add(B, id, size, (uint16)size);
  if (p == NULL)
    return false;
  memcpy(p, src, size);  // host is little-endian (guarded above)
  return true;
}

static bool AddOffset(flatcc_builder_t* B, int id, flatcc_builder_ref_t ref) {
  flatcc_builder_ref_t* p = flatcc_builder_table_add_offset(B, id);
  if (p == NULL)
    return false;
  *p = ref;
  return true;
}

// Int{bitWidth, is_signed}; is_signed omitted when false (matches Arrow C++).
static flatcc_builder_ref_t BuildIntType(flatcc_builder_t* B, int32 bit_width, bool is_signed) {
  if (flatcc_builder_start_table(B, 2) != 0)
    return 0;
  if (!AddScalar(B, 0, &bit_width, 4))
    return 0;
  if (is_signed) {
    uint8 one = 1;
    if (!AddScalar(B, 1, &one, 1))
      return 0;
  }
  return flatcc_builder_end_table(B);
}

static flatcc_builder_ref_t BuildEmptyTable(flatcc_builder_t* B) {
  if (flatcc_builder_start_table(B, 0) != 0)
    return 0;
  return flatcc_builder_end_table(B);
}

static flatcc_builder_ref_t BuildField(flatcc_builder_t* B, const PschIpcColSpec* spec,
                                       int64 dict_id) {
  uint8 type_type;
  flatcc_builder_ref_t type_ref;
  flatcc_builder_ref_t dict_ref = 0;
  uint8 one = 1;

  flatcc_builder_ref_t name_ref = flatcc_builder_create_string(B, spec->name, strlen(spec->name));
  if (name_ref == 0)
    return 0;

  if (spec->kind == PSCH_IPC_COL_UTF8 || spec->kind == PSCH_IPC_COL_DICT) {
    // Dictionary fields carry the VALUE type (Utf8); indices live in
    // DictionaryEncoding.indexType.
    type_type = PSCH_FB_TYPE_UTF8;
    type_ref = BuildEmptyTable(B);
  } else if (spec->fixed_type == PSCH_IPC_TS_NS_UTC) {
    flatcc_builder_ref_t tz_ref = flatcc_builder_create_string(B, "UTC", 3);
    if (tz_ref == 0)
      return 0;
    type_type = PSCH_FB_TYPE_TIMESTAMP;
    if (flatcc_builder_start_table(B, 2) != 0)
      return 0;
    {
      int16 unit = PSCH_FB_TIMEUNIT_NANOSECOND;
      if (!AddScalar(B, 0, &unit, 2))
        return 0;
      if (!AddOffset(B, 1, tz_ref))
        return 0;
    }
    type_ref = flatcc_builder_end_table(B);
  } else {
    type_type = PSCH_FB_TYPE_INT;
    switch (spec->fixed_type) {
      case PSCH_IPC_INT32:
        type_ref = BuildIntType(B, 32, true);
        break;
      case PSCH_IPC_UINT32:
        type_ref = BuildIntType(B, 32, false);
        break;
      case PSCH_IPC_UINT64:
      default:
        type_ref = BuildIntType(B, 64, false);
        break;
    }
  }
  if (type_ref == 0)
    return 0;

  if (spec->kind == PSCH_IPC_COL_DICT) {
    flatcc_builder_ref_t idx_ref = BuildIntType(B, 32, true);
    if (idx_ref == 0)
      return 0;
    if (flatcc_builder_start_table(B, 2) != 0)
      return 0;
    if (!AddScalar(B, 0, &dict_id, 8))
      return 0;
    if (!AddOffset(B, 1, idx_ref))
      return 0;
    dict_ref = flatcc_builder_end_table(B);  // isOrdered/dictionaryKind omitted (defaults)
    if (dict_ref == 0)
      return 0;
  }

  // Arrow C++ writes an empty (not absent) children vector.
  if (flatcc_builder_start_offset_vector(B) != 0)
    return 0;
  flatcc_builder_ref_t children_ref = flatcc_builder_end_offset_vector(B);
  if (children_ref == 0)
    return 0;

  if (flatcc_builder_start_table(B, 6) != 0)
    return 0;
  if (!AddOffset(B, 0, name_ref))
    return 0;
  if (!AddScalar(B, 1, &one, 1))
    return 0;  // nullable=true on every field
  if (!AddScalar(B, 2, &type_type, 1))
    return 0;
  if (!AddOffset(B, 3, type_ref))
    return 0;
  if (dict_ref != 0 && !AddOffset(B, 4, dict_ref))
    return 0;
  if (!AddOffset(B, 5, children_ref))
    return 0;
  return flatcc_builder_end_table(B);
}

// Message{version=V5, header_type, header, [bodyLength]}.  bodyLength is
// omitted for the schema message (Arrow C++ behavior; it is always 0 there).
static flatcc_builder_ref_t BuildMessageTable(flatcc_builder_t* B, uint8 header_type,
                                              flatcc_builder_ref_t header_ref, int64 body_len,
                                              bool with_body_len) {
  int16 version = PSCH_FB_METADATA_V5;

  if (flatcc_builder_start_table(B, 4) != 0)
    return 0;
  if (!AddScalar(B, 0, &version, 2))
    return 0;
  if (!AddScalar(B, 1, &header_type, 1))
    return 0;
  if (!AddOffset(B, 2, header_ref))
    return 0;
  if (with_body_len && !AddScalar(B, 3, &body_len, 8))
    return 0;
  return flatcc_builder_end_table(B);
}

static flatcc_builder_ref_t BuildStructVector(flatcc_builder_t* B, const void* data, int count) {
  if (flatcc_builder_start_vector(B, 16, 8, kVecMaxCount) != 0)
    return 0;
  void* dst = flatcc_builder_extend_vector(B, (size_t)count);
  if (dst == NULL)
    return 0;
  memcpy(dst, data, (size_t)count * 16);
  return flatcc_builder_end_vector(B);
}

// RecordBatch{length, nodes, buffers, compression=BodyCompression{ZSTD}}.
static flatcc_builder_ref_t BuildRecordBatchTable(flatcc_builder_t* B, int64 length,
                                                  const PschIpcNode* nodes, int nnodes,
                                                  const PschIpcBufEnt* ents, int nents) {
  flatcc_builder_ref_t nodes_ref = BuildStructVector(B, nodes, nnodes);
  if (nodes_ref == 0)
    return 0;
  flatcc_builder_ref_t bufs_ref = BuildStructVector(B, ents, nents);
  if (bufs_ref == 0)
    return 0;

  if (flatcc_builder_start_table(B, 2) != 0)
    return 0;
  {
    int8 codec = PSCH_FB_COMPRESSION_ZSTD;
    if (!AddScalar(B, 0, &codec, 1))
      return 0;  // method omitted (=BUFFER default)
  }
  flatcc_builder_ref_t comp_ref = flatcc_builder_end_table(B);
  if (comp_ref == 0)
    return 0;

  if (flatcc_builder_start_table(B, 4) != 0)
    return 0;
  if (!AddScalar(B, 0, &length, 8))
    return 0;
  if (!AddOffset(B, 1, nodes_ref))
    return 0;
  if (!AddOffset(B, 2, bufs_ref))
    return 0;
  if (!AddOffset(B, 3, comp_ref))
    return 0;
  return flatcc_builder_end_table(B);
}

static bool BuildSchemaMessage(flatcc_builder_t* B, const PschIpcColSpec* specs, int ncols) {
  if (flatcc_builder_start_buffer(B, NULL, 0, 0) != 0)
    return false;

  if (flatcc_builder_start_offset_vector(B) != 0)
    return false;
  int64 dict_id = 0;
  for (int i = 0; i < ncols; i++) {
    int64 id = specs[i].kind == PSCH_IPC_COL_DICT ? dict_id++ : -1;
    flatcc_builder_ref_t f = BuildField(B, &specs[i], id);
    if (f == 0)
      return false;
    if (flatcc_builder_offset_vector_push(B, f) == NULL)
      return false;
  }
  flatcc_builder_ref_t fields_ref = flatcc_builder_end_offset_vector(B);
  if (fields_ref == 0)
    return false;

  // Schema{fields}; endianness omitted (=Little), no features/metadata.
  if (flatcc_builder_start_table(B, 2) != 0)
    return false;
  if (!AddOffset(B, 1, fields_ref))
    return false;
  flatcc_builder_ref_t schema_ref = flatcc_builder_end_table(B);
  if (schema_ref == 0)
    return false;

  flatcc_builder_ref_t msg = BuildMessageTable(B, PSCH_FB_HEADER_SCHEMA, schema_ref, 0, false);
  if (msg == 0)
    return false;
  return flatcc_builder_end_buffer(B, msg) != 0;
}

static bool BuildDictBatchMessage(flatcc_builder_t* B, int64 dict_id, int64 dict_count,
                                  const PschIpcNode* nodes, int nnodes, const PschIpcBufEnt* ents,
                                  int nents, int64 body_len) {
  if (flatcc_builder_start_buffer(B, NULL, 0, 0) != 0)
    return false;

  flatcc_builder_ref_t rb = BuildRecordBatchTable(B, dict_count, nodes, nnodes, ents, nents);
  if (rb == 0)
    return false;

  // DictionaryBatch{id, data}; isDelta omitted (=false).  id is stored even
  // when 0 so all dictionary messages have identical metadata size.
  if (flatcc_builder_start_table(B, 3) != 0)
    return false;
  if (!AddScalar(B, 0, &dict_id, 8))
    return false;
  if (!AddOffset(B, 1, rb))
    return false;
  flatcc_builder_ref_t db = flatcc_builder_end_table(B);
  if (db == 0)
    return false;

  flatcc_builder_ref_t msg =
      BuildMessageTable(B, PSCH_FB_HEADER_DICTIONARY_BATCH, db, body_len, true);
  if (msg == 0)
    return false;
  return flatcc_builder_end_buffer(B, msg) != 0;
}

static bool BuildRecBatchMessage(flatcc_builder_t* B, int64 num_rows, const PschIpcNode* nodes,
                                 int nnodes, const PschIpcBufEnt* ents, int nents, int64 body_len) {
  if (flatcc_builder_start_buffer(B, NULL, 0, 0) != 0)
    return false;
  flatcc_builder_ref_t rb = BuildRecordBatchTable(B, num_rows, nodes, nnodes, ents, nents);
  if (rb == 0)
    return false;
  flatcc_builder_ref_t msg = BuildMessageTable(B, PSCH_FB_HEADER_RECORD_BATCH, rb, body_len, true);
  if (msg == 0)
    return false;
  return flatcc_builder_end_buffer(B, msg) != 0;
}

// ---------------------------------------------------------------------------
// Message emission
// ---------------------------------------------------------------------------

static void WriteFraming(uint8* dst, size_t meta_len) {
  int32 cont = -1;
  int32 len32 = (int32)meta_len;
  memcpy(dst, &cont, 4);
  memcpy(dst + 4, &len32, 4);
}

// Copies the finished flatbuffer into the fixed metadata slot at meta_off+8,
// zero-pads the slot remainder, writes framing, and resets the builder.
static bool FinalizeMetaIntoSlot(PschIpcEmitter* e, size_t meta_off, size_t slot) {
  size_t size = flatcc_builder_get_buffer_size(&e->fb);

  if (size == 0 || size > slot || meta_off + 8 + slot > e->out_cap) {
    elog(WARNING, "pg_stat_ch: Arrow IPC metadata size %zu exceeds slot %zu", size, slot);
    flatcc_builder_reset(&e->fb);
    return false;
  }
  if (flatcc_builder_copy_buffer(&e->fb, e->out + meta_off + 8, size) == NULL) {
    elog(WARNING, "pg_stat_ch: Arrow IPC metadata copy failed");
    flatcc_builder_reset(&e->fb);
    return false;
  }
  memset(e->out + meta_off + 8 + size, 0, slot - size);
  WriteFraming(e->out + meta_off, slot);
  flatcc_builder_reset(&e->fb);
  return true;
}

// Appends one body buffer at *used (relative to body_base, 8-aligned) and
// records its Buffer entry.  size==0 buffers are stored raw with length 0
// and do not advance the cursor (Arrow C++ behavior).
static bool EmitBodyBuffer(PschIpcEmitter* e, size_t body_base, size_t* used, const void* src,
                           size_t src_len, int* nents) {
  PschIpcBufEnt* ent = &e->ents[*nents];
  size_t off = *used;

  ent->offset = (int64)off;
  ent->length = 0;
  (*nents)++;
  if (src_len == 0)
    return true;

  size_t abs = body_base + off;
  if (abs + 8 > e->out_cap) {
    elog(WARNING, "pg_stat_ch: Arrow IPC output buffer overflow (body prefix)");
    return false;
  }
  {
    int64 unc = (int64)src_len;
    memcpy(e->out + abs, &unc, 8);
  }
  size_t r = ZSTD_compress2(e->cctx, e->out + abs + 8, e->out_cap - abs - 8, src, src_len);
  if (ZSTD_isError(r)) {
    elog(WARNING, "pg_stat_ch: ZSTD buffer compression failed: %s", ZSTD_getErrorName(r));
    return false;
  }

  size_t len = 8 + r;
  size_t new_used = Align8(off + len);
  if (body_base + new_used > e->out_cap) {
    elog(WARNING, "pg_stat_ch: Arrow IPC output buffer overflow (body padding)");
    return false;
  }
  memset(e->out + abs + len, 0, new_used - (off + len));
  ent->length = (int64)len;
  *used = new_used;
  return true;
}

static bool EmitDictMessage(PschIpcEmitter* e, const PschIpcColData* col, int64 dict_id,
                            size_t* pos) {
  size_t meta_off = *pos;
  size_t slot = e->dict_meta_slot;
  size_t body_base = meta_off + 8 + slot;
  size_t used = 0;
  int nents = 0;

  if (body_base > e->out_cap) {
    elog(WARNING, "pg_stat_ch: Arrow IPC output buffer overflow (dictionary metadata)");
    return false;
  }
  if (!EmitBodyBuffer(e, body_base, &used, NULL, 0, &nents))
    return false;  // validity
  if (!EmitBodyBuffer(e, body_base, &used, col->offsets, (size_t)(col->dict_count + 1) * 4, &nents))
    return false;
  if (!EmitBodyBuffer(e, body_base, &used, col->var_data, col->var_len, &nents))
    return false;

  e->nodes[0].length = col->dict_count;
  e->nodes[0].null_count = 0;
  if (!BuildDictBatchMessage(&e->fb, dict_id, col->dict_count, e->nodes, 1, e->ents, nents,
                             (int64)used)) {
    elog(WARNING, "pg_stat_ch: Arrow IPC dictionary metadata build failed");
    flatcc_builder_reset(&e->fb);
    return false;
  }
  if (!FinalizeMetaIntoSlot(e, meta_off, slot))
    return false;
  *pos = body_base + used;
  return true;
}

static bool EmitRecordBatchMessage(PschIpcEmitter* e, const PschIpcColData* cols, int64 num_rows,
                                   size_t* pos) {
  size_t meta_off = *pos;
  size_t slot = e->rec_meta_slot;
  size_t body_base = meta_off + 8 + slot;
  size_t used = 0;
  int nents = 0;

  if (body_base > e->out_cap) {
    elog(WARNING, "pg_stat_ch: Arrow IPC output buffer overflow (record batch metadata)");
    return false;
  }
  for (int i = 0; i < e->ncols; i++) {
    const PschIpcColData* c = &cols[i];

    e->nodes[i].length = num_rows;
    e->nodes[i].null_count = 0;
    if (!EmitBodyBuffer(e, body_base, &used, NULL, 0, &nents))
      return false;  // validity
    switch (e->specs[i].kind) {
      case PSCH_IPC_COL_UTF8:
        if (!EmitBodyBuffer(e, body_base, &used, c->offsets, (size_t)(num_rows + 1) * 4, &nents))
          return false;
        if (!EmitBodyBuffer(e, body_base, &used, c->var_data, c->var_len, &nents))
          return false;
        break;
      case PSCH_IPC_COL_FIXED:
      case PSCH_IPC_COL_DICT:
      default:
        if (!EmitBodyBuffer(e, body_base, &used, c->data, c->data_len, &nents))
          return false;
        break;
    }
  }

  if (!BuildRecBatchMessage(&e->fb, num_rows, e->nodes, e->ncols, e->ents, nents, (int64)used)) {
    elog(WARNING, "pg_stat_ch: Arrow IPC record batch metadata build failed");
    flatcc_builder_reset(&e->fb);
    return false;
  }
  if (!FinalizeMetaIntoSlot(e, meta_off, slot))
    return false;
  *pos = body_base + used;
  return true;
}

bool PschIpcEmitterEmit(PschIpcEmitter* e, const PschIpcColData* cols, int ncols, int64 num_rows,
                        const uint8** out_data, size_t* out_len) {
  if (e == NULL || cols == NULL || out_data == NULL || out_len == NULL)
    return false;
  if (ncols != e->ncols || num_rows <= 0)
    return false;

  // A longjmp may have interrupted a previous Emit between flatcc calls.
  flatcc_builder_reset(&e->fb);

  size_t pos = 0;
  memcpy(e->out, e->schema_msg, e->schema_msg_bytes);
  pos += e->schema_msg_bytes;

  int64 dict_id = 0;
  for (int i = 0; i < e->ncols; i++) {
    if (e->specs[i].kind != PSCH_IPC_COL_DICT)
      continue;
    if (!EmitDictMessage(e, &cols[i], dict_id, &pos))
      return false;
    dict_id++;
  }
  if (!EmitRecordBatchMessage(e, cols, num_rows, &pos))
    return false;

  if (pos + 8 > e->out_cap) {
    elog(WARNING, "pg_stat_ch: Arrow IPC output buffer overflow (EOS)");
    return false;
  }
  WriteFraming(e->out + pos, 0);
  pos += 8;

  *out_data = e->out;
  *out_len = pos;
  return true;
}

// ---------------------------------------------------------------------------
// Create / Destroy
// ---------------------------------------------------------------------------

// Builds the schema message once into an exact-sized owned buffer.
static bool CreateSchemaMessage(PschIpcEmitter* e, char* errbuf, size_t errlen) {
  if (!BuildSchemaMessage(&e->fb, e->specs, e->ncols)) {
    flatcc_builder_reset(&e->fb);
    EmitErr(errbuf, errlen, "schema flatbuffer build failed%s", "");
    return false;
  }
  size_t size = flatcc_builder_get_buffer_size(&e->fb);
  size_t padded = Align8(size);
  if (size == 0 || 8 + padded > kSchemaMsgCap) {
    flatcc_builder_reset(&e->fb);
    EmitErr(errbuf, errlen, "schema message exceeds reserved capacity%s", "");
    return false;
  }
  e->schema_msg = malloc(8 + padded);
  if (e->schema_msg == NULL) {
    flatcc_builder_reset(&e->fb);
    EmitErr(errbuf, errlen, "out of memory (schema message)%s", "");
    return false;
  }
  WriteFraming(e->schema_msg, padded);
  if (flatcc_builder_copy_buffer(&e->fb, e->schema_msg + 8, size) == NULL) {
    flatcc_builder_reset(&e->fb);
    EmitErr(errbuf, errlen, "schema flatbuffer copy failed%s", "");
    return false;
  }
  memset(e->schema_msg + 8 + size, 0, padded - size);
  e->schema_msg_bytes = 8 + padded;
  flatcc_builder_reset(&e->fb);
  return true;
}

// Determines the fixed metadata slot sizes by building structurally
// identical template messages (metadata size depends only on node/buffer
// counts, not values), and warms up the flatcc builder pages in the
// process so Emit() never grows them.
static bool ComputeMetaSlots(PschIpcEmitter* e, char* errbuf, size_t errlen) {
  if (e->ndict > 0) {
    PschIpcNode node = {1, 0};
    PschIpcBufEnt ents[3] = {{0, 0}, {0, 16}, {16, 16}};

    if (!BuildDictBatchMessage(&e->fb, (int64)e->ndict - 1, 1, &node, 1, ents, 3, 32)) {
      flatcc_builder_reset(&e->fb);
      EmitErr(errbuf, errlen, "dictionary metadata template build failed%s", "");
      return false;
    }
    e->dict_meta_slot = Align8(flatcc_builder_get_buffer_size(&e->fb) + kMetaSlack);
    flatcc_builder_reset(&e->fb);
    if (e->dict_meta_slot > kDictMetaCap) {
      EmitErr(errbuf, errlen, "dictionary metadata exceeds reserved capacity%s", "");
      return false;
    }
  }

  {
    int nents = 0;

    for (int i = 0; i < e->ncols; i++) {
      e->nodes[i].length = 1;
      e->nodes[i].null_count = 0;
      nents += e->specs[i].kind == PSCH_IPC_COL_UTF8 ? 3 : 2;
    }
    for (int i = 0; i < nents; i++) {
      e->ents[i].offset = 0;
      e->ents[i].length = 0;
    }
    if (!BuildRecBatchMessage(&e->fb, 1, e->nodes, e->ncols, e->ents, nents, 0)) {
      flatcc_builder_reset(&e->fb);
      EmitErr(errbuf, errlen, "record batch metadata template build failed%s", "");
      return false;
    }
    e->rec_meta_slot = Align8(flatcc_builder_get_buffer_size(&e->fb) + kMetaSlack);
    flatcc_builder_reset(&e->fb);
    if (e->rec_meta_slot > RecMetaCap(e->ncols)) {
      EmitErr(errbuf, errlen, "record batch metadata exceeds reserved capacity%s", "");
      return false;
    }
  }
  return true;
}

// Static-workspace CCtx + verify-at-start compression of a max-size input.
// If the estimate proves too small, degrade windowLog (with a LOG) rather
// than risking hot-path failure.
static bool CreateZstd(PschIpcEmitter* e, size_t max_src, char* errbuf, size_t errlen) {
  e->cctx_ws_bytes = ZstdWorkspaceBytes(max_src);
  e->cctx_ws = malloc(e->cctx_ws_bytes);
  if (e->cctx_ws == NULL) {
    EmitErr(errbuf, errlen, "out of memory (ZSTD workspace)%s", "");
    return false;
  }
  e->cctx = ZSTD_initStaticCCtx(e->cctx_ws, e->cctx_ws_bytes);
  if (e->cctx == NULL) {
    EmitErr(errbuf, errlen, "ZSTD_initStaticCCtx failed%s", "");
    return false;
  }
  {
    size_t rc = ZSTD_CCtx_setParameter(e->cctx, ZSTD_c_compressionLevel, 1);
    if (ZSTD_isError(rc)) {
      EmitErr(errbuf, errlen, "ZSTD level: %s", ZSTD_getErrorName(rc));
      return false;
    }
  }

  void* src = calloc(1, max_src);
  void* dst = malloc(ZSTD_compressBound(max_src));
  bool ok = false;

  if (src != NULL && dst != NULL) {
    size_t rc = ZSTD_compress2(e->cctx, dst, ZSTD_compressBound(max_src), src, max_src);

    if (ZSTD_isError(rc)) {
      ZSTD_compressionParameters cp = ZSTD_getCParams(1, (unsigned long long)max_src, 0);
      int wlog = (int)cp.windowLog - 1;

      for (; wlog >= ZSTD_WINDOWLOG_MIN; wlog--) {
        if (ZSTD_isError(ZSTD_CCtx_setParameter(e->cctx, ZSTD_c_windowLog, wlog)))
          break;
        rc = ZSTD_compress2(e->cctx, dst, ZSTD_compressBound(max_src), src, max_src);
        if (!ZSTD_isError(rc)) {
          elog(LOG, "pg_stat_ch: ZSTD workspace verify degraded windowLog to %d", wlog);
          break;
        }
      }
    }
    if (!ZSTD_isError(rc))
      ok = true;
    if (!ok)
      EmitErr(errbuf, errlen, "ZSTD verify compression failed: %s", ZSTD_getErrorName(rc));
  } else {
    EmitErr(errbuf, errlen, "out of memory (ZSTD verify buffers)%s", "");
  }
  free(src);
  free(dst);
  return ok;
}

PschIpcEmitter* PschIpcEmitterCreate(const PschIpcColSpec* specs, int ncols, char* errbuf,
                                     size_t errlen) {
  if (specs == NULL || ncols <= 0 || ncols > 1024) {
    EmitErr(errbuf, errlen, "invalid emitter column specs%s", "");
    return NULL;
  }

  PschIpcEmitter* e = calloc(1, sizeof(PschIpcEmitter));
  if (e == NULL) {
    EmitErr(errbuf, errlen, "out of memory (emitter)%s", "");
    return NULL;
  }
  e->ncols = ncols;

  e->specs = malloc((size_t)ncols * sizeof(PschIpcColSpec));
  e->nodes = malloc((size_t)ncols * sizeof(PschIpcNode));
  e->ents = malloc((size_t)ncols * 3 * sizeof(PschIpcBufEnt));
  if (e->specs == NULL || e->nodes == NULL || e->ents == NULL) {
    EmitErr(errbuf, errlen, "out of memory (emitter scratch)%s", "");
    goto fail;
  }
  memcpy(e->specs, specs, (size_t)ncols * sizeof(PschIpcColSpec));
  for (int i = 0; i < ncols; i++) {
    if (e->specs[i].kind == PSCH_IPC_COL_DICT)
      e->ndict++;
  }

  if (flatcc_builder_init(&e->fb) != 0) {
    EmitErr(errbuf, errlen, "flatcc builder init failed%s", "");
    goto fail;
  }
  e->fb_ready = true;

  if (!CreateSchemaMessage(e, errbuf, errlen))
    goto fail;
  if (!ComputeMetaSlots(e, errbuf, errlen))
    goto fail;
  if (!CreateZstd(e, MaxCompressSrcCap(e->specs, ncols), errbuf, errlen))
    goto fail;

  e->out_cap = OutCapacity(e->specs, ncols);
  e->out = malloc(e->out_cap);
  if (e->out == NULL) {
    EmitErr(errbuf, errlen, "out of memory (IPC output buffer)%s", "");
    goto fail;
  }

  e->mem_used = sizeof(PschIpcEmitter) + (uint64)ncols * sizeof(PschIpcColSpec) +
                (uint64)ncols * sizeof(PschIpcNode) + (uint64)ncols * 3 * sizeof(PschIpcBufEnt) +
                e->schema_msg_bytes + e->cctx_ws_bytes + e->out_cap + kFlatccSlackBytes;
  return e;

fail:
  PschIpcEmitterDestroy(e);
  return NULL;
}

void PschIpcEmitterDestroy(PschIpcEmitter* e) {
  if (e == NULL)
    return;
  if (e->fb_ready)
    flatcc_builder_clear(&e->fb);
  free(e->cctx_ws);  // cctx lives inside the workspace
  free(e->schema_msg);
  free(e->out);
  free(e->nodes);
  free(e->ents);
  free(e->specs);
  free(e);
}

uint64 PschIpcEmitterMemUsed(const PschIpcEmitter* e) {
  return e != NULL ? e->mem_used : 0;
}
