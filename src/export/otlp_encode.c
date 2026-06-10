// Protobuf wire encoder/decoder backing the OTLP/HTTP exporter (otlp_encode.h).
//
// Writer discipline: every emit computes its full byte cost before touching
// the buffer, so an overflowing write leaves the buffer byte-for-byte
// untouched and sets `overflow` exactly once; after that every write
// (including PschPbMsgEnd's slot patch) is a no-op.  Nothing here allocates,
// ereports, or longjmps — safe on the export hot path.
//
// Host-test escape hatch: test/unit/otlp_encode_test.c defines
// PSCH_OTLP_ENCODE_HOST_TEST, supplies the minimal PG type surface
// (uint8/uint32/uint64/int32/int64/bool/Assert), and #includes this file
// directly so the encoder is unit-testable without a server build.  In
// extension builds postgres.h stays the first include.
#ifndef PSCH_OTLP_ENCODE_HOST_TEST
#include "postgres.h"
#endif

#include <string.h>

#include "otlp_encode.h"

enum {
  kPbWireVarint = 0,
  kPbWireFixed64 = 1,
  kPbWireLen = 2,
  kPbWireFixed32 = 5,
  // 3/4 are the deprecated group markers (illegal in proto3), 6/7 are unassigned.
};

// Largest message length representable in the 4-byte padded varint slot
// (2^28 - 1).  PSCH_EXPORT_ENCODE_CEILING is 16 MiB, so a real encode can
// never get close; the check in PschPbMsgEnd is pure defense.
static const size_t kPbSlotMaxLen = 0x0FFFFFFF;

// ---------------------------------------------------------------------------
// Debug-only Begin/End balance + depth guard.
//
// PschPbBuf carries no depth state (single-pass encoding needs none), so the
// guard keys file-local state to the buffer pointer.  Marks from an encode
// abandoned by a longjmp self-heal: live ancestor slots are strictly
// increasing, so any recorded slot >= the new slot must be stale and is
// popped before the depth Assert.  All bookkeeping is skipped once
// b->overflow is set — Begin/End are no-ops then, and tracking them would
// desynchronize the mark stack.
// ---------------------------------------------------------------------------
#ifdef USE_ASSERT_CHECKING

static const PschPbBuf* pb_guard_buf = NULL;
static int pb_guard_depth = 0;
static size_t pb_guard_slots[PSCH_PB_MAX_DEPTH];

static void PbGuardBegin(const PschPbBuf* b, size_t slot) {
  if (b != pb_guard_buf) {
    pb_guard_buf = b;
    pb_guard_depth = 0;
  }
  while (pb_guard_depth > 0 && pb_guard_slots[pb_guard_depth - 1] >= slot)
    pb_guard_depth--;
  Assert(pb_guard_depth < PSCH_PB_MAX_DEPTH);
  if (pb_guard_depth < PSCH_PB_MAX_DEPTH)
    pb_guard_slots[pb_guard_depth++] = slot;
}

static void PbGuardEnd(const PschPbBuf* b, size_t slot) {
  if (b != pb_guard_buf)
    return;
  Assert(pb_guard_depth > 0);
  if (pb_guard_depth > 0) {
    Assert(pb_guard_slots[pb_guard_depth - 1] == slot);
    pb_guard_depth--;
  }
}

#else

#define PbGuardBegin(b, slot) ((void)0)
#define PbGuardEnd(b, slot)   ((void)0)

#endif  // USE_ASSERT_CHECKING

// ---------------------------------------------------------------------------
// Writer primitives
// ---------------------------------------------------------------------------

static inline size_t PbVarintLen(uint64 value) {
  size_t n = 1;
  while (value >= 0x80) {
    value >>= 7;
    n++;
  }
  return n;
}

// Sets the sticky overflow flag when `need` bytes do not fit; the caller
// must not have written anything yet (all-or-nothing per emit).
static inline bool PbEnsure(PschPbBuf* b, size_t need) {
  if (b->overflow)
    return false;
  if (b->cap - b->len < need) {
    b->overflow = true;
    return false;
  }
  return true;
}

// Space must have been ensured by the caller.
static inline void PbPutVarint(PschPbBuf* b, uint64 value) {
  while (value >= 0x80) {
    b->data[b->len++] = (uint8)(value | 0x80);
    value >>= 7;
  }
  b->data[b->len++] = (uint8)value;
}

static inline uint64 PbTag(uint32 field, uint32 wire_type) {
  return ((uint64)field << 3) | wire_type;
}

void PschPbVarint(PschPbBuf* b, uint32 field, uint64 value) {
  uint64 tag = PbTag(field, kPbWireVarint);

  if (!PbEnsure(b, PbVarintLen(tag) + PbVarintLen(value)))
    return;
  PbPutVarint(b, tag);
  PbPutVarint(b, value);
}

void PschPbFixed64(PschPbBuf* b, uint32 field, uint64 value) {
  uint64 tag = PbTag(field, kPbWireFixed64);

  if (!PbEnsure(b, PbVarintLen(tag) + 8))
    return;
  PbPutVarint(b, tag);
  for (int i = 0; i < 8; i++)
    b->data[b->len++] = (uint8)(value >> (8 * i));
}

void PschPbFixed32(PschPbBuf* b, uint32 field, uint32 value) {
  uint64 tag = PbTag(field, kPbWireFixed32);

  if (!PbEnsure(b, PbVarintLen(tag) + 4))
    return;
  PbPutVarint(b, tag);
  for (int i = 0; i < 4; i++)
    b->data[b->len++] = (uint8)(value >> (8 * i));
}

void PschPbDouble(PschPbBuf* b, uint32 field, double value) {
  // IEEE-754 bit image, emitted little-endian (protobuf wire type 1).
  uint64 bits;

  memcpy(&bits, &value, sizeof(bits));
  PschPbFixed64(b, field, bits);
}

void PschPbBytes(PschPbBuf* b, uint32 field, const void* data, size_t len) {
  uint64 tag = PbTag(field, kPbWireLen);

  if (!PbEnsure(b, PbVarintLen(tag) + PbVarintLen(len) + len))
    return;
  PbPutVarint(b, tag);
  PbPutVarint(b, len);
  if (len > 0) {
    memcpy(b->data + b->len, data, len);
    b->len += len;
  }
}

void PschPbString(PschPbBuf* b, uint32 field, const char* s, size_t len) {
  PschPbBytes(b, field, s, len);
}

size_t PschPbMsgBegin(PschPbBuf* b, uint32 field) {
  uint64 tag = PbTag(field, kPbWireLen);
  size_t slot;

  // On overflow the returned slot is degenerate; the matching MsgEnd is a
  // no-op, so it never gets dereferenced.
  if (!PbEnsure(b, PbVarintLen(tag) + 4))
    return b->len;
  PbPutVarint(b, tag);
  slot = b->len;
  memset(b->data + slot, 0, 4);
  b->len += 4;
  PbGuardBegin(b, slot);
  return slot;
}

void PschPbMsgEnd(PschPbBuf* b, size_t slot) {
  size_t msg_len;

  if (b->overflow)
    return;
  PbGuardEnd(b, slot);
  if (slot + 4 > b->len) {
    // Garbage/unbalanced slot: programmer error.  Refuse to scribble and flag
    // overflow so the caller drops the batch.  Deliberately NOT Assert(false):
    // this runs on the bgworker, where a cassert/regression build would turn an
    // encoder bug into SIGABRT -> database-wide crash recovery (the exact blast
    // radius this rewrite exists to eliminate).
    b->overflow = true;
    return;
  }
  msg_len = b->len - slot - 4;
  if (msg_len > kPbSlotMaxLen) {
    b->overflow = true;
    return;
  }
  // Overlong 4-byte varint: zero-padded with continuation bits, final byte
  // clear.  Legal protobuf; accepted by all conformant parsers.
  b->data[slot] = (uint8)(msg_len & 0x7F) | 0x80;
  b->data[slot + 1] = (uint8)((msg_len >> 7) & 0x7F) | 0x80;
  b->data[slot + 2] = (uint8)((msg_len >> 14) & 0x7F) | 0x80;
  b->data[slot + 3] = (uint8)((msg_len >> 21) & 0x7F);
}

// ---------------------------------------------------------------------------
// OTLP common.v1 helpers
// ---------------------------------------------------------------------------

void PschPbKvString(PschPbBuf* b, uint32 field, const char* key, const char* val, size_t val_len) {
  size_t kv = PschPbMsgBegin(b, field);
  size_t any;

  PschPbString(b, PSCH_OTLP_KV_KEY, key, strlen(key));
  any = PschPbMsgBegin(b, PSCH_OTLP_KV_VALUE);
  PschPbString(b, PSCH_OTLP_ANY_STRING, val, val_len);
  PschPbMsgEnd(b, any);
  PschPbMsgEnd(b, kv);
}

void PschPbKvInt(PschPbBuf* b, uint32 field, const char* key, int64 value) {
  size_t kv = PschPbMsgBegin(b, field);
  size_t any;

  PschPbString(b, PSCH_OTLP_KV_KEY, key, strlen(key));
  any = PschPbMsgBegin(b, PSCH_OTLP_KV_VALUE);
  // proto3 int64: two's complement varint (10 bytes when negative), not zigzag.
  PschPbVarint(b, PSCH_OTLP_ANY_INT, (uint64)value);
  PschPbMsgEnd(b, any);
  PschPbMsgEnd(b, kv);
}

void PschPbKvDouble(PschPbBuf* b, uint32 field, const char* key, double value) {
  size_t kv = PschPbMsgBegin(b, field);
  size_t any;

  PschPbString(b, PSCH_OTLP_KV_KEY, key, strlen(key));
  any = PschPbMsgBegin(b, PSCH_OTLP_KV_VALUE);
  PschPbDouble(b, PSCH_OTLP_ANY_DOUBLE, value);
  PschPbMsgEnd(b, any);
  PschPbMsgEnd(b, kv);
}

// ---------------------------------------------------------------------------
// Response parsing.  Bounds discipline: every read checks remaining bytes
// first; a parse that would over-read returns false instead.  Unknown fields
// of all four live wire types are skipped; group markers (3/4) and invalid
// wire types (6/7) reject the input, as does field number 0.
// ---------------------------------------------------------------------------

typedef struct PbReader {
  const uint8* p;
  const uint8* end;
} PbReader;

static bool PbReadVarint(PbReader* r, uint64* value_out) {
  uint64 value = 0;

  for (int i = 0; i < 10; i++) {
    uint8 byte;

    if (r->p >= r->end)
      return false;
    byte = *r->p++;
    value |= (uint64)(byte & 0x7F) << (7 * i);
    if ((byte & 0x80) == 0) {
      *value_out = value;
      return true;
    }
  }
  return false;  // continuation bit set past 10 bytes: malformed
}

static bool PbSkipField(PbReader* r, uint32 wire_type) {
  uint64 skip;

  switch (wire_type) {
    case kPbWireVarint:
      return PbReadVarint(r, &skip);
    case kPbWireFixed64:
      if ((size_t)(r->end - r->p) < 8)
        return false;
      r->p += 8;
      return true;
    case kPbWireLen:
      if (!PbReadVarint(r, &skip))
        return false;
      if (skip > (uint64)(r->end - r->p))
        return false;
      r->p += skip;
      return true;
    case kPbWireFixed32:
      if ((size_t)(r->end - r->p) < 4)
        return false;
      r->p += 4;
      return true;
    default:
      return false;
  }
}

// Truncating copy into the caller's fixed message buffer; always
// NUL-terminates when there is room for anything at all.
static void PbCopyMsg(char* msgbuf, size_t msglen, const uint8* src, size_t n) {
  size_t ncopy;

  if (msgbuf == NULL || msglen == 0)
    return;
  ncopy = n < msglen - 1 ? n : msglen - 1;
  if (ncopy > 0)
    memcpy(msgbuf, src, ncopy);
  msgbuf[ncopy] = '\0';
}

// ExportLogsPartialSuccess{ int64=1, string=2 } and google.rpc.Status
// { int32=1, string=2 } share this wire shape; proto3 last-one-wins applies
// to repeated occurrences of a scalar field.
static bool PbParseNumStrMsg(const uint8* data, size_t len, uint64* num_out, char* msgbuf,
                             size_t msglen) {
  PbReader r;

  r.p = data;
  r.end = data + len;
  while (r.p < r.end) {
    uint64 tag;
    uint64 field;
    uint32 wire_type;

    if (!PbReadVarint(&r, &tag))
      return false;
    field = tag >> 3;
    wire_type = (uint32)(tag & 7);
    if (field == 0)
      return false;
    if (field == 1 && wire_type == kPbWireVarint) {
      if (!PbReadVarint(&r, num_out))
        return false;
    } else if (field == 2 && wire_type == kPbWireLen) {
      uint64 n;

      if (!PbReadVarint(&r, &n))
        return false;
      if (n > (uint64)(r.end - r.p))
        return false;
      PbCopyMsg(msgbuf, msglen, r.p, (size_t)n);
      r.p += n;
    } else if (!PbSkipField(&r, wire_type)) {
      return false;
    }
  }
  return true;
}

bool PschOtlpParseLogsResponse(const uint8* data, size_t len, int64* rejected_out, char* msgbuf,
                               size_t msglen) {
  uint64 rejected = 0;
  PbReader r;

  if (rejected_out != NULL)
    *rejected_out = 0;
  if (msgbuf != NULL && msglen > 0)
    msgbuf[0] = '\0';
  if (len == 0)
    return true;  // empty body == full success (partial_success unset)
  if (data == NULL)
    return false;

  r.p = data;
  r.end = data + len;
  while (r.p < r.end) {
    uint64 tag;
    uint64 field;
    uint32 wire_type;

    if (!PbReadVarint(&r, &tag))
      return false;
    field = tag >> 3;
    wire_type = (uint32)(tag & 7);
    if (field == 0)
      return false;
    if (field == 1 && wire_type == kPbWireLen) {
      uint64 n;

      if (!PbReadVarint(&r, &n))
        return false;
      if (n > (uint64)(r.end - r.p))
        return false;
      if (!PbParseNumStrMsg(r.p, (size_t)n, &rejected, msgbuf, msglen))
        return false;
      r.p += n;
    } else if (!PbSkipField(&r, wire_type)) {
      return false;
    }
  }
  if (rejected_out != NULL)
    *rejected_out = (int64)rejected;
  return true;
}

bool PschRpcStatusParse(const uint8* data, size_t len, int32* code_out, char* msgbuf,
                        size_t msglen) {
  uint64 code = 0;

  if (code_out != NULL)
    *code_out = 0;
  if (msgbuf != NULL && msglen > 0)
    msgbuf[0] = '\0';
  if (len == 0)
    return true;  // empty Status: code 0, no message
  if (data == NULL)
    return false;

  if (!PbParseNumStrMsg(data, len, &code, msgbuf, msglen))
    return false;
  // proto3 int32 arrives as the sign-extended 64-bit varint; truncate back.
  if (code_out != NULL)
    *code_out = (int32)code;
  return true;
}
