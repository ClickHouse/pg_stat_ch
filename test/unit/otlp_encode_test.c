// Host unit test for src/export/otlp_encode.c — runs without a PostgreSQL
// build.
//
// THE TRICK: otlp_encode.c normally compiles inside the extension with
// postgres.h as its first include.  Here we define PSCH_OTLP_ENCODE_HOST_TEST
// (which suppresses that include), provide the minimal PG type surface the
// encoder needs — fixed-width typedefs via <stdint.h>, bool via <stdbool.h>,
// Assert via assert(3), USE_ASSERT_CHECKING so the debug depth guard is
// exercised — and then #include the header and the .c directly.
//
// Build & run:
//   cc -std=gnu17 test/unit/otlp_encode_test.c -o /tmp/otlp_test && /tmp/otlp_test
// Optional: /tmp/otlp_test <path> dumps a sample ExportLogsServiceRequest for
// external decoding (e.g. `protoc --decode_raw`).
#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef size_t Size;

#define USE_ASSERT_CHECKING        1
#define Assert(condition)          assert(condition)
#define PSCH_OTLP_ENCODE_HOST_TEST 1

#include "../../src/export/otlp_encode.h"

// The implementation under test, in the same translation unit (see THE TRICK
// above).  Kept in its own include block so tooling cannot resort it above
// the header.
#include "../../src/export/otlp_encode.c"

static int failures = 0;
static int checks = 0;

#define CHECK(cond)                                          \
  do {                                                       \
    checks++;                                                \
    if (!(cond)) {                                           \
      printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
      failures++;                                            \
    }                                                        \
  } while (0)

static void HexDump(const char* label, const uint8* p, size_t n) {
  printf("  %s (%zu):", label, n);
  for (size_t i = 0; i < n; i++)
    printf(" %02X", p[i]);
  printf("\n");
}

static void CheckBytes(int line, const PschPbBuf* b, const uint8* want, size_t want_len) {
  checks++;
  if (b->overflow || b->len != want_len || memcmp(b->data, want, want_len) != 0) {
    printf("FAIL %s:%d: byte vector mismatch (overflow=%d)\n", __FILE__, line, (int)b->overflow);
    HexDump("got ", b->data, b->len);
    HexDump("want", want, want_len);
    failures++;
  }
}

// ---------------------------------------------------------------------------
// Known-answer vectors for the writer primitives (hand-computed per
// https://protobuf.dev/programming-guides/encoding/)
// ---------------------------------------------------------------------------

typedef void (*EmitFn)(PschPbBuf* b);

static void RunVector(int line, EmitFn emit, const uint8* want, size_t want_len) {
  uint8 storage[256];
  PschPbBuf b;

  PschPbInit(&b, storage, sizeof(storage));
  emit(&b);
  CheckBytes(line, &b, want, want_len);
}

static void EmitVarint0(PschPbBuf* b) {
  PschPbVarint(b, 1, 0);
}
static void EmitVarint1(PschPbBuf* b) {
  PschPbVarint(b, 1, 1);
}
static void EmitVarint127(PschPbBuf* b) {
  PschPbVarint(b, 1, 127);
}
static void EmitVarint128(PschPbBuf* b) {
  PschPbVarint(b, 1, 128);
}
static void EmitVarint300(PschPbBuf* b) {
  PschPbVarint(b, 1, 300);
}
static void EmitVarintMax(PschPbBuf* b) {
  PschPbVarint(b, 1, UINT64_MAX);
}
static void EmitVarintField16(PschPbBuf* b) {
  PschPbVarint(b, 16, 1);
}
static void EmitFixed64(PschPbBuf* b) {
  PschPbFixed64(b, 1, 0x0102030405060708ULL);
}
static void EmitFixed32(PschPbBuf* b) {
  PschPbFixed32(b, 8, 0xDEADBEEFu);
}
static void EmitDouble1(PschPbBuf* b) {
  PschPbDouble(b, 4, 1.0);
}
static void EmitDoubleNeg(PschPbBuf* b) {
  PschPbDouble(b, 4, -2.5);
}
static void EmitStringAbc(PschPbBuf* b) {
  PschPbString(b, 2, "abc", 3);
}
static void EmitStringEmpty(PschPbBuf* b) {
  PschPbString(b, 2, "", 0);
}
static void EmitBytesNul(PschPbBuf* b) {
  PschPbBytes(b, 15, "\x00\xFF", 2);
}
static void EmitKvString(PschPbBuf* b) {
  PschPbKvString(b, 6, "a", "b", 1);
}
static void EmitKvIntNeg(PschPbBuf* b) {
  PschPbKvInt(b, 6, "n", -1);
}
static void EmitKvIntZero(PschPbBuf* b) {
  PschPbKvInt(b, 1, "z", 0);
}
static void EmitKvDouble(PschPbBuf* b) {
  PschPbKvDouble(b, 6, "d", 0.5);
}

static void EmitNested(PschPbBuf* b) {
  size_t outer = PschPbMsgBegin(b, 1);
  size_t inner;

  PschPbVarint(b, 1, 150);
  inner = PschPbMsgBegin(b, 2);
  PschPbString(b, 1, "hi", 2);
  PschPbMsgEnd(b, inner);
  PschPbMsgEnd(b, outer);
}

static void TestPrimitiveVectors(void) {
  static const uint8 v0[] = {0x08, 0x00};
  static const uint8 v1[] = {0x08, 0x01};
  static const uint8 v127[] = {0x08, 0x7F};
  static const uint8 v128[] = {0x08, 0x80, 0x01};
  static const uint8 v300[] = {0x08, 0xAC, 0x02};
  static const uint8 vmax[] = {0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01};
  static const uint8 vf16[] = {0x80, 0x01, 0x01};  // field 16: tag itself is a 2-byte varint
  static const uint8 f64[] = {0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01};
  static const uint8 f32[] = {0x45, 0xEF, 0xBE, 0xAD, 0xDE};
  static const uint8 d1[] = {0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F};
  static const uint8 dneg[] = {0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xC0};
  static const uint8 sabc[] = {0x12, 0x03, 0x61, 0x62, 0x63};
  static const uint8 sempty[] = {0x12, 0x00};
  static const uint8 bnul[] = {0x7A, 0x02, 0x00, 0xFF};
  // KeyValue{key="a" value{string_value="b"}}, overlong 4-byte length slots
  static const uint8 kvs[] = {0x32, 0x8B, 0x80, 0x80, 0x00, 0x0A, 0x01, 0x61,
                              0x12, 0x83, 0x80, 0x80, 0x00, 0x0A, 0x01, 0x62};
  // int_value -1 encodes as the 10-byte two's-complement varint
  static const uint8 kvi[] = {0x32, 0x93, 0x80, 0x80, 0x00, 0x0A, 0x01, 0x6E,
                              0x12, 0x8B, 0x80, 0x80, 0x00, 0x18, 0xFF, 0xFF,
                              0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01};
  static const uint8 kvz[] = {0x0A, 0x8A, 0x80, 0x80, 0x00, 0x0A, 0x01, 0x7A,
                              0x12, 0x82, 0x80, 0x80, 0x00, 0x18, 0x00};
  static const uint8 kvd[] = {0x32, 0x91, 0x80, 0x80, 0x00, 0x0A, 0x01, 0x64, 0x12, 0x89, 0x80,
                              0x80, 0x00, 0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xE0, 0x3F};
  // msg1{ varint1=150 msg2{ string1="hi" } } — verifies slot patching
  static const uint8 nested[] = {0x0A, 0x8C, 0x80, 0x80, 0x00, 0x08, 0x96, 0x01, 0x12,
                                 0x84, 0x80, 0x80, 0x00, 0x0A, 0x02, 0x68, 0x69};

  RunVector(__LINE__, EmitVarint0, v0, sizeof(v0));
  RunVector(__LINE__, EmitVarint1, v1, sizeof(v1));
  RunVector(__LINE__, EmitVarint127, v127, sizeof(v127));
  RunVector(__LINE__, EmitVarint128, v128, sizeof(v128));
  RunVector(__LINE__, EmitVarint300, v300, sizeof(v300));
  RunVector(__LINE__, EmitVarintMax, vmax, sizeof(vmax));
  RunVector(__LINE__, EmitVarintField16, vf16, sizeof(vf16));
  RunVector(__LINE__, EmitFixed64, f64, sizeof(f64));
  RunVector(__LINE__, EmitFixed32, f32, sizeof(f32));
  RunVector(__LINE__, EmitDouble1, d1, sizeof(d1));
  RunVector(__LINE__, EmitDoubleNeg, dneg, sizeof(dneg));
  RunVector(__LINE__, EmitStringAbc, sabc, sizeof(sabc));
  RunVector(__LINE__, EmitStringEmpty, sempty, sizeof(sempty));
  RunVector(__LINE__, EmitBytesNul, bnul, sizeof(bnul));
  RunVector(__LINE__, EmitKvString, kvs, sizeof(kvs));
  RunVector(__LINE__, EmitKvIntNeg, kvi, sizeof(kvi));
  RunVector(__LINE__, EmitKvIntZero, kvz, sizeof(kvz));
  RunVector(__LINE__, EmitKvDouble, kvd, sizeof(kvd));
  RunVector(__LINE__, EmitNested, nested, sizeof(nested));
}

// ---------------------------------------------------------------------------
// Overflow safety: full encode sequence against every undersized cap.
// No byte beyond cap (or beyond len) may ever be touched.
// ---------------------------------------------------------------------------

static void EncodeAll(PschPbBuf* b) {
  size_t m;

  PschPbVarint(b, 1, 300);
  PschPbFixed64(b, 2, 0x1122334455667788ULL);
  PschPbFixed32(b, 3, 0xCAFEBABEu);
  PschPbDouble(b, 4, 3.14);
  PschPbString(b, 5, "hello", 5);
  PschPbBytes(b, 6, "\x00\x01", 2);
  m = PschPbMsgBegin(b, 7);
  PschPbKvString(b, 1, "k", "v", 1);
  PschPbKvInt(b, 1, "i", -7);
  PschPbKvDouble(b, 1, "d", 2.5);
  PschPbMsgEnd(b, m);
}

static void TestOverflowFuzz(void) {
  uint8 golden[512];
  uint8 storage[512 + 64];
  PschPbBuf b;
  size_t golden_len;

  PschPbInit(&b, golden, sizeof(golden));
  EncodeAll(&b);
  CHECK(!b.overflow);
  golden_len = b.len;
  CHECK(golden_len > 0 && golden_len < sizeof(golden));

  for (size_t cap = 0; cap < golden_len; cap++) {
    memset(storage, 0xAA, sizeof(storage));
    PschPbInit(&b, cap == 0 ? NULL : storage, cap);
    if (cap == 0)
      b.data = storage;  // keep a real pointer so sentinels are checkable
    EncodeAll(&b);
    CHECK(b.overflow);
    CHECK(b.len <= cap);
    // Bytes written so far must be a prefix of the full encoding (no
    // partial/odd writes), except inside unpatched 4-byte slots.
    bool prefix_ok = true;
    for (size_t i = 0; i < b.len; i++) {
      if (storage[i] != golden[i] && storage[i] != 0x00)
        prefix_ok = false;
    }
    CHECK(prefix_ok);
    bool untouched = true;
    for (size_t i = b.len; i < sizeof(storage); i++) {
      if (storage[i] != 0xAA)
        untouched = false;
    }
    CHECK(untouched);
  }

  // Exact-fit cap must reproduce the golden bytes with no overflow.
  memset(storage, 0xAA, sizeof(storage));
  PschPbInit(&b, storage, golden_len);
  EncodeAll(&b);
  CHECK(!b.overflow);
  CHECK(b.len == golden_len);
  CHECK(memcmp(storage, golden, golden_len) == 0);

  // NULL/zero-cap buffer: overflow without dereferencing data.
  PschPbInit(&b, NULL, 0);
  EncodeAll(&b);
  CHECK(b.overflow);
  CHECK(b.len == 0);
}

static void TestOverflowSemantics(void) {
  uint8 st[8];
  PschPbBuf b;
  size_t m;
  size_t len_before;

  // MsgEnd on an overflowed buffer is a no-op: slot keeps its zero padding.
  memset(st, 0xAA, sizeof(st));
  PschPbInit(&b, st, sizeof(st));
  m = PschPbMsgBegin(&b, 1);
  CHECK(!b.overflow);
  CHECK(m == 1);
  CHECK(b.len == 5);
  PschPbString(&b, 2, "hello", 5);  // needs 7 bytes, 3 remain
  CHECK(b.overflow);
  len_before = b.len;
  PschPbMsgEnd(&b, m);
  CHECK(b.len == len_before);
  CHECK(st[1] == 0 && st[2] == 0 && st[3] == 0 && st[4] == 0);

  // Every subsequent write is a no-op once overflow is set.
  PschPbVarint(&b, 1, 7);
  PschPbFixed64(&b, 1, 7);
  PschPbFixed32(&b, 1, 7);
  PschPbDouble(&b, 1, 7.0);
  PschPbBytes(&b, 1, "x", 1);
  CHECK(PschPbMsgBegin(&b, 1) == b.len);
  CHECK(b.len == len_before);

  // 1-byte buffer: nothing fits, first byte untouched.
  st[0] = 0xAA;
  PschPbInit(&b, st, 1);
  PschPbVarint(&b, 1, 0);
  CHECK(b.overflow);
  CHECK(b.len == 0);
  CHECK(st[0] == 0xAA);
}

static void TestMsgEndLengthCeiling(void) {
  // A message body larger than the 4-byte slot can represent (2^28-1) must
  // flag overflow instead of patching garbage.  Simulated by lying about
  // len/cap; MsgEnd's size check fires before any write.
  uint8 st[16];
  PschPbBuf b;
  size_t m;

  PschPbInit(&b, st, sizeof(st));
  m = PschPbMsgBegin(&b, 1);
  b.cap = SIZE_MAX / 2;
  b.len = m + 4 + 0x0FFFFFFF + 1;
  PschPbMsgEnd(&b, m);
  CHECK(b.overflow);
  CHECK(st[m] == 0 && st[m + 1] == 0 && st[m + 2] == 0 && st[m + 3] == 0);
}

// ---------------------------------------------------------------------------
// Independent structural decode of an encoded sample request.  Deliberately
// re-implements varint/length reading here rather than reusing the .c's
// internals, so encoder bugs cannot self-validate.
// ---------------------------------------------------------------------------

typedef struct TR {
  const uint8* p;
  const uint8* end;
} TR;

static bool TrVarint(TR* t, uint64* v) {
  *v = 0;
  for (int i = 0; i < 10; i++) {
    if (t->p >= t->end)
      return false;
    uint8 byte = *t->p++;
    *v |= (uint64)(byte & 0x7F) << (7 * i);
    if (!(byte & 0x80))
      return true;
  }
  return false;
}

static bool TrTag(TR* t, uint64* field, uint32* wt) {
  uint64 tag;
  if (!TrVarint(t, &tag))
    return false;
  *field = tag >> 3;
  *wt = (uint32)(tag & 7);
  return true;
}

// Expects a length-delimited field with the given number; returns its slice.
static bool TrExpectLen(TR* t, uint64 want_field, TR* sub) {
  uint64 field, n;
  uint32 wt;
  if (!TrTag(t, &field, &wt))
    return false;
  if (field != want_field || wt != 2)
    return false;
  if (!TrVarint(t, &n))
    return false;
  if (n > (uint64)(t->end - t->p))
    return false;
  sub->p = t->p;
  sub->end = t->p + n;
  t->p += n;
  return true;
}

static bool TrExpectString(TR* t, uint64 want_field, const char* want) {
  TR s;
  if (!TrExpectLen(t, want_field, &s))
    return false;
  size_t n = (size_t)(s.end - s.p);
  return n == strlen(want) && memcmp(s.p, want, n) == 0;
}

static bool TrExpectKvString(TR* t, uint64 want_field, const char* key, const char* val) {
  TR kv, any;
  if (!TrExpectLen(t, want_field, &kv))
    return false;
  if (!TrExpectString(&kv, 1, key))
    return false;
  if (!TrExpectLen(&kv, 2, &any))
    return false;
  if (!TrExpectString(&any, 1, val))
    return false;
  return any.p == any.end && kv.p == kv.end;
}

static void BuildSampleRequest(PschPbBuf* b) {
  size_t rl = PschPbMsgBegin(b, PSCH_OTLP_REQ_RESOURCE_LOGS);
  size_t res = PschPbMsgBegin(b, PSCH_OTLP_RL_RESOURCE);
  size_t sl, scope, lr;

  PschPbKvString(b, PSCH_OTLP_RES_ATTRIBUTES, "service.name", "pg_stat_ch", 10);
  PschPbKvString(b, PSCH_OTLP_RES_ATTRIBUTES, "host.name", "testhost", 8);
  PschPbMsgEnd(b, res);
  sl = PschPbMsgBegin(b, PSCH_OTLP_RL_SCOPE_LOGS);
  scope = PschPbMsgBegin(b, PSCH_OTLP_SL_SCOPE);
  PschPbString(b, PSCH_OTLP_SCOPE_NAME, "pg_stat_ch", 10);
  PschPbString(b, PSCH_OTLP_SCOPE_VERSION, "0.4.0", 5);
  PschPbMsgEnd(b, scope);
  lr = PschPbMsgBegin(b, PSCH_OTLP_SL_LOG_RECORDS);
  PschPbFixed64(b, PSCH_OTLP_LR_TIME_UNIX_NANO, 1760000000000000000ULL);
  PschPbVarint(b, PSCH_OTLP_LR_SEVERITY_NUMBER, 9);
  PschPbKvString(b, PSCH_OTLP_LR_ATTRIBUTES, "db", "postgres", 8);
  PschPbMsgEnd(b, lr);
  PschPbMsgEnd(b, sl);
  PschPbMsgEnd(b, rl);
}

static void TestSampleRequestStructure(const char* dump_path) {
  uint8 storage[1024];
  PschPbBuf b;
  TR t, rl, res, sl, scope, lr;
  uint64 field, v;
  uint32 wt;

  PschPbInit(&b, storage, sizeof(storage));
  BuildSampleRequest(&b);
  CHECK(!b.overflow);

  if (dump_path != NULL) {
    FILE* f = fopen(dump_path, "wb");
    if (f != NULL) {
      fwrite(b.data, 1, b.len, f);
      fclose(f);
      printf("sample request dumped to %s (%zu bytes)\n", dump_path, b.len);
    }
  }

  t.p = b.data;
  t.end = b.data + b.len;
  CHECK(TrExpectLen(&t, 1, &rl));    // ResourceLogs
  CHECK(t.p == t.end);               // request has exactly one top-level field
  CHECK(TrExpectLen(&rl, 1, &res));  // Resource
  CHECK(TrExpectKvString(&res, 1, "service.name", "pg_stat_ch"));
  CHECK(TrExpectKvString(&res, 1, "host.name", "testhost"));
  CHECK(res.p == res.end);
  CHECK(TrExpectLen(&rl, 2, &sl));  // ScopeLogs
  CHECK(rl.p == rl.end);
  CHECK(TrExpectLen(&sl, 1, &scope));  // InstrumentationScope
  CHECK(TrExpectString(&scope, 1, "pg_stat_ch"));
  CHECK(TrExpectString(&scope, 2, "0.4.0"));
  CHECK(scope.p == scope.end);
  CHECK(TrExpectLen(&sl, 2, &lr));  // LogRecord
  CHECK(sl.p == sl.end);
  CHECK(TrTag(&lr, &field, &wt));
  CHECK(field == 1 && wt == 1);  // time_unix_nano fixed64
  CHECK((size_t)(lr.end - lr.p) >= 8);
  v = 0;
  for (int i = 7; i >= 0; i--)
    v = (v << 8) | lr.p[i];
  lr.p += 8;
  CHECK(v == 1760000000000000000ULL);
  CHECK(TrTag(&lr, &field, &wt));
  CHECK(field == 2 && wt == 0);
  CHECK(TrVarint(&lr, &v));
  CHECK(v == 9);
  CHECK(TrExpectKvString(&lr, 6, "db", "postgres"));
  CHECK(lr.p == lr.end);
}

// ---------------------------------------------------------------------------
// Response parsers
// ---------------------------------------------------------------------------

static void TestLogsResponseParser(void) {
  // partial_success{ rejected_log_records=5, error_message="boom" }
  static const uint8 ok[] = {0x0A, 0x08, 0x08, 0x05, 0x12, 0x04, 0x62, 0x6F, 0x6F, 0x6D};
  // Same, with the nested length written as an overlong 4-byte varint (the
  // form our own encoder emits) — parsers must accept non-canonical varints.
  static const uint8 overlong[] = {0x0A, 0x88, 0x80, 0x80, 0x00, 0x08, 0x05,
                                   0x12, 0x04, 0x62, 0x6F, 0x6F, 0x6D};
  // Unknown fields of every wire type at top level, then partial_success
  static const uint8 unknown_top[] = {0x10, 0x07, 0x19, 1,    2,    3,    4,    5,
                                      6,    7,    8,    0x25, 1,    2,    3,    4,
                                      0x22, 0x02, 0x61, 0x62, 0x0A, 0x02, 0x08, 0x09};
  // Unknown field inside partial_success
  static const uint8 unknown_in[] = {0x0A, 0x0A, 0x08, 0x05, 0x18, 0x07,
                                     0x12, 0x04, 0x62, 0x6F, 0x6F, 0x6D};
  static const uint8 rejected_only[] = {0x0A, 0x02, 0x08, 0x07};
  int64 rejected;
  char msg[64];

  CHECK(PschOtlpParseLogsResponse(ok, sizeof(ok), &rejected, msg, sizeof(msg)));
  CHECK(rejected == 5);
  CHECK(strcmp(msg, "boom") == 0);

  CHECK(PschOtlpParseLogsResponse(NULL, 0, &rejected, msg, sizeof(msg)));
  CHECK(rejected == 0);
  CHECK(msg[0] == '\0');

  CHECK(PschOtlpParseLogsResponse(overlong, sizeof(overlong), &rejected, msg, sizeof(msg)));
  CHECK(rejected == 5);
  CHECK(strcmp(msg, "boom") == 0);

  CHECK(PschOtlpParseLogsResponse(unknown_top, sizeof(unknown_top), &rejected, msg, sizeof(msg)));
  CHECK(rejected == 9);

  CHECK(PschOtlpParseLogsResponse(unknown_in, sizeof(unknown_in), &rejected, msg, sizeof(msg)));
  CHECK(rejected == 5);
  CHECK(strcmp(msg, "boom") == 0);

  CHECK(
      PschOtlpParseLogsResponse(rejected_only, sizeof(rejected_only), &rejected, msg, sizeof(msg)));
  CHECK(rejected == 7);
  CHECK(msg[0] == '\0');

  // NULL outputs must be tolerated.
  CHECK(PschOtlpParseLogsResponse(ok, sizeof(ok), NULL, NULL, 0));

  // Truncation: ok[] has no field boundary between 1 and 9 bytes, so every
  // proper prefix is malformed.
  for (size_t n = 1; n < sizeof(ok); n++) {
    CHECK(!PschOtlpParseLogsResponse(ok, n, &rejected, msg, sizeof(msg)));
  }

  // msgbuf truncation: only msglen-1 chars + NUL are written.
  CHECK(PschOtlpParseLogsResponse(ok, sizeof(ok), &rejected, msg, 3));
  CHECK(strcmp(msg, "bo") == 0);
}

static void TestGarbageRejection(void) {
  static const uint8 group_start[] = {0x0B};   // field 1, wire type 3
  static const uint8 group_end[] = {0x0C};     // field 1, wire type 4
  static const uint8 wt6[] = {0x0E};           // wire type 6
  static const uint8 wt7[] = {0x0F};           // wire type 7
  static const uint8 field0[] = {0x00};        // field number 0
  static const uint8 trunc_varint[] = {0x08};  // tag then nothing
  static const uint8 varint_11b[] = {0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                     0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01};
  static const uint8 len_overrun[] = {0x12, 0x05, 0x01};  // says 5, has 1
  static const uint8 fixed64_short[] = {0x19, 0x01, 0x02, 0x03};
  static const uint8 fixed32_short[] = {0x25, 0x01};
  static const uint8 nested_overrun[] = {0x0A, 0x05, 0x08, 0x05};  // inner says 5, has 2
  static const uint8 garbage[] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
  int64 rejected;
  int32 code;
  char msg[16];

  CHECK(!PschOtlpParseLogsResponse(group_start, sizeof(group_start), &rejected, msg, sizeof(msg)));
  CHECK(!PschOtlpParseLogsResponse(group_end, sizeof(group_end), &rejected, msg, sizeof(msg)));
  CHECK(!PschOtlpParseLogsResponse(wt6, sizeof(wt6), &rejected, msg, sizeof(msg)));
  CHECK(!PschOtlpParseLogsResponse(wt7, sizeof(wt7), &rejected, msg, sizeof(msg)));
  CHECK(!PschOtlpParseLogsResponse(field0, sizeof(field0), &rejected, msg, sizeof(msg)));
  CHECK(
      !PschOtlpParseLogsResponse(trunc_varint, sizeof(trunc_varint), &rejected, msg, sizeof(msg)));
  CHECK(!PschOtlpParseLogsResponse(varint_11b, sizeof(varint_11b), &rejected, msg, sizeof(msg)));
  CHECK(!PschOtlpParseLogsResponse(len_overrun, sizeof(len_overrun), &rejected, msg, sizeof(msg)));
  CHECK(!PschOtlpParseLogsResponse(fixed64_short, sizeof(fixed64_short), &rejected, msg,
                                   sizeof(msg)));
  CHECK(!PschOtlpParseLogsResponse(fixed32_short, sizeof(fixed32_short), &rejected, msg,
                                   sizeof(msg)));
  CHECK(!PschOtlpParseLogsResponse(nested_overrun, sizeof(nested_overrun), &rejected, msg,
                                   sizeof(msg)));
  CHECK(!PschOtlpParseLogsResponse(garbage, sizeof(garbage), &rejected, msg, sizeof(msg)));
  CHECK(!PschOtlpParseLogsResponse(NULL, 5, &rejected, msg, sizeof(msg)));

  CHECK(!PschRpcStatusParse(group_start, sizeof(group_start), &code, msg, sizeof(msg)));
  CHECK(!PschRpcStatusParse(varint_11b, sizeof(varint_11b), &code, msg, sizeof(msg)));
  CHECK(!PschRpcStatusParse(len_overrun, sizeof(len_overrun), &code, msg, sizeof(msg)));
  CHECK(!PschRpcStatusParse(NULL, 5, &code, msg, sizeof(msg)));

  // Garbage prefixes of valid input must never over-read (run under ASan).
  static const uint8 ok[] = {0x0A, 0x08, 0x08, 0x05, 0x12, 0x04, 0x62, 0x6F, 0x6F, 0x6D};
  uint8 fuzz[sizeof(ok)];
  for (size_t flip = 0; flip < sizeof(ok) * 8; flip++) {
    memcpy(fuzz, ok, sizeof(ok));
    fuzz[flip / 8] ^= (uint8)(1u << (flip % 8));
    (void)PschOtlpParseLogsResponse(fuzz, sizeof(fuzz), &rejected, msg, sizeof(msg));
  }
}

static void TestRpcStatusParser(void) {
  static const uint8 ok[] = {0x08, 0x03, 0x12, 0x03, 0x62, 0x61, 0x64};
  static const uint8 neg[] = {0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01};
  static const uint8 last_wins[] = {0x08, 0x01, 0x08, 0x02};
  // details=3 (google.protobuf.Any) must be skipped
  static const uint8 with_details[] = {0x08, 0x05, 0x1A, 0x02, 0x0A, 0x00, 0x12, 0x02, 0x68, 0x69};
  int32 code;
  char msg[64];

  CHECK(PschRpcStatusParse(ok, sizeof(ok), &code, msg, sizeof(msg)));
  CHECK(code == 3);
  CHECK(strcmp(msg, "bad") == 0);

  CHECK(PschRpcStatusParse(NULL, 0, &code, msg, sizeof(msg)));
  CHECK(code == 0);
  CHECK(msg[0] == '\0');

  CHECK(PschRpcStatusParse(neg, sizeof(neg), &code, msg, sizeof(msg)));
  CHECK(code == -1);

  CHECK(PschRpcStatusParse(last_wins, sizeof(last_wins), &code, msg, sizeof(msg)));
  CHECK(code == 2);

  CHECK(PschRpcStatusParse(with_details, sizeof(with_details), &code, msg, sizeof(msg)));
  CHECK(code == 5);
  CHECK(strcmp(msg, "hi") == 0);

  CHECK(PschRpcStatusParse(ok, sizeof(ok), &code, msg, 2));
  CHECK(strcmp(msg, "b") == 0);
}

int main(int argc, char** argv) {
  TestPrimitiveVectors();
  TestOverflowFuzz();
  TestOverflowSemantics();
  TestMsgEndLengthCeiling();
  TestSampleRequestStructure(argc > 1 ? argv[1] : NULL);
  TestLogsResponseParser();
  TestGarbageRejection();
  TestRpcStatusParser();

  if (failures > 0) {
    printf("FAILED: %d of %d checks\n", failures, checks);
    return 1;
  }
  printf("OK: %d checks passed\n", checks);
  return 0;
}
