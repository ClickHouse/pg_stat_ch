// Hand-rolled protobuf wire encoder for OTLP logs (opentelemetry-proto
// v1.9.0 field surface — see OTEL_REWRITE_DESIGN.md §2/§5).
//
// Properties:
//  * Zero heap allocation: encodes into a caller-provided fixed buffer.
//  * Overflow-safe: once the buffer would overflow, the `overflow` flag is
//    set and all further writes become no-ops; the caller checks once after
//    encoding.  No partial out-of-bounds writes, ever.
//  * Nested messages use fixed-width (4-byte, zero-padded "overlong")
//    varint length slots so encoding is single-pass.  Overlong varints are
//    legal protobuf; all conformant parsers accept them.
//  * Never emits post-v1.9.0 fields (AnyValue.string_value_strindex=8,
//    KeyValue.key_strindex=3, Resource.entity_refs=3), reserved LogRecord
//    field 4, or ResourceLogs field 1000.
#ifndef PG_STAT_CH_OTLP_ENCODE_H
#define PG_STAT_CH_OTLP_ENCODE_H

typedef struct PschPbBuf {
  uint8* data;
  size_t cap;
  size_t len;
  bool overflow;
} PschPbBuf;

#define PSCH_PB_MAX_DEPTH 16  // ExportLogsServiceRequest nests 7 deep; margin

static inline void PschPbInit(PschPbBuf* b, uint8* storage, size_t cap) {
  b->data = storage;
  b->cap = cap;
  b->len = 0;
  b->overflow = false;
}

// --- primitives (field = protobuf field number) ---
extern void PschPbVarint(PschPbBuf* b, uint32 field, uint64 value);   // wire type 0
extern void PschPbFixed64(PschPbBuf* b, uint32 field, uint64 value);  // wire type 1
extern void PschPbFixed32(PschPbBuf* b, uint32 field, uint32 value);  // wire type 5
extern void PschPbDouble(PschPbBuf* b, uint32 field, double value);   // wire type 1
extern void PschPbBytes(PschPbBuf* b, uint32 field, const void* data, size_t len);
extern void PschPbString(PschPbBuf* b, uint32 field, const char* s, size_t len);

// --- single-pass nested messages ---
// Begin returns the position of a 4-byte padded varint length slot;
// End patches it.  Unbalanced Begin/End is a programming error (Assert).
extern size_t PschPbMsgBegin(PschPbBuf* b, uint32 field);
extern void PschPbMsgEnd(PschPbBuf* b, size_t slot);

// --- OTLP common.v1 helpers (KeyValue{key=1, value=2}, AnyValue oneof:
//     string=1 bool=2 int=3 double=4 array=5 kvlist=6 bytes=7) ---
// `field` is the repeated-KeyValue field number in the enclosing message
// (Resource.attributes=1, LogRecord.attributes=6).
extern void PschPbKvString(PschPbBuf* b, uint32 field, const char* key, const char* val,
                           size_t val_len);
extern void PschPbKvInt(PschPbBuf* b, uint32 field, const char* key, int64 value);
extern void PschPbKvDouble(PschPbBuf* b, uint32 field, const char* key, double value);

// --- response parsing (no allocation; messages truncated into msgbuf) ---
// ExportLogsServiceResponse{ partial_success=1 { rejected_log_records=1,
// error_message=2 } }.  Returns false on malformed input.
extern bool PschOtlpParseLogsResponse(const uint8* data, size_t len, int64* rejected_out,
                                      char* msgbuf, size_t msglen);
// google.rpc.Status{ code=1, message=2 } — for 4xx/5xx diagnostics.
extern bool PschRpcStatusParse(const uint8* data, size_t len, int32* code_out, char* msgbuf,
                               size_t msglen);

// ---------------------------------------------------------------------------
// Field-number constants (verified against opentelemetry-proto v1.9.0).
// ---------------------------------------------------------------------------
// ExportLogsServiceRequest
#define PSCH_OTLP_REQ_RESOURCE_LOGS 1
// ResourceLogs
#define PSCH_OTLP_RL_RESOURCE   1
#define PSCH_OTLP_RL_SCOPE_LOGS 2
// Resource
#define PSCH_OTLP_RES_ATTRIBUTES 1
// ScopeLogs
#define PSCH_OTLP_SL_SCOPE       1
#define PSCH_OTLP_SL_LOG_RECORDS 2
// InstrumentationScope
#define PSCH_OTLP_SCOPE_NAME    1
#define PSCH_OTLP_SCOPE_VERSION 2
// LogRecord
#define PSCH_OTLP_LR_TIME_UNIX_NANO          1   // fixed64
#define PSCH_OTLP_LR_SEVERITY_NUMBER         2   // varint enum
#define PSCH_OTLP_LR_SEVERITY_TEXT           3   // string
#define PSCH_OTLP_LR_BODY                    5   // AnyValue
#define PSCH_OTLP_LR_ATTRIBUTES              6   // repeated KeyValue
#define PSCH_OTLP_LR_OBSERVED_TIME_UNIX_NANO 11  // fixed64
// AnyValue oneof
#define PSCH_OTLP_ANY_STRING 1
#define PSCH_OTLP_ANY_BOOL   2
#define PSCH_OTLP_ANY_INT    3
#define PSCH_OTLP_ANY_DOUBLE 4
#define PSCH_OTLP_ANY_BYTES  7
// KeyValue
#define PSCH_OTLP_KV_KEY   1
#define PSCH_OTLP_KV_VALUE 2

#endif  // PG_STAT_CH_OTLP_ENCODE_H
