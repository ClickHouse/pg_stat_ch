extern "C" {
#include "postgres.h"
}

#include <gtest/gtest.h>

#include <arrow/array.h>
#include <arrow/array/array_dict.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "export/arrow_batch.h"
#include "queue/event.h"

// Stub out the PG-dependent bridge functions declared in exporter_interface.h.
void LogNegativeValue(const std::string& /*column*/, int64_t /*value*/) {}
void LogExporterWarning(const char* /*ctx*/, const char* /*msg*/) {}
void RecordExporterFailure(const char* /*msg*/) {}

// Stubs for PostgreSQL elog/ereport machinery (errstart/errfinish/errmsg_internal).
// The real implementations live in the PostgreSQL server binary; we just need
// linkable symbols so arrow_batch.cc's elog() calls resolve.
extern "C" {
bool errstart(int /*elevel*/, const char* /*domain*/) {
  return false;
}
bool errstart_cold(int /*elevel*/, const char* /*domain*/) {
  return false;
}
void errfinish(const char* /*filename*/, int /*lineno*/, const char* /*funcname*/) {}
int errmsg_internal(const char* /*fmt*/, ...) {
  return 0;
}
}

namespace {

// Build a PschEvent with known values for deterministic assertions.
PschEvent MakeTestEvent() {
  PschEvent ev = {};
  // Timing
  ev.ts_start = 1000000;  // 1 second past PG epoch (2000-01-01 00:00:01)
  ev.duration_us = 42000;

  // Identity
  ev.dbid = 16384;
  ev.userid = 10;
  std::memcpy(ev.datname, "testdb", 6);
  ev.datname_len = 6;
  std::memcpy(ev.username, "alice", 5);
  ev.username_len = 5;
  ev.pid = 12345;
  ev.queryid = 987654321ULL;
  ev.top_level = true;
  ev.cmd_type = PSCH_CMD_SELECT;

  // Results
  ev.rows = 10;

  // Buffer usage
  ev.shared_blks_hit = 100;
  ev.shared_blks_read = 5;
  ev.shared_blks_dirtied = 2;
  ev.shared_blks_written = 1;
  ev.local_blks_hit = 3;
  ev.local_blks_read = 0;
  ev.local_blks_dirtied = 0;
  ev.local_blks_written = 0;
  ev.temp_blks_read = 0;
  ev.temp_blks_written = 0;

  // I/O timing
  ev.shared_blk_read_time_us = 500;
  ev.shared_blk_write_time_us = 100;
  ev.local_blk_read_time_us = 0;
  ev.local_blk_write_time_us = 0;
  ev.temp_blk_read_time_us = 0;
  ev.temp_blk_write_time_us = 0;

  // WAL
  ev.wal_records = 7;
  ev.wal_fpi = 1;
  ev.wal_bytes = 4096;

  // CPU
  ev.cpu_user_time_us = 3000;
  ev.cpu_sys_time_us = 500;

  // JIT (all zero — simulates older PG or no JIT)
  ev.jit_functions = 0;

  // Parallel workers
  ev.parallel_workers_planned = 2;
  ev.parallel_workers_launched = 1;

  // Error info (no error)
  std::memset(ev.err_sqlstate, '0', 5);
  ev.err_sqlstate[5] = '\0';
  ev.err_elevel = 0;
  ev.err_message_len = 0;

  // Client context
  std::memcpy(ev.application_name, "pgbench", 7);
  ev.application_name_len = 7;
  std::memcpy(ev.client_addr, "127.0.0.1", 9);
  ev.client_addr_len = 9;

  // Query text
  const char* query = "SELECT * FROM users WHERE id = $1";
  ev.query_len = static_cast<uint16>(std::strlen(query));
  std::memcpy(ev.query, query, ev.query_len);

  return ev;
}

// Read back an IPC buffer into a RecordBatch for assertions.
std::shared_ptr<arrow::RecordBatch> ReadIpcBuffer(const std::shared_ptr<arrow::Buffer>& buf) {
  auto input = std::make_shared<arrow::io::BufferReader>(buf);
  auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
  EXPECT_TRUE(reader_result.ok()) << reader_result.status().ToString();
  auto reader = std::move(reader_result).ValueOrDie();

  std::shared_ptr<arrow::RecordBatch> batch;
  auto status = reader->ReadNext(&batch);
  EXPECT_TRUE(status.ok()) << status.ToString();
  return batch;
}

// Helper to get a string value from a plain UTF8 column.
std::string GetStringValue(const std::shared_ptr<arrow::RecordBatch>& batch,
                           const std::string& field_name, int row) {
  auto col = batch->GetColumnByName(field_name);
  EXPECT_NE(col, nullptr) << "missing column: " << field_name;
  if (col->type_id() == arrow::Type::DICTIONARY) {
    auto dict_array = std::static_pointer_cast<arrow::DictionaryArray>(col);
    auto dict = std::static_pointer_cast<arrow::StringArray>(dict_array->dictionary());
    auto index = dict_array->GetValueIndex(row);
    return dict->GetString(index);
  }
  auto str_array = std::static_pointer_cast<arrow::StringArray>(col);
  return str_array->GetString(row);
}

// Helper to get a uint64 value.
uint64_t GetUInt64Value(const std::shared_ptr<arrow::RecordBatch>& batch,
                        const std::string& field_name, int row) {
  auto col = batch->GetColumnByName(field_name);
  EXPECT_NE(col, nullptr) << "missing column: " << field_name;
  auto arr = std::static_pointer_cast<arrow::UInt64Array>(col);
  return arr->Value(row);
}

// Helper to get a uint32 value.
uint32_t GetUInt32Value(const std::shared_ptr<arrow::RecordBatch>& batch,
                        const std::string& field_name, int row) {
  auto col = batch->GetColumnByName(field_name);
  EXPECT_NE(col, nullptr) << "missing column: " << field_name;
  auto arr = std::static_pointer_cast<arrow::UInt32Array>(col);
  return arr->Value(row);
}

// Helper to get an int32 value.
int32_t GetInt32Value(const std::shared_ptr<arrow::RecordBatch>& batch,
                      const std::string& field_name, int row) {
  auto col = batch->GetColumnByName(field_name);
  EXPECT_NE(col, nullptr) << "missing column: " << field_name;
  auto arr = std::static_pointer_cast<arrow::Int32Array>(col);
  return arr->Value(row);
}

// Helper to get a timestamp (int64 nanoseconds).
int64_t GetTimestampNanos(const std::shared_ptr<arrow::RecordBatch>& batch, int row) {
  auto col = batch->GetColumnByName("ts");
  EXPECT_NE(col, nullptr) << "missing column: ts";
  auto arr = std::static_pointer_cast<arrow::TimestampArray>(col);
  return arr->Value(row);
}

// ============================================================================
// Tests
// ============================================================================

TEST(ArrowBatchBuilder, EmptyBatchReturnsNullBuffer) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init(nullptr, nullptr));

  auto result = builder.Finish();
  EXPECT_EQ(result.num_rows, 0);
  EXPECT_EQ(result.ipc_buffer, nullptr);
}

TEST(ArrowBatchBuilder, SingleEventRoundTrip) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init("", "1.2.3"));

  PschEvent ev = MakeTestEvent();
  ASSERT_TRUE(builder.Append(ev));
  EXPECT_EQ(builder.NumRows(), 1);
  EXPECT_GT(builder.EstimatedBytes(), 0u);

  auto result = builder.Finish();
  ASSERT_NE(result.ipc_buffer, nullptr);
  EXPECT_EQ(result.num_rows, 1);
  EXPECT_GT(result.ipc_buffer->size(), 0);

  auto batch = ReadIpcBuffer(result.ipc_buffer);
  ASSERT_NE(batch, nullptr);
  EXPECT_EQ(batch->num_rows(), 1);
}

TEST(ArrowBatchBuilder, SchemaHasExpectedFields) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init("", "1.0.0"));

  PschEvent ev = MakeTestEvent();
  ASSERT_TRUE(builder.Append(ev));
  auto result = builder.Finish();
  auto batch = ReadIpcBuffer(result.ipc_buffer);
  ASSERT_NE(batch, nullptr);

  auto schema = batch->schema();
  EXPECT_EQ(schema->num_fields(), 55);

  // Spot-check key fields and their types.
  auto check_field = [&](const std::string& name, arrow::Type::type expected_type) {
    auto field = schema->GetFieldByName(name);
    ASSERT_NE(field, nullptr) << "field not found: " << name;
    EXPECT_EQ(field->type()->id(), expected_type)
        << "wrong type for " << name << ": " << field->type()->ToString();
  };

  check_field("ts", arrow::Type::TIMESTAMP);
  check_field("severity", arrow::Type::DICTIONARY);
  check_field("body", arrow::Type::STRING);
  check_field("query_id", arrow::Type::STRING);
  check_field("db_name", arrow::Type::DICTIONARY);
  check_field("db_user", arrow::Type::DICTIONARY);
  check_field("db_operation", arrow::Type::DICTIONARY);
  check_field("query_text", arrow::Type::STRING);
  check_field("pid", arrow::Type::STRING);
  check_field("err_elevel", arrow::Type::INT32);
  check_field("duration_us", arrow::Type::UINT64);
  check_field("rows", arrow::Type::UINT64);
  check_field("shared_blks_hit", arrow::Type::UINT64);
  check_field("wal_bytes", arrow::Type::UINT64);
  check_field("parallel_workers_planned", arrow::Type::UINT32);
  check_field("parallel_workers_launched", arrow::Type::UINT32);
  check_field("service_version", arrow::Type::DICTIONARY);
  check_field("region", arrow::Type::DICTIONARY);
  check_field("host_id", arrow::Type::STRING);
}

TEST(ArrowBatchBuilder, FieldValuesCorrect) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init("", "v0.1.5"));

  PschEvent ev = MakeTestEvent();
  ASSERT_TRUE(builder.Append(ev));
  auto result = builder.Finish();
  auto batch = ReadIpcBuffer(result.ipc_buffer);
  ASSERT_NE(batch, nullptr);

  // String fields
  EXPECT_EQ(GetStringValue(batch, "db_name", 0), "testdb");
  EXPECT_EQ(GetStringValue(batch, "db_user", 0), "alice");
  EXPECT_EQ(GetStringValue(batch, "db_operation", 0), "SELECT");
  EXPECT_EQ(GetStringValue(batch, "app", 0), "pgbench");
  EXPECT_EQ(GetStringValue(batch, "client_addr", 0), "127.0.0.1");
  EXPECT_EQ(GetStringValue(batch, "query_text", 0), "SELECT * FROM users WHERE id = $1");
  EXPECT_EQ(GetStringValue(batch, "pid", 0), "12345");
  EXPECT_EQ(GetStringValue(batch, "service_version", 0), "v0.1.5");

  // query_id is the stringified uint64
  EXPECT_EQ(GetStringValue(batch, "query_id", 0), "987654321");

  // Numeric fields
  EXPECT_EQ(GetUInt64Value(batch, "duration_us", 0), 42000u);
  EXPECT_EQ(GetUInt64Value(batch, "rows", 0), 10u);
  EXPECT_EQ(GetUInt64Value(batch, "shared_blks_hit", 0), 100u);
  EXPECT_EQ(GetUInt64Value(batch, "shared_blks_read", 0), 5u);
  EXPECT_EQ(GetUInt64Value(batch, "shared_blks_written", 0), 1u);
  EXPECT_EQ(GetUInt64Value(batch, "shared_blks_dirtied", 0), 2u);
  EXPECT_EQ(GetUInt64Value(batch, "shared_blk_read_time_us", 0), 500u);
  EXPECT_EQ(GetUInt64Value(batch, "shared_blk_write_time_us", 0), 100u);
  EXPECT_EQ(GetUInt64Value(batch, "wal_records", 0), 7u);
  EXPECT_EQ(GetUInt64Value(batch, "wal_fpi", 0), 1u);
  EXPECT_EQ(GetUInt64Value(batch, "wal_bytes", 0), 4096u);
  EXPECT_EQ(GetUInt64Value(batch, "cpu_user_time_us", 0), 3000u);
  EXPECT_EQ(GetUInt64Value(batch, "cpu_sys_time_us", 0), 500u);

  // uint32 fields
  EXPECT_EQ(GetUInt32Value(batch, "parallel_workers_planned", 0), 2u);
  EXPECT_EQ(GetUInt32Value(batch, "parallel_workers_launched", 0), 1u);

  // int32 fields
  EXPECT_EQ(GetInt32Value(batch, "err_elevel", 0), 0);
}

TEST(ArrowBatchBuilder, TimestampConversion) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init("", ""));

  PschEvent ev = MakeTestEvent();
  // ts_start is microseconds since PostgreSQL epoch (2000-01-01).
  // PG epoch offset = 946684800000000 us.
  // Expected: (ts_start + offset) * 1000 = nanoseconds since Unix epoch.
  ev.ts_start = 1000000;  // 1 second past PG epoch
  ASSERT_TRUE(builder.Append(ev));

  auto result = builder.Finish();
  auto batch = ReadIpcBuffer(result.ipc_buffer);
  ASSERT_NE(batch, nullptr);

  int64_t expected_ns = (1000000LL + 946684800000000LL) * 1000LL;
  EXPECT_EQ(GetTimestampNanos(batch, 0), expected_ns);
}

TEST(ArrowBatchBuilder, NegativeValuesClamped) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init("", ""));

  PschEvent ev = MakeTestEvent();
  ev.shared_blks_hit = -5;    // Should clamp to 0
  ev.shared_blks_read = -10;  // Should clamp to 0
  ev.wal_records = -1;        // Should clamp to 0
  ev.cpu_user_time_us = -99;  // Should clamp to 0
  ASSERT_TRUE(builder.Append(ev));

  auto result = builder.Finish();
  auto batch = ReadIpcBuffer(result.ipc_buffer);
  ASSERT_NE(batch, nullptr);

  EXPECT_EQ(GetUInt64Value(batch, "shared_blks_hit", 0), 0u);
  EXPECT_EQ(GetUInt64Value(batch, "shared_blks_read", 0), 0u);
  EXPECT_EQ(GetUInt64Value(batch, "wal_records", 0), 0u);
  EXPECT_EQ(GetUInt64Value(batch, "cpu_user_time_us", 0), 0u);
}

TEST(ArrowBatchBuilder, ExtraAttributesParsed) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(
      builder.Init("region:us-east-1;cell:a;instance_ubid:inst-123;server_ubid:srv-456;"
                   "server_role:primary;host_id:h-789;pod_name:pod-abc",
                   "2.0.0"));

  PschEvent ev = MakeTestEvent();
  ASSERT_TRUE(builder.Append(ev));

  auto result = builder.Finish();
  auto batch = ReadIpcBuffer(result.ipc_buffer);
  ASSERT_NE(batch, nullptr);

  EXPECT_EQ(GetStringValue(batch, "region", 0), "us-east-1");
  EXPECT_EQ(GetStringValue(batch, "cell", 0), "a");
  EXPECT_EQ(GetStringValue(batch, "instance_ubid", 0), "inst-123");
  EXPECT_EQ(GetStringValue(batch, "server_ubid", 0), "srv-456");
  EXPECT_EQ(GetStringValue(batch, "server_role", 0), "primary");
  EXPECT_EQ(GetStringValue(batch, "host_id", 0), "h-789");
  EXPECT_EQ(GetStringValue(batch, "pod_name", 0), "pod-abc");
  EXPECT_EQ(GetStringValue(batch, "service_version", 0), "2.0.0");
}

TEST(ArrowBatchBuilder, ExtraAttributesWithWhitespace) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init("  region : us-west-2 ; cell : b ", ""));

  PschEvent ev = MakeTestEvent();
  ASSERT_TRUE(builder.Append(ev));

  auto result = builder.Finish();
  auto batch = ReadIpcBuffer(result.ipc_buffer);
  ASSERT_NE(batch, nullptr);

  EXPECT_EQ(GetStringValue(batch, "region", 0), "us-west-2");
  EXPECT_EQ(GetStringValue(batch, "cell", 0), "b");
}

TEST(ArrowBatchBuilder, MultipleEvents) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init("", ""));

  constexpr int kNumEvents = 100;
  PschEvent ev = MakeTestEvent();
  for (int i = 0; i < kNumEvents; ++i) {
    ev.duration_us = static_cast<uint64>(i * 1000);
    ev.rows = static_cast<uint64>(i);
    ASSERT_TRUE(builder.Append(ev)) << "failed at event " << i;
  }
  EXPECT_EQ(builder.NumRows(), kNumEvents);

  auto result = builder.Finish();
  ASSERT_NE(result.ipc_buffer, nullptr);
  EXPECT_EQ(result.num_rows, kNumEvents);

  auto batch = ReadIpcBuffer(result.ipc_buffer);
  ASSERT_NE(batch, nullptr);
  EXPECT_EQ(batch->num_rows(), kNumEvents);

  // Verify first and last row values.
  EXPECT_EQ(GetUInt64Value(batch, "duration_us", 0), 0u);
  EXPECT_EQ(GetUInt64Value(batch, "rows", 0), 0u);
  EXPECT_EQ(GetUInt64Value(batch, "duration_us", kNumEvents - 1),
            static_cast<uint64_t>((kNumEvents - 1) * 1000));
  EXPECT_EQ(GetUInt64Value(batch, "rows", kNumEvents - 1), static_cast<uint64_t>(kNumEvents - 1));
}

TEST(ArrowBatchBuilder, ResetProducesValidSecondBatch) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init("region:eu-west-1", "1.0.0"));

  // First batch
  PschEvent ev1 = MakeTestEvent();
  ev1.duration_us = 111;
  ASSERT_TRUE(builder.Append(ev1));
  auto result1 = builder.Finish();
  ASSERT_NE(result1.ipc_buffer, nullptr);

  // Reset and build a second batch
  builder.Reset();
  EXPECT_EQ(builder.NumRows(), 0);

  PschEvent ev2 = MakeTestEvent();
  ev2.duration_us = 222;
  ASSERT_TRUE(builder.Append(ev2));
  auto result2 = builder.Finish();
  ASSERT_NE(result2.ipc_buffer, nullptr);
  EXPECT_EQ(result2.num_rows, 1);

  // Verify second batch has the new value, not the old one.
  auto batch2 = ReadIpcBuffer(result2.ipc_buffer);
  ASSERT_NE(batch2, nullptr);
  EXPECT_EQ(GetUInt64Value(batch2, "duration_us", 0), 222u);
  EXPECT_EQ(GetStringValue(batch2, "region", 0), "eu-west-1");
}

TEST(ArrowBatchBuilder, ErrorEvent) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init("", ""));

  PschEvent ev = MakeTestEvent();
  std::memcpy(ev.err_sqlstate, "42P01", 5);
  ev.err_sqlstate[5] = '\0';
  ev.err_elevel = 21;  // ERROR
  const char* err_msg = "relation \"foo\" does not exist";
  ev.err_message_len = static_cast<uint16>(std::strlen(err_msg));
  std::memcpy(ev.err_message, err_msg, ev.err_message_len);

  ASSERT_TRUE(builder.Append(ev));
  auto result = builder.Finish();
  auto batch = ReadIpcBuffer(result.ipc_buffer);
  ASSERT_NE(batch, nullptr);

  EXPECT_EQ(GetStringValue(batch, "err_sqlstate", 0), "42P01");
  EXPECT_EQ(GetInt32Value(batch, "err_elevel", 0), 21);
  EXPECT_EQ(GetStringValue(batch, "err_message", 0), "relation \"foo\" does not exist");
}

TEST(ArrowBatchBuilder, NullExtraAttributes) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init(nullptr, nullptr));

  PschEvent ev = MakeTestEvent();
  ASSERT_TRUE(builder.Append(ev));

  auto result = builder.Finish();
  auto batch = ReadIpcBuffer(result.ipc_buffer);
  ASSERT_NE(batch, nullptr);

  // Extra attribute columns should be empty strings, not crash.
  EXPECT_EQ(GetStringValue(batch, "region", 0), "");
  EXPECT_EQ(GetStringValue(batch, "service_version", 0), "");
}

TEST(ArrowBatchBuilder, EstimatedBytesGrows) {
  ArrowBatchBuilder builder;
  ASSERT_TRUE(builder.Init("", ""));

  size_t initial = builder.EstimatedBytes();
  PschEvent ev = MakeTestEvent();
  ASSERT_TRUE(builder.Append(ev));
  size_t after_one = builder.EstimatedBytes();
  EXPECT_GT(after_one, initial);

  ASSERT_TRUE(builder.Append(ev));
  size_t after_two = builder.EstimatedBytes();
  EXPECT_GT(after_two, after_one);
}

TEST(ArrowBatchBuilder, CmdTypeStrings) {
  // Verify all command types serialize correctly.
  struct TestCase {
    PschCmdType cmd;
    const char* expected;
  };
  TestCase cases[] = {
      {PSCH_CMD_SELECT, "SELECT"},   {PSCH_CMD_UPDATE, "UPDATE"},   {PSCH_CMD_INSERT, "INSERT"},
      {PSCH_CMD_DELETE, "DELETE"},   {PSCH_CMD_MERGE, "MERGE"},     {PSCH_CMD_UTILITY, "UTILITY"},
      {PSCH_CMD_NOTHING, "NOTHING"}, {PSCH_CMD_UNKNOWN, "UNKNOWN"},
  };

  for (const auto& tc : cases) {
    ArrowBatchBuilder builder;
    ASSERT_TRUE(builder.Init("", ""));
    PschEvent ev = MakeTestEvent();
    ev.cmd_type = tc.cmd;
    ASSERT_TRUE(builder.Append(ev));
    auto result = builder.Finish();
    auto batch = ReadIpcBuffer(result.ipc_buffer);
    ASSERT_NE(batch, nullptr) << "cmd=" << tc.expected;
    EXPECT_EQ(GetStringValue(batch, "db_operation", 0), tc.expected);
  }
}

}  // namespace
