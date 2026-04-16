extern "C" {
#include "postgres.h"
}

#include "queue/event.h"

#include <arrow/array/array_dict.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_dict.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>

#include <algorithm>
#include <cctype>
#include <cinttypes>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "export/arrow_batch.h"
#include "export/exporter_interface.h"

namespace {

using DictBuilder = arrow::StringDictionary32Builder;

constexpr int64_t kPostgresEpochOffsetUs = 946684800000000LL;
constexpr size_t kFixedBytesPerRow = 512;
constexpr size_t kIpcEnvelopeBytes = 1024;

std::shared_ptr<arrow::DataType> TimestampType() {
  static const auto kType = arrow::timestamp(arrow::TimeUnit::NANO, "UTC");
  return kType;
}

std::shared_ptr<arrow::DataType> DictionaryUtf8Type() {
  static const auto kType = arrow::dictionary(arrow::int32(), arrow::utf8());
  return kType;
}

void LogArrowFailure(const char* context, const arrow::Status& status) {
  elog(WARNING, "pg_stat_ch: %s: %s", context, status.ToString().c_str());
}

template <typename LenT>
LenT ClampFieldLen(LenT len, LenT max, const char* field_name) {
  if (len <= max) {
    return len;
  }
  elog(WARNING, "pg_stat_ch: invalid %s %u, clamping", field_name, static_cast<unsigned>(len));
  return max;
}

template <typename BuilderT, typename ValueT>
bool AppendScalar(BuilderT* builder, ValueT value, const char* context) {
  const arrow::Status status = builder->Append(value);
  if (!status.ok()) {
    LogArrowFailure(context, status);
    return false;
  }
  return true;
}

bool AppendString(arrow::StringBuilder* builder, std::string_view value, const char* context) {
  const char* data = value.empty() ? "" : value.data();
  const arrow::Status status = builder->Append(data, static_cast<int32_t>(value.size()));
  if (!status.ok()) {
    LogArrowFailure(context, status);
    return false;
  }
  return true;
}

bool AppendString(DictBuilder* builder, std::string_view value, const char* context) {
  const char* data = value.empty() ? "" : value.data();
  const arrow::Status status = builder->Append(data, static_cast<int32_t>(value.size()));
  if (!status.ok()) {
    LogArrowFailure(context, status);
    return false;
  }
  return true;
}

uint64_t ClampSignedToUint64(int64_t value, const char* column_name) {
  if (value < 0) {
    LogNegativeValue(column_name, value);
    return 0;
  }
  return static_cast<uint64_t>(value);
}

uint32_t ClampSignedToUint32(int32_t value, const char* column_name) {
  if (value < 0) {
    LogNegativeValue(column_name, value);
    return 0;
  }
  return static_cast<uint32_t>(value);
}

std::string_view TrimAsciiWhitespace(std::string_view input) {
  while (!input.empty() && std::isspace(static_cast<unsigned char>(input.front())) != 0) {
    input.remove_prefix(1);
  }
  while (!input.empty() && std::isspace(static_cast<unsigned char>(input.back())) != 0) {
    input.remove_suffix(1);
  }
  return input;
}

bool FinishArray(arrow::ArrayBuilder* builder, const char* context,
                 std::shared_ptr<arrow::Array>* out) {
  const arrow::Status status = builder->Finish(out);
  if (!status.ok()) {
    LogArrowFailure(context, status);
    return false;
  }
  return true;
}

bool FinishDictionaryArray(DictBuilder* builder, const char* context,
                           std::shared_ptr<arrow::Array>* out) {
  std::shared_ptr<arrow::DictionaryArray> dict_array;
  const arrow::Status status = builder->Finish(&dict_array);
  if (!status.ok()) {
    LogArrowFailure(context, status);
    return false;
  }
  *out = std::static_pointer_cast<arrow::Array>(std::move(dict_array));
  return true;
}

}  // namespace

struct ArrowBatchBuilder::Impl {
  std::shared_ptr<arrow::Schema> schema;

  arrow::TimestampBuilder ts_builder;
  DictBuilder severity_builder;
  arrow::StringBuilder body_builder;
  arrow::StringBuilder trace_id_builder;
  arrow::StringBuilder span_id_builder;
  arrow::StringBuilder query_id_builder;
  DictBuilder db_name_builder;
  DictBuilder db_user_builder;
  DictBuilder db_operation_builder;
  DictBuilder app_builder;
  DictBuilder client_addr_builder;
  arrow::StringBuilder query_text_builder;
  arrow::StringBuilder pid_builder;
  arrow::StringBuilder err_message_builder;
  DictBuilder err_sqlstate_builder;
  arrow::Int32Builder err_elevel_builder;
  arrow::UInt64Builder duration_us_builder;
  arrow::UInt64Builder rows_builder;
  arrow::UInt64Builder shared_blks_hit_builder;
  arrow::UInt64Builder shared_blks_read_builder;
  arrow::UInt64Builder shared_blks_written_builder;
  arrow::UInt64Builder shared_blks_dirtied_builder;
  arrow::UInt64Builder shared_blk_read_time_us_builder;
  arrow::UInt64Builder shared_blk_write_time_us_builder;
  arrow::UInt64Builder local_blks_hit_builder;
  arrow::UInt64Builder local_blks_read_builder;
  arrow::UInt64Builder local_blks_written_builder;
  arrow::UInt64Builder local_blks_dirtied_builder;
  arrow::UInt64Builder local_blk_read_time_us_builder;
  arrow::UInt64Builder local_blk_write_time_us_builder;
  arrow::UInt64Builder temp_blks_read_builder;
  arrow::UInt64Builder temp_blks_written_builder;
  arrow::UInt64Builder temp_blk_read_time_us_builder;
  arrow::UInt64Builder temp_blk_write_time_us_builder;
  arrow::UInt64Builder wal_records_builder;
  arrow::UInt64Builder wal_bytes_builder;
  arrow::UInt64Builder wal_fpi_builder;
  arrow::UInt64Builder cpu_user_time_us_builder;
  arrow::UInt64Builder cpu_sys_time_us_builder;
  arrow::UInt64Builder jit_functions_builder;
  arrow::UInt64Builder jit_generation_time_us_builder;
  arrow::UInt64Builder jit_inlining_time_us_builder;
  arrow::UInt64Builder jit_optimization_time_us_builder;
  arrow::UInt64Builder jit_emission_time_us_builder;
  arrow::UInt64Builder jit_deform_time_us_builder;
  arrow::UInt32Builder parallel_workers_planned_builder;
  arrow::UInt32Builder parallel_workers_launched_builder;
  arrow::StringBuilder instance_ubid_builder;
  arrow::StringBuilder server_ubid_builder;
  DictBuilder server_role_builder;
  DictBuilder region_builder;
  DictBuilder cell_builder;
  DictBuilder service_version_builder;
  arrow::StringBuilder host_id_builder;
  arrow::StringBuilder pod_name_builder;

  std::unordered_map<std::string, std::string> extra_attrs;
  std::string service_version;
  int num_rows = 0;
  size_t estimated_bytes = 0;

  Impl() : ts_builder(TimestampType(), arrow::default_memory_pool()) {}

  bool Init(const char* extra_attrs_input, const char* service_version_input) {
    service_version = service_version_input != nullptr ? service_version_input : "";
    ParseExtraAttributes(extra_attrs_input != nullptr ? extra_attrs_input : "");
    schema = arrow::schema({
        arrow::field("ts", TimestampType()),
        arrow::field("severity", DictionaryUtf8Type()),
        arrow::field("body", arrow::utf8()),
        arrow::field("trace_id", arrow::utf8()),
        arrow::field("span_id", arrow::utf8()),
        arrow::field("query_id", arrow::utf8()),
        arrow::field("db_name", DictionaryUtf8Type()),
        arrow::field("db_user", DictionaryUtf8Type()),
        arrow::field("db_operation", DictionaryUtf8Type()),
        arrow::field("app", DictionaryUtf8Type()),
        arrow::field("client_addr", DictionaryUtf8Type()),
        arrow::field("query_text", arrow::utf8()),
        arrow::field("pid", arrow::utf8()),
        arrow::field("err_message", arrow::utf8()),
        arrow::field("err_sqlstate", DictionaryUtf8Type()),
        arrow::field("err_elevel", arrow::int32()),
        arrow::field("duration_us", arrow::uint64()),
        arrow::field("rows", arrow::uint64()),
        arrow::field("shared_blks_hit", arrow::uint64()),
        arrow::field("shared_blks_read", arrow::uint64()),
        arrow::field("shared_blks_written", arrow::uint64()),
        arrow::field("shared_blks_dirtied", arrow::uint64()),
        arrow::field("shared_blk_read_time_us", arrow::uint64()),
        arrow::field("shared_blk_write_time_us", arrow::uint64()),
        arrow::field("local_blks_hit", arrow::uint64()),
        arrow::field("local_blks_read", arrow::uint64()),
        arrow::field("local_blks_written", arrow::uint64()),
        arrow::field("local_blks_dirtied", arrow::uint64()),
        arrow::field("local_blk_read_time_us", arrow::uint64()),
        arrow::field("local_blk_write_time_us", arrow::uint64()),
        arrow::field("temp_blks_read", arrow::uint64()),
        arrow::field("temp_blks_written", arrow::uint64()),
        arrow::field("temp_blk_read_time_us", arrow::uint64()),
        arrow::field("temp_blk_write_time_us", arrow::uint64()),
        arrow::field("wal_records", arrow::uint64()),
        arrow::field("wal_bytes", arrow::uint64()),
        arrow::field("wal_fpi", arrow::uint64()),
        arrow::field("cpu_user_time_us", arrow::uint64()),
        arrow::field("cpu_sys_time_us", arrow::uint64()),
        arrow::field("jit_functions", arrow::uint64()),
        arrow::field("jit_generation_time_us", arrow::uint64()),
        arrow::field("jit_inlining_time_us", arrow::uint64()),
        arrow::field("jit_optimization_time_us", arrow::uint64()),
        arrow::field("jit_emission_time_us", arrow::uint64()),
        arrow::field("jit_deform_time_us", arrow::uint64()),
        arrow::field("parallel_workers_planned", arrow::uint32()),
        arrow::field("parallel_workers_launched", arrow::uint32()),
        arrow::field("instance_ubid", arrow::utf8()),
        arrow::field("server_ubid", arrow::utf8()),
        arrow::field("server_role", DictionaryUtf8Type()),
        arrow::field("region", DictionaryUtf8Type()),
        arrow::field("cell", DictionaryUtf8Type()),
        arrow::field("service_version", DictionaryUtf8Type()),
        arrow::field("host_id", arrow::utf8()),
        arrow::field("pod_name", arrow::utf8()),
    });
    Reset();
    return true;
  }

  bool Append(const PschEvent& event) {
    if (!AppendScalar(&ts_builder,
                      static_cast<int64_t>((event.ts_start + kPostgresEpochOffsetUs) * 1000LL),
                      "Arrow ts append")) {
      return false;
    }
    if (!AppendString(&severity_builder, "", "Arrow severity append") ||
        !AppendString(&body_builder, "", "Arrow body append") ||
        !AppendString(&trace_id_builder, "", "Arrow trace_id append") ||
        !AppendString(&span_id_builder, "", "Arrow span_id append")) {
      return false;
    }

    const auto datname_len =
        ClampFieldLen(event.datname_len, static_cast<uint8>(sizeof(event.datname)), "datname_len");
    const auto username_len = ClampFieldLen(
        event.username_len, static_cast<uint8>(sizeof(event.username)), "username_len");
    const auto app_len =
        ClampFieldLen(event.application_name_len, static_cast<uint8>(PSCH_MAX_APP_NAME_LEN),
                      "application_name_len");
    const auto client_addr_len = ClampFieldLen(
        event.client_addr_len, static_cast<uint8>(PSCH_MAX_CLIENT_ADDR_LEN), "client_addr_len");
    const auto query_len =
        ClampFieldLen(event.query_len, static_cast<uint16>(PSCH_MAX_QUERY_LEN), "query_len");
    const auto err_message_len = ClampFieldLen(
        event.err_message_len, static_cast<uint16>(PSCH_MAX_ERR_MSG_LEN), "err_message_len");
    const auto err_sqlstate_len =
        static_cast<size_t>(strnlen(event.err_sqlstate, sizeof(event.err_sqlstate) - 1));

    const std::string_view db_name(event.datname, datname_len);
    const std::string_view db_user(event.username, username_len);
    const std::string_view app(event.application_name, app_len);
    const std::string_view client_addr(event.client_addr, client_addr_len);
    const std::string_view query_text(event.query, query_len);
    const std::string_view err_message(event.err_message, err_message_len);
    const std::string_view err_sqlstate(event.err_sqlstate, err_sqlstate_len);

    char queryid_buf[24];
    snprintf(queryid_buf, sizeof(queryid_buf), "%" PRIu64, static_cast<uint64_t>(event.queryid));
    char pid_buf[12];
    snprintf(pid_buf, sizeof(pid_buf), "%d", event.pid);

    if (!AppendString(&query_id_builder, queryid_buf, "Arrow query_id append") ||
        !AppendString(&db_name_builder, db_name, "Arrow db_name append") ||
        !AppendString(&db_user_builder, db_user, "Arrow db_user append") ||
        !AppendString(&db_operation_builder, PschCmdTypeToString(event.cmd_type),
                      "Arrow db_operation append") ||
        !AppendString(&app_builder, app, "Arrow app append") ||
        !AppendString(&client_addr_builder, client_addr, "Arrow client_addr append") ||
        !AppendString(&query_text_builder, query_text, "Arrow query_text append") ||
        !AppendString(&pid_builder, pid_buf, "Arrow pid append") ||
        !AppendString(&err_message_builder, err_message, "Arrow err_message append") ||
        !AppendString(&err_sqlstate_builder, err_sqlstate, "Arrow err_sqlstate append")) {
      return false;
    }

    if (!AppendScalar(&err_elevel_builder, static_cast<int32_t>(event.err_elevel),
                      "Arrow err_elevel append") ||
        !AppendScalar(&duration_us_builder, event.duration_us, "Arrow duration_us append") ||
        !AppendScalar(&rows_builder, event.rows, "Arrow rows append") ||
        !AppendScalar(&shared_blks_hit_builder,
                      ClampSignedToUint64(event.shared_blks_hit, "shared_blks_hit"),
                      "Arrow shared_blks_hit append") ||
        !AppendScalar(&shared_blks_read_builder,
                      ClampSignedToUint64(event.shared_blks_read, "shared_blks_read"),
                      "Arrow shared_blks_read append") ||
        !AppendScalar(&shared_blks_written_builder,
                      ClampSignedToUint64(event.shared_blks_written, "shared_blks_written"),
                      "Arrow shared_blks_written append") ||
        !AppendScalar(&shared_blks_dirtied_builder,
                      ClampSignedToUint64(event.shared_blks_dirtied, "shared_blks_dirtied"),
                      "Arrow shared_blks_dirtied append") ||
        !AppendScalar(&shared_blk_read_time_us_builder,
                      ClampSignedToUint64(event.shared_blk_read_time_us, "shared_blk_read_time_us"),
                      "Arrow shared_blk_read_time_us append") ||
        !AppendScalar(
            &shared_blk_write_time_us_builder,
            ClampSignedToUint64(event.shared_blk_write_time_us, "shared_blk_write_time_us"),
            "Arrow shared_blk_write_time_us append") ||
        !AppendScalar(&local_blks_hit_builder,
                      ClampSignedToUint64(event.local_blks_hit, "local_blks_hit"),
                      "Arrow local_blks_hit append") ||
        !AppendScalar(&local_blks_read_builder,
                      ClampSignedToUint64(event.local_blks_read, "local_blks_read"),
                      "Arrow local_blks_read append") ||
        !AppendScalar(&local_blks_written_builder,
                      ClampSignedToUint64(event.local_blks_written, "local_blks_written"),
                      "Arrow local_blks_written append") ||
        !AppendScalar(&local_blks_dirtied_builder,
                      ClampSignedToUint64(event.local_blks_dirtied, "local_blks_dirtied"),
                      "Arrow local_blks_dirtied append") ||
        !AppendScalar(&local_blk_read_time_us_builder,
                      ClampSignedToUint64(event.local_blk_read_time_us, "local_blk_read_time_us"),
                      "Arrow local_blk_read_time_us append") ||
        !AppendScalar(&local_blk_write_time_us_builder,
                      ClampSignedToUint64(event.local_blk_write_time_us, "local_blk_write_time_us"),
                      "Arrow local_blk_write_time_us append") ||
        !AppendScalar(&temp_blks_read_builder,
                      ClampSignedToUint64(event.temp_blks_read, "temp_blks_read"),
                      "Arrow temp_blks_read append") ||
        !AppendScalar(&temp_blks_written_builder,
                      ClampSignedToUint64(event.temp_blks_written, "temp_blks_written"),
                      "Arrow temp_blks_written append") ||
        !AppendScalar(&temp_blk_read_time_us_builder,
                      ClampSignedToUint64(event.temp_blk_read_time_us, "temp_blk_read_time_us"),
                      "Arrow temp_blk_read_time_us append") ||
        !AppendScalar(&temp_blk_write_time_us_builder,
                      ClampSignedToUint64(event.temp_blk_write_time_us, "temp_blk_write_time_us"),
                      "Arrow temp_blk_write_time_us append") ||
        !AppendScalar(&wal_records_builder, ClampSignedToUint64(event.wal_records, "wal_records"),
                      "Arrow wal_records append") ||
        !AppendScalar(&wal_bytes_builder, event.wal_bytes, "Arrow wal_bytes append") ||
        !AppendScalar(&wal_fpi_builder, ClampSignedToUint64(event.wal_fpi, "wal_fpi"),
                      "Arrow wal_fpi append") ||
        !AppendScalar(&cpu_user_time_us_builder,
                      ClampSignedToUint64(event.cpu_user_time_us, "cpu_user_time_us"),
                      "Arrow cpu_user_time_us append") ||
        !AppendScalar(&cpu_sys_time_us_builder,
                      ClampSignedToUint64(event.cpu_sys_time_us, "cpu_sys_time_us"),
                      "Arrow cpu_sys_time_us append") ||
        !AppendScalar(&jit_functions_builder,
                      ClampSignedToUint64(event.jit_functions, "jit_functions"),
                      "Arrow jit_functions append") ||
        !AppendScalar(&jit_generation_time_us_builder,
                      ClampSignedToUint64(event.jit_generation_time_us, "jit_generation_time_us"),
                      "Arrow jit_generation_time_us append") ||
        !AppendScalar(&jit_inlining_time_us_builder,
                      ClampSignedToUint64(event.jit_inlining_time_us, "jit_inlining_time_us"),
                      "Arrow jit_inlining_time_us append") ||
        !AppendScalar(
            &jit_optimization_time_us_builder,
            ClampSignedToUint64(event.jit_optimization_time_us, "jit_optimization_time_us"),
            "Arrow jit_optimization_time_us append") ||
        !AppendScalar(&jit_emission_time_us_builder,
                      ClampSignedToUint64(event.jit_emission_time_us, "jit_emission_time_us"),
                      "Arrow jit_emission_time_us append") ||
        !AppendScalar(&jit_deform_time_us_builder,
                      ClampSignedToUint64(event.jit_deform_time_us, "jit_deform_time_us"),
                      "Arrow jit_deform_time_us append") ||
        !AppendScalar(
            &parallel_workers_planned_builder,
            ClampSignedToUint32(event.parallel_workers_planned, "parallel_workers_planned"),
            "Arrow parallel_workers_planned append") ||
        !AppendScalar(
            &parallel_workers_launched_builder,
            ClampSignedToUint32(event.parallel_workers_launched, "parallel_workers_launched"),
            "Arrow parallel_workers_launched append")) {
      return false;
    }

    if (!AppendString(&instance_ubid_builder, ExtraAttr("instance_ubid"),
                      "Arrow instance_ubid append") ||
        !AppendString(&server_ubid_builder, ExtraAttr("server_ubid"), "Arrow server_ubid append") ||
        !AppendString(&server_role_builder, ExtraAttr("server_role"), "Arrow server_role append") ||
        !AppendString(&region_builder, ExtraAttr("region"), "Arrow region append") ||
        !AppendString(&cell_builder, ExtraAttr("cell"), "Arrow cell append") ||
        !AppendString(&service_version_builder, service_version, "Arrow service_version append") ||
        !AppendString(&host_id_builder, ExtraAttr("host_id"), "Arrow host_id append") ||
        !AppendString(&pod_name_builder, ExtraAttr("pod_name"), "Arrow pod_name append")) {
      return false;
    }

    estimated_bytes +=
        kFixedBytesPerRow + db_name.size() + db_user.size() + app.size() + client_addr.size() +
        query_text.size() + err_message.size() + err_sqlstate.size() + service_version.size() +
        ExtraAttr("instance_ubid").size() + ExtraAttr("server_ubid").size() +
        ExtraAttr("server_role").size() + ExtraAttr("region").size() + ExtraAttr("cell").size() +
        ExtraAttr("host_id").size() + ExtraAttr("pod_name").size();
    ++num_rows;
    return true;
  }

  ArrowBatchBuilder::FinishResult Finish() {
    ArrowBatchBuilder::FinishResult result;
    result.num_rows = num_rows;
    if (num_rows == 0 || schema == nullptr) {
      return result;
    }

    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(schema->num_fields());

    auto add_array = [&](arrow::ArrayBuilder* builder, const char* context) -> bool {
      std::shared_ptr<arrow::Array> array;
      if (!FinishArray(builder, context, &array)) {
        return false;
      }
      columns.push_back(std::move(array));
      return true;
    };
    auto add_dict_array = [&](DictBuilder* builder, const char* context) -> bool {
      std::shared_ptr<arrow::Array> array;
      if (!FinishDictionaryArray(builder, context, &array)) {
        return false;
      }
      columns.push_back(std::move(array));
      return true;
    };

    if (!add_array(&ts_builder, "Arrow ts finish") ||
        !add_dict_array(&severity_builder, "Arrow severity finish") ||
        !add_array(&body_builder, "Arrow body finish") ||
        !add_array(&trace_id_builder, "Arrow trace_id finish") ||
        !add_array(&span_id_builder, "Arrow span_id finish") ||
        !add_array(&query_id_builder, "Arrow query_id finish") ||
        !add_dict_array(&db_name_builder, "Arrow db_name finish") ||
        !add_dict_array(&db_user_builder, "Arrow db_user finish") ||
        !add_dict_array(&db_operation_builder, "Arrow db_operation finish") ||
        !add_dict_array(&app_builder, "Arrow app finish") ||
        !add_dict_array(&client_addr_builder, "Arrow client_addr finish") ||
        !add_array(&query_text_builder, "Arrow query_text finish") ||
        !add_array(&pid_builder, "Arrow pid finish") ||
        !add_array(&err_message_builder, "Arrow err_message finish") ||
        !add_dict_array(&err_sqlstate_builder, "Arrow err_sqlstate finish") ||
        !add_array(&err_elevel_builder, "Arrow err_elevel finish") ||
        !add_array(&duration_us_builder, "Arrow duration_us finish") ||
        !add_array(&rows_builder, "Arrow rows finish") ||
        !add_array(&shared_blks_hit_builder, "Arrow shared_blks_hit finish") ||
        !add_array(&shared_blks_read_builder, "Arrow shared_blks_read finish") ||
        !add_array(&shared_blks_written_builder, "Arrow shared_blks_written finish") ||
        !add_array(&shared_blks_dirtied_builder, "Arrow shared_blks_dirtied finish") ||
        !add_array(&shared_blk_read_time_us_builder, "Arrow shared_blk_read_time_us finish") ||
        !add_array(&shared_blk_write_time_us_builder, "Arrow shared_blk_write_time_us finish") ||
        !add_array(&local_blks_hit_builder, "Arrow local_blks_hit finish") ||
        !add_array(&local_blks_read_builder, "Arrow local_blks_read finish") ||
        !add_array(&local_blks_written_builder, "Arrow local_blks_written finish") ||
        !add_array(&local_blks_dirtied_builder, "Arrow local_blks_dirtied finish") ||
        !add_array(&local_blk_read_time_us_builder, "Arrow local_blk_read_time_us finish") ||
        !add_array(&local_blk_write_time_us_builder, "Arrow local_blk_write_time_us finish") ||
        !add_array(&temp_blks_read_builder, "Arrow temp_blks_read finish") ||
        !add_array(&temp_blks_written_builder, "Arrow temp_blks_written finish") ||
        !add_array(&temp_blk_read_time_us_builder, "Arrow temp_blk_read_time_us finish") ||
        !add_array(&temp_blk_write_time_us_builder, "Arrow temp_blk_write_time_us finish") ||
        !add_array(&wal_records_builder, "Arrow wal_records finish") ||
        !add_array(&wal_bytes_builder, "Arrow wal_bytes finish") ||
        !add_array(&wal_fpi_builder, "Arrow wal_fpi finish") ||
        !add_array(&cpu_user_time_us_builder, "Arrow cpu_user_time_us finish") ||
        !add_array(&cpu_sys_time_us_builder, "Arrow cpu_sys_time_us finish") ||
        !add_array(&jit_functions_builder, "Arrow jit_functions finish") ||
        !add_array(&jit_generation_time_us_builder, "Arrow jit_generation_time_us finish") ||
        !add_array(&jit_inlining_time_us_builder, "Arrow jit_inlining_time_us finish") ||
        !add_array(&jit_optimization_time_us_builder, "Arrow jit_optimization_time_us finish") ||
        !add_array(&jit_emission_time_us_builder, "Arrow jit_emission_time_us finish") ||
        !add_array(&jit_deform_time_us_builder, "Arrow jit_deform_time_us finish") ||
        !add_array(&parallel_workers_planned_builder, "Arrow parallel_workers_planned finish") ||
        !add_array(&parallel_workers_launched_builder, "Arrow parallel_workers_launched finish") ||
        !add_array(&instance_ubid_builder, "Arrow instance_ubid finish") ||
        !add_array(&server_ubid_builder, "Arrow server_ubid finish") ||
        !add_dict_array(&server_role_builder, "Arrow server_role finish") ||
        !add_dict_array(&region_builder, "Arrow region finish") ||
        !add_dict_array(&cell_builder, "Arrow cell finish") ||
        !add_dict_array(&service_version_builder, "Arrow service_version finish") ||
        !add_array(&host_id_builder, "Arrow host_id finish") ||
        !add_array(&pod_name_builder, "Arrow pod_name finish")) {
      return {};
    }

    const auto batch = arrow::RecordBatch::Make(schema, num_rows, std::move(columns));

    auto sink_result = arrow::io::BufferOutputStream::Create(kIpcEnvelopeBytes);
    if (!sink_result.ok()) {
      LogArrowFailure("Arrow buffer sink create", sink_result.status());
      return {};
    }
    auto sink = std::move(sink_result).ValueOrDie();

    auto codec_result = arrow::util::Codec::Create(arrow::Compression::ZSTD);
    if (!codec_result.ok()) {
      LogArrowFailure("Arrow codec create", codec_result.status());
      return {};
    }

    auto options = arrow::ipc::IpcWriteOptions::Defaults();
    auto codec = std::move(codec_result).ValueOrDie();
    options.codec = std::shared_ptr<arrow::util::Codec>(std::move(codec));

    auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema, options);
    if (!writer_result.ok()) {
      LogArrowFailure("Arrow stream writer create", writer_result.status());
      return {};
    }
    auto writer = std::move(writer_result).ValueOrDie();

    arrow::Status status = writer->WriteRecordBatch(*batch);
    if (!status.ok()) {
      LogArrowFailure("Arrow write record batch", status);
      return {};
    }
    status = writer->Close();
    if (!status.ok()) {
      LogArrowFailure("Arrow close writer", status);
      return {};
    }

    auto buffer_result = sink->Finish();
    if (!buffer_result.ok()) {
      LogArrowFailure("Arrow finish buffer", buffer_result.status());
      return {};
    }

    result.ipc_buffer = std::move(buffer_result).ValueOrDie();
    return result;
  }

  void Reset() {
    ts_builder.Reset();
    severity_builder.ResetFull();
    body_builder.Reset();
    trace_id_builder.Reset();
    span_id_builder.Reset();
    query_id_builder.Reset();
    db_name_builder.ResetFull();
    db_user_builder.ResetFull();
    db_operation_builder.ResetFull();
    app_builder.ResetFull();
    client_addr_builder.ResetFull();
    query_text_builder.Reset();
    pid_builder.Reset();
    err_message_builder.Reset();
    err_sqlstate_builder.ResetFull();
    err_elevel_builder.Reset();
    duration_us_builder.Reset();
    rows_builder.Reset();
    shared_blks_hit_builder.Reset();
    shared_blks_read_builder.Reset();
    shared_blks_written_builder.Reset();
    shared_blks_dirtied_builder.Reset();
    shared_blk_read_time_us_builder.Reset();
    shared_blk_write_time_us_builder.Reset();
    local_blks_hit_builder.Reset();
    local_blks_read_builder.Reset();
    local_blks_written_builder.Reset();
    local_blks_dirtied_builder.Reset();
    local_blk_read_time_us_builder.Reset();
    local_blk_write_time_us_builder.Reset();
    temp_blks_read_builder.Reset();
    temp_blks_written_builder.Reset();
    temp_blk_read_time_us_builder.Reset();
    temp_blk_write_time_us_builder.Reset();
    wal_records_builder.Reset();
    wal_bytes_builder.Reset();
    wal_fpi_builder.Reset();
    cpu_user_time_us_builder.Reset();
    cpu_sys_time_us_builder.Reset();
    jit_functions_builder.Reset();
    jit_generation_time_us_builder.Reset();
    jit_inlining_time_us_builder.Reset();
    jit_optimization_time_us_builder.Reset();
    jit_emission_time_us_builder.Reset();
    jit_deform_time_us_builder.Reset();
    parallel_workers_planned_builder.Reset();
    parallel_workers_launched_builder.Reset();
    instance_ubid_builder.Reset();
    server_ubid_builder.Reset();
    server_role_builder.ResetFull();
    region_builder.ResetFull();
    cell_builder.ResetFull();
    service_version_builder.ResetFull();
    host_id_builder.Reset();
    pod_name_builder.Reset();
    num_rows = 0;
    estimated_bytes = kIpcEnvelopeBytes;
  }

  std::string_view ExtraAttr(const char* key) const {
    const auto it = extra_attrs.find(key);
    if (it == extra_attrs.end()) {
      return {};
    }
    return it->second;
  }

  void ParseExtraAttributes(const char* raw_attrs) {
    extra_attrs.clear();
    std::string_view input = raw_attrs != nullptr ? raw_attrs : "";
    while (!input.empty()) {
      const size_t next_delim = input.find(';');
      const std::string_view token =
          next_delim == std::string_view::npos ? input : input.substr(0, next_delim);
      const size_t sep = token.find(':');
      if (sep != std::string_view::npos) {
        const std::string_view key = TrimAsciiWhitespace(token.substr(0, sep));
        const std::string_view value = TrimAsciiWhitespace(token.substr(sep + 1));
        if (!key.empty()) {
          extra_attrs.emplace(std::string(key), std::string(value));
        }
      }
      if (next_delim == std::string_view::npos) {
        break;
      }
      input.remove_prefix(next_delim + 1);
    }
  }
};

ArrowBatchBuilder::ArrowBatchBuilder() : impl_(std::make_unique<Impl>()) {}

ArrowBatchBuilder::~ArrowBatchBuilder() = default;

ArrowBatchBuilder::ArrowBatchBuilder(ArrowBatchBuilder&&) noexcept = default;

ArrowBatchBuilder& ArrowBatchBuilder::operator=(ArrowBatchBuilder&&) noexcept = default;

bool ArrowBatchBuilder::Init(const char* extra_attrs, const char* service_version) {
  return impl_->Init(extra_attrs, service_version);
}

bool ArrowBatchBuilder::Append(const PschEvent& event) {
  return impl_->Append(event);
}

ArrowBatchBuilder::FinishResult ArrowBatchBuilder::Finish() {
  return impl_->Finish();
}

void ArrowBatchBuilder::Reset() {
  impl_->Reset();
}

int ArrowBatchBuilder::NumRows() const {
  return impl_->num_rows;
}

size_t ArrowBatchBuilder::EstimatedBytes() const {
  return impl_->estimated_bytes;
}
