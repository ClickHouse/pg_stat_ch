// Export events_raw as ZSTD-compressed Arrow IPC over OTLP

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_dict.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "export/arrow_batch.h"
#include "export/arrow_dump.h"
#include "export/diagnostics.h"
#include "export/exporter_config.h"
#include "export/exporter_interface.h"
#include "export/otel_arrow_exporter.h"
#include "export/otel_exporter.h"

namespace {

using DictBuilder = arrow::StringDictionary32Builder;

// Match events_raw DateTime64(6, 'UTC')
std::shared_ptr<arrow::DataType> TimestampType() {
  static const auto kType = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
  return kType;
}

std::shared_ptr<arrow::DataType> DictUtf8Type() {
  static const auto kType = arrow::dictionary(arrow::int32(), arrow::utf8());
  return kType;
}

struct ArrowSlot {
  std::string name;
  std::shared_ptr<arrow::Field> field;
  std::shared_ptr<arrow::ArrayBuilder> builder;
};

// Keep first value for duplicate keys
class ExtraAttrs {
 public:
  explicit ExtraAttrs(const char* raw) {
    if (raw == nullptr) {
      return;
    }
    std::string_view input(raw);
    while (!input.empty()) {
      const size_t delim = input.find(';');
      const std::string_view token =
          (delim == std::string_view::npos) ? input : input.substr(0, delim);
      const size_t sep = token.find(':');
      if (sep != std::string_view::npos) {
        attrs_.emplace_back(std::string(token.substr(0, sep)), std::string(token.substr(sep + 1)));
      }
      if (delim == std::string_view::npos) {
        break;
      }
      input.remove_prefix(delim + 1);
    }
  }

  std::string Get(std::string_view key) const {
    for (const auto& [k, v] : attrs_) {
      if (k == key) {
        return v;
      }
    }
    return {};
  }

 private:
  std::vector<std::pair<std::string, std::string>> attrs_;
};

// ---------------------------------------------------------------------------

class OTelArrowExporter : public StatsExporter {
 public:
  OTelArrowExporter(const ExporterConfig* config, Diagnostics* diag)
      : StatsExporter(config, diag), inner_(MakeOpenTelemetryExporter(config, diag)) {}

  // -- Low-cardinality columns --
  shared_ptr<Column<string>> StatLCString(string_view n) final { return MakeDictStr(n); }
  shared_ptr<Column<uint8_t>> StatLCUInt8(string_view n) final {
    return MakeNum<uint8_t, arrow::UInt8Builder>(n, arrow::uint8());
  }
  shared_ptr<Column<int16_t>> StatLCInt16(string_view n) final {
    return MakeNum<int16_t, arrow::Int16Builder>(n, arrow::int16());
  }
  shared_ptr<Column<int32_t>> StatLCInt32(string_view n) final {
    return MakeNum<int32_t, arrow::Int32Builder>(n, arrow::int32());
  }

  // -- High-cardinality columns --
  shared_ptr<Column<string_view>> StatHCString(string_view n) final { return MakeUtf8Sv(n); }
  shared_ptr<Column<int64_t>> StatHCInt64(string_view n) final {
    return MakeNum<int64_t, arrow::Int64Builder>(n, arrow::int64());
  }
  shared_ptr<Column<uint64_t>> StatHCUInt64(string_view n) final {
    return MakeNum<uint64_t, arrow::UInt64Builder>(n, arrow::uint64());
  }

  shared_ptr<Column<int64_t>> StatTimestamp(string_view n) final { return MakeTimestamp(n); }

  // -- Semantic columns --
  shared_ptr<Column<string>> DbNameColumn() final { return MakeDictStr("db_name"); }
  shared_ptr<Column<string>> DbUserColumn() final { return MakeDictStr("db_user"); }
  shared_ptr<Column<uint64_t>> DbDurationColumn() final {
    return MakeNum<uint64_t, arrow::UInt64Builder>("duration_us", arrow::uint64());
  }
  shared_ptr<Column<string>> DbOperationColumn() final { return MakeDictStr("db_operation"); }
  shared_ptr<Column<string_view>> DbQueryTextColumn() final { return MakeUtf8Sv("query_text"); }

  // -- Lifecycle --
  void BeginBatch() final;
  void BeginRow() final;
  bool CommitBatch() final;

  // Transport pass-through to the inner OTelExporter.
  bool EstablishNewConnection() final { return inner_->EstablishNewConnection(); }
  bool IsConnected() const noexcept final { return inner_->IsConnected(); }

 private:
  // Budget 32-bit offsets even for repeated dictionary values
  static constexpr size_t kVarLenOffsetBytes = 4;

  template <typename T, typename BuilderT>
  class ArrowNumColumn : public Column<T> {
   public:
    ArrowNumColumn(BuilderT* builder, OTelArrowExporter* exp) : builder_(builder), exp_(exp) {}
    void Append(const T& v) final {
      exp_->Appended(builder_->Append(static_cast<typename BuilderT::value_type>(v)),
                     sizeof(typename BuilderT::value_type), "Arrow numeric append");
    }

   private:
    BuilderT* const builder_;
    OTelArrowExporter* const exp_;
  };

  class ArrowDictStrColumn : public Column<std::string> {
   public:
    ArrowDictStrColumn(DictBuilder* builder, OTelArrowExporter* exp)
        : builder_(builder), exp_(exp) {}
    void Append(const std::string& v) final {
      exp_->Appended(builder_->Append(v.data(), static_cast<int32_t>(v.size())),
                     v.size() + kVarLenOffsetBytes, "Arrow dict str append");
    }

   private:
    DictBuilder* const builder_;
    OTelArrowExporter* const exp_;
  };

  class ArrowDictSvColumn : public Column<std::string_view> {
   public:
    ArrowDictSvColumn(DictBuilder* builder, OTelArrowExporter* exp)
        : builder_(builder), exp_(exp) {}
    void Append(const std::string_view& v) final {
      exp_->Appended(builder_->Append(v.data(), static_cast<int32_t>(v.size())),
                     v.size() + kVarLenOffsetBytes, "Arrow dict sv append");
    }

   private:
    DictBuilder* const builder_;
    OTelArrowExporter* const exp_;
  };

  class ArrowUtf8SvColumn : public Column<std::string_view> {
   public:
    ArrowUtf8SvColumn(arrow::StringBuilder* builder, OTelArrowExporter* exp)
        : builder_(builder), exp_(exp) {}
    void Append(const std::string_view& v) final {
      exp_->Appended(builder_->Append(v.data(), static_cast<int32_t>(v.size())),
                     v.size() + kVarLenOffsetBytes, "Arrow utf8 sv append");
    }

   private:
    arrow::StringBuilder* const builder_;
    OTelArrowExporter* const exp_;
  };

  class ArrowTimestampColumn : public Column<int64_t> {
   public:
    ArrowTimestampColumn(arrow::TimestampBuilder* builder, OTelArrowExporter* exp)
        : builder_(builder), exp_(exp) {}
    // Caller passes Unix microseconds matching MICRO column precision
    void Append(const int64_t& v) final {
      exp_->Appended(builder_->Append(v), sizeof(int64_t), "Arrow timestamp append");
    }

   private:
    arrow::TimestampBuilder* const builder_;
    OTelArrowExporter* const exp_;
  };

  // Account a column append, an Arrow failure poisons the batch
  void Appended(const arrow::Status& status, size_t bytes, const char* context) {
    if (!status.ok()) {
      diag_->Fail(context, status.message());
      batch_failed_ = true;
      return;
    }
    bytes_estimate_ += bytes;
  }

  // Schema-side helpers: create a builder, register a slot, return a wrapper.
  template <typename T, typename BuilderT>
  shared_ptr<Column<T>> MakeNum(string_view name, std::shared_ptr<arrow::DataType> dtype) {
    auto builder = std::make_shared<BuilderT>();
    auto* raw = builder.get();
    slots_.push_back(
        {std::string(name), arrow::field(std::string(name), std::move(dtype)), std::move(builder)});
    return std::make_shared<ArrowNumColumn<T, BuilderT>>(raw, this);
  }
  shared_ptr<Column<string>> MakeDictStr(string_view name) {
    auto builder = std::make_shared<DictBuilder>();
    auto* raw = builder.get();
    slots_.push_back(
        {std::string(name), arrow::field(std::string(name), DictUtf8Type()), std::move(builder)});
    return std::make_shared<ArrowDictStrColumn>(raw, this);
  }
  shared_ptr<Column<string_view>> MakeDictSv(string_view name) {
    auto builder = std::make_shared<DictBuilder>();
    auto* raw = builder.get();
    slots_.push_back(
        {std::string(name), arrow::field(std::string(name), DictUtf8Type()), std::move(builder)});
    return std::make_shared<ArrowDictSvColumn>(raw, this);
  }
  shared_ptr<Column<string_view>> MakeUtf8Sv(string_view name) {
    auto builder = std::make_shared<arrow::StringBuilder>();
    auto* raw = builder.get();
    slots_.push_back(
        {std::string(name), arrow::field(std::string(name), arrow::utf8()), std::move(builder)});
    return std::make_shared<ArrowUtf8SvColumn>(raw, this);
  }
  shared_ptr<Column<int64_t>> MakeTimestamp(string_view name) {
    auto builder =
        std::make_shared<arrow::TimestampBuilder>(TimestampType(), arrow::default_memory_pool());
    auto* raw = builder.get();
    slots_.push_back(
        {std::string(name), arrow::field(std::string(name), TimestampType()), std::move(builder)});
    return std::make_shared<ArrowTimestampColumn>(raw, this);
  }

  bool Flush();

  void RegisterEnvelopeColumns();

  std::unique_ptr<StatsExporter> inner_;
  std::vector<ArrowSlot> slots_;
  int row_count_ = 0;
  size_t bytes_estimate_ = 0;
  size_t max_block_bytes_ = 0;
  bool batch_failed_ = false;

  // Synthesized columns (populated implicitly in BeginRow).
  shared_ptr<Column<string_view>> inst_ubid_;
  shared_ptr<Column<string_view>> srv_ubid_;
  shared_ptr<Column<string_view>> srv_role_;
  shared_ptr<Column<string_view>> region_;
  shared_ptr<Column<string_view>> cell_;
  shared_ptr<Column<string_view>> svc_ver_;
  shared_ptr<Column<string_view>> host_id_;
  shared_ptr<Column<string_view>> pod_name_;
  shared_ptr<Column<string_view>> read_replica_type_;

  // Cached for per-row appends.
  std::string instance_ubid_val_;
  std::string server_ubid_val_;
  std::string server_role_val_;
  std::string region_val_;
  std::string cell_val_;
  std::string service_version_val_;
  std::string host_id_val_;
  std::string pod_name_val_;
  std::string read_replica_type_val_;
};

void OTelArrowExporter::RegisterEnvelopeColumns() {
  // OTel resource attributes from configured extra_attributes.
  inst_ubid_ = MakeUtf8Sv("instance_ubid");
  srv_ubid_ = MakeUtf8Sv("server_ubid");
  srv_role_ = MakeDictSv("server_role");
  read_replica_type_ = MakeDictSv("read_replica_type");
  region_ = MakeDictSv("region");
  cell_ = MakeDictSv("cell");
  svc_ver_ = MakeDictSv("service_version");
  host_id_ = MakeUtf8Sv("host_id");
  pod_name_ = MakeUtf8Sv("pod_name");

  const ExtraAttrs attrs(config_->extra_attributes.c_str());
  instance_ubid_val_ = attrs.Get("instance_ubid");
  server_ubid_val_ = attrs.Get("server_ubid");
  server_role_val_ = attrs.Get("server_role");
  region_val_ = attrs.Get("region");
  cell_val_ = attrs.Get("cell");
  host_id_val_ = attrs.Get("host_id");
  pod_name_val_ = attrs.Get("pod_name");

  // Match events_raw DEFAULT 'none' when Arrow supplies this column
  read_replica_type_val_ = attrs.Get("read_replica_type");
  if (read_replica_type_val_.empty()) {
    read_replica_type_val_ = "none";
  }

  service_version_val_ = config_->service_version;
}

void OTelArrowExporter::BeginBatch() {
  slots_.clear();
  row_count_ = 0;
  bytes_estimate_ = 0;
  batch_failed_ = false;
  max_block_bytes_ = std::max<size_t>(65536, config_->arrow_max_block_bytes);
  RegisterEnvelopeColumns();
}

void OTelArrowExporter::BeginRow() {
  if (batch_failed_) {
    return;
  }
  // Flush between rows to keep column lengths aligned
  if (row_count_ > 0 && bytes_estimate_ >= max_block_bytes_) {
    if (!Flush()) {
      batch_failed_ = true;
      return;
    }
  }
  ++row_count_;
  inst_ubid_->Append(instance_ubid_val_);
  srv_ubid_->Append(server_ubid_val_);
  srv_role_->Append(server_role_val_);
  read_replica_type_->Append(read_replica_type_val_);
  region_->Append(region_val_);
  cell_->Append(cell_val_);
  svc_ver_->Append(service_version_val_);
  host_id_->Append(host_id_val_);
  pod_name_->Append(pod_name_val_);
}

bool OTelArrowExporter::CommitBatch() {
  if (batch_failed_) {
    return false;
  }
  if (slots_.empty() || row_count_ == 0) {
    return true;
  }
  return Flush();
}

bool OTelArrowExporter::Flush() {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(slots_.size());
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(slots_.size());

  for (auto& slot : slots_) {
    fields.push_back(slot.field);
    std::shared_ptr<arrow::Array> array;
    const arrow::Status status = slot.builder->Finish(&array);
    if (!status.ok()) {
      char context[PSCH_EXPORTER_TEXT_MAX];
      snprintf(context, sizeof context, "Arrow finish %s", slot.name.c_str());
      diag_->Fail(context, status.message());
      return false;
    }
    arrays.push_back(std::move(array));
  }

  auto schema = arrow::schema(std::move(fields));
  auto record_batch = arrow::RecordBatch::Make(std::move(schema), row_count_, std::move(arrays));

  auto buf_result = SerializeArrowBatch(*record_batch);
  if (!buf_result.ok()) {
    diag_->Fail("Arrow IPC serialize", buf_result.status().message());
    return false;
  }
  auto buf = *buf_result;
  const auto buf_len = static_cast<size_t>(buf->size());

  MaybeDumpArrowBatch(config_->arrow_dump_dir, buf->data(), buf_len, *diag_);
  // Match events_raw routing in clickgres-platform/services/datagres-otelcol
  if (!inner_->SendArrowBatch(buf->data(), buf_len, row_count_, "arrow_events_raw")) {
    return false;
  }
  row_count_ = 0;
  bytes_estimate_ = 0;
  return true;
}

}  // namespace

std::unique_ptr<StatsExporter> MakeUnifiedArrowExporter(const ExporterConfig* config,
                                                        Diagnostics* diag) {
  return std::make_unique<OTelArrowExporter>(config, diag);
}
