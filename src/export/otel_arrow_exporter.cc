// pg_stat_ch OTel/Arrow unified exporter.
//
// Adapts the StatsExporter column-factory API into a typed Arrow column set,
// serializes the assembled RecordBatch as ZSTD-compressed Arrow IPC, and
// ships it through a held OTelExporter via SendArrowBatch. Wire shape
// targets events_raw (the unified schema authored in PR #99), not the legacy
// query_logs_arrow table. The legacy arrow_batch.cc path stays alive for
// the latter; this exporter is selected via
// pg_stat_ch.use_unified_arrow_exporter (default off).
//
// Composition over inheritance: we hold (not extend) OTelExporter so we
// reuse its gRPC connection lifecycle and Arrow-over-OTel wire format
// without inheriting its per-row LogRecord state machine, which is unused
// on the Arrow path.

extern "C" {
#include "postgres.h"

#include "utils/timestamp.h"
}

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_dict.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "pg_stat_ch/pg_stat_ch.h"
#include "config/guc.h"
#include "export/exporter_interface.h"
#include "export/otel_arrow_exporter.h"
#include "export/otel_exporter.h"

namespace {

using DictBuilder = arrow::StringDictionary32Builder;

// PostgreSQL epoch is 2000-01-01, Unix epoch is 1970-01-01. Difference is
// 946684800 seconds = 946684800000000 microseconds.
constexpr int64_t kPostgresEpochOffsetUs = 946684800000000LL;

// Optional debug capture: when psch_debug_arrow_dump_dir is set, write each
// IPC batch to disk before shipping. Used by t/026_arrow_dump and the new
// end-to-end test to assert on Arrow output without standing up an OTel
// collector.
//
// Intentionally a near-duplicate of stats_exporter.cc:MaybeDumpArrowBatch.
// Both copies die when arrow_batch.cc is retired after the
// use_unified_arrow_exporter cutover; extracting a shared helper now would
// create a header just to delete it later.
void MaybeDumpArrowBatch(const uint8_t* data, size_t len) {
  if (data == nullptr || len == 0 || psch_debug_arrow_dump_dir == nullptr ||
      *psch_debug_arrow_dump_dir == '\0') {
    return;
  }
  const auto unix_now_ns =
      static_cast<unsigned long long>((GetCurrentTimestamp() + kPostgresEpochOffsetUs) * 1000LL);
  char path[MAXPGPATH];
  char tmp_path[MAXPGPATH];
  snprintf(path, sizeof(path), "%s/arrow_%llu.ipc", psch_debug_arrow_dump_dir, unix_now_ns);
  snprintf(tmp_path, sizeof(tmp_path), "%s/arrow_%llu.ipc.tmp", psch_debug_arrow_dump_dir,
           unix_now_ns);

  FILE* file = fopen(tmp_path, "wb");
  if (file == nullptr) {
    elog(WARNING, "pg_stat_ch: failed to open Arrow dump file '%s'", tmp_path);
    return;
  }
  const size_t written = fwrite(data, 1, len, file);
  fclose(file);
  if (written != len) {
    remove(tmp_path);
    elog(WARNING, "pg_stat_ch: short Arrow dump write to '%s' (%zu/%zu bytes)", tmp_path, written,
         len);
    return;
  }
  if (rename(tmp_path, path) != 0) {
    remove(tmp_path);
    elog(WARNING, "pg_stat_ch: failed to finalize Arrow dump file '%s'", path);
  }
}

// events_raw declares ts as DateTime64(6, 'UTC') (microsecond precision).
// Match it on the wire: any other precision risks silent truncation or a
// CH type-mismatch on insert.
std::shared_ptr<arrow::DataType> TimestampType() {
  static const auto kType = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
  return kType;
}

std::shared_ptr<arrow::DataType> DictUtf8Type() {
  static const auto kType = arrow::dictionary(arrow::int32(), arrow::utf8());
  return kType;
}

void LogArrowFailure(const char* context, const arrow::Status& status) {
  elog(WARNING, "pg_stat_ch: %s: %s", context, status.ToString().c_str());
}

// One column slot: a typed Arrow builder kept alive by the exporter (the
// Column<T> wrapper handed to the caller holds only a non-owning pointer
// into this slot, so it can safely drop out of scope before CommitBatch).
struct ArrowSlot {
  std::string name;
  std::shared_ptr<arrow::Field> field;
  std::shared_ptr<arrow::ArrayBuilder> builder;
};

// Parse "key1:val1;key2:val2" into a flat list. First match wins on
// duplicate keys (Get linear-scans from the front). Empty input -> empty list.
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
  OTelArrowExporter() : inner_(MakeOpenTelemetryExporter()) {}

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
  bool IsConnected() const final { return inner_->IsConnected(); }
  int NumConsecutiveFailures() const final { return inner_->NumConsecutiveFailures(); }
  void ResetFailures() final { inner_->ResetFailures(); }
  // Report rows from THIS batch, not the inner exporter's cumulative count.
  // BeginBatch resets row_count_; the inner's exported_count_ accumulates
  // across batches because we never call inner_->BeginBatch() (the inner's
  // per-record LogRecord state machine is unused on this path).
  // PschExportBatch reads NumExported() once per successful CommitBatch and
  // fetch_adds it into shared stats, so a cumulative value here would cause
  // quadratic over-reporting.
  int NumExported() const final { return row_count_; }

 private:
  // -- Column<T> wrappers ---------------------------------------------------
  // Nested so they can inherit from StatsExporter::Column<T> (protected in
  // the base; visible to derived classes and their nested types). Each one
  // Append-forwards to a single typed builder; Crunch() is unused on this
  // path — builders are finished together in CommitBatch when we have all
  // slots in hand.

  template <typename T, typename BuilderT>
  class ArrowNumColumn : public Column<T> {
   public:
    explicit ArrowNumColumn(BuilderT* builder) : builder_(builder) {}
    void Append(const T& v) final {
      const arrow::Status s = builder_->Append(static_cast<typename BuilderT::value_type>(v));
      if (!s.ok()) {
        LogArrowFailure("Arrow numeric append", s);
      }
    }
    void Crunch() final {}

   private:
    BuilderT* const builder_;
  };

  class ArrowDictStrColumn : public Column<std::string> {
   public:
    explicit ArrowDictStrColumn(DictBuilder* builder) : builder_(builder) {}
    void Append(const std::string& v) final {
      const arrow::Status s = builder_->Append(v.data(), static_cast<int32_t>(v.size()));
      if (!s.ok()) {
        LogArrowFailure("Arrow dict str append", s);
      }
    }
    void Crunch() final {}

   private:
    DictBuilder* const builder_;
  };

  class ArrowDictSvColumn : public Column<std::string_view> {
   public:
    explicit ArrowDictSvColumn(DictBuilder* builder) : builder_(builder) {}
    void Append(const std::string_view& v) final {
      const arrow::Status s = builder_->Append(v.data(), static_cast<int32_t>(v.size()));
      if (!s.ok()) {
        LogArrowFailure("Arrow dict sv append", s);
      }
    }
    void Crunch() final {}

   private:
    DictBuilder* const builder_;
  };

  class ArrowUtf8SvColumn : public Column<std::string_view> {
   public:
    explicit ArrowUtf8SvColumn(arrow::StringBuilder* builder) : builder_(builder) {}
    void Append(const std::string_view& v) final {
      const arrow::Status s = builder_->Append(v.data(), static_cast<int32_t>(v.size()));
      if (!s.ok()) {
        LogArrowFailure("Arrow utf8 sv append", s);
      }
    }
    void Crunch() final {}

   private:
    arrow::StringBuilder* const builder_;
  };

  class ArrowTimestampColumn : public Column<int64_t> {
   public:
    explicit ArrowTimestampColumn(arrow::TimestampBuilder* builder) : builder_(builder) {}
    // Caller passes Unix microseconds; the column is MICRO-precision, so
    // append directly with no conversion.
    void Append(const int64_t& v) final {
      const arrow::Status s = builder_->Append(v);
      if (!s.ok()) {
        LogArrowFailure("Arrow timestamp append", s);
      }
    }
    void Crunch() final {}

   private:
    arrow::TimestampBuilder* const builder_;
  };

  // Schema-side helpers: create a builder, register a slot, return a wrapper.
  template <typename T, typename BuilderT>
  shared_ptr<Column<T>> MakeNum(string_view name, std::shared_ptr<arrow::DataType> dtype) {
    auto builder = std::make_shared<BuilderT>();
    auto* raw = builder.get();
    slots_.push_back(
        {std::string(name), arrow::field(std::string(name), std::move(dtype)), std::move(builder)});
    return std::make_shared<ArrowNumColumn<T, BuilderT>>(raw);
  }
  shared_ptr<Column<string>> MakeDictStr(string_view name) {
    auto builder = std::make_shared<DictBuilder>();
    auto* raw = builder.get();
    slots_.push_back(
        {std::string(name), arrow::field(std::string(name), DictUtf8Type()), std::move(builder)});
    return std::make_shared<ArrowDictStrColumn>(raw);
  }
  shared_ptr<Column<string_view>> MakeDictSv(string_view name) {
    auto builder = std::make_shared<DictBuilder>();
    auto* raw = builder.get();
    slots_.push_back(
        {std::string(name), arrow::field(std::string(name), DictUtf8Type()), std::move(builder)});
    return std::make_shared<ArrowDictSvColumn>(raw);
  }
  shared_ptr<Column<string_view>> MakeUtf8Sv(string_view name) {
    auto builder = std::make_shared<arrow::StringBuilder>();
    auto* raw = builder.get();
    slots_.push_back(
        {std::string(name), arrow::field(std::string(name), arrow::utf8()), std::move(builder)});
    return std::make_shared<ArrowUtf8SvColumn>(raw);
  }
  shared_ptr<Column<int64_t>> MakeTimestamp(string_view name) {
    auto builder =
        std::make_shared<arrow::TimestampBuilder>(TimestampType(), arrow::default_memory_pool());
    auto* raw = builder.get();
    slots_.push_back(
        {std::string(name), arrow::field(std::string(name), TimestampType()), std::move(builder)});
    return std::make_shared<ArrowTimestampColumn>(raw);
  }

  // ----------------------------------------------------------------
  // Columns the events_raw schema declares that the StatsExporter caller
  // does not explicitly populate. The exporter synthesizes them on
  // BeginRow so stats_exporter.cc's column-emission loop doesn't have to
  // know about them:
  //
  // - 8 envelope columns + read_replica_type: per-process constants from
  //   pg_stat_ch.extra_attributes (or "none" default for read_replica_type
  //   per clickgres-platform's convention).
  // - service_version: PG_STAT_CH_VERSION macro, not from extra_attributes.
  //
  // parent_query_id is *not* synthesized: PR #95 will add the column to
  // events_raw and wire PschEvent::parent_query_id through the interface
  // (StatHCInt64). Sending it before then would either fail the insert
  // (column doesn't exist in events_raw on main yet) or rely on CH's
  // unknown-column drop behavior — both worse than just not sending it.
  // ----------------------------------------------------------------
  void RegisterEnvelopeColumns();

  std::unique_ptr<StatsExporter> inner_;
  std::vector<ArrowSlot> slots_;
  int row_count_ = 0;

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
  // OTel resource attributes from psch_extra_attributes.
  inst_ubid_ = MakeUtf8Sv("instance_ubid");
  srv_ubid_ = MakeUtf8Sv("server_ubid");
  srv_role_ = MakeDictSv("server_role");
  read_replica_type_ = MakeDictSv("read_replica_type");
  region_ = MakeDictSv("region");
  cell_ = MakeDictSv("cell");
  svc_ver_ = MakeDictSv("service_version");
  host_id_ = MakeUtf8Sv("host_id");
  pod_name_ = MakeUtf8Sv("pod_name");

  const ExtraAttrs attrs(psch_extra_attributes);
  instance_ubid_val_ = attrs.Get("instance_ubid");
  server_ubid_val_ = attrs.Get("server_ubid");
  server_role_val_ = attrs.Get("server_role");
  region_val_ = attrs.Get("region");
  cell_val_ = attrs.Get("cell");
  host_id_val_ = attrs.Get("host_id");
  pod_name_val_ = attrs.Get("pod_name");

  // events_raw declares read_replica_type DEFAULT 'none'; if extra_attributes
  // didn't supply one, send 'none' explicitly so the wire shape matches the
  // schema regardless of CH's default-fill behavior for Arrow inserts.
  read_replica_type_val_ = attrs.Get("read_replica_type");
  if (read_replica_type_val_.empty()) {
    read_replica_type_val_ = "none";
  }

  // service_version is a compile-time pg_stat_ch identifier, not a runtime
  // resource attribute — keep it pinned to the macro the rest of the
  // codebase already uses.
  service_version_val_ = PG_STAT_CH_VERSION;
}

void OTelArrowExporter::BeginBatch() {
  slots_.clear();
  row_count_ = 0;
  RegisterEnvelopeColumns();
}

void OTelArrowExporter::BeginRow() {
  ++row_count_;
  // Synthesized columns fire here so the call site doesn't need to know
  // about them.
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
  if (slots_.empty() || row_count_ == 0) {
    return true;
  }

  // TODO(memory-budget): the legacy ExportEventsAsArrowInternal flushes
  // mid-batch when the Arrow builder's estimated bytes exceed
  // psch_otel_max_block_bytes (default 3 MiB). This exporter ships the
  // whole batch in one IPC, so a backlog up to psch_batch_max (default
  // 200000) can produce an oversized payload that exceeds gRPC's 4 MiB
  // wire cap or otelcol's 20 MiB HTTP body cap. Acceptable for the
  // GUC-default-off shadow rollout, but the budget check needs to land
  // before the GUC default flips. Plumbing it through requires either
  // invalidating caller-held column shared_ptrs at the flush boundary
  // or threading a per-row size hook through the StatsExporter interface.

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(slots_.size());
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(slots_.size());

  for (auto& slot : slots_) {
    fields.push_back(slot.field);
    std::shared_ptr<arrow::Array> array;
    // arrow::ArrayBuilder::Finish is virtual; DictBuilder's override returns
    // a DictionaryArray downcast to Array. No RTTI needed.
    const arrow::Status status = slot.builder->Finish(&array);
    if (!status.ok()) {
      LogArrowFailure(("Arrow finish " + slot.name).c_str(), status);
      return false;
    }
    arrays.push_back(std::move(array));
  }

  auto schema = arrow::schema(std::move(fields));
  auto record_batch = arrow::RecordBatch::Make(std::move(schema), row_count_, std::move(arrays));

  auto out_stream_result = arrow::io::BufferOutputStream::Create();
  if (!out_stream_result.ok()) {
    LogArrowFailure("Arrow IPC stream create", out_stream_result.status());
    return false;
  }
  auto out_stream = *out_stream_result;

  arrow::ipc::IpcWriteOptions write_opts = arrow::ipc::IpcWriteOptions::Defaults();
  auto codec_result = arrow::util::Codec::Create(arrow::Compression::ZSTD);
  if (!codec_result.ok()) {
    LogArrowFailure("Arrow ZSTD codec create", codec_result.status());
    return false;
  }
  write_opts.codec = std::shared_ptr<arrow::util::Codec>(std::move(*codec_result));

  auto writer_result = arrow::ipc::MakeStreamWriter(out_stream, record_batch->schema(), write_opts);
  if (!writer_result.ok()) {
    LogArrowFailure("Arrow stream writer create", writer_result.status());
    return false;
  }
  auto writer = *writer_result;
  if (auto s = writer->WriteRecordBatch(*record_batch); !s.ok()) {
    LogArrowFailure("Arrow WriteRecordBatch", s);
    return false;
  }
  if (auto s = writer->Close(); !s.ok()) {
    LogArrowFailure("Arrow writer close", s);
    return false;
  }

  auto buf_result = out_stream->Finish();
  if (!buf_result.ok()) {
    LogArrowFailure("Arrow IPC finalize", buf_result.status());
    return false;
  }
  auto buf = *buf_result;
  const auto buf_len = static_cast<size_t>(buf->size());

  MaybeDumpArrowBatch(buf->data(), buf_len);
  return inner_->SendArrowBatch(buf->data(), buf_len, row_count_);
}

}  // namespace

std::unique_ptr<StatsExporter> MakeUnifiedArrowExporter() {
  return std::make_unique<OTelArrowExporter>();
}
