// Build OTLP requests on protobuf arenas, worker owns batching and retry

#include <opentelemetry/exporters/otlp/otlp_grpc_client.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter_options.h>
#include <opentelemetry/proto/collector/logs/v1/logs_service.grpc.pb.h>
#include <opentelemetry/proto/collector/logs/v1/logs_service.pb.h>
#include <opentelemetry/proto/common/v1/common.pb.h>
#include <opentelemetry/proto/logs/v1/logs.pb.h>
#include <opentelemetry/proto/resource/v1/resource.pb.h>

#include "export/diagnostics.h"
#include "export/exporter_config.h"
#include "export/exporter_interface.h"

#include <google/protobuf/arena.h>
#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <string>
#include <string_view>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

namespace otlp = opentelemetry::exporter::otlp;
namespace logs_pb = opentelemetry::proto::logs::v1;
namespace common_pb = opentelemetry::proto::common::v1;
namespace resource_pb = opentelemetry::proto::resource::v1;
namespace collector_logs = opentelemetry::proto::collector::logs::v1;

namespace {

// Configured hostname, else HOSTNAME environment, else gethostname
std::string Hostname(const std::string& configured, const char* fallback) {
  if (!configured.empty()) {
    return configured;
  }
  const char* env = getenv("HOSTNAME");
  if (env != nullptr && *env != '\0') {
    return env;
  }
  char buf[256];
  if (gethostname(buf, sizeof(buf)) == 0) {
    buf[sizeof(buf) - 1] = '\0';
    return buf;
  }
  return fallback;
}

using string = std::string;
using string_view = std::string_view;

// Create a gRPC channel with compression and keepalive settings optimized
// for high-throughput telemetry export from a long-lived bgworker.
std::shared_ptr<grpc::Channel> MakeOptimizedChannel(
    const otlp::OtlpGrpcLogRecordExporterOptions& opts) {
  grpc::ChannelArguments args;

  args.SetCompressionAlgorithm(GRPC_COMPRESS_GZIP);

  // Detect idle connection drops before next export
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 30000);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);

  std::shared_ptr<grpc::ChannelCredentials> credentials;
  if (opts.use_ssl_credentials) {
    grpc::SslCredentialsOptions ssl_opts;
    if (!opts.ssl_credentials_cacert_as_string.empty()) {
      ssl_opts.pem_root_certs = opts.ssl_credentials_cacert_as_string;
    }
    credentials = grpc::SslCredentials(ssl_opts);
  } else {
    credentials = grpc::InsecureChannelCredentials();
  }

  return grpc::CreateCustomChannel(opts.endpoint, credentials, args);
}

common_pb::KeyValue* AddAttr(logs_pb::LogRecord* rec) {
  return rec->add_attributes();
}

void SetString(common_pb::KeyValue* kv, string_view key, string_view val) {
  kv->set_key(key.data(), key.size());
  kv->mutable_value()->set_string_value(val.data(), val.size());
}

void SetInt(common_pb::KeyValue* kv, string_view key, int64_t val) {
  kv->set_key(key.data(), key.size());
  kv->mutable_value()->set_int_value(val);
}

void SetDouble(common_pb::KeyValue* kv, string_view key, double val) {
  kv->set_key(key.data(), key.size());
  kv->mutable_value()->set_double_value(val);
}

// Conservative sizing constants to stay under the gRPC message budget.
constexpr size_t kMinBytesPerRecord = 1200;
constexpr size_t kRequestOverheadBytes = 512;
constexpr size_t kLogRecordOverheadBytes = 128;
constexpr size_t kAttrOverheadBytes = 24;

size_t EstimateStringAttrBytes(string_view key, string_view value) {
  return kAttrOverheadBytes + key.size() + value.size();
}

size_t EstimateScalarAttrBytes(string_view key) {
  return kAttrOverheadBytes + key.size() + sizeof(int64_t);
}

class OTelExporter : public StatsExporter {
 public:
  OTelExporter(const ExporterConfig* config, Diagnostics* diag) : StatsExporter(config, diag) {}

  void BeginBatch() final {
    batch_failed_ = false;
    ResetChunk();
  }

  void BeginRow() final {
    if (batch_failed_) {
      return;
    }
    if (ChunkFull()) {
      if (!FlushChunk()) {
        batch_failed_ = true;
        return;
      }
      ResetChunk();
    }
    current_record_ = scope_logs_->add_log_records();
    ++chunk_count_;
    chunk_bytes_ += kLogRecordOverheadBytes;
  }

  bool CommitBatch() final;

  // OTLP logs use identical attributes for LC and HC columns
  shared_ptr<Column<string>> StatLCString(string_view name) final { return MakeStringCol(name); }
  shared_ptr<Column<uint8_t>> StatLCUInt8(string_view name) final {
    return MakeIntCol<uint8_t>(name);
  }
  shared_ptr<Column<int16_t>> StatLCInt16(string_view name) final {
    return MakeIntCol<int16_t>(name);
  }
  shared_ptr<Column<int32_t>> StatLCInt32(string_view name) final {
    return MakeIntCol<int32_t>(name);
  }

  // High-cardinality columns
  shared_ptr<Column<string_view>> StatHCString(string_view name) final { return MakeSvCol(name); }
  shared_ptr<Column<int64_t>> StatHCInt64(string_view name) final {
    return MakeIntCol<int64_t>(name);
  }
  shared_ptr<Column<uint64_t>> StatHCUInt64(string_view name) final {
    return MakeIntCol<uint64_t>(name);
  }

  shared_ptr<Column<int64_t>> StatTimestamp(string_view name) final {
    return MakeDateTimeCol(name);
  }

  // Semantic columns
  shared_ptr<Column<string>> DbNameColumn() final { return MakeStringCol("db.name"); }
  shared_ptr<Column<string>> DbUserColumn() final { return MakeStringCol("db.user"); }
  shared_ptr<Column<uint64_t>> DbDurationColumn() final { return MakeDurationCol(); }
  shared_ptr<Column<string>> DbOperationColumn() final {
    return MakeStringCol("db.operation.name");
  }
  shared_ptr<Column<string_view>> DbQueryTextColumn() final { return MakeSvCol("db.query.text"); }

  bool EstablishNewConnection() final;
  bool IsConnected() const noexcept final { return stub_ != nullptr; }
  bool SendArrowBatch(const uint8_t* ipc_data, size_t ipc_len, int num_rows,
                      string_view block_format) final;

 private:
  // -- Lightweight column types (no SDK, direct eager proto writes) ----------

  template <typename T>
  class IntColumn : public Column<T> {
   public:
    IntColumn(OTelExporter* e, string_view n) : exp_(e), name_(n) {}
    void Append(const T& v) final {
      if (exp_->current_record_ == nullptr || v == 0) {
        return;
      }
      SetInt(AddAttr(exp_->current_record_), name_, static_cast<int64_t>(v));
      exp_->chunk_bytes_ += EstimateScalarAttrBytes(name_);
    }

   private:
    OTelExporter* exp_;
    string name_;
  };

  class SvColumn : public Column<string_view> {
   public:
    SvColumn(OTelExporter* e, string_view n) : exp_(e), name_(n) {}
    void Append(const string_view& v) final {
      if (exp_->current_record_ == nullptr) {
        return;
      }
      SetString(AddAttr(exp_->current_record_), name_, v);
      exp_->chunk_bytes_ += EstimateStringAttrBytes(name_, v);
    }

   private:
    OTelExporter* exp_;
    string name_;
  };

  class StrColumn : public Column<string> {
   public:
    StrColumn(OTelExporter* e, string_view n) : exp_(e), name_(n) {}
    void Append(const string& v) final {
      if (exp_->current_record_ == nullptr) {
        return;
      }
      SetString(AddAttr(exp_->current_record_), name_, v);
      exp_->chunk_bytes_ += EstimateStringAttrBytes(name_, v);
    }

   private:
    OTelExporter* exp_;
    string name_;
  };

  class DateTimeCol : public Column<int64_t> {
   public:
    DateTimeCol(OTelExporter* e, string_view n) : exp_(e), name_(n) {}
    void Append(const int64_t& v) final {
      if (exp_->current_record_ == nullptr) {
        return;
      }
      exp_->current_record_->set_time_unix_nano(static_cast<uint64_t>(v) * 1000ULL);
      SetInt(AddAttr(exp_->current_record_), name_, v);
      exp_->chunk_bytes_ += EstimateScalarAttrBytes(name_) + sizeof(uint64_t);
    }

   private:
    OTelExporter* exp_;
    string name_;
  };

  class DurationCol : public Column<uint64_t> {
   public:
    explicit DurationCol(OTelExporter* e) : exp_(e) {}
    void Append(const uint64_t& v) final {
      if (exp_->current_record_ == nullptr) {
        return;
      }
      double seconds = static_cast<double>(v) / 1e6;
      SetDouble(AddAttr(exp_->current_record_), "db.client.operation.duration", seconds);
      SetInt(AddAttr(exp_->current_record_), "duration_us", static_cast<int64_t>(v));
      exp_->chunk_bytes_ += EstimateScalarAttrBytes("db.client.operation.duration") +
                            EstimateScalarAttrBytes("duration_us");
    }

   private:
    OTelExporter* exp_;
  };

  // -- Column factory helpers ------------------------------------------------

  template <typename T>
  shared_ptr<Column<T>> MakeIntCol(string_view name) {
    return std::make_shared<IntColumn<T>>(this, name);
  }

  shared_ptr<Column<string_view>> MakeSvCol(string_view name) {
    return std::make_shared<SvColumn>(this, name);
  }

  shared_ptr<Column<string>> MakeStringCol(string_view name) {
    return std::make_shared<StrColumn>(this, name);
  }

  shared_ptr<Column<int64_t>> MakeDateTimeCol(string_view name) {
    return std::make_shared<DateTimeCol>(this, name);
  }

  shared_ptr<Column<uint64_t>> MakeDurationCol() { return std::make_shared<DurationCol>(this); }

  // -- Chunk management ------------------------------------------------------

  bool ChunkFull() const {
    return chunk_count_ >= max_chunk_records_ || chunk_bytes_ >= max_chunk_bytes_;
  }

  void ResetChunk() {
    // Drop arena-owned pointers before arena replacement can throw
    request_ = nullptr;
    scope_logs_ = nullptr;
    current_record_ = nullptr;

    google::protobuf::ArenaOptions arena_opts;
    arena_opts.initial_block_size = 65536;  // 64 KiB
    arena_opts.max_block_size = 1048576;    // 1 MiB
    arena_ = std::make_unique<google::protobuf::Arena>(arena_opts);

    request_ =
        google::protobuf::Arena::Create<collector_logs::ExportLogsServiceRequest>(arena_.get());
    chunk_count_ = 0;
    chunk_bytes_ = kRequestOverheadBytes;
    current_record_ = nullptr;

    auto* resource_logs = request_->add_resource_logs();
    PopulateResource(resource_logs->mutable_resource());
    scope_logs_ = resource_logs->add_scope_logs();
    scope_logs_->mutable_scope()->set_name("pg_stat_ch");
    scope_logs_->mutable_scope()->set_version(config_->service_version);
  }

  bool FlushChunk() {
    if (chunk_count_ == 0) {
      return true;
    }
    if (stub_ == nullptr) {
      diag_->Fail("OTLP export", "not connected");
      return false;
    }

    auto context = otlp::OtlpGrpcClient::MakeClientContext(grpc_opts_);
    collector_logs::ExportLogsServiceResponse response;
    auto status = otlp::OtlpGrpcClient::DelegateExport(stub_.get(), std::move(context),
                                                       std::move(arena_), request_, &response);

    request_ = nullptr;
    scope_logs_ = nullptr;
    current_record_ = nullptr;

    if (status.ok()) {
      diag_->AddExported(static_cast<uint32_t>(chunk_count_));
      chunk_count_ = 0;
      chunk_bytes_ = 0;
      return true;
    }

    diag_->Fail("gRPC export failed", status.error_message());
    return false;
  }

  // Set block_format for collector routing between Arrow destinations
  void PopulateResource(resource_pb::Resource* resource, string_view block_format = "") const {
    auto add = [&](string_view key, string_view val) {
      SetString(resource->add_attributes(), key, val);
    };
    add("service.name", "pg_stat_ch");
    add("service.version", config_->service_version);
    add("host.name", Hostname(config_->hostname, "postgres-primary"));
    if (!block_format.empty()) {
      add("pg_stat_ch.block_format", block_format);
    }
  }

  void ConfigureLogExport(const string& endpoint) {
    grpc_opts_ = otlp::OtlpGrpcLogRecordExporterOptions();
    if (!endpoint.empty()) {
      grpc_opts_.endpoint = endpoint;
    }
    grpc_opts_.timeout = std::chrono::milliseconds(config_->otel_log_delay_ms);

    max_chunk_bytes_ = std::max<size_t>(kMinBytesPerRecord, config_->otel_log_max_bytes);
    max_chunk_records_ = std::max<size_t>(
        1, std::min<size_t>(config_->otel_log_batch_size, max_chunk_bytes_ / kMinBytesPerRecord));
  }

  // gRPC state
  otlp::OtlpGrpcLogRecordExporterOptions grpc_opts_;
  std::unique_ptr<collector_logs::LogsService::StubInterface> stub_;
  size_t max_chunk_records_ = 1;
  size_t max_chunk_bytes_ = kMinBytesPerRecord;
  bool batch_failed_ = false;

  // Per-chunk state (arena-allocated)
  std::unique_ptr<google::protobuf::Arena> arena_;
  collector_logs::ExportLogsServiceRequest* request_ = nullptr;
  logs_pb::ScopeLogs* scope_logs_ = nullptr;
  logs_pb::LogRecord* current_record_ = nullptr;
  size_t chunk_count_ = 0;
  size_t chunk_bytes_ = 0;
};

bool OTelExporter::EstablishNewConnection() {
  try {
    ConfigureLogExport(config_->otel_endpoint);
    auto channel = MakeOptimizedChannel(grpc_opts_);
    if (channel == nullptr) {
      diag_->Fail("OTel init failed", "invalid or empty OTLP endpoint");
      stub_.reset();
      return false;
    }
    stub_ = collector_logs::LogsService::NewStub(channel);
    return true;
  } catch (const std::exception& e) {
    diag_->Fail("OTel init failed", e.what());
    stub_.reset();
    return false;
  }
}

bool OTelExporter::CommitBatch() {
  if (batch_failed_) {
    return false;
  }
  try {
    return FlushChunk();
  } catch (const std::exception& e) {
    diag_->Fail("OTLP export", e.what());
    return false;
  }
}

bool OTelExporter::SendArrowBatch(const uint8_t* ipc_data, size_t ipc_len, int num_rows,
                                  string_view block_format) {
  if (stub_ == nullptr) {
    diag_->Fail("Arrow batch export", "not connected");
    return false;
  }
  if (ipc_data == nullptr || ipc_len == 0 || num_rows <= 0) {
    diag_->Fail("Arrow batch export", "empty batch");
    return false;
  }

  try {
    google::protobuf::ArenaOptions arena_opts;
    arena_opts.initial_block_size = 8192;
    arena_opts.max_block_size = 65536;
    auto arena = std::make_unique<google::protobuf::Arena>(arena_opts);

    auto* request =
        google::protobuf::Arena::Create<collector_logs::ExportLogsServiceRequest>(arena.get());
    auto* resource_logs = request->add_resource_logs();
    PopulateResource(resource_logs->mutable_resource(), block_format);
    auto* scope_logs = resource_logs->add_scope_logs();
    scope_logs->mutable_scope()->set_name("pg_stat_ch");
    scope_logs->mutable_scope()->set_version(config_->service_version);

    auto* record = scope_logs->add_log_records();
    const auto now_ns =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count());
    record->set_time_unix_nano(now_ns);
    record->set_observed_time_unix_nano(now_ns);
    record->mutable_body()->set_bytes_value(reinterpret_cast<const char*>(ipc_data), ipc_len);
    SetString(AddAttr(record), "pg_stat_ch.block_format", block_format);
    SetInt(AddAttr(record), "pg_stat_ch.block_rows", num_rows);

    auto context = otlp::OtlpGrpcClient::MakeClientContext(grpc_opts_);
    collector_logs::ExportLogsServiceResponse response;
    auto status = otlp::OtlpGrpcClient::DelegateExport(stub_.get(), std::move(context),
                                                       std::move(arena), request, &response);

    if (status.ok()) {
      diag_->AddExported(static_cast<uint32_t>(num_rows));
      return true;
    }
    diag_->Fail("Arrow batch gRPC failed", status.error_message());
    return false;
  } catch (const std::exception& e) {
    diag_->Fail("Arrow batch export", e.what());
    return false;
  }
}

}  // namespace

std::unique_ptr<StatsExporter> MakeOpenTelemetryExporter(const ExporterConfig* config,
                                                         Diagnostics* diag) {
  return std::make_unique<OTelExporter>(config, diag);
}
