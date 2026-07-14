// Direct-proto OTel exporter.
//
// Builds OTLP ExportLogsServiceRequest protobuf messages directly on a
// google::protobuf::Arena, bypassing the OTel SDK entirely. Our bgworker
// already owns batching and retry, so the SDK's pipelines are redundant.
//
// Arena allocation eliminates per-object malloc/free (the largest cost in the
// SDK path) and makes batch destruction O(1) via Arena::Reset().

#include <opentelemetry/exporters/otlp/otlp_http_client.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter_options.h>
#include <opentelemetry/proto/collector/logs/v1/logs_service.pb.h>
#include <opentelemetry/proto/common/v1/common.pb.h>
#include <opentelemetry/proto/logs/v1/logs.pb.h>
#include <opentelemetry/proto/resource/v1/resource.pb.h>
#include <opentelemetry/sdk/common/exporter_utils.h>

#include "config/guc.h"
#include "export/exporter_interface.h"

#include <google/protobuf/arena.h>

#include <algorithm>
#include <atomic>
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

// Exposed with external linkage so unit tests can link against it directly.
std::string GetAHostname(const char* fallback) {
  if (psch_hostname != nullptr && *psch_hostname != '\0') {
    return psch_hostname;
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

namespace {

using string = std::string;
using string_view = std::string_view;

// OtlpHttpClientOptions has no default constructor, copy every field from exporter options
otlp::OtlpHttpClientOptions MakeClientOptions(const otlp::OtlpHttpLogRecordExporterOptions& o) {
  return otlp::OtlpHttpClientOptions(
      o.url, o.ssl_insecure_skip_verify, o.ssl_ca_cert_path, o.ssl_ca_cert_string,
      o.ssl_client_key_path, o.ssl_client_key_string, o.ssl_client_cert_path,
      o.ssl_client_cert_string, o.ssl_min_tls, o.ssl_max_tls, o.ssl_cipher, o.ssl_cipher_suite,
      o.content_type, o.json_bytes_mapping, o.compression, o.use_json_name, o.console_debug,
      o.timeout, o.http_headers, o.retry_policy_max_attempts, o.retry_policy_initial_backoff,
      o.retry_policy_max_backoff, o.retry_policy_backoff_multiplier,
      std::shared_ptr<opentelemetry::sdk::common::ThreadInstrumentation>(nullptr));
}

const char* ExportResultToString(opentelemetry::sdk::common::ExportResult result) {
  using opentelemetry::sdk::common::ExportResult;
  switch (result) {
    case ExportResult::kSuccess:
      return "success";
    case ExportResult::kFailure:
      return "failure";
    case ExportResult::kFailureFull:
      return "collector full";
    case ExportResult::kFailureInvalidArgument:
      return "invalid argument";
  }
  return "unknown";
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

// Conservative sizing constants to stay under collector request-body limits.
constexpr size_t kMinBytesPerRecord = 1200;
constexpr size_t kRequestOverheadBytes = 512;
constexpr size_t kLogRecordOverheadBytes = 128;
constexpr size_t kAttrOverheadBytes = 24;

// Cap concurrent chunk uploads, each in-flight body holds up to max_chunk_bytes
constexpr size_t kMaxInflightChunks = 8;

size_t EstimateStringAttrBytes(string_view key, string_view value) {
  return kAttrOverheadBytes + key.size() + value.size();
}

size_t EstimateScalarAttrBytes(string_view key) {
  return kAttrOverheadBytes + key.size() + sizeof(int64_t);
}

// ---------------------------------------------------------------------------
// Direct-proto exporter — no OTel SDK dependency on the hot path.
// ---------------------------------------------------------------------------
class OTelExporter : public StatsExporter {
 public:
  void BeginBatch() final {
    exported_count_ = 0;
    batch_failed_ = false;
    async_ok_records_ = 0;
    async_failed_chunks_ = 0;
    async_failure_result_ = -1;
    ResetChunk();
  }

  void BeginRow() final {
    if (batch_failed_) {
      return;
    }
    if (async_failed_chunks_.load() > 0) {
      // In-flight chunk failed, stop building; CommitBatch reports failure
      batch_failed_ = true;
      return;
    }
    if (ChunkFull()) {
      FlushChunk();
      ResetChunk();
    }
    current_record_ = scope_logs_->add_log_records();
    ++chunk_count_;
    chunk_bytes_ += kLogRecordOverheadBytes;
  }

  bool CommitBatch() final;

  // -- Column factories (all write directly to the arena-allocated LogRecord) --
  //
  // OTel logs are flat attribute bags, so the LC/HC distinction collapses on
  // this side — both StatLC* and StatHC* of the same wire type produce
  // identical log attributes. The intent declared at the interface layer
  // matters for downstream metric processors (LC -> eligible as dimension;
  // HC -> log-attribute-only) but those decisions live in the collector
  // config, not here.

  // Low-cardinality columns
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
  bool IsConnected() const final { return client_ != nullptr; }
  int NumConsecutiveFailures() const final { return consecutive_failures_; }
  void ResetFailures() final { consecutive_failures_ = 0; }
  int NumExported() const final { return exported_count_; }
  bool SendArrowBatch(const uint8_t* ipc_data, size_t ipc_len, int num_rows) final;

 private:
  // -- Lightweight column types (no SDK, no Crunch, direct proto writes) -----

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
    void Crunch() final {}

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
    void Crunch() final {}

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
    void Crunch() final {}

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
    void Crunch() final {}

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
    void Crunch() final {}

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
    google::protobuf::ArenaOptions arena_opts;
    arena_opts.initial_block_size = 65536;  // 64 KiB — skip many small doublings
    arena_opts.max_block_size = 1048576;    // 1 MiB — sized for 3 MiB chunk budget
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
    scope_logs_->mutable_scope()->set_version(PG_STAT_CH_VERSION);
  }

  // Enqueue chunk upload; Export serializes into POST body before returning,
  // so arena frees immediately. Blocks only when kMaxInflightChunks uploads
  // already run. Result lands via callback on curl's thread; callback touches
  // atomics only, Postgres logging happens in CommitBatch
  void FlushChunk() {
    if (client_ == nullptr || chunk_count_ == 0) {
      return;
    }

    const size_t records = chunk_count_;
    client_->Export(
        *request_,
        [this, records](opentelemetry::sdk::common::ExportResult result) {
          if (result == opentelemetry::sdk::common::ExportResult::kSuccess) {
            async_ok_records_ += records;
          } else {
            async_failure_result_ = static_cast<int>(result);
            ++async_failed_chunks_;
          }
          return true;
        },
        kMaxInflightChunks);

    request_ = nullptr;
    scope_logs_ = nullptr;
    current_record_ = nullptr;
    arena_.reset();
    chunk_count_ = 0;
    chunk_bytes_ = 0;
  }

  static void PopulateResource(resource_pb::Resource* resource, bool arrow_ipc = false) {
    auto add = [&](string_view key, string_view val) {
      SetString(resource->add_attributes(), key, val);
    };
    add("service.name", "pg_stat_ch");
    add("service.version", PG_STAT_CH_VERSION);
    add("host.name", GetAHostname("postgres-primary"));
    if (arrow_ipc) {
      add("pg_stat_ch.block_format", "arrow_ipc");
    }
  }

  void ConfigureLogExport(const string& endpoint) {
    // Default constructor reads OTEL_EXPORTER_OTLP_* env vars; GUC wins
    http_opts_ = otlp::OtlpHttpLogRecordExporterOptions();
    if (!endpoint.empty()) {
      http_opts_.url = endpoint;
    }
    http_opts_.timeout = std::chrono::milliseconds(psch_otel_log_delay_ms);
    // bgworker owns retry backoff, disable client retry
    http_opts_.retry_policy_max_attempts = 0;

    max_chunk_bytes_ = std::max<size_t>(kMinBytesPerRecord, psch_otel_log_max_bytes);
    max_chunk_records_ = std::max<size_t>(
        1, std::min<size_t>(psch_otel_log_batch_size, max_chunk_bytes_ / kMinBytesPerRecord));
  }

  // HTTP client state
  otlp::OtlpHttpLogRecordExporterOptions http_opts_;
  // Chunk upload results, written by curl-thread callbacks, drained by
  // CommitBatch; declared before client_ so they outlive session callbacks
  std::atomic<size_t> async_ok_records_{0};
  std::atomic<int> async_failed_chunks_{0};
  std::atomic<int> async_failure_result_{-1};
  std::unique_ptr<otlp::OtlpHttpClient> client_;
  size_t max_chunk_records_ = 1;
  size_t max_chunk_bytes_ = kMinBytesPerRecord;
  int consecutive_failures_ = 0;
  int exported_count_ = 0;
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
    const string endpoint =
        (psch_otel_endpoint != nullptr && *psch_otel_endpoint != '\0') ? psch_otel_endpoint : "";

    ConfigureLogExport(endpoint);
    // Catch grpc-era host:port values early; OTLP/HTTP needs a full URL
    if (http_opts_.url.find("://") == string::npos) {
      LogExporterWarning("OTel init failed",
                         "otel_endpoint must be an http:// or https:// URL, e.g. "
                         "http://localhost:4318/v1/logs");
      client_.reset();
      return false;
    }
    client_ = std::make_unique<otlp::OtlpHttpClient>(MakeClientOptions(http_opts_));
    return true;
  } catch (const std::exception& e) {
    LogExporterWarning("OTel init failed", e.what());
    client_.reset();
    return false;
  }
}

bool OTelExporter::CommitBatch() {
  if (client_ == nullptr) {
    ResetFailures();
    return true;
  }

  try {
    if (!batch_failed_) {
      FlushChunk();
    }
    // Settle all in-flight chunks so success/failure stays per batch
    const bool flushed = client_->ForceFlush(
        std::chrono::duration_cast<std::chrono::microseconds>(http_opts_.timeout) +
        std::chrono::seconds(1));
    exported_count_ += static_cast<int>(async_ok_records_.exchange(0));
    const int failed_chunks = async_failed_chunks_.exchange(0);
    const int failure_result = async_failure_result_.exchange(-1);

    if (flushed && failed_chunks == 0) {
      ResetFailures();
      return true;
    }

    const char* reason =
        failed_chunks > 0
            ? ExportResultToString(
                  static_cast<opentelemetry::sdk::common::ExportResult>(failure_result))
            : "flush timeout";
    LogExporterWarning("OTLP export failed", reason);
    RecordExporterFailure(reason);
    consecutive_failures_++;
    return false;
  } catch (const std::exception& e) {
    LogExporterWarning("export exception", e.what());
    RecordExporterFailure(e.what());
    consecutive_failures_++;
    return false;
  }
}

bool OTelExporter::SendArrowBatch(const uint8_t* ipc_data, size_t ipc_len, int num_rows) {
  if (client_ == nullptr || ipc_data == nullptr || ipc_len == 0 || num_rows <= 0) {
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
    PopulateResource(resource_logs->mutable_resource(), /*arrow_ipc=*/true);
    auto* scope_logs = resource_logs->add_scope_logs();
    scope_logs->mutable_scope()->set_name("pg_stat_ch");
    scope_logs->mutable_scope()->set_version(PG_STAT_CH_VERSION);

    auto* record = scope_logs->add_log_records();
    const auto now_ns =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count());
    record->set_time_unix_nano(now_ns);
    record->set_observed_time_unix_nano(now_ns);
    record->mutable_body()->set_bytes_value(reinterpret_cast<const char*>(ipc_data), ipc_len);
    SetString(AddAttr(record), "pg_stat_ch.block_format", "arrow_ipc");
    SetInt(AddAttr(record), "pg_stat_ch.block_rows", num_rows);

    auto result = client_->Export(*request);

    if (result == opentelemetry::sdk::common::ExportResult::kSuccess) {
      exported_count_ += num_rows;
      consecutive_failures_ = 0;
      return true;
    }

    LogExporterWarning("Arrow batch export failed", ExportResultToString(result));
    RecordExporterFailure(ExportResultToString(result));
    consecutive_failures_++;
    return false;
  } catch (const std::exception& e) {
    LogExporterWarning("Arrow batch export exception", e.what());
    RecordExporterFailure(e.what());
    consecutive_failures_++;
    return false;
  }
}

}  // namespace

std::unique_ptr<StatsExporter> MakeOpenTelemetryExporter() {
  return std::make_unique<OTelExporter>();
}
