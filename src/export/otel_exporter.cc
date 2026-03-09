#include <opentelemetry/common/key_value_iterable_view.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_factory.h>
#include <opentelemetry/sdk/logs/batch_log_record_processor_factory.h>
#include <opentelemetry/sdk/logs/batch_log_record_processor_options.h>
#include <opentelemetry/sdk/logs/logger_provider.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include "opentelemetry/logs/provider.h"
#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/metrics/provider.h"

#include "config/guc.h"
#include "export/exporter_interface.h"

#include <absl/container/flat_hash_map.h>

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <map>
#include <string>
#include <unistd.h>

// Exposed with external linkage so unit tests can link against it directly.
std::string GetAHostname(const char* fallback) {
  if (psch_hostname && *psch_hostname)
    return psch_hostname;
  const char* env = getenv("HOSTNAME");
  if (env && *env)
    return env;
  char buf[256];
  if (gethostname(buf, sizeof(buf)) == 0) {
    buf[sizeof(buf) - 1] = '\0';
    return buf;
  }
  return fallback;
}

namespace {

namespace logs = opentelemetry::logs;
namespace logs_sdk = opentelemetry::sdk::logs;
namespace metrics = opentelemetry::metrics;
namespace metrics_sdk = opentelemetry::sdk::metrics;
namespace nostd = opentelemetry::nostd;
namespace otlp = opentelemetry::exporter::otlp;
namespace otel_common = opentelemetry::common;

// Because Microsoft ruins everything
template <typename T>
using otel_shared_ptr = nostd::shared_ptr<T>;
template <typename T>
using otel_unique_ptr = nostd::unique_ptr<T>;

class OTelExporter : public StatsExporter {
 public:
  void BeginBatch() final {
    if (row_active)
      EndRow();  // Just in case
    columns.clear();
    exported_count = 0;
  }

  void BeginRow() final {
    if (row_active)
      EndRow();  // We don't make the user call this
    current_row_tags.clear();
    current_log_record = logger->CreateLogRecord();
    row_active = true;
  }

  bool CommitBatch() final;

  shared_ptr<Column<string>> TagString(string_view name) final {
    return Wrap<TagColumn<string>>(name);
  }

  shared_ptr<Column<int16_t>> MetricInt16(string_view name) final {
    return Wrap<HistogramColumn<int16_t>>(name);
  }
  shared_ptr<Column<int32_t>> MetricInt32(string_view name) final {
    return Wrap<HistogramColumn<int32_t>>(name);
  }
  shared_ptr<Column<int64_t>> MetricInt64(string_view name) final {
    return Wrap<HistogramColumn<int64_t>>(name);
  }
  shared_ptr<Column<uint8_t>> MetricUInt8(string_view name) final {
    return Wrap<HistogramColumn<uint8_t>>(name);
  }
  shared_ptr<Column<uint64_t>> MetricUInt64(string_view name) final {
    return Wrap<HistogramColumn<uint64_t>>(name);
  }
  shared_ptr<Column<string_view>> MetricFixedString(int, string_view name) final {
    return Wrap<CounterColumn>(name);
  }

  shared_ptr<Column<int16_t>> RecordInt16(string_view name) final {
    return Wrap<RecordOnlyColumn<int16_t>>(name);
  }
  shared_ptr<Column<int32_t>> RecordInt32(string_view name) final {
    return Wrap<RecordOnlyColumn<int32_t>>(name);
  }
  shared_ptr<Column<int64_t>> RecordInt64(string_view name) final {
    return Wrap<RecordOnlyColumn<int64_t>>(name);
  }
  shared_ptr<Column<uint8_t>> RecordUInt8(string_view name) final {
    return Wrap<RecordOnlyColumn<uint8_t>>(name);
  }
  shared_ptr<Column<uint64_t>> RecordUInt64(string_view name) final {
    return Wrap<RecordOnlyColumn<uint64_t>>(name);
  }
  shared_ptr<Column<int64_t>> RecordDateTime(string_view name) final {
    return Wrap<RecordOnlyColumn<int64_t>>(name);
  }
  shared_ptr<Column<string_view>> RecordString(string_view name) final {
    return Wrap<RecordOnlyColumn<string_view>>(name);
  }

  // Semantic columns
  shared_ptr<Column<string>> DbNameColumn() final { return Wrap<TagColumn<string>>("db.name"); }
  shared_ptr<Column<string>> DbUserColumn() final { return Wrap<TagColumn<string>>("db.user"); }
  shared_ptr<Column<uint64_t>> DbDurationColumn() final { return Wrap<DurationColumn>(); }
  shared_ptr<Column<string>> DbOperationColumn() final {
    return Wrap<TagColumn<string>>("db.operation.name");
  }
  shared_ptr<Column<string_view>> DbQueryTextColumn() final {
    return Wrap<RecordOnlyColumn<string_view>>("db.query.text");
  }

  bool EstablishNewConnection() final;
  bool IsConnected() const final { return metrics_provider && log_provider; }
  int NumConsecutiveFailures() const final { return consecutive_failures; }
  void ResetFailures() final { consecutive_failures = 0; }
  int NumExported() const final { return exported_count; }

 private:
  void EndRow() {
    if (!row_active)
      return;

    for (auto& col : columns) {
      col->Crunch();  // Crunch happens for each row in OTel, not just the batch
    }

    logger->EmitLogRecord(std::move(current_log_record));
    row_active = false;
    ++exported_count;
  }

  // =====================================================================
  // Column implementation classes (translate OTel concepts to CH Columns)
  // =====================================================================

  // No Instrument, Tag Column: Applies a tag to all metrics in the row.
  template <typename T>
  class TagColumn : public Column<T> {
   public:
    TagColumn(OTelExporter* e, string_view n) : exp(e), name(n) {}

    void Append(const T& val) final {
      // Always add values to the log record.
      exp->current_log_record->SetAttribute(name, val);
      // Convert to string and store in the shared map for THIS row
      exp->current_row_tags[name] = to_string(val);
    }
    void Crunch() final {}  // Nothing to do, tags are passive at EndRow

    static string to_string(const string& x) { return x; }
    static string to_string(string_view x) { return string(x); }
    template <typename U>
    static string to_string(U x) {
      return std::to_string(x);
    }

   private:
    OTelExporter* exp;
    string name;
  };

  // Histogram Instrument Metric Column: Buckets numeric metrics.
  template <typename T>
  class HistogramColumn : public Column<T> {
   public:
    HistogramColumn(OTelExporter* e, string_view n)
        : exp(e), name(n), instrument(exp->GetUnsignedHistogram(name)) {}

    void Append(const T& val) final {
      // Always add values to the log record.
      exp->current_log_record->SetAttribute(name, val);
      // Stash the value until later, when all tags have been gathered
      // implicit cast from T (int32_t) to int64_t happens here
      if (val < 0) {
        LogNegativeValue(name, static_cast<int64_t>(val));
        stash_val = 0;
      } else {
        stash_val = static_cast<uint64_t>(val);
      }
    }

    void Crunch() final { instrument->Record(stash_val, exp->current_row_tags, {}); }

   private:
    OTelExporter* exp;
    string name;
    otel_shared_ptr<metrics::Histogram<uint64_t>> instrument;
    uint64_t stash_val = 0;
  };

  // View over current_row_tags that appends one extra K/V pair without copying the map.
  class MapPlusExtraPairView : public otel_common::KeyValueIterable {
   public:
    MapPlusExtraPairView(const absl::flat_hash_map<string, string>& base,
                         nostd::string_view extra_key, nostd::string_view extra_val)
        : base_(base), extra_key_(extra_key), extra_val_(extra_val) {}

    bool ForEachKeyValue(nostd::function_ref<bool(nostd::string_view, otel_common::AttributeValue)>
                             callback) const noexcept override {
      for (const auto& [k, v] : base_) {
        if (!callback(k, nostd::string_view(v)))
          return false;
      }
      return callback(extra_key_, extra_val_);
    }

    size_t size() const noexcept override { return base_.size() + 1; }

   private:
    const absl::flat_hash_map<string, string>& base_;
    nostd::string_view extra_key_;
    nostd::string_view extra_val_;
  };

  // 3. Counter Instrument Metric Column (for histograms of specific tag values)
  class CounterColumn : public Column<string_view> {
   public:
    CounterColumn(OTelExporter* e, string_view n)
        : exp(e), name(n), instrument(exp->GetUnsignedCounter(name + ".count")) {}

    void Append(const string_view& val) final {
      stash_val = std::string(val);
      exp->current_log_record->SetAttribute(name, stash_val);
    }

    void Crunch() final {
      MapPlusExtraPairView view(exp->current_row_tags, name, stash_val);
      instrument->Add(1, view);
    }

   private:
    OTelExporter* exp;
    string name;
    string stash_val;
    otel_shared_ptr<metrics::Counter<uint64_t>> instrument;
  };

  // 4. Record Only Data Column: No metrics, just logs.
  // (Note that all columns are put in records; these just do nothing else.)
  template <typename T>
  class RecordOnlyColumn : public Column<T> {
   public:
    RecordOnlyColumn(OTelExporter* e, string_view n) : exp(e), name(n) {}

    void Append(const T& val) final {
      assert(exp->row_active && exp->current_log_record);
      exp->current_log_record->SetAttribute(name, NoStringViews(val));
    }
    void Crunch() final {}  // No metrics to emit

    template <typename U>
    const U& NoStringViews(const U& x) {
      return x;
    }
    string NoStringViews(std::string_view x) { return string{x}; }

   private:
    OTelExporter* exp;
    std::string name;
  };

  // 5. Duration Column: converts µs → seconds for OTel db.client.operation.duration histogram.
  class DurationColumn : public Column<uint64_t> {
   public:
    explicit DurationColumn(OTelExporter* e)
        : exp(e), instrument(exp->GetDoubleHistogram("db.client.operation.duration")) {}

    void Append(const uint64_t& val) final {
      exp->current_log_record->SetAttribute("duration_us", static_cast<int64_t>(val));
      stash_val = val;
    }

    void Crunch() final {
      double seconds = static_cast<double>(stash_val) / 1e6;
      instrument->Record(seconds, exp->current_row_tags, {});
    }

   private:
    OTelExporter* exp;
    uint64_t stash_val = 0;
    otel_shared_ptr<metrics::Histogram<double>> instrument;
  };

  template <typename T>
  std::shared_ptr<T> Wrap(string_view name) {
    auto col = std::make_shared<T>(this, name);
    columns.push_back(col);  // Keep alive for the batch
    return col;
  }

  // Wrap overload for column types that don't take a name (e.g. DurationColumn).
  template <typename T>
  std::shared_ptr<T> Wrap() {
    auto col = std::make_shared<T>(this);
    columns.push_back(col);
    return col;
  }

  otel_shared_ptr<metrics::Histogram<uint64_t>> GetUnsignedHistogram(std::string_view name) {
    // Insert a nullptr placeholder.
    // 'inserted' is true if the key didn't exist.
    // 'it' points to the element (either new or existing).
    auto [it, inserted] = histogram_cache.insert(HistogramMap::value_type{name, nullptr});
    if (inserted) {
      // Only create the heavy OTel object if we actually inserted a new key
      it->second = meter->CreateUInt64Histogram(it->first, "description", "unit");
    }
    return it->second;
  }

  otel_shared_ptr<metrics::Counter<uint64_t>> GetUnsignedCounter(std::string_view name) {
    auto [it, inserted] = counter_cache.insert(CounterMap::value_type{name, nullptr});
    if (inserted) {
      it->second = meter->CreateUInt64Counter(it->first, "description", "unit");
    }
    return it->second;
  }

  otel_shared_ptr<metrics::Histogram<double>> GetDoubleHistogram(std::string_view name) {
    auto [it, inserted] =
        double_histogram_cache.insert(DoubleHistogramMap::value_type{name, nullptr});
    if (inserted) {
      it->second = meter->CreateDoubleHistogram(it->first, "description", "s");
    }
    return it->second;
  }

  // =====================================================================
  // OTel connection state
  // =====================================================================
  otel_shared_ptr<metrics::Meter> meter;
  otel_shared_ptr<logs::Logger> logger;
  shared_ptr<metrics_sdk::MeterProvider> metrics_provider;
  shared_ptr<metrics_sdk::MetricReader> metrics_reader;
  shared_ptr<logs_sdk::LoggerProvider> log_provider;
  int consecutive_failures = 0;
  int exported_count = 0;

  using HistogramMap = std::map<string, otel_shared_ptr<metrics::Histogram<uint64_t>>, std::less<>>;
  HistogramMap histogram_cache;
  using DoubleHistogramMap =
      std::map<string, otel_shared_ptr<metrics::Histogram<double>>, std::less<>>;
  DoubleHistogramMap double_histogram_cache;
  using CounterMap = std::map<string, otel_shared_ptr<metrics::Counter<uint64_t>>, std::less<>>;
  CounterMap counter_cache;

  // Row state
  bool row_active = false;
  absl::flat_hash_map<string, string> current_row_tags;
  otel_unique_ptr<logs::LogRecord> current_log_record;

  std::vector<shared_ptr<BasicColumn>> columns;
};

bool OTelExporter::EstablishNewConnection() {
  try {
    const std::string hostname = GetAHostname("postgres-primary");
    const std::string endpoint =
        (psch_otel_endpoint && *psch_otel_endpoint) ? psch_otel_endpoint : "localhost:4317";
    const std::string pgch_version = PG_STAT_CH_VERSION;

    // Resource (The "ID Card" for our service)
    auto resource_attributes = opentelemetry::sdk::resource::ResourceAttributes{
        {"service.name", "pg_stat_ch"},
        {"service.version", pgch_version},
        {"host.name", hostname}  // Ideally fetch real hostname
    };
    auto resource = opentelemetry::sdk::resource::Resource::Create(resource_attributes);

    // Configure Metrics
    // -------------------------------------------------------------------------
    otlp::OtlpGrpcMetricExporterOptions metric_opts;
    metric_opts.endpoint = endpoint;

    // Configure Reader (async periodic export — does not block bgworker)
    metrics_sdk::PeriodicExportingMetricReaderOptions reader_opts;
    reader_opts.export_interval_millis = std::chrono::milliseconds(psch_otel_metric_interval_ms);
    reader_opts.export_timeout_millis = std::chrono::milliseconds(psch_otel_metric_interval_ms / 2);

    metrics_reader = metrics_sdk::PeriodicExportingMetricReaderFactory::Create(
        otlp::OtlpGrpcMetricExporterFactory::Create(metric_opts), reader_opts);

    // Create the Provider with our Resource and add our Reader
    // Note: We use the ViewRegistry (default)
    metrics_provider = std::make_shared<metrics_sdk::MeterProvider>(
        std::make_unique<metrics_sdk::ViewRegistry>(), resource);
    metrics_provider->AddMetricReader(metrics_reader);

    // Configure Logs
    // -------------------------------------------------------------------------
    otlp::OtlpGrpcLogRecordExporterOptions log_opts;
    log_opts.endpoint = endpoint;

    // Create Logger Provider with batch processor for throughput.
    // Cap max_export_batch_size by the byte budget: even at the minimum variable-field
    // size the fixed overhead alone can push a large batch over the gRPC 4 MiB default.
    // DequeueEvents already enforces the byte budget on the producer side; this caps
    // the SDK's internal batch size as a second line of defence.
    static constexpr size_t kOtelMinBytesPerRecord = 1200;  // fixed overhead only
    size_t batch_size_by_bytes =
        static_cast<size_t>(psch_otel_log_max_bytes) / kOtelMinBytesPerRecord;
    logs_sdk::BatchLogRecordProcessorOptions batch_opts;
    batch_opts.max_queue_size = psch_otel_log_queue_size;
    batch_opts.max_export_batch_size =
        std::min(static_cast<size_t>(psch_otel_log_batch_size), batch_size_by_bytes);
    batch_opts.schedule_delay_millis = std::chrono::milliseconds(psch_otel_log_delay_ms);

    log_provider = std::make_shared<logs_sdk::LoggerProvider>(
        logs_sdk::BatchLogRecordProcessorFactory::Create(
            otlp::OtlpGrpcLogRecordExporterFactory::Create(log_opts), batch_opts),
        resource);

    // Get Instruments
    // -------------------------------------------------------------------------
    meter = metrics_provider->GetMeter("pg_stat_ch", pgch_version);
    logger = log_provider->GetLogger("pg_stat_ch", "pg_stat_ch_logs");

    return true;
  } catch (const std::exception& e) {
    // PschLog(LogLevel::Warning, "pg_stat_ch: OTel init failed: %s", e.what());
    return false;
  }
}

bool OTelExporter::CommitBatch() {
  EndRow();

  // Both metrics and logs are exported asynchronously by background threads:
  //   - PeriodicExportingMetricReader: exports histograms every metric_interval_ms
  //   - BatchLogRecordProcessor: exports log batches every log_delay_ms
  //
  // We do NOT call ForceFlush here. ForceFlush blocks until the background
  // thread finishes its current gRPC export, which stalls dequeuing for seconds
  // and causes shmem queue overflow. Instead, EmitLogRecord() just enqueues to
  // the batch processor's internal buffer (non-blocking), and the bgworker
  // loops immediately back to dequeue more events.
  //
  // Trade-off: if the batch processor's internal queue fills up (gRPC slower
  // than event rate), it silently drops log records. This is acceptable for
  // best-effort telemetry — the alternative (blocking) causes shmem drops which
  // lose events before they're even processed.

  ResetFailures();
  return true;
}

}  // namespace

std::unique_ptr<StatsExporter> MakeOpenTelemetryExporter() {
  return std::make_unique<OTelExporter>();
}
