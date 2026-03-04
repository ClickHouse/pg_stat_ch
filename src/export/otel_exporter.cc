#include <opentelemetry/common/key_value_iterable_view.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_factory.h>
#include <opentelemetry/sdk/logs/logger_provider.h>
#include <opentelemetry/sdk/logs/simple_log_record_processor.h>
#include <opentelemetry/sdk/logs/simple_log_record_processor_factory.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include "opentelemetry/logs/provider.h"
#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/metrics/provider.h"

#include "config/guc.h"
#include "export/exporter_interface.h"

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <map>
#include <unistd.h>

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

  shared_ptr<Column<int32_t>> RecordInt32(string_view name) final {
    return Wrap<RecordOnlyColumn<int32_t>>(name);
  }
  shared_ptr<Column<int64_t>> RecordInt64(string_view name) final {
    return Wrap<RecordOnlyColumn<int64_t>>(name);
  }
  shared_ptr<Column<int64_t>> RecordDateTime(string_view name) final {
    return Wrap<RecordOnlyColumn<int64_t>>(name);
  }
  shared_ptr<Column<string_view>> RecordString(string_view name) final {
    return Wrap<RecordOnlyColumn<string_view>>(name);
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
      // 1. Copy the shared tags
      auto tags_with_value = exp->current_row_tags;
      // 2. Inject the string value as a tag for this specific metric
      tags_with_value[name] = stash_val;
      // 3. Increment counter for this tag combination
      instrument->Add(1, tags_with_value);
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

  template <typename T>
  std::shared_ptr<T> Wrap(string_view name) {
    auto col = std::make_shared<T>(this, name);
    columns.push_back(col);  // Keep alive for the batch
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
  using CounterMap = std::map<string, otel_shared_ptr<metrics::Counter<uint64_t>>, std::less<>>;
  CounterMap counter_cache;

  // Row state
  bool row_active = false;
  std::map<string, string> current_row_tags;
  otel_unique_ptr<logs::LogRecord> current_log_record;

  std::vector<shared_ptr<BasicColumn>> columns;
};

const char* def(const char* val, const char* default_) {
  return val && *val ? val : default_;
}

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

bool OTelExporter::EstablishNewConnection() {
  try {
    const std::string hostname = GetAHostname("postgres-primary");
    const std::string endpoint = def(psch_otel_endpoint, "localhost:4317");
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

    // Configure Reader (Manual Flush Mode)
    metrics_sdk::PeriodicExportingMetricReaderOptions reader_opts;
    reader_opts.export_interval_millis = std::chrono::milliseconds::max();
    reader_opts.export_timeout_millis = std::chrono::milliseconds(1000);

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

    // Create Logger Provider WITH the same Resource
    log_provider = std::make_shared<logs_sdk::LoggerProvider>(
        logs_sdk::SimpleLogRecordProcessorFactory::Create(
            otlp::OtlpGrpcLogRecordExporterFactory::Create(log_opts)),
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
  // 1. Finish the last row logic (as discussed)
  EndRow();

  // Flush Metrics (The Reader scrapes and sends)
  bool metrics_ok = metrics_reader->ForceFlush(std::chrono::seconds(1));

  // Flush Logs (The Provider pushes the Processor to send)
  bool logs_ok = log_provider->ForceFlush(std::chrono::seconds(1));

  // Only count it as a success if both pipelines are healthy.
  if (metrics_ok && logs_ok) {
    ResetFailures();
    return true;
  } else {
    consecutive_failures++;
    // PschLog(LogLevel::Warning, "pg_stat_ch: OTel export failed "
    //         "(Metrics: %d, Logs: %d)", metrics_ok, logs_ok);
    return false;
  }
}

}  // namespace

std::unique_ptr<StatsExporter> MakeOpenTelemetryExporter() {
  return std::make_unique<OTelExporter>();
}
