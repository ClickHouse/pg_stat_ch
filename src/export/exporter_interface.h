#ifndef PG_STAT_CH_SRC_EXPORT_EXPORTER_INTERFACE_H_
#define PG_STAT_CH_SRC_EXPORT_EXPORTER_INTERFACE_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

class StatsExporter {
 protected:
  using string = std::string;
  using string_view = std::string_view;
  template <typename T>
  using shared_ptr = std::shared_ptr<T>;

  // Consecutive export failures, updated by the implementation.
  // Stored in the base class so implementations don't duplicate it, and so
  // that asynchronous error handlers (e.g. OTel GlobalLogHandler) can
  // increment it safely from a background thread.
  std::atomic<int> consecutive_failures{0};

  class BasicColumn {
   public:
    virtual void Crunch() = 0;  // Implementation-defined processing helper.
    virtual ~BasicColumn() = default;
  };
  template <typename T>
  class Column : public BasicColumn {
   public:
    virtual void Append(const T& t) = 0;
    virtual ~Column() = default;
  };

 public:
  // Tags: Columns that serve as narrowing criteria for metrics.
  virtual shared_ptr<Column<string>> TagString(string_view name) = 0;

  // Metrics: Columns that are generally bucketed into histograms.
  virtual shared_ptr<Column<int16_t>> MetricInt16(string_view name) = 0;
  virtual shared_ptr<Column<int32_t>> MetricInt32(string_view name) = 0;
  virtual shared_ptr<Column<int64_t>> MetricInt64(string_view name) = 0;
  virtual shared_ptr<Column<uint8_t>> MetricUInt8(string_view name) = 0;
  virtual shared_ptr<Column<uint64_t>> MetricUInt64(string_view name) = 0;
  virtual shared_ptr<Column<string_view>> MetricFixedString(int len, string_view name) = 0;

  // Records: Data columns you wouldn't want to filter by.
  virtual shared_ptr<Column<int16_t>> RecordInt16(string_view name) = 0;
  virtual shared_ptr<Column<int32_t>> RecordInt32(string_view name) = 0;
  virtual shared_ptr<Column<int64_t>> RecordInt64(string_view name) = 0;
  virtual shared_ptr<Column<uint8_t>> RecordUInt8(string_view name) = 0;
  virtual shared_ptr<Column<uint64_t>> RecordUInt64(string_view name) = 0;
  virtual shared_ptr<Column<int64_t>> RecordDateTime(string_view name) = 0;
  virtual shared_ptr<Column<string_view>> RecordString(string_view name) = 0;

  // ===========================================================================
  // Semantic columns: name, unit, and instrument type may vary by backend.
  // Add a column here only when its behavior meaningfully differs across
  // exporters (e.g. OTel semconv requires a different name, unit, or
  // instrument). Pure virtuals enforce explicit handling in every exporter.
  // ===========================================================================

  // Database name. CH: TagString "db"; OTel semconv: "db.name" tag.
  virtual shared_ptr<Column<string>> DbNameColumn() = 0;
  // Authenticated user. CH: TagString "username"; OTel semconv: "db.user" tag.
  virtual shared_ptr<Column<string>> DbUserColumn() = 0;
  // Query duration. Caller appends microseconds. CH: MetricUInt64 "duration_us";
  // OTel: converts to seconds, records as Histogram<double> "db.client.operation.duration".
  virtual shared_ptr<Column<uint64_t>> DbDurationColumn() = 0;
  // SQL command type. CH: RecordString "cmd_type"; OTel: TagString "db.operation.name"
  // (used as a dimension on the duration histogram).
  virtual shared_ptr<Column<string>> DbOperationColumn() = 0;
  // Query text. CH: RecordString "query"; OTel semconv: "db.query.text".
  virtual shared_ptr<Column<string_view>> DbQueryTextColumn() = 0;

  virtual void BeginBatch() = 0;
  virtual void BeginRow() = 0;
  virtual bool CommitBatch() = 0;

  virtual bool EstablishNewConnection() = 0;
  virtual bool IsConnected() const = 0;
  virtual int NumExported() const = 0;

  // Whether this backend supports re-establishing a broken connection.
  // Returns false for OTel: the SDK manages reconnection to the collector
  // internally, so the bgworker should not call EstablishNewConnection() after
  // an error or apply exponential backoff — that would fight the SDK's own
  // retry logic.
  virtual bool SupportsReconnection() const { return true; }

  int NumConsecutiveFailures() const { return consecutive_failures.load(); }
  void ResetFailures() { consecutive_failures.store(0); }

  virtual ~StatsExporter() = default;
};

// Allows PG logging of exceptional cases without postgres.h
void LogNegativeValue(const std::string& column_name, int64_t value);

// Expected usage:
// void ProcessBatch(StatsExporter *exporter) {
//   exporter->BeginBatch(); // no op or ClickHouse column reset
//   auto col_user = exporter->TagString("username");
//   auto col_rows = exporter->MetricUInt64("rows");
//
//   for (const auto &ev : events) {
//     exporter->BeginRow(); // no-op or initialize tag map
//     col_user->Append(ev.username);
//     col_rows->Append(ev.rows);
//   }
//
//   exporter->CommitBatch();  // Inserts or collects stats
// }

#endif  // PG_STAT_CH_SRC_EXPORT_EXPORTER_INTERFACE_H_
