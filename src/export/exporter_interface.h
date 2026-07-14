#ifndef PG_STAT_CH_SRC_EXPORT_EXPORTER_INTERFACE_H_
#define PG_STAT_CH_SRC_EXPORT_EXPORTER_INTERFACE_H_

#include <cstddef>
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

  class BasicColumn {
   public:
    virtual void Crunch() = 0;  // Implementation-defined processing helper.
    virtual void Clear() {}
    virtual ~BasicColumn() = default;
  };
  template <typename T>
  class Column : public BasicColumn {
   public:
    virtual void Append(const T& t) = 0;
    virtual ~Column() = default;
  };

 public:
  // ===========================================================================
  // Cardinality-typed columns.
  //
  // Stat<LC|HC><Type>(name) declares both the wire type and the *cardinality
  // intent* of the column. Each backend honors that intent appropriately:
  //
  //   ClickHouse: LC -> may be stored as LowCardinality(<Type>); HC -> plain.
  //               Schema-declared encoding wins on write; the LC hint helps
  //               the producer pick the cheapest column representation.
  //   Arrow IPC:  LC -> DictBuilder (dictionary-encoded array); HC -> plain
  //               typed builder. Required for batch-rate efficiency on
  //               low-cardinality dimensions.
  //   OTel:       LC -> eligible as a histogram dimension or metric label;
  //               HC -> log attribute only, *never* a metric dimension
  //               (cardinality explosion).
  //
  // Cardinality is a property of the data, not just the type width. err_elevel
  // (UInt8) is LC because values repeat (~5 distinct codes). query_id (Int64)
  // is HC because every distinct query produces a distinct value. The author
  // declares intent; the backend implements it.
  // ===========================================================================

  // Low-cardinality columns: dimensions you'd group/filter by.
  virtual shared_ptr<Column<string>> StatLCString(string_view name) = 0;
  virtual shared_ptr<Column<uint8_t>> StatLCUInt8(string_view name) = 0;
  virtual shared_ptr<Column<int16_t>> StatLCInt16(string_view name) = 0;
  virtual shared_ptr<Column<int32_t>> StatLCInt32(string_view name) = 0;

  // High-cardinality columns: values you observe, not dimensions you group by.
  virtual shared_ptr<Column<string_view>> StatHCString(string_view name) = 0;
  virtual shared_ptr<Column<int64_t>> StatHCInt64(string_view name) = 0;
  virtual shared_ptr<Column<uint64_t>> StatHCUInt64(string_view name) = 0;

  // Domain-specific. Caller appends a Unix-epoch microsecond timestamp.
  // (PG-epoch values must be offset by kPostgresEpochOffsetUs before append;
  // CH DateTime64(6) and OTel time_unix_nano both interpret the wire value
  // as Unix-epoch.)
  virtual shared_ptr<Column<int64_t>> StatTimestamp(string_view name) = 0;

  // ===========================================================================
  // Semantic columns: name, unit, and instrument type may vary by backend.
  // Add a column here only when its behavior meaningfully differs across
  // exporters (e.g. OTel semconv requires a different name, unit, or
  // instrument). Pure virtuals enforce explicit handling in every exporter.
  // ===========================================================================

  // Database name. CH: StatLCString "db_name"; OTel semconv: "db.name" tag.
  virtual shared_ptr<Column<string>> DbNameColumn() = 0;
  // Authenticated user. CH: StatLCString "db_user"; OTel semconv: "db.user" tag.
  virtual shared_ptr<Column<string>> DbUserColumn() = 0;
  // Query duration. Caller appends microseconds. CH: StatHCUInt64 "duration_us";
  // OTel: converts to seconds, records as Histogram<double> "db.client.operation.duration".
  virtual shared_ptr<Column<uint64_t>> DbDurationColumn() = 0;
  // SQL command type. CH: StatLCString "db_operation"; OTel: StatLCString "db.operation.name"
  // (used as a dimension on the duration histogram).
  virtual shared_ptr<Column<string>> DbOperationColumn() = 0;
  // Query text. CH: StatHCString "query_text"; OTel semconv: "db.query.text".
  virtual shared_ptr<Column<string_view>> DbQueryTextColumn() = 0;

  virtual void BeginBatch() = 0;
  virtual void BeginRow() = 0;
  virtual bool CommitBatch() = 0;

  virtual bool EstablishNewConnection() = 0;
  virtual bool IsConnected() const = 0;
  virtual int NumConsecutiveFailures() const = 0;
  virtual void ResetFailures() = 0;
  virtual int NumExported() const = 0;
  virtual bool SendArrowBatch(const uint8_t* ipc_data, size_t ipc_len, int num_rows) {
    (void)ipc_data;
    (void)ipc_len;
    (void)num_rows;
    return false;
  }

  virtual ~StatsExporter() = default;
};

// Bridge functions for PG logging from files that cannot include postgres.h
// (e.g. otel_exporter.cc conflicts with libintl.h via Arrow/protobuf headers).
void LogNegativeValue(const std::string& column_name, int64_t value);
void LogExporterWarning(const char* context, const char* message);
void RecordExporterFailure(const char* message);

// Expected usage:
// void ProcessBatch(StatsExporter *exporter) {
//   exporter->BeginBatch(); // no op or ClickHouse column reset
//   auto col_user = exporter->StatLCString("db_user");
//   auto col_rows = exporter->StatHCUInt64("rows");
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
