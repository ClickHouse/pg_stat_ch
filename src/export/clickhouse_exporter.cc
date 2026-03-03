// pg_stat_ch ClickHouse exporter implementation

extern "C" {
#include "postgres.h"
}

#include <memory>
#include <string>
#include <vector>

#include <clickhouse/client.h>

#include "config/guc.h"
#include "export/exporter_interface.h"
#include "queue/shmem.h"  // PschRecordExportFailure

namespace {

// ClickHouse-flavored Stats Exporter
// Builds a clickhouse::Block and uploads it
class ClickHouseExporter : public StatsExporter {
 public:
  // Tags in CH are just ordinary columns; aggregation happens in queries
  shared_ptr<Column<string>> TagString(string_view name) final {
    return Wrap<clickhouse::ColumnString, string>(name);
  }

  // Metrics are also all ordinary columns.
  shared_ptr<Column<int16_t>> MetricInt16(string_view name) final {
    return Wrap<clickhouse::ColumnInt16>(name);
  }
  shared_ptr<Column<int32_t>> MetricInt32(string_view name) final {
    return Wrap<clickhouse::ColumnInt32>(name);
  }
  shared_ptr<Column<int64_t>> MetricInt64(string_view name) final {
    return Wrap<clickhouse::ColumnInt64>(name);
  }
  shared_ptr<Column<uint8_t>> MetricUInt8(string_view name) final {
    return Wrap<clickhouse::ColumnUInt8>(name);
  }
  shared_ptr<Column<uint64_t>> MetricUInt64(string_view name) final {
    return Wrap<clickhouse::ColumnUInt64>(name);
  }
  shared_ptr<Column<string_view>> MetricFixedString(int len, string_view name) final {
    return Wrap<clickhouse::ColumnFixedString, string_view>(name, len);
  }

  // Records... you guessed it
  shared_ptr<Column<int32_t>> RecordInt32(string_view name) final {
    return Wrap<clickhouse::ColumnInt32>(name);
  }
  shared_ptr<Column<int64_t>> RecordInt64(string_view name) final {
    return Wrap<clickhouse::ColumnInt64>(name);
  }
  shared_ptr<Column<int64_t>> RecordDateTime(string_view name) final {
    return Wrap<clickhouse::ColumnDateTime64, int64_t>(name, 6);
  }
  shared_ptr<Column<string_view>> RecordString(string_view name) final {
    return Wrap<clickhouse::ColumnString, string_view>(name);
  }

  void BeginBatch() final {
    block = std::make_unique<clickhouse::Block>();
    columns.clear();
    exported_count = 0;
  }
  void BeginRow() final { ++exported_count; }
  bool CommitBatch() final;

  bool EstablishNewConnection() final;
  bool IsConnected() const final { return (bool)client; }
  int NumConsecutiveFailures() const final { return consecutive_failures; }
  void ResetFailures() final { consecutive_failures = 0; }
  int NumExported() const final { return exported_count; }

 private:
  template <typename T, typename U>
  class ClickHouseColumn : public Column<T> {
   public:
    template <typename... CH_Args>
    ClickHouseColumn(ClickHouseExporter* exporter_, std::string_view name_, CH_Args&&... args)
        : exporter(exporter_), name(name_), ch_column(std::make_shared<U>(args...)) {}

    void Append(const T& t) final { ch_column->Append(t); }
    void Crunch() final { exporter->block->AppendColumn(name, ch_column); }

   private:
    ClickHouseExporter* const exporter;
    std::string name;
    const shared_ptr<U> ch_column;
  };

  template <class T, typename U = typename T::DataType, typename... Args>
  shared_ptr<ClickHouseColumn<U, T>> Wrap(std::string_view name, Args&&... args) {
    auto col = std::make_shared<ClickHouseColumn<U, T>>(this, name, args...);
    columns.push_back(col);
    return col;
  }

  std::unique_ptr<clickhouse::Client> client;
  std::unique_ptr<clickhouse::Block> block;
  std::vector<shared_ptr<BasicColumn>> columns;
  int consecutive_failures = 0;
  int exported_count = 0;
};

bool ClickHouseExporter::CommitBatch() {
  try {
    if (!block) {
      elog(WARNING, "pg_stat_ch: Logic error: Block not built");
      return false;
    }
    for (const auto& col : columns) {
      col->Crunch();
    }

    if (!client && (!EstablishNewConnection() || !client)) {
      elog(WARNING, "pg_stat_ch: Connection not established; bailing.");
      return false;
    }

    elog(DEBUG1, "pg_stat_ch: Inserting Block to ClickHouse");
    client->Insert("events_raw", *block);
    elog(DEBUG1, "pg_stat_ch: insert completed");

    // Success: reset retry state and record success timestamp
    consecutive_failures = 0;
    elog(DEBUG1, "pg_stat_ch: exported %d events to ClickHouse", exported_count);
    return true;

  } catch (const std::exception& ex) {
    std::string err_msg = ex.what();
    elog(WARNING, "pg_stat_ch: failed to insert to ClickHouse: %s", err_msg.c_str());

    // Failure: increment counter, record error, reset client for reconnect
    consecutive_failures++;
    PschRecordExportFailure(err_msg.c_str());
    client.reset();
    return false;
  }
}

bool ClickHouseExporter::EstablishNewConnection() {
  try {
    clickhouse::ClientOptions options;

    // Socket timeouts prevent indefinite blocking during network I/O. Without them,
    // the bgworker can't respond to PostgreSQL signals (e.g., DROP DATABASE waits
    // for ProcSignalBarrier acknowledgment). 30 seconds balances reliability on
    // slow networks against signal responsiveness. See bgworker.cc for details.
    constexpr auto kSocketTimeout = std::chrono::seconds(30);

    options.SetHost(psch_clickhouse_host != nullptr ? psch_clickhouse_host : "localhost")
        .SetPort(psch_clickhouse_port)
        .SetUser(psch_clickhouse_user != nullptr ? psch_clickhouse_user : "default")
        .SetPassword(psch_clickhouse_password != nullptr ? psch_clickhouse_password : "")
        .SetDefaultDatabase(psch_clickhouse_database != nullptr ? psch_clickhouse_database
                                                                : "pg_stat_ch")
        .SetCompressionMethod(clickhouse::CompressionMethod::LZ4)
        .SetPingBeforeQuery(true)
        .SetSendRetries(3)
        .SetRetryTimeout(std::chrono::seconds(5))
        .SetConnectionConnectTimeout(kSocketTimeout)
        .SetConnectionRecvTimeout(kSocketTimeout)
        .SetConnectionSendTimeout(kSocketTimeout);

    if (psch_clickhouse_use_tls) {
      clickhouse::ClientOptions::SSLOptions ssl_opts;
      ssl_opts.SetUseDefaultCALocations(true).SetUseSNI(true).SetSkipVerification(
          psch_clickhouse_skip_tls_verify);
      options.SetSSLOptions(ssl_opts);
      elog(LOG, "pg_stat_ch: TLS enabled for ClickHouse connection%s",
           psch_clickhouse_skip_tls_verify ? " (verification skipped)" : "");
    }

    client = std::make_unique<clickhouse::Client>(options);

    const char* host = psch_clickhouse_host != nullptr ? psch_clickhouse_host : "localhost";
    elog(LOG, "pg_stat_ch: connected to ClickHouse at %s:%d%s", host, psch_clickhouse_port,
         psch_clickhouse_use_tls ? " (TLS)" : "");

    return true;
  } catch (const std::exception& ex) {
    std::string err_msg = ex.what();
    elog(WARNING, "pg_stat_ch: failed to connect to ClickHouse: %s", err_msg.c_str());
    client.reset();
    return false;
  }
}

}  // namespace

std::unique_ptr<StatsExporter> MakeClickHouseExporter() {
  return std::make_unique<ClickHouseExporter>();
}
