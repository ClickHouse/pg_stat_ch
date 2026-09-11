// pg_stat_ch ClickHouse exporter implementation

#include <algorithm>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include "clickhouse-client.h"
#include "clickhouse-compression.h"
#include "clickhouse-openssl.h"
#include "clickhouse-posix-io.h"
#include "clickhouse.h"

#include "export/clickhouse_exporter.h"
#include "export/diagnostics.h"
#include "export/exporter_config.h"
#include "export/exporter_interface.h"

// Abort blocking I/O once caller raises its cancellation flag
extern "C" {
static bool PschChcCheckCancel(void* ud) {
  return *static_cast<const volatile sig_atomic_t*>(ud) != 0;
}
}

namespace {

constexpr int kSocketTimeoutSec = 30;

// Bound packets consumed while awaiting response
constexpr int kMaxRecvPackets = 4096;

const chc_alloc& StdAlloc() {
  static const chc_alloc kAlloc = chc_alloc_stdlib();
  return kAlloc;
}

int64_t MonotonicNowUs() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (static_cast<int64_t>(ts.tv_sec) * 1000000) + (ts.tv_nsec / 1000);
}

// Restore blocking mode after bounded connect, clickhouse-c I/O expects it
bool ConnectWithTimeout(int fd, const struct sockaddr* sa, socklen_t slen, int timeout_sec) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    return false;

  bool ok = false;
  int save_errno = 0;
  if (connect(fd, sa, slen) == 0) {
    ok = true;
  } else if (errno == EINPROGRESS) {
    const int64_t deadline = MonotonicNowUs() + static_cast<int64_t>(timeout_sec) * 1000000;
    for (;;) {
      const int64_t remaining_us = deadline - MonotonicNowUs();
      if (remaining_us <= 0) {
        save_errno = ETIMEDOUT;
        break;
      }
      struct pollfd pfd = {fd, POLLOUT, 0};
      int pr = poll(&pfd, 1, static_cast<int>((remaining_us + 999) / 1000));
      if (pr < 0) {
        if (errno == EINTR)
          continue;  // signal (eg bgworker latch); retry within deadline
        save_errno = errno;
        break;
      }
      if (pr == 0) {
        save_errno = ETIMEDOUT;
        break;
      }
      int soerr = 0;
      socklen_t l = sizeof soerr;
      if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &l) < 0) {
        save_errno = errno;
        break;
      }
      save_errno = soerr;
      ok = soerr == 0;
      break;
    }
  } else {
    save_errno = errno;
  }

  if (fcntl(fd, F_SETFL, flags) < 0)
    return false;
  if (!ok)
    errno = save_errno;
  return ok;
}

struct ChcBlockBuilderDeleter {
  void operator()(chc_block_builder* bb) const { chc_block_builder_destroy(bb); }
};

using ChcBlockBuilderPtr = std::unique_ptr<chc_block_builder, ChcBlockBuilderDeleter>;

struct ChcTypeDeleter {
  void operator()(chc_type* type) const { chc_type_destroy(type, &StdAlloc()); }
};

using ChcTypePtr = std::unique_ptr<chc_type, ChcTypeDeleter>;

class ClickHouseExporter : public StatsExporter {
 public:
  ClickHouseExporter(const ExporterConfig* config, Diagnostics* diag)
      : StatsExporter(config, diag) {}
  ~ClickHouseExporter() override { CloseConnection(); }

  // Send plain types, ClickHouse applies schema-declared LowCardinality encoding

  // Low-cardinality columns
  shared_ptr<Column<string>> StatLCString(string_view name) final {
    return MakeCol<StringCol<string>>(name);
  }
  shared_ptr<Column<uint8_t>> StatLCUInt8(string_view name) final {
    return MakeCol<FixedCol<uint8_t>>(name, "UInt8");
  }
  shared_ptr<Column<int16_t>> StatLCInt16(string_view name) final {
    return MakeCol<FixedCol<int16_t>>(name, "Int16");
  }
  shared_ptr<Column<int32_t>> StatLCInt32(string_view name) final {
    return MakeCol<FixedCol<int32_t>>(name, "Int32");
  }

  // High-cardinality columns
  shared_ptr<Column<string_view>> StatHCString(string_view name) final {
    return MakeCol<StringCol<string_view>>(name);
  }
  shared_ptr<Column<int64_t>> StatHCInt64(string_view name) final {
    return MakeCol<FixedCol<int64_t>>(name, "Int64");
  }
  shared_ptr<Column<uint64_t>> StatHCUInt64(string_view name) final {
    return MakeCol<FixedCol<uint64_t>>(name, "UInt64");
  }

  shared_ptr<Column<int64_t>> StatTimestamp(string_view name) final {
    return MakeCol<FixedCol<int64_t>>(name, "DateTime64(6)");
  }

  // Semantic columns
  shared_ptr<Column<string>> DbNameColumn() final { return StatLCString("db_name"); }
  shared_ptr<Column<string>> DbUserColumn() final { return StatLCString("db_user"); }
  shared_ptr<Column<uint64_t>> DbDurationColumn() final { return StatHCUInt64("duration_us"); }
  shared_ptr<Column<string>> DbOperationColumn() final { return StatLCString("db_operation"); }
  shared_ptr<Column<string_view>> DbQueryTextColumn() final { return StatHCString("query_text"); }

  void BeginBatch() final {
    for (auto& col : columns_)
      col->Clear();
    row_count_ = 0;
  }
  void BeginRow() final { ++row_count_; }
  bool CommitBatch() final;

  bool EstablishNewConnection() final;
  bool IsConnected() const noexcept final { return client_ != nullptr; }

 private:
  // Buffers rows on Append, then Crunch materializes them into the chc block builder at commit;
  // Clear resets buffers between batches.
  class ChColumn {
   public:
    virtual bool Crunch() = 0;  // Flush buffered rows into block builder. True on success.
    virtual void Clear() = 0;
    virtual ~ChColumn() = default;
  };

  template <typename T>
  class FixedCol : public Column<T>, public ChColumn {
   public:
    FixedCol(ClickHouseExporter* exp, string_view name, const char* type_name)
        : exp_(exp), name_(name), type_name_(type_name) {}
    void Append(const T& v) final { data_.push_back(v); }
    bool Crunch() final { return exp_->AppendFixed(name_, type_name_, data_.data(), data_.size()); }
    void Clear() final { data_.clear(); }

   private:
    ClickHouseExporter* const exp_;
    const std::string name_;
    const char* const type_name_;
    std::vector<T> data_;
  };

  template <typename T>
  class StringCol : public Column<T>, public ChColumn {
   public:
    StringCol(ClickHouseExporter* exp, string_view name) : exp_(exp), name_(name) {}
    void Append(const T& s) final {
      bytes_.insert(bytes_.end(), s.data(), s.data() + s.size());
      offsets_.push_back(bytes_.size());
    }
    bool Crunch() final {
      return exp_->AppendString(name_, offsets_.data(),
                                reinterpret_cast<const uint8_t*>(bytes_.data()), offsets_.size());
    }
    void Clear() final {
      bytes_.clear();
      offsets_.clear();
    }

   private:
    ClickHouseExporter* const exp_;
    const std::string name_;
    std::vector<char> bytes_;
    std::vector<uint64_t> offsets_;
  };

  template <typename ColT, typename... Args>
  shared_ptr<ColT> MakeCol(string_view name, Args&&... args) {
    if (auto it = col_index_.find(name); it != col_index_.end()) {
      if (auto col = std::dynamic_pointer_cast<ColT>(columns_[it->second]))
        return col;

      throw std::logic_error("ClickHouse column `" + std::string(name) +
                             "` requested with incompatible type");
    }

    std::string key(name);
    auto col = std::make_shared<ColT>(this, key, std::forward<Args>(args)...);
    const size_t index = columns_.size();

    columns_.reserve(index + 1);
    col_names_.reserve(index + 1);
    try {
      columns_.push_back(col);
      col_names_.push_back(key);
      col_index_.emplace(col_names_.back(), index);
    } catch (...) {
      columns_.resize(index);
      col_names_.resize(index);
      throw;
    }
    return col;
  }

  const chc_type* ResolveType(const char* type_name);

  bool AppendFixed(string_view name, const char* type_name, const void* data, size_t n_rows);
  bool AppendString(string_view name, const uint64_t* offsets, const uint8_t* data, size_t n_rows);

  bool Fail(const char* context, std::string_view message, bool close_conn);
  bool TcpConnect(const char* host, int port);
  bool TlsConnect(const char* host);
  void CloseConnection();
  void SetReadDeadline();
  std::string BuildInsertQuery() const;
  bool SendInsert(const chc_block_builder* bb);
  bool RecvUntil(chc_packet_kind target);

  chc_client* client_ = nullptr;
  int fd_ = -1;
  SSL_CTX* ssl_ctx_ = nullptr;
  SSL* ssl_ = nullptr;
  chc_posix_io posix_io_{};
  chc_openssl_io openssl_io_{};
  chc_io io_{};
  chc_codec codec_{};

  chc_block_builder* bb_ = nullptr;
  chc_err build_err_{};

  std::map<std::string, ChcTypePtr, std::less<>> types_;
  std::vector<shared_ptr<ChColumn>> columns_;
  std::map<std::string, size_t, std::less<>> col_index_;  // name -> index in columns_
  std::vector<std::string> col_names_;
  int row_count_ = 0;
};

bool ClickHouseExporter::Fail(const char* context, std::string_view message, bool close_conn) {
  diag_->Fail(context, message);
  if (close_conn)
    CloseConnection();
  return false;
}

const chc_type* ClickHouseExporter::ResolveType(const char* type_name) {
  auto it = types_.find(type_name);
  if (it != types_.end())
    return it->second.get();
  chc_type* t = nullptr;
  chc_err err = {};
  if (chc_type_parse(type_name, std::strlen(type_name), &StdAlloc(), &t, &err) != CHC_OK) {
    build_err_ = err;
    return nullptr;
  }
  ChcTypePtr type(t);
  types_.emplace(type_name, std::move(type));
  return t;
}

bool ClickHouseExporter::AppendFixed(string_view name, const char* type_name, const void* data,
                                     size_t n_rows) {
  const chc_type* t = ResolveType(type_name);
  if (t == nullptr)
    return false;
  return chc_block_builder_append_fixed(bb_, name.data(), name.size(), t, data, n_rows,
                                        &build_err_) == CHC_OK;
}

bool ClickHouseExporter::AppendString(string_view name, const uint64_t* offsets,
                                      const uint8_t* data, size_t n_rows) {
  return chc_block_builder_append_string(bb_, name.data(), name.size(), offsets, data, n_rows,
                                         &build_err_) == CHC_OK;
}

std::string ClickHouseExporter::BuildInsertQuery() const {
  std::string q = "INSERT INTO events_raw (";
  for (size_t i = 0; i < col_names_.size(); ++i) {
    if (i != 0)
      q += ", ";
    q += col_names_[i];
  }
  q += ") VALUES";
  return q;
}

void ClickHouseExporter::SetReadDeadline() {
  const int64_t dl = MonotonicNowUs() + static_cast<int64_t>(kSocketTimeoutSec) * 1000000;
  if (ssl_ != nullptr)
    chc_openssl_io_set_deadline(&openssl_io_, dl);
  else
    chc_posix_io_set_deadline(&posix_io_, dl);
}

bool ClickHouseExporter::RecvUntil(chc_packet_kind target) {
  struct PacketGuard {
    chc_client* client;
    chc_packet* packet;
    ~PacketGuard() { chc_packet_clear(client, packet); }
  };

  for (int i = 0; i < kMaxRecvPackets; ++i) {
    chc_packet pkt = {};
    chc_err err = {};
    if (chc_client_recv_packet(client_, &pkt, &err) != CHC_OK) {
      return Fail("failed to insert to ClickHouse", err.msg[0] ? err.msg : "recv_packet failed",
                  false);
    }
    PacketGuard guard{client_, &pkt};
    const chc_packet_kind kind = pkt.kind;
    if (kind == CHC_PKT_EXCEPTION) {
      if (pkt.exception != nullptr && pkt.exception->display_text != nullptr)
        return Fail("failed to insert to ClickHouse",
                    {pkt.exception->display_text, pkt.exception->display_text_len}, false);
      return Fail("failed to insert to ClickHouse", "server exception", false);
    }
    if (kind == target)
      return true;
  }
  return Fail("failed to insert to ClickHouse", "too many packets awaiting response", false);
}

bool ClickHouseExporter::SendInsert(const chc_block_builder* bb) {
  chc_err err = {};
  SetReadDeadline();

  const std::string query = BuildInsertQuery();
  if (chc_client_send_query(client_, query.data(), query.size(), "", 0, &err) != CHC_OK) {
    return Fail("failed to insert to ClickHouse", err.msg[0] ? err.msg : "send_query failed",
                false);
  }
  // Server echoes a 0-row Data block describing the target columns.
  if (!RecvUntil(CHC_PKT_DATA))
    return false;

  if (chc_client_send_data(client_, bb, &err) != CHC_OK) {
    return Fail("failed to insert to ClickHouse", err.msg[0] ? err.msg : "send_data failed", false);
  }
  // Empty trailing block terminates the INSERT stream.
  if (chc_client_send_data(client_, nullptr, &err) != CHC_OK) {
    return Fail("failed to insert to ClickHouse",
                err.msg[0] ? err.msg : "send_data terminator failed", false);
  }

  SetReadDeadline();
  return RecvUntil(CHC_PKT_END_OF_STREAM);
}

bool ClickHouseExporter::CommitBatch() {
  struct ActiveBuilder {
    chc_block_builder*& slot;
    ~ActiveBuilder() { slot = nullptr; }
  };

  try {
    if (client_ == nullptr)
      return Fail("ClickHouse insert", "not connected", false);

    chc_block_builder* raw_bb = nullptr;
    chc_err err = {};
    if (chc_block_builder_init(&raw_bb, &StdAlloc(), &err) != CHC_OK)
      return Fail("block builder init failed", err.msg[0] ? err.msg : "OOM", false);
    ChcBlockBuilderPtr bb(raw_bb);

    bb_ = bb.get();
    ActiveBuilder active{bb_};

    build_err_ = {};
    for (const auto& col : columns_) {
      if (!col->Crunch()) {
        return Fail("failed to build ClickHouse block",
                    build_err_.msg[0] ? build_err_.msg : "block build failed", false);
      }
    }

    if (!SendInsert(bb.get())) {
      CloseConnection();
      return false;
    }

    diag_->AddExported(static_cast<uint32_t>(row_count_));
    return true;
  } catch (const std::bad_alloc&) {
    return Fail("ClickHouse insert", "out of memory", true);
  } catch (const std::exception& e) {
    return Fail("ClickHouse insert", e.what(), true);
  }
}

bool ClickHouseExporter::TcpConnect(const char* host, int port) {
  struct addrinfo hints = {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  char port_s[16];
  snprintf(port_s, sizeof port_s, "%d", port);
  char msg[256];

  struct addrinfo* res = nullptr;
  const int rc = getaddrinfo(host, port_s, &hints, &res);
  if (rc != 0) {
    snprintf(msg, sizeof msg, "getaddrinfo(%s:%d): %s", host, port, gai_strerror(rc));
    return Fail("ClickHouse connect", msg, false);
  }

  int fd = -1;
  int save_errno = ECONNREFUSED;
  for (struct addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
    fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
      save_errno = errno;
      continue;
    }
    if (ConnectWithTimeout(fd, ai->ai_addr, ai->ai_addrlen, kSocketTimeoutSec))
      break;
    save_errno = errno;
    close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  if (fd < 0) {
    snprintf(msg, sizeof msg, "connect(%s:%d): %s", host, port, strerror(save_errno));
    return Fail("ClickHouse connect", msg, false);
  }

  const int one = 1;
  (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
  (void)setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof one);
  struct timeval tv = {kSocketTimeoutSec, 0};
  (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
  (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
  fd_ = fd;
  return true;
}

bool ClickHouseExporter::TlsConnect(const char* host) {
  // OpenSSL 1.1.0+ auto-initializes on first use; no explicit library init.
  ssl_ctx_ = SSL_CTX_new(TLS_client_method());
  if (ssl_ctx_ == nullptr)
    return Fail("TLS setup", "SSL_CTX_new failed", false);
  if (!config_->clickhouse_skip_tls_verify) {
    SSL_CTX_set_verify(ssl_ctx_, SSL_VERIFY_PEER, nullptr);
    if (SSL_CTX_set_default_verify_paths(ssl_ctx_) != 1)
      return Fail("TLS setup", "could not load default CA certificates", false);
  }

  ssl_ = SSL_new(ssl_ctx_);
  if (ssl_ == nullptr)
    return Fail("TLS setup", "SSL_new failed", false);
  if (SSL_set_tlsext_host_name(ssl_, host) != 1)  // SNI
    return Fail("TLS setup", "could not set TLS SNI host name", false);
  if (!config_->clickhouse_skip_tls_verify) {
    SSL_set_hostflags(ssl_, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
    if (SSL_set1_host(ssl_, host) != 1)
      return Fail("TLS setup", "could not set TLS verification host", false);
  }
  if (SSL_set_fd(ssl_, fd_) != 1)
    return Fail("TLS setup", "SSL_set_fd failed", false);
  ERR_clear_error();  // queue is per-thread, drop any residue from a prior retry
  if (SSL_connect(ssl_) != 1) {
    // cert-verify failures leave the error queue empty, so check vr first
    const long vr = SSL_get_verify_result(ssl_);
    char ebuf[256];
    if (vr != X509_V_OK) {
      snprintf(ebuf, sizeof ebuf, "certificate verify failed: %s",
               X509_verify_cert_error_string(vr));
    } else if (const unsigned long e = ERR_get_error()) {
      ERR_error_string_n(e, ebuf, sizeof ebuf);  // cipher / protocol mismatch etc
    } else {
      snprintf(ebuf, sizeof ebuf, "SSL_connect failed");
    }
    return Fail("TLS handshake failed", ebuf, false);
  }
  return true;
}

void ClickHouseExporter::CloseConnection() {
  if (client_ != nullptr) {
    chc_client_close(client_);
    client_ = nullptr;
  }
  if (ssl_ != nullptr) {
    SSL_shutdown(ssl_);
    SSL_free(ssl_);
    ssl_ = nullptr;
  }
  if (ssl_ctx_ != nullptr) {
    SSL_CTX_free(ssl_ctx_);
    ssl_ctx_ = nullptr;
  }
  if (fd_ >= 0) {
    close(fd_);
    fd_ = -1;
  }
}

bool ClickHouseExporter::EstablishNewConnection() {
  CloseConnection();

  const char* host =
      config_->clickhouse_host.empty() ? "localhost" : config_->clickhouse_host.c_str();
  const int port = config_->clickhouse_port;

  if (!TcpConnect(host, port))
    return false;

  auto check_cancel = config_->cancel_flag != nullptr ? PschChcCheckCancel : nullptr;
  void* cancel_ud = const_cast<void*>(static_cast<const volatile void*>(config_->cancel_flag));
  if (config_->clickhouse_use_tls) {
    if (!TlsConnect(host)) {
      CloseConnection();
      return false;
    }
    chc_openssl_io_init(&openssl_io_, &io_, ssl_, check_cancel, cancel_ud);
  } else {
    chc_posix_io_init(&posix_io_, &io_, fd_, check_cancel, cancel_ud);
  }

  chc_lz4_codec_init(&codec_);

  chc_client_opts opts = {};
  opts.client_name = "pg_stat_ch";
  opts.database =
      config_->clickhouse_database.empty() ? "pg_stat_ch" : config_->clickhouse_database.c_str();
  opts.user = config_->clickhouse_user.empty() ? "default" : config_->clickhouse_user.c_str();
  opts.password = config_->clickhouse_password.c_str();
  opts.compression = CHC_COMP_LZ4;
  opts.codec = &codec_;

  SetReadDeadline();  // bound the Hello / Ping handshake reads
  chc_err err = {};
  if (chc_client_init(&client_, &opts, &StdAlloc(), &io_, &err) != CHC_OK)
    return Fail("failed to connect to ClickHouse", err.msg[0] ? err.msg : "init failed", true);
  return true;
}

}  // namespace

std::unique_ptr<StatsExporter> MakeClickHouseExporter(const ExporterConfig* config,
                                                      Diagnostics* diag) {
  return std::make_unique<ClickHouseExporter>(config, diag);
}
