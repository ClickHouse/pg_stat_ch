// OTLP/HTTP exporter backend.
//
// Hand-rolled HTTP/1.1 client over blocking sockets on the bgworker thread
// (TLS via OpenSSL when the endpoint scheme is https).  Replaces the gRPC
// direct-proto exporter: same OTLP ExportLogsServiceRequest payloads, but
// with zero heap allocation on the steady-state export path — the protobuf
// encode buffer, network buffer, and constant request-head prefix are all
// preallocated in PschOtelExporterCreate.  Reconnects (getaddrinfo, SSL_new)
// allocate, but only on the connect path, never per batch.
//
// Wire compatibility notes (oracle: the old otel_exporter.cc):
//  * Per-record path: attributes-only LogRecords (no body); zero-valued int
//    attrs are skipped, string attrs always emitted (even empty); duration
//    dual-encoded (db.client.operation.duration seconds-double + duration_us
//    int); time_unix_nano = (ts_start + PG epoch offset) * 1000.
//  * Arrow path: one LogRecord, body = bytes(IPC), block_format/block_rows
//    attrs, plus a resource-level pg_stat_ch.block_format attr.
//  * Encoded field surface pinned to opentelemetry-proto v1.9.0.

#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include "miscadmin.h"
#include "utils/memutils.h"

#include "pg_stat_ch/pg_stat_ch.h"
#include "config/guc.h"
#include "config/memory_budget.h"
#include "export/exporter.h"
#include "export/otlp_encode.h"
#include "queue/shmem.h"

// PostgreSQL epoch (2000-01-01) to Unix epoch (1970-01-01), in microseconds.
static const int64 kPostgresEpochOffsetUs = INT64CONST(946684800000000);

static const char kServiceName[] = "pg_stat_ch";
static const char kDefaultEndpoint[] = "http://localhost:4318";
static const char kDefaultPath[] = "/v1/logs";
static const int kDefaultTimeoutMs = 1000;

// Per-event encode ceiling used to decide when a chunk is full.  Must exceed
// the worst-case encoded LogRecord: all string payloads (~4.4 KiB after the
// clamps) + ~44 attributes of key/tag/length-slot overhead (~2.7 KiB) + ints.
enum { kEventEncodeMargin = 16 * 1024 };
StaticAssertDecl(PSCH_MAX_QUERY_LEN + PSCH_MAX_ERR_MSG_LEN + 8192 <= kEventEncodeMargin,
                 "encode margin must cover a worst-case event record");

// Room reserved in the net buffer after the head prefix for the
// "<Content-Length digits>\r\n\r\n" tail.
enum { kContentLengthTailMax = 32 };

typedef struct PschOtelExporter {
  PschExporter base;

  // Endpoint, parsed once at create (psch_otel_endpoint is PGC_POSTMASTER).
  char host[256];
  char path[512];
  int port;
  bool use_tls;

  char hostname[256];  // resource attr host.name

  // Constant request head up to and including "Content-Length: ".
  char* head_prefix;
  size_t head_prefix_len;
  Size head_alloc_bytes;

  uint8* encode_buf;  // OTLP request bodies (uncompressed protobuf)
  size_t encode_cap;
  uint8* net_buf;  // request head assembly + HTTP response
  size_t net_cap;

  int fd;
  SSL_CTX* ssl_ctx;  // created once at create() when use_tls
  SSL* ssl;
  bool connected;
  bool conn_reused;  // connection has completed >= 1 request (stale-retry gate)
  bool in_flight;    // wire exchange interrupted (longjmp) -> connection poisoned

  int consecutive_failures;
  uint64 mem_used;
} PschOtelExporter;

typedef struct PschHttpResponse {
  int status;
  const uint8* body;  // points into net_buf; valid only when body_complete
  size_t body_len;
  bool body_complete;
  long retry_after_s;  // -1 when absent / not delta-seconds
  bool reusable;
} PschHttpResponse;

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

static int64 PschMonotonicNowUs(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static int PschOtelTimeoutMs(void) {
  return psch_export_timeout_ms > 0 ? psch_export_timeout_ms : kDefaultTimeoutMs;
}

static void* PschOtelAlloc(Size size) {
  return MemoryContextAllocExtended(TopMemoryContext, size,
                                    MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO | MCXT_ALLOC_HUGE);
}

// Leading-whitespace-tolerant delta-seconds / decimal parser; -1 if the
// value does not start with a digit (e.g. an HTTP-date Retry-After).
static long PschParseDigits(const char* s, size_t len) {
  long v = 0;
  bool any = false;
  for (size_t i = 0; i < len; i++) {
    char c = s[i];
    if (c == ' ' || c == '\t') {
      if (any)
        break;
      continue;
    }
    if (c < '0' || c > '9')
      break;
    if (v > (LONG_MAX - 9) / 10)
      return -1;
    v = v * 10 + (c - '0');
    any = true;
  }
  return any ? v : -1;
}

static bool PschValueContains(const char* v, size_t vlen, const char* token) {
  size_t tlen = strlen(token);
  for (size_t i = 0; i + tlen <= vlen; i++) {
    if (pg_strncasecmp(v + i, token, tlen) == 0)
      return true;
  }
  return false;
}

static uint32 PschClampLen(uint32 len, uint32 max, const char* field_name) {
  if (len <= max)
    return len;
  // Rate-limit to at most one WARNING/sec: a corrupt or oversized field could
  // otherwise flood the log at the event rate.
  static time_t last_log = 0;
  time_t now = time(NULL);
  if (now - last_log >= 1) {
    elog(WARNING, "pg_stat_ch: invalid %s %u, clamping", field_name, len);
    last_log = now;
  }
  return max;
}

static void PschOtelFormatSslError(char* errbuf, size_t errlen, const char* what) {
  unsigned long e = ERR_get_error();
  if (e != 0) {
    char ebuf[256];
    ERR_error_string_n(e, ebuf, sizeof ebuf);
    snprintf(errbuf, errlen, "%s: %s", what, ebuf);
  } else {
    snprintf(errbuf, errlen, "%s", what);
  }
  ERR_clear_error();
}

// WARNING + consecutive-failure increment, exactly once per failed
// export_events/send_arrow call.  Shmem failure stats are recorded by the
// driver (stats_exporter.c) from the returned status, so the backend must NOT
// call PschRecordExportFailure here (it would double-count send_failures); the
// detailed message lives in this WARNING.
static void PschOtelFail(PschOtelExporter* exp, const char* context, const char* msg) {
  elog(WARNING, "pg_stat_ch: %s: %s", context, msg);
  exp->consecutive_failures++;
}

// ---------------------------------------------------------------------------
// Connection management (patterns from the proven ClickHouse exporter path)
// ---------------------------------------------------------------------------

static void PschOtelCloseConn(PschOtelExporter* exp) {
  if (exp->ssl != NULL) {
    (void)SSL_shutdown(exp->ssl);  // best-effort; bounded by SO_SNDTIMEO
    SSL_free(exp->ssl);
    exp->ssl = NULL;
  }
  if (exp->fd >= 0) {
    close(exp->fd);
    exp->fd = -1;
  }
  exp->connected = false;
  exp->conn_reused = false;
  exp->in_flight = false;
}

// Nonblocking connect bounded by deadline; restores blocking mode afterwards
// (the send/recv paths rely on SO_RCVTIMEO/SNDTIMEO).
static bool PschOtelSocketConnect(int fd, const struct sockaddr* sa, socklen_t slen,
                                  int64 deadline_us) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    return false;

  bool ok = false;
  int save_errno = 0;
  if (connect(fd, sa, slen) == 0) {
    ok = true;
  } else if (errno == EINPROGRESS) {
    for (;;) {
      if (ProcDiePending) {
        save_errno = ECANCELED;
        break;
      }
      const int64 remaining_us = deadline_us - PschMonotonicNowUs();
      if (remaining_us <= 0) {
        save_errno = ETIMEDOUT;
        break;
      }
      struct pollfd pfd = {.fd = fd, .events = POLLOUT, .revents = 0};
      int pr = poll(&pfd, 1, (int)((remaining_us + 999) / 1000));
      if (pr < 0) {
        if (errno == EINTR)
          continue;  // signal (e.g. bgworker latch); retry within deadline
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

static bool PschOtelTcpConnect(PschOtelExporter* exp, int64 deadline_us, char* errbuf,
                               size_t errlen) {
  struct addrinfo hints = {0};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  char port_s[16];
  snprintf(port_s, sizeof port_s, "%d", exp->port);

  struct addrinfo* res = NULL;
  int rc = getaddrinfo(exp->host, port_s, &hints, &res);
  if (rc != 0) {
    snprintf(errbuf, errlen, "getaddrinfo(%s:%d): %s", exp->host, exp->port, gai_strerror(rc));
    return false;
  }

  int fd = -1;
  int save_errno = ECONNREFUSED;
  for (struct addrinfo* ai = res; ai != NULL; ai = ai->ai_next) {
    if (ProcDiePending) {
      save_errno = ECANCELED;
      break;
    }
    fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
      save_errno = errno;
      continue;
    }
    if (PschOtelSocketConnect(fd, ai->ai_addr, ai->ai_addrlen, deadline_us))
      break;
    save_errno = errno;
    close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  if (fd < 0) {
    snprintf(errbuf, errlen, "connect(%s:%d): %s", exp->host, exp->port, strerror(save_errno));
    return false;
  }

  const int one = 1;
  (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
  (void)setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof one);
#ifdef SO_NOSIGPIPE
  (void)setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif
  const int timeout_ms = PschOtelTimeoutMs();
  struct timeval tv = {timeout_ms / 1000, (timeout_ms % 1000) * 1000};
  (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
  (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
  exp->fd = fd;
  return true;
}

static bool PschOtelTlsHandshake(PschOtelExporter* exp, int64 deadline_us, char* errbuf,
                                 size_t errlen) {
  exp->ssl = SSL_new(exp->ssl_ctx);
  if (exp->ssl == NULL) {
    PschOtelFormatSslError(errbuf, errlen, "SSL_new failed");
    return false;
  }
  if (SSL_set_tlsext_host_name(exp->ssl, exp->host) != 1) {  // SNI
    snprintf(errbuf, errlen, "could not set TLS SNI host name");
    return false;
  }
  SSL_set_hostflags(exp->ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
  if (SSL_set1_host(exp->ssl, exp->host) != 1) {
    snprintf(errbuf, errlen, "could not set TLS verification host");
    return false;
  }
  if (SSL_set_fd(exp->ssl, exp->fd) != 1) {
    snprintf(errbuf, errlen, "SSL_set_fd failed");
    return false;
  }
  ERR_clear_error();  // queue is per-thread, drop residue from a prior retry
  for (;;) {
    if (ProcDiePending) {
      snprintf(errbuf, errlen, "terminating (SIGTERM)");
      return false;
    }
    if (PschMonotonicNowUs() >= deadline_us) {
      snprintf(errbuf, errlen, "TLS handshake timeout");
      return false;
    }
    int rc = SSL_connect(exp->ssl);
    if (rc == 1)
      return true;
    int serr = SSL_get_error(exp->ssl, rc);
    if (serr == SSL_ERROR_WANT_READ || serr == SSL_ERROR_WANT_WRITE)
      continue;
    if (serr == SSL_ERROR_SYSCALL && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
      continue;
    // cert-verify failures leave the OpenSSL error queue empty, so check vr first
    const long vr = SSL_get_verify_result(exp->ssl);
    if (vr != X509_V_OK) {
      snprintf(errbuf, errlen, "TLS handshake failed: certificate verify failed: %s",
               X509_verify_cert_error_string(vr));
    } else {
      PschOtelFormatSslError(errbuf, errlen, "TLS handshake failed");
    }
    return false;
  }
}

static bool PschOtelEstablish(PschOtelExporter* exp, int64 deadline_us, char* errbuf,
                              size_t errlen) {
  PschOtelCloseConn(exp);
  if (!PschOtelTcpConnect(exp, deadline_us, errbuf, errlen))
    return false;
  if (exp->use_tls && !PschOtelTlsHandshake(exp, deadline_us, errbuf, errlen)) {
    PschOtelCloseConn(exp);
    return false;
  }
  exp->connected = true;
  exp->conn_reused = false;
  elog(DEBUG1, "pg_stat_ch: connected to OTLP endpoint %s:%d%s", exp->host, exp->port,
       exp->use_tls ? " (TLS)" : "");
  return true;
}

// ---------------------------------------------------------------------------
// Bounded blocking I/O
// ---------------------------------------------------------------------------

static bool PschOtelSendAll(PschOtelExporter* exp, const void* data, size_t len, int64 deadline_us,
                            char* errbuf, size_t errlen) {
  const uint8* p = data;
  size_t left = len;
  while (left > 0) {
    if (ProcDiePending) {
      snprintf(errbuf, errlen, "terminating (SIGTERM)");
      return false;
    }
    if (PschMonotonicNowUs() >= deadline_us) {
      snprintf(errbuf, errlen, "send timeout (%d ms)", PschOtelTimeoutMs());
      return false;
    }
    ssize_t n;
    if (exp->ssl != NULL) {
      n = SSL_write(exp->ssl, p, (int)Min(left, (size_t)INT_MAX));
      if (n <= 0) {
        int serr = SSL_get_error(exp->ssl, (int)n);
        if (serr == SSL_ERROR_WANT_READ || serr == SSL_ERROR_WANT_WRITE)
          continue;
        if (serr == SSL_ERROR_SYSCALL &&
            (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
          continue;
        PschOtelFormatSslError(errbuf, errlen, "TLS send failed");
        return false;
      }
    } else {
#ifdef MSG_NOSIGNAL
      n = send(exp->fd, p, left, MSG_NOSIGNAL);
#else
      n = send(exp->fd, p, left, 0);
#endif
      if (n < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
          continue;
        snprintf(errbuf, errlen, "send failed: %s", strerror(errno));
        return false;
      }
    }
    p += n;
    left -= (size_t)n;
  }
  return true;
}

// Returns >0 bytes read, 0 on EOF, -1 on error/timeout (errbuf filled).
static int PschOtelRecvSome(PschOtelExporter* exp, uint8* buf, size_t cap, int64 deadline_us,
                            char* errbuf, size_t errlen) {
  for (;;) {
    if (ProcDiePending) {
      snprintf(errbuf, errlen, "terminating (SIGTERM)");
      return -1;
    }
    if (PschMonotonicNowUs() >= deadline_us) {
      snprintf(errbuf, errlen, "response timeout (%d ms)", PschOtelTimeoutMs());
      return -1;
    }
    if (exp->ssl != NULL) {
      int n = SSL_read(exp->ssl, buf, (int)Min(cap, (size_t)INT_MAX));
      if (n > 0)
        return n;
      int serr = SSL_get_error(exp->ssl, n);
      if (serr == SSL_ERROR_ZERO_RETURN)
        return 0;
      if (serr == SSL_ERROR_WANT_READ || serr == SSL_ERROR_WANT_WRITE)
        continue;
      if (serr == SSL_ERROR_SYSCALL) {
        if (n == 0 || errno == 0)
          return 0;  // unclean close
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
          continue;
        snprintf(errbuf, errlen, "TLS recv failed: %s", strerror(errno));
        return -1;
      }
      PschOtelFormatSslError(errbuf, errlen, "TLS recv failed");
      return -1;
    }
    ssize_t n = recv(exp->fd, buf, Min(cap, (size_t)INT_MAX), 0);
    if (n > 0)
      return (int)n;
    if (n == 0)
      return 0;
    if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
      continue;
    snprintf(errbuf, errlen, "recv failed: %s", strerror(errno));
    return -1;
  }
}

// Consume exactly n more body bytes, discarding them (body larger than the
// net buffer).  False means the connection can no longer be reused.
static bool PschOtelDrainN(PschOtelExporter* exp, size_t n, int64 deadline_us) {
  char ebuf[64];
  while (n > 0) {
    int got =
        PschOtelRecvSome(exp, exp->net_buf, Min(n, exp->net_cap), deadline_us, ebuf, sizeof ebuf);
    if (got <= 0)
      return false;
    n -= (size_t)got;
  }
  return true;
}

static void PschOtelDrainToEof(PschOtelExporter* exp, int64 deadline_us) {
  char ebuf[64];
  for (;;) {
    int got = PschOtelRecvSome(exp, exp->net_buf, exp->net_cap, deadline_us, ebuf, sizeof ebuf);
    if (got <= 0)
      return;
  }
}

// ---------------------------------------------------------------------------
// HTTP request / response
// ---------------------------------------------------------------------------

static bool PschOtelSendRequest(PschOtelExporter* exp, const uint8* body, size_t body_len,
                                int64 deadline_us, char* errbuf, size_t errlen) {
  // Assemble the head in net_buf (free until the response is read).
  char* head = (char*)exp->net_buf;
  memcpy(head, exp->head_prefix, exp->head_prefix_len);
  int n = snprintf(head + exp->head_prefix_len, exp->net_cap - exp->head_prefix_len, "%zu\r\n\r\n",
                   body_len);
  if (n < 0 || (size_t)n >= exp->net_cap - exp->head_prefix_len) {
    snprintf(errbuf, errlen, "request head does not fit network buffer");
    return false;
  }
  if (!PschOtelSendAll(exp, head, exp->head_prefix_len + (size_t)n, deadline_us, errbuf, errlen))
    return false;
  return PschOtelSendAll(exp, body, body_len, deadline_us, errbuf, errlen);
}

// Reads status line + headers + body into net_buf.  Returns false only for
// transport-level failures (caller treats as send failure / stale retry);
// HTTP error statuses return true with r->status set.  *got_any reports
// whether any response byte arrived (gates the silent stale-retry).
static bool PschOtelReadResponse(PschOtelExporter* exp, int64 deadline_us, PschHttpResponse* r,
                                 bool* got_any, char* errbuf, size_t errlen) {
  uint8* buf = exp->net_buf;
  const size_t cap = exp->net_cap;
  size_t have = 0;
  size_t header_end = 0;
  size_t scanned = 0;

  memset(r, 0, sizeof *r);
  r->retry_after_s = -1;
  *got_any = false;

  for (;;) {
    while (scanned + 4 <= have) {
      if (memcmp(buf + scanned, "\r\n\r\n", 4) == 0) {
        header_end = scanned + 4;
        break;
      }
      scanned++;
    }
    if (header_end != 0)
      break;
    if (have == cap) {
      snprintf(errbuf, errlen, "response headers exceed %zu bytes", cap);
      return false;
    }
    int n = PschOtelRecvSome(exp, buf + have, cap - have, deadline_us, errbuf, errlen);
    if (n < 0)
      return false;
    if (n == 0) {
      snprintf(errbuf, errlen,
               have == 0 ? "connection closed before response" : "connection closed mid-headers");
      return false;
    }
    have += (size_t)n;
    *got_any = true;
  }

  if (have < 12 || memcmp(buf, "HTTP/1.", 7) != 0) {
    snprintf(errbuf, errlen, "malformed HTTP status line");
    return false;
  }
  r->reusable = buf[7] == '1';  // HTTP/1.1 defaults to keep-alive

  const uint8* sp = memchr(buf, ' ', header_end);
  long status =
      sp != NULL ? PschParseDigits((const char*)sp + 1, header_end - (size_t)(sp + 1 - buf)) : -1;
  if (status < 100 || status > 999) {
    snprintf(errbuf, errlen, "malformed HTTP status line");
    return false;
  }
  r->status = (int)status;

  size_t pos = 0;
  while (pos + 2 <= header_end && !(buf[pos] == '\r' && buf[pos + 1] == '\n'))
    pos++;
  pos += 2;  // first header line

  long content_length = -1;
  bool chunked = false;
  while (pos + 2 <= header_end) {
    size_t eol = pos;
    while (eol + 1 < header_end && !(buf[eol] == '\r' && buf[eol + 1] == '\n'))
      eol++;
    size_t llen = eol - pos;
    if (llen == 0)
      break;  // blank line terminates headers
    const char* line = (const char*)buf + pos;
    const char* colon = memchr(line, ':', llen);
    if (colon != NULL) {
      size_t nlen = (size_t)(colon - line);
      const char* val = colon + 1;
      size_t vlen = llen - nlen - 1;
      while (vlen > 0 && (*val == ' ' || *val == '\t')) {
        val++;
        vlen--;
      }
      if (nlen == 14 && pg_strncasecmp(line, "Content-Length", 14) == 0) {
        content_length = PschParseDigits(val, vlen);
      } else if (nlen == 17 && pg_strncasecmp(line, "Transfer-Encoding", 17) == 0) {
        chunked |= PschValueContains(val, vlen, "chunked");
      } else if (nlen == 11 && pg_strncasecmp(line, "Retry-After", 11) == 0) {
        r->retry_after_s = PschParseDigits(val, vlen);
      } else if (nlen == 10 && pg_strncasecmp(line, "Connection", 10) == 0) {
        if (PschValueContains(val, vlen, "close"))
          r->reusable = false;
        else if (PschValueContains(val, vlen, "keep-alive"))
          r->reusable = true;
      }
    }
    pos = eol + 2;
  }

  size_t body_have = have - header_end;

  if (chunked) {
    // We do not parse chunked framing; the connection cannot be reused.
    r->reusable = false;
    r->body_complete = false;
    PschOtelDrainToEof(exp, deadline_us);
    return true;
  }

  if (content_length < 0) {
    // No length: body is delimited by connection close.
    r->reusable = false;
    bool eof = false;
    while (have < cap) {
      int n = PschOtelRecvSome(exp, buf + have, cap - have, deadline_us, errbuf, errlen);
      if (n < 0) {
        r->body_complete = false;
        return true;  // status is known; give up on the body
      }
      if (n == 0) {
        eof = true;
        break;
      }
      have += (size_t)n;
    }
    if (eof) {
      r->body = buf + header_end;
      r->body_len = have - header_end;
      r->body_complete = true;
    } else {
      PschOtelDrainToEof(exp, deadline_us);
      r->body_complete = false;
    }
    return true;
  }

  if ((size_t)content_length <= cap - header_end) {
    while (body_have < (size_t)content_length) {
      int n = PschOtelRecvSome(exp, buf + have, cap - have, deadline_us, errbuf, errlen);
      if (n <= 0) {
        r->reusable = false;
        r->body_complete = false;
        // A 200 status already arrived: the export succeeded server-side, so
        // do not fail the call (a resend would duplicate the whole batch).
        if (r->status == 200)
          return true;
        snprintf(errbuf, errlen, "connection closed mid-response body (HTTP %d)", r->status);
        return false;
      }
      have += (size_t)n;
      body_have += (size_t)n;
    }
    r->body = buf + header_end;
    r->body_len = (size_t)content_length;
    r->body_complete = true;
  } else {
    if (!PschOtelDrainN(exp, (size_t)content_length - body_have, deadline_us))
      r->reusable = false;
    r->body_complete = false;
  }
  return true;
}

static PschExportStatus PschOtelHandleResponse(const PschHttpResponse* r, char* errbuf,
                                               size_t errlen) {
  if (r->status == 200) {
    if (r->body_complete && r->body_len > 0) {
      int64 rejected = 0;
      char msg[256] = "";
      if (PschOtlpParseLogsResponse(r->body, r->body_len, &rejected, msg, sizeof msg)) {
        // Spec: partial success is NOT retried; rejected records are dropped.
        if (rejected > 0) {
          elog(WARNING, "pg_stat_ch: OTLP partial success: %lld log records rejected: %s",
               (long long)rejected, msg[0] != '\0' ? msg : "(no message)");
        } else if (msg[0] != '\0') {
          elog(LOG, "pg_stat_ch: OTLP export warning from collector: %s", msg);
        }
      }
    } else if (!r->body_complete) {
      elog(WARNING,
           "pg_stat_ch: OTLP response body unparseable (chunked or oversized); "
           "treating export as success");
    }
    return PSCH_EXPORT_OK;
  }

  if (r->status == 429 || r->status == 502 || r->status == 503 || r->status == 504) {
    if (r->retry_after_s >= 0)
      snprintf(errbuf, errlen, "collector returned HTTP %d (Retry-After: %ld s)", r->status,
               r->retry_after_s);
    else
      snprintf(errbuf, errlen, "collector returned HTTP %d", r->status);
    return PSCH_EXPORT_ERR_SEND;
  }

  // Non-retryable per OTLP spec: permanent failure for this batch.
  int32 code = 0;
  char msg[256] = "";
  if (r->body_complete && r->body_len > 0 &&
      PschRpcStatusParse(r->body, r->body_len, &code, msg, sizeof msg) && msg[0] != '\0') {
    snprintf(errbuf, errlen, "collector rejected request: HTTP %d, rpc status %d: %s", r->status,
             (int)code, msg);
  } else {
    snprintf(errbuf, errlen, "collector rejected request: HTTP %d", r->status);
  }
  return PSCH_EXPORT_ERR_INTERNAL;
}

// One deadline-bounded POST of `body`, including connect-if-needed and one
// silent reconnect when a kept-alive connection turns out stale (failure
// before any response byte on a connection that already served a request).
static PschExportStatus PschOtelDoRequest(PschOtelExporter* exp, const uint8* body, size_t body_len,
                                          char* errbuf, size_t errlen) {
  const int64 deadline_us = PschMonotonicNowUs() + (int64)PschOtelTimeoutMs() * 1000;
  bool retried = false;

  for (;;) {
    if (!exp->connected && !PschOtelEstablish(exp, deadline_us, errbuf, errlen))
      return PSCH_EXPORT_ERR_CONN;

    const bool reusing = exp->conn_reused;
    exp->in_flight = true;
    bool got_any = false;
    PschHttpResponse resp;
    bool ok = PschOtelSendRequest(exp, body, body_len, deadline_us, errbuf, errlen) &&
              PschOtelReadResponse(exp, deadline_us, &resp, &got_any, errbuf, errlen);
    exp->in_flight = false;

    if (!ok) {
      PschOtelCloseConn(exp);
      if (reusing && !got_any && !retried && !ProcDiePending &&
          PschMonotonicNowUs() < deadline_us) {
        retried = true;  // stale keep-alive connection: one silent reconnect
        continue;
      }
      return PSCH_EXPORT_ERR_SEND;
    }

    exp->conn_reused = true;
    if (!resp.reusable)
      PschOtelCloseConn(exp);
    return PschOtelHandleResponse(&resp, errbuf, errlen);
  }
}

// ---------------------------------------------------------------------------
// OTLP payload encoding (opentelemetry-proto v1.9.0 surface)
// ---------------------------------------------------------------------------

// Opens ResourceLogs + Resource(+attrs) + ScopeLogs + scope; leaves the
// ResourceLogs and ScopeLogs length slots open for LogRecords.
static void PschOtelEnvelopeBegin(PschPbBuf* b, const PschOtelExporter* exp, bool arrow_ipc,
                                  size_t* rl_slot, size_t* sl_slot) {
  *rl_slot = PschPbMsgBegin(b, PSCH_OTLP_REQ_RESOURCE_LOGS);
  size_t res = PschPbMsgBegin(b, PSCH_OTLP_RL_RESOURCE);
  PschPbKvString(b, PSCH_OTLP_RES_ATTRIBUTES, "service.name", kServiceName, strlen(kServiceName));
  PschPbKvString(b, PSCH_OTLP_RES_ATTRIBUTES, "service.version", PG_STAT_CH_VERSION,
                 strlen(PG_STAT_CH_VERSION));
  PschPbKvString(b, PSCH_OTLP_RES_ATTRIBUTES, "host.name", exp->hostname, strlen(exp->hostname));
  if (arrow_ipc)
    PschPbKvString(b, PSCH_OTLP_RES_ATTRIBUTES, "pg_stat_ch.block_format", "arrow_ipc", 9);
  PschPbMsgEnd(b, res);

  *sl_slot = PschPbMsgBegin(b, PSCH_OTLP_RL_SCOPE_LOGS);
  size_t scope = PschPbMsgBegin(b, PSCH_OTLP_SL_SCOPE);
  PschPbString(b, PSCH_OTLP_SCOPE_NAME, kServiceName, strlen(kServiceName));
  PschPbString(b, PSCH_OTLP_SCOPE_VERSION, PG_STAT_CH_VERSION, strlen(PG_STAT_CH_VERSION));
  PschPbMsgEnd(b, scope);
}

static void PschOtelEnvelopeEnd(PschPbBuf* b, size_t rl_slot, size_t sl_slot) {
  PschPbMsgEnd(b, sl_slot);
  PschPbMsgEnd(b, rl_slot);
}

// Zero-valued int attrs are skipped (sparse encoding quirk of the original).
static inline void PschKvIntNz(PschPbBuf* b, const char* key, int64 v) {
  if (v != 0)
    PschPbKvInt(b, PSCH_OTLP_LR_ATTRIBUTES, key, v);
}

static void PschOtelEncodeEvent(PschPbBuf* b, const PschEvent* ev) {
  const uint32 attr = PSCH_OTLP_LR_ATTRIBUTES;
  size_t rec = PschPbMsgBegin(b, PSCH_OTLP_SL_LOG_RECORDS);

  const int64 ts_unix_us = ev->ts_start + kPostgresEpochOffsetUs;
  PschPbFixed64(b, PSCH_OTLP_LR_TIME_UNIX_NANO, (uint64)ts_unix_us * 1000);
  PschPbKvInt(b, attr, "ts_start", ts_unix_us);

  PschPbKvDouble(b, attr, "db.client.operation.duration", (double)ev->duration_us / 1e6);
  PschPbKvInt(b, attr, "duration_us", (int64)ev->duration_us);

  // Clamp to sizeof-1 (63), matching the Arrow and ClickHouse paths: emitting
  // the full 64-byte buffer could surface a trailing non-NUL byte on a corrupt
  // event and diverges from the NAMEDATALEN-1 contract.
  PschPbKvString(b, attr, "db.name", ev->datname,
                 Min(ev->datname_len, (uint8)(sizeof(ev->datname) - 1)));
  PschPbKvString(b, attr, "db.user", ev->username,
                 Min(ev->username_len, (uint8)(sizeof(ev->username) - 1)));
  PschKvIntNz(b, "pid", ev->pid);
  PschKvIntNz(b, "query_id", (int64)ev->queryid);
  {
    const char* cmd = PschCmdTypeToString(ev->cmd_type);
    PschPbKvString(b, attr, "db.operation.name", cmd, strlen(cmd));
  }
  PschKvIntNz(b, "rows", (int64)ev->rows);
  PschPbKvString(b, attr, "db.query.text", ev->query,
                 PschClampLen(ev->query_len, PSCH_MAX_QUERY_LEN, "query_len"));

  PschKvIntNz(b, "shared_blks_hit", ev->shared_blks_hit);
  PschKvIntNz(b, "shared_blks_read", ev->shared_blks_read);
  PschKvIntNz(b, "shared_blks_dirtied", ev->shared_blks_dirtied);
  PschKvIntNz(b, "shared_blks_written", ev->shared_blks_written);
  PschKvIntNz(b, "local_blks_hit", ev->local_blks_hit);
  PschKvIntNz(b, "local_blks_read", ev->local_blks_read);
  PschKvIntNz(b, "local_blks_dirtied", ev->local_blks_dirtied);
  PschKvIntNz(b, "local_blks_written", ev->local_blks_written);
  PschKvIntNz(b, "temp_blks_read", ev->temp_blks_read);
  PschKvIntNz(b, "temp_blks_written", ev->temp_blks_written);

  PschKvIntNz(b, "shared_blk_read_time_us", ev->shared_blk_read_time_us);
  PschKvIntNz(b, "shared_blk_write_time_us", ev->shared_blk_write_time_us);
  PschKvIntNz(b, "local_blk_read_time_us", ev->local_blk_read_time_us);
  PschKvIntNz(b, "local_blk_write_time_us", ev->local_blk_write_time_us);
  PschKvIntNz(b, "temp_blk_read_time_us", ev->temp_blk_read_time_us);
  PschKvIntNz(b, "temp_blk_write_time_us", ev->temp_blk_write_time_us);

  PschKvIntNz(b, "wal_records", ev->wal_records);
  PschKvIntNz(b, "wal_fpi", ev->wal_fpi);
  PschKvIntNz(b, "wal_bytes", (int64)ev->wal_bytes);

  PschKvIntNz(b, "cpu_user_time_us", ev->cpu_user_time_us);
  PschKvIntNz(b, "cpu_sys_time_us", ev->cpu_sys_time_us);

  PschKvIntNz(b, "jit_functions", ev->jit_functions);
  PschKvIntNz(b, "jit_generation_time_us", ev->jit_generation_time_us);
  PschKvIntNz(b, "jit_deform_time_us", ev->jit_deform_time_us);
  PschKvIntNz(b, "jit_inlining_time_us", ev->jit_inlining_time_us);
  PschKvIntNz(b, "jit_optimization_time_us", ev->jit_optimization_time_us);
  PschKvIntNz(b, "jit_emission_time_us", ev->jit_emission_time_us);

  PschKvIntNz(b, "parallel_workers_planned", ev->parallel_workers_planned);
  PschKvIntNz(b, "parallel_workers_launched", ev->parallel_workers_launched);

  // Always exactly 5 bytes, embedded NULs included (collector-side contract).
  PschPbKvString(b, attr, "err_sqlstate", ev->err_sqlstate, 5);
  PschKvIntNz(b, "err_elevel", ev->err_elevel);
  PschPbKvString(b, attr, "err_message", ev->err_message,
                 PschClampLen(ev->err_message_len, PSCH_MAX_ERR_MSG_LEN, "err_message_len"));

  PschPbKvString(b, attr, "app", ev->application_name,
                 PschClampLen(ev->application_name_len, PSCH_MAX_APP_NAME_LEN, "app_name_len"));
  PschPbKvString(b, attr, "client_addr", ev->client_addr,
                 PschClampLen(ev->client_addr_len, PSCH_MAX_CLIENT_ADDR_LEN, "client_addr_len"));

  PschPbMsgEnd(b, rec);
}

static void PschOtelEncodeArrowRecord(PschPbBuf* b, const uint8* ipc_data, size_t ipc_len,
                                      int num_rows) {
  size_t rec = PschPbMsgBegin(b, PSCH_OTLP_SL_LOG_RECORDS);

  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  const uint64 now_ns = (uint64)ts.tv_sec * 1000000000 + (uint64)ts.tv_nsec;
  PschPbFixed64(b, PSCH_OTLP_LR_TIME_UNIX_NANO, now_ns);
  PschPbFixed64(b, PSCH_OTLP_LR_OBSERVED_TIME_UNIX_NANO, now_ns);

  size_t body = PschPbMsgBegin(b, PSCH_OTLP_LR_BODY);
  PschPbBytes(b, PSCH_OTLP_ANY_BYTES, ipc_data, ipc_len);
  PschPbMsgEnd(b, body);

  PschPbKvString(b, PSCH_OTLP_LR_ATTRIBUTES, "pg_stat_ch.block_format", "arrow_ipc", 9);
  PschPbKvInt(b, PSCH_OTLP_LR_ATTRIBUTES, "pg_stat_ch.block_rows", num_rows);

  PschPbMsgEnd(b, rec);
}

// ---------------------------------------------------------------------------
// PschExporterOps implementation
// ---------------------------------------------------------------------------

static bool PschOtelOpConnect(PschExporter* self, char* errbuf, size_t errlen) {
  PschOtelExporter* exp = (PschOtelExporter*)self;
  char local_err[256];
  if (errbuf == NULL || errlen == 0) {
    errbuf = local_err;
    errlen = sizeof local_err;
  }
  errbuf[0] = '\0';

  const int64 deadline_us = PschMonotonicNowUs() + (int64)PschOtelTimeoutMs() * 1000;
  if (!PschOtelEstablish(exp, deadline_us, errbuf, errlen)) {
    exp->consecutive_failures++;
    return false;
  }
  return true;
}

static bool PschOtelOpIsConnected(const PschExporter* self) {
  return ((const PschOtelExporter*)self)->connected;
}

static PschExportStatus PschOtelOpExportEvents(PschExporter* self, const PschEvent* events,
                                               int nevents, int* exported_out) {
  PschOtelExporter* exp = (PschOtelExporter*)self;
  char errbuf[512];

  if (exported_out != NULL)
    *exported_out = 0;
  if (events == NULL || nevents <= 0)
    return PSCH_EXPORT_OK;

  // A longjmp interrupted the previous call mid-exchange: the HTTP state on
  // that connection is poisoned, force a reconnect.
  if (exp->in_flight)
    PschOtelCloseConn(exp);

  int idx = 0;
  while (idx < nevents) {
    PschPbBuf b;
    PschPbInit(&b, exp->encode_buf, exp->encode_cap);
    size_t rl_slot;
    size_t sl_slot;
    PschOtelEnvelopeBegin(&b, exp, false, &rl_slot, &sl_slot);

    int in_chunk = 0;
    while (idx < nevents) {
      if (b.len + kEventEncodeMargin > b.cap)
        break;  // chunk full: send and continue
      const size_t mark = b.len;
      PschOtelEncodeEvent(&b, &events[idx]);
      if (b.overflow) {
        // Margin backstop: roll the partial record back and flush the chunk.
        b.overflow = false;
        b.len = mark;
        break;
      }
      idx++;
      in_chunk++;
    }
    PschOtelEnvelopeEnd(&b, rl_slot, sl_slot);

    if (in_chunk == 0 || b.overflow) {
      PschOtelFail(exp, "OTLP export failed", "event record does not fit encode buffer");
      return PSCH_EXPORT_ERR_INTERNAL;
    }

    PschExportStatus st = PschOtelDoRequest(exp, exp->encode_buf, b.len, errbuf, sizeof errbuf);
    if (st != PSCH_EXPORT_OK) {
      // All-or-nothing: earlier chunks of this call may already be at the
      // collector, but the caller sees 0 exported and decides consume/requeue.
      PschOtelFail(exp, "OTLP export failed", errbuf);
      return st;
    }
  }

  if (exported_out != NULL)
    *exported_out = nevents;
  exp->consecutive_failures = 0;
  return PSCH_EXPORT_OK;
}

static PschExportStatus PschOtelOpSendArrow(PschExporter* self, const uint8* ipc_data,
                                            size_t ipc_len, int num_rows) {
  PschOtelExporter* exp = (PschOtelExporter*)self;
  char errbuf[512];

  if (exp->in_flight)
    PschOtelCloseConn(exp);

  if (ipc_data == NULL || ipc_len == 0 || num_rows <= 0) {
    PschOtelFail(exp, "OTLP Arrow export failed", "invalid Arrow IPC payload");
    return PSCH_EXPORT_ERR_INTERNAL;
  }

  PschPbBuf b;
  PschPbInit(&b, exp->encode_buf, exp->encode_cap);
  size_t rl_slot;
  size_t sl_slot;
  PschOtelEnvelopeBegin(&b, exp, true, &rl_slot, &sl_slot);
  PschOtelEncodeArrowRecord(&b, ipc_data, ipc_len, num_rows);
  PschOtelEnvelopeEnd(&b, rl_slot, sl_slot);
  if (b.overflow) {
    snprintf(errbuf, sizeof errbuf, "Arrow IPC payload (%zu bytes) exceeds encode buffer (%zu)",
             ipc_len, exp->encode_cap);
    PschOtelFail(exp, "OTLP Arrow export failed", errbuf);
    return PSCH_EXPORT_ERR_INTERNAL;
  }

  PschExportStatus st = PschOtelDoRequest(exp, exp->encode_buf, b.len, errbuf, sizeof errbuf);
  if (st != PSCH_EXPORT_OK) {
    PschOtelFail(exp, "OTLP Arrow export failed", errbuf);
    return st;
  }

  exp->consecutive_failures = 0;
  return PSCH_EXPORT_OK;
}

static int PschOtelOpConsecutiveFailures(const PschExporter* self) {
  return ((const PschOtelExporter*)self)->consecutive_failures;
}

static void PschOtelOpResetFailures(PschExporter* self) {
  ((PschOtelExporter*)self)->consecutive_failures = 0;
}

static uint64 PschOtelOpMemUsed(const PschExporter* self) {
  return ((const PschOtelExporter*)self)->mem_used;
}

static void PschOtelOpDestroy(PschExporter* self) {
  if (self == NULL)
    return;
  PschOtelExporter* exp = (PschOtelExporter*)self;
  PschOtelCloseConn(exp);
  if (exp->ssl_ctx != NULL) {
    SSL_CTX_free(exp->ssl_ctx);
    exp->ssl_ctx = NULL;
  }
  if (exp->head_prefix != NULL)
    pfree(exp->head_prefix);
  if (exp->encode_buf != NULL)
    pfree(exp->encode_buf);
  if (exp->net_buf != NULL)
    pfree(exp->net_buf);
  pfree(exp);
}

static const PschExporterOps kOtelExporterOps = {
    .connect = PschOtelOpConnect,
    .is_connected = PschOtelOpIsConnected,
    .export_events = PschOtelOpExportEvents,
    .send_arrow = PschOtelOpSendArrow,
    .consecutive_failures = PschOtelOpConsecutiveFailures,
    .reset_failures = PschOtelOpResetFailures,
    .mem_used = PschOtelOpMemUsed,
    .destroy = PschOtelOpDestroy,
};

// ---------------------------------------------------------------------------
// Creation (the only place allocation is allowed)
// ---------------------------------------------------------------------------

// URL semantics per include/config/guc.h: scheme decides TLS; bare host:port
// is http; missing path means /v1/logs.  Missing port: scheme default when a
// scheme was given (80/443), conventional OTLP/HTTP port 4318 otherwise.
static bool PschOtelParseEndpoint(const char* raw, PschOtelExporter* exp, char* errbuf,
                                  size_t errlen) {
  const char* p = (raw != NULL && raw[0] != '\0') ? raw : kDefaultEndpoint;
  const char* display = p;
  bool tls = false;
  bool have_scheme = false;

  if (pg_strncasecmp(p, "http://", 7) == 0) {
    p += 7;
    have_scheme = true;
  } else if (pg_strncasecmp(p, "https://", 8) == 0) {
    p += 8;
    tls = true;
    have_scheme = true;
  } else if (strstr(p, "://") != NULL) {
    snprintf(errbuf, errlen, "unsupported scheme in otel_endpoint \"%s\" (use http:// or https://)",
             display);
    return false;
  }

  const char* host_start;
  const char* host_end;
  if (*p == '[') {  // IPv6 literal
    host_start = p + 1;
    const char* close_br = strchr(host_start, ']');
    if (close_br == NULL) {
      snprintf(errbuf, errlen, "malformed IPv6 host in otel_endpoint \"%s\"", display);
      return false;
    }
    host_end = close_br;
    p = close_br + 1;
  } else {
    host_start = p;
    while (*p != '\0' && *p != ':' && *p != '/')
      p++;
    host_end = p;
  }
  if (host_end == host_start) {
    snprintf(errbuf, errlen, "missing host in otel_endpoint \"%s\"", display);
    return false;
  }
  size_t hlen = (size_t)(host_end - host_start);
  if (hlen >= sizeof(exp->host)) {
    snprintf(errbuf, errlen, "host too long in otel_endpoint \"%s\"", display);
    return false;
  }
  memcpy(exp->host, host_start, hlen);
  exp->host[hlen] = '\0';

  int port = -1;
  if (*p == ':') {
    p++;
    port = 0;
    bool any = false;
    while (*p >= '0' && *p <= '9' && port <= 65535) {
      port = port * 10 + (*p - '0');
      p++;
      any = true;
    }
    if (!any || port < 1 || port > 65535) {
      snprintf(errbuf, errlen, "invalid port in otel_endpoint \"%s\"", display);
      return false;
    }
  }
  if (*p != '\0' && *p != '/') {
    snprintf(errbuf, errlen, "malformed otel_endpoint \"%s\"", display);
    return false;
  }
  if (port < 0)
    port = have_scheme ? (tls ? 443 : 80) : 4318;

  if (*p == '\0' || strcmp(p, "/") == 0) {
    strlcpy(exp->path, kDefaultPath, sizeof(exp->path));
  } else {
    if (strlen(p) >= sizeof(exp->path)) {
      snprintf(errbuf, errlen, "path too long in otel_endpoint \"%s\"", display);
      return false;
    }
    strlcpy(exp->path, p, sizeof(exp->path));
  }

  exp->use_tls = tls;
  exp->port = port;
  return true;
}

static void PschOtelResolveHostname(char* out, size_t outlen) {
  if (psch_hostname != NULL && psch_hostname[0] != '\0') {
    strlcpy(out, psch_hostname, outlen);
    return;
  }
  const char* env = getenv("HOSTNAME");
  if (env != NULL && env[0] != '\0') {
    strlcpy(out, env, outlen);
    return;
  }
  if (gethostname(out, outlen) == 0) {
    out[outlen - 1] = '\0';
    if (out[0] != '\0')
      return;
  }
  strlcpy(out, "postgres-primary", outlen);
}

static bool PschHeaderNameReserved(const char* name, size_t len) {
  static const char* const kReserved[] = {"host",       "content-length",    "content-type",
                                          "connection", "transfer-encoding", "content-encoding"};
  for (size_t i = 0; i < lengthof(kReserved); i++) {
    if (strlen(kReserved[i]) == len && pg_strncasecmp(name, kReserved[i], len) == 0)
      return true;
  }
  return false;
}

// psch_otel_headers: newline-separated "Name: value" lines.  Returns a
// palloc'd "Name: value\r\n..." block ("" when none), NULL on alloc failure.
static char* PschOtelBuildHeaderBlock(const char* raw, char* errbuf, size_t errlen) {
  if (raw == NULL)
    raw = "";
  Size cap = strlen(raw) + 3;
  for (const char* q = raw; *q != '\0'; q++) {
    if (*q == '\n')
      cap += 2;
  }
  char* out = PschOtelAlloc(cap);
  if (out == NULL) {
    snprintf(errbuf, errlen, "out of memory parsing otel_headers");
    return NULL;
  }

  size_t olen = 0;
  const char* p = raw;
  while (*p != '\0') {
    const char* nl = strchr(p, '\n');
    const char* line = p;
    size_t llen = nl != NULL ? (size_t)(nl - p) : strlen(p);
    p = nl != NULL ? nl + 1 : p + llen;

    while (llen > 0 && (line[0] == ' ' || line[0] == '\t')) {
      line++;
      llen--;
    }
    while (llen > 0 && (line[llen - 1] == '\r' || line[llen - 1] == ' ' || line[llen - 1] == '\t'))
      llen--;
    if (llen == 0)
      continue;

    // Reject embedded control characters (CR, NUL, other C0 except TAB).  A
    // mid-line CR would otherwise smuggle a second header into the request
    // head (header injection); only the trailing CR was stripped above.
    bool ctl = false;
    for (size_t i = 0; i < llen; i++) {
      unsigned char c = (unsigned char)line[i];
      if (c < 0x20 && c != '\t') {
        ctl = true;
        break;
      }
    }
    if (ctl) {
      elog(WARNING, "pg_stat_ch: ignoring otel_headers entry containing a control character");
      continue;
    }

    const char* colon = memchr(line, ':', llen);
    if (colon == NULL || colon == line) {
      elog(WARNING, "pg_stat_ch: ignoring malformed otel_headers entry (expected \"Name: value\")");
      continue;
    }
    size_t nlen = (size_t)(colon - line);
    while (nlen > 0 && (line[nlen - 1] == ' ' || line[nlen - 1] == '\t'))
      nlen--;
    if (PschHeaderNameReserved(line, nlen)) {
      elog(WARNING, "pg_stat_ch: ignoring otel_headers entry overriding a reserved HTTP header");
      continue;
    }

    memcpy(out + olen, line, llen);
    olen += llen;
    out[olen++] = '\r';
    out[olen++] = '\n';
  }
  out[olen] = '\0';
  return out;
}

static bool PschOtelBuildHead(PschOtelExporter* exp, char* errbuf, size_t errlen) {
  char host_hdr[300];
  const bool v6 = strchr(exp->host, ':') != NULL;
  const bool default_port =
      (exp->use_tls && exp->port == 443) || (!exp->use_tls && exp->port == 80);
  if (v6 && default_port)
    snprintf(host_hdr, sizeof host_hdr, "[%s]", exp->host);
  else if (v6)
    snprintf(host_hdr, sizeof host_hdr, "[%s]:%d", exp->host, exp->port);
  else if (default_port)
    snprintf(host_hdr, sizeof host_hdr, "%s", exp->host);
  else
    snprintf(host_hdr, sizeof host_hdr, "%s:%d", exp->host, exp->port);

  char* hdr_block = PschOtelBuildHeaderBlock(psch_otel_headers, errbuf, errlen);
  if (hdr_block == NULL)
    return false;

  Size need =
      strlen(exp->path) + strlen(host_hdr) + strlen(hdr_block) + strlen(PG_STAT_CH_VERSION) + 128;
  exp->head_prefix = PschOtelAlloc(need);
  if (exp->head_prefix == NULL) {
    pfree(hdr_block);
    snprintf(errbuf, errlen, "out of memory building HTTP request head");
    return false;
  }
  int n = snprintf(exp->head_prefix, need,
                   "POST %s HTTP/1.1\r\n"
                   "Host: %s\r\n"
                   "User-Agent: pg_stat_ch/%s\r\n"
                   "Content-Type: application/x-protobuf\r\n"
                   "Connection: keep-alive\r\n"
                   "%s"
                   "Content-Length: ",
                   exp->path, host_hdr, PG_STAT_CH_VERSION, hdr_block);
  pfree(hdr_block);
  if (n < 0 || (Size)n >= need) {
    snprintf(errbuf, errlen, "HTTP request head too large");
    return false;
  }
  exp->head_prefix_len = (size_t)n;
  exp->head_alloc_bytes = need;
  return true;
}

static bool PschOtelInitSslCtx(PschOtelExporter* exp, char* errbuf, size_t errlen) {
  // OpenSSL 1.1.0+ auto-initializes on first use; no explicit library init.
  exp->ssl_ctx = SSL_CTX_new(TLS_client_method());
  if (exp->ssl_ctx == NULL) {
    PschOtelFormatSslError(errbuf, errlen, "SSL_CTX_new failed");
    return false;
  }
  SSL_CTX_set_verify(exp->ssl_ctx, SSL_VERIFY_PEER, NULL);
  SSL_CTX_set_mode(exp->ssl_ctx,
                   SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
  if (psch_otel_ca_file != NULL && psch_otel_ca_file[0] != '\0') {
    if (SSL_CTX_load_verify_locations(exp->ssl_ctx, psch_otel_ca_file, NULL) != 1) {
      snprintf(errbuf, errlen, "could not load CA bundle \"%s\"", psch_otel_ca_file);
      return false;
    }
  } else if (SSL_CTX_set_default_verify_paths(exp->ssl_ctx) != 1) {
    snprintf(errbuf, errlen, "could not load default CA certificates");
    return false;
  }
  return true;
}

PschExporter* PschOtelExporterCreate(char* errbuf, size_t errlen) {
  char local_err[256];
  if (errbuf == NULL || errlen == 0) {
    errbuf = local_err;
    errlen = sizeof local_err;
  }
  errbuf[0] = '\0';

  PschOtelExporter* exp = PschOtelAlloc(sizeof(PschOtelExporter));
  if (exp == NULL) {
    snprintf(errbuf, errlen, "out of memory allocating OTel exporter state");
    return NULL;
  }
  exp->base.ops = &kOtelExporterOps;
  exp->fd = -1;

  if (!PschOtelParseEndpoint(psch_otel_endpoint, exp, errbuf, errlen))
    goto fail;
  PschOtelResolveHostname(exp->hostname, sizeof(exp->hostname));
  if (exp->use_tls && !PschOtelInitSslCtx(exp, errbuf, errlen))
    goto fail;
  if (!PschOtelBuildHead(exp, errbuf, errlen))
    goto fail;

  {
    PschExportArenaPlan plan;
    PschExportArenaSplit(PschMemoryBudgetGet()->export_arena_bytes, &plan);
    exp->encode_buf = PschOtelAlloc(plan.encode_buf_bytes);
    exp->net_buf = PschOtelAlloc(plan.net_buf_bytes);
    if (exp->encode_buf == NULL || exp->net_buf == NULL) {
      snprintf(errbuf, errlen, "out of memory preallocating OTLP export buffers (%zu + %zu bytes)",
               plan.encode_buf_bytes, plan.net_buf_bytes);
      goto fail;
    }
    exp->encode_cap = plan.encode_buf_bytes;
    exp->net_cap = plan.net_buf_bytes;
  }

  if (exp->head_prefix_len + kContentLengthTailMax > exp->net_cap) {
    snprintf(errbuf, errlen, "otel_headers too large (%zu bytes) for network buffer (%zu bytes)",
             exp->head_prefix_len, exp->net_cap);
    goto fail;
  }

  exp->mem_used =
      sizeof(*exp) + exp->head_alloc_bytes + (uint64)exp->encode_cap + (uint64)exp->net_cap;
  return &exp->base;

fail:
  PschOtelOpDestroy(&exp->base);
  return NULL;
}
