// Standalone harness for the Arrow IPC batch builder (no postgres server).
//
// Compiles src/export/arrow_batch.c + arrow_ipc_emit.c against the real
// PostgreSQL server headers and stubs the few elog symbols the export path
// references (errstart returning false suppresses message evaluation).
// Generates deterministic synthetic events — the SAME recipe is mirrored in
// arrow_batch_validate.py, which decodes the payloads with pyarrow and
// asserts schema and value equality.
#include "postgres.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "export/arrow_batch.h"
#include "queue/event.h"

// --- PG snprintf replacements (port.h redirects to pg_*; back them with libc) ---
#undef snprintf
#undef vsnprintf
#undef fprintf
#undef vfprintf
#undef printf
#undef vprintf

int pg_snprintf(char* str, size_t count, const char* fmt, ...) {
  va_list args;
  int n;

  va_start(args, fmt);
  n = vsnprintf(str, count, fmt, args);
  va_end(args);
  return n;
}

int pg_fprintf(FILE* stream, const char* fmt, ...) {
  va_list args;
  int n;

  va_start(args, fmt);
  n = vfprintf(stream, fmt, args);
  va_end(args);
  return n;
}

int pg_printf(const char* fmt, ...) {
  va_list args;
  int n;

  va_start(args, fmt);
  n = vprintf(fmt, args);
  va_end(args);
  return n;
}

// --- PG error-API stubs (errstart=false => message args never evaluated) ---
static int warn_count = 0;

bool errstart(int elevel, const char* domain) {
  (void)domain;
  if (elevel == WARNING)
    warn_count++;
  return false;
}

bool errstart_cold(int elevel, const char* domain) {
  return errstart(elevel, domain);
}

void errfinish(const char* filename, int lineno, const char* funcname) {
  (void)filename;
  (void)lineno;
  (void)funcname;
}

int errmsg(const char* fmt, ...) {
  (void)fmt;
  return 0;
}

int errmsg_internal(const char* fmt, ...) {
  (void)fmt;
  return 0;
}

int errcode(int sqlerrcode) {
  (void)sqlerrcode;
  return 0;
}

// --- synthetic event recipe (mirrored in arrow_batch_validate.py) ---
static void FillEvent(PschEvent* ev, int i) {
  memset(ev, 0, sizeof(*ev));
  ev->ts_start = 1000000 + (int64)i * 1000;
  ev->queryid = i == 3 ? UINT64_MAX : (uint64)(i % 7) * UINT64CONST(1234567890123);
  ev->pid = i == 5 ? 2147483647 : 100000 + i;

  if (i == 19) {
    memset(ev->datname, 'x', sizeof(ev->datname));
    ev->datname_len = 70;  // invalid (> 63): exercises the clamp + WARNING path
  } else {
    ev->datname_len = (uint8)snprintf(ev->datname, sizeof(ev->datname), "db_%d", i % 3);
  }
  ev->username_len = (uint8)snprintf(ev->username, sizeof(ev->username), "user_%d", i % 2);

  if (i == 7) {
    memset(ev->application_name, 'a', 63);
    ev->application_name_len = 63;
  } else {
    ev->application_name_len =
        (uint8)snprintf(ev->application_name, sizeof(ev->application_name), "app");
  }

  if (i % 4 == 0) {
    ev->client_addr_len = 0;
  } else if (i == 9) {
    memset(ev->client_addr, 'c', 45);
    ev->client_addr_len = 45;
  } else {
    ev->client_addr_len = (uint8)snprintf(ev->client_addr, sizeof(ev->client_addr), "10.0.0.%d", i);
  }

  if (i == 11) {
    memset(ev->query, 'q', PSCH_MAX_QUERY_LEN);
    ev->query_len = PSCH_MAX_QUERY_LEN;
  } else {
    ev->query_len = (uint16)snprintf(ev->query, sizeof(ev->query), "SELECT %d", i);
  }

  if (i == 13) {
    memset(ev->err_message, 'e', PSCH_MAX_ERR_MSG_LEN);
    ev->err_message_len = PSCH_MAX_ERR_MSG_LEN;
  } else if (i % 5 == 0) {
    ev->err_message_len = 0;
  } else {
    ev->err_message_len = (uint16)snprintf(ev->err_message, sizeof(ev->err_message), "error %d", i);
  }

  if (i % 6 != 0)
    memcpy(ev->err_sqlstate, "42P01", 6);
  ev->err_elevel = i % 3 == 0 ? 0 : 21;
  ev->cmd_type = (PschCmdType)(i % 8);

  ev->duration_us = (uint64)i * 10 + 1;
  ev->rows = (uint64)i * 100;
  ev->wal_bytes = (uint64)i * 67;

  ev->shared_blks_hit = i == 17 ? -5 : (int64)i * 2;
  ev->shared_blks_read = (int64)i * 3;
  ev->shared_blks_written = (int64)i * 7;
  ev->shared_blks_dirtied = (int64)i * 5;
  ev->shared_blk_read_time_us = (int64)i * 31;
  ev->shared_blk_write_time_us = (int64)i * 37;
  ev->local_blks_hit = (int64)i * 11;
  ev->local_blks_read = (int64)i * 13;
  ev->local_blks_written = (int64)i * 19;
  ev->local_blks_dirtied = (int64)i * 17;
  ev->local_blk_read_time_us = (int64)i * 41;
  ev->local_blk_write_time_us = (int64)i * 43;
  ev->temp_blks_read = (int64)i * 23;
  ev->temp_blks_written = (int64)i * 29;
  ev->temp_blk_read_time_us = (int64)i * 47;
  ev->temp_blk_write_time_us = (int64)i * 53;
  ev->wal_records = (int64)i * 59;
  ev->wal_fpi = (int64)i * 61;
  ev->cpu_user_time_us = (int64)i * 71;
  ev->cpu_sys_time_us = (int64)i * 73;

  ev->jit_functions = i == 17 ? -1 : i % 50;
  ev->jit_generation_time_us = i * 3 + 1;
  ev->jit_deform_time_us = i * 5 + 2;
  ev->jit_inlining_time_us = i * 7 + 3;
  ev->jit_optimization_time_us = i * 11 + 4;
  ev->jit_emission_time_us = i * 13 + 5;

  ev->parallel_workers_planned = (int16)(i == 17 ? -2 : i % 10);
  ev->parallel_workers_launched = (int16)((i + 1) % 10);
}

#define ATTRS1                                                         \
  "instance_ubid: inst-1 ;server_ubid:srv-9; server_role : primary; "  \
  "read_replica_type:; region:us-east-1;cell:cell-7;host_id:host-abc;" \
  "pod_name:pod-xyz;region:OTHER; bogus; unknown_key:zzz"
#define ATTRS2                                                                     \
  "instance_ubid:inst-1;server_ubid:srv-9;server_role:primary;read_replica_type:;" \
  "region:eu-west-1;cell:cell-7;host_id:host-abc;pod_name:pod-xyz"

#define CHECK(cond)                                                   \
  do {                                                                \
    if (!(cond)) {                                                    \
      fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
      exit(1);                                                        \
    }                                                                 \
  } while (0)

static void WritePayload(const char* dir, const char* name, const uint8* data, size_t len) {
  char path[1024];
  FILE* f;

  snprintf(path, sizeof(path), "%s/%s", dir, name);
  f = fopen(path, "wb");
  CHECK(f != NULL);
  CHECK(fwrite(data, 1, len, f) == len);
  fclose(f);
}

int main(int argc, char** argv) {
  char errbuf[256] = "";
  PschEvent* ev = malloc(sizeof(PschEvent));
  const uint8* data;
  size_t len;
  int rows;

  CHECK(argc == 2);
  CHECK(ev != NULL);

  // --- main payload: 50 events covering the edge cases ---
  {
    PschArrowBuilderConfig cfg = {
        .scratch_budget_bytes = 64 * 1024 * 1024,
        .ipc_budget_bytes = 4 * 1024 * 1024,
        .max_rows = 1024,
        .extra_attributes = ATTRS1,
        .service_version = "0.4.0-test",
    };
    PschArrowBuilder* b = PschArrowBuilderCreate(&cfg, errbuf, sizeof(errbuf));

    if (b == NULL) {
      fprintf(stderr, "create failed: %s\n", errbuf);
      return 1;
    }
    CHECK(PschArrowBuilderMemUsed(b) > 0);
    CHECK(PschArrowBuilderNumRows(b) == 0);
    CHECK(PschArrowBuilderEstimatedBytes(b) == 1024);

    for (int i = 0; i < 50; i++) {
      size_t before = PschArrowBuilderEstimatedBytes(b);

      FillEvent(ev, i);
      CHECK(PschArrowBuilderAppend(b, ev) == PSCH_ARROW_APPEND_OK);
      CHECK(PschArrowBuilderEstimatedBytes(b) > before);
    }
    CHECK(PschArrowBuilderNumRows(b) == 50);
    CHECK(warn_count >= 2);  // negative clamp (i=17) + invalid datname_len (i=19)

    CHECK(PschArrowBuilderFinish(b, &data, &len, &rows));
    CHECK(rows == 50);
    CHECK(len > 16);
    CHECK(memcmp(data + len - 8, "\xff\xff\xff\xff\x00\x00\x00\x00", 8) == 0);
    WritePayload(argv[1], "payload1.bin", data, len);

    // Finish is repeatable until the state changes.
    {
      size_t len2;

      CHECK(PschArrowBuilderFinish(b, &data, &len2, &rows));
      CHECK(len2 == len);
    }

    // --- second payload after Reset + attribute reload: fresh dictionaries ---
    PschArrowBuilderReset(b);
    CHECK(PschArrowBuilderNumRows(b) == 0);
    CHECK(!PschArrowBuilderFinish(b, &data, &len, &rows));  // nothing to serialize
    CHECK(PschArrowBuilderSetAttributes(b, ATTRS2));
    for (int i = 100; i < 105; i++) {
      FillEvent(ev, i);
      CHECK(PschArrowBuilderAppend(b, ev) == PSCH_ARROW_APPEND_OK);
    }
    CHECK(PschArrowBuilderFinish(b, &data, &len, &rows));
    CHECK(rows == 5);
    WritePayload(argv[1], "payload2.bin", data, len);

    PschArrowBuilderDestroy(b);
  }

  // --- max_rows bound: FULL, then Finish+Reset+retry ---
  {
    PschArrowBuilderConfig cfg = {.max_rows = 4, .service_version = "t"};
    PschArrowBuilder* b = PschArrowBuilderCreate(&cfg, errbuf, sizeof(errbuf));

    CHECK(b != NULL);
    for (int i = 0; i < 4; i++) {
      FillEvent(ev, i);
      CHECK(PschArrowBuilderAppend(b, ev) == PSCH_ARROW_APPEND_OK);
    }
    FillEvent(ev, 4);
    CHECK(PschArrowBuilderAppend(b, ev) == PSCH_ARROW_APPEND_FULL);
    CHECK(PschArrowBuilderFinish(b, &data, &len, &rows));
    CHECK(rows == 4);
    PschArrowBuilderReset(b);
    CHECK(PschArrowBuilderAppend(b, ev) == PSCH_ARROW_APPEND_OK);
    PschArrowBuilderDestroy(b);
  }

  // --- ipc-budget bound: estimate-driven FULL before 50 rows ---
  {
    PschArrowBuilderConfig cfg = {.ipc_budget_bytes = 65536, .service_version = "t"};
    PschArrowBuilder* b = PschArrowBuilderCreate(&cfg, errbuf, sizeof(errbuf));
    int appended = 0;

    CHECK(b != NULL);
    for (int i = 0; i < 50; i++) {
      PschArrowAppendResult r;

      FillEvent(ev, 11);  // max-size query every row
      r = PschArrowBuilderAppend(b, ev);
      if (r == PSCH_ARROW_APPEND_FULL)
        break;
      CHECK(r == PSCH_ARROW_APPEND_OK);
      appended++;
    }
    CHECK(appended >= 2 && appended < 50);
    CHECK(PschArrowBuilderFinish(b, &data, &len, &rows));
    CHECK(rows == appended);
    PschArrowBuilderDestroy(b);
  }

  // --- NULL safety ---
  PschArrowBuilderDestroy(NULL);
  CHECK(PschArrowBuilderNumRows(NULL) == 0);
  CHECK(PschArrowBuilderEstimatedBytes(NULL) == 0);
  CHECK(PschArrowBuilderMemUsed(NULL) == 0);
  CHECK(PschArrowBuilderAppend(NULL, ev) == PSCH_ARROW_APPEND_ERR);
  CHECK(!PschArrowBuilderSetAttributes(NULL, ""));

  free(ev);
  printf("harness OK\n");
  return 0;
}
