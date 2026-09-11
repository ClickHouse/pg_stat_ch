// Drives exporter library through its C interface without PostgreSQL.
// Linking proves library resolves no PostgreSQL symbols, running proves
// failures surface as results instead of escaping exceptions

#include <stdio.h>
#include <string.h>

#include "export/psch_exporter.h"

static int failures = 0;

extern int CheckExporterOutOfMemory(void);

static void Check(const char* what, int ok, const PschExporterResult* result) {
  if (ok)
    return;
  failures++;
  fprintf(stderr, "FAIL: %s", what);
  if (result != NULL)
    fprintf(stderr, " (status %d: %s)", (int)result->status, result->error);
  fprintf(stderr, "\n");
}

// Rejection must fill result, not only return a status
static void CheckRejected(const char* what, PschExporterStatus status,
                          const PschExporterResult* result) {
  Check(what,
        status == PSCH_EXPORTER_INVALID_ARGUMENT && result->status == status &&
            result->error[0] != '\0' && result->exported == 0,
        result);
}

static void CheckMode(PschExporterMode mode) {
  // Closed loopback ports fail fast: ECONNREFUSED for ClickHouse, gRPC
  // deadline of otel_log_delay_ms for OTLP
  PschExporterConfig config = {
      .mode = mode,
      .service_version = "check",
      .clickhouse_host = "127.0.0.1",
      .clickhouse_port = 1,
      .otel_endpoint = "127.0.0.1:1",
      .otel_log_batch_size = 16,
      .otel_log_max_bytes = 65536,
      .otel_log_delay_ms = 100,
      .arrow_max_block_bytes = 65536,
      .extra_attributes = "region:check",
  };
  PschExporter* exporter = NULL;
  PschExporterResult result;

  Check("create",
        PschExporterCreate(&config, &exporter, &result) == PSCH_EXPORTER_OK && exporter != NULL,
        &result);
  if (exporter == NULL)
    return;

  Check("export of zero events succeeds",
        PschExporterExport(exporter, NULL, 0, &result) == PSCH_EXPORTER_OK, &result);

  PschExportEvent bad_length = {.query_text = {"", PSCH_EXPORT_QUERY_MAX + 1}};
  CheckRejected("length contract rejected", PschExporterExport(exporter, &bad_length, 1, &result),
                &result);

  PschExporterConfig other = config;
  other.mode = mode == PSCH_EXPORTER_CLICKHOUSE ? PSCH_EXPORTER_OTEL : PSCH_EXPORTER_CLICKHOUSE;
  CheckRejected("mode change rejected", PschExporterConfigure(exporter, &other, &result), &result);
  Check("reconfigure", PschExporterConfigure(exporter, &config, &result) == PSCH_EXPORTER_OK,
        &result);

  PschExporterStatus connect = PschExporterConnect(exporter, &result);
  if (mode == PSCH_EXPORTER_CLICKHOUSE) {
    Check("refused connection reported",
          connect == PSCH_EXPORTER_FAILED && result.error[0] != '\0' && !result.connected, &result);
  } else {
    Check("gRPC channel created", connect == PSCH_EXPORTER_OK && result.connected, &result);

    PschExportEvent event = {
        .ts_unix_us = 1700000000000000LL,
        .db_name = {"postgres", 8},
        .db_operation = {"SELECT", 6},
        .query_text = {"SELECT 1", 8},
        .err_sqlstate = {"", 0},
    };
    PschExporterStatus export_status = PschExporterExport(exporter, &event, 1, &result);
    Check("unreachable collector reported",
          export_status == PSCH_EXPORTER_FAILED && result.error[0] != '\0' && result.exported == 0,
          &result);
  }

  PschExporterDestroy(exporter);
}

int main(void) {
  failures += CheckExporterOutOfMemory();
  const PschExporterMode modes[] = {PSCH_EXPORTER_CLICKHOUSE, PSCH_EXPORTER_OTEL,
                                    PSCH_EXPORTER_OTEL_ARROW_LEGACY,
                                    PSCH_EXPORTER_OTEL_ARROW_UNIFIED};
  for (size_t i = 0; i < sizeof modes / sizeof modes[0]; i++)
    CheckMode(modes[i]);

  // Stale values would survive a rejection that skips result initialization
  const PschExporterResult stale = {.status = PSCH_EXPORTER_OK, .exported = 7};
  PschExporterResult result = stale;
  CheckRejected("NULL exporter rejected by connect", PschExporterConnect(NULL, &result), &result);
  result = stale;
  CheckRejected("NULL exporter rejected by configure", PschExporterConfigure(NULL, NULL, &result),
                &result);
  result = stale;
  CheckRejected("NULL exporter rejected by export", PschExporterExport(NULL, NULL, 0, &result),
                &result);
  Check("NULL result rejected", PschExporterConnect(NULL, NULL) == PSCH_EXPORTER_INVALID_ARGUMENT,
        NULL);
  PschExporterDestroy(NULL);

  if (failures == 0)
    printf("exporter check passed\n");
  return failures == 0 ? 0 : 1;
}
