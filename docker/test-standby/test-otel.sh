#!/bin/bash
# End-to-end OTel export test for BgWorkerStart_ConsistentState change.
# Verifies the pg_stat_ch bgworker can export telemetry via OTel from
# both primary and standby, with metrics reaching the Prometheus endpoint.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

PASS=0
FAIL=0
IMAGE_NAME="psch-test-otel"

pass() { echo "PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL + 1)); }

cleanup() {
  echo "Cleaning up..."
  docker rm -f psch-otel-primary psch-otel-standby 2>/dev/null || true
  docker compose -f "$PROJECT_DIR/docker/docker-compose.otel.yml" down -v 2>/dev/null || true
  rm -rf /tmp/psch-otel-standby-data /tmp/psch-otel-standby-pgdir
}
trap cleanup EXIT

# Helper: sum all _count values for the duration histogram from Prometheus
prometheus_duration_count() {
  curl -s http://localhost:9091/metrics \
    | awk '/^pg_stat_ch_db_client_operation_duration_seconds_count/ {sum+=$2} END{print int(sum+0)}'
}

# ============================================================================
# Phase 1: Start OTel collector
# ============================================================================
echo "=== Starting OTel collector ==="
docker compose -f "$PROJECT_DIR/docker/docker-compose.otel.yml" up -d

echo "Waiting for OTel collector health..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:13133/ >/dev/null 2>&1; then
    echo "OTel collector ready after ${i}s"
    break
  fi
  if [ "$i" = "30" ]; then
    echo "FATAL: OTel collector failed to start"
    exit 1
  fi
  sleep 1
done

# ============================================================================
# Phase 2: Build image and start primary
# ============================================================================
echo ""
echo "=== Building pg_stat_ch Docker image ==="
docker build -t "$IMAGE_NAME" -f "$PROJECT_DIR/docker/postgres-ext.Dockerfile" "$PROJECT_DIR"

echo ""
echo "=== Starting primary with OTel export ==="
docker run -d --name psch-otel-primary \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p 55432:5432 \
  "$IMAGE_NAME" \
  postgres \
  -c shared_preload_libraries=pg_stat_ch \
  -c pg_stat_ch.enabled=on \
  -c pg_stat_ch.use_otel=on \
  -c "pg_stat_ch.otel_endpoint=host.docker.internal:4317" \
  -c pg_stat_ch.flush_interval_ms=250 \
  -c wal_level=replica \
  -c max_wal_senders=10 \
  -c hot_standby=on

echo "Waiting for primary..."
for i in $(seq 1 30); do
  if docker exec psch-otel-primary pg_isready -U postgres >/dev/null 2>&1; then
    echo "Primary ready after ${i}s"
    break
  fi
  if [ "$i" = "30" ]; then
    echo "FATAL: Primary failed to start"
    exit 1
  fi
  sleep 1
done

echo "Creating extension on primary..."
docker exec psch-otel-primary psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_stat_ch"

# ============================================================================
# Phase 3: Primary baseline tests
# ============================================================================

# Test 1: OTel collector is healthy
echo ""
echo "=== Test 1: OTel collector healthy ==="
if curl -sf http://localhost:13133/ >/dev/null 2>&1; then
  pass "OTel collector is healthy"
else
  fail "OTel collector is NOT healthy"
fi

# Test 2: bgworker runs on primary
echo ""
echo "=== Test 2: bgworker on primary ==="
PRIMARY_BGW=$(docker exec psch-otel-primary psql -U postgres -tA -c \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'pg_stat_ch exporter'")
if [ "$PRIMARY_BGW" = "1" ]; then
  pass "bgworker running on primary"
else
  fail "bgworker NOT running on primary (count=$PRIMARY_BGW)"
fi

# Test 3: primary exports to OTel
echo ""
echo "=== Test 3: primary exports to OTel ==="
docker exec psch-otel-primary psql -U postgres -c "SELECT 1" -c "SELECT 2" -c "SELECT 3" >/dev/null
docker exec psch-otel-primary psql -U postgres -c "SELECT pg_stat_ch_flush()" >/dev/null
sleep 2
PRIMARY_EXPORTED=$(docker exec psch-otel-primary psql -U postgres -tA -c \
  "SELECT exported_events FROM pg_stat_ch_stats()")
if [ "$PRIMARY_EXPORTED" -gt 0 ]; then
  pass "primary exported $PRIMARY_EXPORTED events via OTel"
else
  fail "primary exported 0 events (expected > 0)"
fi

# Test 4: primary metrics in Prometheus (poll up to 10s for collector to flush)
echo ""
echo "=== Test 4: primary metrics in Prometheus ==="
PROM_COUNT=0
for i in $(seq 1 10); do
  PROM_COUNT=$(prometheus_duration_count)
  if [ "$PROM_COUNT" -gt 0 ]; then
    break
  fi
  sleep 1
done
if [ "$PROM_COUNT" -gt 0 ]; then
  pass "Prometheus has $PROM_COUNT duration observations from primary"
else
  fail "Prometheus has 0 observations after 10s (expected > 0)"
fi

# ============================================================================
# Phase 4: Set up replication and start standby
# ============================================================================
echo ""
echo "=== Setting up replication ==="
docker exec psch-otel-primary psql -U postgres -c \
  "SELECT pg_create_physical_replication_slot('standby_slot', true)" 2>/dev/null || true

echo "=== Creating base backup ==="
docker exec psch-otel-primary bash -c \
  "pg_basebackup -D /tmp/standby_backup -U postgres -Fp -Xs -P -R -S standby_slot"

rm -rf /tmp/psch-otel-standby-data /tmp/psch-otel-standby-pgdir
docker cp psch-otel-primary:/tmp/standby_backup /tmp/psch-otel-standby-data
mkdir -p /tmp/psch-otel-standby-pgdir/18/docker
cp -a /tmp/psch-otel-standby-data/* /tmp/psch-otel-standby-pgdir/18/docker/

echo "=== Starting standby with OTel export ==="
docker run -d --name psch-otel-standby \
  --network "$(docker inspect psch-otel-primary --format '{{range .NetworkSettings.Networks}}{{.NetworkID}}{{end}}')" \
  -e POSTGRES_PASSWORD=postgres \
  -v /tmp/psch-otel-standby-pgdir:/var/lib/postgresql \
  "$IMAGE_NAME" \
  postgres \
  -c shared_preload_libraries=pg_stat_ch \
  -c pg_stat_ch.enabled=on \
  -c pg_stat_ch.use_otel=on \
  -c "pg_stat_ch.otel_endpoint=host.docker.internal:4317" \
  -c pg_stat_ch.flush_interval_ms=250 \
  -c hot_standby=on

echo "Waiting for standby..."
for i in $(seq 1 30); do
  if docker exec psch-otel-standby pg_isready -U postgres >/dev/null 2>&1; then
    echo "Standby ready after ${i}s"
    break
  fi
  if [ "$i" = "30" ]; then
    echo "FATAL: Standby failed to start"
    exit 1
  fi
  sleep 1
done

# ============================================================================
# Phase 5: Standby OTel tests
# ============================================================================

# Test 5: standby is in recovery
echo ""
echo "=== Test 5: standby is in recovery ==="
STANDBY_RECOVERY=$(docker exec psch-otel-standby psql -U postgres -tA -c \
  "SELECT pg_is_in_recovery()")
if [ "$STANDBY_RECOVERY" = "t" ]; then
  pass "standby is in recovery mode"
else
  fail "standby is NOT in recovery (expected hot standby)"
fi

# Test 6: bgworker runs on standby
echo ""
echo "=== Test 6: bgworker on standby ==="
STANDBY_BGW=$(docker exec psch-otel-standby psql -U postgres -tA -c \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'pg_stat_ch exporter'")
if [ "$STANDBY_BGW" = "1" ]; then
  pass "bgworker running on standby in recovery"
else
  fail "bgworker NOT running on standby (count=$STANDBY_BGW)"
fi

# Test 7: standby exports to OTel
echo ""
echo "=== Test 7: standby exports to OTel ==="
STANDBY_EXPORTED_BEFORE=$(docker exec psch-otel-standby psql -U postgres -tA -c \
  "SELECT exported_events FROM pg_stat_ch_stats()")
docker exec psch-otel-standby psql -U postgres -c "SELECT 1" -c "SELECT 2" -c "SELECT 3" >/dev/null
docker exec psch-otel-standby psql -U postgres -c "SELECT pg_stat_ch_flush()" >/dev/null
sleep 2
STANDBY_EXPORTED_AFTER=$(docker exec psch-otel-standby psql -U postgres -tA -c \
  "SELECT exported_events FROM pg_stat_ch_stats()")
if [ "$STANDBY_EXPORTED_AFTER" -gt "$STANDBY_EXPORTED_BEFORE" ]; then
  pass "standby exported events via OTel (before=$STANDBY_EXPORTED_BEFORE, after=$STANDBY_EXPORTED_AFTER)"
else
  fail "standby did NOT export events (before=$STANDBY_EXPORTED_BEFORE, after=$STANDBY_EXPORTED_AFTER)"
fi

# Test 8: standby metrics reach Prometheus
echo ""
echo "=== Test 8: standby metrics in Prometheus ==="
# The Prometheus count should have increased since test 4 (which only had primary metrics)
PROM_COUNT_AFTER=$(prometheus_duration_count)
if [ "$PROM_COUNT_AFTER" -gt "$PROM_COUNT" ]; then
  pass "Prometheus count increased after standby export (before=$PROM_COUNT, after=$PROM_COUNT_AFTER)"
else
  fail "Prometheus count did NOT increase (before=$PROM_COUNT, after=$PROM_COUNT_AFTER)"
fi

# ============================================================================
# Phase 6: SQL function tests on standby
# ============================================================================

# Test 9: pg_stat_ch_flush() triggers immediate export
echo ""
echo "=== Test 9: flush() triggers immediate export on standby ==="
EXPORTED_PRE_FLUSH=$(docker exec psch-otel-standby psql -U postgres -tA -c \
  "SELECT exported_events FROM pg_stat_ch_stats()")
docker exec psch-otel-standby psql -U postgres -c "SELECT 42 AS test_flush" >/dev/null
docker exec psch-otel-standby psql -U postgres -c "SELECT pg_stat_ch_flush()" >/dev/null
sleep 2
EXPORTED_POST_FLUSH=$(docker exec psch-otel-standby psql -U postgres -tA -c \
  "SELECT exported_events FROM pg_stat_ch_stats()")
if [ "$EXPORTED_POST_FLUSH" -gt "$EXPORTED_PRE_FLUSH" ]; then
  pass "flush() triggered export on standby (before=$EXPORTED_PRE_FLUSH, after=$EXPORTED_POST_FLUSH)"
else
  fail "flush() did NOT trigger export (before=$EXPORTED_PRE_FLUSH, after=$EXPORTED_POST_FLUSH)"
fi

# Test 10: pg_stat_ch_reset() zeroes counters
# Note: after reset, the SELECT to read stats itself gets captured, so enqueued_events <= 2
echo ""
echo "=== Test 10: reset() zeroes counters on standby ==="
ENQUEUED_BEFORE_RESET=$(docker exec psch-otel-standby psql -U postgres -tA -c \
  "SELECT enqueued_events FROM pg_stat_ch_stats()")
docker exec psch-otel-standby psql -U postgres -c "SELECT pg_stat_ch_reset()" >/dev/null
ENQUEUED_AFTER_RESET=$(docker exec psch-otel-standby psql -U postgres -tA -c \
  "SELECT enqueued_events FROM pg_stat_ch_stats()")
if [ "$ENQUEUED_AFTER_RESET" -le 2 ] && [ "$ENQUEUED_AFTER_RESET" -lt "$ENQUEUED_BEFORE_RESET" ]; then
  pass "reset() zeroed counters on standby (before=$ENQUEUED_BEFORE_RESET, after=$ENQUEUED_AFTER_RESET)"
else
  fail "reset() did NOT zero counters (before=$ENQUEUED_BEFORE_RESET, after=$ENQUEUED_AFTER_RESET)"
fi

# ============================================================================
# Report
# ============================================================================
echo ""
echo "==============================="
echo "Results: $PASS passed, $FAIL failed"
echo "==============================="
exit $FAIL
