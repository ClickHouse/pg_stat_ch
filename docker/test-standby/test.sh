#!/bin/bash
# End-to-end test for BgWorkerStart_ConsistentState change.
# Verifies the pg_stat_ch bgworker runs on both primary and standby.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PASS=0
FAIL=0

pass() { echo "PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL + 1)); }

cleanup() {
  echo "Cleaning up..."
  docker compose down -v 2>/dev/null || true
  docker rm -f psch-test-standby 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Building and starting primary ==="
docker compose up -d --build --wait

echo "=== Creating extension on primary ==="
docker exec psch-test-primary psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_stat_ch"

# Test 1: bgworker runs on primary
echo ""
echo "=== Test 1: bgworker on primary ==="
PRIMARY_BGW=$(docker exec psch-test-primary psql -U postgres -tA -c \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'pg_stat_ch exporter'")
if [ "$PRIMARY_BGW" = "1" ]; then
  pass "bgworker running on primary"
else
  fail "bgworker NOT running on primary (count=$PRIMARY_BGW)"
fi

# Test 2: primary is NOT in recovery
PRIMARY_RECOVERY=$(docker exec psch-test-primary psql -U postgres -tA -c \
  "SELECT pg_is_in_recovery()")
if [ "$PRIMARY_RECOVERY" = "f" ]; then
  pass "primary is not in recovery"
else
  fail "primary is unexpectedly in recovery"
fi

# Set up replication user and slot
echo ""
echo "=== Setting up replication ==="
docker exec psch-test-primary psql -U postgres -c \
  "SELECT pg_create_physical_replication_slot('standby_slot', true)" 2>/dev/null || true

# Create base backup for standby
echo "=== Creating base backup ==="
docker exec psch-test-primary bash -c \
  "pg_basebackup -D /tmp/standby_backup -U postgres -Fp -Xs -P -R \
   -S standby_slot"

# Copy backup out and create standby container
rm -rf /tmp/psch-standby-data
docker cp psch-test-primary:/tmp/standby_backup /tmp/psch-standby-data

# Start standby container from the backup
# PG 18 Docker image uses PGDATA=/var/lib/postgresql/18/docker
echo "=== Starting standby ==="
rm -rf /tmp/psch-standby-pgdir
mkdir -p /tmp/psch-standby-pgdir/18/docker
cp -a /tmp/psch-standby-data/* /tmp/psch-standby-pgdir/18/docker/

docker run -d --name psch-test-standby \
  --network "$(docker inspect psch-test-primary --format '{{range .NetworkSettings.Networks}}{{.NetworkID}}{{end}}')" \
  -e POSTGRES_PASSWORD=postgres \
  -v /tmp/psch-standby-pgdir:/var/lib/postgresql \
  "$(docker inspect psch-test-primary --format '{{.Config.Image}}')" \
  postgres \
  -c shared_preload_libraries=pg_stat_ch \
  -c pg_stat_ch.enabled=on \
  -c hot_standby=on

# Wait for standby to be ready
echo "Waiting for standby..."
for i in $(seq 1 30); do
  if docker exec psch-test-standby pg_isready -U postgres >/dev/null 2>&1; then
    echo "Standby ready after ${i}s"
    break
  fi
  sleep 1
done

# Test 3: standby IS in recovery
echo ""
echo "=== Test 3: standby is in recovery ==="
STANDBY_RECOVERY=$(docker exec psch-test-standby psql -U postgres -tA -c \
  "SELECT pg_is_in_recovery()")
if [ "$STANDBY_RECOVERY" = "t" ]; then
  pass "standby is in recovery mode"
else
  fail "standby is NOT in recovery (expected hot standby)"
fi

# Test 4: bgworker runs on standby (THE KEY TEST)
echo ""
echo "=== Test 4: bgworker on standby (key test) ==="
STANDBY_BGW=$(docker exec psch-test-standby psql -U postgres -tA -c \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'pg_stat_ch exporter'")
if [ "$STANDBY_BGW" = "1" ]; then
  pass "bgworker running on standby in recovery"
else
  fail "bgworker NOT running on standby (count=$STANDBY_BGW)"
fi

# Test 5: standby enqueues events from read queries
echo ""
echo "=== Test 5: standby enqueues events ==="
BEFORE=$(docker exec psch-test-standby psql -U postgres -tA -c \
  "SELECT enqueued_events FROM pg_stat_ch_stats()")
docker exec psch-test-standby psql -U postgres -c "SELECT 1; SELECT 2; SELECT 3;" >/dev/null
AFTER=$(docker exec psch-test-standby psql -U postgres -tA -c \
  "SELECT enqueued_events FROM pg_stat_ch_stats()")
if [ "$AFTER" -gt "$BEFORE" ]; then
  pass "standby enqueues events (before=$BEFORE, after=$AFTER)"
else
  fail "standby did NOT enqueue events (before=$BEFORE, after=$AFTER)"
fi

# Test 6: pg_stat_ch_version() works on standby
echo ""
echo "=== Test 6: version function on standby ==="
STANDBY_VERSION=$(docker exec psch-test-standby psql -U postgres -tA -c \
  "SELECT pg_stat_ch_version()")
if [ -n "$STANDBY_VERSION" ]; then
  pass "pg_stat_ch_version() works on standby: $STANDBY_VERSION"
else
  fail "pg_stat_ch_version() failed on standby"
fi

# Test 7: primary bgworker still running after standby joined
echo ""
echo "=== Test 7: primary still healthy ==="
PRIMARY_BGW_AFTER=$(docker exec psch-test-primary psql -U postgres -tA -c \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'pg_stat_ch exporter'")
if [ "$PRIMARY_BGW_AFTER" = "1" ]; then
  pass "primary bgworker still running"
else
  fail "primary bgworker stopped (count=$PRIMARY_BGW_AFTER)"
fi

echo ""
echo "==============================="
echo "Results: $PASS passed, $FAIL failed"
echo "==============================="
exit $FAIL
