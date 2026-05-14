#!/usr/bin/env bash
# End-to-end local validation for parent_query_id linkage.
#
# Asserts (via real Postgres + real ClickHouse) that:
#   1. Top-level queries report parent_query_id = 0.
#   2. Nested SPI queries report parent_query_id = outer's query_id.
#   3. Error events captured inside a plpgsql EXCEPTION block carry both
#      a non-zero query_id (the running statement) and parent_query_id
#      equal to the outer caller's query_id, distinct from each other.
#
# Mirrors the TAP test in t/028_parent_query_id.pl but runs against the
# quickstart stack instead of a freshly-built TAP-enabled PG.  Useful for
# fast local verification without rebuilding pg_stat_ch.
#
# Prerequisite: ./scripts/quickstart.sh up (this script will run it if needed).

set -euo pipefail

COMPOSE_FILE="docker/quickstart/docker-compose.yml"
PASSED=0
FAILED=0

pg_exec() {
  docker compose -f "$COMPOSE_FILE" exec -T postgres \
    psql -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}

ch_query() {
  docker compose -f "$COMPOSE_FILE" exec -T clickhouse \
    clickhouse-client -q "$1" 2>/dev/null | tr -d '[:space:]'
}

ensure_stack_up() {
  if ! docker compose -f "$COMPOSE_FILE" ps --status running 2>/dev/null | grep -q postgres; then
    echo "Quickstart stack not running. Bringing it up (first run takes ~10-15 min)..."
    ./scripts/quickstart.sh up
  fi
}

expect() {
  local name="$1" expected="$2" actual="$3" comparator="${4:-=}"
  local ok=0
  case "$comparator" in
    '=')  [[ "$actual" == "$expected" ]] && ok=1 ;;
    '>=') [[ "$actual" -ge "$expected" ]] && ok=1 ;;
  esac
  if [[ "$ok" -eq 1 ]]; then
    printf '  \033[32mPASS\033[0m  %s\n' "$name"
    PASSED=$((PASSED + 1))
  else
    printf '  \033[31mFAIL\033[0m  %s (expected %s %s, got %s)\n' \
      "$name" "$comparator" "$expected" "$actual"
    FAILED=$((FAILED + 1))
  fi
}

ensure_stack_up

echo "Setting up fixtures..."
pg_exec <<'SQL' >/dev/null
CREATE EXTENSION IF NOT EXISTS pg_stat_ch;

DROP TABLE IF EXISTS pqid_top_marker CASCADE;
DROP TABLE IF EXISTS pqid_inner_marker CASCADE;
DROP TABLE IF EXISTS pqid_err_inner CASCADE;
DROP FUNCTION IF EXISTS pqid_outer_caller();
DROP FUNCTION IF EXISTS pqid_err_outer();

CREATE TABLE pqid_top_marker(x int);
CREATE TABLE pqid_inner_marker(x int);
INSERT INTO pqid_inner_marker VALUES (1);
CREATE TABLE pqid_err_inner(x int);
INSERT INTO pqid_err_inner VALUES (0);

CREATE FUNCTION pqid_outer_caller() RETURNS int
LANGUAGE plpgsql AS $$
DECLARE v int;
BEGIN
  SELECT x INTO v FROM pqid_inner_marker;
  RETURN v;
END$$;

CREATE FUNCTION pqid_err_outer() RETURNS int
LANGUAGE plpgsql AS $$
DECLARE v int;
BEGIN
  BEGIN
    SELECT 1 / x INTO v FROM pqid_err_inner;
  EXCEPTION WHEN division_by_zero THEN
    NULL;
  END;
  RETURN 1;
END$$;
SQL

echo "Clearing events_raw and pg_stat_ch stats..."
ch_query "TRUNCATE TABLE pg_stat_ch.events_raw" >/dev/null
pg_exec -c "SELECT pg_stat_ch_reset();" >/dev/null

echo "Driving test queries..."
pg_exec <<'SQL' >/dev/null
SELECT * FROM pqid_top_marker;
SELECT count(*) FROM pqid_top_marker;
SELECT pqid_outer_caller();
SELECT pqid_err_outer();
SELECT pg_stat_ch_flush();
SQL

echo "Waiting for events to land..."
for _ in $(seq 1 30); do
  count=$(ch_query "SELECT count() FROM pg_stat_ch.events_raw")
  if [[ "${count:-0}" -ge 5 ]]; then break; fi
  sleep 1
done

echo
echo "Test 1: top-level queries report parent_query_id = 0"
expect "top-level rows have parent_query_id = 0" 0 \
  "$(ch_query "SELECT count() FROM pg_stat_ch.events_raw WHERE query LIKE '%pqid_top_marker%' AND parent_query_id != 0")"

echo
echo "Test 2: nested SPI parent_query_id links to outer query_id"
expect "inner row joins to outer via parent_query_id" 1 \
  "$(ch_query "
    SELECT count() FROM pg_stat_ch.events_raw inner_q
    JOIN pg_stat_ch.events_raw outer_q
      ON inner_q.parent_query_id = outer_q.query_id
    WHERE inner_q.query LIKE '%pqid_inner_marker%'
      AND outer_q.query LIKE '%pqid_outer_caller%'
  ")" '>='
expect "nested SPI query is not reported as top-level" 0 \
  "$(ch_query "SELECT count() FROM pg_stat_ch.events_raw WHERE query LIKE '%pqid_inner_marker%' AND parent_query_id = 0")"
expect "outer call still reports parent_query_id = 0" 0 \
  "$(ch_query "SELECT count() FROM pg_stat_ch.events_raw WHERE query LIKE '%pqid_outer_caller%' AND parent_query_id != 0")"

echo
echo "Test 3: error inside nested SPI"
expect "div-by-zero log event landed" 1 \
  "$(ch_query "SELECT count() FROM pg_stat_ch.events_raw WHERE cmd_type = 'UNKNOWN' AND err_sqlstate = '22012'")" '>='
expect "log event parent_query_id = outer caller's query_id" 1 \
  "$(ch_query "
    SELECT count() FROM pg_stat_ch.events_raw err_q
    JOIN pg_stat_ch.events_raw outer_q
      ON err_q.parent_query_id = outer_q.query_id
    WHERE err_q.cmd_type = 'UNKNOWN'
      AND err_q.err_sqlstate = '22012'
      AND outer_q.query LIKE '%pqid_err_outer%'
  ")" '>='
expect "log event query_id is the running statement (non-zero)" 1 \
  "$(ch_query "SELECT count() FROM pg_stat_ch.events_raw WHERE cmd_type = 'UNKNOWN' AND err_sqlstate = '22012' AND query_id != 0")" '>='
expect "log event query_id != parent_query_id (no self-parent)" 0 \
  "$(ch_query "SELECT count() FROM pg_stat_ch.events_raw WHERE cmd_type = 'UNKNOWN' AND err_sqlstate = '22012' AND query_id = parent_query_id AND query_id != 0")"

echo
echo "Cleaning up fixtures..."
pg_exec <<'SQL' >/dev/null
DROP FUNCTION IF EXISTS pqid_outer_caller();
DROP FUNCTION IF EXISTS pqid_err_outer();
DROP TABLE IF EXISTS pqid_top_marker;
DROP TABLE IF EXISTS pqid_inner_marker;
DROP TABLE IF EXISTS pqid_err_inner;
SQL

echo
echo "==================================================="
printf "Result: \033[32m%d passed\033[0m, \033[31m%d failed\033[0m\n" "$PASSED" "$FAILED"
echo "==================================================="
[[ "$FAILED" -eq 0 ]]
