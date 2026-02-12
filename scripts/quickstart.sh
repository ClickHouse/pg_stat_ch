#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker/quickstart/docker-compose.yml"

usage() {
  cat <<USAGE
Usage: $0 <up|check|down|pg|ch> [args...]

  up     Build and start local quickstart stack (PostgreSQL + ClickHouse)
  check  Run end-to-end check (create extension, run query, flush, verify CH row count)
  down   Stop and remove quickstart stack volumes
  pg     Open psql in the quickstart PostgreSQL container
  ch     Open clickhouse-client in the quickstart ClickHouse container
USAGE
}

cmd_up() {
  docker compose -f "$COMPOSE_FILE" up -d --build --wait
  echo "Quickstart stack is up."
  echo "PostgreSQL: localhost:55432"
  echo "ClickHouse HTTP: localhost:28123"
  echo "ClickHouse Native: localhost:29000"
}

cmd_check() {
  marker="pg_stat_ch_check_$(date +%s)_${RANDOM}${RANDOM}"

  echo "Creating extension (idempotent)..."
  docker compose -f "$COMPOSE_FILE" exec -T postgres \
    psql -U postgres -d postgres -v ON_ERROR_STOP=1 \
    -c "CREATE EXTENSION IF NOT EXISTS pg_stat_ch;"

  echo "Generating sample queries and flushing..."
  docker compose -f "$COMPOSE_FILE" exec -T postgres \
    psql -U postgres -d postgres -v ON_ERROR_STOP=1 <<SQL
CREATE TABLE IF NOT EXISTS quickstart_demo (id int);
INSERT INTO quickstart_demo(id) VALUES (1), (2), (3);
SELECT count(*) FROM quickstart_demo;
SELECT 42;
SELECT '${marker}'::text;
SELECT pg_stat_ch_flush();
SQL

  echo "Checking events in ClickHouse (marker: ${marker})..."
  found=0
  deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if count="$(cmd_ch -q "SELECT count() FROM events_raw WHERE ts_start > now() - INTERVAL 10 MINUTE AND query_id != 0 AND position(query, '${marker}') > 0" 2>/dev/null | tr -d '[:space:]')" \
      && [[ "$count" =~ ^[0-9]+$ ]] && [[ "$count" -gt 0 ]]; then
      found=1
      break
    fi
    sleep 1
  done

  if [[ "$found" -eq 1 ]]; then
    echo "Check passed."
    echo "marker rows (last 10m): $count"
    echo "Latest 10 events (query_id != 0):"
    cmd_ch -q \
      "SELECT ts_start, query_id, cmd_type, duration_us, query FROM events_raw WHERE query_id != 0 ORDER BY ts_start DESC LIMIT 10 FORMAT PrettyCompactMonoBlock"
    echo
    echo "Explore further:"
    echo "  ./scripts/quickstart.sh pg"
    echo "  ./scripts/quickstart.sh ch"
  else
    echo "Check failed: marker query not found in ClickHouse within 30s" >&2
    exit 1
  fi
}

cmd_down() {
  docker compose -f "$COMPOSE_FILE" down -v
  echo "Quickstart stack stopped."
}

cmd_pg() {
  docker compose -f "$COMPOSE_FILE" exec postgres psql -U postgres -d postgres "$@"
}

cmd_ch() {
  docker compose -f "$COMPOSE_FILE" exec clickhouse clickhouse-client -d pg_stat_ch "$@"
}

main() {
  if [[ $# -lt 1 ]]; then
    usage
    exit 1
  fi

  cmd="$1"
  shift

  case "$cmd" in
    up) cmd_up ;;
    check) cmd_check ;;
    down) cmd_down ;;
    pg) cmd_pg "$@" ;;
    ch) cmd_ch "$@" ;;
    *) usage; exit 1 ;;
  esac
}

main "$@"
