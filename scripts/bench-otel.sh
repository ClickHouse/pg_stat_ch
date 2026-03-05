#!/usr/bin/env bash
# bench-otel.sh — Benchmark OTel export throughput for pg_stat_ch
#
# Measures how fast the bgworker drains the shared-memory queue when
# exporting via OTel gRPC, and how many events are lost at sustained load.
#
# Usage: scripts/bench-otel.sh [--tps 40000] [--duration 30] [--clients 16]
#
# Prerequisites:
#   - OTel collector running (docker compose -f docker/docker-compose.otel.yml up -d)
#   - PostgreSQL running with pg_stat_ch loaded and use_otel=on
#
# Output files:
#   bench-otel-perf.data   - raw perf data
#   bench-otel-perf.txt    - perf report (top functions)
#   bench-otel-flame.svg   - flamegraph (if flamegraph.pl available)

set -euo pipefail

# --- Defaults ---
TPS=40000
DURATION=30
CLIENTS=16
JOBS=4
OTEL_HEALTH="http://localhost:13133"
OTEL_ENDPOINT="localhost:4317"
PGPORT="${PGPORT:-5432}"
DRAIN_TIMEOUT=30
SAMPLE_INTERVAL=1
PERF_DURATION=10

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tps)       TPS="$2"; shift 2 ;;
    --duration)  DURATION="$2"; shift 2 ;;
    --clients)   CLIENTS="$2"; shift 2 ;;
    --jobs)      JOBS="$2"; shift 2 ;;
    --port)      PGPORT="$2"; shift 2 ;;
    --help|-h)
      sed -n '2,/^$/s/^# //p' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

export PGPORT

# --- Helpers ---
bold()  { printf '\033[1m%s\033[0m' "$*"; }
red()   { printf '\033[31m%s\033[0m' "$*"; }
green() { printf '\033[32m%s\033[0m' "$*"; }
cyan()  { printf '\033[36m%s\033[0m' "$*"; }

die() { red "ERROR: $*"; echo; exit 1; }

comma() {
  # Format number with commas: 1234567 -> 1,234,567
  printf "%'d" "$1" 2>/dev/null || printf "%d" "$1"
}

psql_val() {
  # Run a psql query and return the trimmed single value
  psql -XAtqc "$1" 2>/dev/null
}

get_stats() {
  # Returns: enqueued|dropped|exported|send_failures|queue_size|queue_capacity|queue_usage_pct
  psql -XAtqc "SELECT enqueued_events, dropped_events, exported_events, \
    send_failures, queue_size, queue_capacity, queue_usage_pct \
    FROM pg_stat_ch_stats()" 2>/dev/null
}

# --- Prerequisites ---
echo
bold "=== pg_stat_ch OTel Benchmark ==="; echo
echo "Backend: OTel gRPC ($OTEL_ENDPOINT)"
echo "Duration: ${DURATION}s | Target TPS: $(comma "$TPS") | Clients: $CLIENTS"
echo

echo -n "Checking OTel collector health... "
if curl -sf "$OTEL_HEALTH" >/dev/null 2>&1; then
  green "OK"; echo
else
  die "OTel collector not reachable at $OTEL_HEALTH. Start it with: docker compose -f docker/docker-compose.otel.yml up -d"
fi

echo -n "Checking PostgreSQL connection... "
if psql -XAtqc "SELECT 1" >/dev/null 2>&1; then
  green "OK"; echo
else
  die "Cannot connect to PostgreSQL. Is it running?"
fi

echo -n "Checking pg_stat_ch is loaded... "
if psql_val "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_stat_ch'" | grep -q 1; then
  green "OK"; echo
else
  die "pg_stat_ch extension not available"
fi

echo -n "Checking use_otel is enabled... "
OTEL_ON=$(psql_val "SHOW pg_stat_ch.use_otel" 2>/dev/null || echo "off")
if [[ "$OTEL_ON" == "on" ]]; then
  green "OK"; echo
else
  die "pg_stat_ch.use_otel is '$OTEL_ON', expected 'on'"
fi

# --- Configure for throughput ---
echo
bold "Configuring GUCs for throughput..."; echo
psql -XAtqc "ALTER SYSTEM SET pg_stat_ch.flush_interval_ms = 100"
psql -XAtqc "ALTER SYSTEM SET pg_stat_ch.batch_max = 200000"
psql -XAtqc "SELECT pg_reload_conf()"
sleep 1  # let reload take effect
echo "  flush_interval_ms = 100"
echo "  batch_max = 200000"

# --- Reset stats ---
echo
echo -n "Resetting stats... "
psql -XAtqc "SELECT pg_stat_ch_reset()" >/dev/null
green "OK"; echo

# --- Find bgworker PID ---
BGWORKER_PID=$(psql_val "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_stat_ch exporter' LIMIT 1" || true)
if [[ -n "$BGWORKER_PID" ]]; then
  echo "Bgworker PID: $BGWORKER_PID"
else
  echo "Warning: Could not find bgworker PID (perf profiling will be skipped)"
fi

# --- Prepare pgbench ---
BENCH_SCRIPT=$(mktemp /tmp/bench-otel-XXXXXX.sql)
echo "SELECT 1;" > "$BENCH_SCRIPT"
trap 'rm -f "$BENCH_SCRIPT" /tmp/bench-otel-sampler.pid' EXIT

# Initialize pgbench (creates required tables)
pgbench -i -q 2>/dev/null || true

# --- Start live sampler in background ---
echo
bold "--- Live Stats (sampled every ${SAMPLE_INTERVAL}s) ---"; echo
printf "  %6s | %12s | %12s | %10s | %8s | %6s | %12s\n" \
  "time" "enqueued" "exported" "dropped" "queue_sz" "usage%" "drain_rate"
printf "  %6s-+-%12s-+-%12s-+-%10s-+-%8s-+-%6s-+-%12s\n" \
  "------" "------------" "------------" "----------" "--------" "------" "------------"

# Temp files for cross-process state (background sampler → parent)
PEAK_FILE=$(mktemp /tmp/bench-otel-peak-XXXXXX)
DRAIN_FILE=$(mktemp /tmp/bench-otel-drain-XXXXXX)
echo "0" > "$PEAK_FILE"
SAMPLE_START=$(date +%s)

sample_stats() {
  local prev_exported=0
  local elapsed=0
  local peak=0
  while true; do
    local stats
    stats=$(get_stats) || { sleep "$SAMPLE_INTERVAL"; continue; }

    IFS='|' read -r enq drop exp fail qsz qcap usage <<< "$stats"
    local now=$(($(date +%s) - SAMPLE_START))
    local drain_rate=0

    if [[ $elapsed -gt 0 && $exp -gt $prev_exported ]]; then
      drain_rate=$(( (exp - prev_exported) / SAMPLE_INTERVAL ))
    fi

    # Track peak usage (compare as floats using awk)
    peak=$(awk "BEGIN { print ($usage > $peak) ? $usage : $peak }")
    echo "$peak" > "$PEAK_FILE"

    if [[ $drain_rate -gt 0 ]]; then
      echo "$drain_rate" >> "$DRAIN_FILE"
    fi

    printf "  %5ds | %12s | %12s | %10s | %8s | %5.1f%% | %12s\n" \
      "$now" "$(comma "$enq")" "$(comma "$exp")" "$(comma "$drop")" \
      "$(comma "$qsz")" "$usage" "$(comma "$drain_rate")"

    prev_exported=$exp
    elapsed=$((elapsed + SAMPLE_INTERVAL))
    sleep "$SAMPLE_INTERVAL"
  done
}

# Run sampler in background subshell
sample_stats &
SAMPLER_PID=$!
echo "$SAMPLER_PID" > /tmp/bench-otel-sampler.pid

cleanup() {
  kill "$SAMPLER_PID" 2>/dev/null || true
  wait "$SAMPLER_PID" 2>/dev/null || true
  rm -f "$BENCH_SCRIPT" "$PEAK_FILE" "$DRAIN_FILE" /tmp/bench-otel-sampler.pid
}
trap cleanup EXIT

# --- Run pgbench ---
echo
WALL_START=$(date +%s.%N)
pgbench -c "$CLIENTS" -j "$JOBS" -T "$DURATION" -R "$TPS" -f "$BENCH_SCRIPT" --no-vacuum 2>&1 | tail -5 &
PGBENCH_PID=$!

# --- Start perf recording while pgbench is running ---
PERF_PID=""
if [[ -n "$BGWORKER_PID" ]] && command -v perf >/dev/null 2>&1; then
  # Wait a few seconds for load to build up, then start perf
  sleep 5
  echo
  echo "Starting perf record on bgworker (PID $BGWORKER_PID) for ${PERF_DURATION}s..."
  perf record -g -o bench-otel-perf.data -p "$BGWORKER_PID" -- sleep "$PERF_DURATION" &
  PERF_PID=$!
fi

# Wait for pgbench to finish
wait "$PGBENCH_PID" || true
PGBENCH_DONE=$(date +%s.%N)

echo
bold "pgbench finished. Waiting for queue to drain (up to ${DRAIN_TIMEOUT}s)..."; echo

# --- Wait for drain ---
DRAIN_START=$(date +%s)
while true; do
  stats=$(get_stats) || break
  IFS='|' read -r enq drop exp fail qsz qcap usage <<< "$stats"
  if [[ "$qsz" -eq 0 ]]; then
    green "Queue drained."; echo
    break
  fi
  if [[ $(($(date +%s) - DRAIN_START)) -ge $DRAIN_TIMEOUT ]]; then
    red "Drain timeout after ${DRAIN_TIMEOUT}s (queue_size=$qsz)"; echo
    break
  fi
  sleep 1
done

WALL_END=$(date +%s.%N)

# --- Stop sampler ---
kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true

# --- Wait for perf if running ---
if [[ -n "$PERF_PID" ]]; then
  echo -n "Waiting for perf to finish... "
  wait "$PERF_PID" 2>/dev/null || true
  green "OK"; echo
fi

# --- Generate perf report ---
if [[ -f bench-otel-perf.data ]]; then
  echo
  bold "Generating perf report..."; echo

  perf report --stdio --no-children -i bench-otel-perf.data > bench-otel-perf.txt 2>/dev/null || true

  if [[ -f bench-otel-perf.txt ]]; then
    echo
    bold "--- Top 15 Hotspots ---"; echo
    # Extract overhead lines (skip headers), print top 15
    grep -E '^\s+[0-9]+\.[0-9]+%' bench-otel-perf.txt | head -15
    echo
    echo "Full report: bench-otel-perf.txt"
  fi

  # Generate flamegraph if available
  if command -v flamegraph.pl >/dev/null 2>&1 && command -v stackcollapse-perf.pl >/dev/null 2>&1; then
    echo -n "Generating flamegraph SVG... "
    perf script -i bench-otel-perf.data 2>/dev/null \
      | stackcollapse-perf.pl 2>/dev/null \
      | flamegraph.pl > bench-otel-flame.svg 2>/dev/null \
      && { green "OK"; echo "  → bench-otel-flame.svg"; } \
      || { red "failed"; }
    echo
  else
    echo "(flamegraph.pl not found, skipping SVG generation)"
  fi
fi

# --- Final stats ---
stats=$(get_stats)
IFS='|' read -r enq drop exp fail qsz qcap usage <<< "$stats"

WALL_TIME=$(awk "BEGIN { printf \"%.1f\", $WALL_END - $WALL_START }")
DROP_RATE=$(awk "BEGIN { if ($enq > 0) printf \"%.2f\", ($drop / $enq) * 100; else print \"0.00\" }")

# Read peak usage from temp file
PEAK_USAGE=$(cat "$PEAK_FILE" 2>/dev/null || echo "0")

# Compute average drain rate from sampled values
AVG_DRAIN=0
if [[ -s "$DRAIN_FILE" ]]; then
  AVG_DRAIN=$(awk '{ sum += $1; n++ } END { if (n>0) printf "%d", sum/n; else print 0 }' "$DRAIN_FILE")
fi

# Simple fallback: total exported / wall time
if [[ "$AVG_DRAIN" -eq 0 && "$exp" -gt 0 ]]; then
  AVG_DRAIN=$(awk "BEGIN { printf \"%d\", $exp / $WALL_TIME }")
fi

echo
bold "--- Summary ---"; echo
printf "Total enqueued:   %12s\n" "$(comma "$enq")"
printf "Total exported:   %12s\n" "$(comma "$exp")"
printf "Total dropped:    %12s\n" "$(comma "$drop")"
printf "Drop rate:        %11s%%\n" "$DROP_RATE"
printf "Send failures:    %12s\n" "$(comma "$fail")"
printf "Peak queue usage: %11s%%\n" "$PEAK_USAGE"
printf "Avg drain rate:   %12s events/sec\n" "$(comma "$AVG_DRAIN")"
printf "Wall time:        %11ss\n" "$WALL_TIME"
echo

# --- Restore GUCs ---
echo -n "Restoring GUCs... "
psql -XAtqc "ALTER SYSTEM RESET pg_stat_ch.flush_interval_ms"
psql -XAtqc "ALTER SYSTEM RESET pg_stat_ch.batch_max"
psql -XAtqc "SELECT pg_reload_conf()"
green "done"; echo
