#!/usr/bin/env bash
# run-perf.sh — Build and run the pg_stat_ch OTel benchmark in Docker.
#
# Builds a Linux container with PostgreSQL, pg_stat_ch (RelWithDebInfo),
# pgbench, and Linux perf tools, then runs bench-otel.sh inside it.
#
# Usage: scripts/run-perf.sh [bench-otel.sh options]
#   e.g. scripts/run-perf.sh --tps 20000 --duration 60
#
# Output files are written to docker/perf-results/:
#   bench-otel-perf.data  - raw perf samples
#   bench-otel-perf.txt   - perf report (top functions by overhead)
#   bench-otel-flame.svg  - flamegraph (if flamegraph.pl is in the image)

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

COMPOSE="docker compose -f docker/docker-compose.perf.yml"
RESULTS_DIR="docker/perf-results"

# Create the results directory on the host so Docker doesn't create it as root
mkdir -p "$RESULTS_DIR"

echo "=== pg_stat_ch OTel Benchmark ==="
echo "Results: $PROJECT_DIR/$RESULTS_DIR/"
echo

cleanup() {
    $COMPOSE down --volumes 2>/dev/null || true
}
trap cleanup EXIT

# Build (only rebuilds if source has changed)
echo "Building benchmark image..."
$COMPOSE build pg-perf

# Run — pg-perf exits when the bench finishes; otelcol is stopped by cleanup
$COMPOSE up \
    --no-build \
    --abort-on-container-exit \
    --exit-code-from pg-perf \
    -- "$@" 2>&1 || true

# Show a summary of any perf report produced
echo
if [[ -f "$RESULTS_DIR/bench-otel-perf.txt" ]]; then
    echo "=== Top Hotspots ==="
    grep -E '^\s+[0-9]+\.[0-9]+%' "$RESULTS_DIR/bench-otel-perf.txt" | head -15 || true
    echo
    echo "Full report : $RESULTS_DIR/bench-otel-perf.txt"
fi
if [[ -f "$RESULTS_DIR/bench-otel-flame.svg" ]]; then
    echo "Flamegraph  : $RESULTS_DIR/bench-otel-flame.svg"
fi
