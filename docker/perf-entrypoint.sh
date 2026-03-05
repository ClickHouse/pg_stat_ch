#!/usr/bin/env bash
# perf-entrypoint.sh — Docker entrypoint for the pg_stat_ch perf benchmark container.
#
# Bootstraps a throwaway PostgreSQL instance with pg_stat_ch loaded and
# use_otel=on, then hands off to bench-otel.sh. Any extra arguments are
# forwarded to bench-otel.sh (e.g. --tps 20000 --duration 60).

set -euo pipefail

OTEL_ENDPOINT="${OTEL_ENDPOINT:-otelcol:4317}"
OTEL_HEALTH="${OTEL_HEALTH:-http://otelcol:13133}"
PGDATA="${PGDATA:-/var/lib/postgresql/data}"
RESULTS_DIR="${RESULTS_DIR:-/results}"

# Ensure results directory exists (may be a volume mount)
mkdir -p "$RESULTS_DIR"

echo "=== pg_stat_ch OTel Benchmark (Docker) ==="
echo "OTel endpoint : $OTEL_ENDPOINT"
echo "OTel health   : $OTEL_HEALTH"
echo "Results dir   : $RESULTS_DIR"
echo

# --- Bootstrap PostgreSQL ---
install -o postgres -g postgres -m 700 -d "$PGDATA"

echo -n "Initializing database cluster..."
gosu postgres initdb \
    -D "$PGDATA" \
    --auth-host=trust \
    --auth-local=trust \
    -U postgres \
    --encoding=UTF8 \
    --locale=C \
    --no-instructions \
    -q
echo " done."

# Configure pg_stat_ch with OTel export
cat >> "$PGDATA/postgresql.conf" <<EOF

# pg_stat_ch configuration (injected by perf-entrypoint.sh)
shared_preload_libraries = 'pg_stat_ch'
pg_stat_ch.enabled = on
pg_stat_ch.use_otel = on
pg_stat_ch.otel_endpoint = '${OTEL_ENDPOINT}'
pg_stat_ch.queue_capacity = 131072
EOF

# --- Start PostgreSQL in the background ---
gosu postgres postgres -D "$PGDATA" -k /var/run/postgresql &
PG_PID=$!

echo -n "Waiting for PostgreSQL..."
until gosu postgres pg_isready -U postgres -h /var/run/postgresql -q 2>/dev/null; do
    printf '.'
    sleep 0.5
done
echo " ready. (PID $PG_PID)"

# Create extension and initialize pgbench schema
gosu postgres psql -U postgres -h /var/run/postgresql \
    -c "CREATE EXTENSION pg_stat_ch" postgres >/dev/null

echo -n "Initializing pgbench schema..."
gosu postgres pgbench -U postgres -h /var/run/postgresql -i -q postgres 2>/dev/null
echo " done."

# --- Run the benchmark ---
# Use Unix socket so bench-otel.sh psql calls don't need a password.
export PGHOST=/var/run/postgresql
export PGUSER=postgres
export PGDATABASE=postgres
export OTEL_HEALTH
export OTEL_ENDPOINT

cd "$RESULTS_DIR"
bench-otel.sh "$@"
BENCH_EXIT=$?

# --- Shutdown ---
gosu postgres pg_ctl -D "$PGDATA" stop -m fast 2>/dev/null || kill -TERM "$PG_PID"
wait "$PG_PID" 2>/dev/null || true

exit "$BENCH_EXIT"
