#!/usr/bin/env bash
#
# Load generator for pg_stat_ch benchmark
# Uses pgbench with custom SQL scripts to generate mixed workloads
#

set -euo pipefail

# Default configuration
CONNECTIONS=128
DURATION=60
HOST="localhost"
PORT=5433
DATABASE="benchmark"
USER="postgres"
PASSWORD="postgres"

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_DIR="${SCRIPT_DIR}/.pgbench_scripts"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Generate load for pg_stat_ch benchmark testing.

Options:
    -c, --connections NUM    Number of concurrent connections (default: $CONNECTIONS)
    -d, --duration SECS      Duration in seconds (default: $DURATION)
    -h, --host HOST          PostgreSQL host (default: $HOST)
    -p, --port PORT          PostgreSQL port (default: $PORT)
    -D, --database DB        Database name (default: $DATABASE)
    -U, --user USER          Database user (default: $USER)
    --help                   Show this help message

Examples:
    $(basename "$0") -c 128 -d 60      # 128 connections for 60 seconds
    $(basename "$0") -c 64 -d 300      # 64 connections for 5 minutes
EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--connections) CONNECTIONS="$2"; shift 2 ;;
        -d|--duration) DURATION="$2"; shift 2 ;;
        -h|--host) HOST="$2"; shift 2 ;;
        -p|--port) PORT="$2"; shift 2 ;;
        -D|--database) DATABASE="$2"; shift 2 ;;
        -U|--user) USER="$2"; shift 2 ;;
        --help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

export PGPASSWORD="$PASSWORD"

# Create temp directory for pgbench scripts
mkdir -p "$TEMP_DIR"

# Create SELECT workload (40%) - reads, aggregations, joins
cat > "$TEMP_DIR/select.sql" <<'EOF'
-- Simple user lookup
\set user_id random(1, 10000)
SELECT id, username, email, balance, status FROM bench.users WHERE id = :user_id;

-- Aggregation query
SELECT status, COUNT(*), AVG(balance)::DECIMAL(15,2) as avg_balance
FROM bench.users
GROUP BY status;

-- Join query: user orders with products
\set user_id random(1, 10000)
SELECT o.id, o.quantity, o.total_price, o.status, i.name, i.price
FROM bench.orders o
JOIN bench.inventory i ON o.product_id = i.id
WHERE o.user_id = :user_id
ORDER BY o.created_at DESC
LIMIT 10;

-- Range query on transactions
\set days random(1, 30)
SELECT type, COUNT(*), SUM(amount)::DECIMAL(15,2) as total
FROM bench.transactions
WHERE created_at > NOW() - (:days || ' days')::INTERVAL
GROUP BY type;

-- Full-text style search (LIKE)
\set pattern random(1, 100)
SELECT id, name, category, price
FROM bench.inventory
WHERE name LIKE '%' || :pattern || '%'
LIMIT 20;
EOF

# Create INSERT workload (25%) - orders and transactions
cat > "$TEMP_DIR/insert.sql" <<'EOF'
-- Insert new order
\set user_id random(1, 10000)
\set product_id random(1, 1000)
\set qty random(1, 5)
\set price random(10, 500)
INSERT INTO bench.orders (user_id, product_id, quantity, total_price, status)
VALUES (:user_id, :product_id, :qty, :price, 'pending');

-- Insert transaction
\set user_id random(1, 10000)
\set amount random(1, 1000)
\set type_num random(0, 2)
INSERT INTO bench.transactions (user_id, amount, type, description)
VALUES (
    :user_id,
    :amount,
    CASE :type_num WHEN 0 THEN 'credit' WHEN 1 THEN 'debit' ELSE 'refund' END,
    'Benchmark transaction ' || NOW()::TEXT
);
EOF

# Create UPDATE workload (25%) - balances and statuses
cat > "$TEMP_DIR/update.sql" <<'EOF'
-- Update user balance
\set user_id random(1, 10000)
\set delta random(-100, 100)
UPDATE bench.users
SET balance = balance + :delta, updated_at = NOW()
WHERE id = :user_id;

-- Update order status
\set order_id random(1, 50000)
\set status_num random(0, 3)
UPDATE bench.orders
SET status = CASE :status_num
    WHEN 0 THEN 'processing'
    WHEN 1 THEN 'shipped'
    WHEN 2 THEN 'completed'
    ELSE 'cancelled'
END
WHERE id = :order_id;

-- Update inventory quantity
\set product_id random(1, 1000)
\set qty_delta random(-10, 10)
UPDATE bench.inventory
SET quantity = GREATEST(0, quantity + :qty_delta)
WHERE id = :product_id;
EOF

# Create DELETE workload (10%) - cleanup
cat > "$TEMP_DIR/delete.sql" <<'EOF'
-- Delete old pending orders (small cleanup)
\set days random(30, 90)
DELETE FROM bench.orders
WHERE status = 'pending'
  AND created_at < NOW() - (:days || ' days')::INTERVAL
  AND id IN (SELECT id FROM bench.orders WHERE status = 'pending' LIMIT 1);

-- Delete old transactions (targeted)
\set days random(60, 180)
DELETE FROM bench.transactions
WHERE created_at < NOW() - (:days || ' days')::INTERVAL
  AND id IN (SELECT id FROM bench.transactions ORDER BY created_at LIMIT 1);
EOF

echo "=============================================="
echo "pg_stat_ch Benchmark Load Generator"
echo "=============================================="
echo "Host:        $HOST:$PORT"
echo "Database:    $DATABASE"
echo "Connections: $CONNECTIONS"
echo "Duration:    ${DURATION}s"
echo "=============================================="
echo ""

# Get initial stats
echo "Initial pg_stat_ch stats:"
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" -c "SELECT * FROM pg_stat_ch_stats();" 2>/dev/null || echo "(stats unavailable)"
echo ""

# Calculate connections per workload
# 40% SELECT, 25% INSERT, 25% UPDATE, 10% DELETE
SELECT_CONNS=$((CONNECTIONS * 40 / 100))
INSERT_CONNS=$((CONNECTIONS * 25 / 100))
UPDATE_CONNS=$((CONNECTIONS * 25 / 100))
DELETE_CONNS=$((CONNECTIONS * 10 / 100))

# Ensure at least 1 connection per workload
[[ $SELECT_CONNS -lt 1 ]] && SELECT_CONNS=1
[[ $INSERT_CONNS -lt 1 ]] && INSERT_CONNS=1
[[ $UPDATE_CONNS -lt 1 ]] && UPDATE_CONNS=1
[[ $DELETE_CONNS -lt 1 ]] && DELETE_CONNS=1

echo "Workload distribution:"
echo "  SELECT: $SELECT_CONNS connections (40%)"
echo "  INSERT: $INSERT_CONNS connections (25%)"
echo "  UPDATE: $UPDATE_CONNS connections (25%)"
echo "  DELETE: $DELETE_CONNS connections (10%)"
echo ""

echo "Starting benchmark at $(date)..."
echo ""

# Run pgbench in parallel for each workload type with different app names
run_workload() {
    local name=$1
    local script=$2
    local conns=$3
    local app_name="pgbench_${name}"

    pgbench -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" \
        --client="$conns" \
        --jobs="$((conns > 4 ? 4 : conns))" \
        --time="$DURATION" \
        --no-vacuum \
        --file="$script" \
        --progress=10 \
        2>&1 | sed "s/^/[$name] /" &
}

# Set application names via connection string and run workloads
# Note: pgbench doesn't support per-connection app names directly,
# so we'll use different ports or other methods in production

run_workload "SELECT" "$TEMP_DIR/select.sql" "$SELECT_CONNS"
run_workload "INSERT" "$TEMP_DIR/insert.sql" "$INSERT_CONNS"
run_workload "UPDATE" "$TEMP_DIR/update.sql" "$UPDATE_CONNS"
run_workload "DELETE" "$TEMP_DIR/delete.sql" "$DELETE_CONNS"

# Wait for all background jobs
wait

echo ""
echo "=============================================="
echo "Benchmark completed at $(date)"
echo "=============================================="
echo ""

# Get final stats
echo "Final pg_stat_ch stats:"
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" -c "SELECT * FROM pg_stat_ch_stats();" 2>/dev/null || echo "(stats unavailable)"
echo ""

# Cleanup temp files
rm -rf "$TEMP_DIR"

echo "Done!"
