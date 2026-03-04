#!/usr/bin/env bash
# Run pg_stat_ch tests against a specific PostgreSQL version
# Usage: ./scripts/run-tests.sh <PG_VERSION|PG_PATH> [test_type]
#   PG_VERSION: 16, 17, or 18 (uses mise-installed PostgreSQL)
#   PG_PATH: path to a local PostgreSQL installation (e.g., ../postgres/install_tap)
#   test_type: regress, tap, isolation, or all (default: all)

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 <PG_VERSION|PG_PATH> [test_type] [test_filter]"
    echo "  PG_VERSION:  PostgreSQL version (16, 17, 18) - uses mise"
    echo "  PG_PATH:     Path to local PostgreSQL installation"
    echo "  test_type:   regress, tap, isolation, stress, clickhouse, otel, or all (default: all)"
    echo "  test_filter: (tap only) pattern to match test files, e.g., '021' for t/*021*.pl"
    echo ""
    echo "Examples:"
    echo "  $0 18                           # Run all tests against mise PG 18"
    echo "  $0 17 regress                   # Run only regression tests against mise PG 17"
    echo "  $0 ../postgres/install_tap tap  # Run TAP tests against local build"
    echo "  $0 ../postgres/install_tap tap 021  # Run only t/*021*.pl test"
    echo "  $0 18 stress                    # Run only stress test against PG 18"
    echo "  $0 18 clickhouse                # Run ClickHouse integration tests (requires Docker)"
    echo "  $0 18 otel                      # Run OTel integration tests (requires Docker)"
    exit 1
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check arguments
if [[ $# -lt 1 ]]; then
    usage
fi

PG_ARG="$1"
TEST_TYPE="${2:-all}"
TEST_FILTER="${3:-}"

# Determine if PG_ARG is a version number or a path
if [[ "$PG_ARG" =~ ^[0-9]+$ ]]; then
    # It's a version number, use mise
    PG_VERSION="$PG_ARG"
    PG_DIR=$(mise where "postgres@${PG_VERSION}" 2>/dev/null) || {
        log_error "PostgreSQL ${PG_VERSION} not found. Install with: mise install postgres@${PG_VERSION}"
        exit 1
    }
    log_info "Using PostgreSQL ${PG_VERSION} from mise: ${PG_DIR}"
else
    # It's a path to a local PostgreSQL installation
    PG_DIR="$(cd "$(dirname "$0")/.." && cd "$PG_ARG" 2>/dev/null && pwd)" || PG_DIR="$PG_ARG"
    if [[ ! -d "$PG_DIR" ]]; then
        log_error "PostgreSQL directory not found: ${PG_DIR}"
        exit 1
    fi
    log_info "Using local PostgreSQL from: ${PG_DIR}"
fi

# Set up paths
PG_BIN="${PG_DIR}/bin"
PG_LIB="${PG_DIR}/lib"
PG_CONFIG="${PG_BIN}/pg_config"

# Verify pg_config exists
if [[ ! -x "${PG_CONFIG}" ]]; then
    log_error "pg_config not found at ${PG_CONFIG}"
    exit 1
fi

# Export PATH so child processes find the right binaries
export PATH="${PG_BIN}:${PATH}"

# Project root
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "${PROJECT_DIR}"

# Run regression tests
run_regress() {
    log_info "Running regression tests..."

    local pg_regress="${PG_LIB}/pgxs/src/test/regress/pg_regress"
    if [[ ! -x "${pg_regress}" ]]; then
        log_error "pg_regress not found at ${pg_regress}"
        return 1
    fi

    "${pg_regress}" \
        --bindir="${PG_BIN}" \
        --inputdir=test/regression \
        --outputdir=test/regression/results \
        --temp-instance=test/regression/tmp_check \
        --temp-config=test/regression/pg_stat_ch.conf \
        basic version guc stats utility buffers cmd_type client_info error_capture drop_database_barrier
}

# Run TAP tests
# Optional argument: specific test file pattern (e.g., "021" to run t/021*.pl)
run_tap() {
    local test_pattern="${1:-}"

    if [[ -n "$test_pattern" ]]; then
        log_info "Running TAP test: t/*${test_pattern}*.pl"
    else
        log_info "Running TAP tests..."
    fi

    local perl_lib="${PG_LIB}/pgxs/src/test/perl"
    if [[ ! -d "${perl_lib}" ]]; then
        log_warn "PostgreSQL TAP test modules not found at ${perl_lib}"
        log_warn "TAP tests require PostgreSQL built with --enable-tap-tests"
        log_warn "Skipping TAP tests."
        return 0
    fi

    local pg_regress="${PG_LIB}/pgxs/src/test/regress/pg_regress"
    if [[ ! -x "${pg_regress}" ]]; then
        log_warn "pg_regress not found at ${pg_regress}"
        log_warn "TAP tests require pg_regress binary."
        log_warn "Skipping TAP tests."
        return 0
    fi

    # Clean up stale test data directories
    rm -rf tmp_check

    # Determine which tests to run
    local test_files
    if [[ -n "$test_pattern" ]]; then
        test_files=$(ls t/*${test_pattern}*.pl 2>/dev/null) || {
            log_error "No test files matching: t/*${test_pattern}*.pl"
            return 1
        }
    else
        test_files="t/*.pl"
    fi

    # PG_REGRESS is required by PostgreSQL::Test::Cluster for auth configuration
    PG_REGRESS="${pg_regress}" prove -v --timer \
        -I "${perl_lib}" \
        -I t \
        $test_files
}

# Run only stress test
run_stress() {
    log_info "Running stress test..."

    local perl_lib="${PG_LIB}/pgxs/src/test/perl"
    if [[ ! -d "${perl_lib}" ]]; then
        log_warn "PostgreSQL TAP test modules not found at ${perl_lib}"
        log_warn "Stress test requires PostgreSQL built with --enable-tap-tests"
        log_warn "Skipping stress test."
        return 0
    fi

    local pg_regress="${PG_LIB}/pgxs/src/test/regress/pg_regress"
    if [[ ! -x "${pg_regress}" ]]; then
        log_warn "pg_regress not found at ${pg_regress}"
        log_warn "Stress test requires pg_regress binary."
        log_warn "Skipping stress test."
        return 0
    fi

    PG_REGRESS="${pg_regress}" prove -v --timer \
        -I "${perl_lib}" \
        -I t \
        t/001_stress_test.pl
}

# Run isolation tests
run_isolation() {
    log_info "Running isolation tests..."

    local pg_isolation="${PG_LIB}/pgxs/src/test/isolation/pg_isolation_regress"
    if [[ ! -x "${pg_isolation}" ]]; then
        log_warn "pg_isolation_regress not found at ${pg_isolation}"
        log_warn "Skipping isolation tests."
        return 0
    fi

    # Create results directory if needed
    mkdir -p specs/results

    "${pg_isolation}" \
        --bindir="${PG_BIN}" \
        --inputdir=specs \
        --outputdir=specs/results \
        --temp-instance=specs/tmp_check \
        --temp-config=test/regression/pg_stat_ch.conf \
        --load-extension=pg_stat_ch \
        ring_buffer_concurrent ring_buffer_boundary overflow_race
}

# Run ClickHouse integration tests
run_clickhouse() {
    log_info "Running ClickHouse integration tests..."

    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        log_error "Docker is required for ClickHouse tests"
        return 1
    fi

    # Check if ClickHouse container is running
    if ! curl -s 'http://localhost:18123/' --data 'SELECT 1' 2>/dev/null | grep -q '^1'; then
        log_info "Starting ClickHouse container..."
        docker compose -f docker/docker-compose.test.yml up -d --wait
        sleep 5  # Extra wait for healthcheck
    fi

    local perl_lib="${PG_LIB}/pgxs/src/test/perl"
    if [[ ! -d "${perl_lib}" ]]; then
        log_warn "PostgreSQL TAP test modules not found at ${perl_lib}"
        log_warn "ClickHouse tests require PostgreSQL built with --enable-tap-tests"
        log_warn "Skipping ClickHouse tests."
        return 0
    fi

    local pg_regress="${PG_LIB}/pgxs/src/test/regress/pg_regress"
    if [[ ! -x "${pg_regress}" ]]; then
        log_warn "pg_regress not found at ${pg_regress}"
        log_warn "Skipping ClickHouse tests."
        return 0
    fi

    # Clean up stale test data directories
    rm -rf tmp_check

    # Set PROJECT_DIR for the test helper module
    export PROJECT_DIR="${PROJECT_DIR}"

    # Run only ClickHouse-related tests
    PG_REGRESS="${pg_regress}" prove -v --timer \
        -I "${perl_lib}" \
        -I t \
        t/010_clickhouse_export.pl t/011_clickhouse_reconnect.pl
}

# Run OTel integration tests
run_otel() {
    log_info "Running OTel integration tests..."

    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        log_error "Docker is required for OTel tests"
        return 1
    fi

    # Check if OTel collector is running; start it if not
    if ! curl -sf 'http://localhost:13133/' 2>/dev/null | grep -q 'Server available'; then
        log_info "Starting OTel collector container..."
        docker compose -f docker/docker-compose.otel.yml up -d

        # Poll until health endpoint responds (up to 30 seconds)
        local i
        for i in $(seq 1 30); do
            if curl -sf 'http://localhost:13133/' 2>/dev/null | grep -q 'Server available'; then
                break
            fi
            sleep 1
        done

        if ! curl -sf 'http://localhost:13133/' 2>/dev/null | grep -q 'Server available'; then
            log_error "OTel collector failed to become healthy"
            return 1
        fi
    fi

    local perl_lib="${PG_LIB}/pgxs/src/test/perl"
    if [[ ! -d "${perl_lib}" ]]; then
        log_warn "PostgreSQL TAP test modules not found at ${perl_lib}"
        log_warn "OTel tests require PostgreSQL built with --enable-tap-tests"
        log_warn "Skipping OTel tests."
        return 0
    fi

    local pg_regress="${PG_LIB}/pgxs/src/test/regress/pg_regress"
    if [[ ! -x "${pg_regress}" ]]; then
        log_warn "pg_regress not found at ${pg_regress}"
        log_warn "Skipping OTel tests."
        return 0
    fi

    # Clean up stale test data directories
    rm -rf tmp_check

    # Set PROJECT_DIR for the test helper module
    export PROJECT_DIR="${PROJECT_DIR}"

    # Run only OTel-related tests
    PG_REGRESS="${pg_regress}" prove -v --timer \
        -I "${perl_lib}" \
        -I t \
        t/024_otel_export.pl t/025_otel_reconnect.pl
}

# Main execution
case "${TEST_TYPE}" in
    regress)
        run_regress
        ;;
    tap)
        run_tap "${TEST_FILTER}"
        ;;
    stress)
        run_stress
        ;;
    isolation)
        run_isolation
        ;;
    clickhouse)
        run_clickhouse
        ;;
    otel)
        run_otel
        ;;
    all)
        run_regress
        run_tap
        run_isolation
        ;;
    *)
        log_error "Unknown test type: ${TEST_TYPE}"
        usage
        ;;
esac

log_info "Tests completed!"
