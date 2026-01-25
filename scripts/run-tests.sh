#!/usr/bin/env bash
# Run pg_stat_ch tests against a specific PostgreSQL version
# Usage: ./scripts/run-tests.sh <PG_VERSION> [test_type]
#   PG_VERSION: 16, 17, or 18
#   test_type: regress, tap, isolation, or all (default: all)

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 <PG_VERSION> [test_type]"
    echo "  PG_VERSION: PostgreSQL version (16, 17, 18)"
    echo "  test_type:  regress, tap, isolation, stress, or all (default: all)"
    echo ""
    echo "Examples:"
    echo "  $0 18              # Run all tests against PG 18"
    echo "  $0 17 regress      # Run only regression tests against PG 17"
    echo "  $0 18 tap          # Run only TAP tests against PG 18"
    echo "  $0 18 stress       # Run only stress test against PG 18"
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

PG_VERSION="$1"
TEST_TYPE="${2:-all}"

# Find PostgreSQL installation
PG_DIR=$(mise where "postgres@${PG_VERSION}" 2>/dev/null) || {
    log_error "PostgreSQL ${PG_VERSION} not found. Install with: mise install postgres@${PG_VERSION}"
    exit 1
}

log_info "Using PostgreSQL ${PG_VERSION} from: ${PG_DIR}"

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
        basic version guc stats utility buffers
}

# Run TAP tests
run_tap() {
    log_info "Running TAP tests..."

    local perl_lib="${PG_LIB}/pgxs/src/test/perl"
    if [[ ! -d "${perl_lib}" ]]; then
        log_warn "PostgreSQL TAP test modules not found at ${perl_lib}"
        log_warn "TAP tests require PostgreSQL built with --enable-tap-tests"
        log_warn "Skipping TAP tests."
        return 0
    fi

    prove -v --timer \
        -I "${perl_lib}" \
        -I t \
        t/*.pl
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

    prove -v --timer \
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
        ring_buffer_concurrent
}

# Main execution
case "${TEST_TYPE}" in
    regress)
        run_regress
        ;;
    tap)
        run_tap
        ;;
    stress)
        run_stress
        ;;
    isolation)
        run_isolation
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
