#!/usr/bin/env bash
# Unit test for src/export/arrow_batch.c + arrow_ipc_emit.c.
#
# Builds a standalone harness (real PostgreSQL server headers, stubbed elog
# symbols — see arrow_batch_harness.c), generates synthetic-event payloads,
# and validates them with arrow_batch_validate.py: structural V5/ZSTD framing
# checks plus pyarrow schema- and value-equality (via `uv run`, like
# t/026_arrow_dump.pl; falls back to structural-only if pyarrow is
# unavailable).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UNIT_DIR="$ROOT/test/unit"

# --- locate pg_config ---
PG_CONFIG="${PG_CONFIG:-}"
if [[ -z "$PG_CONFIG" ]]; then
  for cand in pg_config "mise x -- pg_config"; do
    if $cand --includedir-server >/dev/null 2>&1; then
      PG_CONFIG="$cand"
      break
    fi
  done
fi
if [[ -z "$PG_CONFIG" ]] && command -v brew >/dev/null 2>&1; then
  for ver in 18 17 16; do
    cand="$(brew --prefix "postgresql@$ver" 2>/dev/null || true)/bin/pg_config"
    if [[ -x "$cand" ]]; then
      PG_CONFIG="$cand"
      break
    fi
  done
fi
if [[ -z "$PG_CONFIG" ]]; then
  echo "SKIP: pg_config not found (set PG_CONFIG)" >&2
  exit 1
fi
PG_INC="$($PG_CONFIG --includedir-server)"
echo "using pg_config: $PG_CONFIG (server includes: $PG_INC)"

# --- locate zstd (vcpkg dep in real builds; brew/system for the harness) ---
ZSTD_CFLAGS=""
ZSTD_LDFLAGS="-lzstd"
if command -v brew >/dev/null 2>&1 && brew --prefix zstd >/dev/null 2>&1; then
  ZSTD_PREFIX="$(brew --prefix zstd)"
  ZSTD_CFLAGS="-I$ZSTD_PREFIX/include"
  ZSTD_LDFLAGS="-L$ZSTD_PREFIX/lib -lzstd"
fi

# PG built with --enable-nls needs libintl.h (gettext) for c.h.
NLS_CFLAGS=""
if command -v brew >/dev/null 2>&1 && brew --prefix gettext >/dev/null 2>&1; then
  NLS_CFLAGS="-I$(brew --prefix gettext)/include"
fi

CC="${CC:-cc}"
BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/psch_arrow_test.XXXXXX")"
trap 'rm -rf "$BUILD_DIR"' EXIT

INCLUDES=(
  -I "$ROOT/include"
  -I "$ROOT/src"
  -I "$ROOT/third_party/nanoarrow/include"
  -I "$PG_INC"
)
CFLAGS=(-std=gnu17 -g -O1 -DNDEBUG -DFLATCC_NO_ASSERT $ZSTD_CFLAGS $NLS_CFLAGS)

echo "compiling harness..."
$CC "${CFLAGS[@]}" -Wall -Wextra "${INCLUDES[@]}" -c "$ROOT/src/export/arrow_batch.c" \
  -o "$BUILD_DIR/arrow_batch.o"
$CC "${CFLAGS[@]}" -Wall -Wextra "${INCLUDES[@]}" -c "$ROOT/src/export/arrow_ipc_emit.c" \
  -o "$BUILD_DIR/arrow_ipc_emit.o"
$CC "${CFLAGS[@]}" "${INCLUDES[@]}" -c "$ROOT/third_party/nanoarrow/src/flatcc.c" \
  -o "$BUILD_DIR/flatcc.o"
$CC "${CFLAGS[@]}" -Wall -Wextra "${INCLUDES[@]}" -c "$UNIT_DIR/arrow_batch_harness.c" \
  -o "$BUILD_DIR/harness.o"
$CC "$BUILD_DIR/arrow_batch.o" "$BUILD_DIR/arrow_ipc_emit.o" "$BUILD_DIR/flatcc.o" \
  "$BUILD_DIR/harness.o" $ZSTD_LDFLAGS -o "$BUILD_DIR/harness"

echo "running harness..."
"$BUILD_DIR/harness" "$BUILD_DIR"

P1="$BUILD_DIR/payload1.bin"
P2="$BUILD_DIR/payload2.bin"
[[ -s "$P1" && -s "$P2" ]] || { echo "FAIL: payloads missing" >&2; exit 1; }

echo "validating payloads..."
if command -v uv >/dev/null 2>&1; then
  uv run --quiet "$UNIT_DIR/arrow_batch_validate.py" "$P1" "$P2"
elif python3 -c "import pyarrow" >/dev/null 2>&1; then
  python3 "$UNIT_DIR/arrow_batch_validate.py" "$P1" "$P2"
else
  echo "WARNING: uv/pyarrow unavailable; running structural-only validation" >&2
  python3 "$UNIT_DIR/arrow_batch_validate.py" --structural-only "$P1" "$P2"
fi

echo "arrow_batch_test PASS"
