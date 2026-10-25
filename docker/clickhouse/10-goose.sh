#!/bin/bash
# Runs inside the entrypoint's temporary server (localhost:9000).
set -euo pipefail

# Percent-encode a value so a password with reserved characters (& = # ...)
# can't corrupt the DSN. LC_ALL=C so the loop iterates bytes.
urlencode() {
  local LC_ALL=C i char
  for ((i = 0; i < ${#1}; i++)); do
    char="${1:i:1}"
    case "$char" in
      [a-zA-Z0-9.~_-]) printf '%s' "$char" ;;
      *) printf '%%%02X' "'$char" ;;
    esac
  done
}

dsn="tcp://localhost:9000"
dsn+="?database=$(urlencode "${CLICKHOUSE_DB:-pg_stat_ch}")"
dsn+="&username=$(urlencode "${CLICKHOUSE_USER:-default}")"
dsn+="&password=$(urlencode "${CLICKHOUSE_PASSWORD:-}")"
GOOSE_DRIVER=clickhouse GOOSE_DBSTRING="$dsn" goose -dir /migrations up
