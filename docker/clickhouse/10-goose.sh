#!/bin/bash
# Runs inside the entrypoint's temporary server (localhost:9000).
set -euo pipefail
goose -dir /migrations clickhouse "tcp://localhost:9000?database=${CLICKHOUSE_DB:-pg_stat_ch}" up
