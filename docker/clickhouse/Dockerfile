# ClickHouse with the goose migrations applied on first start (same path as CI).
# Build context: repo root.
FROM golang:1.27.3-bookworm AS goose
# go install verifies modules against go.sum/sumdb; version pinned to match CI.
RUN GOBIN=/out go install github.com/pressly/goose/v3/cmd/goose@v3.27.1

FROM clickhouse/clickhouse-server:26.9
COPY --from=goose /out/goose /usr/local/bin/goose
COPY schema/migrations/ /migrations/
COPY docker/clickhouse/10-goose.sh /docker-entrypoint-initdb.d/10-goose.sh
