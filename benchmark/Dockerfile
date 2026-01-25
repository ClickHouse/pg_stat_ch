# Multi-stage build: compile pg_stat_ch extension + PostgreSQL 18 runtime

# Build stage - use plain debian for faster apt
FROM debian:bookworm AS builder

# Install build dependencies (add PostgreSQL apt repo for server-dev)
RUN apt-get update && apt-get install -y curl ca-certificates gnupg \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    postgresql-server-dev-18 \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy extension source code
WORKDIR /build
COPY . /build/pg_stat_ch/

# Build pg_stat_ch extension (clickhouse-cpp is statically linked via add_subdirectory)
WORKDIR /build/pg_stat_ch
RUN cmake --preset release -DPostgreSQL_ROOT=/usr/lib/postgresql/18 \
    && cmake --build build/release --parallel $(nproc)

# Runtime stage
FROM postgres:18-bookworm

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled extension from builder (clickhouse-cpp is statically linked)
COPY --from=builder /build/pg_stat_ch/build/release/libpg_stat_ch.so /usr/lib/postgresql/18/lib/
COPY --from=builder /build/pg_stat_ch/sql/pg_stat_ch--0.1.0.sql /usr/share/postgresql/18/extension/
COPY --from=builder /build/pg_stat_ch/sql/pg_stat_ch.control /usr/share/postgresql/18/extension/

# Copy init scripts
COPY benchmark/init/ /docker-entrypoint-initdb.d/

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD pg_isready -U postgres || exit 1

EXPOSE 5432
