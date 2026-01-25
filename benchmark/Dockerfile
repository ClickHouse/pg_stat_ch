# Multi-stage build: compile pg_stat_ch extension + PostgreSQL 18 runtime

# Build stage
FROM postgres:18-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    postgresql-server-dev-18 \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy extension source code
WORKDIR /build
COPY . /build/pg_stat_ch/

# Build clickhouse-cpp dependency
WORKDIR /build/pg_stat_ch/deps/clickhouse-cpp
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release -DWITH_OPENSSL=ON \
    && cmake --build build --parallel $(nproc) \
    && cmake --install build

# Build pg_stat_ch extension
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

# Copy compiled extension from builder
COPY --from=builder /build/pg_stat_ch/build/release/libpg_stat_ch.so /usr/lib/postgresql/18/lib/
COPY --from=builder /build/pg_stat_ch/sql/pg_stat_ch--0.1.0.sql /usr/share/postgresql/18/extension/
COPY --from=builder /build/pg_stat_ch/sql/pg_stat_ch.control /usr/share/postgresql/18/extension/

# Copy clickhouse-cpp shared library
COPY --from=builder /usr/local/lib/libclickhouse-cpp-lib.so* /usr/local/lib/
RUN ldconfig

# Copy init scripts
COPY benchmark/init/ /docker-entrypoint-initdb.d/

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD pg_isready -U postgres || exit 1

EXPOSE 5432
