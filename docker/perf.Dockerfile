# pg_stat_ch performance benchmark image
#
# Single container with PostgreSQL, pg_stat_ch (RelWithDebInfo), pgbench,
# and Linux perf tools. Keeping everything in one container means perf can
# attach to the bgworker PID without crossing container boundaries.

FROM debian:bookworm AS builder

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
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY CMakeLists.txt /build/pg_stat_ch/
COPY cmake/ /build/pg_stat_ch/cmake/
COPY include/ /build/pg_stat_ch/include/
COPY src/ /build/pg_stat_ch/src/
COPY sql/ /build/pg_stat_ch/sql/
COPY third_party/ /build/pg_stat_ch/third_party/
COPY pg_stat_ch.control /build/pg_stat_ch/
WORKDIR /build/pg_stat_ch
# RelWithDebInfo: optimized but with debug symbols for perf/flamegraph
RUN cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_OPENSSL=ON \
    && cmake --build build --parallel 2

# ---- Runtime ----
FROM postgres:18-bookworm

RUN apt-get update && apt-get install -y \
        linux-perf \
        postgresql-contrib-18 \
        curl \
        procps \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Install pg_stat_ch
COPY --from=builder /build/pg_stat_ch/build/pg_stat_ch.so /usr/lib/postgresql/18/lib/
COPY --from=builder /build/pg_stat_ch/sql/pg_stat_ch--0.1.sql /usr/share/postgresql/18/extension/
COPY --from=builder /build/pg_stat_ch/pg_stat_ch.control /usr/share/postgresql/18/extension/

COPY scripts/bench-otel.sh /usr/local/bin/bench-otel.sh
COPY docker/perf-entrypoint.sh /usr/local/bin/perf-entrypoint.sh
RUN chmod +x /usr/local/bin/bench-otel.sh /usr/local/bin/perf-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/perf-entrypoint.sh"]
