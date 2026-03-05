# pg_stat_ch performance benchmark image
#
# Single container with PostgreSQL, pg_stat_ch (RelWithDebInfo), pgbench,
# and Linux perf tools. Keeping everything in one container means perf can
# attach to the bgworker PID without crossing container boundaries.
#
# Layer-cache strategy:
#   deps layer  — cmake/ + third_party/ + stubs → full configure+build of all
#                 gRPC/OTel/protobuf deps; invalidated only when third-party
#                 deps or CMakeLists.txt change (rare).
#   source layer — real src/ + include/ → only the 9 pg_stat_ch .cc files
#                 recompile; invalidated on every source edit (fast).

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

WORKDIR /build/pg_stat_ch

# ── Deps layer (cached unless CMakeLists.txt / cmake/ / third_party/ change) ──
COPY CMakeLists.txt ./
COPY cmake/ cmake/
COPY third_party/ third_party/

# Create empty stub sources so cmake configure + dep compilation can run
# without the real source files.  Empty .cc files compile to empty .o files
# and link into a placeholder shared library — all we need to warm the cache.
RUN mkdir -p \
        src/config \
        src/export \
        src/hooks \
        src/queue \
        src/worker \
        include/config \
        include/export \
        include/hooks \
        include/pg_stat_ch \
        include/queue \
        include/worker \
    && touch \
        src/pg_stat_ch.cc \
        src/config/guc.cc \
        src/export/clickhouse_exporter.cc \
        src/export/otel_exporter.cc \
        src/export/stats_exporter.cc \
        src/hooks/hooks.cc \
        src/queue/local_batch.cc \
        src/queue/shmem.cc \
        src/worker/bgworker.cc \
    && touch pg_stat_ch.control

# RelWithDebInfo: optimized but with debug symbols for perf/flamegraph.
# This step compiles all 2000+ gRPC/OTel/protobuf dependency targets.
# It is expensive (~5 min) but cached as long as the files above don't change.
RUN cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_OPENSSL=ON \
    && cmake --build build --parallel 2

# ── Source layer (invalidated on every source edit, but fast ~30 s) ──
COPY include/ include/
COPY src/ src/
COPY sql/ sql/
COPY pg_stat_ch.control ./

# Only the 9 pg_stat_ch .cc files need recompiling; all dep targets are cached.
RUN cmake --build build --parallel 2

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
