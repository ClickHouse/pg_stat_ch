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

# Use the same base image as the runtime stage so postgresql-server-dev-18
# is pinned to exactly the same PostgreSQL minor version.  Building against
# a different minor version produces a .so with a mismatched magic block that
# PostgreSQL refuses to load ("missing magic block" FATAL at startup).
FROM postgres:18-bookworm AS builder

RUN apt-get update \
    && PG_PKG_VERSION=$(dpkg-query -W -f='${Version}' postgresql-18) \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        build-essential \
        cmake \
        ninja-build \
        git \
        libssl-dev \
        zlib1g-dev \
        "postgresql-server-dev-18=${PG_PKG_VERSION}" \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build/pg_stat_ch

# ── Deps layer (cached unless CMakeLists.txt / cmake/ / third_party/ change) ──
COPY CMakeLists.txt ./
COPY cmake/ cmake/
COPY third_party/ third_party/

# Create empty stub sources so cmake can configure and compile all third-party
# deps (gRPC, OTel, protobuf, abseil) without the real pg_stat_ch sources.
# The stub list does NOT need to exactly match the real source tree: the source
# layer re-runs cmake configure, which re-evaluates file(GLOB_RECURSE) against
# the real files, so new .cc files are picked up automatically.
RUN mkdir -p \
        src/config src/export src/hooks src/queue src/worker \
        include/config include/export include/hooks \
        include/pg_stat_ch include/queue include/worker \
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
# Compiles all 2000+ gRPC/OTel/protobuf dep targets (~40 min, heavily cached).
RUN cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_OPENSSL=ON \
    && cmake --build build --parallel $(nproc)

# ── Source layer (invalidated on every source edit, but fast ~30 s) ──
COPY include/ include/
COPY src/ src/
COPY sql/ sql/
COPY pg_stat_ch.control ./

# Re-run configure so file(GLOB_RECURSE) picks up the real source list.
# FetchContent skips re-population (populated state is cached in the build dir).
# Touch sources so ninja sees them as newer than the stub objects.
RUN cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_OPENSSL=ON \
    && find src include -name '*.cc' -o -name '*.h' | xargs touch \
    && cmake --build build --parallel $(nproc)

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
