# pg_stat_ch performance benchmark image
#
# Single container with PostgreSQL, pg_stat_ch (RelWithDebInfo), pgbench,
# and Linux perf tools. Keeping everything in one container means perf can
# attach to the bgworker PID without crossing container boundaries.
#
# Layer-cache strategy:
#   deps layer  — vcpkg.json + triplets/ + cmake/ + stubs → full vcpkg install
#                 + configure of all gRPC/OTel/protobuf deps; invalidated only
#                 when dependency versions or CMakeLists.txt change (rare).
#   source layer — real src/ + include/ → only the pg_stat_ch .cc files
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
        pkg-config \
        curl \
        zip \
        unzip \
        tar \
        "postgresql-server-dev-18=${PG_PKG_VERSION}" \
    && rm -rf /var/lib/apt/lists/*

# Install vcpkg
# Pin to the same commit as the vcpkg submodule for reproducible builds
ARG VCPKG_COMMIT=12159785447291b4069c82a3fe9c2770a393ac7f
RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg \
    && git -C /opt/vcpkg checkout "$VCPKG_COMMIT" \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics
ENV VCPKG_ROOT=/opt/vcpkg
ENV PATH="/opt/vcpkg:${PATH}"

WORKDIR /build/pg_stat_ch

# ── Deps layer (cached unless vcpkg.json / triplets / CMakeLists.txt change) ──
COPY vcpkg.json vcpkg-configuration.json ./
COPY triplets/ triplets/
COPY CMakeLists.txt ./
COPY cmake/ cmake/

# One empty stub is enough for cmake to configure the pg_stat_ch target and
# build all third-party deps.  The source layer re-runs cmake configure, which
# re-evaluates file(GLOB_RECURSE SOURCES src/*.cc) against the real files, so
# the stub list never needs to be kept in sync with the actual source tree.
RUN mkdir -p src include \
    && touch src/stub.cc pg_stat_ch.control

# RelWithDebInfo: optimized but with debug symbols for perf/flamegraph.
RUN cmake -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
    -DVCPKG_TARGET_TRIPLET=x64-linux-pic \
    -DVCPKG_OVERLAY_TRIPLETS=/build/pg_stat_ch/triplets \
    && cmake --build build --parallel $(nproc)

# ── Source layer (invalidated on every source edit, but fast ~30 s) ──
COPY include/ include/
COPY src/ src/
COPY sql/ sql/
COPY pg_stat_ch.control ./

# Re-run configure so file(GLOB_RECURSE) picks up the real source list.
# Touch sources so ninja sees them as newer than the stub objects.
RUN cmake -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
    -DVCPKG_TARGET_TRIPLET=x64-linux-pic \
    -DVCPKG_OVERLAY_TRIPLETS=/build/pg_stat_ch/triplets \
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
