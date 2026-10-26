# Shared Postgres image: builds pg_stat_ch for PostgreSQL 18 and provides runtime

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
    libkrb5-dev \
    flex \
    bison \
    pkg-config \
    zip \
    unzip \
    tar \
    && rm -rf /var/lib/apt/lists/*

# Install vcpkg
# Pin to the same commit as the vcpkg submodule for reproducible builds
# (vcpkg tag 2026.06.24).
ARG VCPKG_COMMIT=cd61e1e26a038e82d6550a3ebbe0fbbfe7da78e3
RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg \
    && git -C /opt/vcpkg checkout "$VCPKG_COMMIT" \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics
ENV VCPKG_ROOT=/opt/vcpkg
ENV PATH="/opt/vcpkg:${PATH}"

WORKDIR /build/pg_stat_ch

# Build vcpkg dependencies in their own layer so source changes don't rebuild them.
# The cache mount keeps vcpkg's per-package binary archives across builds, so a
# vcpkg.json change only rebuilds the ports it affects.
COPY vcpkg.json vcpkg-configuration.json ./
COPY triplets/ triplets/
RUN --mount=type=cache,target=/root/.cache/vcpkg/archives \
    case "$(uname -m)" in \
      x86_64) triplet=x64-linux-pic ;; \
      aarch64) triplet=arm64-linux-pic ;; \
      *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;; \
    esac \
    && echo "$triplet" > /build/vcpkg-triplet \
    && vcpkg install --triplet="$triplet" --host-triplet="$triplet" \
      --x-install-root=/build/vcpkg_installed \
      --clean-after-build

COPY CMakeLists.txt ./
COPY cmake/ cmake/
COPY include/ include/
COPY src/ src/
COPY sql/ sql/
COPY pg_stat_ch.control ./
# Vendored header-only clickhouse-c client (git submodule, must be checked out
# on the host before `docker build`).
COPY third_party/clickhouse-c/ third_party/clickhouse-c/

RUN cmake -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DPSCH_BUILD_UNIT_TESTS=OFF \
    -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
    -DVCPKG_TARGET_TRIPLET="$(cat /build/vcpkg-triplet)" \
    -DVCPKG_HOST_TRIPLET="$(cat /build/vcpkg-triplet)" \
    -DVCPKG_INSTALLED_DIR=/build/vcpkg_installed \
    -DVCPKG_OVERLAY_TRIPLETS=/build/pg_stat_ch/triplets \
    && cmake --build build --parallel $(nproc)

FROM postgres:18-bookworm

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/pg_stat_ch/build/pg_stat_ch.so /usr/lib/postgresql/18/lib/
COPY --from=builder /build/pg_stat_ch/sql/ /usr/share/postgresql/18/extension/
COPY --from=builder /build/pg_stat_ch/pg_stat_ch.control /usr/share/postgresql/18/extension/

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD pg_isready -U postgres || exit 1

EXPOSE 5432
