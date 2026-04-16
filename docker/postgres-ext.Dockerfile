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
    flex \
    bison \
    pkg-config \
    zip \
    unzip \
    tar \
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

# Copy dependency manifests first for layer caching
COPY vcpkg.json vcpkg-configuration.json ./
COPY triplets/ triplets/
COPY CMakeLists.txt ./
COPY cmake/ cmake/
COPY include/ include/
COPY src/ src/
COPY sql/ sql/
COPY pg_stat_ch.control ./

RUN cmake -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
    -DVCPKG_TARGET_TRIPLET=x64-linux-pic \
    -DVCPKG_OVERLAY_TRIPLETS=/build/pg_stat_ch/triplets \
    && cmake --build build --parallel $(nproc)

FROM postgres:18-bookworm

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/pg_stat_ch/build/pg_stat_ch.so /usr/lib/postgresql/18/lib/
COPY --from=builder /build/pg_stat_ch/sql/pg_stat_ch--0.1.sql /usr/share/postgresql/18/extension/
COPY --from=builder /build/pg_stat_ch/pg_stat_ch.control /usr/share/postgresql/18/extension/

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD pg_isready -U postgres || exit 1

EXPOSE 5432
