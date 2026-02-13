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
RUN cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_OPENSSL=ON \
    && cmake --build build --parallel $(nproc)

FROM postgres:18-bookworm

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/pg_stat_ch/build/pg_stat_ch.so /usr/lib/postgresql/18/lib/
COPY --from=builder /build/pg_stat_ch/sql/pg_stat_ch--0.1.0.sql /usr/share/postgresql/18/extension/
COPY --from=builder /build/pg_stat_ch/pg_stat_ch.control /usr/share/postgresql/18/extension/

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD pg_isready -U postgres || exit 1

EXPOSE 5432
