# Configuring Docker for ClickHouse Tests

The ClickHouse integration tests (`mise run test:clickhouse`) require Docker and Docker Compose to spin up a local ClickHouse instance. Setup differs slightly between macOS and Linux.

## macOS

### Why `brew install docker` isn't enough

`brew install docker` installs only the Docker **CLI**. On macOS the Docker daemon requires Linux kernel features (namespaces, cgroups) and cannot run natively — it always runs inside a lightweight VM. You need a runtime that provides that VM and wires the socket up to the CLI.

Options:

| Runtime | Install | Notes |
|---------|---------|-------|
| **Colima** | `brew install colima` | Lightweight, headless, recommended |
| Docker Desktop | `brew install --cask docker` | Official GUI app; licensing restrictions for large orgs |
| OrbStack | `brew install --cask orbstack` | Fast, polished; paid after trial |

### Colima setup (recommended)

```bash
brew install colima
colima start
```

To start Colima automatically at login:

```bash
brew services start colima
```

### Wire up the Docker Compose plugin

`brew install docker-compose` puts the compose binary in `/opt/homebrew/lib/docker/cli-plugins/`, but Docker's plugin search path doesn't include Homebrew's prefix by default. Symlink it once:

```bash
mkdir -p ~/.docker/cli-plugins
ln -sfn /opt/homebrew/lib/docker/cli-plugins/docker-compose \
        ~/.docker/cli-plugins/docker-compose
```

Verify both are working:

```bash
docker ps                # should return an empty container list
docker compose version   # should print Docker Compose version x.y.z
```

## Linux

Install Docker Engine via your distro's package manager, then add the Compose plugin:

```bash
# Debian/Ubuntu
sudo apt install docker-compose-plugin

# or using the Docker-provided repo (includes the plugin):
# https://docs.docker.com/engine/install/
```

Verify with the same commands above.

## Building a TAP-enabled PostgreSQL (required for ClickHouse tests)

Mise-installed PostgreSQL doesn't include the Perl TAP test modules (`PostgreSQL::Test::Cluster` etc.) because those are only built when PostgreSQL itself is compiled with `--enable-tap-tests`. The ClickHouse tests use these modules, so you need a source build.

Clone the PG 18 source next to the `pg_stat_ch` directory:

```bash
git clone --depth=1 --branch=REL_18_STABLE \
    https://github.com/postgres/postgres.git ../postgres
```

Install the required Perl module and Meson build system:

```bash
# macOS
brew install cpanminus meson ninja
cpanm --notest IPC::Run

# Linux (Debian/Ubuntu)
sudo apt install cpanminus meson ninja-build
cpanm --notest IPC::Run
```

When `cpanm` runs without root it installs to `~/perl5`. Both the Meson configure step and the test runner need to find `IPC::Run` there, so add this to your shell profile (`~/.zshrc`, `~/.bash_profile`, etc.) and reload it:

```bash
export PERL5LIB="$HOME/perl5/lib/perl5${PERL5LIB:+:$PERL5LIB}"
```

Configure and build:

```bash
cd ../postgres
meson setup build_tap --prefix="$(pwd)/install_tap" -Dtap_tests=enabled
ninja -C build_tap -j$(nproc)
ninja -C build_tap install
```

Build and install the extension against this PostgreSQL:

```bash
cd ../pg_stat_ch
cmake -B build -G Ninja -DPG_CONFIG=../postgres/install_tap/bin/pg_config
cmake --build build && cmake --install build
```

Run the ClickHouse tests:

```bash
./scripts/run-tests.sh ../postgres/install_tap clickhouse
```
