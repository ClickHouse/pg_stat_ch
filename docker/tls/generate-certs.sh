#!/usr/bin/env bash
# Generate throwaway self-signed certs for the TLS ClickHouse test container.
# Idempotent: skips when certs already exist. Certs are gitignored; the test
# uses skip_tls_verify so contents never need to be trusted.
set -euo pipefail

cert_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs"
crt="$cert_dir/server.crt"
key="$cert_dir/server.key"

if [[ -f "$crt" && -f "$key" ]]; then
  echo "TLS certs already present in $cert_dir"
  exit 0
fi

mkdir -p "$cert_dir"
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout "$key" -out "$crt"

# clickhouse-server runs as uid 101; bind mount preserves host perms
chmod 644 "$key" "$crt"
echo "Generated TLS certs in $cert_dir"
