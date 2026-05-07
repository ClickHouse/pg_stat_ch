#!/usr/bin/env bash
# Bump distribution version in META.json and (if needed) default_version in
# pg_stat_ch.control.
#
# Usage: ./scripts/bump-version.sh <version>
#   version: full distribution semver, e.g. 0.3.8
#
# META.json gets the full version (top-level + provides.pg_stat_ch.version).
# pg_stat_ch.control's default_version is the major.minor portion and is only
# rewritten when it differs from the current value (per CLAUDE.md: only bump
# when the SQL interface changes).

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <version>" >&2
  echo "  e.g.: $0 0.3.8" >&2
  exit 2
fi

VERSION="$1"

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: version must be MAJOR.MINOR.PATCH (got '$VERSION')" >&2
  exit 2
fi

MAJOR_MINOR="${VERSION%.*}"

cd "$(dirname "$0")/.."

META=META.json
CONTROL=pg_stat_ch.control

[[ -f "$META" ]]    || { echo "error: $META not found" >&2; exit 1; }
[[ -f "$CONTROL" ]] || { echo "error: $CONTROL not found" >&2; exit 1; }

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

jq --arg v "$VERSION" \
  '.version = $v | .provides.pg_stat_ch.version = $v' \
  "$META" > "$tmp"
mv "$tmp" "$META"
echo "updated $META -> $VERSION"

current=$(awk -F"'" '/^default_version/ {print $2}' "$CONTROL")
if [[ "$current" != "$MAJOR_MINOR" ]]; then
  sed -i.bak -E "s/^(default_version[[:space:]]*=[[:space:]]*)'[^']*'/\1'$MAJOR_MINOR'/" "$CONTROL"
  rm -f "$CONTROL.bak"
  echo "updated $CONTROL default_version: $current -> $MAJOR_MINOR"
  echo "note: SQL file sql/pg_stat_ch--$MAJOR_MINOR.sql and a migration sql/pg_stat_ch--$current--$MAJOR_MINOR.sql may be needed"
else
  echo "$CONTROL default_version already $MAJOR_MINOR (no change)"
fi
