#!/usr/bin/env bash
# Package Linux release archive + SHA256 (native Linux; first-class, not via Windows/WSL).
# Requires scripts/release/build-release.sh VERSION first.
# Usage: bash scripts/release/package-linux.sh 0.2.0

set -euo pipefail

VERSION="${1:?usage: package-linux.sh VERSION}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
STAGING="$REPO_ROOT/releases/staging/v${VERSION}/linux-x86_64"
DIST="$REPO_ROOT/releases/dist"
ARCHIVE="$DIST/ce-stream-v${VERSION}-x86_64-unknown-linux-gnu.tar.gz"

if [ -f "$HOME/.cargo/env" ]; then
  # shellcheck disable=SC1091
  . "$HOME/.cargo/env"
fi

if [ ! -x "$STAGING/ce-stream" ] || [ ! -x "$STAGING/ce-stream-perf-sink" ]; then
  echo "Missing Linux staging at $STAGING - run: bash scripts/release/build-release.sh ${VERSION}" >&2
  exit 1
fi

mkdir -p "$DIST"
tar -czf "$ARCHIVE" -C "$STAGING" .
(
  cd "$DIST"
  sha256sum "$(basename "$ARCHIVE")" > SHA256SUMS.linux
)

echo "Linux archive: $ARCHIVE"
cat "$DIST/SHA256SUMS.linux"
