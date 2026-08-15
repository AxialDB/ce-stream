#!/usr/bin/env bash
# Build Linux release binaries for ce-stream.
# Usage: bash scripts/release/build-release.sh 0.2.0

set -euo pipefail

VERSION="${1:?usage: build-release.sh VERSION}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
STAGING="$REPO_ROOT/releases/staging/v${VERSION}/linux-x86_64"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/.cache/ce-stream-target}"

if [ -f "$HOME/.cargo/env" ]; then
  # shellcheck disable=SC1091
  . "$HOME/.cargo/env"
fi

cd "$REPO_ROOT"
echo "Building ce-stream-cli and ce-stream-perf-sink (release)..."
cargo build -p ce-stream-cli -p ce-stream-perf-sink --release

mkdir -p "$STAGING"
install -m 755 "$CARGO_TARGET_DIR/release/ce-stream" "$STAGING/"
install -m 755 "$CARGO_TARGET_DIR/release/ce-stream-perf-sink" "$STAGING/"

cat >"$STAGING/VERSION" <<EOF
ce-stream v${VERSION}
target: x86_64-unknown-linux-gnu
built: $(date -Iseconds)
EOF

echo "Linux binaries staged at: $STAGING"
