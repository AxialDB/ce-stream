#!/usr/bin/env bash
# Build and install ce-stream Linux binaries to ~/.local/bin (WSL/dev lab).
# Usage: bash scripts/crash-harness/mysql/install-linux.sh

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"
ensure_lf_scripts "$SCRIPT_DIR/_common.sh" "$0"

CE_STREAM_REPO=$(ce_stream_repo_root)
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/.cache/ce-stream-target}"
INSTALL_DIR="${CE_STREAM_INSTALL_DIR:-$HOME/.local/bin}"

echo "Building ce-stream in ${CE_STREAM_REPO} (target: ${CARGO_TARGET_DIR}) ..."
pushd "$CE_STREAM_REPO" >/dev/null
cargo build -p ce-stream-perf-sink -p ce-stream-cli --release
popd >/dev/null

mkdir -p "$INSTALL_DIR"
install -m 755 "${CARGO_TARGET_DIR}/release/ce-stream" "${CARGO_TARGET_DIR}/release/ce-stream-perf-sink" "$INSTALL_DIR/"

echo "Installed:"
echo "  ${INSTALL_DIR}/ce-stream"
echo "  ${INSTALL_DIR}/ce-stream-perf-sink"
"${INSTALL_DIR}/ce-stream" --help | head -3
