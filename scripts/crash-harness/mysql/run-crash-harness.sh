#!/usr/bin/env bash
# Gate 0 crash harness for Linux/WSL MySQL (default port 3307).
# Run from ce-stream repo. ASCII-only.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"
ensure_lf_scripts "$SCRIPT_DIR/_common.sh" "$0"

SETUP_LAB=0
SKIP_BUILD=0
RESTART_MYSQL=0
DELIVERY_UNIT="row"
TXN_ROWS=3
LISTEN_PORT=18082
SERVER_ID=19301

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3307}"
MYSQL_TLS="${MYSQL_TLS:-false}"
MYSQL_DEFAULTS_FILE="${MYSQL_DEFAULTS_FILE:-$HOME/.my.cnf}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --setup-lab) SETUP_LAB=1; shift ;;
        --skip-build) SKIP_BUILD=1; shift ;;
        --restart-mysql) RESTART_MYSQL=1; shift ;;
        --delivery-unit) DELIVERY_UNIT="$2"; shift 2 ;;
        --txn-rows) TXN_ROWS="$2"; shift 2 ;;
        --listen-port) LISTEN_PORT="$2"; shift 2 ;;
        --server-id) SERVER_ID="$2"; shift 2 ;;
        --mysql-port) MYSQL_PORT="$2"; shift 2 ;;
        --mysql-defaults) MYSQL_DEFAULTS_FILE="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--setup-lab] [--skip-build] [--restart-mysql] [--delivery-unit row|transaction]"
            exit 0
            ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

CE_STREAM_REPO=$(ce_stream_repo_root)
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/.cache/ce-stream-target}"
export RUST_LOG="${RUST_LOG:-info}"

if [[ "$RESTART_MYSQL" -eq 1 ]]; then
    echo "Restarting MySQL (WSL service) ..."
    sudo service mysql restart
    wait_mysql_ready 180
fi

assert_capture_gates

if [[ "$SETUP_LAB" -eq 1 ]]; then
    echo "Installing ce_stream_spike lab schema (scripts/spike-setup.sql) ..."
    install_ce_stream_lab
fi

OUT_DIR="$SCRIPT_DIR/out"
mkdir -p "$OUT_DIR"
RUN_ID=$(date +%Y%m%d-%H%M%S)
RUN_DIR="$OUT_DIR/$RUN_ID"
mkdir -p "$RUN_DIR"

CHECKPOINT_PATH="$RUN_DIR/checkpoint.json"
TOML_PATH="$RUN_DIR/ce-stream.toml"
SINK_URL="http://127.0.0.1:${LISTEN_PORT}/events"
BASE_URL="http://127.0.0.1:${LISTEN_PORT}"

SINK_BIN="${CE_STREAM_SINK_BIN:-$HOME/.local/bin/ce-stream-perf-sink}"
CLI_BIN="${CE_STREAM_BIN:-$HOME/.local/bin/ce-stream}"
if [[ ! -x "$SINK_BIN" ]]; then SINK_BIN="${CARGO_TARGET_DIR}/release/ce-stream-perf-sink"; fi
if [[ ! -x "$CLI_BIN" ]]; then CLI_BIN="${CARGO_TARGET_DIR}/release/ce-stream"; fi

if [[ "$SKIP_BUILD" -eq 0 ]]; then
    echo "Building ce-stream in ${CE_STREAM_REPO} ..."
    pushd "$CE_STREAM_REPO" >/dev/null
    cargo build -p ce-stream-perf-sink -p ce-stream-cli --release
    popd >/dev/null
    SINK_BIN="${CARGO_TARGET_DIR}/release/ce-stream-perf-sink"
    CLI_BIN="${CARGO_TARGET_DIR}/release/ce-stream"
fi

if [[ ! -x "$SINK_BIN" || ! -x "$CLI_BIN" ]]; then
    echo "Missing ce-stream binaries. Run install-linux.sh or omit --skip-build." >&2
    exit 1
fi

write_crash_harness_toml "$TOML_PATH" "$SINK_URL" "$CHECKPOINT_PATH" "$DELIVERY_UNIT" "$SERVER_ID"

SINK_LOG="$RUN_DIR/sink.log"
SINK_ERR="$RUN_DIR/sink.err"
CLI_LOG="$RUN_DIR/cli-phase1.log"
CLI_ERR="$RUN_DIR/cli-phase1.err"
CLI_LOG2="$RUN_DIR/cli-phase2.log"
CLI_ERR2="$RUN_DIR/cli-phase2.err"

SINK_PID=""
CLI_PID=""

cleanup() {
    stop_pid_safe "$CLI_PID"
    stop_pid_safe "$SINK_PID"
}
trap cleanup EXIT

start_perf_sink() {
    local stall_after=${1:-0}
    # shellcheck disable=SC2086
    "$SINK_BIN" --listen "127.0.0.1:${LISTEN_PORT}" --delay-ms 0 --stall-after "$stall_after" \
        >"$SINK_LOG" 2>"$SINK_ERR" &
    SINK_PID=$!
}

start_capture() {
    local out_log=$1 err_log=$2
    # shellcheck disable=SC2086
    "$CLI_BIN" --config "$TOML_PATH" >"$out_log" 2>"$err_log" &
    CLI_PID=$!
}

echo "Target: MySQL ${MYSQL_HOST}:${MYSQL_PORT} tls=${MYSQL_TLS}"
echo "Starting perf sink on ${BASE_URL} ..."
start_perf_sink 0
sleep 1
reset_sink_stats "$BASE_URL"
rm -f "$CHECKPOINT_PATH"

echo "Phase 1: warmup single-row commit (establishes durable checkpoint) ..."
start_capture "$CLI_LOG" "$CLI_ERR"
sleep 3
if ! kill -0 "$CLI_PID" 2>/dev/null; then
    cat "$CLI_ERR" >&2 || true
    echo "ce-stream exited during warmup start" >&2
    exit 1
fi

WARMUP_TAG="warmup-${RUN_ID}"
invoke_mysql_sql "warmup-insert" "INSERT INTO ce_stream_spike.t1(name) VALUES ('${WARMUP_TAG}');"
wait_sink_count "$BASE_URL" 1 60 >/dev/null
WARMUP_GTID=$(wait_checkpoint_gtid "$CHECKPOINT_PATH" 60)
echo "Warmup checkpoint gtid: ${WARMUP_GTID}"

reset_sink_stats "$BASE_URL"

echo "Phase 2: ${TXN_ROWS}-row txn, stall sink after row 1, kill ce-stream mid-fanout ..."
stop_pid_safe "$SINK_PID"
SINK_PID=""
start_perf_sink 1
sleep 1
reset_sink_stats "$BASE_URL"

CRASH_TAG="crash-${RUN_ID}"
invoke_mysql_sql "crash-txn" "$(new_multi_row_txn_sql "$CRASH_TAG" "$TXN_ROWS")"
wait_sink_count "$BASE_URL" 1 60 >/dev/null
PARTIAL_STATS=$(get_sink_stats "$BASE_URL")
sleep 0.1
stop_pid_safe "$CLI_PID"
CLI_PID=""
stop_pid_safe "$SINK_PID"
SINK_PID=""

GTID_AFTER_KILL=$(get_checkpoint_gtid "$CHECKPOINT_PATH")
if [[ "$GTID_AFTER_KILL" != "$WARMUP_GTID" ]]; then
    echo "checkpoint advanced after partial delivery (warmup=${WARMUP_GTID}, after_kill=${GTID_AFTER_KILL})" >&2
    exit 1
fi
echo "Checkpoint unchanged after kill (expected)."

PARTIAL_COUNT=$(printf '%s' "$PARTIAL_STATS" | sed -n 's/.*"received":\([0-9]*\).*/\1/p')
if [[ "${PARTIAL_COUNT:-0}" -ne 1 ]]; then
    echo "expected 1 partial delivery before kill, got ${PARTIAL_COUNT:-0}" >&2
    exit 1
fi

echo "Phase 3: restart ce-stream, expect full txn redelivery ..."
start_perf_sink 0
sleep 1
reset_sink_stats "$BASE_URL"
start_capture "$CLI_LOG2" "$CLI_ERR2"
sleep 5
if ! kill -0 "$CLI_PID" 2>/dev/null; then
    AFTER_EXIT=$(get_sink_stats "$BASE_URL")
    AFTER_COUNT=$(printf '%s' "$AFTER_EXIT" | sed -n 's/.*"received":\([0-9]*\).*/\1/p')
    if [[ "$DELIVERY_UNIT" == "row" && "${AFTER_COUNT:-0}" -lt "$TXN_ROWS" ]]; then
        cat "$CLI_ERR2" >&2 || true
        echo "ce-stream exited during restart before delivering ${TXN_ROWS} rows (got ${AFTER_COUNT:-0})" >&2
        exit 1
    fi
fi

if [[ "$DELIVERY_UNIT" == "row" ]]; then
    wait_sink_count "$BASE_URL" "$TXN_ROWS" 90 >/dev/null
    REDELIVERED=$(get_sink_stats "$BASE_URL" | sed -n 's/.*"received":\([0-9]*\).*/\1/p')
    if [[ "${REDELIVERED:-0}" -lt "$TXN_ROWS" ]]; then
        echo "expected >=${TXN_ROWS} row redeliveries, got ${REDELIVERED:-0}" >&2
        exit 1
    fi
    echo "Redelivered ${REDELIVERED} row CloudEvents (>= ${TXN_ROWS})."
else
    wait_sink_count "$BASE_URL" 1 90 >/dev/null
    echo "Redelivered 1 committed-transaction envelope."
fi

FINAL_GTID=$(wait_checkpoint_gtid "$CHECKPOINT_PATH" 60)
if [[ "$FINAL_GTID" == "$WARMUP_GTID" ]]; then
    echo "checkpoint did not advance after successful full redelivery" >&2
    exit 1
fi
echo "Final checkpoint gtid: ${FINAL_GTID}"

SUMMARY_PATH="$RUN_DIR/summary.json"
cat >"$SUMMARY_PATH" <<EOF
{
  "run_id": "${RUN_ID}",
  "target": "linux",
  "mysql_host": "${MYSQL_HOST}",
  "mysql_port": ${MYSQL_PORT},
  "delivery_unit": "${DELIVERY_UNIT}",
  "txn_rows": ${TXN_ROWS},
  "warmup_gtid": "${WARMUP_GTID}",
  "gtid_after_kill": "${GTID_AFTER_KILL}",
  "final_gtid": "${FINAL_GTID}",
  "partial_deliveries": 1,
  "redelivery_mode": "${DELIVERY_UNIT}",
  "pass": true,
  "artifacts": "${RUN_DIR}"
}
EOF

echo ""
echo "ce-stream crash harness PASS"
echo "Artifacts: ${RUN_DIR}"
