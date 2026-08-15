#!/usr/bin/env bash
# Shared helpers for MySQL crash harness (Linux/WSL). ASCII-only.

set -euo pipefail

ce_stream_repo_root() {
    if [[ -n "${CE_STREAM_REPO:-}" ]]; then
        if [[ ! -f "$CE_STREAM_REPO/Cargo.toml" ]]; then
            echo "CE_STREAM_REPO missing Cargo.toml: $CE_STREAM_REPO" >&2
            exit 1
        fi
        printf '%s' "$CE_STREAM_REPO"
        return
    fi
    local script_dir repo
    script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
    repo=$(cd "$script_dir/../../.." && pwd)
    if [[ -f "$repo/Cargo.toml" ]]; then
        printf '%s' "$repo"
        return
    fi
    echo "Set CE_STREAM_REPO to your ce-stream clone root." >&2
    exit 1
}

get_capture_password() {
    if [[ -n "${CE_STREAM_PASSWORD:-}" ]]; then
        printf '%s' "$CE_STREAM_PASSWORD"
        return
    fi
    echo "Set CE_STREAM_PASSWORD to the ce_stream MySQL user password." >&2
    exit 1
}

write_crash_harness_toml() {
    local path=$1 sink_url=$2 checkpoint_path=$3
    local delivery_unit=${4:-row} server_id=${5:-19301} queue_capacity=${6:-64}
    local mysql_port=${MYSQL_PORT:-3307}
    local mysql_host=${MYSQL_HOST:-127.0.0.1}
    local mysql_tls=${MYSQL_TLS:-false}
    local password
    password=$(get_capture_password)
    cat >"$path" <<EOF
[source]
adapter = "mysql"
source_id = "mysql://${mysql_host}:${mysql_port}/ce-stream-crash-harness"
host = "${mysql_host}"
port = ${mysql_port}
user = "ce_stream"
password = "${password}"
server_id = ${server_id}
tls = ${mysql_tls}
payload_mode = "full"
delivery_unit = "${delivery_unit}"
queue_capacity = ${queue_capacity}
include_tables = ["ce_stream_spike.t1"]

[checkpoint]
path = "${checkpoint_path}"

[sink]
kind = "http"
url = "${sink_url}"
format = "json"
EOF
}

mysql_admin_args() {
    local defaults="${MYSQL_DEFAULTS_FILE:-}"
    if [[ -z "$defaults" ]]; then
        echo "Set MYSQL_DEFAULTS_FILE to a mysql client defaults file." >&2
        exit 1
    fi
    printf '%s' "--defaults-extra-file=${defaults} -h ${MYSQL_HOST:-127.0.0.1} -P ${MYSQL_PORT:-3307}"
}

get_sink_stats() {
    local base_url=$1
    curl -fsS "${base_url%/}/stats"
}

reset_sink_stats() {
    local base_url=$1
    curl -fsS -X POST "${base_url%/}/reset" >/dev/null
}

wait_sink_count() {
    local base_url=$1 expected=$2 timeout_sec=${3:-120}
    local deadline=$((SECONDS + timeout_sec)) last=0
    while (( SECONDS < deadline )); do
        last=$(get_sink_stats "$base_url" | sed -n 's/.*"received":\([0-9]*\).*/\1/p')
        if [[ -z "$last" ]]; then last=0; fi
        if (( last >= expected )); then
            get_sink_stats "$base_url"
            return 0
        fi
        sleep 0.2
    done
    echo "timeout waiting for sink count>=${expected} (last=${last})" >&2
    return 1
}

get_checkpoint_gtid() {
    local path=$1
    if [[ ! -f "$path" ]]; then
        return 0
    fi
    python3 - "$path" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    doc = json.load(f)
print(doc.get("payload", {}).get("gtid", "") or "")
PY
}

wait_checkpoint_gtid() {
    local path=$1 timeout_sec=${2:-60}
    local deadline=$((SECONDS + timeout_sec)) gtid=""
    while (( SECONDS < deadline )); do
        gtid=$(get_checkpoint_gtid "$path")
        if [[ -n "$gtid" ]]; then
            printf '%s' "$gtid"
            return 0
        fi
        sleep 0.25
    done
    echo "timeout waiting for checkpoint gtid at $path" >&2
    return 1
}

invoke_mysql_sql() {
    local label=$1 sql=$2
    local tmp
    tmp=$(mktemp "/tmp/ce-stream-crash-${label}.XXXXXX.sql")
    printf '%s' "$sql" >"$tmp"
    # shellcheck disable=SC2086
    mysql $(mysql_admin_args) <"$tmp"
    rm -f "$tmp"
}

invoke_mysql_scalar() {
    local sql=$1
    # shellcheck disable=SC2086
    mysql $(mysql_admin_args) -N -B -e "$sql"
}

assert_capture_gates() {
    local meta format log_bin
    meta=$(invoke_mysql_scalar "SELECT @@GLOBAL.binlog_row_metadata;")
    format=$(invoke_mysql_scalar "SELECT @@GLOBAL.binlog_format;")
    log_bin=$(invoke_mysql_scalar "SELECT @@GLOBAL.log_bin;")
    echo "binlog_row_metadata=${meta} binlog_format=${format} log_bin=${log_bin}"
    if [[ "$meta" != "FULL" ]]; then
        echo "binlog_row_metadata must be FULL. Got: $meta" >&2
        exit 1
    fi
    if [[ "$format" != "ROW" ]]; then
        echo "binlog_format must be ROW. Got: $format" >&2
        exit 1
    fi
    if [[ "$log_bin" != "1" ]]; then
        echo "log_bin must be ON. Got: $log_bin" >&2
        exit 1
    fi
}

install_ce_stream_lab() {
    local repo pw pw_sql sql
    repo=$(ce_stream_repo_root)
    pw=$(get_capture_password)
    pw_sql=${pw//\'/\'\'}
    if [[ ! -f "$repo/scripts/spike-setup.sql" ]]; then
        echo "missing $repo/scripts/spike-setup.sql" >&2
        exit 1
    fi
    sql=$(sed "s/CHANGE_ME/${pw_sql}/g" "$repo/scripts/spike-setup.sql")
    invoke_mysql_sql "setup-lab" "$sql"
}

new_multi_row_txn_sql() {
    local prefix=$1 rows=${2:-3}
    local i name namesql
    echo "START TRANSACTION;"
    for ((i = 1; i <= rows; i++)); do
        name="${prefix}-${i}"
        namesql=${name//\'/\'\'}
        echo "INSERT INTO ce_stream_spike.t1(name) VALUES ('${namesql}');"
    done
    echo "COMMIT;"
}

stop_pid_safe() {
    local pid=${1:-}
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        sleep 0.5
        kill -KILL "$pid" 2>/dev/null || true
    fi
}

wait_mysql_ready() {
    local timeout_sec=${1:-120}
    local deadline=$((SECONDS + timeout_sec))
    while (( SECONDS < deadline )); do
        if invoke_mysql_scalar "SELECT 1;" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    echo "MySQL not ready after ${timeout_sec}s" >&2
    return 1
}

ensure_lf_scripts() {
    local f
    for f in "$@"; do
        if [[ -f "$f" ]]; then
            sed -i 's/\r$//' "$f"
        fi
    done
}

if [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi
