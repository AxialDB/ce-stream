#!/usr/bin/env bash
# Kill ce-stream under concurrent writers, restart it, and assert no gap.
# See README.md in this directory.
set -euo pipefail

: "${CE_STREAM_BIN:?path to the ce-stream binary}"
: "${PSQL:?path to psql}"
: "${PGPORT:?port of the server}"
: "${PGPASSWORD:?password of the superuser postgres}"
: "${CE_STREAM_PASSWORD:?password of the capture role}"
PGDATABASE=${PGDATABASE:-ce_stream_crash}
CAPTURE_USER=${CAPTURE_USER:-ce_stream}
WRITERS=${WRITERS:-4}
PER_WRITER=${PER_WRITER:-1500}
KILLS=${KILLS:-12}
export PGPASSWORD

HERE=$(cd "$(dirname "$0")" && pwd)
OUT="$HERE/out/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
P=("$PSQL" -X -q -At -h 127.0.0.1 -p "$PGPORT" -U postgres -d "$PGDATABASE")

"${P[@]}" 2>/dev/null <<'SQL'
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'crash_slot';
DROP PUBLICATION IF EXISTS crash_pub;
DROP TABLE IF EXISTS crash_rows;
CREATE TABLE crash_rows(id bigint PRIMARY KEY, who int);
ALTER TABLE crash_rows REPLICA IDENTITY FULL;
CREATE PUBLICATION crash_pub FOR TABLE crash_rows;
SQL

cat > "$OUT/conf.toml" <<EOF
[source]
adapter = "postgres"
source_id = "postgres://lab/$PGDATABASE"
host = "127.0.0.1"
port = $PGPORT
user = "$CAPTURE_USER"
password = "$CE_STREAM_PASSWORD"
database = "$PGDATABASE"
tls = false
slot = "crash_slot"
publication = "crash_pub"
create_slot = true
checkpoint = "on_confirm"
delivery_unit = "transaction"
include_tables = ["public.crash_rows"]
[checkpoint]
path = "$OUT/checkpoint.json"
[sink]
kind = "stdout"
EOF

free_slot() {
    for _ in $(seq 1 100); do
        [ "$("${P[@]}" -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'crash_slot' AND active")" = "0" ] && return
        sleep 0.1
    done
    echo "the slot is still held by the killed connection" >&2
    exit 1
}
start() { free_slot; RUST_LOG=warn "$CE_STREAM_BIN" -c "$OUT/conf.toml" >>"$OUT/raw.log" 2>&1 & PID=$!; sleep 1; }
stop() { kill -9 "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; }
ids() { grep '^{' "$OUT/raw.log" | jq -r 'select(.commit) | .commit.events[] | .data.after.id'; }

# The slot first: what is written before it is not on this stream.
start
stop

PIDS=()
for w in $(seq 1 "$WRITERS"); do
    (
        for i in $(seq 1 "$PER_WRITER"); do
            echo "INSERT INTO crash_rows(id, who) VALUES ($((w * 1000000 + i)), $w);"
        done | "${P[@]}" >/dev/null
    ) &
    PIDS+=($!)
done
for _ in $(seq 1 "$KILLS"); do
    start
    sleep "0.$((RANDOM % 9 + 1))"
    stop
done
wait "${PIDS[@]}"

WRITTEN=$((WRITERS * PER_WRITER))
start
for _ in $(seq 1 240); do
    [ "$(ids | sort -u | wc -l)" -ge "$WRITTEN" ] && break
    sleep 0.5
done
stop

ids | sort -n >"$OUT/ids"
DISTINCT=$(uniq "$OUT/ids" | wc -l)
DELIVERED=$(wc -l <"$OUT/ids")
ADJACENT=$(grep '^{' "$OUT/raw.log" | jq -r 'select(.commit) | .commit.position | [.at.lsn, .after.lsn] | @tsv' \
    | awk -F'\t' 'prev == $1 { n++ } { prev = $2 } END { print n + 0 }')
echo "written $WRITTEN; distinct delivered $DISTINCT; delivered $DELIVERED; redelivered $((DELIVERED - DISTINCT)); kills $KILLS"
echo "commits starting exactly where the one before ended: $ADJACENT"

"${P[@]}" -c "SELECT pg_drop_replication_slot('crash_slot')" >/dev/null
"${P[@]}" -c "DROP PUBLICATION crash_pub" -c "DROP TABLE crash_rows"

if [ "$DISTINCT" -ne "$WRITTEN" ]; then
    echo "FAIL: a gap. Artifacts in $OUT" >&2
    exit 1
fi
echo "PASS"
