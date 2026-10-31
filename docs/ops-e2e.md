# Ops / E2E / production

**Status:** Phase 3–4 path complete (JSON HTTP E2E + continuous prod). Optional Avro: `sink.format = "avro"` ([`avro.md`](avro.md)). PostgreSQL: [below](#postgresql-prerequisites).

Happy path: MySQL 9.x **replica** -> ce-stream (continuous) -> HTTP CloudEvents (JSON default).

Also see [`delivery.md`](delivery.md) (at-least-once), [`library.md`](library.md) (embed), [`planning.md`](planning.md) (status).

## MySQL prerequisites

```sql
-- required (ce-stream validates these at connect unless --skip-gate-check)
-- log_bin=ON, binlog_format=ROW, binlog_row_image=FULL, gtid_mode=ON

-- required for named columns (hard-fail if not FULL)
SET PERSIST binlog_row_metadata = 'FULL';

-- recommended
SET PERSIST enforce_gtid_consistency = ON;
```

On startup, ce-stream runs capture gate checks via `SHOW VARIABLES`. Use `--skip-gate-check` only in lab environments.

Replication user: `REPLICATION SLAVE`, `REPLICATION CLIENT`, typically `SELECT` (`scripts/spike-setup.sql`).

## PostgreSQL prerequisites

```sql
ALTER SYSTEM SET wal_level = logical;  -- then restart the server

CREATE ROLE ce_stream LOGIN REPLICATION PASSWORD '...';
GRANT CONNECT ON DATABASE app TO ce_stream;

ALTER TABLE public.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION ce_stream FOR TABLE public.orders;
```

`pgoutput` needs no `SELECT`. `pg_hba.conf` matches a logical replication connection with an ordinary database line, not `replication`. Authentication is SCRAM-SHA-256.

```toml
[source]
adapter = "postgres"
source_id = "postgres://127.0.0.1:5432/app"
host = "127.0.0.1"
port = 5432
user = "ce_stream"
password = "..."
database = "app"
tls = true
# tls_ca = "ca.pem"             # verifies the certificate and name; without it, TLS only encrypts
slot = "ce_stream_app"
create_slot = true              # otherwise a missing slot is an error
# publication = "ce_stream"     # the default
# min_server_version = "17"     # the default
# message_prefixes = []         # logical messages to deliver
# checkpoint = "on_ok"          # or on_confirm, with delivery_unit = "transaction"
include_tables = ["public.orders"]
```

A subject is `schema.table`. The gate reports all of these at start, each with its fix:

| Object | Needs |
|--------|-------|
| Server | At least `min_server_version`, `wal_level = logical`, a free slot and WAL sender |
| Account | `REPLICATION` |
| Database | Not `SQL_ASCII` |
| Publication | Exists in the database; publishes insert, update, delete, truncate; `publish_via_partition_root = false` |
| Table | In the publication with no row filter and no column list; a primary key; `REPLICA IDENTITY FULL` (each partition too); not `UNLOGGED`, temporary, or foreign |
| Generated columns | None on 17; on 18, `publish_generated_columns = stored` |

It warns when `max_slot_wal_keep_size` is unlimited, since a stopped consumer can then fill the disk, and when `idle_replication_slot_timeout` is under an hour.

**From a standby:** `hot_standby_feedback = on`, and making a slot waits until `SELECT pg_log_standby_snapshot()` runs on the primary. A seed cut always runs on the primary.

**A stopped stream holds WAL.** Drop a slot you no longer need. Starting after the slot was lost (`HistoryLost`) means seeding again.

## Production run (continuous)

Do **not** pass `--max-events` (that flag is smoke/CI only).

```powershell
$env:CARGO_TARGET_DIR = Join-Path (Get-Location) "target"
$env:RUST_LOG = "info"
cargo run -p ce-stream-cli --release -- --config ce-stream.toml
```

Or install the binary and use `deploy/ce-stream.service` (systemd): restart on failure, durable checkpoint path, unique `server_id` per host.

### Config sketch

```toml
[source]
adapter = "mysql"
host = "replica.example"   # prefer replica, not primary
port = 3306
tls = true
server_id = 19001          # unique per ce-stream instance
payload_mode = "full"      # or "signal"
# row = one CloudEvent per message after commit; transaction = one envelope per commit
delivery_unit = "row"
queue_capacity = 64
include_tables = ["demo_perf.orders"]

[checkpoint]
path = "/var/lib/ce-stream/checkpoint.json"

[sink]
kind = "http"
url = "https://hooks.example/events"
```

### Impact notes

- Capture reads the **binlog stream** as a replica client; CPU/network cost is mostly decode + sink.
- Prefer a **replica** so primary OLTP is not coupled to sink latency (backpressure stalls the dump client).
- Tight **include-list** reduces decode/sink work; filtered tables still advance GTID without HTTP.
- Measure lag via `ce_stream::health` (`lag_ms`, `last_gtid`, `events_total`).

## Smoke E2E (one event)

```powershell
# ce-stream.toml: sink.kind=http, url=http://127.0.0.1:18080/events
# Optional: sink.format=avro (see docs/avro.md); default is json
.\scripts\e2e-http.ps1
```

Uses `--max-events 1` and a local HttpListener (emitter + catcher in one script).

## HTTP body

`Content-Type: application/cloudevents+json` — structured-mode JSON with `gtid` / `gtidset` extensions when available.

## Regression: mid-transaction crash (Gate 0)

Validates [#1](https://github.com/AxialDB/ce-stream/issues/1): kill ce-stream after the first row of a 3-row commit is delivered; checkpoint must not advance; restart must redeliver the full transaction.

**AxialDB harness** (MySQL 9.x lab, Windows or WSL):

- [`scripts/crash-harness/README.md`](../../scripts/crash-harness/README.md)
- Windows: `.\scripts\crash-harness\mysql\run-crash-harness.ps1`
- WSL Ubuntu (port 3307): `.\scripts\crash-harness\mysql\run-crash-harness.ps1 -Wsl`

Requires `binlog_row_metadata=FULL`, `CE_STREAM_PASSWORD`, `MYSQL_DEFAULTS_FILE`, and `ce-stream-perf-sink --stall-at N` (the sink hangs on the Nth POST; the harness picks N). Lab-only (not GitHub Actions). Expected result: **PASS**.

Unit tests (no MySQL): `cargo test -p ce-stream-mysql --test txn_checkpoint`.

