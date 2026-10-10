# PostgreSQL source — design

**Status:** Planning. No crate yet. Target release **v0.5.0**. Written 2026-10-07 against the PostgreSQL 18 docs, ce-stream v0.4.1, and `pgwire-replication` 0.4.1.

**Related:** [`planning.md`](planning.md) (Phase 6) · MongoDB source [`issues/5.md`](issues/5.md) · embedding [`library.md`](library.md) · Avro [`avro.md`](avro.md)

`ce-stream-postgres` is the third `ChangeSource`. Same product shape as `ce-stream-mysql` and `ce-stream-mongo`: one ordered reader, one `CommittedTransaction` per source commit, no Kafka, no Debezium, no triggers. The log is different. PostgreSQL has no binlog; it has logical decoding.

## Decisions

- **Capture is logical decoding through the built-in `pgoutput` plugin**, over the streaming replication protocol.
- **Do not parse WAL bytes.** The write-ahead log is physical redo (block addresses, heap headers, HOT, TOAST pointers) and its layout changes between major versions. Logical decoding is the supported API that turns that redo into tuples ([concepts](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html)).
- **Do not require an output plugin install.** `wal2json` and `decoderbufs` are a shared library on the user's server. PostgreSQL 18 limits every user to `output_plugin_libraries`, default `'pgoutput, test_decoding'` ([replication settings](https://www.postgresql.org/docs/18/runtime-config-replication.html)). Managed services generally offer `pgoutput` only.
- **One persistent slot per source database; one temporary slot per seed.** The persistent slot carries the stream. Each table's initial copy uses the snapshot exported by a short-lived `TEMPORARY` slot, because the persistent slot's snapshot is gone by the time a later table is added. See [Seed helper](#seed-helper).
- **The publication decides what the server sends.** `pgoutput` sends only published tables, from the commit of `ALTER PUBLICATION … ADD TABLE` onward. `SourceConfig.include_tables` still filters client-side, for parity with the other sources. An embedder that seeds tables while the stream runs should leave the include list open and pick rows per table by seed position; a client-side filter changed at seed time can race the seed.
- **Streaming of in-progress transactions stays off.** `proto_version '1'`. Row messages for one transaction arrive between `Begin` and `Commit`, then the source emits one commit.
- **Two-phase stays off.** A prepared transaction is decoded as an ordinary commit when `COMMIT PREPARED` runs. The spike confirms that.
- **v1 reads the primary.** Logical slots on a standby exist since PostgreSQL 16 but are invalidated when the primary removes catalog rows the standby needs. Standby capture and failover slots (17) are later.
- **Values pass through as `pgoutput` text.** The crate does not parse values into numbers or dates and does not need a type table. Typing is the consumer's job. See [Values](#values).
- **Before-images are what `pgoutput` sends.** Require a primary key. Do not require `REPLICA IDENTITY FULL`. A consumer that needs the full old row keeps its own state by key, or the DBA sets `FULL`.
- **The capture role does not own or write tables.** It creates its own slots. A DBA creates the publication and adds tables.
- **Schema changes and `TRUNCATE` become control events.** `pgoutput` does not emit DDL SQL ([restrictions](https://www.postgresql.org/docs/18/logical-replication-restrictions.html)). It sends a `Relation` message when a table's definition changes, and a `Truncate` message.

## Prerequisites

Checked 2026-10-07 against the PostgreSQL 18 docs ([publication](https://www.postgresql.org/docs/current/logical-replication-publication.html), [replication protocol](https://www.postgresql.org/docs/18/protocol-replication.html), [logical protocol](https://www.postgresql.org/docs/18/protocol-logical-replication.html), [message formats](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html), [security](https://www.postgresql.org/docs/18/logical-replication-security.html)), the [versioning page](https://www.postgresql.org/support/versioning/), and the official `postgres` image tags on Docker Hub.

### Source server

| Need | Requirement | Why |
|------|-------------|-----|
| Server version | **16 or newer** | 16 is the floor we test (supported until 2028-11-09). 14 reaches end of life 2026-11-12. 15 works for logical replication; untested, so unsupported. |
| `wal_level` | `logical` | Lower levels do not emit logical changes. Changing it restarts the server. |
| Database encoding | `UTF8` | Logical decoding emits text in the database encoding. The crate does not convert. |
| Slots and senders | `max_replication_slots` and `max_wal_senders` leave room for **two** per source database | One persistent slot, plus one temporary slot while a seed runs. |
| WAL retention bound | `max_slot_wal_keep_size` set to something the disk can hold | A slot that is not advanced keeps WAL forever. Past this limit the slot is invalidated and the consumer must reseed. Same class of failure as a purged MySQL binlog. The gate warns when it is `-1` (unlimited). |
| Publication | One publication listing each source table, publishing `insert`, `update`, `delete`, and `truncate` (the default) | A publication without `delete` drops deletes without an error. |
| Publication filters | No row filter (`WHERE`) and no column list on captured tables | A row filter turns some updates into inserts or deletes, so a seed would not match the stream. Column lists are a later option. |
| Primary key | Every captured table | Keys for update and delete. |
| Replica identity | Not `NOTHING`, and not `DEFAULT` on a table with no primary key | `UPDATE` and `DELETE` error on the publisher otherwise. `FULL` is allowed, not required. |
| Capture role | Can open a replication connection, `LOGIN`, `SELECT` on each published table | Slots, the stream, and the seed copy. On RDS the right is the `rds_replication` role, not the `REPLICATION` attribute, so the gate opens a replication connection instead of reading `pg_roles`. |
| `pg_hba.conf` | A replication line for that role | Separate from the SQL line. |
| Row security | Role has `BYPASSRLS`, or the session sets `row_security=off` | Otherwise a table owner's policy runs inside logical decoding. `row_security=off` halts replication if a policy appears later, which is the fail-closed behavior we want. |

On PostgreSQL 18, leave `output_plugin_libraries` at its default. 16 and 17 do not have the setting; `pgoutput` is built in.

Managed services (RDS, Aurora, Cloud SQL, Azure, Neon, Supabase) expose `pgoutput` once logical replication is enabled. Their slot limits, role names, and parameter names differ. v1 tests self-hosted only; a managed pass is a later step.

### Client crates

| Crate | Version | License | Role |
|-------|---------|---------|------|
| `pgwire-replication` | 0.4.1 (crates.io, 2026-09-21), Rust 1.88 | Apache-2.0 OR MIT | Replication connection: TLS, SCRAM, `START_REPLICATION`, keepalives, feedback. **Vendored and patched.** |
| `tokio-postgres` | 0.7.18 (crates.io, 2026-06-12), Rust 1.85 | MIT OR Apache-2.0 | SQL connection: gates and the seed copy. Needs a rustls connector; check its license when it is added. |

`pgwire-replication` 0.4.1 opens a `replication=database` connection and runs `START_REPLICATION … LOGICAL … (proto_version '1', publication_names '…', messages 'true')`. It has no `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT`, or `DROP_REPLICATION_SLOT`. `tokio-postgres` has no replication mode. Slot creation with an exported snapshot must run on a replication connection, so one crate has to grow it.

Vendor `pgwire-replication` under `vendor/pgwire-replication` with a `PATCHES.md`, the same way as `vendor/mysql-binlog-connector-rust`. The patch adds a replication session that runs `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT … [TEMPORARY] LOGICAL pgoutput (SNAPSHOT 'export' | 'nothing')`, and `DROP_REPLICATION_SLOT` over the crate's existing connect, TLS, and auth, and can stay idle while its snapshot is in use. Offer the patch upstream once it works.

`messages 'true'` delivers `pg_logical_emit_message` output as `Message` events. The source ignores them.

`ce-stream-postgres` sets `rust-version = "1.88"`; the rest of the workspace keeps 1.75.

### Lab

Official `postgres` image in Docker. Tags checked 2026-10-07:

| Tag | Role |
|-----|------|
| `postgres:16.15` | Floor. The only dev and test server until the release pass. |
| `postgres:18.6` | Current major. One pass before release. |

Start with `-c wal_level=logical -c max_slot_wal_keep_size=1GB`, a capture role, and a replication `pg_hba` line. PostgreSQL 18 images keep data under a versioned path (`/var/lib/postgresql/18/docker`); mount `/var/lib/postgresql`. The 16 image uses the older path.

## What the binlog gives us that the WAL does not

MySQL's binary log is a documented replication stream: GTID, table-map, row images, XID, and query events with DDL text. `ce-stream-mysql` reads it with `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL`, and `gtid_mode=ON`. Resume is a GTID set. Nothing is emitted before XID.

PostgreSQL's WAL is the redo those row images would be decoded *from*. A client that parsed it would own heap layout, TOAST, HOT updates, and a decoder per major version. `pgoutput` is the row format. The rest of this document maps it onto the envelope the other sources already use.

## Logical decoding

A logical slot is a stream of changes for **one database**, in commit order, kept across connections and crashes. One receiver reads a slot at a time. The server persists the slot position at checkpoint, so after a crash it can resend recent commits; the client dedupes.

Commands, replication connection only:

```text
IDENTIFY_SYSTEM
CREATE_REPLICATION_SLOT ce_stream_app LOGICAL pgoutput (SNAPSHOT 'nothing')
START_REPLICATION SLOT ce_stream_app LOGICAL 0/1A2B3C4 (
  proto_version '1',
  publication_names 'ce_stream'
)
```

`START_REPLICATION` starts at the requested LSN or the slot's `confirmed_flush_lsn`, whichever is greater. `pg_recvlogical` speaks the same protocol and is useful as a manual check.

### Messages

| `pgoutput` (protocol 1) | ce-stream |
|-------------------------|-----------|
| `Begin` (final LSN, commit time, xid) | Open the transaction. Emit nothing. |
| `Relation` (OID, schema, name, replica identity, columns: key flag, name, type OID, typmod) | `ControlKind::Relation` with the column list. Sent before the first change of a relation on each connection, and again after its definition changes. Cached by OID for the session. |
| `Type` (OID, schema, name) | Cached by OID. Named in errors. |
| `Insert` + new tuple | `ChangeOp::Insert` |
| `Update` + new tuple, plus key (`K`) or old tuple (`O`) when present | `ChangeOp::Update` |
| `Delete` + key or old tuple | `ChangeOp::Delete` |
| `Truncate` (relation OIDs, options) | `ControlKind::Truncated` per relation |
| `Commit` (commit LSN, end LSN, commit time) | Close and emit the commit. |
| `Origin` | Recorded on the position. |
| `Message` | Ignored. |

On PostgreSQL 15+, `pgoutput` does not send `Begin`/`Commit` for a transaction with no published changes. The slot advances on those through keepalives. See [Position and feedback](#position-and-feedback).

### Row data

`data` carries `pgoutput` text as JSON strings, SQL `NULL` as JSON `null`:

```json
{
  "op": "update",
  "after":  { "id": "42", "status": "shipped", "total": "19.90" },
  "key":    { "id": "41" },
  "unchanged": ["notes"]
}
```

- `after`: the new tuple (insert, update). Columns marked `'u'` (unchanged TOAST) are left out of `after` and listed in `unchanged`.
- `key`: the `K` tuple, or the key columns of the `O` tuple. Present on delete, and on update only when the key changed or identity is `FULL`.
- `before`: the whole `O` tuple when identity is `FULL`. Otherwise absent.
- `subject` is `schema.table`. CloudEvent extensions: `commitLsn`, `xid`, `relationOid`.
- `PayloadMode::Signal` drops the row images, as on the other sources.

What a consumer that keeps rows by key does with that:

- Update without `key`: the key is in `after`. Upsert.
- Update with `key`: the key changed. Delete the old key, upsert the new row.
- Delete: delete by `key`.
- A column in `unchanged` keeps its stored value. With no stored row for that key, the consumer cannot rebuild the row and should fail rather than write a placeholder.

### Values

Text mode in v1. The source pins the replication session's startup options so the text does not depend on server or role defaults: `-c DateStyle=ISO -c TimeZone=UTC -c IntervalStyle=iso_8601 -c extra_float_digits=1 -c bytea_output=hex`. The seed connection sets the same options, so a seeded row and a streamed row of the same value produce the same text. Binary tuples are a later option.

Text is lossless where a typed mapping is not. Unconstrained `numeric` (no precision or scale) holds up to 131072 digits before the decimal point and 16383 after, with a different scale per value, plus `NaN` and, since PostgreSQL 14, `Infinity` and `-Infinity`. A consumer mapping to a fixed decimal type (for example Arrow `Decimal128`, 38 digits, one scale per column) has to refuse, cast, or keep text. The `Relation` event carries `typmod` so it can tell: `numeric(p,s)` is `((p << 16) | s) + 4`, unconstrained is `-1`. The same applies to `infinity` dates and timestamps.

### Control events

ce-stream-core v0.4.1 has `ControlKind::{Dropped, Renamed, DatabaseDropped, Invalidated}`. PostgreSQL adds two:

| New kind | Carries | Consumer |
|----------|---------|----------|
| `Truncated` | subject, `relation_oid` | All rows of the subject are gone as of this commit. |
| `Relation` | subject, `relation_oid`, `replica_identity`, `columns: [{name, type_oid, typmod, key}]` | The definition the following rows use. Compare with what it expects. |

Both ride in `CommittedTransaction.control` and become CloudEvents `io.ce-stream.control.truncated` and `io.ce-stream.control.relation`. Avro `committed-transaction-v2` already carries control events as JSON (`control_json`), so no schema change. MySQL and MongoDB output does not change.

Unlike the MongoDB controls, these do not end capture: a truncate or an added column is normal traffic. `Relation` is emitted when the server sends it, which includes the first change of each table on every connection; consumers treat an identical definition as a no-op.

Ordering: a `Relation` for a table precedes that table's rows in the same transaction. The v0.4.1 doc comment says controls come after row events; v0.5.0 changes it to "a consumer reads all controls of a commit before applying its rows". `ControlKind` is not `#[non_exhaustive]`, so the new variants break exhaustive matches downstream. That is why this ships as 0.5.0. Add `#[non_exhaustive]` in the same release.

`DROP TABLE` and `ALTER TABLE … RENAME` are not stream messages. A rename is expected to show as a `Relation` with the new name before the next change to that table. A drop sends nothing; the table's changes stop. The spike records exactly what each case emits.

## Slot, publication, privileges

| Object | Who creates it | Name |
|--------|----------------|------|
| Persistent logical slot | The source, on first start, `SNAPSHOT 'nothing'` | Configured (`slot`), default `ce_stream_<database>`. Slot names are unique in the cluster. |
| Temporary seed slot | The seed helper, `TEMPORARY … (SNAPSHOT 'export')` | `ce_stream_seed_<pid>_<n>`. The server drops it when the session ends. |
| Publication | DBA, as a role that owns the tables or has `CREATE` on the database | Configured (`publication`), required. `ALTER PUBLICATION … ADD TABLE` before a table is seeded. |

The capture role cannot add a table it does not own. A gate failure names the publication and the missing table and prints the `ALTER PUBLICATION` to run. The source never connects as a superuser to fix it.

`IDENTIFY_SYSTEM` returns the system identifier and the timeline. The checkpoint stores both. A different identifier or timeline is a different history: fail with a distinct error, so the embedder can reseed. Never resume an LSN on another timeline.

An invalidated slot (`pg_replication_slots.wal_status = 'lost'`, or `conflicting`) is the same distinct error, the PostgreSQL counterpart of MongoDB's `ChangeStreamHistoryLost`. Recovery drops and recreates the slot and reseeds.

The source does not drop the persistent slot on its own; an unused slot keeps holding WAL. `ce-stream-cli` gets a `drop-slot` command, and the library exposes the same call.

## Position and feedback

`Begin` … changes … `Commit` is one `CommittedTransaction`. GTID fields stay empty. `position`:

```json
{
  "adapter": "postgres",
  "at":    { "commit_lsn": "0/16B3748", "end_lsn": "0/16B3770", "xid": "742", "commit_time": "2026-10-07T20:26:00.123456Z" },
  "after": { "lsn": "0/16B3770", "slot": "ce_stream_app", "timeline": 1, "system_id": "7423..." }
}
```

- Commits arrive in `commit_lsn` order. On resume the source drops a commit whose `commit_lsn` is not greater than the checkpoint's. One ordered value, not a set.
- `after` is the checkpoint payload; `after.lsn` (the commit end LSN) is where `START_REPLICATION` resumes.
- xid wraps and is never a key.

**Feedback.** Standby status updates (`write`, `flush`, `apply`) confirm an LSN only after the callback for that commit returned `Ok`, the same rule as checkpoint advance today. Confirming earlier lets the server drop WAL the consumer has not stored.

**Idle source.** Transactions with nothing published are not sent, so on a quiet table in a busy cluster no commit arrives and the slot would pin WAL. When no transaction is open and every delivered commit is acknowledged, the source confirms the keepalive's WAL end. The [spike](#spike-checklist) has to show that a keepalive's WAL end is never ahead of a commit not yet sent. If it can be, feedback only moves on commits and idle sources need another answer before release.

Because of idle feedback, the slot's `confirmed_flush_lsn` can be ahead of the stored checkpoint. That is normal on resume and not a lost-history signal; lost history shows as an invalidated slot. An embedder that needs the idle position itself (for example to decide when a seeded table has caught up) needs a progress callback in `ce-stream-core`. Not in v0.5.0 unless the spike or the first embedder needs it.

**Keepalives while the consumer is slow.** The walsender kills a connection that does not answer within `wal_sender_timeout`. `pgwire-replication` replies to keepalives on its own task while the consumer is behind; the patch must keep that.

**Memory.** The source holds one transaction in memory until `Commit`, as the MySQL reader does until XID. The server spills large ones to disk on its side (`logical_decoding_work_mem`). A transaction too large for the client is an open point shared with MySQL.

## Seed helper

A consumer seeds each table on its own, whether the stream is already running or not. Creating a slot exports a snapshot of exactly the state after which that slot's changes begin ([exported snapshots](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html)). The persistent slot's snapshot is gone, so a temporary slot gives a fresh one at a known LSN.

What the helper does:

1. **Gate** the table ([Gates](#gates)). It must already be in the publication, so the persistent stream has carried its changes since that `ALTER PUBLICATION` committed.
2. **Create the seed slot** on a new replication connection: `CREATE_REPLICATION_SLOT ce_stream_seed_… TEMPORARY LOGICAL pgoutput (SNAPSHOT 'export')`. Read `consistent_point` (call it `X`) and `snapshot_name`. Creation waits until transactions already running on the server finish, so a long open transaction holds it. Time out (`slot_create_timeout`, default 300 s) with an error naming the cause.
3. **Leave that connection idle.** The snapshot is valid only until it runs another command or closes.
4. **Copy** on a `tokio-postgres` connection with the pinned options: `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`; `SET TRANSACTION SNAPSHOT '<snapshot_name>'`; `DECLARE c NO SCROLL CURSOR FOR SELECT <columns> FROM <table>`; `FETCH 512 FROM c` until empty; `COMMIT`. Rows go to the caller in batches of 512 (`SEED_BATCH`, as in the MongoDB seed), never one `Vec`, in the same JSON shape as `after`.
5. **Close the seed connection.** The server drops the temporary slot. The copy's snapshot holds back vacuum until step 4 commits, the same cost as a long `REPEATABLE READ` seed on MySQL.
6. **Return `X`.**

What the consumer does with it: apply every stream commit for that table with `commit_lsn >= X`, and ignore earlier ones; they are in the copy. That is the per-table seed position, like `seed_cluster_time` on MongoDB. The persistent stream must be running (or the slot must exist, so it will replay) before step 2.

The boundary `commit_lsn >= consistent_point` matches how logical decoding decides whether a transaction is in a new slot's stream. The spike proves it with writes running across step 2.

If a copy cannot hold a snapshot that long, the fallback is an overlap copy: no snapshot, replay from a recorded LSN, last write wins by primary key. Do not build that until a real seed needs it.

Views, materialized views, foreign tables, and large objects cannot be published. Partitioned tables replicate from the leaf by default. v1 refuses all of those. `publish_via_partition_root` is a later option, not a silent default.

## Gates

The source runs the server and slot gates at connect (`skip_gate_check` for lab use, as on MySQL). The table gates run for each table in `include_tables` and in the seed helper, and the library exposes them so an embedder can re-run them (dropped and unpublished tables are not stream events). Each failure names the object and the fix.

| Check | How |
|-------|-----|
| Server 16+ | `server_version_num` |
| `wal_level = logical` | `SHOW wal_level` |
| Database encoding UTF8 | `pg_database.encoding` |
| Replication connection works | Open one, run `IDENTIFY_SYSTEM` |
| System id and timeline match the checkpoint | `IDENTIFY_SYSTEM` |
| Slot is `pgoutput`, not invalidated, not active elsewhere | `pg_replication_slots` |
| Free slot for a seed | `max_replication_slots` minus slots in use |
| `max_slot_wal_keep_size` | Warn if `-1` |
| Publication exists, publishes insert, update, delete, truncate | `pg_publication` |
| Table in publication, no row filter, no column list | `pg_publication_tables` (`rowfilter`, `attnames`) |
| Ordinary table, not partitioned, not a partition | `pg_class.relkind`, `relispartition` |
| Primary key, usable replica identity | `pg_index.indisprimary`, `pg_class.relreplident` |
| No generated columns in the published list | `pg_attribute.attgenerated` |
| `SELECT` on the table | `has_table_privilege` |

Generated columns: PostgreSQL 16 and 17 never publish them, and 18 needs `publish_generated_columns`. A seed would read them and the stream would not, so the seed and the stream would disagree. v1 refuses them.

## CLI

```toml
[source]
adapter = "postgres"
source_id = "postgres://127.0.0.1:5432/app"
host = "127.0.0.1"
port = 5432
user = "ce_stream"
password = "CHANGE_ME"
database = "app"
tls = true
publication = "ce_stream"
# slot = "ce_stream_app"            # default ce_stream_<database>
# seed = true copies each include_tables table, then applies the stream from its X
seed = false
delivery_unit = "transaction"
include_tables = ["public.orders"]
```

All existing sinks, `delivery_unit`, `payload_mode`, and `sink.format = "avro"` (commits with a position use `committed-transaction-v2`, as for MongoDB). A `drop-slot` command removes the persistent slot. Exact option names are settled in the crate PR.

## Set aside

| Approach | Why not |
|----------|---------|
| Physical WAL parser | Unstable redo, no public row image, a new decoder every major version. |
| `wal2json`, `decoderbufs`, a plugin we ship | A shared library on the user's server. Outside the default `output_plugin_libraries` on 18 and on most managed services. |
| `test_decoding` | Built in, but a debug text format, not a typed row protocol. |
| Triggers, audit tables, `LISTEN/NOTIFY` | Writes into the user's database, misses DDL, not a resumable log. |
| `pg_logical_slot_get_changes` in SQL | Polling. The replication protocol is the streaming form of the same slot. |
| `pg_create_logical_replication_slot` in SQL | Does not export a snapshot. |
| `pg_export_snapshot()` plus `pg_current_wal_lsn()` | Not tied to a decoding boundary; the copy and the stream could overlap or leave a gap. |
| One persistent slot per table | Each slot decodes all of the database's WAL: N tables cost N decoders and N WAL holds. |
| Debezium, Kafka Connect, Supabase `etl` | Pipeline frameworks with their own runtime. ce-stream is the library layer. |

## Spike checklist

On `postgres:16.15`, through `ce-stream-cli` with `source.adapter = "postgres"` to stdout, before the mapping is locked. Each item records what the server actually sent.

- Insert, update (key unchanged, key changed), delete, under identity `DEFAULT` and `FULL`
- A multi-statement transaction emitted as one commit; a rolled-back one not emitted
- An update that leaves a TOAST column unchanged (`'u'`)
- `TRUNCATE`; `ALTER TABLE` add, drop, retype a column; `RENAME`; `DROP TABLE`
- `ALTER PUBLICATION … ADD TABLE` while the stream runs: the first write after it arrives
- Replica identity changed to `NOTHING` while the slot is active
- `COMMIT PREPARED` with two-phase off
- Seed boundary: writes committing before, during, and after the temporary slot's creation; each appears exactly once (copy or stream)
- Slot creation held by an open transaction; the timeout fires
- Keepalive WAL end with a busy unpublished table and a busy other database: never ahead of an unsent commit; feedback lets `restart_lsn` advance
- Kill the client mid-stream, resume, duplicate commit dropped, nothing lost
- `max_slot_wal_keep_size` exceeded: the slot reports `lost`, the gate fails with the distinct error
- Startup options change the text the walsender sends (`DateStyle`, `TimeZone`, `bytea_output`)

`postgres:18.6` gets one pass of the same list before release.

## Order

One issue, one branch, reviewable commits:

1. **Core:** `ControlKind::Truncated` and `ControlKind::Relation`, `#[non_exhaustive]`, control ordering doc. MySQL and MongoDB tests green, output unchanged.
2. **Vendor** `pgwire-replication` 0.4.1 with the slot-command patch and `PATCHES.md`.
3. **`ce-stream-postgres`:** `pgoutput` decoding, transaction grouping, mapping, position and checkpoint, feedback, gates. Unit tests on captured byte fixtures.
4. **CLI** `source.adapter = "postgres"` to stdout and the existing sinks; `drop-slot`. Run the [spike checklist](#spike-checklist).
5. **Seed helper:** temporary slot, `X`, snapshot copy in batches.
6. **Live tests** (ignored by default), docs (`library.md`, `ops-e2e.md`, README), version bump, changelog, release notes. `postgres:18.6` pass.

## Tests

CI: unit tests for `pgoutput` message decoding, transaction grouping, row mapping, position serde, and the new control kinds (`cargo test -p ce-stream-core -p ce-stream-mysql -p ce-stream-mongo -p ce-stream-postgres`).

Lab (ignored by default, `postgres:16.15`): the spike checklist as tests, plus seed overlap with concurrent writes. One pass on `postgres:18.6` before release.

## Out of scope for v0.5.0

PostgreSQL older than 16, logical decoding from a standby, failover slots, streaming of in-progress transactions, two-phase decoding, partitioned tables via `publish_via_partition_root`, publication row filters and column lists, binary tuples, typed value conversion, a managed-service pass, and Avro schema changes.
