# SQLite source — design

**Status:** Planning. No crate yet. Unscheduled, after the PostgreSQL source. Written 2026-10-07 against the SQLite 3.53.4 docs (released 2026-07-24), `rusqlite` 0.40.2, and ce-stream v0.4.1.

**Related:** [`planning.md`](planning.md) (Phase 6) · PostgreSQL source [`postgres-design.md`](postgres-design.md) · embedding [`library.md`](library.md)

`ce-stream-sqlite` is a later `ChangeSource`. Same product shape: one ordered reader, one `CommittedTransaction` per source commit, nothing emitted before commit, no Kafka, no Debezium, no Kafka Connect. The log is different. SQLite has no binlog and no logical slot. The supported row API is a callback on the connection that writes.

The PostgreSQL source targets v0.5.0. This document does not. It takes no release number.

## Decisions

- **v1 is a library embed.** The application writes through a connection wrapper in the same process. That wrapper registers the hooks. A CLI that opens the file from another process is not v1.
- **Capture is `sqlite3_preupdate_hook` plus `sqlite3_commit_hook` and `sqlite3_rollback_hook` on that connection.** The preupdate hook copies old and new values out before it returns. The wrapper emits one `CommittedTransaction` only after the SQL call that committed has returned success and the connection is back in autocommit.
- **The consumer runs on the write path, after that return, before the wrapper's method returns to the application.** The commit hook itself returns zero and does not call the consumer. A non-zero commit hook turns the commit into a rollback, and the hook is forbidden from touching the connection, so it cannot record a position and it must not deliver an event.
- **Hooks are not a log.** A crash after SQLite has committed and before the callback returns `Ok` loses the event. v1 does not resume across a restart. Recovery is a new seed. See [Crash](#crash).
- **Do not parse the WAL file.** A WAL frame is a revised database page plus a commit marker, salt, and checksum ([file format](https://www.sqlite.org/fileformat2.html#walformat)). There is no public row image in it. `sqlite3_wal_hook` reports a page count after a WAL commit. It is not a logical API.
- **Do not use the session extension as the stream.** A session object is an in-memory recording on one connection. The application extracts a changeset blob when it chooses. SQLite does not append that blob to a durable log. The blob also coalesces repeated edits of one row. See [Set aside](#set-aside).
- **Do not install triggers or a shadow table in v1.** That writes into the user's database. Hooks cannot see other processes; a shadow table would be how a sidecar learned anything. v1 does not build that path.
- **One wrapped connection.** Other connections in the process, and every other process, are invisible to the hooks. `PRAGMA data_version` can show that some other connection committed. It carries no row image. That change is a distinct error. The embedder reseeds.
- **Values pass through as SQLite's text.** `NULL`, `INTEGER`, `REAL`, and `TEXT` use the text SQLite already produces. `BLOB` has no text form; v1 hex-encodes the bytes. The crate does not parse numbers or dates. See [Values](#values).
- **v1 adds no `ControlKind`.** Core v0.4.1 has `Dropped`, `Renamed`, `DatabaseDropped`, and `Invalidated`. A silent full-table delete would need `Truncated`, which this tree does not have. The spike blocks release if the preupdate hook is silent for that delete. See [Control events](#control-events).
- **Client-side include list.** The hook fires for every real table on the connection. The wrapper keeps the included ones. Empty `include_tables` at start means all tables. After that, removing the last table or replacing the list with an empty one means no row events, matching `IncludeList`.

## Prerequisites

Checked 2026-10-07. SQLite docs are the 3.53.4 set: the [download page](https://www.sqlite.org/download.html) lists `sqlite-src-3530400` as 3.53.4, released 2026-07-24 ([release log](https://www.sqlite.org/releaselog/3_53_4.html)). Pages cited below were read from sqlite.org on that date. `rusqlite` 0.40.2 was read from the `v0.40.2` tag and from crates.io (published 2026-08-08).

### What each callback is

| API | In the 3.53.4 docs | Build flag | Fires with |
|-----|--------------------|------------|------------|
| `sqlite3_update_hook` | [c3ref](https://www.sqlite.org/c3ref/update_hook.html), default API | none | `SQLITE_INSERT` / `UPDATE` / `DELETE`, database name, table name, rowid. Rowid tables only. No column values. |
| `sqlite3_preupdate_hook` | [c3ref](https://www.sqlite.org/c3ref/preupdate_hook.html) | `SQLITE_ENABLE_PREUPDATE_HOOK` | Before the change, on a real table. Old and new `sqlite3_value`s via `sqlite3_preupdate_old` / `sqlite3_preupdate_new`. `WITHOUT ROWID` included; the rowid arguments are undefined for it. Incremental blob writes are distinguished with [`sqlite3_preupdate_blobwrite`](https://www.sqlite.org/c3ref/preupdate_blobwrite.html). |
| `sqlite3_commit_hook`, `sqlite3_rollback_hook` | [c3ref](https://www.sqlite.org/c3ref/commit_hook.html), default API | none | A commit or a rollback on that connection. Non-zero from the commit hook converts the commit into a rollback. |
| `sqlite3_wal_hook` | [c3ref](https://www.sqlite.org/c3ref/wal_hook.html) | none | After a commit in WAL mode: database name and the number of pages in the WAL. No rows. Registering it replaces auto-checkpoint. |

Each function takes the `sqlite3*` to register on. The docs describe that connection. They do not describe a callback for some other connection's writes. `PRAGMA data_version` is the separate, imageless signal that another connection has committed ([pragma](https://www.sqlite.org/pragma.html#pragma_data_version), added in 3.8.8, 2015-01-16, per the [changelog](https://www.sqlite.org/changes.html)).

Versions I could lock, and versions I could not:

- The session extension has been in the amalgamation since 3.13.0 (2016-05-18) and is off unless the build sets `SQLITE_ENABLE_SESSION` and `SQLITE_ENABLE_PREUPDATE_HOOK` ([sessionintro](https://www.sqlite.org/sessionintro.html)). The 3.13.0 [release note](https://www.sqlite.org/releaselog/3_13_0.html) says the session extension was merged. It does not name `sqlite3_preupdate_hook` by itself. I did not find a separate introduction line for the preupdate hook.
- WAL mode dates to 3.7.0 (2010-07-21) ([wal.html](https://www.sqlite.org/wal.html), [file format](https://www.sqlite.org/fileformat2.html)). `sqlite3_wal_hook` is the callback that mode uses. I did not fetch a 3.7.0 release note that names the function.
- `sqlite3_update_hook` and `sqlite3_commit_hook` are in the default 3.53.4 API, with no compile flag. The [3.3.6 release notes](https://www.sqlite.org/releaselog/3_3_6.html) do not add them. The fetched changelog text did not include their original addition. First shipped version: not confirmed here.
- `WITHOUT ROWID` preupdate behavior is in the current preupdate page (rowid arguments undefined). Session support for `WITHOUT ROWID` starts at 3.17.0, per sessionintro. v1 does not use the session extension for that.

### File and connection

| Need | Requirement | Why |
|------|-------------|-----|
| Who writes | The application, through the wrapper, on one connection | Hooks see that connection only. |
| Who reads the stream | The same process | Another process cannot register those hooks. |
| Journal | `DELETE`, `TRUNCATE`, `PERSIST`, `MEMORY`, or `WAL` | Hooks do not depend on WAL. `OFF` makes `ROLLBACK` undefined ([transactions](https://www.sqlite.org/lang_transaction.html)). The gate refuses `OFF`. |
| Encoding | `UTF-8` | The text accessors return UTF-8. The gate reads `PRAGMA encoding`. |
| Preupdate enabled | `PRAGMA compile_options` includes `ENABLE_PREUPDATE_HOOK` | The bundled build turns the flag on. A system `libsqlite3` usually does not. |
| Include list | Real tables in `main` | `temp`, `ATTACH`, views, virtual tables, and shadow tables are out. |
| Generated columns | None on a captured table | `PRAGMA table_info` omits them; `PRAGMA table_xinfo` reports them (`hidden` 2 or 3). The preupdate column count is not documented against that list. v1 refuses the table. |
| Other writers | None for the life of the capture | A `data_version` change on this connection means a commit this wrapper did not see. Distinct error, then reseed. |

`PRAGMA table_list` (since 3.37.0) reports `type`, `wr` (`WITHOUT ROWID`), and `strict`. The gate uses it.

### Client crates

| Crate | Version | License | Role |
|-------|---------|---------|------|
| `rusqlite` | 0.40.2 (crates.io, 2026-08-08) | MIT | Connection, SQL, hooks. Features `bundled` and `preupdate_hook`. |
| `libsqlite3-sys` | 0.38.2 (the version `rusqlite` 0.40.2 depends on) | MIT | Bundled SQLite. `preupdate_hook` adds `-DSQLITE_ENABLE_PREUPDATE_HOOK` and enables `buildtime_bindgen`. |

`rusqlite` 0.40.2's README says the `bundled` feature compiles SQLite 3.53.2. Upstream on the download page is 3.53.4. v1 tests the bundled 3.53.2. I did not confirm 3.53.4 behavior beyond the docs.

The `preupdate_hook` feature on `libsqlite3-sys` 0.38.2 is `["buildtime_bindgen"]`. The lab and CI need a libclang `bindgen` 0.72 can use. The `hooks` feature covers the commit, rollback, and update hooks. v1 also enables `preupdate_hook`. The `session` feature is not enabled.

`rusqlite` 0.40.2's `Cargo.toml` does not set `rust-version`. Its README says the MSRV is the latest stable Rust at release and that older compilers might work. The GitHub release text for 0.40.2 says "Lower MSRV to 1.88.0". Those three statements do not pick one number. All of them sit above the workspace `rust-version` of 1.75. `ce-stream-sqlite` sets its own `rust-version`, the same split planned for `ce-stream-postgres`. Re-check the number when the crate is added. It does not stay on 1.75.

SQLite itself, when bundled, is public domain ([copyright](https://www.sqlite.org/copyright.html), as cited by the rusqlite README).

### Lab

No Docker. Tests link the bundled SQLite 3.53.2 from `rusqlite` 0.40.2 with `bundled` and `preupdate_hook`. That is the version the suite runs.

The official Windows tools zip on the download page (`sqlite-tools-win-x64-3530400`, SQLite 3.53.4) can open a file the suite produced. It cannot register a preupdate hook. It is a manual file check, not the capture test.

## What the binlog gives us that hooks do not

MySQL's binary log is a durable stream another process can read. Resume is a GTID set. Nothing is emitted before XID. PostgreSQL's logical slot is the same kind of object: it survives the reader, and `pgoutput` turns redo into tuples.

SQLite's hooks are a function call on one connection. They fire while that connection's statement runs. They are not stored. After the process is gone, the calls are gone. The WAL file that remains is page redo, overwritten from the start after a checkpoint resets the salt ([WAL reset](https://www.sqlite.org/fileformat2.html)). The file change counter at header offset 24 is not incremented on every transaction in WAL mode ([file change counter](https://www.sqlite.org/fileformat2.html)). Neither one is a commit id the hook receives.

v1 accepts that. The wrapper sees the commits it makes. It does not tail a file.

## The wrapper

The application opens the database through the wrapper and runs its writes there. The wrapper registers:

1. **Preupdate hook.** For `main` and an included table, copy the operation, table name, column texts, and (on a rowid table) the rowid arguments into a buffer owned by the wrapper. The `sqlite3_value` pointers die when the callback returns. The hook does not prepare or step SQL.
2. **Commit hook.** Return zero. Leave the buffer in place. Do not call the consumer.
3. **Rollback hook.** Drop the buffer.

The consumer runs only when all of these are true: the application's SQL call has returned success, `sqlite3_get_autocommit` is non-zero (the [transaction](https://www.sqlite.org/lang_transaction.html) page documents that call), and the buffer has something to emit. The wrapper then builds one `CommittedTransaction`, calls the callback, and advances the checkpoint only after `Ok`.

`ChangeSource::run_transactions` borrows the source until capture stops, so writes go through a handle that shares the callback. The callback runs on the thread that committed, inline, and that call does not return to the application until the callback does. Awaiting `run_transactions` and writing on the same thread waits on itself. The embedder awaits `run` on the runtime and writes from its own tasks. The callback must not use the wrapped connection.

Autocommit follows the transaction docs: a statement that is not inside `BEGIN` is its own transaction and commits when the statement finishes. One such statement is one `CommittedTransaction`. An explicit `BEGIN` / `COMMIT` is one `CommittedTransaction` for the whole transaction. `RELEASE` of an inner savepoint does not write the outer transaction ([savepoints](https://www.sqlite.org/lang_savepoint.html)). v1 emits only when the connection is back in autocommit after a successful call. The commit-hook page does not mention savepoints or autocommit; the spike checks both.

A rolled-back transaction emits nothing. `COMMIT` that returns `SQLITE_BUSY` leaves the transaction open and emits nothing.

Several connections in one process are still several connections. v1 wraps one. A second connection's commits do not enter the buffer. The wrapper reads `PRAGMA data_version` when any SQL call it made returns. The pragma's value changes when another connection has committed, and it does not change for this connection's own commits. A change is the distinct error. An idle wrapper does not poll the file, so another process can commit and stay invisible until the next call through the wrapper. `data_version` is a property of the connection, not of the file, so it is not a resume key.

### Messages

| SQLite | ce-stream |
|--------|-----------|
| Preupdate `SQLITE_INSERT`, then the commit succeeds | `ChangeOp::Insert` in that commit's `events` |
| Preupdate `SQLITE_UPDATE` | `ChangeOp::Update` |
| Preupdate `SQLITE_DELETE` | `ChangeOp::Delete` |
| `sqlite3_preupdate_depth` > 0 | The same row events. A trigger's writes are included. They belong to the statement's commit. |
| `sqlite3_preupdate_blobwrite` ≥ 0 | Not a delete. `sqlite3_blob_write` is reported as `SQLITE_DELETE` because the new bytes are not available. v1 returns an error. Captured tables do not use incremental blob I/O. |
| Commit hook, then the call returns in autocommit | Close and emit the commit. |
| Rollback hook | Discard the buffer. |
| `CREATE` / `ALTER` / `DROP` / `RENAME` | No row hook. After the commit returns, if an included table is gone from `sqlite_schema`, `ControlKind::Dropped`. |
| `wal_hook` | Not registered. |

`ddl` stays empty. The statement text the application passed is not a server log record. `gtid` and `gtid_set_after` stay empty. `position` is set.

Included-table changes are kept in statement order. A commit that touches only excluded tables emits nothing. There is no server watermark to advance.

### Row data

`data` carries SQLite text as JSON strings, SQL `NULL` as JSON `null`:

```json
{
  "op": "update",
  "before": { "id": "41", "status": "open" },
  "after":  { "id": "42", "status": "shipped" },
  "key":    { "id": "41" }
}
```

- `after`: every new column (`sqlite3_preupdate_new`) on insert and update.
- `before`: every old column (`sqlite3_preupdate_old`) on update and delete.
- `key`: the declared primary-key columns. On a rowid table with no primary key, `key` is `{"rowid": "<text>"}` from the hook's rowid argument (`iKey2` on insert and update, `iKey1` on delete). A `VACUUM` can change that rowid. The gate warns. It does not refuse the table.
- `WITHOUT ROWID`: the rowid arguments are undefined. `key` is the primary key, which the table has. `table_list.wr` tells the wrapper which case it is.
- `subject` is `main.<table>`. `TableRef.database` is the hook's database name, `main`.
- `PayloadMode::Signal` drops the row images, as on the other sources.

Column names come from `PRAGMA table_xinfo`, read outside the hook and cached. Inside the hook the wrapper stores values by index. If `sqlite3_preupdate_count` disagrees with the cached non-generated columns, that commit is an error. v1 does not guess an alignment.

The include list is snapshotted when the transaction's first buffered change arrives, so a live update does not split the envelope.

### Values

Text in v1. Read `sqlite3_value_type` first. The text accessors are the same conversions as `sqlite3_column_text` ([column values](https://www.sqlite.org/c3ref/column_blob.html), and [value](https://www.sqlite.org/c3ref/value_blob.html) says the value routines work like the column routines):

| Storage class | JSON |
|---------------|------|
| `NULL` | `null` |
| `INTEGER` | String. SQLite's ASCII rendering (`INTEGER` → `TEXT`). |
| `REAL` | String. SQLite's ASCII rendering via `sqlite3_snprintf` (`FLOAT` → `TEXT`). |
| `TEXT` | That UTF-8 text. |
| `BLOB` | Lowercase hex of `sqlite3_value_blob`, no prefix. |

`BLOB` → `TEXT` in that conversion table is a cast that adds a zero terminator. It is not a text form of arbitrary bytes. Hex is the exception, because SQLite does not give a text form. It is not a typed converter. The crate does not call `sqlite3_value_int64`, `sqlite3_value_double`, or `sqlite3_value_numeric_type` to build the payload.

The seed uses the same text accessor, so a seeded row and a streamed row of the same value produce the same JSON.

A non-strict column can hold a blob in one row and text in another. A `STRICT` `ANY` column can too ([strict tables](https://www.sqlite.org/stricttables.html)). The JSON is still a string either way. The consumer that needs the storage class keeps the schema itself. v1 does not attach a type tag.

`STRICT` tables are allowed. The on-disk record format is the same, and the storage classes above still apply. Strict typing has been available since SQLite 3.37.0 (2021-11-27).

## Control events

v0.4.1 `ControlKind` is `Dropped`, `Renamed`, `DatabaseDropped`, `Invalidated`. The doc comment on `CommittedTransaction` says `control` comes after row events. v1 keeps that order.

| Situation | v1 |
|-----------|-----|
| Included table missing from `sqlite_schema` after a successful commit | `ControlKind::Dropped` on that commit. |
| `ALTER TABLE … RENAME` | Not emitted. A gone name plus a new name is the same shape as drop plus create. |
| `DatabaseDropped` | Not used. One file, not a server database. |
| Another connection committed | Distinct error, not `Invalidated`. There is no commit of ours to hang the control on, and no subject. |
| `DELETE` with no `WHERE` when the hook is silent | Not shipped. See below. |

The [update hook](https://www.sqlite.org/c3ref/update_hook.html) says the current implementation skips rows deleted by `ON CONFLICT REPLACE`, and skips rows deleted by the truncate optimization. It says that may change. The preupdate page does not say either sentence. The spike records what preupdate actually does. If those deletes produce no preupdate calls, a full-table delete would vanish. The core type for "all rows of this subject are gone" is `Truncated`, and v0.4.1 does not have it. The PostgreSQL plan adds it in the core bump aimed at v0.5.0. SQLite does not assume the variant is already in the tree. If the spike needs it, that variant ships with or after that core bump, and MySQL and MongoDB output stays unchanged. Until the variant exists, this source does not ship a mapping that drops the delete.

`Relation` is the other variant that plan adds. SQLite v1 does not need it. Column names are read from `table_xinfo` outside the hook. Avro `committed-transaction-v2` already carries `control` as JSON. No schema change.

## Crash

The buffer is process memory. SQLite commits, then the wrapper calls the consumer, then the wrapper's method returns.

- Crash before the committing call returns success: SQLite has not committed, or the call failed. No event. The buffer dies with the process.
- Crash after SQLite has committed and before the callback returns `Ok`: the rows are in the file. The event is gone. The checkpoint has not moved. Nothing on disk lists the missed commit.
- Callback returns `Err`: the checkpoint stays. The wrapper can retry the callback. It cannot roll the user's commit back. Exiting on `Err` loses the event the same way.
- Callback returns `Ok`: the checkpoint advances. The wrapper's method returns.

`FileCheckpointStore` lives outside the database. The hook is not allowed to write, so v1 does not insert a counter or a changeset in the same transaction.

There is no public commit id on the hook.

| Candidate | Gap |
|-----------|-----|
| Wrapper counter in the checkpoint | Counts commits this process has acknowledged. It is not in the database file. After a restart it does not name a SQLite transaction to replay or skip. |
| `PRAGMA data_version` | Local to the connection. Unchanged by this connection's own commits. Two connections do not share the number. Not a file position. |
| File change counter (header offset 24) | Not updated on every commit in WAL mode. The hook does not receive it. |
| WAL frame index, salt, `sqlite3_wal_hook` page count | Page redo. The salt changes when the WAL is reset. The page count shrinks on checkpoint. |
| Changeset blob | Exists only after the application calls `sqlite3session_changeset`. A crash frees the session object. The blob is not a position. |

A stored checkpoint at start is not a resume offset. v1 fails with the distinct error when one is present, so a restart is not silent. The embedder clears it and reseeds. A clean shutdown has the same checkpoint shape as a crash after the last `Ok`, and the source cannot see commits that died in the window after that. Reseed is the recovery either way.

## Position

`Begin` is not a message. The open transaction is the buffer. One successful outermost commit is one `CommittedTransaction`.

```json
{
  "adapter": "sqlite",
  "at":    { "counter": 12 },
  "after": { "counter": 12 }
}
```

- `counter` increments by one for each commit the wrapper emits, starting at 1 for the process.
- `at` and `after` are the same number. There is no later LSN to seek to. `after` is what the checkpoint stores, because that is the field the other adapters treat as the resume payload.
- On the next start the number is not comparable to the file. See [Crash](#crash).

CloudEvent extensions carry `counter`. GTID fields stay empty.

**Memory.** The wrapper holds one transaction until the commit call returns, as the MySQL reader holds one until XID. A transaction too large for the process is the same open point as MySQL.

## Seed helper

The file is local. Rows that exist before the wrapper is armed are not in any hook buffer. A database that is empty when the wrapper starts, and is written only through the wrapper, needs no seed. Anything else does.

v1 does not overlap the seed with capture. There is no commit id to draw the boundary the way PostgreSQL uses `consistent_point`.

What the helper does, on the connection that will be wrapped, before hooks are armed, with no other writer:

1. **Gate** the include list ([Gates](#gates)).
2. **`BEGIN IMMEDIATE`.** This starts a write transaction immediately, so another connection cannot commit during the copy ([transactions](https://www.sqlite.org/lang_transaction.html)). Readers are not blocked. In rollback-journal modes, `EXCLUSIVE` is what blocks readers; v1 does not use it. In WAL mode `IMMEDIATE` and `EXCLUSIVE` are the same.
3. **Copy** each included table: `SELECT` the columns from `table_xinfo` that the stream would emit, in batches of 512 (`SEED_BATCH`, as in the other sources), through the text accessor, in the same JSON shape as `after`. Never one `Vec` for the whole table.
4. **`COMMIT`.**
5. **Arm the hooks before the helper returns.**

What the embedder does: apply the copy, then apply commits the wrapper emits after that return. A write on this connection cannot sneak between steps 4 and 5 if the helper arms before it returns. A write from another process can. v1 does not support that writer. The `data_version` check is what reports it.

`sqlite3_snapshot_get` is a WAL-only snapshot handle. v1 does not use it. `BEGIN IMMEDIATE` plus "no other writer" is the lineup.

## Gates

The source runs the file gates when the wrapper is armed (`skip_gate_check` for lab use, as on MySQL). The table gates run for each included table and inside the seed helper. Each failure names the object and the fix.

| Check | How |
|-------|-----|
| Bundled library has preupdate | `PRAGMA compile_options` contains `ENABLE_PREUPDATE_HOOK` |
| Encoding `UTF-8` | `PRAGMA encoding` |
| Journal mode is not `OFF` | `PRAGMA journal_mode` |
| `main` only | Include-list database name is `main` |
| Real table | `PRAGMA table_list.type` is `table`. Refuse `view`, `virtual`, `shadow`. |
| No generated columns | `PRAGMA table_xinfo.hidden` is never 2 or 3 |
| Primary key | Warn when `wr = 0` and no primary key. Rowid is the key, and `VACUUM` can change it. |
| `data_version` stable while armed | Read it when any wrapper SQL call returns. A change is the distinct error. An idle wrapper does not poll. |

`ATTACH`ed databases and `temp` are ignored by the hook filter. They are not a gate failure unless the include list names them.

## CLI

v1 does not add `source.adapter = "sqlite"` to `ce-stream-cli`. The CLI is another process. Hooks on the connection it opens see the CLI's own writes. `PRAGMA data_version` would tell it the file changed and nothing else. A sidecar waits until a durable side log exists. v1 does not create one.

The embedder's settings, for when a later reader has a log to open:

```toml
# Not loaded by ce-stream-cli in v1.
[source]
adapter = "sqlite"
source_id = "sqlite://app.db"
path = "app.db"
# seed = true copies include_tables, then arms the wrapper before it returns
seed = false
delivery_unit = "transaction"
include_tables = ["main.orders"]
```

`delivery_unit` and `payload_mode` mean what they mean on the other sources. `sink.format = "avro"` stays the existing envelope. Exact field names are settled in the crate PR.

## Set aside

| Approach | Why not |
|----------|---------|
| WAL frame parser | Page images, salts, checksums. No row API. Layout is the file format's business. A checkpoint rewrites the file from the start. |
| `sqlite3_wal_hook` | A page count after a WAL commit. Replacing the hook disables auto-checkpoint. No old or new row. |
| `sqlite3_update_hook` alone | No column values. Skips `WITHOUT ROWID`. Documented silent spots for `ON CONFLICT REPLACE` and the truncate optimization. |
| Session extension (`sqlite3session`) | In-memory, one connection, one database. `sqlite3session_changeset` builds a blob by reading the table at call time and coalesces several edits of one row into one change. An update cannot carry a primary-key change. Rows with `NULL` in a primary-key column are ignored. Not a log SQLite retains across a crash. |
| Triggers, an audit table, a shadow table | Writes into the user's database. The PostgreSQL plan sets the same idea aside. It becomes interesting only if a sidecar must see commits, and v1 has no sidecar. |
| Checkpoint file holding the last unacknowledged commit | Narrows the crash window. Still misses a crash between SQLite's commit and that write. Not a log of every commit. Later, if an embedder needs it. |
| `PRAGMA data_version` or the file change counter as the resume key | Not a per-commit id of this connection. See [Crash](#crash). |
| Debezium, Kafka Connect | Pipeline frameworks. ce-stream is the library layer. |

## Spike checklist

On bundled SQLite 3.53.2, in a Rust test that writes through the wrapper, before the mapping is locked. Each item records what the hook did.

- Insert, update (key unchanged, key changed), delete, on a rowid table with a primary key
- The same three on `WITHOUT ROWID`
- A rowid table with no primary key: `key.rowid` matches the hook argument; `VACUUM` changes it
- Autocommit: one statement, one commit. Explicit `BEGIN` / `COMMIT`: several statements, one commit
- `ROLLBACK` and a constraint failure: nothing emitted
- Inner `SAVEPOINT` / `RELEASE`, then outer `ROLLBACK`: nothing emitted. Outer `COMMIT`: one commit
- `COMMIT` returning `SQLITE_BUSY`: nothing emitted, buffer kept
- Trigger body and a foreign-key action: row events present, one commit, `preupdate_depth` recorded
- `ON CONFLICT REPLACE` and `DELETE FROM t` with no `WHERE`: either one event per row, or the spike stops the release for a `Truncated` core change
- `sqlite3_blob_write`: the wrapper errors, no `Delete` event
- `ALTER TABLE` add column; `RENAME`; `DROP TABLE`: drop becomes `ControlKind::Dropped`; rename does not become `Renamed`
- Generated column: the gate refuses. The spike also records whether `preupdate_count` includes it
- `STRICT` table, including an `ANY` column that holds text in one row and a blob in the next: text versus hex
- Second connection commits: `data_version` changes, distinct error, no row event for that commit
- Kill the process after SQLite has committed and before the callback returns: the row is in the file, the checkpoint is unchanged, restart fails until reseed
- Seed: `BEGIN IMMEDIATE`, copy, arm. A write on the wrapped connection after return appears once, in the stream. A write attempted from another connection during the copy waits or fails
- Journal modes `DELETE` and `WAL`. `journal_mode = OFF` fails the gate
- Integer, real, text, null, and blob: the JSON string matches the seed's text accessor, blob as lowercase hex

One manual open of a suite-written file with the official 3.53.4 `sqlite3` shell before release. The shell does not run the hooks.

## Order

One issue, one branch, reviewable commits. No release number in this document.

1. **Spike the silent deletes** (`DELETE` with no `WHERE`, `ON CONFLICT REPLACE`) on bundled 3.53.2. If preupdate is silent, stop and add `ControlKind::Truncated` with or after the PostgreSQL core bump. MySQL and MongoDB tests stay green and their output stays unchanged. If preupdate fires per row, no core change.
2. **`ce-stream-sqlite`:** wrapper, preupdate buffer, commit and rollback hooks, include list, mapping, position, checkpoint rule, gates. Unit tests in-process. `rust-version` set from the rusqlite MSRV check, not from the workspace 1.75.
3. **`ChangeSource` wiring:** `run` / `run_transactions` park until stop; the write handle invokes the callback inline after commit. Same-thread deadlock covered by a test.
4. **Seed helper:** `BEGIN IMMEDIATE`, batches of 512, arm before return.
5. **Docs** (`library.md` when the crate exists). No CLI adapter. No version bump in this design.

## Tests

CI: in-process tests against bundled SQLite 3.53.2 (`cargo test -p ce-stream-sqlite`), plus `ce-stream-core`, `ce-stream-mysql`, and `ce-stream-mongo` when a core change is required. The build needs libclang because `preupdate_hook` turns on `buildtime_bindgen`.

The spike checklist is that suite. No Docker service. The 3.53.4 shell pass is manual.

## Out of scope

A CLI or any other process tailing the file, a WAL parser, the session extension as a stream, triggers and shadow tables, crash replay, more than one writing connection, `ATTACH`, `temp`, views, virtual tables, shadow tables, generated columns, `journal_mode = OFF`, incremental blob I/O, typed value conversion, a `Relation` control, and a release number.
