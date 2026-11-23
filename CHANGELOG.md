# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

One contract for every source, and `ce-stream-postgres` ([issue #19](https://github.com/AxialDB/ce-stream/issues/19)). To be released as 0.6.0.
Release notes (draft): [`docs/releases/v0.6.0.md`](docs/releases/v0.6.0.md).

### Fixed

- MongoDB: `IncludeList::in_effect_allows` reported a collection as delivered once the open stream's server filter sent it, while the reader still judged the events of the read it was waiting on by the list it had taken before that read. A collection the server already sent (an earlier table had it) was then reported at once, and its next change could be dropped: an embedder that copies the collection once it is reported lost that change. The reader now reports what it delivers, the server's filter and the list it judges events by (and an open transaction's own list), at each read. `IncludeFilter::both` and `TxnGrouper::pinned` are new.
- MySQL: a resume on a server that purged binary logs the checkpoint never received is `Error::HistoryLost`. The dump used to be sent the server's `gtid_purged` too, so the server never refused and the purged transactions were skipped with no error. The server's own refusal (error 1236) is `HistoryLost` as well, and a read error of the dump ends `run` with that error instead of `Ok`.
- MySQL: the rows of an XA transaction were never delivered, and its prepared GTID never entered the executed set. The rows are kept in memory from `XA PREPARE`, delivered at `XA COMMIT` as one commit at that position, and dropped at `XA ROLLBACK`. The prepared GTID stays out of the executed set until its `XA COMMIT` is delivered, so a restart between the two makes the server send the prepared transaction again. `XA COMMIT ... ONE PHASE` is an ordinary commit. An `XA COMMIT` whose rows were never read (prepared before capture first started, or its prepare purged) stops the stream with an error.
- `FileCheckpointStore` wrote in place, so a kill during the write left an empty file that every restart refused. A save writes a temporary file and renames it.
- `ce-stream-mysql-binlog`: `XaPrepareEvent::one_phase` was inverted. It is true for `XA COMMIT ... ONE PHASE` and false for `XA PREPARE`.
- `ce-stream-mysql-binlog`: a negative `TIME` inside a `JSON` value was read wrongly. The whole packed value is negated, not the hour alone.

### Added

- MySQL: `server_missing` and `purged_missing`, what a server can no longer send to a checkpoint.
- MySQL: a seed cut's `after` carries `baseline_gtid`, the baseline a checkpoint made from it holds.
- `ce-stream-core`: `Delivery`, what every source hands its consumer through `ChangeSource::run_deliveries`: `Commit(CommittedTransaction)`, `Progress { position }` (the stream passed a position with nothing in it for this consumer), and `Schema(TableSchema)`.
- `ChangeSource::covers(at, target)`: an embedder compares two positions without knowing the host. MySQL compares GTID sets, MongoDB cluster times, PostgreSQL LSNs by the end of a commit. Each adapter crate has it as `covers_position` too.
- `SourceConfig::checkpoint`: `CheckpointMode::OnOk` (default, the callback's `Ok` moves the checkpoint) or `OnConfirm` (the checkpoint moves when the consumer calls `Confirm::confirm(position)` on the handle from `ChangeSource::confirm()`). CLI: `checkpoint = "on_ok" | "on_confirm"` under `[source]`; `on_confirm` needs `delivery_unit = "transaction"`.
- `SeedCut` (`mark`, `wait_settled`): the cut for a seed, on a connection of the consumer's. `MysqlSeedCut`, `MongoSeedCut`, `PgSeedCut`.
- `TableSchema` and `ColumnSchema` (`name`, `host_type`, `nullable`, `key`). MySQL delivers a `Schema` on a table's first sight and when its columns change, PostgreSQL every time the server announces the table, MongoDB none. In row mode it is a CloudEvent of type `io.ce-stream.schema`.
- `GateReport { failures, warnings }` of `GateItem { object, problem, fix }` in `ce-stream-core`, and `ChangeSource::gate()` on every source.
- `ControlKind::Truncated`, `ControlKind::DefinitionChanged`, and `ControlKind::Message { prefix, content }`. `ControlEvent::identity`, the table's host identity where it has one that survives a rename.
- `CheckpointStore::sync`: the last save survives the machine. The default does nothing. `FileCheckpointStore` is in `ce-stream-core`.
- `ce_stream_core::queue` (`QueueBytes`, `ReaderTasks`, `delivery_bytes`): the bounded queue of #16, for every adapter with a reader task. The MongoDB reader now runs apart from the consumer and uses it, with `queue_capacity` and `queue_max_bytes`.
- Avro `committed-transaction-v4`, `progress-v1`, and `table-schema-v1`. `avro_encode::encode_delivery`, `encode_progress`, `decode_progress`, `encode_table_schema`, `decode_table_schema`, `decode_committed_transaction_v4`. `HttpSink::post_delivery`.
- `ce-stream-postgres`: PostgreSQL 17+ logical replication (`pgoutput`) over its own protocol layer, with SCRAM-SHA-256 and TLS through `rustls`, no libpq. `PostgresSource`, `PostgresOptions`, `PostgresStreamOptions`, `PgSeedCut`, `create_slot`, `drop_slot`, `slot_figures` (`SlotFigures`), `catalog_tables`, `validate_capture_gates`. CLI: `source.adapter = "postgres"` with `database`, `tls_ca`, `slot`, `create_slot`, `publication`, `message_prefixes`, `min_server_version`.
- PostgreSQL failover slots: `create_slot(options, slot, failover)`, `alter_slot_failover`, `SlotFigures::failover` and `synced`, CLI `failover = true`. A checkpoint on an older timeline than the server's resumes when it is at or before the point where the server's timeline forked from it, and is `Error::Checkpoint` past it. `fork_point` gives that point, `server_identity` the server's system identifier and timeline.
- MySQL: a row of a partitioned table carries the extension `partition_id`, and on an update `source_partition_id`, the partition the row left. A table that is not partitioned has neither.
- MySQL: `DROP TABLE`, `RENAME TABLE`, `ALTER TABLE`, `TRUNCATE`, and `DROP DATABASE` give typed controls in `control`, beside the text in `ddl`. A statement the parser cannot read gives the text only.
- MySQL: `ce_stream_mysql::charset` (`charset_of_collation`, `admitted`, `decode`).
- MySQL: `commit_position`, `commit_gtid`, `commit_gtid_set_after`, `covers_position`, `seed_cut_position`, `subtract_gtid_sets`, `union_gtid_sets`, `is_purged_refusal`, `CaptureServer`, `deliver_progress`, `new_confirm`.
- MongoDB: each row has `types` beside its image, for the values whose form does not show their type. `MongoSeedCut`, `covers_position`, `new_confirm`.
- `ce-stream-mysql-binlog`: `RowsEventHeader`; `partition_id` on `WriteRowsEvent`, `UpdateRowsEvent`, and `DeleteRowsEvent`, and `source_partition_id` on `UpdateRowsEvent`; `JsonFormatter::value_negative_time`.
- [`docs/values.md`](docs/values.md): what a value of each host type looks like, for every adapter.
- The examples in [`docs/library.md`](docs/library.md) compile: `cargo test --doc -p ce-stream-cli`.
- Crash harness for PostgreSQL: `scripts/crash-harness/postgres/`.

### Changed

- **Breaking (callback):** `ChangeSource::run_transactions` with a `FnMut(CommittedTransaction)` is `ChangeSource::run_deliveries` with a `FnMut(Delivery)`. A consumer matches `Delivery::Commit(txn)`, `Delivery::Progress { position }`, and `Delivery::Schema(schema)`.
- **Breaking (implementors):** `ChangeSource` requires `gate`, `confirm`, and `covers`, and is `Send + Sync` (was `Send`).
- **Breaking:** `CommittedTransaction::gtid` and `gtid_set_after` are gone. MySQL fills `position` like every adapter: read `position.at["gtid"]` and `position.after["gtid"]`, or call `ce_stream_mysql::commit_gtid(&txn)` and `commit_gtid_set_after(&txn)`. A literal sets `position: Some(ce_stream_mysql::commit_position(gtid, set_after))`.
- **Breaking:** `CommittedTransaction::source_position()` is gone. Read `txn.position`; every adapter sets it.
- **Breaking (MySQL):** a transaction with nothing for the consumer (filtered by the include list, or a statement such as `GRANT`) was a commit with no events. It is a `Delivery::Progress` with the executed set in `position.after["gtid"]`. Row mode delivers nothing for it; the checkpoint still moves.
- **Breaking (MongoDB):** the idle resume token was a commit with no events. It is a `Delivery::Progress`.
- **Breaking:** `ControlKind` is `#[non_exhaustive]` and no longer `Copy`: a match needs a wildcard arm. `ControlKind::Renamed` is `ControlKind::Renamed { to }`, and `ControlEvent::renamed_to` is gone. `ControlEvent` has `identity`; a literal adds `identity: None`. In JSON the new name is `to` beside `kind`, where v0.5 wrote `renamed_to`; a v0.5 control event still decodes.
- **Breaking (struct literals):** `SourceConfig` has `checkpoint`. A literal without `..Default::default()` adds `checkpoint: CheckpointMode::OnOk`.
- **Breaking (struct literals):** `MysqlBinlogSource` and `MongoChangeStreamSource` have `confirm: Confirm`. A literal adds `confirm: ce_stream_mysql::new_confirm()` or `confirm: ce_stream_mongo::new_confirm()`.
- **Breaking:** `ce_stream_mysql::IncludeList` is gone. Import `ce_stream_core::IncludeList`.
- **Breaking:** `ce_stream_mysql::FileCheckpointStore` is `ce_stream_core::FileCheckpointStore`.
- **Breaking:** `ce_stream_mysql::GateReport { warnings: Vec<String> }` is gone. `ce_stream_mysql::validate_capture_gates` returns `ce_stream_core::GateReport`, with the failures in the report where it returned `Err` on the first one. Call `.into_result()?` on the report to refuse on failures as before.
- **Breaking:** `HttpSink::post_committed_transaction(&txn)` is `HttpSink::post_delivery(&delivery)`.
- **Breaking (wire, transaction mode, JSON):** the body is a `Delivery`, `{"commit": {...}}`, `{"progress": {"position": {...}}}`, or `{"schema": {...}}`, where it was a bare `CommittedTransaction`. A commit has no `gtid` and `gtid_set_after`; it has `position` on every adapter.
- **Breaking (wire, transaction mode, Avro):** `encode_committed_transaction` writes `committed-transaction-v4` for every commit, where it wrote v1, v2, or v3. v4 is v3 without the `gtid` fields, with `position` for every adapter. `Progress` and `Schema` are records of their own, each with its own `Content-Type`. v1 to v3 are read, not written.
- **Breaking (wire, row mode):** a table's definition arrives as a CloudEvent of type `io.ce-stream.schema` before the table's first rows on a session (MySQL and PostgreSQL). A sink that takes every event as a row has to skip it.
- **Breaking (MySQL values):** text is decoded from the column's own charset, where the bytes were read as UTF-8 and anything else became a `hex:` string. Decoded: `utf8mb4`, `utf8mb3`, `ascii`, `latin1`, `latin2`, `latin7`, `cp1250`, `cp1251`, `cp1256`, `cp1257`, `koi8r`, `macroman`, `cp932`, `euckr`, `gbk`. A column in any other charset is left out of the row. A value that cannot be decoded is an error naming the column.
- **Breaking (MySQL values):** `ENUM` and `SET` are delivered as their labels (a `SET` joined by commas) and `JSON` as text, where they were a position, a bit mask, and binary.
- **Breaking (MySQL values):** `BINARY`, `VARBINARY`, and `BLOB` are always a `hex:` string, where bytes that happened to be valid UTF-8 arrived as text.
- **Breaking (MongoDB values):** a row holds the contract's values where it held extended JSON tags: an instant as microseconds, Decimal128 as a string, ObjectId as 24 hex characters, Binary as a `hex:` string. The types are in `types` beside the image ([`docs/values.md`](docs/values.md#mongodb)).
- **Breaking (`ce-stream-mysql-binlog` 0.2.0):** the vendored crate goes from 0.1.0 to 0.2.0. `WriteRowsEvent`, `UpdateRowsEvent`, and `DeleteRowsEvent` have the public field `partition_id`, and `UpdateRowsEvent` has `source_partition_id`; a literal adds them. `EventHeader::parse_rows_event_common_header` takes `is_update: bool` and returns `RowsEventHeader`, where it returned a tuple.
- MySQL: the commit that delivers an XA transaction's rows carries `prepared_gtid` in its `position.at`, beside `gtid`.
- MySQL and MongoDB: the gate reports every failure at once, each with its object and fix.
- CLI: an unknown `source.adapter` names the adapters it has (`mysql`, `mongo`, `postgres`). The health log in transaction mode counts `deliveries_total`, where it counted `commits_total`.
- The workspace `rust-version` stays 1.75, and `ce-stream-postgres` builds on it. The CLI and `ce-stream-mongo` stay on 1.88.

### Notes

- Sources: MySQL 9.x, MongoDB 8.0+ replica sets, PostgreSQL 17+.
- PostgreSQL: the slot is the embedder's to make and drop. A missing or invalidated slot is `Error::HistoryLost`, never a quiet new slot, and so is a checkpoint behind its slot.
- PostgreSQL: the gate requires `REPLICA IDENTITY FULL` and a primary key on every included table, and `publish_via_partition_root = false`. A drop or a rename sends nothing on the stream; `catalog_tables` shows them.
- A `Progress` is not delivered in row mode. It still moves the checkpoint.
- Live MySQL, MongoDB, and PostgreSQL tests stay out of CI.

## [0.5.0] - 2026-10-25

Long transactions in parts ([issue #16](https://github.com/AxialDB/ce-stream/issues/16)).
Release notes: [`docs/releases/v0.5.0.md`](docs/releases/v0.5.0.md).

### Added

- MySQL and MongoDB: a transaction larger than one part (`part_max_rows`, default 10,000 rows, or `part_max_bytes`, default 16 MB of row data) is delivered in parts as it is read. Both limits are settings in `SourceConfig` and under `[source]` in the CLI config.
- MySQL: `queue_max_bytes` (default 32 MB): the queue to the consumer holds at most this many bytes of row data besides `queue_capacity` deliveries. A setting in `SourceConfig` and under `[source]` in the CLI config.
- `CommittedTransaction::part` (`TransactionPart { number, last }`) and `CommittedTransaction::ends_transaction()`.
- Avro `committed-transaction-v3` for a part.
- `ce_stream_core::event::approx_json_bytes`.
- `ce-stream-perf-sink --stall-at N`, and a `stalled` count in its stats.
- Crash harness: `-PartMaxRows N` / `--part-max-rows N`.

### Changed

- A transaction larger than one part arrives as several deliveries. Only the last part carries the position after the transaction and moves the checkpoint.
- MySQL: the executed set (`gtid_set_after`, the `gtidset` extension) is on the last part only.
- MySQL: a `ROLLBACK` after a delivered part stops the stream with an error.

### Fixed

- MySQL: a delivery the consumer refused could hang the stream until the server dropped the connection.
- Crash harness: transaction mode with a whole transaction could not hang the first delivery, so the check of the checkpoint after a kill failed.

## [0.4.3] - 2026-10-12

MySQL: transactions that do not end with an `Xid` event ([issue #13](https://github.com/AxialDB/ce-stream/issues/13)).
Release notes: [`docs/releases/v0.4.3.md`](docs/releases/v0.4.3.md).

### Fixed

- MySQL: a DDL statement on its own (`CREATE TABLE`, `ALTER TABLE`, `TRUNCATE TABLE`, `DROP TABLE`, `CREATE DATABASE`, ...) was never delivered, and its GTID never entered the executed set. The server writes such a statement as one `Query` event with no `Xid` after it, and a transaction was delivered only at an `Xid`. It is now delivered when its `Query` event is read.
- MySQL: a transaction on a table without transactions (MyISAM) was never delivered. It ends with a `COMMIT` query where InnoDB has an `Xid`.
- MySQL: a statement that is neither DDL nor in a transaction (`GRANT`, `ANALYZE TABLE`) left a hole in the executed set. It is delivered as a commit without events, like a commit whose rows the include list leaves out.
- MySQL: a DDL statement with a comment in front (`/* ... */ ALTER TABLE ...`, as some clients send it) was not recognized as DDL.
- MySQL: `gtid_set_after` of a commit (and `gtidset` of its row events) left out the commits before it that the consumer had not taken from the queue yet, as happens while the source catches up after a start. The reader built the set from what the consumer had executed; it now counts what it has passed on. The checkpoint was not affected.

### Changed

- MySQL: `row` delivery now emits the `io.ce-stream.mysql.ddl` CloudEvent for DDL statements on their own, and `transaction` delivery an envelope with `ddl` set and no events. Before, DDL arrived only for `CREATE TABLE ... SELECT`, whose rows end with an `Xid`.
- MySQL: more commits without events arrive: one for each statement that is a transaction by itself and is not DDL.

## [0.4.2] - 2026-10-09

Mongo: server-side include filter ([issue #9](https://github.com/AxialDB/ce-stream/issues/9)).
Release notes: [`docs/releases/v0.4.2.md`](docs/releases/v0.4.2.md).

### Fixed

- Mongo: capture resumes from a position on a collection that has left the include list ([issue #11](https://github.com/AxialDB/ce-stream/issues/11)). The filter of #9 hid the event the stream resumes after, and the server refused the resume. Never released.
- Mongo: an update or replace in a collection that is not in `include_tables` and has no post-images ended the stream under `full_document = "required"`, and a restart failed on the same event. A finite include list is now a server-side `$match`, so the server neither looks up nor sends the writes of other collections.
- Mongo: a bulk load into a collection that is not listed no longer delays the listed ones. The server leaves those writes out.

### Added

- `IncludeList::set_in_effect`, `in_effect`, and `in_effect_allows` in `ce-stream-core`: the list an adapter's open stream filters by at the source. The Mongo source reports it after each open and clears it when capture ends. The MySQL adapter reports nothing.
- `ClusterTime::of_resume_token`.

### Changed

- Mongo: when the include list gains a collection, the change stream is opened again at its last position with the wider filter. Changes to the new collection are delivered from the moment `in_effect_allows` is true for it, not from the next transaction after `insert` or `replace`. An embedder that copies a collection and then applies its changes waits for that before the copy.
- Mongo: writes to collections that are not listed no longer arrive as commits without events. To keep the checkpoint moving, an empty batch whose position has moved is delivered as a commit with no events.
- Mongo: `drop` and `rename` of a collection that is not listed are no longer seen at all. They only advanced the resume token before.
- Mongo: `maxAwaitTimeMS` is 250 ms.

### Notes

- An empty `include_tables` (every table) keeps the unfiltered stream, and with it the post-image requirement for every collection of the database.
- Live MongoDB tests stay out of CI: `crates/ce-stream-mongo/tests/server_filter_live.rs`.

## [0.4.1] - 2026-10-04

Docs-only. No code or output changes from 0.4.0.

### Fixed

- README links are absolute. crates.io resolves relative links against each crate's folder, so every doc link on the 0.4.0 crate pages returned 404.

## [0.4.0] - 2026-10-04

MongoDB change-stream capture ([issue #5](https://github.com/AxialDB/ce-stream/issues/5)).
Release notes: [`docs/releases/v0.4.0.md`](docs/releases/v0.4.0.md).

### Added

- `ce-stream-mongo`: MongoDB 8.0+ replica-set change streams (one node is enough). CLI `source.adapter = "mongo"`.
- `CommittedTransaction.position` (`adapter`, `at`, `after`) and `control` events (`dropped`, `renamed`, `database_dropped`, `invalidated`).
- Avro `ce-stream.committed-transaction.v2` for commits that carry a position or control events. Row events stay `cloudevent.v1`.
- Seed helper: majority cluster time, then a majority cursor in batches of 512. The stream starts at that time.
- Checkpoint payload: `resume_token`, `cluster_time`, `seed_cluster_time`. A missing resume token is `Error::HistoryLost`.
- `IncludeList` / `IncludeFilter` live in `ce-stream-core`. `ce-stream-mysql` re-exports `IncludeList` until v0.5.

### Changed

- **Breaking (struct literals):** `CommittedTransaction` has `position` and `control`. MySQL leaves both empty, so MySQL JSON and Avro v1 output stay byte-identical to v0.3.0. Literals need `..Default::default()`.
- CLI and `ce-stream-mongo` set `rust-version = "1.88"`. The workspace floor stays 1.75.

### Notes

- No server-side `$match`. The include list filters in this process and is pinned for one transaction.
- `fullDocument = "required"` by default (post-images). `"update_lookup"` is the opt-out. Pre-images are never requested.
- Drop or rename of a watched collection, plus dropDatabase and invalidate, end capture. A drop of an unwatched collection only advances the resume token.
- Sharded clusters (`mongos`) and MongoDB older than 8.0 are out of scope. Live MongoDB tests stay out of CI.

## [0.3.0] - 2026-08-17

Live include-list updates on a running capture session ([issue #3](https://github.com/AxialDB/ce-stream/issues/3)).
Release notes: [`docs/releases/v0.3.0.md`](docs/releases/v0.3.0.md).

### Added

- `IncludeList` handle on `MysqlBinlogSource` (`include` / `include_handle()`): add, remove, or replace `database.table` entries without ending the binlog dump thread.
- Include snapshot is taken at **GTID** (start of the next transaction). Mid-envelope updates never split a commit.
- Unit tests: `include_live` (add/remove/replace, mid-txn freeze, empty commit after last-table remove, DDL still unfiltered).

### Changed

- **Breaking (struct literals):** `MysqlBinlogSource` has a new field `include: IncludeList`. Existing literals need `include: Default::default()` (capture start still seeds from `config.include_tables` if the handle was never mutated).

### Notes

- Change is **best-effort** from the next GTID. There is no ack.
- Empty `include_tables` at start still means all tables. After start, removing the last table or `replace([])` means no row events; empty commits still advance the GTID watermark.
- DDL Query events are not filtered by the include list (same as v0.2.0).

## [0.2.0] - 2026-08-15

Gate 0 transaction-boundary capture ([issue #1](https://github.com/AxialDB/ce-stream/issues/1)).
Release notes: [`docs/releases/v0.2.0.md`](docs/releases/v0.2.0.md).

### Fixed

- Checkpoint and emit now occur at **commit boundary** (XID), not per row event. Mid-transaction crash no longer loses uncommitted rows from redelivery.
- GTID resume on restart merges `gtid_purged`, connect-time `baseline_gtid`, and checkpoint watermark so binlog dump succeeds on servers with purged history (fixes silent connect failure after partial delivery).
- Compressed binlog transactions (`TransactionPayload`) are unpacked instead of silently dropped.

### Added

- `source.delivery_unit`: `row` (default) or `transaction` — same knob for CLI and embedders.
- `CommittedTransaction` envelope for transaction mode (`ddl[]` then `events[]`).
- DDL CloudEvents (`io.ce-stream.mysql.ddl`) in row mode; DDL in envelope for transaction mode.
- Avro schema `ce-stream.committed-transaction.v1` for transaction-mode sinks.
- MySQL capture gate validation (`binlog_format`, `binlog_row_image`, `binlog_row_metadata`, `gtid_mode`); CLI `--skip-gate-check` escape hatch.
- Checkpoint payload field `baseline_gtid` (recorded at first connect from `Latest`) for correct GTID handshake on resume.
- `ce-stream-perf-sink --stall-after N` for lab crash/restart harnesses (HTTP sink blocks after N POSTs).
- Unit/integration tests: `gtid_set`, `txn_checkpoint`, `transaction_payload`, `ddl`.
- GitHub Actions **Release** workflow: first-class **Linux x86_64** and **Windows x86_64** binaries (no MySQL); tag `v*` attaches archives + `SHA256SUMS`.

### Changed

- **Breaking wire semantics (row mode):** row CloudEvents are emitted **after XID**, not as binlog row events arrive. Message shape unchanged; timing changed.

## [0.1.1] - 2026-08-02

### Security

- Removed lab password and machine-local AxialDB paths from examples, scripts, and docs. Use `CHANGE_ME` / `CE_STREAM_PASSWORD` / `MYSQL_DEFAULTS_FILE` (or `-MysqlDefaults`). Rotate any MySQL password that matched the old example value; it remains in `v0.1.0` git history.

## [0.1.0] - 2026-08-02

### Added

- MySQL 9.x ROW binlog capture (`ce-stream-mysql`) with GTID checkpoint, include-list, TLS.
- CloudEvents 1.0 sinks: stdout and HTTP (`application/cloudevents+json`).
- Optional Avro sink encoding (`sink.format = avro`, schema `ce-stream.cloudevent.v1`).
- At-least-once delivery (checkpoint after successful sink), bounded queue backpressure.
- `payload_mode`: `full` | `signal`.
- CLI (`ce-stream`), embed example, systemd unit, E2E and perf harness scripts.
- Lab perf baselines (JSON and Avro) documented in `docs/perf-harness.md`.
- Lean OSS MVP: `LICENSE`, `NOTICE` (`Copyright 2026 AxialDB`), `CONTRIBUTING.md`, `SECURITY.md`, CI, issue/PR templates.
- Discussion category forms (`q-a`, `ideas`, `general`) and Issues contact links to Discussions.
- README credit: created and maintained by the AxialDB vendor ([axialdb.com](https://axialdb.com/), [AxialDB/releases](https://github.com/AxialDB/releases)).

### Deferred

- Other database engines (Phase 6 parked).
- Schema Registry / typed per-table Avro.

[Unreleased]: https://github.com/AxialDB/ce-stream/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/AxialDB/ce-stream/releases/tag/v0.4.1
[0.4.0]: https://github.com/AxialDB/ce-stream/releases/tag/v0.4.0
[0.3.0]: https://github.com/AxialDB/ce-stream/releases/tag/v0.3.0
[0.2.0]: https://github.com/AxialDB/ce-stream/releases/tag/v0.2.0
[0.1.1]: https://github.com/AxialDB/ce-stream/releases/tag/v0.1.1
[0.1.0]: https://github.com/AxialDB/ce-stream/releases/tag/v0.1.0
