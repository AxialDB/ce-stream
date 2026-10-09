# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
