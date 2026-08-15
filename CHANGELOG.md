# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0] - 2026-08-15

Gate 0 transaction-boundary capture ([issue #1](https://github.com/ce-stream/ce-stream/issues/1)).
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

[Unreleased]: https://github.com/ce-stream/ce-stream/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/ce-stream/ce-stream/releases/tag/v0.2.0
[0.1.1]: https://github.com/ce-stream/ce-stream/releases/tag/v0.1.1
[0.1.0]: https://github.com/ce-stream/ce-stream/releases/tag/v0.1.0
