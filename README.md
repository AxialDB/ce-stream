# ce-stream

CloudEvents change streams from database logs.

**Created and maintained by the [AxialDB](https://axialdb.com/) vendor** ([releases](https://github.com/AxialDB/releases)). Open-source under Apache-2.0 — not an AxialDB-only runtime; anyone can run it against MySQL 9.x, MongoDB 8.0+, or PostgreSQL 17+.

[![CI](https://github.com/AxialDB/ce-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/AxialDB/ce-stream/actions/workflows/ci.yml)

**Sources:** MySQL **9.x** ROW binlog, MongoDB **8.0+** change streams, and PostgreSQL **17+** logical replication ([#19](https://github.com/AxialDB/ce-stream/issues/19), not released) → CloudEvents 1.0 (JSON default; optional Avro). Kafka not required.  
**Deferred:** other DB adapters — see [`docs/planning.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/planning.md). MongoDB shipped in [v0.4.0](https://github.com/AxialDB/ce-stream/blob/main/docs/releases/v0.4.0.md) ([#5](https://github.com/AxialDB/ce-stream/issues/5)).

## Quick start

```powershell
copy ce-stream.toml.example ce-stream.toml
# edit host / user / password / include_tables / sink

cargo build -p ce-stream-cli --release
cargo run -p ce-stream-cli --release -- --config ce-stream.toml
```

Install the CLI from crates.io:

```powershell
cargo install ce-stream-cli
```

Pre-built binaries: [GitHub Releases](https://github.com/AxialDB/ce-stream/releases) (latest: [`v0.5.0`](https://github.com/AxialDB/ce-stream/releases/tag/v0.5.0) - Linux x64 and Windows x64, see `SHA256SUMS`). Built in CI on `ubuntu-latest` and `windows-latest` (no database).

### v0.5.0 highlights

- **Long transactions in parts** — a MySQL or MongoDB transaction larger than `part_max_rows` (10,000) or `part_max_bytes` (16 MB of row data) is delivered in parts as it is read, so its memory is bounded by a part ([#16](https://github.com/AxialDB/ce-stream/issues/16)). Only the last part carries the position after the transaction and moves the checkpoint.
- **MySQL: the queue to the consumer is bounded by bytes** — `queue_max_bytes` (32 MB) besides `queue_capacity` deliveries.
- **MySQL: a delivery the consumer refused no longer hangs the stream**, and a `ROLLBACK` after a delivered part stops it with an error.

See [`docs/releases/v0.5.0.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/releases/v0.5.0.md) and [`CHANGELOG.md`](https://github.com/AxialDB/ce-stream/blob/main/CHANGELOG.md).

Prefer a **replica**. For real column names: MySQL `binlog_row_metadata=FULL` (required; validated at connect).

## Docs

| Doc | Topic |
|-----|--------|
| [`docs/INDEX.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/INDEX.md) | Doc map |
| [`docs/ops-e2e.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/ops-e2e.md) | Ops / continuous run |
| [`docs/delivery.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/delivery.md) | At-least-once, backpressure |
| [`docs/library.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/library.md) | Embed as a library |
| [`docs/values.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/values.md) | What a value of each host type looks like |
| [`docs/avro.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/avro.md) | Optional Avro |
| [`docs/perf-harness.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/perf-harness.md) | Perf harness + lab results |
| [`CONTRIBUTING.md`](https://github.com/AxialDB/ce-stream/blob/main/CONTRIBUTING.md) | Bugs, PRs, Discussions |
| [`SECURITY.md`](https://github.com/AxialDB/ce-stream/blob/main/SECURITY.md) | Vulnerability reporting |
| [`CHANGELOG.md`](https://github.com/AxialDB/ce-stream/blob/main/CHANGELOG.md) | Releases |
| [`docs/oss-readiness.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/oss-readiness.md) | Lean OSS MVP plan |
| [`docs/planning.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/planning.md) | Internal phase status |

## Pipeline

```text
ChangeSource (mysql|mongo|postgres) → include-list → CloudEvent → Sink (stdout|http; json|avro)
                              ↑
                         Checkpoint (GTID, resume token, or LSN)
```

## License

Licensed under the [Apache License, Version 2.0](https://github.com/AxialDB/ce-stream/blob/main/LICENSE).  
Copyright notice: [`NOTICE`](https://github.com/AxialDB/ce-stream/blob/main/NOTICE) (`Copyright 2026 AxialDB`).
