# ce-stream

CloudEvents change streams from database logs.

**Created and maintained by the [AxialDB](https://axialdb.com/) vendor** ([releases](https://github.com/AxialDB/releases)). Open-source under Apache-2.0 — not an AxialDB-only runtime; anyone can run it against MySQL 9.x, MongoDB 8.0+, or PostgreSQL 17+.

[![CI](https://github.com/AxialDB/ce-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/AxialDB/ce-stream/actions/workflows/ci.yml)

**Sources:** MySQL **9.x** ROW binlog, MongoDB **8.0+** change streams, and PostgreSQL **17+** logical replication ([#19](https://github.com/AxialDB/ce-stream/issues/19), v0.6.0) → CloudEvents 1.0 (JSON default; optional Avro). Kafka not required.  
**Deferred:** other DB adapters — see [`docs/planning.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/planning.md). MongoDB shipped in [v0.4.0](https://github.com/AxialDB/ce-stream/blob/main/docs/releases/v0.4.0.md) ([#5](https://github.com/AxialDB/ce-stream/issues/5)), PostgreSQL in [v0.6.0](https://github.com/AxialDB/ce-stream/blob/main/docs/releases/v0.6.0.md) ([#19](https://github.com/AxialDB/ce-stream/issues/19)).

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

Pre-built binaries: [GitHub Releases](https://github.com/AxialDB/ce-stream/releases) (latest: [`v0.6.0`](https://github.com/AxialDB/ce-stream/releases/tag/v0.6.0) - Linux x64 and Windows x64, see `SHA256SUMS`). Built in CI on `ubuntu-latest` and `windows-latest` (no database).

### v0.6.0 highlights

- **PostgreSQL 17+** — `ce-stream-postgres` reads logical replication (`pgoutput`) over its own protocol layer (SCRAM-SHA-256, TLS through `rustls`, no libpq), with failover slots ([#19](https://github.com/AxialDB/ce-stream/issues/19)).
- **One contract for every source** — every adapter delivers a `Delivery` (`Commit`, `Progress`, `Schema`), compares positions with `covers`, confirms with `Confirm`, lines a seed up with `SeedCut`, and reports its gate as a `GateReport`. Values follow one mapping ([`docs/values.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/values.md)).
- **MySQL** — XA transactions are delivered at `XA COMMIT`; rows of a partitioned table carry `partition_id`; purged binary logs are `Error::HistoryLost`; text is decoded from the column's charset.
- **Breaking** — the library API and the transaction-mode output change: see [what changes for a consumer](https://github.com/AxialDB/ce-stream/blob/main/docs/releases/v0.6.0.md#what-changes-for-a-consumer).

See [`docs/releases/v0.6.0.md`](https://github.com/AxialDB/ce-stream/blob/main/docs/releases/v0.6.0.md) and [`CHANGELOG.md`](https://github.com/AxialDB/ce-stream/blob/main/CHANGELOG.md).

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
