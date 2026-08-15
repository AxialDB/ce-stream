# ce-stream

CloudEvents change streams from database logs.

**Created and maintained by the [AxialDB](https://axialdb.com/) vendor** ([releases](https://github.com/AxialDB/releases)). Open-source under Apache-2.0 — not an AxialDB-only runtime; anyone can run it against MySQL 9.x.

[![CI](https://github.com/ce-stream/ce-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/ce-stream/ce-stream/actions/workflows/ci.yml)

**v1:** MySQL **9.x** ROW binlog → CloudEvents 1.0 (JSON default; optional Avro). Kafka not required.  
**Deferred:** other DB adapters — see [`docs/planning.md`](docs/planning.md).

## Quick start

```powershell
copy ce-stream.toml.example ce-stream.toml
# edit host / user / password / include_tables / sink

cargo build -p ce-stream-cli --release
cargo run -p ce-stream-cli --release -- --config ce-stream.toml
```

Install from git (crates.io publish comes later):

```powershell
cargo install --git https://github.com/ce-stream/ce-stream --locked --tag v0.2.0 ce-stream-cli
```

Pre-built binaries: [GitHub Releases](https://github.com/ce-stream/ce-stream/releases) (`v0.2.0` - Linux x64 and Windows x64, see `SHA256SUMS`). Built in CI on `ubuntu-latest` and `windows-latest` (no MySQL).

### v0.2.0 highlights

- **Commit-boundary capture** — checkpoint and emit at XID; fixes mid-transaction data loss ([#1](https://github.com/ce-stream/ce-stream/issues/1)).
- **`delivery_unit`** — `row` (default) or `transaction` (`CommittedTransaction` envelope).
- **GTID resume** — reliable restart after crash, including `gtid_purged` servers.
- **DDL + compressed transactions** — first-class DDL; `TransactionPayload` unpack.

See [`docs/releases/v0.2.0.md`](docs/releases/v0.2.0.md) and [`CHANGELOG.md`](CHANGELOG.md).

Prefer a **replica**. For real column names: MySQL `binlog_row_metadata=FULL` (required; validated at connect).

## Docs

| Doc | Topic |
|-----|--------|
| [`docs/INDEX.md`](docs/INDEX.md) | Doc map |
| [`docs/ops-e2e.md`](docs/ops-e2e.md) | Ops / continuous run |
| [`docs/delivery.md`](docs/delivery.md) | At-least-once, backpressure |
| [`docs/library.md`](docs/library.md) | Embed as a library |
| [`docs/avro.md`](docs/avro.md) | Optional Avro |
| [`docs/perf-harness.md`](docs/perf-harness.md) | Perf harness + lab results |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Bugs, PRs, Discussions |
| [`SECURITY.md`](SECURITY.md) | Vulnerability reporting |
| [`CHANGELOG.md`](CHANGELOG.md) | Releases |
| [`docs/oss-readiness.md`](docs/oss-readiness.md) | Lean OSS MVP plan |
| [`docs/planning.md`](docs/planning.md) | Internal phase status |

## Pipeline

```text
ChangeSource (mysql) → include-list → CloudEvent → Sink (stdout|http; json|avro)
                              ↑
                         Checkpoint (GTID)
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).  
Copyright notice: [`NOTICE`](NOTICE) (`Copyright 2026 AxialDB`).
