# Contributing to ce-stream

Thanks for your interest. ce-stream is created and maintained by the [AxialDB](https://axialdb.com/) vendor ([releases](https://github.com/AxialDB/releases)). Contributions are welcome under the Apache-2.0 license.

## Where to ask / report

| Intent | Channel |
|--------|---------|
| Question / how-to | [Discussions → Q&A](https://github.com/ce-stream/ce-stream/discussions/new?category=q-a) |
| Bug | [GitHub Issues](https://github.com/ce-stream/ce-stream/issues) (use the Bug report template) |
| Feature idea | [Discussions → Ideas](https://github.com/ce-stream/ce-stream/discussions/new?category=ideas); we open an Issue only if it goes on the roadmap |
| Security vulnerability | See [`SECURITY.md`](SECURITY.md) — **not** a public Issue |
| Code change | Pull request from a fork |

There is no public Jira. GitHub is the tracker.

## Development

Requirements: Rust stable (see `rust-version` in workspace `Cargo.toml`), MySQL **9.x** only if you run live capture / E2E.

```powershell
cargo fmt --all
cargo clippy -p ce-stream-core -p ce-stream-mysql -p ce-stream-cli -p ce-stream-perf-sink --no-deps -- -D warnings
cargo test -p ce-stream-core -p ce-stream-mysql
```

Copy `ce-stream.toml.example` → `ce-stream.toml` for local runs (gitignored). Do not commit secrets.

Default CI does **not** start MySQL and does **not** run the crash harness. Lab scripts under `scripts/crash-harness/` and `scripts/e2e-http.ps1` need your own MySQL 9.x.

Release binaries (Linux + Windows) are built by `.github/workflows/release.yml` with no database.

## Pull requests

1. Fork and branch from `main`.
2. Keep changes focused; update docs when behavior changes.
3. Ensure fmt / clippy / unit tests pass locally.
4. Fill in the PR template.

## Scope reminders (v1)

- MySQL **9.x** only; other DB engines are deferred.
- JSON CloudEvents are the default interchange; Avro is optional.
