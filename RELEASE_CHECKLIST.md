# Release checklist - v0.6.0

Use this before merge and tagging. Canonical binaries come from GitHub Actions, not a laptop. The workflow is in [`docs/releasing.md`](docs/releasing.md).

## Branch

Feature branch + PR: `issue-19/source-contract-and-postgres` with **`Fixes #19`** in the PR body. Do not commit directly to `main`.

## Pre-merge

- [ ] Workspace version `0.6.0` in `Cargo.toml` (and the four internal crates in `[workspace.dependencies]`); `ce-stream-mysql-binlog` `0.2.0`; `Cargo.lock` committed and `--locked` clean.
- [ ] `CHANGELOG.md` - `[0.6.0] - 2026-11-23` finalized, link references up to date.
- [ ] `docs/releases/v0.6.0.md` exists (filename matches git tag `v0.6.0`) and has its date.
- [ ] `docs/issues/19.md` - status Released in v0.6.0; `docs/INDEX.md` names v0.6.0.
- [ ] `README.md` - latest release `v0.6.0`, its highlights, Linux and Windows listed equally. (crates.io shows this README for every crate: its links are absolute.)
- [ ] Every Markdown link resolves: relative links name a file that exists, release links name a tag that exists or `v0.6.0`.
- [ ] PR **CI** job green (`fmt`, `clippy`, tests; no database).
- [ ] PR **Release** job green for **Linux x86_64** and **Windows x86_64** (package only).
- [ ] `cargo publish --dry-run` of `ce-stream-mysql-binlog` and `ce-stream-core` (the others resolve against crates.io only after these are there).
- [ ] No secrets staged (`ce-stream.toml`, harness `out/`, `releases/dist/`).

Crash harness is **not** a CI gate. Optional lab run before tag: [`scripts/crash-harness/README.md`](scripts/crash-harness/README.md).

## After merge to main

```powershell
git tag -a v0.6.0 -m "v0.6.0: one contract for every source, and ce-stream-postgres"
git push origin v0.6.0
```

The Release workflow creates the GitHub Release and attaches Linux tar.gz, Windows zip, and `SHA256SUMS`.

## After the tag job

- [ ] Linux asset `ce-stream-v0.6.0-x86_64-unknown-linux-gnu.tar.gz`
- [ ] Windows asset `ce-stream-v0.6.0-x86_64-pc-windows-msvc.zip`
- [ ] `SHA256SUMS`
- [ ] Issue #19 closed

## crates.io

From the tag, in this order (`docs/releasing.md`, "crates.io"): `ce-stream-mysql-binlog` (0.2.0), `ce-stream-core`, `ce-stream-mysql`, `ce-stream-mongo`, `ce-stream-postgres` (new), `ce-stream-cli`.

## AxialDB

Move the engine's pin from a path to `tag = "v0.6.0"`, with `ce-stream-postgres` beside the other three.
