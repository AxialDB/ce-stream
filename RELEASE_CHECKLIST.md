# Release checklist - v0.3.0

Use this before merge and tagging. Canonical binaries come from GitHub Actions, not a laptop.

## Branch

Feature branch + PR: `issue-3/live-include-tables` with **`Fixes #3`** in the PR body. Do not commit directly to `main`.

## Pre-merge

- [ ] Workspace version `0.3.0` in `Cargo.toml`; `Cargo.lock` committed and `--locked` clean.
- [ ] `CHANGELOG.md` - `[0.3.0] - 2026-08-17` finalized.
- [ ] `docs/releases/v0.3.0.md` exists (filename matches git tag `v0.3.0`).
- [ ] `docs/issues/3.md` - status Implemented, pending tag.
- [ ] `README.md` - install tag `v0.3.0`, Linux and Windows listed equally.
- [ ] PR **CI** job green (`fmt`, `clippy`, tests; no MySQL).
- [ ] PR **Release** job green for **Linux x86_64** and **Windows x86_64** (package only).
- [ ] No secrets staged (`ce-stream.toml`, harness `out/`, `releases/dist/`).

Crash harness is **not** a CI gate. Optional lab run before tag: [`scripts/crash-harness/README.md`](scripts/crash-harness/README.md).

## After merge to main

```powershell
git tag -a v0.3.0 -m "v0.3.0: live include-list updates"
git push origin v0.3.0
```

The Release workflow creates the GitHub Release and attaches Linux tar.gz, Windows zip, and `SHA256SUMS`.

## After the tag job

- [ ] Linux asset `ce-stream-v0.3.0-x86_64-unknown-linux-gnu.tar.gz`
- [ ] Windows asset `ce-stream-v0.3.0-x86_64-pc-windows-msvc.zip`
- [ ] `SHA256SUMS`
- [ ] Issue #3 closed

## AxialDB

Bump the git pin from `tag = "v0.2.0"` to `v0.3.0`, then continue live `run_transactions` embed.
