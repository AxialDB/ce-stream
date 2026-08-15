# Releasing ce-stream

Maintainer checklist for semver tags and GitHub Releases. See also [`RELEASE_CHECKLIST.md`](../RELEASE_CHECKLIST.md).

## Version source of truth

- Workspace version: root [`Cargo.toml`](../Cargo.toml) `[workspace.package] version`
- Changelog: [`CHANGELOG.md`](../CHANGELOG.md) (Keep a Changelog)
- Release notes: [`docs/releases/vX.Y.Z.md`](releases/) (filename must match the git tag, e.g. `v0.2.0.md`)

## What CI does (and does not)

| Workflow | When | What |
|----------|------|------|
| [`ci.yml`](../.github/workflows/ci.yml) | PR + `main` | `fmt`, `clippy`, unit tests. **No MySQL.** |
| [`release.yml`](../.github/workflows/release.yml) | PR + tag `v*` | Release-build **Linux x86_64** (`ubuntu-latest`) and **Windows x86_64** (`windows-latest`) as equal first-class targets. On tags, attach archives + `SHA256SUMS` to the GitHub Release. **No MySQL.** |

The Gate 0 crash harness ([`scripts/crash-harness/`](../scripts/crash-harness/README.md)) is **manual lab only**. It is not a GitHub Actions job.

## Branch workflow

1. Feature branch from `main` (example: `issue-1/gate0-transaction-boundary`).
2. Open one PR. Include `Fixes #1` in the body when the PR closes that issue.
3. Wait for **CI** and **Release** (package) jobs on the PR. Linux and Windows builds must both succeed.
4. Merge to `main`.
5. Annotated tag on `main`: `git tag -a v0.2.0 -m "v0.2.0: Gate 0 transaction-boundary capture"`
6. Push the tag: `git push origin v0.2.0`
7. The **Release** workflow publishes the GitHub Release from `docs/releases/v0.2.0.md` and attaches:
   - `ce-stream-v0.2.0-x86_64-unknown-linux-gnu.tar.gz`
   - `ce-stream-v0.2.0-x86_64-pc-windows-msvc.zip`
   - `SHA256SUMS`

Do **not** tag from unmerged feature branches. Do **not** attach laptop/WSL binaries as the canonical GitHub Release assets.

## Local packaging (optional)

Canonical artifacts come from Actions. Local scripts are for lab rebuilds only: [`scripts/release/README.md`](../scripts/release/README.md).

### Linux (native, first-class)

```bash
bash scripts/release/build-release.sh 0.2.0
bash scripts/release/package-linux.sh 0.2.0
```

### Windows

```powershell
.\scripts\release\build-release.ps1 -Version 0.2.0
```

## Post-release

- [ ] Confirm issue #1 closed (`Fixes #1` on the merged PR, or close after the tag).
- [ ] Confirm both Linux and Windows assets plus `SHA256SUMS` on the GitHub Release.
- [ ] Announce in Discussions (optional).
