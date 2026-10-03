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

## Issue to release workflow

One issue, one branch, one PR, one release (#1 shipped as v0.2.0, #3 as v0.3.0). Related core changes go inside the feature issue, not in a separate one.

1. **Issue** on `AxialDB/ce-stream`. Features use the sections Summary, Proposed behavior, Why, Alternatives, Non-goals (see #3). Bugs use the bug report template and the `bug` label. Features get no label.
2. **`docs/issues/N.md`** in the PR: GitHub link, label, status, resolution, problem summary, behavior, non-goals, tests (see [`issues/3.md`](issues/3.md)). Add it to [`INDEX.md`](INDEX.md).
3. **Branch** from `main`: `issue-N/<slug>` (example: `issue-1/gate0-transaction-boundary`).
4. **Scope change.** If the issue widens what ce-stream supports (a new source engine, a new sink class) or pulls back a deferred phase, the same PR records the decision in [`planning.md`](planning.md) and updates the scope lines in `README.md`, `CONTRIBUTING.md`, [`INDEX.md`](INDEX.md), and `.github/DISCUSSION_TEMPLATE/ideas.yml`.
5. **Large features: open the PR as a draft early.** Keep commits in reviewable order: core or refactor changes first with the existing tests green, then the new code, then tests and docs. Mark ready for review when the checklist is done.
6. **PR** from the template. Include `Fixes #N`, the checklist, a test plan, and reviewer notes that name any breaking change. The same PR bumps the workspace version and adds the `CHANGELOG.md` entry and `docs/releases/vX.Y.Z.md`.
7. Wait for **CI** and **Release** (package) jobs on the PR. Linux and Windows builds must both succeed.
8. The maintainer merges to `main` by hand.
9. Annotated tag on `main`: `git tag -a v0.3.0 -m "v0.3.0: live include-list updates"`
10. Push the tag: `git push origin v0.3.0`
7. The **Release** workflow publishes the GitHub Release from `docs/releases/v0.3.0.md` and attaches:
   - `ce-stream-v0.3.0-x86_64-unknown-linux-gnu.tar.gz`
   - `ce-stream-v0.3.0-x86_64-pc-windows-msvc.zip`
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

- [ ] Confirm the issue closed (`Fixes #N` on the merged PR, or close after the tag), and set `docs/issues/N.md` status to released.
- [ ] Confirm both Linux and Windows assets plus `SHA256SUMS` on the GitHub Release.
- [ ] Announce in Discussions (optional).
