# Local release packaging (optional)

**Canonical GitHub Release binaries** are built by [`.github/workflows/release.yml`](../../.github/workflows/release.yml):

- **Linux x86_64** on `ubuntu-latest`
- **Windows x86_64** on `windows-latest`

Equal first-class targets. No MySQL. Triggered on PRs (package only) and on `v*` tags (attach to the GitHub Release).

Use the scripts below only for a local lab rebuild. Do not commit `releases/staging/` or `releases/dist/`.

## Linux (native)

```bash
bash scripts/release/build-release.sh 0.2.0
bash scripts/release/package-linux.sh 0.2.0
```

## Windows

```powershell
.\scripts\release\build-release.ps1 -Version 0.2.0
```

`package-release.ps1` combines already-staged Windows + Linux directories. Prefer the GitHub Actions artifacts over mixing hosts.
