# Crash harness

Manual integration tests that kill ce-stream mid-delivery and assert full transaction redelivery on restart (Gate 0 / [issue #1](https://github.com/ce-stream/ce-stream/issues/1)).

| Adapter | Path | Status |
|---------|------|--------|
| MySQL 9.x | [`mysql/`](mysql/) | Active |

Run artifacts land under `<adapter>/out/<timestamp>/` (gitignored). Never commit passwords.

**Not in CI.** GitHub Actions never starts MySQL and never runs this harness. PR/main CI is `fmt` / `clippy` / unit tests. Release CI compiles Linux and Windows binaries only. Use this harness on a lab MySQL 9.x host before tagging if you want a live Gate 0 regression.

## Prerequisites

| Requirement | Notes |
|-------------|--------|
| MySQL 9.x | `log_bin=ON`, `binlog_format=ROW`, **`binlog_row_metadata=FULL`** |
| Admin client | `MYSQL_DEFAULTS_FILE` or `-MysqlDefaults` (`mysql --defaults-extra-file`) |
| Capture user password | `CE_STREAM_PASSWORD` env var only |
| First-time lab | `-SetupLab` applies [`scripts/spike-setup.sql`](../spike-setup.sql) |

## How to run (ce-stream repo)

From the **ce-stream** repo root:

### Windows (MySQL :3306)

```powershell
cd "D:\Work\ITART Repos\ce-stream"
$env:CE_STREAM_PASSWORD = "<ce_stream user password>"
$env:MYSQL_DEFAULTS_FILE = "D:\Work\ITART Repos\axialdb\my.cnf"   # admin client for DDL + inserts

.\scripts\crash-harness\mysql\run-crash-harness.ps1 -SetupLab
```

Skip rebuild on repeat runs:

```powershell
.\scripts\crash-harness\mysql\run-crash-harness.ps1 -SkipBuild
```

Transaction mode (one envelope per commit):

```powershell
.\scripts\crash-harness\mysql\run-crash-harness.ps1 -DeliveryUnit transaction -SkipBuild
```

Optional after editing `my.ini`:

```powershell
.\scripts\crash-harness\mysql\run-crash-harness.ps1 -RestartMysql -MysqlServiceName MySQL97
```

### WSL / Linux (MySQL :3307)

From PowerShell (delegates to bash inside WSL):

```powershell
cd "D:\Work\ITART Repos\ce-stream"
$env:CE_STREAM_PASSWORD = "<ce_stream user password>"

.\scripts\crash-harness\mysql\run-crash-harness.ps1 -Wsl -SetupLab
```

Skip rebuild / skip Linux install on repeat runs:

```powershell
.\scripts\crash-harness\mysql\run-crash-harness.ps1 -Wsl -SkipBuild -SkipLinuxInstall
```

Native bash (Linux or WSL shell):

```bash
cd /path/to/ce-stream
export CE_STREAM_PASSWORD='<ce_stream user password>'
export MYSQL_DEFAULTS_FILE="$HOME/.my.cnf"
export MYSQL_PORT=3307
export MYSQL_TLS=false

bash scripts/crash-harness/mysql/run-crash-harness.sh --setup-lab
bash scripts/crash-harness/mysql/run-crash-harness.sh --skip-build
```

Install Linux binaries to `~/.local/bin` (optional):

```bash
bash scripts/crash-harness/mysql/install-linux.sh
```

## Related docs (AxialDB)

When ce-stream is cloned as a sibling of axialdb, point `MYSQL_DEFAULTS_FILE` at this repo's root `my.cnf` for the harness admin client. See AxialDB [`shared/docs/architecture/mini-cdc.md`](https://github.com/AxialDB/axialdb/blob/main/shared/docs/architecture/mini-cdc.md) for Gate 0 context.


```text
ce-stream crash harness PASS
```

Both phases must pass:

1. **After kill:** checkpoint GTID unchanged (no mid-delivery advance).
2. **After restart:** full 3-row transaction redelivered (row mode) or one envelope (transaction mode).

See [`mysql/README.md`](mysql/README.md) for scenario details and failure modes.

## Related

- MySQL adapter details: [`mysql/README.md`](mysql/README.md)
- Unit test (no MySQL): `crates/ce-stream-mysql/tests/txn_checkpoint.rs`
- Ops docs: [`docs/ops-e2e.md`](../../docs/ops-e2e.md)
