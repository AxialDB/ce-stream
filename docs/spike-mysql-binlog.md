# Spike: MySQL 9.x binlog (2026-08-01)

**Historical Phase 1 gate.** Product path through Phase 5 is done; other DB engines deferred — see [`planning.md`](planning.md).

**Verdict:** Rust stays viable for Phase 2 with a small owned patch. TLS + ROW + GTID resume + include-list worked on lab MySQL **9.7.0**. One upstream gap blocks "Latest" auto-position.

## Lab

| Item | Value |
|------|--------|
| Server | Local MySQL **9.7.0** on `127.0.0.1:3306` |
| Binlog | `log_bin=ON`, `ROW`, `FULL`, `gtid_mode=ON` |
| User | `ce_stream`@`%` (`scripts/spike-setup.sql`; set your own password) |
| Crate | `mysql-binlog-connector-rust` **git** `master` @ `f7cca8ec` (**0.3.4**), feature `rustls` |
| Why git | crates.io **0.3.3** has **no TLS features** (README ahead of publish) |

## Gate checklist

| Check | Result | Notes |
|-------|--------|--------|
| Connect + TLS (`ssl-mode=required`) | **PASS** | rustls; crate uses custom cert verifier (no CA pin yet) |
| Auth (`caching_sha2_password`) | **PASS** | |
| ROW insert / update / delete | **PASS** | Full before/after on UPDATE |
| Include-list | **PASS** | Client filter on `database.table` |
| GTID resume | **PASS** | Start from `@@GLOBAL.gtid_executed`; saw next GTID on events |
| `StartPosition::Latest` (empty) | **FAIL** | Upstream runs `show master status` (removed in 8.4+) |

### Evidence (abbreviated)

- Insert: `op=insert` on `ce_stream_spike.t1`, 3 row images, `last_gtid=...:263`
- Update: before/after pair (`spike-a` → `updated`)
- Delete: before image for deleted row
- Binary: `cargo run -p ce-stream-spike` with `CE_STREAM_DB_URL=...?ssl-mode=required`

## Upstream gap: `SHOW MASTER STATUS`

MySQL 8.4+ / 9.x removed `SHOW MASTER STATUS` in favor of `SHOW BINARY LOG STATUS` / `SHOW REPLICA STATUS`. The connector's empty-`Latest` path still issues the old statement → server error → spike cannot auto-discover current file/pos without a GTID set.

Replacement (same column layout on lab):

```sql
SHOW BINARY LOG STATUS;
-- File | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set
```

**Applied:** vendored crate at `vendor/mysql-binlog-connector-rust` with one-line rename only (no older-MySQL fallback). `Latest` re-verified PASS.

**Workaround used in spike:** always pass an explicit GTID set so `fetch_binlog_info` is skipped (`scripts/run-spike.ps1`).

## Non-issues for this spike

- `require_secure_transport=OFF` on lab: TLS still negotiated when client requests `ssl-mode=required`

## Re-run

```powershell
# Apply scripts/spike-setup.sql once (set CE_STREAM_PASSWORD, then substitute via -SetupLab or manual edit), then:
$env:CE_STREAM_PASSWORD = "<your ce_stream password>"
.\scripts\run-spike.ps1

# In another shell, generate DML while the spike waits, e.g.:
mysql --defaults-extra-file=PATH\to\my.cnf -e "INSERT INTO ce_stream_spike.t1(name) VALUES ('x'),('y'),('z');"
```

Proceed with `mysql-binlog-connector-rust` (git + rustls) behind `ce-stream-mysql`, after the SQL rename patch (or equivalent). Map `WriteRows` / `UpdateRows` / `DeleteRows` + GTID to CloudEvents; do not rely on crate `Latest` until patched.
