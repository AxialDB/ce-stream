# Spike: SQL Server transaction log (2026-10-07)

**Status:** 2022 CU26 fixture on `buildcomp`. CDC off. TDE decrypts with the certificate. No file parser yet.

**Verdict so far:** With CDC off, a clustered primary key is in the log record. A heap update of a non-key column is not. The whole-transaction rollback is `LOP_ABORT_XACT`. A savepoint undo is a compensating modify inside the commit. A checkpoint in simple recovery dropped the rows from `fn_dblog`. On Linux a second process can read the live log file while `sqlservr` has it open. The file mode is `0640` and owned by `mssql`, so a user outside that group cannot.

## Lab

The Linux lab is `buildcomp` (address and login in the ignored `scripts/spike-mssql/local/lab.env`), from the axialdb repo `shared/docs/shared_engine_tests.md`. The container is `ce-stream-mssql-spike`, published on `127.0.0.1:14333` only, memory cap 4 GiB. `mongo-rs`, `mongo-rs9`, and `mysql-ce` were left running.

| Item | Value |
|------|--------|
| Image | `mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04` |
| Version | `16.0.4265.3` RTM, Developer Edition (64-bit) |
| Database | `ce_stream_spike`, `RECOVERY FULL`, `is_cdc_enabled = 0`, `log_reuse_wait_desc = NOTHING` |
| Script | [`scripts/spike-mssql/run-on-buildcomp.sh`](../scripts/spike-mssql/run-on-buildcomp.sh), SQL in [`scripts/spike-mssql/fixture.sql`](../scripts/spike-mssql/fixture.sql) |
| Raw output | `scripts/spike-mssql/out/fixture-2022.txt` (gitignored) |

An earlier fixture ran on the Windows PC against the local `MSSQLSERVER` service, SQL Server 2025 RTM `17.0.1135.8`, Standard Developer Edition. That machine's Docker engine is Windows containers, so it cannot run the Linux image. The Start menu on that PC shows SQL Server 2025 Setup. The engine is a service (`sqlservr.exe -sMSSQLSERVER` under `MSSQL17.MSSQLSERVER`), not a Start-menu program, which is why it is easy to miss next to the installer. That pass is not the lab. Its opcode notes matched the 2022 run. Its log-file open failed on the data-directory ACL (the interactive Windows user, `UnauthorizedAccessException`), which does not answer sharing.

`fn_dblog` is the oracle. Column values below are `RowLog Contents 0/1/2` as `fn_dblog` returned them. Hex is truncated in the output at 180 characters. Lengths are `DATALENGTH`. The 2022 file has the same shapes. Clustered non-key modify of `dbo.fixed_pk` carries contents 2 `0x1601000000010000`. Heap non-key modify of `dbo.heap_pk` has an empty contents 2.

## What the records contained

| Case | Records | Key and images |
|------|---------|----------------|
| Insert, clustered `int` key, fixed columns | `LOP_INSERT_ROWS` / `LCX_CLUSTERED` | Full row. `id = 1`, `n = 10` are in contents 0 (`0x10000D00010000000A000000…`). |
| Update non-key column, same table (`n` 10 → 11) | `LOP_MODIFY_ROW` / `LCX_CLUSTERED` | Before byte `0x0A`, after byte `0x0B`. Contents 2 is `0x1601000000010000`, which carries the clustering key `1`. The unchanged `bit` column is not in the delta. |
| Update the clustering key (`id` 1 → 2) | `LOP_DELETE_ROWS` / `LCX_MARK_AS_GHOST`, then `LOP_INSERT_ROWS` | Old row and new row, both complete. |
| Delete, clustered | `LOP_DELETE_ROWS` / `LCX_MARK_AS_GHOST` | Deleted row image includes the key. |
| Insert and update, `nvarchar` column | `LOP_INSERT_ROWS`, then `LOP_MODIFY_ROW` | Insert holds `N'open'` as UTF-16. Update holds before `open` and after `shipped`, plus the clustering key in contents 2. |
| Insert, heap, nonclustered PK, second nonclustered index | `LOP_INSERT_ROWS` on `LCX_HEAP`, then two `LCX_INDEX_LEAF` | Heap row has `id` and `n`. Index leaf rows repeat the key. Those are not a second user change. |
| Update non-key column on that heap (`n` 10 → 11) | `LOP_MODIFY_ROW` / `LCX_HEAP`, plus index-leaf delete and insert on `ix_heap_n` | Heap modify is `0x0A` → `0x0B`. Contents 2 length is 0. The nonclustered PK index is not touched. The primary key is not in this update. |
| Delete on that heap | `LOP_DELETE_ROWS` on the nonclustered PK, the other index, and `LCX_HEAP` | The heap delete image includes `id`. |
| Two tables, one `COMMIT` | Two `LOP_INSERT_ROWS`, one `LOP_COMMIT_XACT` | One transaction. `two_b` shows as `Unknown Alloc Unit` because the table was dropped before `fn_dblog` resolved the name. |
| `ROLLBACK` | Insert, compensation delete with empty images, `LOP_ABORT_XACT` | No commit. |
| Nested `BEGIN TRAN` / `COMMIT` | One `LOP_BEGIN_XACT`, insert, modify, one `LOP_COMMIT_XACT` | The inner commit is not its own log transaction. |
| `ROLLBACK` to a savepoint | Insert, `LOP_MARK_SAVEPOINT`, modify `0x01` → `0x63` (1 → 99), compensating modify back to `0x01`, then commit | The undone value is in the log. Skipping the compensation would emit it. |
| `nvarchar(max)` of 100 characters | `LOP_INSERT_ROWS`, contents 0 length 215 | Inline. The `x` characters are in the row. |
| `nvarchar(max)` updated to 4000 characters | `LOP_MODIFY_ROW`, contents 1 length 8000 | The new value is in the log record, not a separate LOB chain, at this size. Larger than 8000 bytes was not tried. |
| `INSERT … WITH (TABLOCK)` of 100 rows, full recovery | 100 `LOP_INSERT_ROWS` / `LCX_HEAP`, then commit | Not minimally logged on this build in full recovery. |
| `TRUNCATE` | `LOP_SET_BITS`, `LOP_MODIFY_ROW` on PFS, `LOP_HOBT_DDL`, catalog rows | No per-row delete. Not a row image, and not a single truncate record. |
| `ALTER TABLE ADD`, `sp_rename`, `DROP TABLE` | System catalog rows (`sys.sysschobjs`, `sys.syscolpars`, and the rest) | Names show up as UTF-16 inside those rows (`extra`, `two_a_renamed`, `two_b`). There is no DDL statement text. Not a clean `Dropped` or `Renamed` without parsing the catalog format. |
| `LOP_LOCK_XACT` | Inside ordinary inserts | Not a row change. Failing the stream on it would reject every commit. |

`log_reuse_wait_desc` stayed `NOTHING` after the full-recovery commits. Nothing held the log.

## Simple recovery

Database `ce_stream_spike_simple`, one insert named `spike_simple`, then `CHECKPOINT`.

| | `fn_dblog` rows for that transaction |
|--|--------------------------------------|
| Before checkpoint | 4 |
| After checkpoint | 0 |

`is_cdc_enabled` was 0. Simple recovery dropped the records. Full recovery stays the rule.

## Live log file

Path inside the container: `/var/opt/mssql/data/ce_stream_spike_log.ldf`

`-rw-r----- mssql mssql`, 8388608 bytes, while the engine was accepting connections.

| Reader | Result |
|--------|--------|
| Second process as `mssql` (`uid` 10001), the container's default user | Read 8192 bytes. Head `01 0f 00 00 08 02 00 00 …`. |
| Second process as root | Read 512 bytes. |
| User `ce_reader`, not in the `mssql` group | `Permission denied` on open. |

The engine did not hold the file so that another process could not read it. The denial is the file mode. The ce-stream OS user has to be `mssql` or in that group (or root). This does not fall back to change tables or a CLR DLL.

The preferred production file is still the replica's log, not the primary's. This container is one instance. It only shows that a live log can be read beside `sqlservr`.

## TDE

A certificate `ce_stream_tde` was created in `master` on the same 2022 instance. The database encryption key is AES-256. `encryption_state` reached 3. The active VLF's `vlf_encryptor_thumbprint` is that certificate. Scripts: [`tde.sql`](../scripts/spike-mssql/tde.sql), [`tde-log.sql`](../scripts/spike-mssql/tde-log.sql), check [`decrypt-tde.py`](../scripts/spike-mssql/decrypt-tde.py). The certificate files are gitignored under `scripts/spike-mssql/out/`.

The raw `.mdf` and `.ldf` do not contain the marker strings. `DBCC PAGE` and `fn_dblog` do, because they run inside the engine.

Outside the process, with the exported certificate:

| Step | What matched |
|------|----------------|
| Private key | PVK, password from the backup. RSA-3072. |
| Database encryption key | On the boot page, after the certificate thumbprint. AES-256. The same blob is in the encrypted VLF header. |
| Data page `(1:360)` | Body after the 96-byte header, AES-CBC. IV is the page id, the file id, then eight zero bytes. The decrypted body matches `DBCC PAGE`, including `CESTREAM_TDE_MARKER_7f3a`. |
| Log block for LSN `0000002B:000003B8` | File offset `vlf_begin_offset + 0x3B8 * 512`. A 24-byte header is readable and carries that block id. The rest is AES-CBC with the same key. The 57-byte row image matches `fn_dblog` for `CESTREAM_TDE_LOG_9c2e`. |

That insert was made after `RECOVERY FULL` and a full backup, so a checkpoint could not drop it. EKM and Azure Key Vault were not tried.

## Not done

- 2025 CU9 (`2025-CU9-ubuntu-24.04`) on `buildcomp`.
- A parser. Bytes are captured. They are not decoded into `CommittedTransaction`.
- `nvarchar(max)` above 8000 bytes.
- TDE protected by EKM or Azure Key Vault.
- `tiberius` against this container.
- An availability-group secondary. The opcode fixture does not need one. Production still prefers that secondary's files.
