# SQL Server source — design

**Status:** Planning. No crate yet. Unscheduled, after the PostgreSQL source (that source targets v0.5.0). Written 2026-10-07 against the SQL Server 2025 docs, ce-stream v0.4.1, and `tiberius` 0.13.0. Revised the same day: the reader is the transaction log. Change data capture is not part of this source. On 2026-10-08 the 2022 CU26 lab decrypted a TDE log block with an exported certificate.

**Related:** [`planning.md`](planning.md) · PostgreSQL source [`postgres-design.md`](postgres-design.md) · embedding [`library.md`](library.md) · spike [`spike-mssql-log.md`](spike-mssql-log.md)

`ce-stream-mssql` is a later `ChangeSource`. Same product shape as `ce-stream-mysql` and `ce-stream-mongo`: one ordered reader, one `CommittedTransaction` per source commit, no Kafka, no Debezium, no triggers. SQL Server has no documented binlog. The transaction log is the redo log. v1 parses that log in the ce-stream process.

The reader opens the log files the operator exposes and decodes log records. It does not load a DLL into SQL Server. It does not enable change data capture, and it does not read change tables. Fivetran's file reader still turns CDC on so the engine writes a fatter image, then parses the log from a CLR DLL inside the SQL Server process ([Binary Log Reader](https://fivetran.com/docs/connectors/databases/sql-server)). Both of those are out. The spike records what an ordinary log record contains with CDC left off. A record that does not carry the key fails that commit. v1 does not turn CDC on to fill the gap.

## Decisions

- **Capture is a parser of the SQL Server transaction log, in the ce-stream process.** The operator mounts the instance log directory. The reader tails those files. Resume is an LSN. An unknown operation code fails the stream. It does not skip the record.
- **Change data capture is not used.** Not as the row source, and not as a logging switch. Enabling it creates change tables, a capture job, and a replication truncation hold, and it is unavailable on Express. v1 does not call `sys.sp_cdc_enable_db` or `sys.sp_cdc_enable_table`.
- **Do not load a CLR assembly, or any other code, into SQL Server.** Fivetran's online reader does that (`UNSAFE ASSEMBLY`) because their service cannot see the customer's disk. ce-stream runs where the operator can mount the log files.
- **Do not call `sp_replcmds` or `sp_repldone`.** Those procedures belong to replication. This reader does not publish the database and does not advance a replication truncation point.
- **`fn_dblog` is the spike's oracle, not the reader.** It is undocumented ([Microsoft Q&A](https://learn.microsoft.com/en-us/answers/questions/269017/using-fn-dblog-cause-we-lost-support)), it only sees the active log, and a scan of it is the wrong shape for a tail. The file reader has to agree with it on the fixtures. Production does not call it.
- **Do not use change tracking or Change Event Streaming.** Change tracking stores keys, not row images. CES is still preview and only pushes to Azure Event Hubs or Fabric Eventstream.
- **The library creates nothing in the user database.** `SourceConfig.include_tables` filters client-side, after decode.
- **There is no edition gate.** Express, Web, Standard, Enterprise, and Developer all write a transaction log. The lab image is Developer because that is what `mcr.microsoft.com/mssql/server` runs. Express is not refused.
- **v1 has no seed.** The consumer starts from "now": the end LSN at connect. Rows already in the database are not emitted. See [Seed](#seed).
- **The preferred log is the replica's, not the primary's.** Same rule as MySQL in [`ops-e2e.md`](ops-e2e.md): capture stays off the primary, so file reads and a slow consumer are not coupled to primary OLTP, and the primary's live log may be locked. For SQL Server the replica is an availability-group secondary. The reader opens that secondary's log files. A single-instance lab is only the opcode fixture. When a replica exists, the primary's log is not the source.
- **TDE uses the certificate that protects the database encryption key.** The files on disk are ciphertext. The reader takes that certificate and the password for its private key, the same files a restore uses, and unwraps the key itself. It does not ask SQL Server to return plaintext rows. No certificate means no read. EKM and Azure Key Vault are out of v1. The 2022 CU26 test is in [Transparent data encryption](#transparent-data-encryption).
- **Recovery model is full.** In simple recovery a checkpoint can reuse a virtual log file the reader has not finished. Full recovery truncates only after a log backup. The reader still loses history if that backup's virtual log files are reused before the checkpoint LSN is consumed. That is a distinct error. v1 does not pin the log.
- **Values are text rendered from logged bytes.** The log has no server text. The crate renders storage bytes to JSON strings with a style table the spike locks against `CONVERT`. It does not parse those strings into Rust numbers or dates. See [Values](#values).
- **Before-images are what the log record contains.** Require a primary key. A record that does not carry the key fails that commit. It does not emit a row keyed by a file:page:slot locator, and it does not enable CDC to obtain one.
- **No new `ControlKind`.** The log does not contain DDL SQL. `ddl` stays empty. A change to an included table's columns or allocation unit stops the stream with a distinct error. The spike records whether drop, rename, and truncate are classifiable as the existing control kinds. v1 does not assume they are. MySQL and MongoDB output does not change.
- **v1 is self-hosted SQL Server only.** Azure SQL Database, Azure SQL Managed Instance, and RDS do not expose the log files to this process.

## Prerequisites

Checked 2026-10-07 against the SQL Server log docs ([log architecture](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-log-architecture-and-management-guide?view=sql-server-ver17), [dm_db_log_stats](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-db-log-stats-transact-sql?view=sql-server-ver17), [lifecycle](https://learn.microsoft.com/en-us/lifecycle/products/sql-server-2022)), the [2016 ESU FAQ](https://learn.microsoft.com/en-us/sql/sql-server/end-of-support/extended-security-updates-frequently-asked-questions?view=sql-server-ver17), and manifest requests against `mcr.microsoft.com/mssql/server`.

### Source server

| Need | Requirement | Why |
|------|-------------|-----|
| Server version | **2022 or newer** | 2022 is the oldest release still in mainstream support (through 2028-01-12; extended support through 2033-01-12). 2025 is current (GA 2025-11-18; mainstream through 2031-01-07). The log layout is not a contract. v1 tests these two builds. |
| 2019 | Unsupported | Mainstream support ended 2025-03-01. Extended support runs to 2030-01-08. Untested, so unsupported. Same stance as PostgreSQL 15 in the PostgreSQL design. |
| 2016 and older | Unsupported | Extended support ended 2026-07-14 ([ESU FAQ](https://learn.microsoft.com/en-us/sql/sql-server/end-of-support/extended-security-updates-frequently-asked-questions?view=sql-server-ver17)). |
| Edition | Any edition that writes a transaction log | No feature in this design is edition-gated. Express is in scope. The official container image used in the lab is Developer. |
| Log files | The ce-stream OS user can read the replica's log files (`sys.database_files` where `type_desc = 'LOG'`) | The reader has no other view of the log. Prefer the secondary so the primary file can stay locked. A shared read while that engine holds the file open is a spike gate. If the open fails, this design stops. It does not fall back to change tables or a CLR DLL. |
| Recovery model | `FULL` | A checkpoint in simple recovery can reuse the log the reader has not read. |
| TDE | Off, or a certificate backup plus the private-key password | `encryption_state` 0 needs nothing. `encryption_state` 3 requires the certificate whose thumbprint is `encryptor_thumbprint`. Any other state fails the gate. |
| Primary key | Every included table | Key for update and delete. A locator is not a key. |
| Capture login | Catalog `SELECT`, `VIEW DATABASE PERFORMANCE STATE` (2022+) | Gates and `sys.dm_db_log_stats`. Not `sysadmin`. Not `db_owner`. The spike's `fn_dblog` oracle is sysadmin-only and is not this login. |
| CDC | Off | `sys.databases.is_cdc_enabled = 0`. The gate fails if it is on, so a database that already has capture objects is not silently read as if this were a CDC client. |

### Client crates

| Crate | Version | License | Role |
|-------|---------|---------|------|
| `tiberius` | 0.13.0 (crates.io, 2026-09-25), Rust 1.88 | MIT OR Apache-2.0 | TDS connection: gates, catalog, `sys.dm_db_log_stats`. Do not enable its `chrono`, `time`, or `rust_decimal` features. |

`tiberius` 0.13.0's own table says SQL Server 2022, 2019, and 2017 are tested in its CI. It does not list 2025. The release pass on the 2025 image is ours.

A crates.io search on 2026-10-07 found no decoder of SQL Server log records. The parser in this crate is new.

`ce-stream-mssql` sets `rust-version = "1.88"`. The rest of the workspace keeps 1.75.

### Lab

Official image `mcr.microsoft.com/mssql/server`. Manifests returned HTTP 200 on 2026-10-07. `2025-latest` did not match the CU9 digest, so the lab pins a CU tag.

| Tag | Role |
|-----|------|
| `2022-CU26-ubuntu-22.04` | Floor. The only dev and test server until the release pass. |
| `2025-CU9-ubuntu-24.04` | Current. One pass before release. |

Start with `ACCEPT_EULA=Y` and a strong `MSSQL_SA_PASSWORD`. On the 2022 image set `MSSQL_PID=Developer`. The SQL Server 2025 environment-variable table does not list `Developer`; it lists `EnterpriseDeveloper` and `StandardDeveloper`. The 2025 lab image uses `EnterpriseDeveloper`. The same Microsoft page's Docker sample still passes `Developer` for `2025-latest`; the spike records which value the CU9 image accepts.

Linux lab runs on `buildcomp` (address and login in the ignored `scripts/spike-mssql/local/lab.env`), the AxialDB Linux machine documented in the axialdb repo at `shared/docs/shared_engine_tests.md`. The container binds `127.0.0.1` on that host. It does not replace `mongo-rs`, `mongo-rs9`, or `mysql-ce`.

The test mounts the instance log directory into the ce-stream process, read-only. The fixture database is created with `RECOVERY FULL` and CDC left off. No Agent, no capture job, no publication. That container is one instance, standing in for the replica's log files. An availability group is not required to read opcodes.

The Windows PC is not the Linux lab. Docker there is Windows containers. The Start menu shows SQL Server 2025 Setup; the database engine, when installed, is the `MSSQLSERVER` service and does not appear as its own program. A first fixture accidentally used that service. The 2022 CU26 pass ran on `buildcomp` (`16.0.4265.3`, Developer). A second process could read the live log while `sqlservr` had it open, for `mssql` and for root. Mode `0640` denied a user outside the `mssql` group. See [`spike-mssql-log.md`](spike-mssql-log.md).

## What the other tools actually read

MySQL's binary log is a documented row stream with a GTID resume point. SQL Server's log is recovery redo. Microsoft documents the LSN and that a modification record holds a before image and an after image of the logged operation ([log architecture](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-log-architecture-and-management-guide?view=sql-server-ver17)). It does not publish the record layout. The opcodes below are the names `fn_dblog` returns, described from the outside by [SQLPerformance, 2022](https://sqlperformance.com/2022/07/transaction-log/part-4-log-records). They are the spike's vocabulary, not a Microsoft contract.

Checked 2026-10-07. The products below are context. v1 does not follow their CDC or CLR setup.

| Tool | What it reads |
|------|----------------|
| Fivetran SaaS connector | Four incremental methods: change tracking, CDC, Binary Log Reader, and Teleport Sync ([overview](https://fivetran.com/docs/connectors/databases/sql-server)). Binary Log Reader reads log files. Online reads on Windows go through a Fivetran CLR DLL. Their reader also enables CDC so the log contains supplemental images. |
| Fivetran HVR 6 | `Capture_Method=DIRECT` reads the log file with file I/O ([direct capture](https://fivetran.com/docs/hvr6/requirements/source-and-target-requirements/sql-server-requirements/sql-server-as-source/capture-from-sql-server-using-direct-transaction-log-access)). They still require a CDC table or a replication article for the row identifier. |
| Qlik Replicate | The log endpoint reads active logs and can read backup logs at file level ([limitations](https://help.qlik.com/en-US/replicate/May2026/Content/Global_Common/Content/SharedReplicateHDD/SQLServer-Source/limitations_source_SQLServerDB.htm)). A separate endpoint reads change tables. I did not find an official Qlik page that names `sys.fn_dblog`. |
| Oracle GoldenGate | Extract reads change tables ([operational considerations](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/prepare-cdc-capture-method-operational-considerations-sqlserver.html)). |
| Striim | One reader uses change tables. MSJet reads transaction log backups on disk ([SQL Server CDC](https://developer.striim.com/onlinedocs/en/sql-server-cdc.html)). |
| AWS DMS | Replication for tables with a primary key, and change tables otherwise ([DMS SQL Server](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.SQLServer.CDC.html)). |

v1 is a file reader on a database that is not enabled for CDC. Where a logged update has no key, the commit fails. The other tools avoid that by enabling CDC. This design does not.

## Transparent data encryption

Tested 2026-10-08 on `mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04` (`16.0.4265.3`), CDC off, on `buildcomp`. Certificate `ce_stream_tde` was created in `master`. The database encryption key is AES-256. `encryption_state` reached 3, and the active VLF's `vlf_encryptor_thumbprint` is that certificate. Scripts: [`tde.sql`](../scripts/spike-mssql/tde.sql), [`tde-log.sql`](../scripts/spike-mssql/tde-log.sql), [`decrypt-tde.py`](../scripts/spike-mssql/decrypt-tde.py). Notes: [`spike-mssql-log.md`](spike-mssql-log.md). The certificate files are gitignored under `scripts/spike-mssql/out/`.

`fn_dblog` and `DBCC PAGE` show the row, because they run inside the engine. The raw `.mdf` and `.ldf` do not contain the marker strings. v1 recovers them from the files.

| Piece | What the test locked |
|-------|----------------------|
| Private key | `BACKUP CERTIFICATE` writes a PVK. This certificate is RSA-3072. The 8-byte blob header is in the clear. The rest is RC4. The RC4 key is the first 16 bytes of SHA-1 over the salt and the backup password. |
| Database encryption key | On the boot page, after the 20-byte certificate thumbprint and a 4-byte length. The RSA ciphertext is little-endian PKCS #1. The plaintext is a 32-byte AES-256 key. The encrypted VLF header carries the same blob. |
| Data page | The first 96 bytes stay readable. The rest is AES-CBC. The IV is the page id, the file id, then eight zero bytes. Page `(1:360)` matched `DBCC PAGE`, including `CESTREAM_TDE_MARKER_7f3a`. |
| Log block | File offset is `vlf_begin_offset + block_id * 512`. The block id is the middle field of the LSN (`0000002B:000003B8` is block `0x3B8`). Twenty-four bytes of header stay readable and contain that id. The rest is AES-CBC with the same key. The 57-byte insert image matched `fn_dblog` for `CESTREAM_TDE_LOG_9c2e`. |

That insert ran after `RECOVERY FULL` and a full backup, so the later checkpoint left the record in `fn_dblog`.

The row in that block starts after the first AES block. CBC chaining fixes those bytes whatever the IV is. A record that begins in the first 16 bytes of the payload still needs the IV pinned. The crate does that before it parses such a record. The thumbprint on the VLF has to match the certificate. A mismatch fails the gate.

## Log records

A transaction on the log is `LOP_BEGIN_XACT` … row records … `LOP_COMMIT_XACT`, tied by transaction id. The source buffers that transaction and emits one `CommittedTransaction` at the commit record. `LOP_ABORT_XACT` discards the buffer. An inner `COMMIT` in T-SQL only decrements `@@TRANCOUNT`. The outer commit is the one that commits ([COMMIT](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/commit-transaction-transact-sql?view=sql-server-ver17)). A rollback of the whole transaction ends in `LOP_ABORT_XACT` and is not emitted. A rollback to a savepoint stays inside the open transaction. On SQL Server 2025 RTM the forward change and a compensating `LOP_MODIFY_ROW` are both in the log before `LOP_COMMIT_XACT`. The reader applies the compensation. Emitting the forward change on its own would publish a value that was undone.

| Log | ce-stream |
|-----|-----------|
| `LOP_INSERT_ROWS` on the heap or clustered index of an included table | `ChangeOp::Insert` |
| `LOP_DELETE_ROWS` on that same allocation unit | `ChangeOp::Delete` |
| `LOP_MODIFY_ROW`, `LOP_MODIFY_COLUMNS` | `ChangeOp::Update`. The before image plus the logged delta is the after image. A delta with no before image, or no key, fails the commit. |
| `LOP_COMMIT_XACT` | Close and emit the commit. |
| `LOP_ABORT_XACT` | Discard. |
| Nonclustered index allocation units of an included table | Ignored. Emitting them would repeat the row. The 2025 RTM fixture logs `LCX_INDEX_LEAF` for a heap's nonclustered indexes beside the `LCX_HEAP` row. |
| `LOP_LOCK_XACT` | Ignored. The same fixture has it inside ordinary inserts. It is not a row change. |
| `LOP_EXPUNGE_ROWS`, `LOP_SET_BITS`, page-format and allocation records (`LCX_PFS`, `LCX_GAM`, `LCX_DIFF_MAP`, and the rest of that family) | Ignored. Not a user change. |
| Any other opcode on the heap or clustered allocation unit of an included table | Fail the stream. Do not skip. |
| Bulk or minimal logging with no row image | Fail that commit. The spike names the statement that produced it. |

Included tables are identified by allocation unit id, cached from the catalog at start (`sys.allocation_units`, `sys.partitions`, `sys.indexes`). A record for any other allocation unit is skipped. The cache is the include list: client-side, applied after the record is classified, so a live include change takes effect on the next commit and does not split the one in hand.

The trailing log block is incomplete while the engine is writing it. The reader does not parse a partial block. The spike learns how a reused block is told from a partial one, on this build, and checks that a kill mid-block loses nothing and duplicates nothing.

### Row data

`data` carries rendered text as JSON strings, SQL `NULL` as JSON `null`:

```json
{
  "op": "update",
  "after":  { "id": "42", "status": "shipped", "total": "19.90" },
  "before": { "id": "41", "status": "open", "total": "19.90" },
  "key":    { "id": "41" }
}
```

- `after`: insert, and update once the delta is applied.
- `before`: delete, and update.
- `key`: primary-key columns of the before image on update and delete. On insert the key is in `after`.
- `subject` is `schema.table`. `TableRef.database` holds the schema and `TableRef.table` holds the table. Include-list entries are `schema.table`.
- Extensions: `commitLsn`.
- `PayloadMode::Signal` drops the row images.
- A key change is `key` = old key, `after` = new row.

Column names and types come from the catalog cache, not from the log record. If the logged row does not match the cached column count and lengths, that commit fails. v1 does not guess an alignment. LOB values that the record does not inline are a spike item. If the fixture cannot rebuild them, v1 refuses `varchar(max)`, `nvarchar(max)`, `varbinary(max)`, `text`, `ntext`, and `image`.

### Values

The style table is locked in the spike by rendering a logged value and comparing it to `CONVERT(nvarchar(max), <column>, <style>)` on the same row. `binary` and `varbinary` use style 1 (`0x` hex). `decimal` and `numeric` use the server's default decimal text. `float` and `real` are refused if no style round-trips. `datetime` is timezone-naive; the position's `commit_time` is style 126 with no `Z` added. `money`, `smallmoney`, `rowversion`, and `uniqueidentifier` are in the spike. CLR types, `sql_variant`, and `xml` are refused.

This rendering is a storage-to-text step. It is not a typed converter, and it does not enable a decimal or date feature in `tiberius`.

### Schema changes

There is no DDL string on the log. An `ALTER TABLE` is a set of catalog and allocation records. v1 does not turn those into SQL.

At each commit the reader re-reads the cached table's column list and allocation unit id. A difference is a distinct error. The stream stops. The embedder reseeds after the DBA's change. The spike records `TRUNCATE`, `ADD`, `DROP`, `ALTER COLUMN`, `sp_rename`, and `DROP TABLE`, and says whether any of them are a reliable `Dropped`, `Renamed`, or `Truncated`. Those kinds are not emitted unless that fixture is unambiguous. v1 does not add `ControlKind::Truncated` unless the spike shows a truncate the existing kinds cannot carry, coordinated with the PostgreSQL core bump. MySQL and MongoDB output stays unchanged.

## Position and truncation

One outer commit is one `CommittedTransaction`. `position`:

```json
{
  "adapter": "mssql",
  "at":    { "commit_lsn": "00000031:00000da0:0001" },
  "after": { "lsn": "00000031:00000da0:0002" }
}
```

- `commit_lsn` is the LSN of `LOP_COMMIT_XACT`, formatted as in the log architecture guide: 4 bytes, 4 bytes, 2 bytes, zero-padded hex. The text sorts in LSN order.
- `after.lsn` is the next LSN the reader will consider. The source does not add one with decimal arithmetic. The next record's LSN, as the file and `fn_dblog` both report it on the fixture, is the value. `sys.dm_db_log_stats.log_end_lsn` is the documented end-of-log check the spike compares against.
- On resume, records at or below the checkpoint's `after.lsn` are not emitted.
- `commit_time` is set when the commit record carries a time the spike can render. Otherwise the field is omitted.

The checkpoint advances only after the callback for that commit returns `Ok`.

**Truncation.** Nothing in this design holds the log. Full recovery keeps a virtual log file until a log backup completes and the engine reuses it. If the checkpoint LSN is no longer in the active log, the gate raises the history-lost error and does not skip ahead. The embedder starts again from "now". The spike records `log_reuse_wait_desc` after the fixture commits, and whether a checkpoint in simple recovery drops those rows from `fn_dblog`. That simple-recovery run is evidence for the full-recovery rule. It is not a second supported mode.

**Memory.** The source holds one transaction until its commit record. A transaction larger than the process is the same open point as the MySQL reader.

## Seed

v1 has no seed. On first connect, with no checkpoint, the reader starts at `sys.dm_db_log_stats.log_end_lsn`. Earlier records are not emitted.

There is no exported snapshot tied to an LSN. A copy taken at the end LSN can miss commits that land during the copy, and a copy taken before it overlaps the tail. The PostgreSQL seed works because a temporary replication slot exports a snapshot at a known LSN. SQL Server does not. A blocking copy is a spike item, not part of v1. The consumer that needs existing rows copies them itself and treats this stream as changes after the start LSN.

## Gates

The source runs the server gates at connect (`skip_gate_check` for lab use, as on MySQL). The table gates run for each `include_tables` entry, and the library exposes them so an embedder can re-run them. Each failure names the object and the fix.

| Check | How |
|-------|-----|
| Server 2022+ | `SERVERPROPERTY('ProductMajorVersion')` |
| Recovery model `FULL` | `sys.databases.recovery_model_desc` |
| CDC off | `sys.databases.is_cdc_enabled = 0` |
| TDE certificate, when `encryption_state` is 3 | `sys.dm_database_encryption_keys.encryptor_thumbprint` matches the supplied certificate |
| Log files readable | `sys.database_files` physical names, then an open of each log file |
| Checkpoint LSN is still in the active log | `sys.dm_db_log_stats` against the checkpoint. A gap is the history-lost error. The source does not skip ahead. |
| Primary key | `sys.indexes.is_primary_key` |
| Not partitioned, not a clustered columnstore, not memory-optimized | `sys.tables`, `sys.indexes` |

The history-lost error is the same class as a purged MySQL binlog or an invalidated Postgres slot.

## CLI

```toml
[source]
adapter = "mssql"
source_id = "mssql://127.0.0.1:1433/app"
host = "replica.example"   # availability-group secondary, not the primary
port = 1433
user = "ce_stream"
password = "CHANGE_ME"
database = "app"
tls = true
log_dir = "/var/opt/mssql/data"  # that secondary's log directory
# Present only when the database uses TDE. Same files as a certificate backup.
tde_certificate = "/var/opt/mssql/tde/app.cer"
tde_private_key = "/var/opt/mssql/tde/app.pvk"
tde_password = "CHANGE_ME"
seed = false
delivery_unit = "transaction"
include_tables = ["dbo.orders"]
```

`seed = false` is the only v1 value. `log_dir` is the directory that contains the files from `sys.database_files`. All existing sinks, `delivery_unit`, `payload_mode`, and `sink.format = "avro"` (commits with a position use `committed-transaction-v2`, as for MongoDB). Exact option names are settled in the crate PR.

## Set aside

| Approach | Why not |
|----------|---------|
| Change data capture | The capture job inserts every change back into the user database. Enabling it at all, including as a logging switch with the job stopped, still creates those tables and is refused on Express. Ruled out. |
| CLR / extended procedure inside SQL Server | Fivetran's online reader. `UNSAFE ASSEMBLY` in the engine's process. |
| `sys.fn_dblog` / `sys.fn_dump_dblog` as the reader | Undocumented. `fn_dblog` sees the active log only and is a heavy scan. The spike uses `fn_dblog` as an oracle next to the file parser. |
| `sp_replcmds` / `sp_repldone` | Replication procedures. The first `sp_replcmds` caller becomes the log reader (error 18752 for the next). This source does not publish the database. |
| Replication article | Turns extra logging on by installing distribution and the log reader. |
| Change tracking | Keys and a net operation. No before-image of non-key columns ([track data changes](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/track-data-changes-sql-server?view=sql-server-ver17)). |
| Change Event Streaming | Still preview. Publishes to Azure Event Hubs or Fabric Eventstream ([overview](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/change-event-streaming/overview?view=sql-server-ver17)). No pull API. |
| Log-backup tail as the only reader | Latency is the backup schedule. v1 tails the active file. Backups are a later path for a host that will not expose that file. |
| TDE with EKM or Azure Key Vault | v1 takes a certificate file and a private-key password. It does not call an HSM. |
| Triggers, audit tables | Writes into the user database. |
| Hash diffs (Fivetran Teleport) | Full scans. No commit order, no resume LSN. |
| Debezium, Kafka Connect | Pipeline frameworks with their own runtime. ce-stream is the library layer. |

## Spike checklist

On `mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04`, CDC off, before the mapping is locked. The file reader and `fn_dblog` are compared on the same commits. Each item records the opcode, the allocation unit, and whether the key and both images were present. Script: [`scripts/spike-mssql.ps1`](../scripts/spike-mssql.ps1). Notes: [`spike-mssql-log.md`](spike-mssql-log.md).

- Shared read of the live log file while the engine is writing. If the open fails, stop. Do not switch the design to change tables or a CLR DLL.
- Insert, update (key unchanged, key changed), delete. Clustered primary key, and a heap with a nonclustered primary key. CDC stays off.
- Fixed-length columns only, and a table with a variable-length column.
- A nonclustered index on an included table: those log records are not a second row event.
- A multi-statement transaction emitted as one commit. A rolled-back one ends in `LOP_ABORT_XACT` and is not emitted. Nested `BEGIN TRAN` / `COMMIT` is one log transaction. `ROLLBACK` to a savepoint leaves a compensating modify before the commit.
- Two included tables in one transaction: one commit, row order matching the log.
- Minimal logging (`INSERT … WITH (TABLOCK)` into an empty heap, or the statement the lab finds): fail closed, and name the opcode.
- A LOB update. Record whether the value is inline.
- `TRUNCATE`, `ALTER TABLE` add, drop, retype, `sp_rename`, `DROP TABLE`. Record the opcodes. Say whether any are a clean `Dropped`, `Renamed`, or truncate.
- `log_reuse_wait_desc` after the commits. A separate simple-recovery database: checkpoint, then whether `fn_dblog` still returns those rows.
- Kill the reader on a partial trailing block. Restart from the checkpoint. No gap, no duplicate commit.
- TDE on, with the exported certificate. Done on 2022 CU26. The raw log does not contain the row. Decrypting the block recovers the same 57 bytes `fn_dblog` shows. Layout in [Transparent data encryption](#transparent-data-encryption).
- Quiet database: the reader blocks on the file and emits nothing.
- `tiberius` 0.13.0 against the 2025 image, and which `MSSQL_PID` that image accepts.
- One pass of the same list on `2025-CU9-ubuntu-24.04` before release. A record the 2022 fixture decoded and the 2025 log changed fails the release. The layout is pinned to builds the suite has read.

## Order

One issue, one branch, reviewable commits. No core change unless the spike forces one.

1. **Spike the images.** `fn_dblog` fixtures for the checklist's row shapes, CDC off. If the live log file cannot be opened, stop. No parser beyond capturing bytes.
2. **File reader.** Tail the mounted log, refuse a partial trailing block, match LSN and opcode to the `fn_dblog` fixture. Unknown opcode fails.
3. **`ce-stream-mssql`:** map the fixture's opcodes to `CommittedTransaction`, position and checkpoint, gates. Unit tests on the captured bytes. `rust-version = "1.88"`.
4. **CLI** `source.adapter = "mssql"` to stdout and the existing sinks. Run the checklist as ignored live tests.
5. **Docs** (`library.md`, `ops-e2e.md`, README), changelog, release notes. `2025-CU9-ubuntu-24.04` pass.

If a later build moves an opcode, that is a new fixture and a reader change, not a silent skip.

## Tests

CI: unit tests for the fixture decoder, commit grouping, LSN formatting, position serde, the partial-block rule, and the history-lost gate (`cargo test -p ce-stream-core -p ce-stream-mysql -p ce-stream-mongo -p ce-stream-mssql`). Core, MySQL, and MongoDB output stays unchanged. CI does not need a SQL Server. The fixtures are checked in.

Lab (ignored by default, `2022-CU26-ubuntu-22.04`, log directory mounted): the spike checklist. One pass on `2025-CU9-ubuntu-24.04` before release.

## Out of scope

SQL Server older than 2022, Azure SQL Database, Azure SQL Managed Instance, RDS, reading the primary's live log when a replica exists, a seed, TDE protected by EKM or Azure Key Vault, simple recovery, reading log backups as the tail, a CLR assembly, `fn_dblog` as the production reader, replication procedures, change data capture, change tracking, Change Event Streaming, typed value conversion, CLR column types, partitioned tables, memory-optimized tables, Windows authentication, and Avro schema changes.
