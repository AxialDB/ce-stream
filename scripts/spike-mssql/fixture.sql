-- Spike fixture. CDC stays off. Named transactions so fn_dblog can find them.
-- Lab only. Run against a fresh instance.

SET NOCOUNT ON;

IF DB_ID(N'ce_stream_spike') IS NOT NULL
BEGIN
    ALTER DATABASE ce_stream_spike SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ce_stream_spike;
END;

CREATE DATABASE ce_stream_spike;
ALTER DATABASE ce_stream_spike SET RECOVERY FULL;
GO

USE ce_stream_spike;

SELECT
    DB_NAME() AS database_name,
    recovery_model_desc,
    is_cdc_enabled,
    log_reuse_wait_desc,
    containment_desc
FROM sys.databases
WHERE name = N'ce_stream_spike';

SELECT SERVERPROPERTY('Edition') AS edition, SERVERPROPERTY('ProductVersion') AS product_version, SERVERPROPERTY('ProductLevel') AS product_level;

SELECT name, type_desc, physical_name, size
FROM sys.database_files
ORDER BY type_desc, name;

CREATE TABLE dbo.fixed_pk (
    id int NOT NULL CONSTRAINT pk_fixed PRIMARY KEY,
    n int NOT NULL,
    flag bit NOT NULL
);

CREATE TABLE dbo.var_pk (
    id int NOT NULL CONSTRAINT pk_var PRIMARY KEY,
    name nvarchar(40) NOT NULL
);

CREATE TABLE dbo.heap_pk (
    id int NOT NULL,
    n int NOT NULL,
    CONSTRAINT pk_heap PRIMARY KEY NONCLUSTERED (id)
);
CREATE INDEX ix_heap_n ON dbo.heap_pk (n);

CREATE TABLE dbo.two_a (id int NOT NULL CONSTRAINT pk_two_a PRIMARY KEY, v int NOT NULL);
CREATE TABLE dbo.two_b (id int NOT NULL CONSTRAINT pk_two_b PRIMARY KEY, v int NOT NULL);

CREATE TABLE dbo.lob_t (
    id int NOT NULL CONSTRAINT pk_lob PRIMARY KEY,
    note nvarchar(max) NOT NULL
);

CREATE TABLE dbo.bulk_heap (id int NOT NULL, n int NOT NULL);

BEGIN TRAN spike_ins_fixed;
INSERT INTO dbo.fixed_pk (id, n, flag) VALUES (1, 10, 1);
COMMIT;

BEGIN TRAN spike_upd_fixed;
UPDATE dbo.fixed_pk SET n = 11 WHERE id = 1;
COMMIT;

BEGIN TRAN spike_upd_key;
UPDATE dbo.fixed_pk SET id = 2 WHERE id = 1;
COMMIT;

BEGIN TRAN spike_del_fixed;
DELETE FROM dbo.fixed_pk WHERE id = 2;
COMMIT;

BEGIN TRAN spike_ins_var;
INSERT INTO dbo.var_pk (id, name) VALUES (1, N'open');
COMMIT;

BEGIN TRAN spike_upd_var;
UPDATE dbo.var_pk SET name = N'shipped' WHERE id = 1;
COMMIT;

BEGIN TRAN spike_ins_heap;
INSERT INTO dbo.heap_pk (id, n) VALUES (1, 10);
COMMIT;

BEGIN TRAN spike_upd_heap;
UPDATE dbo.heap_pk SET n = 11 WHERE id = 1;
COMMIT;

BEGIN TRAN spike_del_heap;
DELETE FROM dbo.heap_pk WHERE id = 1;
COMMIT;

BEGIN TRAN spike_two;
INSERT INTO dbo.two_a (id, v) VALUES (1, 1);
INSERT INTO dbo.two_b (id, v) VALUES (1, 2);
COMMIT;

BEGIN TRAN spike_rollback;
INSERT INTO dbo.fixed_pk (id, n, flag) VALUES (9, 9, 0);
ROLLBACK;

BEGIN TRAN spike_nested;
INSERT INTO dbo.fixed_pk (id, n, flag) VALUES (3, 1, 0);
BEGIN TRAN inner_one;
UPDATE dbo.fixed_pk SET n = 2 WHERE id = 3;
COMMIT;
COMMIT;

BEGIN TRAN spike_save;
INSERT INTO dbo.fixed_pk (id, n, flag) VALUES (4, 1, 0);
SAVE TRAN mark;
UPDATE dbo.fixed_pk SET n = 99 WHERE id = 4;
ROLLBACK TRAN mark;
COMMIT;

BEGIN TRAN spike_lob;
INSERT INTO dbo.lob_t (id, note) VALUES (1, REPLICATE(N'x', 100));
COMMIT;

BEGIN TRAN spike_lob_big;
UPDATE dbo.lob_t SET note = REPLICATE(N'y', 4000) WHERE id = 1;
COMMIT;

BEGIN TRAN spike_bulk;
INSERT INTO dbo.bulk_heap WITH (TABLOCK) (id, n)
SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 1
FROM sys.all_objects a CROSS JOIN sys.all_objects b;
COMMIT;

BEGIN TRAN spike_truncate;
TRUNCATE TABLE dbo.bulk_heap;
COMMIT;

BEGIN TRAN spike_add_col;
ALTER TABLE dbo.var_pk ADD extra int NULL;
COMMIT;

BEGIN TRAN spike_rename;
EXEC sp_rename N'dbo.two_a', N'two_a_renamed';
COMMIT;

BEGIN TRAN spike_drop;
DROP TABLE dbo.two_b;
COMMIT;

SELECT
    recovery_model_desc,
    is_cdc_enabled,
    log_reuse_wait_desc
FROM sys.databases
WHERE name = N'ce_stream_spike';

IF OBJECT_ID('tempdb..#log') IS NOT NULL DROP TABLE #log;

SELECT
    [Current LSN],
    Operation,
    Context,
    [Transaction ID],
    [Transaction Name],
    AllocUnitName,
    [Begin Time],
    DATALENGTH([RowLog Contents 0]) AS len0,
    DATALENGTH([RowLog Contents 1]) AS len1,
    DATALENGTH([RowLog Contents 2]) AS len2,
    DATALENGTH([RowLog Contents 3]) AS len3,
    CONVERT(varchar(180), [RowLog Contents 0], 1) AS hex0,
    CONVERT(varchar(180), [RowLog Contents 1], 1) AS hex1,
    CONVERT(varchar(120), [RowLog Contents 2], 1) AS hex2
INTO #log
FROM sys.fn_dblog(NULL, NULL);

SELECT [Transaction ID]
INTO #ids
FROM #log
WHERE [Transaction Name] LIKE N'spike[_]%';

SELECT
    [Current LSN],
    Operation,
    Context,
    [Transaction Name],
    AllocUnitName,
    len0, len1, len2, len3,
    hex0, hex1, hex2
FROM #log
WHERE [Transaction ID] IN (SELECT [Transaction ID] FROM #ids)
   OR [Transaction Name] LIKE N'spike[_]%'
ORDER BY [Current LSN];

-- Simple recovery: does a checkpoint drop the rows from fn_dblog?
IF DB_ID(N'ce_stream_spike_simple') IS NOT NULL
BEGIN
    ALTER DATABASE ce_stream_spike_simple SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ce_stream_spike_simple;
END;

CREATE DATABASE ce_stream_spike_simple;
ALTER DATABASE ce_stream_spike_simple SET RECOVERY SIMPLE;
GO

USE ce_stream_spike_simple;

CREATE TABLE dbo.t (id int NOT NULL CONSTRAINT pk_t PRIMARY KEY, n int NOT NULL);

BEGIN TRAN spike_simple;
INSERT INTO dbo.t (id, n) VALUES (1, 1);
COMMIT;

SELECT recovery_model_desc, log_reuse_wait_desc, is_cdc_enabled
FROM sys.databases
WHERE name = N'ce_stream_spike_simple';

SELECT COUNT(*) AS rows_before_checkpoint
FROM sys.fn_dblog(NULL, NULL)
WHERE [Transaction Name] = N'spike_simple'
   OR [Transaction ID] IN (
        SELECT [Transaction ID] FROM sys.fn_dblog(NULL, NULL) WHERE [Transaction Name] = N'spike_simple'
      );

CHECKPOINT;

SELECT COUNT(*) AS rows_after_checkpoint
FROM sys.fn_dblog(NULL, NULL)
WHERE [Transaction Name] = N'spike_simple'
   OR [Transaction ID] IN (
        SELECT [Transaction ID] FROM sys.fn_dblog(NULL, NULL) WHERE [Transaction Name] = N'spike_simple'
      );
