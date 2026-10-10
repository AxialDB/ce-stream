-- TDE spike. Certificate lives in master. CDC stays off.
-- The two passwords are sqlcmd variables: run-tde-on-buildcomp.sh passes
-- MSSQL_DMK_PASSWORD and TDE_PVK_PASSWORD with -v. The private-key file is
-- gitignored with the rest of out/.

USE master;
GO
IF DB_ID(N'ce_stream_tde') IS NOT NULL
BEGIN
    ALTER DATABASE ce_stream_tde SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ce_stream_tde;
END
GO
IF EXISTS (SELECT 1 FROM sys.certificates WHERE name = N'ce_stream_tde')
    DROP CERTIFICATE ce_stream_tde;
GO
IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = N'##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = N'$(MSSQL_DMK_PASSWORD)';
GO
CREATE CERTIFICATE ce_stream_tde WITH SUBJECT = N'ce-stream tde spike';
GO
CREATE DATABASE ce_stream_tde;
GO
USE ce_stream_tde;
GO
CREATE DATABASE ENCRYPTION KEY
    WITH ALGORITHM = AES_256
    ENCRYPTION BY SERVER CERTIFICATE ce_stream_tde;
GO
ALTER DATABASE ce_stream_tde SET ENCRYPTION ON;
GO

DECLARE @i int = 0;
WHILE @i < 90
BEGIN
    IF EXISTS (
        SELECT 1
        FROM sys.dm_database_encryption_keys
        WHERE database_id = DB_ID(N'ce_stream_tde')
          AND encryption_state = 3
    )
        BREAK;
    WAITFOR DELAY '00:00:01';
    SET @i += 1;
END
GO

CREATE TABLE dbo.t (
    id int NOT NULL CONSTRAINT pk_t PRIMARY KEY CLUSTERED,
    marker nvarchar(40) NOT NULL
);
GO
BEGIN TRAN spike_tde;
INSERT INTO dbo.t (id, marker) VALUES (1, N'CESTREAM_TDE_MARKER_7f3a');
COMMIT TRAN;
GO
CHECKPOINT;
GO

SELECT
    CAST(SERVERPROPERTY(N'ProductVersion') AS nvarchar(40)) AS version,
    CAST(SERVERPROPERTY(N'Edition') AS nvarchar(80)) AS edition;
GO
SELECT
    db_name(database_id) AS db_name,
    encryption_state,
    key_algorithm,
    key_length,
    encryptor_type,
    CONVERT(varchar(80), encryptor_thumbprint, 1) AS thumbprint
FROM sys.dm_database_encryption_keys
WHERE database_id = DB_ID(N'ce_stream_tde');
GO
SELECT
    vlf_begin_offset,
    vlf_size_mb,
    vlf_active,
    CONVERT(varchar(80), vlf_encryptor_thumbprint, 1) AS vlf_thumbprint
FROM sys.dm_db_log_info(DB_ID(N'ce_stream_tde'));
GO
SELECT
    [Current LSN],
    Operation,
    Context,
    AllocUnitName,
    [Page ID],
    [Slot ID],
    DATALENGTH([RowLog Contents 0]) AS c0_len,
    CONVERT(varchar(200), [RowLog Contents 0], 1) AS c0
FROM sys.fn_dblog(NULL, NULL)
WHERE [Transaction Name] = N'spike_tde'
   OR AllocUnitName LIKE N'%dbo.t%'
   OR Operation IN (N'LOP_COMMIT_XACT', N'LOP_BEGIN_XACT');
GO
SELECT physical_name, type_desc
FROM sys.database_files;
GO
SELECT
    allocated_page_file_id,
    allocated_page_page_id,
    page_type_desc,
    is_allocated
FROM sys.dm_db_database_page_allocations(DB_ID(), OBJECT_ID(N'dbo.t'), NULL, NULL, N'DETAILED')
WHERE is_allocated = 1
  AND page_type_desc = N'DATA_PAGE';
GO

USE master;
GO
BACKUP CERTIFICATE ce_stream_tde
    TO FILE = N'/var/opt/mssql/data/ce_stream_tde.cer'
    WITH PRIVATE KEY (
        FILE = N'/var/opt/mssql/data/ce_stream_tde.pvk',
        ENCRYPTION BY PASSWORD = N'$(TDE_PVK_PASSWORD)'
    );
GO

DBCC TRACEON(3604);
GO
DBCC PAGE(N'ce_stream_tde', 1, 0, 3);
GO
