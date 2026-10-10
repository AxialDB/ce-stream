-- Second TDE pass: full recovery so the insert stays in the log.
USE master;
GO
ALTER DATABASE ce_stream_tde SET RECOVERY FULL;
GO
BACKUP DATABASE ce_stream_tde TO DISK = N'/tmp/ce_stream_tde.bak' WITH INIT, COMPRESSION;
GO
USE ce_stream_tde;
GO
BEGIN TRAN spike_tde_log;
INSERT INTO dbo.t (id, marker) VALUES (2, N'CESTREAM_TDE_LOG_9c2e');
COMMIT TRAN;
GO
SELECT
    [Current LSN],
    Operation,
    Context,
    AllocUnitName,
    [Page ID],
    DATALENGTH([RowLog Contents 0]) AS c0_len,
    CONVERT(varchar(120), [RowLog Contents 0], 1) AS c0
FROM sys.fn_dblog(NULL, NULL)
WHERE [Transaction Name] = N'spike_tde_log'
   OR AllocUnitName LIKE N'%dbo.t%';
GO
SELECT
    vlf_begin_offset,
    vlf_size_mb,
    vlf_active,
    CONVERT(varchar(80), vlf_encryptor_thumbprint, 1) AS vlf_thumbprint
FROM sys.dm_db_log_info(DB_ID());
GO
