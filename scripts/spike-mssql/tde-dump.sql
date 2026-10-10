-- The private-key password is a sqlcmd variable: run with
-- -v TDE_PVK_PASSWORD=<the value in local/lab.env>.
USE ce_stream_tde;
GO
SELECT id, marker FROM dbo.t;
GO
SELECT COUNT(*) AS log_rows FROM sys.fn_dblog(NULL, NULL);
GO
SELECT TOP 40
    [Current LSN],
    Operation,
    Context,
    AllocUnitName,
    [Transaction Name],
    [Page ID],
    DATALENGTH([RowLog Contents 0]) AS c0_len
FROM sys.fn_dblog(NULL, NULL)
ORDER BY [Current LSN] DESC;
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
DBCC PAGE(N'ce_stream_tde', 1, 360, 2);
GO
