# Shared helpers for MySQL crash harness. ASCII-only (Windows PowerShell 5.1).

function Get-CeStreamRepoRoot {
    if (-not [string]::IsNullOrWhiteSpace($script:CeStreamRepoRootOverride)) {
        return $script:CeStreamRepoRootOverride
    }
    return (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
}

function Get-CapturePassword {
    . (Join-Path $PSScriptRoot "..\..\perf\_common.ps1")
    return Get-PerfCapturePassword
}

function Get-MysqlCliArgs {
    param([string]$MysqlDefaults)
    . (Join-Path $PSScriptRoot "..\..\perf\_common.ps1")
    $path = Resolve-MysqlDefaults -MysqlDefaults $MysqlDefaults
    return @("--defaults-extra-file=$path")
}

function Write-CrashHarnessToml {
    param(
        [string]$Path,
        [string]$SinkUrl,
        [string]$CheckpointPath,
        [ValidateSet("row", "transaction")]
        [string]$DeliveryUnit = "row",
        [int]$ServerId = 19301,
        [int]$QueueCapacity = 64,
        [int]$MysqlPort = 3306,
        [string]$MysqlHost = "127.0.0.1",
        [bool]$Tls = $true
    )
    $password = Get-CapturePassword
    $cp = $CheckpointPath.Replace('\', '/')
    $tlsLiteral = if ($Tls) { "true" } else { "false" }
    $content = @"
[source]
adapter = "mysql"
source_id = "mysql://${MysqlHost}:${MysqlPort}/ce-stream-crash-harness"
host = "$MysqlHost"
port = $MysqlPort
user = "ce_stream"
password = "$password"
server_id = $ServerId
tls = $tlsLiteral
payload_mode = "full"
delivery_unit = "$DeliveryUnit"
queue_capacity = $QueueCapacity
include_tables = ["ce_stream_spike.t1"]

[checkpoint]
path = "$cp"

[sink]
kind = "http"
url = "$SinkUrl"
format = "json"
"@
    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    [System.IO.File]::WriteAllText($Path, $content, $utf8NoBom)
}

function Get-CheckpointGtid {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }
    $raw = Get-Content -LiteralPath $Path -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return $null
    }
    $doc = $raw | ConvertFrom-Json
    if ($doc.payload -and $doc.payload.gtid) {
        return [string]$doc.payload.gtid
    }
    return $null
}

function Wait-CheckpointGtid {
    param(
        [string]$Path,
        [int]$TimeoutSec = 60
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $gtid = Get-CheckpointGtid -Path $Path
        if (-not [string]::IsNullOrWhiteSpace($gtid)) {
            return $gtid
        }
        Start-Sleep -Milliseconds 250
    }
    throw "timeout waiting for checkpoint gtid at $Path"
}

function Invoke-MysqlSqlFile {
    param(
        [string[]]$MysqlArgs,
        [string]$Sql,
        [string]$Label = "mysql"
    )
    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    $tmp = Join-Path $env:TEMP ("ce-stream-crash-{0}-{1}.sql" -f $Label, [guid]::NewGuid().ToString("N"))
    try {
        [System.IO.File]::WriteAllText($tmp, $Sql, $utf8NoBom)
        Get-Content -LiteralPath $tmp -Raw | & mysql @MysqlArgs 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "$Label failed (exit $LASTEXITCODE)"
        }
    }
    finally {
        if (Test-Path $tmp) { Remove-Item -Force $tmp }
    }
}

function Invoke-MysqlScalar {
    param(
        [string[]]$MysqlArgs,
        [string]$Sql
    )
    $out = & mysql @MysqlArgs -N -B -e $Sql 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "mysql scalar failed: $out"
    }
    if ($out -is [System.Array]) {
        return ($out | Select-Object -First 1)
    }
    return [string]$out
}

function Assert-CaptureGates {
    param([string[]]$MysqlArgs)
    $meta = Invoke-MysqlScalar -MysqlArgs $MysqlArgs -Sql "SELECT @@GLOBAL.binlog_row_metadata;"
    $format = Invoke-MysqlScalar -MysqlArgs $MysqlArgs -Sql "SELECT @@GLOBAL.binlog_format;"
    $logBin = Invoke-MysqlScalar -MysqlArgs $MysqlArgs -Sql "SELECT @@GLOBAL.log_bin;"
    Write-Host ("binlog_row_metadata={0} binlog_format={1} log_bin={2}" -f $meta, $format, $logBin)
    if ($meta -ne "FULL") {
        throw "binlog_row_metadata must be FULL (set in my.ini and restart MySQL). Got: $meta"
    }
    if ($format -ne "ROW") {
        throw "binlog_format must be ROW. Got: $format"
    }
    if ($logBin -ne "1") {
        throw "log_bin must be ON. Got: $logBin"
    }
}

function Install-CeStreamLab {
    param(
        [string[]]$MysqlArgs,
        [string]$RepoRoot
    )
    $password = Get-CapturePassword
    $pwSql = $password.Replace("'", "''")
    $setupPath = Join-Path $RepoRoot "scripts\spike-setup.sql"
    if (-not (Test-Path $setupPath)) {
        throw "missing spike-setup.sql at $setupPath"
    }
    $sql = (Get-Content -LiteralPath $setupPath -Raw).Replace("CHANGE_ME", $pwSql)
    Invoke-MysqlSqlFile -MysqlArgs $MysqlArgs -Sql $sql -Label "setup-lab"
}

function New-MultiRowTxnSql {
    param(
        [string]$Prefix,
        [int]$Rows = 3
    )
    $sb = New-Object System.Text.StringBuilder
    [void]$sb.AppendLine("START TRANSACTION;")
    for ($i = 1; $i -le $Rows; $i++) {
        $name = "{0}-{1}" -f $Prefix, $i
        $nameSql = $name.Replace("'", "''")
        [void]$sb.AppendLine("INSERT INTO ce_stream_spike.t1(name) VALUES ('$nameSql');")
    }
    [void]$sb.AppendLine("COMMIT;")
    return $sb.ToString()
}

function Stop-ProcessSafe {
    param($Proc)
    if ($Proc -and -not $Proc.HasExited) {
        Stop-Process -Id $Proc.Id -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 500
    }
}

function Wait-MysqlReady {
    param(
        [string[]]$MysqlArgs,
        [int]$TimeoutSec = 120
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $v = Invoke-MysqlScalar -MysqlArgs $MysqlArgs -Sql "SELECT 1;"
            if ($v -eq "1") { return }
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    throw "MySQL not ready after ${TimeoutSec}s"
}
