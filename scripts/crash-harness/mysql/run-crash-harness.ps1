# Gate 0 manual crash harness: kill ce-stream after first post-commit row, restart, assert full txn redelivery.
# Run from ce-stream repo root. Requires MySQL 9.x lab + CE_STREAM_PASSWORD.
# ASCII-only (Windows PowerShell 5.1).

param(
    [string]$MysqlDefaults = "",
    [string]$MysqlServiceName = "MySQL97",
    [switch]$Wsl,
    [switch]$RestartMysql,
    [switch]$SetupLab,
    [switch]$SkipBuild,
    [switch]$SkipLinuxInstall,
    [ValidateSet("row", "transaction")]
    [string]$DeliveryUnit = "row",
    [int]$TxnRows = 3,
    [int]$ListenPort = 18082,
    [int]$ServerId = 19301,
    [int]$MysqlPort = 3306,
    [string]$MysqlHost = "127.0.0.1",
    [switch]$MysqlTls,
    [int]$WslMysqlPort = 3307
)

$ErrorActionPreference = "Stop"

$HarnessDir = $PSScriptRoot
. (Join-Path $HarnessDir "_common.ps1")
. (Join-Path $HarnessDir "..\..\perf\_common.ps1")

$repoRoot = Get-CeStreamRepoRoot

if ($Wsl) {
    $linuxScript = Join-Path $HarnessDir "run-crash-harness.sh"
    if (-not (Test-Path $linuxScript)) {
        throw "missing $linuxScript"
    }

    $wslRepo = (wsl wslpath -a $repoRoot).Trim()
    $wslHarness = (wsl wslpath -a $HarnessDir).Trim()

    $capturePassword = $env:CE_STREAM_PASSWORD
    if ([string]::IsNullOrWhiteSpace($capturePassword)) {
        $capturePassword = Get-CapturePassword
    }
    $pwEsc = $capturePassword.Replace("'", "'\''")

    $bashArgs = @(
        "export CE_STREAM_REPO='${wslRepo}'",
        "export CE_STREAM_PASSWORD='${pwEsc}'",
        "export MYSQL_PORT=${WslMysqlPort}",
        "export MYSQL_TLS=false",
        "sed -i 's/\r$//' '${wslHarness}/_common.sh' '${wslHarness}/run-crash-harness.sh' '${wslHarness}/install-linux.sh'"
    )

    if (-not $SkipLinuxInstall) {
        $bashArgs += "bash '${wslHarness}/install-linux.sh'"
    }

    $runArgs = @("'${wslHarness}/run-crash-harness.sh'")
    if ($SetupLab) { $runArgs += "--setup-lab" }
    if ($SkipBuild) { $runArgs += "--skip-build" }
    if ($RestartMysql) { $runArgs += "--restart-mysql" }
    if ($DeliveryUnit -ne "row") { $runArgs += @("--delivery-unit", $DeliveryUnit) }
    if ($TxnRows -ne 3) { $runArgs += @("--txn-rows", "$TxnRows") }
    if ($ListenPort -ne 18082) { $runArgs += @("--listen-port", "$ListenPort") }
    if ($ServerId -ne 19301) { $runArgs += @("--server-id", "$ServerId") }
    $runArgs += @("--mysql-port", "$WslMysqlPort")

    $bashArgs += ("bash " + ($runArgs -join " "))
    $cmd = $bashArgs -join " && "
    Write-Host "WSL harness: MySQL 127.0.0.1:${WslMysqlPort} ..."
    wsl bash -lc $cmd
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    exit 0
}

$mysqlArgs = Get-MysqlCliArgs -MysqlDefaults $MysqlDefaults

if ($RestartMysql) {
    Write-Host "Restarting MySQL service $MysqlServiceName ..."
    Restart-Service -Name $MysqlServiceName -Force
    Wait-MysqlReady -MysqlArgs $mysqlArgs -TimeoutSec 180
}

Assert-CaptureGates -MysqlArgs $mysqlArgs

if ($SetupLab) {
    Write-Host "Installing ce_stream_spike lab schema (scripts/spike-setup.sql) ..."
    Install-CeStreamLab -MysqlArgs $mysqlArgs -RepoRoot $repoRoot
}

$outDir = Join-Path $HarnessDir "out"
if (-not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir | Out-Null
}

$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $outDir $runId
New-Item -ItemType Directory -Path $runDir | Out-Null

$checkpointPath = Join-Path $runDir "checkpoint.json"
$tomlPath = Join-Path $runDir "ce-stream.toml"
$sinkUrl = "http://127.0.0.1:$ListenPort/events"
$baseUrl = "http://127.0.0.1:$ListenPort"

$env:CARGO_TARGET_DIR = Join-Path $repoRoot "target"
$env:RUST_LOG = "info"

$sinkExe = Join-Path $env:CARGO_TARGET_DIR "release\ce-stream-perf-sink.exe"
$cliExe = Join-Path $env:CARGO_TARGET_DIR "release\ce-stream.exe"

if (-not $SkipBuild) {
    Write-Host "Building ce-stream-perf-sink + ce-stream-cli ..."
    Push-Location $repoRoot
    try {
        cargo build -p ce-stream-perf-sink -p ce-stream-cli --release
        if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
    }
    finally {
        Pop-Location
    }
}

if (-not (Test-Path $sinkExe)) { throw "missing $sinkExe" }
if (-not (Test-Path $cliExe)) { throw "missing $cliExe" }

$useTls = $MysqlTls.IsPresent
Write-CrashHarnessToml -Path $tomlPath -SinkUrl $sinkUrl -CheckpointPath $checkpointPath `
    -DeliveryUnit $DeliveryUnit -ServerId $ServerId -MysqlPort $MysqlPort -MysqlHost $MysqlHost -Tls $useTls

$sinkLog = Join-Path $runDir "sink.log"
$sinkErr = Join-Path $runDir "sink.err"
$cliLog = Join-Path $runDir "cli-phase1.log"
$cliErr = Join-Path $runDir "cli-phase1.err"
$cliLog2 = Join-Path $runDir "cli-phase2.log"
$cliErr2 = Join-Path $runDir "cli-phase2.err"

$sinkProc = $null
$cliProc = $null

function Start-PerfSink {
    param(
        [string]$StdoutLog,
        [string]$StderrLog,
        [int]$StallAfter = 0
    )
    $args = @("--listen", "127.0.0.1:$ListenPort", "--delay-ms", "0")
    if ($StallAfter -gt 0) {
        $args += @("--stall-after", "$StallAfter")
    }
    return Start-Process -FilePath $sinkExe `
        -ArgumentList $args `
        -PassThru -NoNewWindow `
        -RedirectStandardOutput $StdoutLog `
        -RedirectStandardError $StderrLog
}

function Start-Capture {
    param(
        [string]$StdoutLog,
        [string]$StderrLog
    )
    $argLine = '--config "{0}"' -f $tomlPath
    return Start-Process -FilePath $cliExe `
        -ArgumentList $argLine `
        -PassThru -NoNewWindow `
        -RedirectStandardOutput $StdoutLog `
        -RedirectStandardError $StderrLog `
        -WorkingDirectory $repoRoot
}

try {
    Write-Host "Starting perf sink on $baseUrl ..."
    $sinkProc = Start-PerfSink -StdoutLog $sinkLog -StderrLog $sinkErr -StallAfter 0

    Start-Sleep -Seconds 1
    Reset-SinkStats -BaseUrl $baseUrl

    if (Test-Path $checkpointPath) { Remove-Item -Force $checkpointPath }

    Write-Host "Phase 1: warmup single-row commit (establishes durable checkpoint) ..."
    $cliProc = Start-Capture -StdoutLog $cliLog -StderrLog $cliErr
    Start-Sleep -Seconds 3
    if ($cliProc.HasExited) {
        if (Test-Path $cliErr) { Get-Content $cliErr }
        throw "ce-stream exited during warmup start"
    }

    $warmupTag = "warmup-$runId"
    $warmupSql = "INSERT INTO ce_stream_spike.t1(name) VALUES ('$warmupTag');"
    Invoke-MysqlSqlFile -MysqlArgs $mysqlArgs -Sql $warmupSql -Label "warmup-insert"

    $null = Wait-SinkCount -BaseUrl $baseUrl -Expected 1 -TimeoutSec 60
    $warmupGtid = Wait-CheckpointGtid -Path $checkpointPath -TimeoutSec 60
    Write-Host "Warmup checkpoint gtid: $warmupGtid"

    Reset-SinkStats -BaseUrl $baseUrl

    Write-Host "Phase 2: $TxnRows-row txn, stall sink after row 1, kill ce-stream mid-fanout ..."
    Stop-ProcessSafe -Proc $sinkProc
    $sinkProc = Start-PerfSink -StdoutLog $sinkLog -StderrLog $sinkErr -StallAfter 1
    Start-Sleep -Seconds 1
    Reset-SinkStats -BaseUrl $baseUrl

    $crashTag = "crash-$runId"
    $txnSql = New-MultiRowTxnSql -Prefix $crashTag -Rows $TxnRows
    Invoke-MysqlSqlFile -MysqlArgs $mysqlArgs -Sql $txnSql -Label "crash-txn"

    $null = Wait-SinkCount -BaseUrl $baseUrl -Expected 1 -TimeoutSec 60
    $partialStats = Get-SinkStats -BaseUrl $baseUrl
    Start-Sleep -Milliseconds 100
    Stop-ProcessSafe -Proc $cliProc
    $cliProc = $null
    Stop-ProcessSafe -Proc $sinkProc
    $sinkProc = $null

    $gtidAfterKill = Get-CheckpointGtid -Path $checkpointPath
    if ($gtidAfterKill -ne $warmupGtid) {
        throw ("checkpoint advanced after partial delivery (warmup={0}, after_kill={1})" -f $warmupGtid, $gtidAfterKill)
    }
    Write-Host "Checkpoint unchanged after kill (expected)."

    if ([int]$partialStats.received -ne 1) {
        throw ("expected 1 partial delivery before kill, got {0}" -f $partialStats.received)
    }

    Write-Host "Phase 3: restart ce-stream, expect full txn redelivery ..."
    $sinkProc = Start-PerfSink -StdoutLog $sinkLog -StderrLog $sinkErr -StallAfter 0
    Start-Sleep -Seconds 1
    Reset-SinkStats -BaseUrl $baseUrl
    $cliProc = Start-Capture -StdoutLog $cliLog2 -StderrLog $cliErr2
    Start-Sleep -Seconds 5
    if ($cliProc.HasExited) {
        $statsAfterExit = Get-SinkStats -BaseUrl $baseUrl
        if ([int]$statsAfterExit.received -lt $TxnRows -and $DeliveryUnit -eq "row") {
            if (Test-Path $cliErr2) { Get-Content $cliErr2 }
            throw "ce-stream exited during restart before delivering $TxnRows rows (got $($statsAfterExit.received))"
        }
    }

    if ($DeliveryUnit -eq "row") {
        $null = Wait-SinkCount -BaseUrl $baseUrl -Expected $TxnRows -TimeoutSec 90
        $redelivered = [int](Get-SinkStats -BaseUrl $baseUrl).received
        if ($redelivered -lt $TxnRows) {
            throw ("expected >={0} row redeliveries, got {1}" -f $TxnRows, $redelivered)
        }
        Write-Host "Redelivered $redelivered row CloudEvents (>= $TxnRows)."
    }
    else {
        $null = Wait-SinkCount -BaseUrl $baseUrl -Expected 1 -TimeoutSec 90
        Write-Host "Redelivered 1 committed-transaction envelope."
    }

    $finalGtid = Wait-CheckpointGtid -Path $checkpointPath -TimeoutSec 60
    if ($finalGtid -eq $warmupGtid) {
        throw "checkpoint did not advance after successful full redelivery"
    }
    Write-Host "Final checkpoint gtid: $finalGtid"

    $summary = [ordered]@{
        run_id = $runId
        target = "windows"
        mysql_host = $MysqlHost
        mysql_port = $MysqlPort
        delivery_unit = $DeliveryUnit
        txn_rows = $TxnRows
        warmup_gtid = $warmupGtid
        gtid_after_kill = $gtidAfterKill
        final_gtid = $finalGtid
        partial_deliveries = 1
        redelivery_mode = $DeliveryUnit
        pass = $true
        artifacts = $runDir
    }
    $summaryPath = Join-Path $runDir "summary.json"
    $summary | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $summaryPath -Encoding UTF8

    Write-Host ""
    Write-Host "ce-stream crash harness PASS" -ForegroundColor Green
    Write-Host "Artifacts: $runDir"
}
finally {
    Stop-ProcessSafe -Proc $cliProc
    Stop-ProcessSafe -Proc $sinkProc
}
