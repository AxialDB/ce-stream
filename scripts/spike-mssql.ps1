# SQL Server log spike. CDC stays off.
# The Linux lab is buildcomp; its address and login are in the ignored
# scripts/spike-mssql/local/lab.env (see lab.env.example). Run
# scripts/spike-mssql/run-on-buildcomp.sh there. This Windows script is not
# that lab: Docker here is Windows containers, so it cannot run the Linux image.
# -Mode local talks to a local MSSQLSERVER service if one is installed. The
# Start menu may only show SQL Server Setup. -Mode docker is for a Linux
# Docker engine and publishes 127.0.0.1 only. The sa password comes from
# MSSQL_SA_PASSWORD or from that lab.env file.

param(
    [ValidateSet("auto", "docker", "local")]
    [string] $Mode = "auto"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$outDir = Join-Path $repo "scripts\spike-mssql\out"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$image = "mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04"
$name = "ce-stream-mssql-spike"
$port = 14333
$saPassword = $env:MSSQL_SA_PASSWORD
$labEnv = Join-Path $repo "scripts\spike-mssql\local\lab.env"
if (-not $saPassword -and (Test-Path $labEnv)) {
    $line = Get-Content $labEnv | Where-Object { $_ -match '^MSSQL_SA_PASSWORD=' } | Select-Object -First 1
    if ($line) { $saPassword = $line.Substring('MSSQL_SA_PASSWORD='.Length).Trim() }
}
$sql = Join-Path $repo "scripts\spike-mssql\fixture.sql"

if ($Mode -eq "auto") {
    $osType = ""
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        $osType = docker info --format "{{.OSType}}" 2>$null
    }
    if ($osType -eq "linux") { $Mode = "docker" } else { $Mode = "local" }
    Write-Host "Mode auto -> $Mode (docker OSType='$osType')"
}

if ($Mode -eq "local") {
    if (-not (Get-Command sqlcmd -ErrorAction SilentlyContinue)) {
        throw "sqlcmd is not on PATH"
    }
    $fixtureOut = Join-Path $outDir "fixture.txt"
    sqlcmd -E -S localhost -i $sql -o $fixtureOut -y 0 -Y 80 -s "`t" -b
    if ($LASTEXITCODE -ne 0) { throw "fixture sql failed (see $fixtureOut)" }

    $logPath = sqlcmd -E -S localhost -h-1 -W -Q "SET NOCOUNT ON; SELECT physical_name FROM ce_stream_spike.sys.database_files WHERE type_desc = N'LOG';"
    $logPath = ($logPath | Where-Object { $_ -and $_.Trim() -ne "" } | Select-Object -First 1).Trim()
    $readOut = Join-Path $outDir "log-read.txt"
    $readOutLines = @("log=$logPath")
    try {
        $fs = [System.IO.File]::Open($logPath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
        try {
            $buf = New-Object byte[] 64
            $n = $fs.Read($buf, 0, 64)
            $hex = ($buf[0..($n - 1)] | ForEach-Object { $_.ToString("x2") }) -join " "
            $readOutLines += "bytes_read=$n"
            $readOutLines += "length=$($fs.Length)"
            $readOutLines += "head=$hex"
        } finally {
            $fs.Dispose()
        }
    } catch {
        $readOutLines += "open_error=$($_.Exception.GetType().FullName)"
        $readOutLines += "open_message=$($_.Exception.Message)"
    }
    $readOutLines | Set-Content -Path $readOut -Encoding utf8
    Write-Host "Wrote $fixtureOut"
    Write-Host "Wrote $readOut"
    return
}

if (-not $saPassword) { throw "set MSSQL_SA_PASSWORD, or copy lab.env.example to local\lab.env" }

$existing = docker ps -a --filter "name=^/${name}$" --format "{{.ID}}"
if ($existing) {
    docker rm -f $name | Out-Null
}

Write-Host "Starting $image on port $port"
docker run -d --name $name `
    -e "ACCEPT_EULA=Y" `
    -e "MSSQL_SA_PASSWORD=$saPassword" `
    -e "MSSQL_PID=Developer" `
    -p "127.0.0.1:${port}:1433" `
    $image | Out-Null
if ($LASTEXITCODE -ne 0) { throw "docker run failed" }

$ready = $false
for ($i = 0; $i -lt 60; $i++) {
    docker exec $name /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $saPassword -C -Q "SELECT 1" -b 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { $ready = $true; break }
    Start-Sleep -Seconds 3
}
if (-not $ready) {
    docker logs $name
    throw "SQL Server did not accept connections"
}

docker cp $sql "${name}:/tmp/fixture.sql"
if ($LASTEXITCODE -ne 0) { throw "docker cp failed" }

$fixtureOut = Join-Path $outDir "fixture.txt"
docker exec $name /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $saPassword -C -i /tmp/fixture.sql -y 0 -Y 80 -s "`t" -o /tmp/fixture.txt -b
if ($LASTEXITCODE -ne 0) {
    docker exec $name cat /tmp/fixture.txt
    throw "fixture sql failed"
}
docker cp "${name}:/tmp/fixture.txt" $fixtureOut

$readScript = @'
set -eu
log=$(ls /var/opt/mssql/data/*.ldf | head -n 1)
echo "log=$log"
ls -l "$log"
dd if="$log" of=/tmp/ldf-head.bin bs=512 count=16 status=none
wc -c /tmp/ldf-head.bin
od -An -tx1 -N 64 /tmp/ldf-head.bin
'@
$readOut = Join-Path $outDir "log-read.txt"
$readScript | docker exec -i $name bash -s | Out-File -FilePath $readOut -Encoding utf8
if ($LASTEXITCODE -ne 0) { throw "live log read failed" }

Write-Host "Wrote $fixtureOut"
Write-Host "Wrote $readOut"
