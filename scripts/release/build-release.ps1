# Build Windows release binaries for ce-stream.
# Usage: .\scripts\release\build-release.ps1 -Version 0.2.0

param(
    [Parameter(Mandatory = $true)]
    [string] $Version
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$Staging = Join-Path $RepoRoot "releases\staging\v$Version\windows-x86_64"

Push-Location $RepoRoot
try {
    $env:CARGO_TARGET_DIR = Join-Path $RepoRoot "target"
    Write-Host "Building ce-stream-cli and ce-stream-perf-sink (release)..."
    cargo build -p ce-stream-cli -p ce-stream-perf-sink --release
    if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }

    New-Item -ItemType Directory -Force -Path $Staging | Out-Null
    Copy-Item (Join-Path $env:CARGO_TARGET_DIR "release\ce-stream.exe") $Staging -Force
    Copy-Item (Join-Path $env:CARGO_TARGET_DIR "release\ce-stream-perf-sink.exe") $Staging -Force

    $versionFile = Join-Path $Staging "VERSION"
    @(
        "ce-stream v$Version",
        "target: x86_64-pc-windows-msvc",
        "built: $(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK')"
    ) | Set-Content -Path $versionFile -Encoding ascii

    Write-Host "Windows binaries staged at: $Staging"
}
finally {
    Pop-Location
}
