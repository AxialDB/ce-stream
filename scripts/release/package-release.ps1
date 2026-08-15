# Package ce-stream release archives and SHA256SUMS.
# Requires staging from build-release.ps1 (Windows) and build-release.sh (Linux).
# Usage: .\scripts\release\package-release.ps1 -Version 0.2.0

param(
    [Parameter(Mandatory = $true)]
    [string] $Version
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$StagingRoot = Join-Path $RepoRoot "releases\staging\v$Version"
$Dist = Join-Path $RepoRoot "releases\dist"
$WinDir = Join-Path $StagingRoot "windows-x86_64"
$LinuxDir = Join-Path $StagingRoot "linux-x86_64"

New-Item -ItemType Directory -Force -Path $Dist | Out-Null

$winZip = Join-Path $Dist "ce-stream-v$Version-x86_64-pc-windows-msvc.zip"
$linuxTar = Join-Path $Dist "ce-stream-v$Version-x86_64-unknown-linux-gnu.tar.gz"
$sums = Join-Path $Dist "SHA256SUMS"

if (-not (Test-Path $WinDir)) {
    throw "Missing Windows staging: $WinDir - run build-release.ps1 first"
}
if (-not (Test-Path $LinuxDir)) {
    throw "Missing Linux staging: $LinuxDir - on Linux run build-release.sh (first-class). On a Windows box, build Linux in WSL then stage here."
}

Write-Host "Packaging Windows zip..."
if (Test-Path $winZip) { Remove-Item $winZip -Force }
Compress-Archive -Path (Join-Path $WinDir "*") -DestinationPath $winZip

Write-Host "Packaging Linux tar.gz (via WSL)..."
function ConvertTo-WslPath([string]$Path) {
    $p = $Path -replace '\\', '/'
    if ($p -match '^([A-Za-z]):(.*)$') {
        return "/mnt/$($Matches[1].ToLower())$($Matches[2])"
    }
    return $p
}
$linuxTarWsl = ConvertTo-WslPath $linuxTar
$linuxDirWsl = ConvertTo-WslPath $LinuxDir
wsl bash -c "set -euo pipefail; tar -czf '$linuxTarWsl' -C '$linuxDirWsl' ."

Write-Host "Writing SHA256SUMS..."
$hashLines = @()
foreach ($file in @($winZip, $linuxTar)) {
    $h = Get-FileHash -Path $file -Algorithm SHA256
    $name = Split-Path $file -Leaf
    $hashLines += "$($h.Hash.ToLower())  $name"
}
$hashLines | Set-Content -Path $sums -Encoding ascii

Write-Host ""
Write-Host "Release artifacts:"
Get-ChildItem $Dist | ForEach-Object { Write-Host "  $($_.FullName)" }
Write-Host ""
Get-Content $sums
