[CmdletBinding()]
param(
    [string]$DestinationRoot,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
$skillName = 'evalfrog-workflow'
$skillRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path

if ([string]::IsNullOrWhiteSpace($DestinationRoot)) {
    $codexHomeDirectory = [Environment]::GetEnvironmentVariable('CODEX_HOME')
    if ([string]::IsNullOrWhiteSpace($codexHomeDirectory)) {
        $userProfileDirectory = [Environment]::GetFolderPath('UserProfile')
        $DestinationRoot = Join-Path $userProfileDirectory '.codex\skills'
    } else {
        $DestinationRoot = Join-Path $codexHomeDirectory 'skills'
    }
}

$resolvedDestinationRoot = [IO.Path]::GetFullPath($DestinationRoot)
$destinationSkill = Join-Path $resolvedDestinationRoot $skillName
if ([IO.Path]::GetFullPath($destinationSkill) -eq $skillRoot) {
    throw 'source and destination skill directories must differ'
}

New-Item -ItemType Directory -Path $resolvedDestinationRoot -Force | Out-Null
if (Test-Path -LiteralPath $destinationSkill) {
    if (-not $Force) {
        throw "skill already exists at $destinationSkill; rerun with -Force to replace it"
    }
    $resolvedExisting = (Resolve-Path -LiteralPath $destinationSkill).Path
    if ($resolvedExisting -ne [IO.Path]::GetFullPath($destinationSkill)) {
        throw 'resolved destination does not match the requested skill path'
    }
    Remove-Item -LiteralPath $resolvedExisting -Recurse -Force
}

Copy-Item -LiteralPath $skillRoot -Destination $resolvedDestinationRoot -Recurse
Write-Output "installed $skillName to $destinationSkill"
