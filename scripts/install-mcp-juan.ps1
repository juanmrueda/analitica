param(
  [string]$Root = "H:\My Drive\MCP_Portable\MCP Juan",
  [ValidateSet("Symlink", "Copy")]
  [string]$Mode = "Symlink",
  [switch]$UseTemplate,
  [switch]$StopClaude
)

$ErrorActionPreference = "Stop"

$claudeProcs = Get-Process -Name "claude" -ErrorAction SilentlyContinue
if ($claudeProcs) {
  if ($StopClaude) {
    $claudeProcs | Stop-Process -Force
    Start-Sleep -Milliseconds 500
    Write-Host "Claude processes stopped before applying config."
  } else {
    Write-Warning "Claude is running. It may overwrite claude_desktop_config.json. Close Claude or rerun with -StopClaude."
  }
}

function Backup-IfExists {
  param(
    [string]$Path,
    [string]$BackupRoot
  )
  if (Test-Path $Path) {
    $safe = ($Path -replace ':', '') -replace '^\\', ''
    $dest = Join-Path $BackupRoot $safe
    New-Item -ItemType Directory -Path (Split-Path $dest -Parent) -Force | Out-Null
    Copy-Item -Path $Path -Destination $dest -Force
  }
}

function Set-PortableFile {
  param(
    [string]$Target,
    [string]$Source,
    [string]$Mode
  )

  if (!(Test-Path $Source)) {
    throw "Missing source file: $Source"
  }

  New-Item -ItemType Directory -Path (Split-Path $Target -Parent) -Force | Out-Null

  if (Test-Path $Target) {
    Remove-Item -Path $Target -Force
  }

  if ($Mode -eq "Symlink") {
    try {
      New-Item -ItemType SymbolicLink -Path $Target -Target $Source -Force | Out-Null
      return "symlink"
    } catch {
      Copy-Item -Path $Source -Destination $Target -Force
      return "copy-fallback"
    }
  }

  Copy-Item -Path $Source -Destination $Target -Force
  return "copy"
}

$codexPrivate = Join-Path $Root "config\\codex\\config.toml.private"
$codexTemplate = Join-Path $Root "config\\codex\\config.toml.template"
$claudePrivate = Join-Path $Root "config\\claude\\claude_desktop_config.private.json"
$claudeTemplate = Join-Path $Root "config\\claude\\claude_desktop_config.template.json"

$codexSource = if ($UseTemplate -or !(Test-Path $codexPrivate)) { $codexTemplate } else { $codexPrivate }
$claudeSource = if ($UseTemplate -or !(Test-Path $claudePrivate)) { $claudeTemplate } else { $claudePrivate }

$codexTarget = Join-Path $env:USERPROFILE ".codex\\config.toml"
$claudeTarget = Join-Path $env:APPDATA "claude\\claude_desktop_config.json"

$backupRoot = Join-Path $Root ("backups\\" + (Get-Date -Format "yyyyMMdd_HHmmss"))
New-Item -ItemType Directory -Path $backupRoot -Force | Out-Null

Backup-IfExists -Path $codexTarget -BackupRoot $backupRoot
Backup-IfExists -Path $claudeTarget -BackupRoot $backupRoot

$codexResult = Set-PortableFile -Target $codexTarget -Source $codexSource -Mode $Mode
$claudeResult = Set-PortableFile -Target $claudeTarget -Source $claudeSource -Mode $Mode

Write-Host "Done."
Write-Host "Backup: $backupRoot"
Write-Host "Codex config: $codexResult <- $codexSource"
Write-Host "Claude config: $claudeResult <- $claudeSource"
Write-Host "Package scope: analytics-mcp + google-ads-mcp only."

