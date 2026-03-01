param(
  [string]$Root = "H:\My Drive\MCP_Portable\MCP Juan"
)

$codexPortable = Join-Path $Root "config\\codex\\config.toml.private"
$claudePortable = Join-Path $Root "config\\claude\\claude_desktop_config.private.json"
$codexLocal = Join-Path $env:USERPROFILE ".codex\\config.toml"
$claudeLocal = Join-Path $env:APPDATA "claude\\claude_desktop_config.json"

Write-Host "== Package files =="
@(
  $codexPortable,
  $claudePortable,
  (Join-Path $Root "scripts\\install-mcp-juan.ps1"),
  (Join-Path $Root "README.md")
) | ForEach-Object {
  $ok = Test-Path $_
  Write-Host ("{0} -> {1}" -f $_, $(if ($ok) { "OK" } else { "MISSING" }))
}

Write-Host "`n== Local wiring =="
foreach ($p in @($codexLocal, $claudeLocal)) {
  if (Test-Path $p) {
    $item = Get-Item -Path $p -Force
    $kind = if ($item.LinkType) { "SYMLINK" } else { "FILE" }
    $target = if ($item.LinkType) { $item.Target } else { "(not linked)" }
    Write-Host "$p -> $kind $target"
  } else {
    Write-Host "$p -> MISSING"
  }
}

Write-Host "`n== Codex MCP blocks found =="
if (Test-Path $codexLocal) {
  $blocks = Select-String -Path $codexLocal -Pattern '^\[mcp_servers\..+\]' | ForEach-Object { $_.Line.Trim() }
  if ($blocks) { $blocks | ForEach-Object { Write-Host $_ } } else { Write-Host "No MCP blocks found." }
}

Write-Host "`n== Expected MCP servers =="
Write-Host "analytics-mcp"
Write-Host "google-ads-mcp"

