param(
  [string]$AppId = $env:META_APP_ID,
  [string]$AppSecret = $env:META_APP_SECRET,
  [string]$AccessToken = $env:META_ADS_ACCESS_TOKEN,
  [int]$RefreshThresholdDays = 7,
  [string]$GraphVersion = "v23.0",
  [string]$ConfigPath = (Join-Path $env:USERPROFILE ".codex\config.toml"),
  [string]$ConfigTokenKey = "META_ADS_ACCESS_TOKEN",
  [string[]]$RequiredScopes = @("instagram_basic", "instagram_manage_insights"),
  [switch]$SkipConfigUpdate,
  [switch]$ForceRefresh
)

$ErrorActionPreference = "Stop"

function Test-RequiredValue {
  param(
    [string]$Name,
    [string]$Value
  )
  if ([string]::IsNullOrWhiteSpace($Value)) {
    throw "Missing required value: $Name. Pass it as parameter or environment variable."
  }
}

function Mask-Token {
  param([string]$Token)
  if ([string]::IsNullOrEmpty($Token)) { return "(empty)" }
  if ($Token.Length -le 12) { return ("*" * $Token.Length) }
  return "{0}...{1}" -f $Token.Substring(0, 6), $Token.Substring($Token.Length - 4)
}

function ConvertTo-QueryString {
  param([hashtable]$Query)
  $parts = @()
  foreach ($key in $Query.Keys) {
    $escapedKey = [System.Uri]::EscapeDataString([string]$key)
    $escapedValue = [System.Uri]::EscapeDataString([string]$Query[$key])
    $parts += ("{0}={1}" -f $escapedKey, $escapedValue)
  }
  return ($parts -join "&")
}

function Invoke-MetaGraphGet {
  param(
    [string]$Endpoint,
    [hashtable]$Query
  )

  $baseUrl = "https://graph.facebook.com/$GraphVersion/$Endpoint"
  $queryString = ConvertTo-QueryString -Query $Query
  # Use explicit variable boundaries to avoid PowerShell parsing `$baseUrl?` as a variable name.
  $uri = "${baseUrl}?${queryString}"

  try {
    return Invoke-RestMethod -Method Get -Uri $uri -TimeoutSec 45
  } catch {
    $apiMessage = $null
    $exceptionMessage = $null
    if ($_.Exception -and $_.Exception.Message) {
      $exceptionMessage = [string]$_.Exception.Message
    }
    if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
      try {
        $errPayload = $_.ErrorDetails.Message | ConvertFrom-Json -ErrorAction Stop
        if ($errPayload.error -and $errPayload.error.message) {
          $apiMessage = [string]$errPayload.error.message
        }
      } catch {
      }
    }

    if ($apiMessage) {
      throw "Meta API error on '$Endpoint': $apiMessage"
    }
    if ($exceptionMessage) {
      throw "Meta API request failed on '$Endpoint': $exceptionMessage"
    }
    throw "Meta API request failed on '$Endpoint'."
  }
}

function Get-TokenDebugData {
  param(
    [string]$Token,
    [string]$AppId,
    [string]$AppSecret
  )

  $appAccessToken = "{0}|{1}" -f $AppId, $AppSecret
  $response = Invoke-MetaGraphGet -Endpoint "debug_token" -Query @{
    input_token  = $Token
    access_token = $appAccessToken
  }

  if (-not $response.data) {
    throw "Meta debug_token response did not include 'data'."
  }

  return $response.data
}

function Get-ExpiryUtc {
  param([object]$DebugData)

  $hasExpiresAt = $DebugData.PSObject.Properties.Name -contains "expires_at"
  if (-not $hasExpiresAt) { return $null }

  $expiresAtRaw = [int64]$DebugData.expires_at
  if ($expiresAtRaw -le 0) { return $null }

  return [DateTimeOffset]::FromUnixTimeSeconds($expiresAtRaw).UtcDateTime
}

function Get-GrantedScopes {
  param([object]$DebugData)

  $scopes = New-Object System.Collections.Generic.HashSet[string]

  if ($DebugData.PSObject.Properties.Name -contains "scopes" -and $DebugData.scopes) {
    foreach ($s in $DebugData.scopes) {
      if (-not [string]::IsNullOrWhiteSpace([string]$s)) {
        [void]$scopes.Add(([string]$s).Trim())
      }
    }
  }

  if ($DebugData.PSObject.Properties.Name -contains "granular_scopes" -and $DebugData.granular_scopes) {
    foreach ($g in $DebugData.granular_scopes) {
      if ($g -and $g.scope -and -not [string]::IsNullOrWhiteSpace([string]$g.scope)) {
        [void]$scopes.Add(([string]$g.scope).Trim())
      }
    }
  }

  return @($scopes)
}

function Assert-RequiredScopes {
  param(
    [object]$DebugData,
    [string[]]$RequiredScopes
  )

  if (-not $RequiredScopes -or $RequiredScopes.Count -eq 0) {
    return
  }

  $granted = Get-GrantedScopes -DebugData $DebugData
  $grantedSet = New-Object System.Collections.Generic.HashSet[string]
  foreach ($g in $granted) {
    [void]$grantedSet.Add(([string]$g).ToLowerInvariant())
  }

  $missing = @()
  foreach ($req in $RequiredScopes) {
    $normalized = ([string]$req).Trim().ToLowerInvariant()
    if ([string]::IsNullOrWhiteSpace($normalized)) { continue }
    if (-not $grantedSet.Contains($normalized)) {
      $missing += $req
    }
  }

  if ($missing.Count -gt 0) {
    $grantedList = if ($granted.Count -gt 0) { $granted -join ", " } else { "(none)" }
    throw ("Token missing required scopes: {0}. Granted scopes: {1}" -f ($missing -join ", "), $grantedList)
  }
}

function Update-TomlTokenValue {
  param(
    [string]$Path,
    [string]$Key,
    [string]$Token
  )

  if (-not (Test-Path -LiteralPath $Path)) {
    throw "Config file not found: $Path"
  }

  $content = Get-Content -LiteralPath $Path -Raw
  $escapedKey = [regex]::Escape($Key)
  $pattern = "(?m)^(\s*$escapedKey\s*=\s*)`"[^`"]*`"(\s*)$"

  if (-not [regex]::IsMatch($content, $pattern)) {
    throw "Key '$Key' not found in config file: $Path"
  }

  $updated = [regex]::Replace(
    $content,
    $pattern,
    { param($m) "{0}`"{1}`"{2}" -f $m.Groups[1].Value, $Token, $m.Groups[2].Value },
    1
  )

  $backupPath = "{0}.bak.{1}" -f $Path, (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item -LiteralPath $Path -Destination $backupPath -Force
  Set-Content -LiteralPath $Path -Value $updated -Encoding UTF8

  Write-Host ("Config updated: {0}" -f $Path)
  Write-Host ("Backup created: {0}" -f $backupPath)
}

function Get-TomlTokenValue {
  param(
    [string]$Path,
    [string]$Key
  )

  if (-not (Test-Path -LiteralPath $Path)) {
    return $null
  }

  $content = Get-Content -LiteralPath $Path -Raw
  $escapedKey = [regex]::Escape($Key)
  $pattern = "(?m)^\s*$escapedKey\s*=\s*`"([^`"]+)`"\s*$"
  $match = [regex]::Match($content, $pattern)
  if (-not $match.Success) {
    return $null
  }

  return [string]$match.Groups[1].Value
}

Test-RequiredValue -Name "AppId / META_APP_ID" -Value $AppId
Test-RequiredValue -Name "AppSecret / META_APP_SECRET" -Value $AppSecret

if ([string]::IsNullOrWhiteSpace($AccessToken)) {
  $tokenFromConfig = Get-TomlTokenValue -Path $ConfigPath -Key $ConfigTokenKey
  if (-not [string]::IsNullOrWhiteSpace($tokenFromConfig)) {
    $AccessToken = $tokenFromConfig
    Write-Host ("Loaded access token from config key '{0}'." -f $ConfigTokenKey)
  }
}

Test-RequiredValue -Name "AccessToken / META_ADS_ACCESS_TOKEN or config key" -Value $AccessToken

Write-Host "Meta token guard started."
Write-Host ("Graph version: {0}" -f $GraphVersion)
Write-Host ("Token (masked): {0}" -f (Mask-Token -Token $AccessToken))

$debugData = Get-TokenDebugData -Token $AccessToken -AppId $AppId -AppSecret $AppSecret
if (-not $debugData.is_valid) {
  throw "Token is not valid according to Meta debug_token."
}
Assert-RequiredScopes -DebugData $debugData -RequiredScopes $RequiredScopes

$tokenType = if ($debugData.type) { [string]$debugData.type } else { "unknown" }
$expiresAtUtc = Get-ExpiryUtc -DebugData $debugData

Write-Host ("Token type: {0}" -f $tokenType)

if ($expiresAtUtc) {
  $nowUtc = (Get-Date).ToUniversalTime()
  $daysRemaining = [math]::Floor(($expiresAtUtc - $nowUtc).TotalDays)
  Write-Host ("Token expires at (UTC): {0}" -f $expiresAtUtc.ToString("yyyy-MM-dd HH:mm:ss"))
  Write-Host ("Days remaining: {0}" -f $daysRemaining)

  if (-not $ForceRefresh -and $daysRemaining -gt $RefreshThresholdDays) {
    Write-Host ("No refresh needed (threshold: {0} days)." -f $RefreshThresholdDays)
    exit 0
  }
} else {
  Write-Host "Token appears non-expiring (expires_at missing or 0)."
  if (-not $ForceRefresh) {
    Write-Host "No refresh attempted."
    exit 0
  }
}

Write-Host "Refreshing token with fb_exchange_token flow..."
$refreshResponse = Invoke-MetaGraphGet -Endpoint "oauth/access_token" -Query @{
  grant_type        = "fb_exchange_token"
  client_id         = $AppId
  client_secret     = $AppSecret
  fb_exchange_token = $AccessToken
}

if (-not $refreshResponse.access_token) {
  throw "Refresh response did not include access_token."
}

$newToken = [string]$refreshResponse.access_token
$changed = $newToken -ne $AccessToken
Write-Host ("Refresh succeeded. Token changed: {0}" -f $changed)
Write-Host ("New token (masked): {0}" -f (Mask-Token -Token $newToken))

$newDebugData = Get-TokenDebugData -Token $newToken -AppId $AppId -AppSecret $AppSecret
if (-not $newDebugData.is_valid) {
  throw "New token failed validation."
}
Assert-RequiredScopes -DebugData $newDebugData -RequiredScopes $RequiredScopes

$newExpiresAtUtc = Get-ExpiryUtc -DebugData $newDebugData
if ($newExpiresAtUtc) {
  $newDaysRemaining = [math]::Floor(($newExpiresAtUtc - (Get-Date).ToUniversalTime()).TotalDays)
  Write-Host ("New token expires at (UTC): {0}" -f $newExpiresAtUtc.ToString("yyyy-MM-dd HH:mm:ss"))
  Write-Host ("New days remaining: {0}" -f $newDaysRemaining)
} else {
  Write-Host "New token appears non-expiring."
}

$env:META_ADS_ACCESS_TOKEN = $newToken
Write-Host "Updated process environment variable META_ADS_ACCESS_TOKEN."

if (-not $SkipConfigUpdate) {
  Update-TomlTokenValue -Path $ConfigPath -Key $ConfigTokenKey -Token $newToken
} else {
  Write-Host "Config update skipped by flag."
}

Write-Host "Meta token guard completed."
