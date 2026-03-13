param(
  [string]$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
  [string]$AntigravityProcess = "antigravity",
  [string]$PythonExe = ".\.venv\Scripts\python.exe",
  [int]$PollSeconds = 2,
  [switch]$RunOnce,
  [switch]$RelaunchOnAntigravityRestart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($PollSeconds -lt 1) {
  throw "PollSeconds must be >= 1."
}

$procName = [System.IO.Path]::GetFileNameWithoutExtension($AntigravityProcess)
$rootPath = (Resolve-Path $Root).Path
$pythonPath = if ([System.IO.Path]::IsPathRooted($PythonExe)) {
  $PythonExe
} else {
  Join-Path $rootPath $PythonExe
}

if (-not (Test-Path $pythonPath)) {
  throw "Python executable not found: $pythonPath"
}

function Test-AntigravityRunning {
  param([string]$Name)
  return [bool](Get-Process -Name $Name -ErrorAction SilentlyContinue)
}

function Test-StreamlitDashboardRunning {
  param([string]$RootFolder)

  $dashboardPath = (Join-Path $RootFolder "dashboard.py").ToLowerInvariant()
  $processes = Get-CimInstance Win32_Process -Filter "Name = 'python.exe' OR Name = 'pythonw.exe' OR Name = 'streamlit.exe'" -ErrorAction SilentlyContinue
  if (-not $processes) {
    return $false
  }

  foreach ($proc in $processes) {
    $cmd = [string]$proc.CommandLine
    if (-not $cmd) {
      continue
    }
    $cmdLower = $cmd.ToLowerInvariant()
    if ($cmdLower -like "*streamlit*run*dashboard.py*") {
      return $true
    }
    if ($cmdLower.Contains($dashboardPath)) {
      return $true
    }
  }

  return $false
}

function Start-StreamlitDashboard {
  param(
    [string]$PythonPath,
    [string]$RootFolder
  )

  Start-Process -FilePath $PythonPath -ArgumentList @("-m", "streamlit", "run", "dashboard.py") -WorkingDirectory $RootFolder | Out-Null
  Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Streamlit started."
}

Write-Host "Watching process '$procName'..."
Write-Host "Root: $rootPath"
Write-Host "Python: $pythonPath"
Write-Host "Relaunch on Antigravity restart: $([bool]$RelaunchOnAntigravityRestart)"

$triggered = $false

while ($true) {
  $agRunning = Test-AntigravityRunning -Name $procName

  if ($agRunning -and -not $triggered) {
    if (Test-StreamlitDashboardRunning -RootFolder $rootPath) {
      Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Streamlit is already running."
    } else {
      Start-StreamlitDashboard -PythonPath $pythonPath -RootFolder $rootPath
    }

    $triggered = $true
    if ($RunOnce) {
      break
    }
  }

  if ($RelaunchOnAntigravityRestart -and -not $agRunning) {
    $triggered = $false
  }

  Start-Sleep -Seconds $PollSeconds
}
