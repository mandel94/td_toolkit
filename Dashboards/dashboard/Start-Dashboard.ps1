<#!
.SYNOPSIS
Starts, stops, and manages the Dashboard using Docker Compose.

.DESCRIPTION
PowerShell CLI wrapper around `docker compose` for the Editorial Analytics Dashboard.
Supports demo mode (no GA4), building, detached runs, logs, and opening the browser.

.PARAMETER Demo
Use the demo compose file (`docker-compose.demo.yml`) without GA4 credentials.

.PARAMETER Build
Build images before starting.

.PARAMETER Detach
Run containers in the background (`-d`).

.PARAMETER Down
Stop and remove containers and network.

.PARAMETER Logs
Tail container logs.

.PARAMETER Status
Show `docker compose ps` status.

.PARAMETER ComposeFile
Override compose file path. Defaults to `docker-compose.yml` or the demo file when `-Demo` is set.

.PARAMETER Open
Open browser to the dashboard URL after successful start.

.PARAMETER WebHost
Override host used for the browser URL. Defaults to value from `.env` or `localhost`.

.PARAMETER Port
Override port used for the browser URL. Defaults to value from `.env` or `8050`.

.EXAMPLE
./Start-Dashboard.ps1 -Demo -Build -Detach -Open

.EXAMPLE
./Start-Dashboard.ps1 -Down

#>

param(
    [switch]$Demo,
    [switch]$Build,
    [switch]$Detach,
    [switch]$Down,
    [switch]$Logs,
    [switch]$Status,
    [string]$ComposeFile,
    [switch]$Open,
    [string]$WebHost,
    [int]$Port
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Err($msg)  { Write-Host "[ERROR] $msg" -ForegroundColor Red }

# Ensure we run from the dashboard directory
$ScriptDir = Split-Path -Parent $PSCommandPath
Push-Location $ScriptDir

# Verify Docker availability
try {
    $null = Get-Command docker -ErrorAction Stop
} catch {
    Write-Err "Docker is not available. Please install Docker Desktop and try again."
    Pop-Location
    exit 1
}

# Select compose file
if (-not $ComposeFile) {
    $ComposeFile = if ($Demo) { 'docker-compose.demo.yml' } else { 'docker-compose.yml' }
}
if (-not (Test-Path $ComposeFile)) {
    Write-Err "Compose file not found: $ComposeFile"
    Pop-Location
    exit 1
}

# Read .env to infer host/port if not provided
function Get-EnvValue([string]$name, [string]$default) {
    $envPath = Join-Path $ScriptDir '.env'
    if (Test-Path $envPath) {
        $line = Select-String -Path $envPath -Pattern "^$name=(.*)$" -CaseSensitive -SimpleMatch -ErrorAction SilentlyContinue
        if ($line) {
            $val = $line.Matches.Groups[1].Value.Trim()
            if ($val) { return $val }
        }
    }
    return $default
}

if (-not $WebHost) { $WebHost = Get-EnvValue 'HOST' 'localhost' }
if (-not $Port) { $Port = [int](Get-EnvValue 'PORT' '8050') }

# Perform actions
if ($Down) {
    Write-Info "Stopping and removing containers (compose: $ComposeFile)"
    docker compose -f $ComposeFile down
    Write-Info "Done."
    Pop-Location
    exit 0
}

if ($Build) {
    Write-Info "Building images (compose: $ComposeFile)"
    docker compose -f $ComposeFile build
}

if ($Status) {
    Write-Info "Compose status (ps):"
    docker compose -f $ComposeFile ps
}

if ($Logs) {
    Write-Info "Tailing logs (Ctrl+C to stop)"
    docker compose -f $ComposeFile logs -f
    Pop-Location
    exit 0
}

Write-Info "Starting dashboard (compose: $ComposeFile)"
if ($Detach) {
    docker compose -f $ComposeFile up -d
} else {
    docker compose -f $ComposeFile up
}

if ($Open) {
    $url = "http://${WebHost}:${Port}"
    Write-Info "Opening browser: $url"
    try { Start-Process $url } catch { Write-Warn "Could not open browser. URL: $url" }
}

Pop-Location
Write-Info "All done."
