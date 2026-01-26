# Generate ERD Documentation for Articles Database
# This script uses SchemaSpy to generate interactive HTML documentation with ERD diagrams

param(
    [switch]$Open = $false,
    [switch]$Clean = $false
)

$ErrorActionPreference = "Stop"

Write-Host "=== Articles Database ERD Generator ===" -ForegroundColor Green
Write-Host ""

# Clean output directory if requested
if ($Clean -and (Test-Path ".\schemaspy-output")) {
    Write-Host "Cleaning previous output..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force ".\schemaspy-output"
}

# Create output directory if it doesn't exist
if (!(Test-Path ".\schemaspy-output")) {
    New-Item -ItemType Directory -Path ".\schemaspy-output" | Out-Null
}

# Check if database is running
Write-Host "Checking if database is running..." -ForegroundColor Cyan
$dbRunning = docker ps --filter "name=articles_postgres" --filter "status=running" --format "{{.Names}}"

if (!$dbRunning) {
    Write-Host "Database is not running. Starting database..." -ForegroundColor Yellow
    docker-compose up -d articles_postgres
    Write-Host "Waiting 10 seconds for database to be ready..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
}

# Generate ERD using SchemaSpy
Write-Host "Generating ERD documentation with SchemaSpy..." -ForegroundColor Cyan
docker-compose --profile erd run --rm schemaspy

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "✓ ERD documentation generated successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Output location: .\schemaspy-output\" -ForegroundColor Yellow
    Write-Host "Main page: .\schemaspy-output\index.html" -ForegroundColor Yellow
    Write-Host ""
    
    # Open in browser if requested
    if ($Open) {
        $indexPath = Join-Path $PSScriptRoot "schemaspy-output\index.html"
        if (Test-Path $indexPath) {
            Write-Host "Opening ERD documentation in browser..." -ForegroundColor Cyan
            Start-Process $indexPath
        }
    } else {
        Write-Host "Tip: Use -Open flag to automatically open the documentation" -ForegroundColor Gray
    }
} else {
    Write-Host "✗ Failed to generate ERD documentation" -ForegroundColor Red
    exit 1
}
