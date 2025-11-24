# PowerShell Database Management Script for Windows
# Articles Database Management

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("start", "stop", "restart", "logs", "connect", "backup", "help")]
    [string]$Command
)

# Function to check if Docker is running
function Test-Docker {
    try {
        docker info | Out-Null
        return $true
    }
    catch {
        Write-Host "ERROR: Docker is not running. Please start Docker and try again." -ForegroundColor Red
        return $false
    }
}

# Function to start database
function Start-Database {
    Write-Host "Starting Articles Database..." -ForegroundColor Green
    if (-not (Test-Docker)) { return }
    
    docker-compose up -d
    Write-Host "Database started! Access Adminer at http://localhost:8080" -ForegroundColor Green
    Write-Host "Database connection: localhost:5432" -ForegroundColor Green
}

# Function to stop database
function Stop-Database {
    Write-Host "Stopping Articles Database..." -ForegroundColor Green
    docker-compose down
    Write-Host "Database stopped." -ForegroundColor Green
}

# Function to restart database
function Restart-Database {
    Write-Host "Restarting Articles Database..." -ForegroundColor Green
    if (-not (Test-Docker)) { return }
    
    docker-compose down
    docker-compose up -d
    Write-Host "Database restarted!" -ForegroundColor Green
}

# Function to view logs
function Show-Logs {
    Write-Host "Showing database logs..." -ForegroundColor Green
    docker-compose logs -f articles_postgres
}

# Function to connect to database
function Connect-Database {
    Write-Host "Connecting to database..." -ForegroundColor Green
    docker-compose exec articles_postgres psql -U articles_user -d articles_db
}

# Function to backup database
function Backup-Database {
    Write-Host "Creating database backup..." -ForegroundColor Green
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    docker-compose exec articles_postgres pg_dump -U articles_user articles_db > "./data/backup_$timestamp.sql"
    Write-Host "Backup created: ./data/backup_$timestamp.sql" -ForegroundColor Green
}

# Function to show help
function Show-Help {
    Write-Host "Articles Database Management Script" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Usage: .\manage_db.ps1 -Command [command]" -ForegroundColor White
    Write-Host ""
    Write-Host "Commands:" -ForegroundColor White
    Write-Host "  start     Start the database containers" -ForegroundColor Yellow
    Write-Host "  stop      Stop the database containers" -ForegroundColor Yellow
    Write-Host "  restart   Restart the database containers" -ForegroundColor Yellow
    Write-Host "  logs      Show database logs" -ForegroundColor Yellow
    Write-Host "  connect   Connect to database via psql" -ForegroundColor Yellow
    Write-Host "  backup    Create a database backup" -ForegroundColor Yellow
    Write-Host "  help      Show this help message" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Examples:" -ForegroundColor White
    Write-Host "  .\manage_db.ps1 -Command start" -ForegroundColor Gray
    Write-Host "  .\manage_db.ps1 -Command backup" -ForegroundColor Gray
}

# Main script logic
switch ($Command) {
    "start" { Start-Database }
    "stop" { Stop-Database }
    "restart" { Restart-Database }
    "logs" { Show-Logs }
    "connect" { Connect-Database }
    "backup" { Backup-Database }
    "help" { Show-Help }
    default { 
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Show-Help
    }
}