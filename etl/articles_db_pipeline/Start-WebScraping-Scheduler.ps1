# Start Web Scraping Scheduler for TaxiDrivers.it Articles
# This script starts the automated weekly scraping scheduler

param(
    [string]$Day = "friday",
    [string]$Time = "02:00",
    [switch]$RunNow = $false,
    [switch]$Help = $false
)

# Show help
if ($Help) {
    Write-Host @"
    
TaxiDrivers.it Web Scraping Scheduler
======================================

Avvia lo scheduler per scraping automatico settimanale degli articoli.

USO:
    .\Start-WebScraping-Scheduler.ps1 [opzioni]

OPZIONI:
    -Day <giorno>      Giorno della settimana (default: friday)
                       Valori: monday, tuesday, wednesday, thursday, friday, saturday, sunday
    
    -Time <ora>        Ora di esecuzione in formato HH:MM (default: 02:00)
    
    -RunNow            Esegui immediatamente invece di schedulare
    
    -Help              Mostra questo messaggio di aiuto

ESEMPI:
    .\Start-WebScraping-Scheduler.ps1
    → Esegue ogni venerdì alle 02:00
    
    .\Start-WebScraping-Scheduler.ps1 -Day monday -Time 03:30
    → Esegue ogni lunedì alle 03:30
    
    .\Start-WebScraping-Scheduler.ps1 -RunNow
    → Esegue immediatamente (per test)

"@
    exit 0
}

# Get script directory
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  TaxiDrivers.it Web Scraping Scheduler" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

# Check if Python is available
try {
    $pythonVersion = python --version 2>&1
    Write-Host "✓ Python trovato: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ Python non trovato!" -ForegroundColor Red
    Write-Host "Installare Python 3.8+ e aggiungere al PATH" -ForegroundColor Yellow
    exit 1
}

# Check if required packages are installed
Write-Host ""
Write-Host "Verifica dipendenze..." -ForegroundColor Yellow

$RequiredPackages = @("schedule", "beautifulsoup4", "requests", "sqlalchemy", "loguru", "pydantic")
$MissingPackages = @()

foreach ($package in $RequiredPackages) {
    $check = python -c "import $($package.Replace('-', '_'))" 2>&1
    if ($LASTEXITCODE -ne 0) {
        $MissingPackages += $package
    }
}

if ($MissingPackages.Count -gt 0) {
    Write-Host "✗ Dipendenze mancanti: $($MissingPackages -join ', ')" -ForegroundColor Red
    Write-Host ""
    Write-Host "Installare con:" -ForegroundColor Yellow
    Write-Host "  cd $ScriptDir" -ForegroundColor Cyan
    Write-Host "  pip install -r requirements.txt" -ForegroundColor Cyan
    exit 1
}

Write-Host "✓ Tutte le dipendenze installate" -ForegroundColor Green

# Check database connection
Write-Host ""
Write-Host "Verifica connessione database..." -ForegroundColor Yellow

$dbCheck = python -c @"
import sys
sys.path.append('$($ScriptDir.Replace('\', '/'))')
from etl.articles_db_pipeline.web_scraping_pipeline import WebScrapingPipeline
pipeline = WebScrapingPipeline()
status = pipeline.get_pipeline_status()
print('connected' if status.get('database_connected') else 'failed')
"@ 2>&1

if ($dbCheck -like "*connected*") {
    Write-Host "✓ Database connesso" -ForegroundColor Green
} else {
    Write-Host "✗ Database non connesso!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Avviare il database prima:" -ForegroundColor Yellow
    Write-Host "  cd articles_db" -ForegroundColor Cyan
    Write-Host "  docker-compose up -d" -ForegroundColor Cyan
    exit 1
}

# Start scheduler
Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan

if ($RunNow) {
    Write-Host "Esecuzione IMMEDIATA (modalità test)" -ForegroundColor Yellow
    Write-Host "================================================" -ForegroundColor Cyan
    Write-Host ""
    
    # Run immediately
    python "$ScriptDir\scheduler.py" --run-now
    
} else {
    Write-Host "Avvio scheduler: $Day alle $Time" -ForegroundColor Green
    Write-Host "================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Log salvati in: $ScriptDir\logs\" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Premi Ctrl+C per fermare lo scheduler" -ForegroundColor Yellow
    Write-Host ""
    
    # Start scheduler
    python "$ScriptDir\scheduler.py" --day $Day --time $Time
}
