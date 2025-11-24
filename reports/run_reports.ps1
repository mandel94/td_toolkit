# PowerShell script to execute all reporting commands
# Taxi Drivers Analytics Reporting Suite
param(
    [string]$ReportType = "all",
    [string]$StartDate = "",
    [string]$EndDate = "",
    [int]$Days = 0,
    [switch]$OpenOutput = $false,
    [switch]$AutoOpenExcel = $true,
    [switch]$NoAutoOpenExcel = $false,
    [switch]$Help = $false
)

# Set error handling
$ErrorActionPreference = "Stop"

# Get the script directory
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Split-Path -Parent $ScriptDir

# Python executable path
$PythonExe = "$RootDir\.td_ds_venv\Scripts\python.exe"

# Set Excel auto-open environment variable
if ($NoAutoOpenExcel) {
    $env:EXCEL_AUTO_OPEN = "false"
} elseif ($AutoOpenExcel) {
    $env:EXCEL_AUTO_OPEN = "true"
} else {
    # Default behavior - auto-open enabled
    $env:EXCEL_AUTO_OPEN = "true"
}

# Help function
function Show-Help {
    Write-Host "=== Taxi Drivers Reporting Suite - Help ===" -ForegroundColor Green
    Write-Host ""
    Write-Host "Usage: .\run_reports.ps1 [OPTIONS]" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Parameters:" -ForegroundColor Cyan
    Write-Host "  -ReportType    Type of report to run (weekly, monthly, adhoc, sandra, analysis, all)" -ForegroundColor White
    Write-Host "  -StartDate     Start date for reports (YYYY-MM-DD format)" -ForegroundColor White
    Write-Host "  -EndDate       End date for reports (YYYY-MM-DD format)" -ForegroundColor White
    Write-Host "  -Days          Number of days back from today (for sandra report)" -ForegroundColor White
    Write-Host "  -OpenOutput    Open output directory after completion" -ForegroundColor White
    Write-Host "  -AutoOpenExcel Automatically open Excel files after generation (default: true)" -ForegroundColor White
    Write-Host "  -NoAutoOpenExcel Disable automatic Excel file opening" -ForegroundColor White
    Write-Host "  -Help          Show this help message" -ForegroundColor White
    Write-Host ""
    Write-Host "Examples:" -ForegroundColor Cyan
    Write-Host "  .\run_reports.ps1 -ReportType weekly -OpenOutput" -ForegroundColor Gray
    Write-Host "  .\run_reports.ps1 -ReportType monthly -StartDate 2025-10-01 -EndDate 2025-10-31" -ForegroundColor Gray
    Write-Host "  .\run_reports.ps1 -ReportType sandra -Days 7 -NoAutoOpenExcel" -ForegroundColor Gray
    Write-Host "  .\run_reports.ps1 -ReportType all -AutoOpenExcel" -ForegroundColor Gray
    exit 0
}

# Function to run Python script with error handling
function Run-PythonScript {
    param(
        [string]$ScriptPath,
        [string]$Description,
        [array]$Arguments = @()
    )
    
    Write-Host "Running: $Description" -ForegroundColor Cyan
    Write-Host "Script: $ScriptPath" -ForegroundColor Gray
    
    try {
        if ($Arguments.Count -gt 0) {
            & $PythonExe $ScriptPath @Arguments
        } else {
            & $PythonExe $ScriptPath
        }
        Write-Host "Completed: $Description" -ForegroundColor Green
    }
    catch {
        Write-Host "Failed: $Description" -ForegroundColor Red
        Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
    Write-Host ""
    return $true
}

# Weekly Reports Function
function Run-WeeklyReports {
    param(
        [string]$StartDate = "",
        [string]$EndDate = ""
    )
    
    Write-Host "=== WEEKLY REPORTS ===" -ForegroundColor Blue
    
    $args = @()
    if ($StartDate) { $args += "--start-date", $StartDate }
    if ($EndDate) { $args += "--end-date", $EndDate }
    
    Run-PythonScript "$ScriptDir\weekly\weekly_report.py" "Weekly Analytics Report" $args
}

# Monthly Reports Function
function Run-MonthlyReports {
    param(
        [string]$StartDate = "",
        [string]$EndDate = ""
    )
    
    Write-Host "=== MONTHLY REPORTS ===" -ForegroundColor Blue
    
    $args = @()
    if ($StartDate) { $args += "--start-date", $StartDate }
    if ($EndDate) { $args += "--end-date", $EndDate }
    
    Run-PythonScript "$ScriptDir\monthly\monthly_report.py" "Monthly Analytics Report" $args
}

# Ad Hoc Reports Function
function Run-AdHocReports {
    param(
        [string]$StartDate = "",
        [string]$EndDate = "",
        [int]$Days = 0
    )
    
    Write-Host "=== AD HOC REPORTS ===" -ForegroundColor Blue
    
    $args = @()
    if ($StartDate) { $args += "--start-date", $StartDate }
    if ($EndDate) { $args += "--end-date", $EndDate }
    if ($Days -gt 0) { $args += "--days", $Days }
    
    Run-PythonScript "$ScriptDir\ad_hoc_reports\sandra_report.py" "Sandra Custom Report" $args
    
    if (Test-Path "$ScriptDir\ad_hoc_reports\ytd_report.py") {
        Run-PythonScript "$ScriptDir\ad_hoc_reports\ytd_report.py" "Year-to-Date Report" $args
    }
    if (Test-Path "$ScriptDir\ad_hoc_reports\may24_to_may25.py") {
        Run-PythonScript "$ScriptDir\ad_hoc_reports\may24_to_may25.py" "May 2024 to May 2025 Comparison" $args
    }
}

# Sandra Report Function (dedicated)
function Run-SandraReport {
    param(
        [string]$StartDate = "",
        [string]$EndDate = "",
        [int]$Days = 7
    )
    
    Write-Host "=== SANDRA REPORT ===" -ForegroundColor Blue
    
    $args = @()
    if ($StartDate) { $args += "--start-date", $StartDate }
    if ($EndDate) { $args += "--end-date", $EndDate }
    if ($Days -gt 0) { $args += "--days", $Days }
    
    Run-PythonScript "$ScriptDir\ad_hoc_reports\sandra_report.py" "Sandra Custom Report" $args
}

# Analysis Reports Function
function Run-AnalysisReports {
    param(
        [string]$StartDate = "",
        [string]$EndDate = ""
    )
    
    Write-Host "=== ANALYSIS REPORTS ===" -ForegroundColor Blue
    
    $args = @()
    if ($StartDate) { $args += "--start-date", $StartDate }
    if ($EndDate) { $args += "--end-date", $EndDate }
    
    if (Test-Path "$ScriptDir\ad_hoc_reports\insights_report_082025.py") {
        Run-PythonScript "$ScriptDir\ad_hoc_reports\insights_report_082025.py" "August 2025 Insights Report" $args
    }
}

# Main execution
if ($Help) {
    Show-Help
}

Write-Host "=== Taxi Drivers Reporting Suite ===" -ForegroundColor Green
Write-Host "Root Directory: $RootDir" -ForegroundColor Yellow
Write-Host "Python Executable: $PythonExe" -ForegroundColor Yellow
Write-Host "Report Type: $ReportType" -ForegroundColor Yellow
if ($StartDate) { Write-Host "Start Date: $StartDate" -ForegroundColor Yellow }
if ($EndDate) { Write-Host "End Date: $EndDate" -ForegroundColor Yellow }
if ($Days -gt 0) { Write-Host "Days: $Days" -ForegroundColor Yellow }
Write-Host ""

# Execute based on report type
switch ($ReportType.ToLower()) {
    "weekly" {
        Run-WeeklyReports -StartDate $StartDate -EndDate $EndDate
    }
    "monthly" {
        Run-MonthlyReports -StartDate $StartDate -EndDate $EndDate
    }
    "adhoc" {
        Run-AdHocReports -StartDate $StartDate -EndDate $EndDate -Days $Days
    }
    "sandra" {
        Run-SandraReport -StartDate $StartDate -EndDate $EndDate -Days $Days
    }
    "analysis" {
        Run-AnalysisReports -StartDate $StartDate -EndDate $EndDate
    }
    "all" {
        Run-WeeklyReports -StartDate $StartDate -EndDate $EndDate
        Run-MonthlyReports -StartDate $StartDate -EndDate $EndDate
        Run-AdHocReports -StartDate $StartDate -EndDate $EndDate -Days $Days
        Run-AnalysisReports -StartDate $StartDate -EndDate $EndDate
    }
    default {
        Write-Host "Invalid report type: $ReportType" -ForegroundColor Red
        Write-Host "Valid options: weekly, monthly, adhoc, sandra, analysis, all" -ForegroundColor Yellow
        Show-Help
    }
}

Write-Host "=== REPORTS COMPLETED ===" -ForegroundColor Green
Write-Host "Check the output directories for generated files." -ForegroundColor Yellow

# Optional: Open output directory in Explorer
if ($OpenOutput) {
    $WeeklyOutput = "$ScriptDir\weekly\weekly_output_tmp"
    if (Test-Path $WeeklyOutput) {
        Invoke-Item $WeeklyOutput
    }
}