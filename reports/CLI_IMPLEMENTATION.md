# CLI Implementation Summary for Taxi Drivers Reporting Suite

## What Was Implemented

### 1. Weekly Report CLI (`weekly_report.py`)
**Arguments:**
- `--start-date`: Start date in YYYY-MM-DD format
- `--end-date`: End date in YYYY-MM-DD format  
- `--output-dir`: Custom output directory
- `--gemini`: Enable Gemini AI summary generation
- `--template`: Enable template summary generation

**Usage Examples:**
```bash
python weekly_report.py --start-date 2025-10-01 --end-date 2025-10-07
python weekly_report.py --start-date 2025-10-01 --end-date 2025-10-07 --gemini --template
```

### 2. Monthly Report CLI (`monthly_report.py`)
**Arguments:**
- `--start-date`: Start date in YYYY-MM-DD format
- `--end-date`: End date in YYYY-MM-DD format
- `--output-dir`: Custom output directory
- `--month`: Month name for labeling (e.g., "October")

**Usage Examples:**
```bash
python monthly_report.py --start-date 2025-10-01 --end-date 2025-10-31 --month October
```

### 3. Sandra Report CLI (`sandra_report.py`)
**Arguments:**
- `--start-date`: Start date in YYYY-MM-DD format
- `--end-date`: End date in YYYY-MM-DD format
- `--days`: Number of days back from today (default: 7)
- `--output-dir`: Custom output directory
- `--top-n`: Number of top articles to include (default: 100)

**Usage Examples:**
```bash
python sandra_report.py --start-date 2025-10-15 --end-date 2025-10-22 --top-n 50
python sandra_report.py --days 14 --top-n 200
```

### 4. PowerShell Orchestrator (`run_reports.ps1`)
**Parameters:**
- `-ReportType`: weekly, monthly, adhoc, analysis, all
- `-StartDate`: Start date in YYYY-MM-DD format
- `-EndDate`: End date in YYYY-MM-DD format
- `-OpenOutput`: Automatically open output directory
- `-Help`: Show usage help

**Usage Examples:**
```powershell
# Run weekly reports for specific date range
.\run_reports.ps1 -ReportType weekly -StartDate 2025-10-01 -EndDate 2025-10-07 -OpenOutput

# Run all reports for a month
.\run_reports.ps1 -ReportType all -StartDate 2025-10-01 -EndDate 2025-10-31

# Run only ad-hoc reports
.\run_reports.ps1 -ReportType adhoc -StartDate 2025-10-15 -EndDate 2025-10-22

# Show help
.\run_reports.ps1 -Help
```

## Key Features

### Flexible Date Handling
- All scripts fall back to configuration defaults if no dates provided
- Supports both absolute dates and relative date calculations
- Dynamic filename generation based on date ranges

### Error Handling
- Graceful fallback when CLI arguments are missing
- Clear error messages and help text
- PowerShell script includes comprehensive error handling

### Extensibility
- Easy to add new CLI arguments to existing scripts
- PowerShell functions can be extended with new parameters
- File existence checks prevent errors for missing reports

### Integration
- PowerShell script automatically passes arguments to Python scripts
- Consistent argument naming across all reports
- Unified interface for running multiple report types

## Benefits

1. **Automation-Ready**: Can be easily integrated into scheduled tasks or CI/CD pipelines
2. **Flexible Scheduling**: Support for custom date ranges beyond default configurations
3. **Batch Processing**: PowerShell script can run multiple reports with consistent parameters
4. **User-Friendly**: Clear help text and error messages
5. **Maintainable**: Consistent CLI patterns across all report scripts