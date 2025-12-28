@echo off
REM Quick Start Script for Editorial Analytics Dashboard
REM Windows Batch File

echo ========================================
echo Editorial Analytics Dashboard
echo Starting application...
echo ========================================
echo.

REM Check if virtual environment exists
if not exist "venv\" (
    echo Creating virtual environment...
    python -m venv venv
    echo.
)

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Install dependencies if needed
echo Checking dependencies...
pip install -q -r requirements.txt

echo.
echo ========================================
echo Starting dashboard server...
echo ========================================
echo.

REM Run the application
python app.py

pause
