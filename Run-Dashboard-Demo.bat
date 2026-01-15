@echo off
setlocal
rem One-click demo launcher for Editorial Analytics Dashboard
set SCRIPT_DIR=%~dp0
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%Start-Dashboard.ps1" -Demo -Build -Detach -Open
endlocal
