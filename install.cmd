@echo off
:: Messages — installer for Windows
:: Requires Python 3.12+ and PowerShell 5+
setlocal EnableDelayedExpansion

echo.
echo  Messages - self-hosted federated chat
echo  ========================================
echo.

:: Check Python
python --version 2>nul | findstr /r "3\.1[2-9]\|3\.[2-9][0-9]" >nul
if errorlevel 1 (
    echo  [ERROR] Python 3.12 or newer is required.
    echo  Download from https://python.org
    pause
    exit /b 1
)
echo  [OK] Python found

:: Run PowerShell installer
powershell -ExecutionPolicy Bypass -File "%~dp0install.ps1" %*
