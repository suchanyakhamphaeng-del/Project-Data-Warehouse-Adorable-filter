@echo off
title Data Ingestion Pipeline
echo ============================================================
echo               Starting Data Ingestion Pipeline
echo ============================================================
echo.

:: Navigate to project root directory
cd /d "%~dp0..\.."

powershell -ExecutionPolicy Bypass -File "scripts\powershell\ingest_data.ps1"

echo.
echo ============================================================
echo Process finished. Press any key to close this window.
echo ============================================================
pause