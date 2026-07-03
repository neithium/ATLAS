@echo off
cd /d "%~dp0..\.."
echo ============================================
echo   ATLAS: Continuous Telemetry Stream
echo   Sending fleet data in waves...
echo ============================================
echo.

:loop
echo [%time%] Sending fleet telemetry wave...
curl.exe -s -X POST http://localhost:80/fleet/telemetry/export -H "Content-Type: application/json"
echo.
timeout /t 2 /nobreak > nul
goto loop
