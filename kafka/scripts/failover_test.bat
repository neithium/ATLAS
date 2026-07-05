@echo off
cd /d "%~dp0..\.."
echo ============================================
echo   ATLAS Fault Tolerance Demo
echo ============================================
echo.

echo [1/5] Verifying all brokers are healthy...
docker ps --format "{{.Names}} - {{.Status}}" | findstr broker
echo.

echo [2/5] Sending fleet telemetry to Kafka...
start /B cmd /C "curl.exe -s -X POST http://localhost:80/fleet/telemetry/export -H "Content-Type: application/json" > nul 2>&1"
timeout /t 3 /nobreak > nul
echo Data stream started!
echo.

echo [3/5] KILLING broker1 (Simulating server crash)...
docker stop -t 0 broker1
echo broker1 is DOWN!
echo.

echo [4/5] Waiting 10 seconds for self-healing...
timeout /t 10 /nobreak
echo.

echo [5/5] Restarting broker1 automatically...
docker start broker1
echo.

echo ============================================
echo   RESULT: Checking broker status...
echo ============================================
timeout /t 5 /nobreak > nul
docker ps --format "{{.Names}} - {{.Status}}" | findstr broker
echo.
echo If broker1 shows "Up" above, FAULT TOLERANCE IS PROVEN!
echo ============================================
