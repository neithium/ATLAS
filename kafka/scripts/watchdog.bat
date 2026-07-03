@echo off
cd /d "%~dp0..\.."
echo ============================================
echo   ATLAS Broker Watchdog (Self-Healing)
echo   Monitors and auto-restarts dead brokers
echo   Check interval: every 10 seconds
echo ============================================
echo.

:loop
docker inspect --format "{{.State.Status}}" broker1 2>nul | findstr /I "running" >nul
if errorlevel 1 (
    echo [%date% %time%] WARNING: broker1 is DOWN ^(status: not running^)! Restarting...
    docker start broker1
    if errorlevel 0 (
        echo [%date% %time%] broker1 restart command issued. Waiting 30s for quorum rejoin...
        timeout /t 30 /nobreak >nul
    ) else (
        echo [%date% %time%] ERROR: Failed to restart broker1. Check docker logs.
    )
) else (
    echo [%date% %time%] broker1 OK
)

docker inspect --format "{{.State.Status}}" broker2 2>nul | findstr /I "running" >nul
if errorlevel 1 (
    echo [%date% %time%] WARNING: broker2 is DOWN ^(status: not running^)! Restarting...
    docker start broker2
    if errorlevel 0 (
        echo [%date% %time%] broker2 restart command issued. Waiting 30s for quorum rejoin...
        timeout /t 30 /nobreak >nul
    ) else (
        echo [%date% %time%] ERROR: Failed to restart broker2. Check docker logs.
    )
) else (
    echo [%date% %time%] broker2 OK
)

docker inspect --format "{{.State.Status}}" broker3 2>nul | findstr /I "running" >nul
if errorlevel 1 (
    echo [%date% %time%] WARNING: broker3 is DOWN ^(status: not running^)! Restarting...
    docker start broker3
    if errorlevel 0 (
        echo [%date% %time%] broker3 restart command issued. Waiting 30s for quorum rejoin...
        timeout /t 30 /nobreak >nul
    ) else (
        echo [%date% %time%] ERROR: Failed to restart broker3. Check docker logs.
    )
) else (
    echo [%date% %time%] broker3 OK
)

echo.
timeout /t 10 /nobreak >nul
goto loop
