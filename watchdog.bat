@echo off
echo ============================================
echo   ATLAS Broker Watchdog (Self-Healing)
echo   Monitors and auto-restarts dead brokers
echo ============================================
echo.

:loop
REM Check broker1
docker inspect broker1 --format "{{.State.Running}}" 2>nul | findstr "true" >nul
if errorlevel 1 (
    echo [%date% %time%] WARNING: broker1 is DOWN! Restarting...
    docker start broker1
    echo [%date% %time%] broker1 restarted successfully.
)

REM Check broker2
docker inspect broker2 --format "{{.State.Running}}" 2>nul | findstr "true" >nul
if errorlevel 1 (
    echo [%date% %time%] WARNING: broker2 is DOWN! Restarting...
    docker start broker2
    echo [%date% %time%] broker2 restarted successfully.
)

REM Check broker3
docker inspect broker3 --format "{{.State.Running}}" 2>nul | findstr "true" >nul
if errorlevel 1 (
    echo [%date% %time%] WARNING: broker3 is DOWN! Restarting...
    docker start broker3
    echo [%date% %time%] broker3 restarted successfully.
)

timeout /t 5 /nobreak > nul
goto loop
