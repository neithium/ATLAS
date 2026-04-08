@echo off
echo WARNING: This will delete ALL data (Redis, MinIO, Kafka) and reset the environment.
set /p confirm="Are you sure? (y/n): "
if /i "%confirm%" neq "y" exit /b

echo Stopping containers and WIPING volumes...
docker-compose --profile full-cluster down -v
echo Done! All persistent data has been removed.
