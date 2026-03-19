@echo off
echo Stopping all brokers...
docker-compose --profile full-cluster down
echo Done!