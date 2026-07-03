@echo off
cd /d "%~dp0..\.."
echo =======================================================
echo  STARTING ATLAS 3-BROKER KAFKA CLUSTER (KRaft Mode)
echo =======================================================
echo.

echo Stopping any running containers and clearing old volumes...
echo (Crucial for KRaft: mismatched metadata logs will cause split-brain)
docker-compose --profile full-cluster down
docker-compose stop broker1 broker2 broker3 kafka-init 2>nul
docker-compose rm -f broker1 broker2 broker3 kafka-init 2>nul
docker volume rm atlas_kafka-data-1 atlas_kafka-data-2 atlas_kafka-data-3 2>nul

echo.
echo Configuring 3-Broker KRaft cluster environment...
set KAFKA_QUORUM_VOTERS=1@broker1:9093,2@broker2:9093,3@broker3:9093
set KAFKA_REPLICATION_FACTOR=3
set KAFKA_MIN_ISR=2
set KAFKA_BOOTSTRAP=broker1:9092,broker2:9092,broker3:9092

echo.
echo Launching full cluster stack (Brokers + Ingestion + Airflow + Processing)...
cmd /V /C "set KAFKA_QUORUM_VOTERS=1@broker1:9093,2@broker2:9093,3@broker3:9093&&set KAFKA_REPLICATION_FACTOR=3&&set KAFKA_MIN_ISR=2&&docker-compose --profile full-cluster up -d broker1 broker2 broker3 kafka-init atlas-ingestion airflow-db airflow-webserver airflow-scheduler atlas-processor atlas-lakehouse atlas-analytics"

echo.
echo =======================================================
echo Done! Run 'docker ps' to check status.
echo =======================================================
