@echo off
echo =======================================================
echo  STARTING ATLAS 3-BROKER KAFKA CLUSTER (KRaft Mode)
echo =======================================================
echo.

echo Stopping any running containers and clearing old volumes...
echo (Crucial for KRaft: mismatched metadata logs will cause split-brain)
docker-compose stop broker1 broker2 broker3 kafka-init
docker-compose rm -f broker1 broker2 broker3 kafka-init
docker volume rm atlas_kafka-data-1 atlas_kafka-data-2 atlas_kafka-data-3 2>nul

echo.
echo Configuring 3-Broker KRaft cluster environment...
set KAFKA_QUORUM_VOTERS=1@broker1:9093,2@broker2:9093,3@broker3:9093
set KAFKA_REPLICATION_FACTOR=3
set KAFKA_MIN_ISR=2
set KAFKA_BOOTSTRAP=broker1:9092,broker2:9092,broker3:9092

echo.
echo Launching cluster containers...
docker-compose --profile full-cluster up -d broker1 broker2 broker3 kafka-init
docker-compose up -d --force-recreate atlas-ingestion

echo.
echo =======================================================
echo Done! Run 'docker ps' or check logs of broker1/2/3.
echo =======================================================
