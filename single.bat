@echo off
echo Stopping any running Kafka brokers...
docker-compose --profile full-cluster stop broker1 broker2 broker3 kafka-init
docker-compose --profile full-cluster rm -f broker1 broker2 broker3 kafka-init

echo Wiping old Kafka cluster state to prevent topic corruption...
docker volume rm atlas_kafka-data-1 atlas_kafka-data-2 atlas_kafka-data-3 >nul 2>&1

set KAFKA_BOOTSTRAP=broker1:9092

echo Starting Atlas Stack (Single Broker + API + Airflow + Processing)...
docker-compose up -d broker1 kafka-init atlas-ingestion airflow-db airflow-webserver airflow-scheduler atlas-processor atlas-lakehouse atlas-analytics

echo Done! Run 'docker ps' to check status.
