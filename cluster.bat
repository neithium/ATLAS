@echo off
echo Stopping any running containers (preserving data)...
docker-compose --profile full-cluster down

echo Starting Full 3-Node Atlas Cluster (Brokers + Full Stack)...
REM We override the .env defaults with Cluster-specific settings (RF=3, ISR=2)
cmd /V /C "set KAFKA_QUORUM_VOTERS=1@broker1:9093,2@broker2:9093,3@broker3:9093&&set KAFKA_REPLICATION_FACTOR=3&&set KAFKA_MIN_ISR=2&&docker-compose --profile full-cluster up -d broker1 broker2 broker3 kafka-init atlas-ingestion airflow-db airflow-webserver airflow-scheduler atlas-processor atlas-lakehouse atlas-analytics"

echo Done! Run 'docker ps' to check status.