@echo off
echo Stopping any running containers and cleaning volumes...
docker-compose --profile full-cluster down -v
echo Starting full 3-node Kafka cluster...
set KAFKA_QUORUM_VOTERS=1@broker1:9093,2@broker2:9093,3@broker3:9093
set KAFKA_REPLICATION_FACTOR=3
set KAFKA_MIN_ISR=2
set KAFKA_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092,broker3:9092
docker-compose --profile full-cluster up -d broker1 broker2 broker3
docker-compose up -d kafka-init
echo Done! Run 'docker ps' to check status.