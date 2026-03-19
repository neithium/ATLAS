@echo off
echo Stopping any running containers and cleaning volumes...
docker-compose --profile full-cluster down -v
echo Starting full 3-node Kafka cluster...
set KAFKA_QUORUM_VOTERS=1@broker1:9093,2@broker2:9093,3@broker3:9093
set KAFKA_REPLICATION_FACTOR=3
set KAFKA_MIN_ISR=2
docker-compose --profile full-cluster up -d
echo Done! Run 'docker ps' to check status.