@echo off
echo Stopping any running Kafka brokers...
docker-compose --profile full-cluster stop broker1 broker2 broker3 kafka-init
docker-compose --profile full-cluster rm -f broker1 broker2 broker3 kafka-init

set KAFKA_BOOTSTRAP=broker1:9092

echo Starting Single Kafka Broker...
docker-compose up -d broker1 kafka-init
docker-compose up -d --force-recreate atlas-ingestion

echo Done! Run 'docker ps' to check status.