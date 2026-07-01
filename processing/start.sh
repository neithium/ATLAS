#!/bin/bash

set -eo pipefail

echo "======================================"
echo " ATLAS Spark Processing Engine"
echo "======================================"

mkdir -p /app/data/raw
mkdir -p /app/data/processed/stream
mkdir -p /app/data/processed/batch
mkdir -p /app/checkpoints
mkdir -p /app/logs

touch /app/logs/worker1.log
touch /app/logs/worker2.log
touch /app/logs/dlq.log

echo ""
echo "Waiting for Kafka..."

until nc -z broker1 9092
do
    echo "Kafka not ready..."
    sleep 3
done

echo "Kafka detected."

sleep 10

echo ""
echo "======================================"
echo "Starting DLQ Reviewer"
echo "======================================"

python3 /app/dlq/dlq_reviewer.py \
> /app/logs/dlq.log 2>&1 &

sleep 10

echo ""
echo "======================================"
echo "Starting Spark Worker 1"
echo "======================================"

WORKER_ID=1 spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
/app/jobs/kafka_streaming.py \
> /app/logs/worker1.log 2>&1 &

sleep 30

echo ""
echo "======================================"
echo "Starting Spark Worker 2"
echo "======================================"

WORKER_ID=2 spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
/app/jobs/kafka_streaming.py \
> /app/logs/worker2.log 2>&1 &

echo ""
echo "======================================"
echo "ATLAS Processor Ready"
echo "======================================"

tail -F \
/app/logs/worker1.log \
/app/logs/worker2.log \
/app/logs/dlq.log