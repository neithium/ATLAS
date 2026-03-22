#!/bin/bash
set -e

echo '=== Starting All-in-One PowerPulse ==='

# Data dirs
mkdir -p /data/redis /data/minio

# Setup Logging
LOG_DIR="/var/log/powerpulse"
mkdir -p $LOG_DIR
touch $LOG_DIR/redis.log $LOG_DIR/minio.log $LOG_DIR/api.log

# Redis
echo 'Starting Redis...'
redis-server --port 6379 --dir /data/redis --appendonly yes --daemonize yes --maxclients 20000 > $LOG_DIR/redis.log 2>&1

# Wait Redis
sleep 3
until redis-cli -h localhost ping; do
  echo 'Waiting Redis...'
  sleep 1
done

# MinIO
echo 'Starting MinIO...'
mkdir -p /data/minio
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin minio server /data/minio --console-address ':9001' --address ':9000' > $LOG_DIR/minio.log 2>&1 &

# Wait MinIO
sleep 5
until curl -f http://localhost:9000/minio/health/live; do
  echo 'Waiting MinIO...'
  sleep 2
done

# Python API
echo 'Starting API...'
cd /app
uvicorn main:app --host 0.0.0.0 --port 8000 --log-level info > $LOG_DIR/api.log 2>&1 &

sleep 5

# Nginx
echo 'Starting Nginx...'
nginx -g 'daemon off;' &

echo 'All services running - tailing logs...'
tail -f $LOG_DIR/*.log

