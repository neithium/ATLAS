#!/bin/bash
set -e

echo '=== Starting All-in-One PowerPulse ==='

# Data dirs
mkdir -p /data/redis /data/minio

# Redis
echo 'Starting Redis...'
redis-server --port 6379 --dir /data/redis --appendonly yes --daemonize yes --maxclients 20000 &

# Wait Redis
sleep 3
until redis-cli -h localhost ping; do
  echo 'Waiting Redis...'
  sleep 1
done

# MinIO
echo 'Starting MinIO...'
mkdir -p /data/minio
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin minio server /data/minio --console-address ':9001' --address ':9000' &

# Wait MinIO
sleep 5
until curl -f http://localhost:9000/minio/health/live; do
  echo 'Waiting MinIO...'
  sleep 2
done

# Python API
echo 'Starting API...'
cd /app
uvicorn main:app --host 0.0.0.0 --port 8000 --log-level info &

sleep 5

# Nginx
echo 'Starting Nginx...'
nginx -g 'daemon off;' &

echo 'All services running - tailing logs to keep alive...'
tail -f /dev/null

