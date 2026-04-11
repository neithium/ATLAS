#!/bin/bash
set -e

echo "=== PowerPulse V2 All-in-One Ingestion Starting ==="

# ── 1. Identity & Data Directory Setup ──────────────────────────────────
# Ensure postgres user exists (Fixes "chown: invalid user" on some systems)
if ! id -u postgres >/dev/null 2>&1; then
    echo "Creating postgres user/group for data security..."
    groupadd -r postgres && useradd -r -g postgres -s /bin/false postgres
fi

DATA_DIR="/data"
mkdir -p $DATA_DIR/redis $DATA_DIR/timescale $DATA_DIR/minio $DATA_DIR/redpanda $DATA_DIR/logs

# Fix permissions for the Silo 1 (Hot Path) storage
chown -R postgres:postgres $DATA_DIR/timescale $DATA_DIR/redis $DATA_DIR/minio $DATA_DIR/redpanda

# ── 1.5. Initialize Postgres Cluster if empty ───────────────────────────
if [ ! -s "$DATA_DIR/timescale/PG_VERSION" ]; then
    echo "🏗️ Initializing empty TimescaleDB cluster in $DATA_DIR/timescale..."
    su postgres -s /bin/bash -c "/usr/lib/postgresql/15/bin/initdb -D $DATA_DIR/timescale"
    echo "shared_preload_libraries = 'timescaledb'" >> $DATA_DIR/timescale/postgresql.conf
fi

# ── 2. Start Redis ────────────────────────────────────────────────────
echo "Starting Redis..."
redis-server --port 6379 --dir $DATA_DIR/redis --appendonly yes --daemonize yes 

# ── 3. Start TimescaleDB ──────────────────────────────────────────────
echo "Starting TimescaleDB..."
su postgres -s /bin/bash -c "/usr/lib/postgresql/15/bin/postgres -D $DATA_DIR/timescale" > $DATA_DIR/logs/timescale.log 2>&1 &

# ── 4. Start Redpanda (Kafka) ──────────────────────────────────────────
echo "Starting Redpanda (Kafka-compatible)..."
# Start Redpanda in the background with persistent data dir
# Boosted memory and message limits for 1,600-device history bursts (5MB payloads)
rpk redpanda start --mode dev-container --smp 1 --memory 1G --overprovisioned --node-id 0 --check=false \
    --set redpanda.auto_create_topics_enabled=true \
    --set redpanda.log_segment_size=536870912 \
    --set redpanda.kafka_max_message_size=5242880 \
    --kafka-addr 0.0.0.0:9092 \
    --data-dir $DATA_DIR/redpanda > $DATA_DIR/logs/redpanda.log 2>&1 &

# Wait for essential services
sleep 5
until su postgres -s /bin/bash -c "psql -c 'SELECT 1'" >/dev/null 2>&1; do
  echo "Waiting for TimescaleDB..."
  sleep 2
done

# Initialize V2 Schema
echo "Initializing Telemetry Hypertable..."
# Force password for postgres user to match TS_CONN_STR
su postgres -s /bin/bash -c "psql -c \"ALTER USER postgres WITH PASSWORD 'postgres';\""
su postgres -s /bin/bash -c "psql -c 'CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;'"
su postgres -s /bin/bash -c "psql -c \"
CREATE TABLE IF NOT EXISTS telemetry_live (
    metric_time TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    platform_customer_id TEXT,
    application_customer_id TEXT,
    amb_temp FLOAT,
    avg_watts FLOAT,
    cpu_avg_freq BIGINT,
    cpu_max INT,
    cpu_pwr_sav_lim INT,
    cpu_util INT,
    cpu_watts INT,
    gpu_watts INT,
    min_watts INT,
    peak_watts INT,
    server_name TEXT,
    model TEXT,
    processor_vendor TEXT,
    server_generation TEXT,
    report_type TEXT,
    metric_type TEXT,
    status BOOLEAN,
    error_reason TEXT,
    tags TEXT,
    location_id TEXT,
    location_city TEXT,
    location_state TEXT,
    location_country TEXT,
    location_name TEXT
);\""
su postgres -s /bin/bash -c "psql -c \"SELECT create_hypertable('telemetry_live', 'metric_time', if_not_exists => TRUE);\""
su postgres -s /bin/bash -c "psql -c \"CREATE INDEX IF NOT EXISTS idx_device_time ON telemetry_live (device_id, metric_time DESC);\""

# ── 5. Start MinIO ────────────────────────────────────────────────────
echo "Starting MinIO..."
export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin
minio server $DATA_DIR/minio --address ":9000" --console-address ":9001" > $DATA_DIR/logs/minio.log 2>&1 &

# ── 6. Identity Auto-Bootstrap ──────────────────────────────────────────
# Ensure the 80,000-device registry exists (Scaling from Zero)
if [ ! -f "/app/device_configs.json" ]; then
    echo "🏝️ Registry missing! Auto-Bootstrapping 80,000-device hierarchical fleet..."
    python3 v2/scripts/generate_registry.py --scale 80000
fi

# ── 7. Start Unified Python Service ──────────────────────────────────
echo "Starting V2 API + Poller (Uvicorn)..."
cd /app

# Crucial Environment Flags for V2 logic
export ENABLE_POLLER=${ENABLE_POLLER:-true}
export ENABLE_TSDB_PUSH=1
export TS_CONN_STR="host=127.0.0.1 port=5432 dbname=postgres user=postgres password=postgres"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"

# Execute Uvicorn in the background and stream to terminal
python3 -m uvicorn main:app --host 0.0.0.0 --port 8001 2>&1 | tee -a $DATA_DIR/logs/api.log &

# ── 7. Start Nginx ────────────────────────────────────────────────────
echo "Starting Nginx Proxy (Core Entry)..."
nginx -g "daemon off;"
