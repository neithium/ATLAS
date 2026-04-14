# PowerPulse V3 High-Scale Ingestion (100k+ Devices)

This service implements a production-grade **Hot/Cold IoT Architecture** designed to handle **100,000 devices** at 5-minute intervals (~200,000,000 telemetry records per week).

## 🏙 V3 Triple-Silo Persistence Strategy

The system implements a **Triple-Silo** lifecycle to ensure absolute data durability and high-performance analytics:

1.  **Silo 1 (Hot Path - TimescaleDB)**:
    *   **Usage**: Real-time REST API discovery and immediate historical lookups (last 7 days).
    *   **Scale**: Optimized with **Columnar Compression** and segment-based partitioning.
2.  **Silo 2 (Cold Path - MinIO `telemetry-raw`)**:
    *   **Strategy**: **Hourly Time-Sliced Migration** (Prevents system-lock spikes).
    *   **Format**: Snappy-Compressed Parquet in **Hive Partitioning** (`year/month/day/hour/`).
3.  **Silo 3 (Archive Path - MinIO `telemetry-archive`)**:
    *   **Backup Backend**: Bit-for-bit immutable permanent backup of every hourly slice.

## 🚀 High-Performance Archival & Analytics

### 1. Time-Sliced Archival (The Migration Engine)
Instead of a massive daily batch, the system migrates data every hour to keep resource usage flat and stable.
*   **Run Archival**: `docker exec atlas-ingestion python3 /app/v2/scripts/test_archive_48field.py`
*   **Manual Override**: Use `manual_archive.py` to push specific historical windows for testing.

### 2. Spark Analytics (The Fetch Test)
Spark is configured to read the hourly Parquet files directly from MinIO with **Columnar Pruning** for maximum speed.
*   **Run Fetch Benchmark**: 
    `docker compose run --rm atlas-processor spark-submit --master local[*] --packages org.apache.hadoop:hadoop-aws:3.3.4 /app/jobs/spark_minio_reader.py`

## 📡 Hierarchical API Endpoints (V3.1)

Proxied through **Nginx on Port 80**. 

*   **Fixed Window Export**: `GET /pcid/{pid}/acid/{aid}/telemetry?days=7`
*   **Latest Points Sync**: `GET /pcid/{pid}/acid/{aid}/telemetry/latest?count=2016`

**Performance**: Handles 1,600 devices (~3.2 Million points) in **42 seconds** via Kafka-LZ4 streaming.

## 🛠 Project Structure (Cleaned)

*   `v2/api/`: Core FastAPI logic with Hierarchical stream workers.
*   `v2/scripts/`: Time-sliced archival and database pre-fill tools.
*   `processing/jobs/`: Spark Streaming and Batch analytical jobs.

---
**Baseline Throughput**: Engineered to handle **200 Million records per week** on 3-node localized infrastructure. 🏆🏁
