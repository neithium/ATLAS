# PowerPulse V3 High-Scale Ingestion (80k Devices)

This service implements a production-grade **Hot/Cold IoT Architecture** designed to handle **80,000 devices** at 5-minute intervals (~161,000,000 telemetry records per week).

## 🏙 V3 Triple-Silo Persistence Strategy

The system implements a **Triple-Silo** lifecycle for every ingestion cycle to ensure absolute data durability:

1.  **Silo 1 (Hot Path - TimescaleDB)**:
    *   **Data**: High-resolution history (5-min intervals) for the past 7 days (160M+ records).
    *   **Usage**: Powering sub-second Hierarchical REST API Discovery and On-Demand Kafka Exports.
2.  **Silo 2 (Cold Path - MinIO `telemetry-raw`)**:
    *   **Organization**: **Hive Partitioning** (`year=.../month=.../day=.../`) for Spark ingestion.
    *   **Format**: Mega-Compacted Parquet (~2GB daily blocks).
3.  **Silo 3 (Archive Path - MinIO `telemetry-archive`)**:
    *   **Baseline Backend**: Bit-for-bit permanent backup of every ingestion cycle.
    *   **Usage**: Long-term recovery and immutable auditing baseline.

## 📅 Background Event Scheduling (`APScheduler`)

The core engine uses **`APScheduler` (AsyncIOScheduler)** to manage non-blocking production work background tasks:

*   **Ingest 5-min Interval**: Executes the 80,000-device parallel poll and Hot Path bulk-insert.
*   **Archival Daily Cron (00:00)**: Triggers the Dual-Archive Mega-Compactor to flush TSDB history into the Raw and Archive MinIO buckets.

## 🚀 Deployment (Atlas Production Integration)

The V3 engine is integrated directly into the root Atlas compose environment:

```powershell
# Launch the V3 Engine from the root directory
docker compose up -d --build atlas-ingestion
```

## 📡 Hierarchical API Endpoints (V3)

The system is proxied through **Nginx on Port 80**.

### 1. Customer Fleet Discovery (Hot Path)
Retrieve snapshots for all active devices in a customer hierarchy:
`GET http://localhost/pcid/PLATCUST001/acid/APPCUST0001/devices`

### 2. Hierarchical Kafka Export (The V3 Demand-Streamer)
Trigger an on-demand historical push for specific devices in a hierarchy to the Kafka bus:
`POST http://localhost/pcid/PLATCUST001/acid/APPCUST0001/id/DEV-001,DEV-002/export`

### 3. Operational Statistics & Health
Get a real-time summary of the 160M+ record Hot Path pool:
`GET http://localhost:8001/stats` (Discovery API Port)

## 🛠 Maintenance & High-Scale Ops CLI

### 1. Backfilling History (The 80k Baseline)
Populate 7 days of historical baseline for the **80,000 device** fleet (161M records):
`docker exec atlas-ingestion python3 v2/scripts/prefill_tsdb.py --days 7 --workers 8 --scale 80000`

### 2. Verify Hot Path Count
Estimate row counts across the telemetry hypertable instantly:
`docker exec atlas-ingestion sudo -u postgres psql -c "SELECT hypertable_approximate_row_count('telemetry_live');"`

### 3. Watch the Kafka Outgress Mirror
Monitor the background demand-based streaming worker in real-time:
`docker logs -f atlas-ingestion | grep "export] Stream Complete"`

## 🪐 Scaling Strategy
*   **Parallelism**: Parallelized via `ThreadPoolExecutor` (50 workers) for 80k-device cycles in **< 3 seconds**.
*   **Throughput**: Engineered to handle **161 Million records per week** on a single-container cluster.
