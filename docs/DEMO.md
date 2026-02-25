# TVMJNS — Demo Instructions

Step-by-step guide to run the real-time data streaming platform.

---

## Prerequisites

- **Docker Desktop** running (with Linux containers)
- **Python 3.11+** installed
- Virtual environment set up with dependencies installed

---

## 1. Start the Infrastructure

```powershell
# Navigate to project root
cd c:\Users\manth\Documents\GitHub\TVMJNS

# Start all 6 containers (Zookeeper, Kafka, Spark Master, Spark Worker, PostgreSQL, Adminer)
docker compose up -d

# Verify all services are healthy
docker compose ps
```

Expected output — all containers should show `(healthy)`:
```
NAME           IMAGE                             STATUS
adminer        adminer:latest                    Up
kafka          confluentinc/cp-kafka:7.6.1       Up (healthy)
postgres       postgres:16                       Up (healthy)
spark-master   apache/spark:3.5.1                Up (healthy)
spark-worker   apache/spark:3.5.1                Up
zookeeper      confluentinc/cp-zookeeper:7.6.1   Up (healthy)
```

### Web UIs

| Service | URL |
|---------|-----|
| **Adminer (DB GUI)** | http://localhost:8888 |
| Spark Master | http://localhost:8080 |
| Spark Worker | http://localhost:8081 |
| Spark App UI | http://localhost:4040 (when job running) |

---

## 2. Initialize the Database

```powershell
# Run from project root
.\.venv\Scripts\python.exe scripts\test_db.py --init --sample
```

This will:
- Connect to PostgreSQL
- Create tables (`sensors`, `telemetry_readings`, `alerts`, `telemetry_stats`)
- Insert 5 sample sensors and some test data

---

## 3. Run the Kafka Producer

Open a **new terminal** and run:

```powershell
cd c:\Users\manth\Documents\GitHub\TVMJNS
.\.venv\Scripts\python.exe scripts\producer.py
```

You'll see output like:
```
============================================================
TVMJNS — Kafka Producer (Telemetry Simulator)
============================================================
Bootstrap servers: localhost:9092
Topic: telemetry
Sensors: sensor_001, sensor_002, sensor_003, sensor_004, sensor_005
Interval: 1s
============================================================
Press Ctrl+C to stop

✓ Connected to Kafka

[1] sensor_003: temp=28.45°C, humidity=52.3%
✓ Sent to telemetry [partition=0, offset=0]
[2] sensor_001: temp=22.10°C, humidity=45.8%
✓ Sent to telemetry [partition=0, offset=1]
...
```

Press `Ctrl+C` to stop the producer.

---

## 4. Run the Kafka Consumer

Open **another terminal** and run:

```powershell
cd c:\Users\manth\Documents\GitHub\TVMJNS
.\.venv\Scripts\python.exe scripts\consumer.py
```

You'll see the telemetry messages being consumed:
```
============================================================
TVMJNS — Kafka Consumer (Telemetry Reader)
============================================================
Bootstrap servers: localhost:9092
Topic: telemetry
Consumer group: telemetry-consumers
============================================================
Press Ctrl+C to stop

✓ Connected to Kafka

[0:0] sensor_003 @ 2026-02-25T22:40:15 | temp= 28.5°C  humidity= 52.3%  pressure= 1015.2hPa  battery= 85.0%
[0:1] sensor_001 @ 2026-02-25T22:40:16 | temp= 22.1°C  humidity= 45.8%  pressure= 1012.8hPa  battery= 92.3%
...
```

Press `Ctrl+C` to stop the consumer.

---

## 5. Run Spark Alert Processor

This demonstrates Spark processing: reads telemetry from Kafka, checks thresholds, and writes alerts to PostgreSQL.

**Option 1: Use the batch script (easiest)**
```powershell
cd c:\Users\manth\Documents\GitHub\TVMJNS
.\run_spark.bat
```

**Option 2: Run directly with docker exec**
```powershell
docker exec spark-master /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy2 --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3" /scripts/spark_docker_alerts.py
```

> **Note:** The Spark job runs inside the Docker container because Windows Java 21+ is incompatible with Hadoop. The container uses Java 11 which works correctly.

**Thresholds checked:**
- `temperature > 30°C` → **HIGH_TEMPERATURE** alert (warning)
- `battery_level < 20%` → **LOW_BATTERY** alert (critical)

Expected output:
```
============================================================
TVMJNS — Spark Batch Alert Processor (Docker)
============================================================
Kafka: kafka:29092
PostgreSQL: jdbc:postgresql://postgres:5432/streaming_db
Thresholds: temp > 30.0°C, battery < 20.0%
============================================================

Reading from Kafka topic 'telemetry'...
Found 947 telemetry records

Sample telemetry data:
+----------+--------------------------------+-----------+--------+--------+-------------+
|sensor_id |timestamp                       |temperature|humidity|pressure|battery_level|
+----------+--------------------------------+-----------+--------+--------+-------------+
|sensor_004|2026-02-25T17:35:58.300128+00:00|19.96      |42.71   |1017.47 |64.3         |
|sensor_003|2026-02-25T17:36:03.054518+00:00|34.94      |59.84   |1016.3  |95.2         |
...

Checking thresholds:
  - Temperature > 30.0C
  - Battery < 20.0%

ALERTS FOUND: 334

+----------+----------------+--------+-----------+--------------------------+
|sensor_id |alert_type      |severity|message    |triggered_at              |
+----------+----------------+--------+-----------+--------------------------+
|sensor_003|HIGH_TEMPERATURE|warning |Temp=34.9C |2026-02-25 18:08:32.056612|
|sensor_004|LOW_BATTERY     |critical|Battery=15%|2026-02-25 18:08:32.056612|
...

Writing to PostgreSQL...
Wrote 334 alerts to 'alerts' table

View in Adminer: http://localhost:8888

Done!
```

---

## 6. View Data in Adminer (Database GUI)

Open **http://localhost:8888** in your browser.

**Login credentials:**
| Field | Value |
|-------|-------|
| System | PostgreSQL |
| Server | `postgres` |
| Username | `streaming_user` |
| Password | `streaming_pass` |
| Database | `streaming_db` |

**Tables to explore:**
- `sensors` — Registered sensors
- `alerts` — Threshold violations detected by Spark
- `telemetry_readings` — Raw telemetry data
- `telemetry_stats` — Aggregated statistics

---

## 7. Query PostgreSQL Directly

### Using Python

```powershell
.\.venv\Scripts\python.exe scripts\test_db.py
```

### Using psql (from Docker)

```powershell
docker exec -it postgres psql -U streaming_user -d streaming_db
```

Then run SQL queries:
```sql
-- List all sensors
SELECT * FROM sensors;

-- View latest readings
SELECT * FROM v_latest_readings;

-- Check active alerts
SELECT * FROM v_active_alerts;

-- Count telemetry readings
SELECT COUNT(*) FROM telemetry_readings;
```

Type `\q` to exit psql.

---

## 8. Stop Everything

```powershell
# Stop all containers
docker compose down

# To also remove data volumes (fresh start):
docker compose down -v
```

---

## Quick Reference

| Component | Command |
|-----------|---------|
| Start infrastructure | `docker compose up -d` |
| Check status | `docker compose ps` |
| View logs | `docker logs <container_name>` |
| Init database | `.\.venv\Scripts\python.exe scripts\test_db.py --init --sample` |
| Run producer | `.\.venv\Scripts\python.exe scripts\producer.py` |
| Run consumer | `.\.venv\Scripts\python.exe scripts\consumer.py` |
| **Run Spark alerts** | `.\run_spark.bat` |
| **Open Adminer** | http://localhost:8888 |
| Stop all | `docker compose down` |

---

## Troubleshooting

### "Connection refused" to Kafka
```powershell
# Check if Kafka is running
docker compose ps kafka

# Check Kafka logs
docker logs kafka --tail 50
```

### "Password authentication failed" for PostgreSQL
```powershell
# Check if you have a local PostgreSQL running on port 5432
netstat -ano | Select-String ":5432"

# If yes, stop your local PostgreSQL service (run as Admin):
Stop-Service "postgresql-x64-17"
```

### Container not starting
```powershell
# View container logs
docker logs <container_name>

# Restart a specific service
docker compose restart <service_name>
```

---

## Architecture Recap

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Windows Host                                │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌─────────────────────────┐ │
│  │  producer.py │   │  consumer.py │   │  spark_batch_alerts.py  │ │
│  └──────┬───────┘   └───────┬──────┘   └───────────┬─────────────┘ │
│         │                   │                      │                │
└─────────┼───────────────────┼──────────────────────┼────────────────┘
          │ :9092             │ :9092                │ :9092 / :5432
          ▼                   ▼                      ▼
    ┌─────────────────────────────────────────────────────────┐
    │                       Kafka                             │
    │                  (localhost:9092)                       │
    │                         │                               │
    │                   Zookeeper                             │
    │                  (localhost:2181)                       │
    └─────────────────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────────────────┐
    │              Spark Master  ←──────  Spark Worker        │
    │             (localhost:8080)       (localhost:8081)     │
    └─────────────────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────────────────┐
    │                     PostgreSQL                          │
    │                  (localhost:5432)                       │
    │                         │                               │
    │                      Adminer                            │
    │                  (localhost:8888)                       │
    └─────────────────────────────────────────────────────────┘
```

### Data Flow

```
producer.py  ──▶  Kafka (telemetry topic)  ──▶  spark_batch_alerts.py
                                                        │
                                                        ▼
                                               PostgreSQL (alerts)
                                                        │
                                                        ▼
                                                    Adminer (GUI)
```
