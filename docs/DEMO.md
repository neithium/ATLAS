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

# Start all 5 containers (Zookeeper, Kafka, Spark Master, Spark Worker, PostgreSQL)
docker compose up -d

# Verify all services are healthy
docker compose ps
```

Expected output — all containers should show `(healthy)`:
```
NAME           IMAGE                             STATUS
kafka          confluentinc/cp-kafka:7.6.1       Up (healthy)
postgres       postgres:16                       Up (healthy)
spark-master   apache/spark:3.5.1                Up (healthy)
spark-worker   apache/spark:3.5.1                Up
zookeeper      confluentinc/cp-zookeeper:7.6.1   Up (healthy)
```

### Web UIs

| Service | URL |
|---------|-----|
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

## 5. Query PostgreSQL Directly

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

## 6. Stop Everything

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
┌─────────────────────────────────────────────────────────────┐
│                    Windows Host                             │
│  ┌──────────────┐          ┌──────────────┐                │
│  │  producer.py │──────────│  consumer.py │                │
│  └──────┬───────┘          └───────┬──────┘                │
│         │ :9092                    │ :9092                  │
└─────────┼──────────────────────────┼────────────────────────┘
          │                          │
          ▼                          ▼
    ┌─────────────────────────────────────┐
    │              Kafka                  │
    │         (localhost:9092)            │
    └─────────────┬───────────────────────┘
                  │
                  ▼
    ┌─────────────────────────────────────┐
    │           Zookeeper                 │
    │         (localhost:2181)            │
    └─────────────────────────────────────┘

    ┌─────────────────────────────────────┐
    │         Spark Master                │
    │         (localhost:8080)            │
    └─────────────┬───────────────────────┘
                  │
                  ▼
    ┌─────────────────────────────────────┐
    │         Spark Worker                │
    │         (localhost:8081)            │
    └─────────────────────────────────────┘

    ┌─────────────────────────────────────┐
    │          PostgreSQL                 │
    │         (localhost:5432)            │
    └─────────────────────────────────────┘
```
