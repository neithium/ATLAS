# ATLAS Kafka Broker Layer

Apache Kafka (KRaft) cluster infrastructure for the ATLAS telemetry platform — single-broker dev mode and 3-broker HA mode.

**Author:** Nandini

**Related docs:** Airflow orchestration lives in [`orchestration/README.md`](../orchestration/README.md). Spark consumer details in [`processing/readme.md`](../processing/readme.md).

---

# Overview

The Kafka layer is the **real-time shock absorber** between ingestion and Spark processing:

```text
Ingestion API  ──►  raw-server-metrics (12 partitions)  ──►  Spark Streaming / Batch
                           │
                           └── raw-server-metrics-dlq (invalid records)
```

Broker definitions live in the repo-root `docker-compose.yml` (`broker1`, `broker2`, `broker3`, `kafka-init`). Operational scripts for this module live under **`kafka/scripts/`**.

---

# Architecture

```text
                    ┌─────────────────────────────┐
                    │   atlas-ingestion (API)     │
                    │   kafka_producer.py         │
                    └─────────────────────────────┘
                                  │
                                  ▼
              ┌───────────────────────────────────────┐
              │         Kafka KRaft Cluster           │
              │  broker1  broker2  broker3 (optional) │
              │  topic: raw-server-metrics (RF 1/3)   │
              └───────────────────────────────────────┘
                                  │
                                  ▼
              ┌───────────────────────────────────────┐
              │   atlas-processor (Spark consumer)    │
              └───────────────────────────────────────┘
```

---

# Technology

| Setting | Value |
|---------|-------|
| Image | `soldevelo/kafka:4.0` (KRaft — no Zookeeper) |
| Cluster ID | `atlas-telemetry-cluster-3node` |
| Default partitions | 12 on `raw-server-metrics` |
| Producer compression | LZ4 (`aiokafka` in `ingestion/core/kafka_producer.py`) |

---

# Deployment Modes

| Mode | Script | Brokers | RF / MIN_ISR | Use case |
|------|--------|---------|--------------|----------|
| Single broker (dev) | `kafka/scripts/single.bat` | `broker1` | RF=1, MIN_ISR=1 | Local dev, demos |
| Full cluster (HA) | `kafka/scripts/cluster.bat` | `broker1–3` | RF=3, MIN_ISR=2 | Fault-tolerance demos |

Both scripts **wipe stale KRaft volumes** before startup. Always use them when switching modes — mismatched quorum metadata causes split-brain.

**Root shortcuts:** `single.bat` and `cluster.bat` at repo root forward to `kafka/scripts/`.

---

# Broker Topology (Full Cluster)

```text
broker1  — controller + broker — host ports 9062/9063/9064
broker2  — controller + broker — host ports 9065/9066/9067  [profile: full-cluster]
broker3  — controller + broker — host ports 9068/9069/9070  [profile: full-cluster]
kafka-init — one-shot topic creation after broker1 is healthy
```

Environment injected by `cluster.bat`:

```text
KAFKA_QUORUM_VOTERS=1@broker1:9093,2@broker2:9093,3@broker3:9093
KAFKA_REPLICATION_FACTOR=3
KAFKA_MIN_ISR=2
KAFKA_BOOTSTRAP=broker1:9092,broker2:9092,broker3:9092
```

---

# Topics

| Topic | Partitions | Purpose |
|-------|------------|---------|
| `raw-server-metrics` | 12 | Primary telemetry (48-field Golden Schema) |
| `raw-server-metrics-dlq` | 3 | Dead Letter Queue (Spark invalid records) |

Retry/failure topics are created by the Spark processing engine — see `processing/readme.md`.

---

# Module Structure

```text
kafka/
├── README.md                 # This document
└── scripts/
    ├── single.bat            # Single-broker stack startup
    ├── cluster.bat           # 3-broker HA stack startup
    ├── stream_data.bat       # Continuous fleet export loop → Kafka
    ├── failover_test.bat     # Kill broker1 demo / self-heal
    ├── watchdog.bat          # Auto-restart dead brokers
    └── check_kafka.py        # One-message consumer smoke test

Repo root (shared infra):
├── docker-compose.yml        # broker1/2/3, kafka-init service definitions
└── single.bat / cluster.bat  # Thin wrappers → kafka/scripts/
```

**Producer integration** (not duplicated here): `ingestion/core/kafka_producer.py` and export endpoints in `ingestion/v2/api/api_v2.py`.

---

# Execution Guide

## 1. Start single-broker mode (development)

**Where:** repo root (HOST)

```powershell
.\kafka\scripts\single.bat
# or
.\single.bat
```

**Expect:** `broker1`, `kafka-init`, `atlas-ingestion`, and downstream services running.

## 2. Start 3-broker cluster (HA)

```powershell
.\kafka\scripts\cluster.bat
# or
.\cluster.bat
```

**Expect:** `broker1`, `broker2`, `broker3` all healthy after ~2–3 minutes.

## 3. Verify topics

```powershell
docker exec broker1 kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec broker1 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw-server-metrics
```

**Expect:** `raw-server-metrics` with 12 partitions; RF=1 (single) or RF=3 (cluster).

## 4. Produce test messages (via ingestion API)

```powershell
curl -X POST "http://localhost:8001/pcid/PLATCUST0001/acid/PLATCUST0001_APPCUST01/telemetry/latest/export"
```

Or continuous stream:

```powershell
.\kafka\scripts\stream_data.bat
```

## 5. Fault-tolerance demo (cluster mode only)

```powershell
.\kafka\scripts\failover_test.bat
```

Optional background watchdog:

```powershell
.\kafka\scripts\watchdog.bat
```

---

# Testing

## Broker smoke test

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}" | findstr broker
docker exec broker1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## Producer smoke test

```powershell
curl http://localhost:8001/health
curl -X POST "http://localhost:8001/pcid/PLATCUST0001/acid/PLATCUST0001_APPCUST01/telemetry/latest/export"
docker exec atlas-ingestion python3 /app/v2/scripts/check_kafka_msg.py
```

## Consumer smoke test (host, external port)

```powershell
$env:KAFKA_BOOTSTRAP="localhost:9064"
python kafka/scripts/check_kafka.py
```

Note: Kafka 4.0 consumer CLI can show SyncGroup quirks; topic describe + producer tests are more reliable.

---

# Ingestion API Endpoints (Kafka producers)

| Endpoint | Scope |
|----------|-------|
| `POST /fleet/telemetry/export` | Fleet-wide export |
| `POST /pcid/{pcid}/acid/{acid}/telemetry/latest/export` | Hierarchy export |
| `POST /pcid/{pcid}/acid/{acid}/telemetry/historical/first/export` | Oldest N points |
| `POST /pcid/{pcid}/acid/{acid}/id/{devices}/export` | Surgical device export |

---

# Troubleshooting

## KRaft split-brain / broker won't start

**Symptom:** Quorum voter mismatch after switching single ↔ cluster.

**Fix:**

```powershell
docker-compose --profile full-cluster down
docker volume rm atlas_kafka-data-1 atlas_kafka-data-2 atlas_kafka-data-3
.\kafka\scripts\cluster.bat
```

## Producer disconnected

**Check:** `KAFKA_BOOTSTRAP` in `docker-compose.yml` — cluster mode needs all three brokers listed.

## Consumer CLI timeout

Producer and broker health can still be fine. Prefer `check_kafka_msg.py` or topic `--describe`.

---

# Integration

| Module | Role |
|--------|------|
| `ingestion/` | Publishes Golden Schema records to Kafka |
| `processing/` | Consumes `raw-server-metrics`, writes DLQ |
| `orchestration/` | Airflow triggers exports and monitors DLQ/broker health |

---

# Output

- Healthy replicated Kafka topics buffering telemetry bursts
- DLQ topic for invalid downstream records
- Scripts for HA demos, continuous streaming, and broker self-healing
