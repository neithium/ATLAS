# ATLAS Airflow Orchestration Layer

> **Ownership**: S Nandini (Streaming & Orchestration sub-team)  

Apache Airflow DAGs that coordinate the ATLAS batch ingestion path and operational supervisor systems.

---

## Overview

Airflow acts as the **control plane** for ATLAS. It does not run heavy compute locally; instead, it triggers and supervises tasks inside Docker containers via shell commands executed against the mounted Docker Unix socket.

```text
┌──────────────────────────────────────────────────────────┐
│              Apache Airflow (orchestration)               │
│  atlas_batch_pipeline   │   atlas_streaming_supervisor   │
│  atlas_kafka_health                                      │
└──────────────────────────────────────────────────────────┘
         │ docker exec via Unix socket
         ▼
   atlas-ingestion │ atlas-processor │ atlas-lakehouse │ atlas-analytics
```

---

## Services (docker-compose)

| Service | Host Port | Role |
| :--- | :--- | :--- |
| `airflow-db` | Internal | PostgreSQL metadata store |
| `airflow-webserver` | `8081 → 8080` | DAG web management console |
| `airflow-scheduler` | Internal | Workflow trigger and task scheduling engine |

Volumes mounted into Airflow containers:
* `./orchestration/dags` → `/opt/airflow/dags`
* `./orchestration/plugins` → `/opt/airflow/plugins`
* `/var/run/docker.sock` → Docker API socket for container exec operations

---

## Required Airflow Connection

Create once in the UI (`Admin → Connections`):

| Conn Id | Type | Host |
| :--- | :--- | :--- |
| `atlas_ingestion_api` | HTTP | `http://atlas-ingestion:8001` |

---

## DAG Inventory

The platform orchestrates 3 core DAGs:

| DAG ID | Schedule | Purpose |
| :--- | :--- | :--- |
| `atlas_batch_pipeline` | `@hourly` | Coordinates the Lambda Batch path: export → Spark Batch → check refined parquet → ClickHouse Loader → Data Guard Verify. |
| `atlas_streaming_supervisor` | `*/10 * * * *` | Restarts `kafka_streaming.py` in the processor container if it stops. |
| `atlas_kafka_health` | Manual | Performs diagnostics and connectivity checks against all cluster Kafka brokers. |

---

## Batch Pipeline Workflow

**DAG**: `atlas_batch_pipeline`  
**File**: `dags/dag_master_pipeline.py`

```text
wait_for_raw_parquet
        │  PythonSensor — waits for *.parquet in raw storage volume
        ▼
settle_after_archive
        │  3-minute sleep period to ensure file writes finish cleanly
        ▼
run_spark_batch
        │  Spark processing: reads raw parquet, outputs process parquet
        ▼
trigger_lakehouse_deduplication
        │  Runs Delta Lake MERGE and Z-Ordering in atlas-lakehouse
        ▼
check_refined_parquet
        │  PythonSensor — waits for refined *.parquet under /refined
        ▼
run_clickhouse_load
        │  Delta Loader parses refined data into ClickHouse
        ▼
verify_clickhouse_data
        │  Data Guard: verifies record count and averages via local docker exec
        ▼
log_pipeline_status
```

### Design Decisions
* **Docker Exec Pattern**: Tasks shell directly into operational containers via Unix socket, preventing network timeout issues on long-running compute jobs.
* **Refined Parquet Sensor**: The ClickHouse loader only fires if refined parquet data is actually present, preventing unnecessary empty writes.
* **ClickHouse Local Exec**: The Data Guard runs queries using `clickhouse-client` inside the analytics container to bypass loopback binding restrictions on port `8123`.

---

## Docker Exec Helper

**File**: `dags/atlas_utils.py`

Airflow calls the Docker Unix socket via `curl` subprocess (avoids breaking the Airflow image with the `docker` Python SDK):

| Function | Use |
| :--- | :--- |
| `_docker_exec()` | Run command, poll until exit |
| `docker_exec_or_raise()` | Same, raises on non-zero exit |
| `docker_exec_fire_and_forget()` | Start long-running jobs |
| `container_is_running()` | Container state check |
| `container_top_contains()` | Process pattern check |
| `wait_for_container_process()` | Poll until process up/down |

---

## Module Structure

```text
orchestration/
├── README.md                    # This document
├── dags/
│   ├── atlas_utils.py           # Docker socket exec helper functions
│   ├── atlas_pipeline_ops.py    # Shared task callables
│   ├── dag_master_pipeline.py   # atlas_batch_pipeline DAG
│   ├── dag_streaming_supervisor.py # atlas_streaming_supervisor DAG
│   └── dag_kafka_health.py      # atlas_kafka_health DAG
└── deprecated_dags/             # Legacy workflow designs
```

---

## Execution and Setup Guide

### 1. First-time Airflow Setup
To initialize the database schema and create a default admin user, run:
```powershell
docker compose run --rm airflow-scheduler airflow db init

docker compose run --rm airflow-scheduler airflow users create `
  --username admin --password admin --firstname Admin --lastname User `
  --role Admin --email admin@atlas.local
```

### 2. Verify DAG Imports
Ensure there are no parsing issues and all 3 DAGs load successfully:
```powershell
docker exec airflow-scheduler airflow dags list-import-errors
docker exec airflow-scheduler airflow dags list | findstr atlas_
```
*Expected: Empty list-import-errors, and the 3 core DAGs listed.*

### 3. Trigger DAGs via CLI
You can trigger workflows directly from the host terminal:
```powershell
docker exec airflow-scheduler airflow dags trigger atlas_batch_pipeline
docker exec airflow-scheduler airflow dags trigger atlas_kafka_health
docker exec airflow-scheduler airflow dags trigger atlas_streaming_supervisor
```
Alternatively, manage the workflow states dynamically by logging into the Airflow UI at `http://localhost:8081` (credentials: `admin`/`admin`).
