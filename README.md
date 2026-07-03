# ATLAS: Advanced Telemetry Logging & Analytics System

## Project Overview

ATLAS (Advanced Telemetry Logging & Analytics System) is a highly scalable, distributed data pipeline and AIOps platform. Engineered to ingest, process, deduplicate, and analyze high-frequency server power telemetry data in near real-time, the system leverages a **Lambda Architecture** paired with an advanced **Machine Learning Intelligence Layer**.

ATLAS transforms chaotic, deeply nested telemetry into mathematically scored, analytics-ready datasets. Finally, an integrated **Small Language Model (SLM)** automates Tier-3 Site Reliability Engineering (SRE) tasks by generating deterministic Root Cause Analysis (RCA) reports directly on the dashboard.

---

##  System Architecture

The pipeline processes telemetry through a resilient, multi-tier architecture ensuring zero data loss, exact-once processing, and sub-second analytics latency.

<img width="1859" height="1021" alt="Project - 0" src="https://github.com/user-attachments/assets/35b46749-17dd-454a-ad72-8168ada948e8" />

---

##  Core Subsystems & Pipeline Flow

### 1. Unified Ingestion & Generation Layer

**Owner:** Jnana Prasad G R
The critical "front door" designed to absorb massive traffic bursts and normalize chaotic incoming metrics.

* **Hardware Registry:** Utilizes a static registry (`device_configs.json`) to assign realistic hardware profiles (Intel Xeon, AMD EPYC, DDR4/DDR5) to nodes, bypassing continuous API parsing.
* **TimescaleDB Hot Path:** Rapidly ingests raw, 5-minute interval power and thermal metrics (`cpu_watts`, `amb_temp`, `cpu_util`) into TimescaleDB, acting as a shock absorber.
* **Dynamic Hydration:** Fetches raw metrics from TimescaleDB, merges them with the in-memory hardware registry, and streams fully hydrated Golden Records into Kafka.
* **Historical Physics Engine:** Synthetic generation of 30 days of causal server data with specific hardware failures (e.g., `gpu_overload`, `thermal_failure`) for ML training.

### 2. Streaming & Message Broker Layer

**Owner:** Nandini
A highly durable data transportation layer ensuring no telemetry is dropped during network partitions or node failures.

* **Kafka KRaft Cluster:** Operates a Zookeeper-less Kafka cluster with 3 brokers and a replication factor of 3 for strict fault tolerance.
* **Optimized Partitioning:** The primary `raw-server-metrics` topic is distributed across 12 partitions to maximize parallel processing throughput.
* **Dead Letter Queue (DLQ) Routing:** Invalid or malformed JSON payloads are immediately routed to a dedicated `raw-server-metrics-dlq` topic to prevent pipeline blocking.

### 3. Data Processing & Validation Engine (Apache Spark)

**Owner:** Sanjula S

- The heavy computational muscle of the platform, utilizing **Spark Structured Streaming**. 
- Processes both real-time **streaming** and **historical batch** telemetry data using Apache Spark.
- Consumes telemetry from Apache Kafka through Spark Structured Streaming.
- Performs **schema validation**, **transformation**, **flattening** (explode), and 1-hour **window-based aggregations**.
- Uses **watermarking** to handle late-arriving events and **checkpointing** for fault-tolerant recovery.
- Routes invalid records to a **Dead Letter Queue** (DLQ) with automated retry and failure classification.
- Generates Snappy-compressed **Parquet** datasets in a shared volume for seamless Delta Lake integration.
- Supports scalable, low-latency processing through micro-batch execution.

### 4. Refined Storage & Deduplication Layer (Delta Lake)

**Owner:** Manthan R M
Acts as the immutable Source of Truth and the gatekeeper for the analytical databases.

* **Format:** Strictly utilizes Snappy-compressed **Parquet**, allowing analytics engines to leverage columnar data skipping.
* **Deep Partitioning:** Implements an optimized 5-level directory structure: `/refined/metric_name/date/pcid/acid/device_id/` to manage 80,000+ devices seamlessly.
* **ACID Deduplication:** Employs advanced `MERGE` (Upsert) operations mapped to a composite key (`device_id` + `metric_time` + `application_customer_id`). This mathematically strips massive 7-day rolling overlaps without data corruption.
* **Optimization:** Periodically compacts small streaming files into 128MB Parquet blocks to neutralize the "small file problem."

### 5. Analytics & Machine Learning (AIOps)

**Owners:** Varna (Databases) & Sanjula (ML Training)
Two analytical databases consume the refined data, feeding into the Isolation Forest anomaly detection engine.

* **PostgreSQL:** Stores persistent relational metadata and maintains the backend state for the Copilot chat history.
* **ClickHouse (Varna):** Serves as the ultra-fast columnar backend. Utilizes native Kafka Engines for live streaming alerts and persistent schedulers to pull deduplicated Parquet batches.
* **Isolation Forest Pipeline (Sanjula):** Trains an Isolation Forest model on historical telemetry for anomaly detection.
Performs feature engineering and preprocessing to generate consistent model inputs.
Computes anomaly scores and normalized health scores (0–100) for every device.
Serializes trained artifacts (preprocessor.pkl, isolation_forest.pkl, health_score_config.pkl) for production inference.
Supplies health predictions to the analytics layer, enabling real-time monitoring and AI-assisted Root Cause Analysis (RCA).

### 6. ATLAS Dashboard & SRE Copilot

**Owner:** Manthan R M
The user-facing control center integrating observability and Generative AI.

* **Streamlit Global Dashboard:** Features real-time ClickHouse explorers, live time-series visualizers, and Delta Lake streaming metrics.
* **SLM Context Truncation:** Converts ClickHouse ML predictions into a lean JSON payload, stripping heavy categorical UUIDs (customer IDs) to optimize the LLM token context window.
* **Phi-4-Mini RCA Engine:** Feeds the chronological telemetry history of failing devices to a localized Phi-4-Mini LLM (via Ollama). The SLM outputs a deterministic, JSON-formatted Root Cause Analysis containing incident summaries, affected subsystems, and actionable bash remediation commands.

---

##  Data Dictionaries & Schemas

### Golden Record (Post-Ingestion)

The unified 48-field schema standardized before entering the data lake.

* **Identifiers:** `report_id`, `device_id`, `server_name`, `application_customer_id`, `platform_customer_id`, `tags`
* **Geography:** `location_name`, `location_city`, `location_state`, `location_country`
* **Hardware Profiles:** `processor_vendor`, `server_generation`, `cpu_inventory`, `memory_inventory`, `socket_count`
* **Raw Telemetry:** `avg_metric_value`, `cpu_utilization`, `memory_utilization`, `cpu_temperature`, `amb_temp`, `fan_speed_rpm`

### SLM Payload Schema (Post-Inference)

Optimized specifically for the SRE Copilot to maximize context window efficiency.

```text
device_id, server_name, tags, location_name
metric_time, avg_metric_value, cpu_utilization, memory_utilization, disk_utilization
network_throughput, cpu_temperature, amb_temp, fan_speed_rpm, gpu_utilization
uptime_hours, processor_vendor, server_generation, memory_capacity_gb
prediction, anomaly_score, health_score

```

---

##  Technology Stack

* **Streaming & Transport:** Apache Kafka (KRaft), FastAPI, Uvicorn
* **Data Processing:** Apache Spark (PySpark), Spark Structured Streaming
* **Lakehouse Storage:** Delta Lake, Apache Parquet (Snappy Compression)
* **Analytical Databases:** ClickHouse, PostgreSQL, TimescaleDB
* **Machine Learning:** Scikit-Learn (Isolation Forest), Pandas, NumPy
* **Generative AI:** Ollama, Phi-4-mini, requests (REST API)
* **Visualization:** Streamlit, Plotly Express
* **Infrastructure:** Docker, Docker Compose, Nginx

---

##  Execution & Quickstart Guide

### 1. Boot the Stack

Ensure Docker Desktop is running, then deploy the fully containerized environment:

```bash
docker compose up -d --build

```

*This spins up Kafka, Spark, ClickHouse, Postgres, the ML Engine, and the Streamlit UI.*

### 2. Generate Initial Telemetry (Data Simulation)

Execute the ingestion scripts to pre-fill the raw directories and TSDB with synthetic fleet data:

```bash
docker exec -it atlas-ingestion python3 /app/v2/scripts/generate_registry.py
docker exec -it atlas-ingestion python3 /app/v2/scripts/prefill_tsdb.py --days=7

```

### 3. Model Training (One-Time Setup)

Generate historical labeled data and train the Isolation Forest model:

```bash
docker exec -it atlas-ml python data_generator.py --days 30
docker exec -it atlas-ml python train_model.py

```

*This generates `isolation_forest.pkl` and configuration artifacts in the `/models/` volume.*

### 4. Access the Platform

Navigate to **http://localhost:8501** in your web browser.
The system will now continuously poll the live Parquet batches, run ML predictions, load them into ClickHouse, and make them available for SRE Copilot RCA.
