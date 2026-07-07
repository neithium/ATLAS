# ATLAS Event Streaming, Orchestration, and ML Inference Platform

This module forms the backbone of the ATLAS real-time event streaming, batch and stream orchestration, and machine learning inference pipeline. It ensures high availability, fault tolerance, data quality validation via a Dead Letter Queue (DLQ), and automated device health monitoring.

---

## System Architecture Overview

The platform is divided into three interconnected layers:
1. **Event Streaming Layer (Kafka KRaft)**: Manages ingestion buffering, distributed parallel storage, and fault-tolerant message queues.
2. **Orchestration Layer (Apache Airflow)**: Coordinates batch ingestion, spark streaming jobs, DLQ monitoring, and cluster health checks.
3. **ML Inference Pipeline (Isolation Forest)**: Performs real-time anomaly detection, computes device health scores, and routes outputs to ClickHouse.

---

## 1. Event Streaming Layer (Apache Kafka KRaft Cluster)

The event streaming layer acts as a scalable, high-throughput buffer between the Ingestion APIs and downstream Apache Spark processors.

### Key Features
* **Telemetry Event Ingestion**: Telemetry events are serialized and published via `kafka_producer.py` directly to the primary ingest topic (`raw-server-metrics`).
* **Distributed Parallel Storage**: The primary topic `raw-server-metrics` is configured with **12 partitions** to distribute write loads evenly and enable concurrent high-throughput processing.
* **High Availability & Quorum Durability**: A 3-broker ZooKeeper-less KRaft cluster is deployed with a Replication Factor of 3 (**RF=3**) and Minimum In-Sync Replicas of 2 (**MIN_ISR=2**), guaranteeing message durability and cluster availability during broker failure scenarios.
* **Automatic Quorum Recovery**: Handles broker failure detection, automatic leader election, and partition rebalancing to restore cluster stability without manual intervention.
* **Isolating & Reprocessing DLQ Pipeline**: Invalid schema or corrupted records are routed to `raw-server-metrics-dlq` (3 partitions) for repair by the DLQ Reviewer. Recoverable records are republished to `raw-server-metrics-retry` (3 partitions) for reprocessing, while unrecoverable records are permanently isolated in `raw-server-metrics-failure` (3 partitions).

### Kafka Architecture Diagram

```mermaid
graph TD
    %% Nodes definition
    Producer["kafka_producer.py<br>(Ingestion Container)"]
    
    subgraph Cluster ["KAFKA CLUSTER (KRAFT MODE)"]
        subgraph Brokers ["BROKERS"]
            B1["Broker 1"]
            B2["Broker 2"]
            B3["Broker 3"]
        end
        
        subgraph Topics ["TOPICS"]
            T_Raw["raw-server-metrics<br>partitions = 12<br>RF = 3"]
            T_DLQ["raw-server-metrics-dlq<br>partitions = 3<br>RF = 3"]
            T_Retry["raw-server-metrics-retry<br>partitions = 3<br>RF = 3"]
            T_Fail["raw-server-metrics-failure<br>partitions = 3<br>RF = 3"]
        end
    end
    
    Consumer["Spark Structured Streaming<br>(Kafka Consumer)"]
    DLQ_Reviewer(["DLQ reviewer"])
    
    %% Connections
    Producer -->|Publish Telemetry Events| T_Raw
    T_Raw -->|Consume Stream| Consumer
    Consumer -->|Invalid schema| T_DLQ
    T_DLQ --> DLQ_Reviewer
    DLQ_Reviewer -->|Recoverable ?| T_Retry
    DLQ_Reviewer -->|Non-Recoverable ?| T_Fail
    T_Retry -->|Re-consume| Consumer
    
    %% Style
    style Cluster fill:#f5f5f5,stroke:#333,stroke-width:2px;
    style Brokers fill:#ffffff,stroke:#333,stroke-width:1px;
    style Topics fill:#ffffff,stroke:#333,stroke-width:1px;
    style DLQ_Reviewer fill:#e8daef,stroke:#8e44ad,stroke-width:2px;
    style Consumer fill:#d6eaf8,stroke:#2980b9,stroke-width:2px;
    style Producer fill:#fef9e7,stroke:#f1c40f,stroke-width:2px;
    
    classDef brokerClass fill:#ffffff,stroke:#333,stroke-width:1.5px,rx:5px,ry:5px;
    classDef topicClass fill:#ffffff,stroke:#333,stroke-width:1.5px,rx:10px,ry:10px;
    class B1,B2,B3 brokerClass;
    class T_Raw,T_DLQ,T_Retry,T_Fail topicClass;
```

---

## 2. Orchestration Layer (Apache Airflow Scheduler)

Apache Airflow orchestrates workflows across the entire platform, scheduling batch jobs, tracking dependencies, and running operational monitoring.

### Key Features
* **Core Batch Ingestion**: Schedules and triggers the Spark Batch and Delta merge processes of the Lambda Architecture.
* **Stream Monitoring**: Keeps active Spark Structured Streaming jobs alive and automatically restarts failed streaming instances.
* **Kafka Diagnostics**: Performs automated broker diagnostics and connection checks via the health check pipeline.
* **Task Retries & Failover**: Automatically schedules retries and captures pipeline state on failure, raising alerts when errors cascade.

### Airflow DAGs Overview Diagram

```mermaid
graph LR
    subgraph DAGS [DAGS]
        direction TB
        dag1["atlas_kafka_health"]
        dag2["atlas_stream_pipeline"]
        dag3["atlas_streaming_supervisor"]
        dag4["atlas_dlq_monitor"]
    end
    
    Scheduler["AIRFLOW SCHEDULER"] --> dag1
    Scheduler --> dag2
    Scheduler --> dag3
    Scheduler --> dag4

    classDef sched fill:#ffffff,stroke:#000000,stroke-width:2px;
    classDef health fill:#fef5e7,stroke:#f39c12,stroke-width:2px,rx:10px,ry:10px;
    classDef stream fill:#ebf5fb,stroke:#2980b9,stroke-width:2px,rx:10px,ry:10px;
    classDef supervisor fill:#fdedec,stroke:#e74c3c,stroke-width:2px,rx:10px,ry:10px;
    classDef dlq fill:#eaf2f8,stroke:#27ae60,stroke-width:2px,rx:10px,ry:10px;
    
    class Scheduler sched;
    class dag1 health;
    class dag2 stream;
    class dag3 supervisor;
    class dag4 dlq;
```

---

## 3. Machine Learning Inference Pipeline

The ML Inference Pipeline runs anomaly detection models against telemetry data to determine real-time device health metrics.

### Key Features
* **Isolation Forest Model**: Loads the pre-trained `isolation_forest.pkl` model to predict whether incoming metrics indicate normal operation or an anomaly.
* **Feature Engineering**: Auto-derives temporal features (`hour_of_day`, `day_of_week`) from `metric_time` to align with the training schema.
* **Composite Health Scoring**: Computes a health score (0–100) based on weighted parameters, categorizing server status into Healthy, Warning, Degraded, or Critical.
* **ClickHouse Integration**: Stores the enriched records containing predictions, scores, and status flags into ClickHouse.

### ML Inference Pipeline Diagram

```mermaid
graph LR
    ModelFile["Isolation_forest_model.pkl"]
    DataFile["CSV/Parquet files"]
    
    Pandas((Pandas))
    FeatEng{Feature engineering}
    
    FeatVector["Feature vector:<br>• MetricValue<br>• avg_metric_value<br>• max_metric_value<br>• min_metric_value<br>• hour_of_day (Derived)<br>• day_of_week (Derived)"]
    
    ModelPredict{Isolation_forest_model.pkl}
    Outputs["Outputs:<br>• is_anomaly<br>• anomaly_score"]
    
    HealthCalc((Health score calculation))
    
    Appended["Appended columns:<br>• MetricValue<br>• avg_metric_value<br>• max_metric_value<br>• min_metric_value<br>• hour_of_day<br>• day_of_week<br>• is_anomaly (New)<br>• anomaly_score (New)<br>• health_score (New)"]
    
    Clickhouse["Clickhouse"]
    
    %% Flows
    ModelFile -->|Load model into memory| Pandas
    DataFile --> Pandas
    Pandas --> FeatEng
    FeatEng --> FeatVector
    FeatVector -->|send data to the model| ModelPredict
    ModelPredict --> Outputs
    Outputs --> HealthCalc
    FeatVector --> HealthCalc
    HealthCalc --> Appended
    Appended --> Clickhouse
```

---

## Operational Execution Demos

### 1. Kafka Operations Demo
To run the Kafka cluster and verify event streaming, keep **3 host terminals** open.

#### Cluster Setup and Ingestion
1. Start the KRaft cluster:
   ```powershell
   .\cluster.bat
   ```
2. Verify all brokers are running:
   ```powershell
   docker ps --format "table {{.Names}}\t{{.Status}}"
   ```
3. List active topics:
   ```powershell
   docker exec broker1 kafka-topics.sh --bootstrap-server localhost:9092 --list
   ```
4. Verify topic configurations and partitions:
   ```powershell
   docker exec broker1 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw-server-metrics
   docker exec broker1 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw-server-metrics-dlq
   docker exec broker1 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw-server-metrics-retry
   docker exec broker1 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw-server-metrics-failure
   ```
5. Check Ingestion API health check:
   ```powershell
   Invoke-RestMethod -Uri "http://localhost:8001/health"
   ```
6. Trigger a telemetry data export to Kafka:
   ```powershell
   Invoke-RestMethod -Uri "http://localhost:8001/fleet/telemetry/export" -Method POST
   ```
7. Verify messages are landing in Kafka from the Ingestion API:
   ```powershell
   docker exec atlas-ingestion python3 /app/v2/scripts/check_kafka_msg.py
   ```
8. Stream continuous telemetry data generation:
   ```powershell
   .\kafka\scripts\stream_data.bat
   ```

#### DLQ Testing and Verification
* **Terminal 1 (Produce Invalid Message)**:
  Run console producer to write directly to raw metrics:
  ```powershell
  docker exec -it broker1 kafka-console-producer.sh --bootstrap-server localhost:9092 --topic raw-server-metrics
  ```
  Produce a **valid** schema message:
  ```json
  {"device_id":"DEV-VALID","report_id":"REP-000","created_at":"2026-05-26T10:20:00","inventory_data":{"socket_count":4},"data":{"PowerDetail":[{"Average":95.0,"Minimum":80.0,"Peak":120.0,"Time":"2026-05-26T10:20:00"}]}}
  ```
  Produce an **invalid** schema message (string socket_count instead of integer) to trigger the DLQ flow:
  ```json
  {"device_id":"DEV-001","report_id":"REP-001","created_at":"2026-05-26T10:20:00","inventory_data":{"socket_count":"4"},"data":{"PowerDetail":[{"Average":91.2,"Minimum":80.1,"Peak":120.0,"Time":"2026-05-26T10:20:00"}]}}
  ```
* **Terminal 2 (Monitor Spark Worker)**:
  ```powershell
  docker exec atlas-processor tail -f /app/logs/worker1.log
  ```
* **Terminal 3 (Monitor DLQ Reviewer)**:
  ```powershell
  docker exec atlas-processor tail -f /app/logs/dlq.log
  ```

#### Fault Tolerance Test
* **Terminal 1**: Trigger a broker failover:
  ```powershell
  .\kafka\scripts\failover_test.bat
  ```
  Describe the topic to check partition leaders reassignment:
  ```powershell
  docker exec broker2 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw-server-metrics
  ```
* **Terminal 2**: Start the broker watchdog to monitor and recover down nodes:
  ```powershell
  .\kafka\scripts\watchdog.bat
  ```
* **Terminal 1**: Confirm that the failed broker is back online and re-joined the ISR:
  ```powershell
  docker exec broker1 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw-server-metrics
  ```

---

### 2. Airflow Orchestration Demo
1. Verify Airflow has registered the local database connections:
   ```powershell
   docker exec airflow-scheduler airflow connections list | findstr atlas_
   ```
   *Expected: `atlas_ingestion_api` is listed.*
2. Check for DAG compilation/import errors:
   ```powershell
   docker exec airflow-scheduler airflow dags list-import-errors
   ```
   *Expected: Empty output (zero errors).*
3. Verify all operational DAGs are parsed and listed:
   ```powershell
   docker exec airflow-scheduler airflow dags list | findstr atlas_
   ```
4. Access the web interface at `http://localhost:8081`.
5. Trigger the DAGs in the following operational order:
   1. `atlas_kafka_health`
   2. `atlas_stream_pipeline`
   3. `atlas_streaming_supervisor`
   4. `atlas_dlq_monitor`

---

### 3. ML Inference Pipeline Demo
1. Ensure the ML inference environment is started:
   ```powershell
   docker start ml-inference
   ```
2. Generate fresh live telemetry data containing random anomalies:
   ```powershell
   docker exec atlas-ml python live_data_gen.py --anomalies --anomaly-rate 0.05
   ```
3. Run the inference pipeline (this triggers the one-shot container execution):
   ```powershell
   docker start ml-inference
   ```
4. Watch container logs in real-time:
   ```powershell
   docker logs -f ml-inference
   ```
5. Locate the newly generated prediction Parquet files on the host:
   ```powershell
   Get-ChildItem ML-Model\telemetry-data\predictions\ml_predictions_*.parquet | Sort-Object LastWriteTime -Descending | Select-Object -First 1
   ```
6. Inspect the prediction outputs directly:
   ```powershell
   docker run --rm -v ".\ML-Model\telemetry-data\predictions:/data/ml_predictions" atlas-ml-inference python -c "
   import pandas as pd, glob
   f = sorted(glob.glob('/data/ml_predictions/ml_predictions_*.parquet'))[-1]
   df = pd.read_parquet(f)
   print('File:', f)
   print('Rows:', len(df))
   print(df[['device_id','avg_metric_value','prediction','anomaly_score','health_score','health_status']].head(8).to_string())
   print('--- Status counts ---')
   print(df['health_status'].value_counts().to_string())
   print('--- Anomalies flagged ---')
   print('prediction=-1:', (df['prediction']==-1).sum())
   "
   ```
