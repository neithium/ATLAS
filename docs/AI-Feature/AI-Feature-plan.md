 
# ATLAS Observability: AI Integration Plan (Anomaly Detection)

## 1. Executive Summary
This document outlines our strategy for integrating the Isolation Forest anomaly detection model into the core ATLAS telemetry pipeline. Our primary goal is to transition the AI feature from isolated local development into our shared Docker Compose environment. This approach ensures we can score server metrics in real time without disrupting the existing ingestion or visualization layers. By automating the detection of statistical outliers, we aim to reduce alert fatigue and transition from reactive troubleshooting to proactive system monitoring.

## 2. Target Architecture and Data Flow
The AI module will act as a decoupled consumer and producer within our event driven architecture. This microservice design guarantees zero bottlenecking on the main data path, allowing the ingestion API to remain highly available.

* **Ingestion:** Telemetry arrives via FastAPI and is published to the `raw_server_metrics` Kafka topic in JSON format.
* **Inference:** The containerized AI Model subscribes to the raw topic, deserializes the payload, applies the Isolation Forest algorithm, and flags statistical outliers based on pre-defined contamination thresholds.
* **Publishing:** The model constructs a new JSON payload containing the original metrics appended with an `anomaly_score` (float) and an `is_anomaly` (boolean) flag. This is published to a new Kafka topic called `enriched_server_metrics`.
* **Downstream:** The PySpark processor consumes the enriched topic, applies schema validation, writes to the Delta Lake storage layer, and syncs to ClickHouse for the Streamlit dashboard to render alerts.

## 3. Phased Execution Strategy

### Phase 1: Model Finalization and Dockerization (Active Focus)
**Focus:** Complete the algorithmic core, lock in hyperparameters, and ensure reproducible local execution.
* Finalize feature engineering for server telemetry, specifically isolating CPU utilization, memory spikes, and disk IO patterns.
* Train and serialize the baseline Isolation Forest model using a representative historical dataset. Define the base `contamination` rate to minimize false positives.
* Wrap the inference script in a lightweight Python application (e.g., using a basic consumer loop).
* **Deliverable:** A functional Dockerfile containing the Python environment, serialized model weights, and inference logic that can read from and write to local mock files or a local Kafka instance.

### Phase 2: Pipeline Integration (Local Docker Compose)
**Focus:** Connect the AI container to the broader data streams and ensure network stability.
* Integrate the AI Docker container into the team's shared `docker-compose.yml` file, ensuring it shares the same virtual network as Kafka and Zookeeper/KRaft.
* Establish robust Kafka consumer and producer logic within the AI script, including proper connection retry mechanisms and offset management.
* Validate the complete end to end flow: Generate mock data > FastAPI > Kafka > AI Model > Kafka > ClickHouse.
* **Deliverable:** A fully integrated Docker Compose environment where anomalies are successfully tagged, routed, and persisted in the database without dropping messages.

### Phase 3: Dashboard Visualization 
**Focus:** Expose the AI insights to the end user through clear visual indicators.
* Update the ClickHouse table schemas to formally accept the new `anomaly_score` and `is_anomaly` columns.
* Modify the Streamlit frontend to dynamically highlight data points flagged by the Isolation Forest.
* Implement a dedicated "Alerts Log" or data table in the UI that filters and displays only the anomalous events for rapid auditing.
* **Deliverable:** Real time visual confirmation of model accuracy on the frontend dashboard, complete with time series charts reflecting the anomaly overlays.

## 4. Risk Mitigation
* **Latency Spikes:** If the model inference slows down the data pipeline, we will adjust the Kafka consumer group settings to batch process metrics in micro-batches rather than scoring them individually.
* **Cold Start Penalties:** The model initialization, including loading the serialized weights into RAM, must occur during the container's startup routine rather than during the first inference request. This prevents initial connection timeouts.
* **Data Format Mismatches:** We will implement strict JSON schema validation at the input and output stages of the AI container to prevent malformed data from crashing the downstream PySpark processor.
* **Scope Lock:** The immediate priority is locking down Phase 1 and 2 for the upcoming project deadline. We will rely entirely on our stable Docker Compose architecture to ensure we hit our targets without introducing unnecessary infrastructure overhead or deployment complexity.

 