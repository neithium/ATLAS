# ATLAS Spark Processing Engine

Apache Spark-based batch and streaming processing engine for the ATLAS platform.

### Author: Sanjula S

---

## Overview

The Spark Processing Engine is responsible for processing telemetry data received from Apache Kafka and transforming it into analytics-ready Parquet files. It supports both **real-time stream processing** using Spark Structured Streaming and **historical batch processing**.

The pipeline is designed to provide:

- Real-time ingestion of server telemetry
- Data validation using predefined schemas
- Automatic error detection and classification
- Dead Letter Queue (DLQ) support for invalid records
- Recovery of recoverable records
- Batch processing of archived telemetry
- Optimized Parquet generation for downstream analytics

---

# Architecture

```
Kafka Topic
raw-server-metrics
        │
        ▼
────────────────────────────────────────────
Spark Structured Streaming
• Schema Validation
• Error Classification
• Data Processing
────────────────────────────────────────────
        │                     │
        │                     │
   Valid Records         Invalid Records
        │                     │
        ▼                     ▼
 Process Pipeline         DLQ Topic
                           raw-server-metrics-dlq
        │                     │
        ▼                     ▼
   Parquet Output        DLQ Reviewer
                               │
                  ┌────────────┴────────────┐
                  │                         │
          Recoverable Errors      Non-Recoverable Errors
                  │                         │
                  ▼                         ▼
      raw-server-metrics-retry      raw-server-metrics-failure
                  │
                  ▼
          Replay to Main Pipeline
```

---

# Components

## 1. Kafka Producer

Telemetry JSON records are published to the Kafka topic:

```
raw-server-metrics
```

This serves as the entry point of the streaming pipeline.

---

## 2. Spark Structured Streaming

The streaming application continuously consumes data from:

- raw-server-metrics
- raw-server-metrics-retry

Each incoming record undergoes:

- JSON parsing
- Schema validation
- Error classification
- Data transformation
- Aggregation
- Parquet generation

Valid records continue through the processing pipeline while invalid records are redirected to the Dead Letter Queue (DLQ).

---

## 3. Error Classification

Every incoming message is categorized into one of the following types:

### Valid

The record satisfies all schema and business validation rules.

---

### Recoverable

The record contains minor data issues that can be corrected automatically.

Example fixes include:

- String to Integer conversion
- Timestamp normalization

Recoverable records are replayed back into the streaming pipeline after successful repair.

---

### Non-Recoverable

Records missing mandatory information cannot be repaired automatically.

Example:

```json
{
  "device_id":null,
   ...,
}
```

These records are moved permanently to the failure topic.

---

# Dead Letter Queue (DLQ)

Invalid records are written to

```
raw-server-metrics-dlq
```

Each DLQ message contains:

- Original JSON
- Error type
- Kafka topic
- Partition
- Offset
- Timestamp
- Worker ID

This information allows failed records to be reviewed and replayed if possible. 

---

# DLQ Reviewer

The DLQ Reviewer continuously consumes messages from the DLQ.

Its responsibilities include:

- Reading failed records
- Identifying recoverable errors
- Applying recovery rules
- Republishing repaired records to

```
raw-server-metrics-retry
```

- Sending permanently failed records to

```
raw-server-metrics-failure
```

Supported automatic fixes include:

- socket_count datatype conversion
- Timestamp normalization

---

# Streaming Output

Valid records are:

- Exploded from nested PowerDetail arrays
- Aggregated per device
- Converted into analytics-friendly format
- Written as compressed Parquet files

Worker-specific output directories are used to support parallel processing. 

---

# Batch Processing

The batch engine processes archived telemetry stored in Parquet format.

Operations performed include:

- Reading archived Parquet files
- Exploding nested PowerDetail arrays
- Computing aggregated metrics
- Generating analytics-ready datasets
- Writing processed Parquet output

This mode is useful for historical analytics and backfilling data.

---
# Execution Guide

## 1. Start the Required Services

Ensure all required Docker containers (Kafka, Spark Processor, dlq-reviewer, ingestion) are running.

---

## 2. Generate Telemetry Data

To test the streaming pipeline, invoke the telemetry export API.

**Request**

```http
POST http://localhost:8001/pcid/PLATCUSTxxxx/acid/APPCUSTxxxx/telemetry/latest/export
```

This API generates the latest telemetry data and publishes it to the Kafka topic:

```
raw-server-metrics
```

---

## 3. (Optional) Publish Test Records Manually

For testing specific validation scenarios, you can manually publish JSON records to Kafka.

Enter the Kafka broker:

```bash
docker exec -it broker1 bash
```

Start the Kafka producer:

```bash
/opt/bitnami/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server broker1:9092 \
  --topic raw-server-metrics
```

Paste any of the sample JSON records (Valid, Recoverable, or Non-Recoverable).

---

## 4. Enter the Spark Processor Container

```bash
docker exec -it atlas-processor bash
```

---

## 5. Start Streaming Worker 1

```bash
WORKER_ID=1 spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
/app/jobs/kafka_streaming.py > worker1.log 2>&1 &
```

Monitor logs:

```bash
tail -f worker1.log
```

---

## 6. Start Streaming Worker 2

```bash
WORKER_ID=2 spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
/app/jobs/kafka_streaming.py > worker2.log 2>&1 &
```

Monitor logs:

```bash
tail -f worker2.log
```

---

## 7. Verify Streaming Output

The Spark workers consume telemetry from:

```
raw-server-metrics
```

- Valid records are processed and written as Parquet files.
- Invalid records are routed to `raw-server-metrics-dlq`.
- Recoverable records are repaired and replayed through `raw-server-metrics-retry`.
- Non-recoverable records are published to `raw-server-metrics-failure`.

---

## 8. Archive Historical Data

```bash
python3 ./v2/scripts/manual_archive.py 2025-05-16 2025-05-17
```

---

## 9. Execute the Batch Pipeline

Inside the Spark container:

```bash
spark-submit ./jobs/batch_job.py
```

The batch pipeline processes archived Parquet data and generates aggregated analytics-ready Parquet output.

## 10. Test Invalid Data Handling (DLQ Pipeline)

To verify the error handling and recovery mechanism, manually publish test records to Kafka.

### Enter the Kafka Broker

```bash
docker exec -it broker1 bash
```

### Start the Kafka Producer

```bash
/opt/bitnami/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server broker1:9092 \
  --topic raw-server-metrics
```

### Test Case 1 – Valid Record

Paste the following JSON:

```json
{"device_id":"DEV-VALID","report_id":"REP-000","created_at":"2026-05-26T10:20:00","inventory_data":{"socket_count":4},"data":{"PowerDetail":[{"Average":95.0,"Minimum":80.0,"Peak":120.0,"Time":"2026-05-26T10:20:00"}]}}
```

**Expected Result**

- Successfully processed by Spark.
- Written to the processed Parquet output.
- No DLQ entry is created.

---

### Test Case 2 – Recoverable Record

Paste:

```json
{"device_id":"DEV-001","report_id":"REP-001","created_at":"2026-05-26T10:20:00","inventory_data":{"socket_count":"4"},"data":{"PowerDetail":[{"Average":91.2,"Minimum":80.1,"Peak":120.0,"Time":"2026-05-26T10:20:00"}]}}
```

**Expected Result**

- Routed to `raw-server-metrics-dlq`.
- DLQ Reviewer converts `socket_count` from String to Integer.
- Repaired record is published to `raw-server-metrics-retry`.
- Spark consumes the retried message and processes it successfully.

---

### Test Case 3 – Non-Recoverable Record

Paste:

```json
{"device_id":null,"report_id":"REP-003","created_at":"2026-05-26T10:20:00","inventory_data":{"socket_count":4},"data":{"PowerDetail":[{"Average":90.0,"Minimum":75.0,"Peak":110.0,"Time":"2026-05-26T10:20:00"}]}}
```

**Expected Result**

- Routed to `raw-server-metrics-dlq`.
- Spark UI can be accessed for visual analytics 
- DLQ Reviewer identifies it as non-recoverable.
- Record is published to `raw-server-metrics-failure`.
- It is not replayed into the streaming pipeline.
---

# Kafka Topics

| Topic | Purpose |
|--------|----------|
| raw-server-metrics | Incoming telemetry |
| raw-server-metrics-dlq | Invalid records |
| raw-server-metrics-retry | Successfully repaired records |
| raw-server-metrics-failure | Permanently failed records |

---

# Output

The Spark Processing Engine produces:

- Real-time processed Parquet datasets
- Batch aggregated Parquet datasets
- DLQ records for failed messages
- Retry records after automatic recovery
- Failure records for manual inspection

This architecture provides a scalable, fault-tolerant, and reliable processing pipeline capable of handling both streaming and historical telemetry workloads.