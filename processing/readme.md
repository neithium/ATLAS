# ATLAS Spark Processing Engine

Apache Spark-based batch and streaming processing engine for the ATLAS platform.

**Author:** Sanjula S

---

# Overview

The ATLAS Spark Processing Engine is responsible for consuming high-volume telemetry data from Apache Kafka, validating incoming records, classifying invalid messages, automatically recovering recoverable failures, processing streaming telemetry, executing historical batch jobs, and generating analytics-ready Parquet datasets for downstream analytics.

The processing engine provides:

- Real-time telemetry ingestion
- Spark Structured Streaming processing
- Automatic schema validation
- Dead Letter Queue (DLQ) support
- Automatic recovery of recoverable records
- Retry pipeline
- Permanent failure pipeline
- Historical batch processing
- Analytics-ready Parquet generation
- Fault-tolerant checkpointing
- Parallel Spark worker execution

---

# Architecture

```text
                    Kafka Producer
                          │
                          ▼
                raw-server-metrics
                          │
                          ▼
             Spark Structured Streaming
      ┌────────────────────────────────────┐
      │ JSON Parsing                       │
      │ Schema Validation                  │
      │ Error Classification               │
      │ Watermark Processing               │
      │ Streaming Aggregation              │
      └────────────────────────────────────┘
             │                      │
             │                      │
             ▼                      ▼
      Valid Records         Invalid Records
             │                      │
             ▼                      ▼
     Stream Parquet Output  raw-server-metrics-dlq
   (/worker_1, /worker_2)            │
                                     ▼
                              DLQ Reviewer
                          ┌──────────┴──────────┐
                          │                     │
                          ▼                     ▼
                   Recoverable          Non-Recoverable
                          │                     │
                          ▼                     ▼
             raw-server-metrics-retry   raw-server-metrics-failure
                          │
                          ▼
             Spark Structured Streaming
                          │
                          ▼
                 Stream Parquet Output
```

---

# Processing Pipeline

The Spark Processing Engine consists of four major components:

1. Kafka Streaming Consumer
2. DLQ Processing Engine
3. Streaming Analytics Pipeline
4. Historical Batch Processing

Each component performs a dedicated task while sharing the same telemetry schema and processing rules.

---

# Kafka Topics

| Topic | Purpose |
|--------|----------|
| `raw-server-metrics` | Incoming telemetry |
| `raw-server-metrics-dlq` | Invalid records awaiting review |
| `raw-server-metrics-retry` | Successfully repaired records |
| `raw-server-metrics-failure` | Permanently failed records |

---

# Spark Structured Streaming

The Spark streaming application continuously consumes telemetry from two Kafka topics:

- `raw-server-metrics`
- `raw-server-metrics-retry`

Each incoming record passes through the following stages:

1. Kafka message ingestion
2. JSON parsing
3. Schema validation
4. Error classification
5. Watermark assignment
6. Streaming aggregation
7. Parquet generation

Valid records continue through the analytics pipeline.

Invalid records are automatically redirected to the Dead Letter Queue for further review.

---

# Streaming Workflow

```text
                    Kafka
                      │
                      ▼
                 ReadStream
                      │
                      ▼
                 JSON Parsing
                      │
                      ▼
             Error Classification
                ┌───────────────┐
                │               │
                ▼               ▼
             VALID          INVALID
                │               │
                ▼               ▼
        Streaming        DLQ Topic
        Aggregation          │
                │            ▼
                ▼      DLQ Reviewer
          Parquet Output      │
                              │
                      ┌───────┴────────┐
                      ▼                ▼
                 Retry Topic     Failure Topic
                      │
                      ▼
                 ReadStream
                      │
                      ▼
                 JSON Parsing
                      │
                      ▼
             Streaming Aggregation
                      │
                      ▼
                Parquet Output
```

---

# Schema Validation

Every Kafka message is validated against the Spark telemetry schema before processing begins.

Validation includes:

- Required field validation
- Datatype validation
- Nested `PowerDetail` validation
- Inventory validation
- Timestamp validation

Messages failing validation never enter the analytics pipeline.

---

# Error Classification

Each incoming message is classified into one of the following categories.

## VALID

The record satisfies all schema and business validation rules.

The record immediately enters the Spark aggregation pipeline.

---

## INVALID_SCHEMA

The incoming JSON cannot be parsed correctly using the telemetry schema.

Examples include:

- Missing nested structures
- Incorrect JSON layout
- Invalid datatypes

These records are routed to the DLQ.

---

## INVALID_SOCKET_COUNT

The `socket_count` field cannot be parsed as an integer.

Example

```json
{
  "inventory_data": {
    "socket_count": "4"
  }
}
```

This record is recoverable.

---

## MISSING_DEVICE_ID

Example

```json
{
  "device_id": null
}
```

Since the device identifier is mandatory, this record cannot be repaired automatically.

---

## MISSING_POWERDETAIL

Example

```json
{
  "data": {
    "PowerDetail": null
  }
}
```

Without telemetry measurements, Spark cannot perform analytics.

The record is permanently rejected.

---

# Dead Letter Queue (DLQ)

All invalid records are automatically published to:

```text
raw-server-metrics-dlq
```

Each DLQ message contains:

- Original JSON payload
- Error type
- Source Kafka topic
- Partition
- Offset
- Kafka timestamp
- Failure timestamp
- Worker ID

This metadata enables debugging, auditing, replay, and recovery.

---

# DLQ Reviewer

The DLQ Reviewer starts automatically when the Spark Processor container launches.

It continuously consumes messages from:

```text
raw-server-metrics-dlq
```

For every failed record it performs:

- Error inspection
- Recovery rule selection
- Automatic repair (when possible)
- Republishing repaired records
- Routing permanent failures

---

# Recovery Rules

The reviewer automatically repairs supported recoverable errors.

Current recovery rules include:

- `socket_count` datatype conversion
- Timestamp normalization

Recovered records receive additional recovery metadata before being republished.

Example:

```json
{
  "recovery_metadata": {
    "reviewed_by": "DLQ_REVIEWER_V1",
    "recovery_type": "INVALID_SOCKET_COUNT",
    "recovered_at": "2026-07-01T08:15:43"
  }
}
```

---

# Recoverable Records

Recoverable errors include:

- `INVALID_SCHEMA`
- `INVALID_SOCKET_COUNT`

Recovered records are automatically published to:

```text
raw-server-metrics-retry
```

Spark continuously consumes this topic together with the primary telemetry topic.

No manual replay is required.

---

# Non-Recoverable Records

Records that cannot be repaired automatically include:

- `MISSING_DEVICE_ID`
- `MISSING_POWERDETAIL`

These records are published to:

```text
raw-server-metrics-failure
```

They are never replayed into the streaming pipeline.

---

# Streaming Processing Pipeline

After validation, Spark processes only valid telemetry.

Processing stages include:

- Exploding nested `PowerDetail` arrays
- Event-time conversion
- Watermark assignment
- Device-level aggregation
- Metric computation
- Analytics transformation
- Writing Snappy-compressed Parquet datasets

Each Spark worker processes records independently while sharing Kafka partitions.

---

# Watermark Processing

Spark applies a one-hour watermark on telemetry timestamps.

Benefits include:

- Late event handling
- Stateful aggregation cleanup
- Reduced memory consumption

Events arriving later than the configured watermark threshold are discarded automatically.

---

# Parallel Workers

The processing engine launches two Spark workers automatically.

### Worker 1

- Independent Kafka consumer
- Dedicated checkpoint directory
- Dedicated output directory

### Worker 2

- Independent Kafka consumer
- Dedicated checkpoint directory
- Dedicated output directory

Both workers consume telemetry simultaneously, enabling higher throughput and improved fault tolerance.

---

# Streaming Output

The streaming pipeline produces analytics-ready Parquet datasets for downstream consumers.

Features include:

- Append-mode writes
- Snappy compression
- Worker-specific output directories

Example output directory:

```text
/app/data/processed/stream/
├── worker_1/
└── worker_2/
```

---

# Batch Processing

The ATLAS Processing Engine also supports historical batch processing.

Unlike the streaming pipeline, batch mode processes archived telemetry already stored in Parquet format.

The batch pipeline performs:

- Reading archived telemetry
- Schema enforcement
- Exploding nested `PowerDetail` arrays
- Device-level aggregations
- Analytics transformation
- Writing analytics-ready Parquet output

Batch processing is useful for:

- Historical analytics
- Backfilling datasets
- Reprocessing archived telemetry
- Data migration

---

# Batch Processing Workflow

```text
Archived Parquet
        │
        ▼
  Spark Batch Job
        │
        ▼
 Explode PowerDetail
        │
        ▼
   Device Aggregation
        │
        ▼
 Processed Parquet
```
---

# Execution Guide

## 1. Start Required Services

Start the required ATLAS services using Docker Compose.

```bash
docker compose up -d
```

Ensure the following services are running:

- Kafka Broker
- Spark Processor
- Ingestion Service

---

## 2. Spark Processor Startup

The Spark Processor starts automatically.

During startup it performs the following operations:

- Creates required directories
- Creates checkpoint directories
- Creates worker log files
- Creates the DLQ log
- Waits for Kafka availability
- Starts the DLQ Reviewer
- Starts Spark Worker 1
- Starts Spark Worker 2

No manual Spark startup is required.

---

## 3. Generate Telemetry

Invoke the ingestion API.

```http
POST http://localhost:8001/pcid/PLATCUSTxxxx/acid/APPCUSTxxxx/telemetry/latest/export
```

Telemetry is automatically published to:

```text
raw-server-metrics
```

---

## 4. Monitor Processing

Enter the Spark Processor container.

```bash
docker exec -it atlas-processor bash
```

Monitor Worker 1

```bash
tail -f /app/logs/worker1.log
```

Monitor Worker 2

```bash
tail -f /app/logs/worker2.log
```

Monitor the DLQ Reviewer

```bash
tail -f /app/logs/dlq.log
```

---

# Testing the DLQ Pipeline

For validation testing, publish messages manually to Kafka.

Enter the Kafka container.

```bash
docker exec -it broker1 bash
```

Start the Kafka producer.

```bash
/opt/bitnami/kafka/bin/kafka-console-producer.sh \
--bootstrap-server broker1:9092 \
--topic raw-server-metrics
```

---

## Test Case 1 – Valid Record

```json
{
  "device_id": "DEV-VALID",
  "report_id": "REP-000",
  "created_at": "2026-05-26T10:20:00",
  "inventory_data": {
    "socket_count": 4
  },
  "data": {
    "PowerDetail": [
      {
        "Average": 95.0,
        "Minimum": 80.0,
        "Peak": 120.0,
        "Time": "2026-05-26T10:20:00"
      }
    ]
  }
}
```

### Expected Result

- Successfully processed by Spark
- Aggregated successfully
- Written to Parquet output
- No DLQ message generated

---

## Test Case 2 – Recoverable Record

```json
{
  "device_id": "DEV-001",
  "report_id": "REP-001",
  "created_at": "2026-05-26T10:20:00",
  "inventory_data": {
    "socket_count": "4"
  },
  "data": {
    "PowerDetail": [
      {
        "Average": 91.2,
        "Minimum": 80.1,
        "Peak": 120.0,
        "Time": "2026-05-26T10:20:00"
      }
    ]
  }
}
```

### Expected Result

- Routed to `raw-server-metrics-dlq`
- DLQ Reviewer repairs the record
- `socket_count` converted to Integer
- Recovery metadata added
- Published to `raw-server-metrics-retry`
- Spark automatically consumes the repaired record
- Successfully written to Parquet

---

## Test Case 3 – Non-Recoverable Record

```json
{
  "device_id": null,
  "report_id": "REP-003",
  "created_at": "2026-05-26T10:20:00",
  "inventory_data": {
    "socket_count": 4
  },
  "data": {
    "PowerDetail": [
      {
        "Average": 90.0,
        "Minimum": 75.0,
        "Peak": 110.0,
        "Time": "2026-05-26T10:20:00"
      }
    ]
  }
}
```

### Expected Result

- Routed to `raw-server-metrics-dlq`
- Classified as non-recoverable
- Published to `raw-server-metrics-failure`
- Never replayed into the streaming pipeline

---

# Troubleshooting

If Spark reports

```text
UnknownTopicOrPartitionException
```

verify that the required Kafka topics exist.

List available topics:

```bash
docker exec -it broker1 bash

kafka-topics.sh \
--bootstrap-server localhost:9092 \
--list
```

If either of the following topics is missing:

- `raw-server-metrics-retry`
- `raw-server-metrics-failure`

create them before restarting the Spark Processor.

Create the retry topic:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic raw-server-metrics-retry \
  --partitions 12 \
  --replication-factor 1
```

Create the failure topic:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic raw-server-metrics-failure \
  --partitions 12 \
  --replication-factor 1
```

Restart the processor:

```bash
docker compose restart atlas-processor
```

---

# Batch Processing

To process archived telemetry:

```bash
spark-submit /app/jobs/batch_job.py
```

The batch job:

- Reads archived Parquet files
- Applies the telemetry schema
- Explodes nested `PowerDetail` arrays
- Computes device-level aggregations
- Generates analytics-ready datasets
- Writes compressed Parquet output

---

# Automatic Startup

When the Spark Processor container starts, it automatically:

- Creates required directories
- Creates checkpoint directories
- Creates worker log files
- Creates the DLQ log
- Waits for Kafka availability
- Starts the DLQ Reviewer
- Starts Spark Worker 1
- Starts Spark Worker 2
- Streams worker logs to the container console

---

# Output Directories

```text
/app/data/
│
├── raw/
│
├── processed/
│   ├── stream/
│   │   ├── worker_1/
│   │   └── worker_2/
│   │
│   └── batch/
│
├── checkpoints/
│   ├── stream_1/
│   ├── stream_2/
│   ├── dlq_1/
│   └── dlq_2/
│
└── logs/
    ├── worker1.log
    ├── worker2.log
    └── dlq.log
```

---

# Technologies Used

- Apache Spark 3.5
- Spark Structured Streaming
- Apache Kafka
- Python
- PySpark
- Docker
- Apache Parquet
- Snappy Compression

---

# Features

- Real-time Kafka Streaming
- Automatic Schema Validation
- Dead Letter Queue (DLQ)
- Automatic Error Recovery
- Retry Pipeline
- Permanent Failure Pipeline
- Historical Batch Processing
- Parallel Spark Workers
- Watermark-based Streaming
- Fault Tolerance
- Checkpoint Recovery
- Worker-level Logging
- Analytics-ready Parquet Output

---

# Output

The Spark Processing Engine generates:

- Real-time processed Parquet datasets
- Historical batch Parquet datasets
- Dead Letter Queue (DLQ) records
- Retry topic records after automatic recovery
- Permanent failure topic records
- Worker-specific log files
- Spark checkpoint metadata

The architecture provides a scalable, fault-tolerant, and resilient telemetry processing platform capable of handling both high-volume streaming workloads and historical batch analytics for the ATLAS platform.