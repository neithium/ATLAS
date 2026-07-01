# ATLAS Spark Processing Engine

Apache Spark-based batch and streaming processing engine for the ATLAS platform.

### Author: Sanjula S

---

# Overview

The ATLAS Spark Processing Engine is responsible for consuming high-volume telemetry data from Apache Kafka, validating incoming records, classifying invalid messages, automatically recovering recoverable failures, processing streaming telemetry, executing historical batch jobs, and generating analytics-ready Parquet datasets for downstream platforms.

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
- Parallel worker execution

---

# Architecture

```

```
                    Kafka Producer
                          │
                          ▼
                raw-server-metrics
                          │
                          ▼
             Spark Structured Streaming
      ┌────────────────────────────────────┐
      │                                    │
      │ JSON Parsing                       │
      │ Schema Validation                  │
      │ Error Classification               │
      │ Watermark Processing               │
      │ Streaming Aggregation              │
      └────────────────────────────────────┘
             │                      │
             │                      │
      Valid Records          Invalid Records
             │                      │
             ▼                      ▼
 Stream Parquet Output      raw-server-metrics-dlq
 (/worker_1, worker_2)               │
                                     ▼
                              DLQ Reviewer
                          ┌──────────┴──────────┐
                          │                     │
                   Recoverable          Non-Recoverable
                          │                     │
                          ▼                     ▼
             raw-server-metrics-retry   raw-server-metrics-failure
                          │
                          ▼
           Spark consumes repaired records
```

---

# Processing Pipeline

The Spark processing engine consists of four major components:

1. Kafka Streaming Consumer
2. DLQ Processing Engine
3. Streaming Analytics Pipeline
4. Historical Batch Processing

Each component performs a dedicated task while sharing the same telemetry schema and processing rules.

---

# Kafka Topics

| Topic | Purpose |
|--------|----------|
| raw-server-metrics | Incoming telemetry |
| raw-server-metrics-dlq | Invalid records awaiting review |
| raw-server-metrics-retry | Successfully repaired records |
| raw-server-metrics-failure | Permanently failed records |

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

Invalid records are redirected to the Dead Letter Queue for further review.

---

# Streaming Workflow

```

```
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
      │
 ┌────┴────┐
 │         │
 ▼         ▼
VALID   INVALID
 │         │
 ▼         ▼
Aggregation  DLQ
 │         │
 ▼         ▼
Parquet  Reviewer
            │
      ┌─────┴─────┐
      ▼           ▼
 Retry Topic   Failure Topic
      │
      ▼
 ReadStream
```

---

# Schema Validation

Every Kafka message is validated against the Spark telemetry schema before processing begins.

Validation includes:

- Required field validation
- Datatype validation
- Nested PowerDetail validation
- Inventory validation
- Timestamp validation

Messages failing validation never enter the analytics pipeline.

---

# Error Classification

Each incoming message is classified into one of five categories.

## VALID

The record satisfies every schema and business validation rule.

The record immediately enters the Spark aggregation pipeline.

---

## INVALID_SCHEMA

The incoming JSON cannot be parsed correctly using the telemetry schema.

Examples include:

- Missing nested structures
- Incorrect JSON layout
- Invalid datatypes

These records are sent to the DLQ.

---

## INVALID_SOCKET_COUNT

The socket_count field cannot be parsed as an integer.

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

Since the device identifier is mandatory, this record cannot be recovered automatically.

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

All invalid records are automatically published to

```
raw-server-metrics-dlq
```

The DLQ stores additional metadata together with the original record.

Each DLQ message contains:

- Original JSON payload
- Error Type
- Source Kafka topic
- Partition
- Offset
- Kafka timestamp
- Failure timestamp
- Worker ID

This metadata enables replay, debugging, auditing, and recovery.

---

# DLQ Reviewer

The DLQ Reviewer starts automatically when the processor container launches.

It continuously consumes messages from

```
raw-server-metrics-dlq
```

For every failed message it performs:

- Error inspection
- Recovery rule selection
- Automatic repair (if possible)
- Republishing repaired records
- Routing permanent failures

---

# Recovery Rules

The reviewer automatically repairs supported recoverable errors.

Current recovery rules include:

- socket_count datatype conversion
- Timestamp normalization

Recovered records receive additional recovery metadata before being republished.

Example metadata

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

Recoverable errors include

- INVALID_SCHEMA
- INVALID_SOCKET_COUNT

Recovered records are automatically published to

```
raw-server-metrics-retry
```

Spark continuously consumes this topic together with the main telemetry topic.

No manual replay is required.

---

# Non-Recoverable Records

Records that cannot be repaired automatically include

- MISSING_DEVICE_ID
- MISSING_POWERDETAIL

These records are published to

```
raw-server-metrics-failure
```

They are never replayed into the streaming pipeline.

---

# Streaming Analytics

After validation, Spark processes only valid telemetry.

Processing stages include:

- Exploding nested PowerDetail arrays
- Event time conversion
- Watermark assignment
- Device-level aggregation
- Metric computation
- Analytics transformation
- Parquet generation

Each worker processes records independently while sharing Kafka partitions.

---

# Watermark Processing

Spark applies a one-hour watermark on telemetry timestamps.

Benefits include:

- Late event handling
- Stateful aggregation cleanup
- Reduced memory consumption

Older events beyond the watermark threshold are discarded automatically.

---

# Parallel Workers

The processing engine launches two Spark workers automatically.

Worker 1

- Independent Kafka consumer
- Dedicated checkpoint directory
- Dedicated output directory

Worker 2

- Independent Kafka consumer
- Dedicated checkpoint directory
- Dedicated output directory

Both workers consume telemetry simultaneously, enabling higher throughput and fault tolerance.

---

# Streaming Output

The streaming pipeline produces analytics-ready Parquet datasets for downstream consumers.
The generated Parquet files are written using Snappy compression for efficient storage and analytics.
Worker-specific output directories are used to support parallel processing.

Example directory structure

```

/app/data/processed/stream/

├── worker_1/

└── worker_2/

```

---

# Batch Processing

The ATLAS processing engine also supports historical batch processing.

Unlike the streaming pipeline, batch mode processes archived telemetry already stored in Parquet format.

The batch pipeline performs:

- Reading archived telemetry
- Schema enforcement 
- Exploding nested PowerDetail arrays
- Device-level aggregations
- Analytics transformation
- Writing analytics-ready Parquet output

This mode is primarily used for

- Historical analytics
- Backfilling datasets
- Reprocessing archived telemetry
- Data migration

---

# Batch Processing Workflow

```

Archived Parquet
↓
Spark Batch Job
↓
Explode PowerDetail
↓
Aggregation
↓
Processed Parquet

```

---

# Execution Guide

## 1. Start Required Services

Start the complete ATLAS platform using Docker Compose.

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
- Creates checkpoint folders
- Creates worker log files
- Creates DLQ log file
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

Telemetry is published directly into

```
raw-server-metrics
```

---

## 4. Monitor Processing

Enter the Spark container

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

Monitor DLQ Reviewer

```bash
tail -f /app/logs/dlq.log
```

---

# Testing the DLQ Pipeline

Manual testing can be performed using the Kafka console producer.

Enter the Kafka container

```bash
docker exec -it broker1 bash
```

Start the producer

```bash
/opt/bitnami/kafka/bin/kafka-console-producer.sh \
--bootstrap-server broker1:9092 \
--topic raw-server-metrics
```

---

## Test Case 1 — Valid Record

```json
{
  "device_id":"DEV-VALID",
  "report_id":"REP-000",
  "created_at":"2026-05-26T10:20:00",
  "inventory_data":{
      "socket_count":4
  },
  "data":{
      "PowerDetail":[
          {
              "Average":95.0,
              "Minimum":80.0,
              "Peak":120.0,
              "Time":"2026-05-26T10:20:00"
          }
      ]
  }
}
```

Expected Result

- Successfully processed
- Aggregated by Spark
- Written as Parquet
- No DLQ message generated

---

## Test Case 2 — Recoverable Record

```json
{
  "device_id":"DEV-001",
  "report_id":"REP-001",
  "created_at":"2026-05-26T10:20:00",
  "inventory_data":{
      "socket_count":"4"
  },
  "data":{
      "PowerDetail":[
          {
              "Average":91.2,
              "Minimum":80.1,
              "Peak":120.0,
              "Time":"2026-05-26T10:20:00"
          }
      ]
  }
}
```

Expected Result

- Routed to `raw-server-metrics-dlq`
- DLQ Reviewer repairs the record
- socket_count converted to Integer
- Recovery metadata added
- Record published to `raw-server-metrics-retry`
- Spark automatically consumes the repaired record
- Successfully written to Parquet

---

## Test Case 3 — Non-Recoverable Record

```json
{
  "device_id":null,
  "report_id":"REP-003",
  "created_at":"2026-05-26T10:20:00",
  "inventory_data":{
      "socket_count":4
  },
  "data":{
      "PowerDetail":[
          {
              "Average":90.0,
              "Minimum":75.0,
              "Peak":110.0,
              "Time":"2026-05-26T10:20:00"
          }
      ]
  }
}
```

Expected Result

- Routed to `raw-server-metrics-dlq`
- DLQ Reviewer classifies as non-recoverable
- Published to `raw-server-metrics-failure`
- Never replayed into Spark



Note: if spark fails, check kafka partitions using 
docker exec -it broker1 bash
> kafka-topics.sh --bootstrap-server localhost:9092 --list

if it does not list retry and failure topic, create these topics, 
failure topic: 

kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic raw-server-metrics-failure \
  --partitions 12 \
  --replication-factor 1



retry topic: 

kafka-topics.sh \                                                                                                                   
  --bootstrap-server localhost:9092 \
  --create \
  --topic raw-server-metrics-retry \
  --partitions 12 \
  --replication-factor 1



then restart spark-processor-container
---

# Batch Processing

To process archived telemetry

```bash
spark-submit /app/jobs/batch_job.py
```

The batch job

- Reads archived Parquet files
- Computes aggregated metrics
- Produces analytics-ready Parquet datasets

---

# Automatic Startup

When the processor container starts it automatically:

- Creates required directories
- Creates checkpoint directories
- Creates worker log files
- Creates the DLQ log
- Waits for Kafka to become available
- Starts the DLQ Reviewer
- Starts Spark Worker 1
- Starts Spark Worker 2
- Streams all logs to the container console


# Output Directories

```
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
- Parquet
- Snappy Compression

---

# Features

- Real-time Kafka Streaming
- Automatic Schema Validation
- Dead Letter Queue
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

The Spark Processing Engine generates

- Real-time processed Parquet datasets
- Historical batch Parquet datasets
- DLQ messages
- Retry topic messages
- Permanent failure topic messages
- Worker-specific log files
- Checkpoint metadata

The architecture provides a scalable, fault-tolerant, and resilient telemetry processing platform capable of handling both high-volume streaming workloads and historical batch analytics for the ATLAS ecosystem.