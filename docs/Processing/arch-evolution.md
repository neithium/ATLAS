# ATLAS Spark Processing Engine – Design Evolution
# Author: Sanjula S
## Overview

The ATLAS Spark Processing Engine was developed incrementally rather than as a single implementation. Throughout the project, the architecture evolved multiple times to improve scalability, fault tolerance, maintainability, and integration with the rest of the ATLAS platform.

Each iteration addressed limitations identified during testing, mentor reviews, and integration with other project components.

---

# Evolution Timeline

```
Version 1
──────────────
Static Batch Processing

        │
        ▼

Version 2
──────────────
Structured Streaming

        │
        ▼

Version 3
──────────────
Kafka Integration

        │
        ▼

Version 4
──────────────
Checkpointing & Watermarking

        │
        ▼

Version 5
──────────────
Dead Letter Queue (DLQ)

        │
        ▼

Version 6
──────────────
Automatic Recovery Pipeline

        │
        ▼

Version 7
──────────────
Parallel Multi-Worker Processing

        │
        ▼

Current Architecture
```

---

# Version 1 – Initial Batch Processing

## Objective

The first implementation focused on processing historical telemetry data stored as static files, inside the same container (before initial integration).

### Architecture

```
Raw JSON
    │
    ▼
Spark Batch Job
    │
    ▼
Aggregation
    │
    ▼
Parquet Output
```

### Characteristics

- Reads complete datasets
- Stateless execution
- Single Spark application
- Historical analytics only

### Limitations

- No real-time processing
- High processing latency
- Unable to process continuously arriving telemetry

---

# Version 2 – Structured Streaming

To support continuous telemetry ingestion, Spark Structured Streaming was introduced.

### Changes

- Streaming DataFrames replaced static DataFrames
- Micro-batch execution model adopted
- Continuous processing enabled

### Benefits

- Near real-time analytics
- Continuous execution
- Lower latency

---

# Version 3 – Kafka Integration

Initially, Spark consumed telemetry directly from JSON files. As the project evolved, Apache Kafka became the central ingestion layer.

### Previous Flow

```
Generator
    │
    ▼
JSON Files
    │
    ▼
Spark
```

### Updated Flow

```
Generator
    │
    ▼
Kafka
    │
    ▼
Spark Streaming
```

### Why Kafka?

- Decouples producers and consumers
- Supports scalable event streaming
- Enables replay through Kafka offsets
- Improves reliability during downstream failures

---

# Version 4 – Checkpointing and Watermarking

Streaming applications maintain state across multiple micro-batches. To improve fault tolerance and manage late-arriving data, checkpointing and watermarking were introduced.

## Checkpointing

Checkpoint directories store processing progress, allowing Spark to resume after unexpected failures without reprocessing completed data.

### Benefits

- Fault recovery
- Offset tracking
- Stateful aggregation support

---

## Watermarking

Watermarks define how long Spark waits for delayed events before finalizing aggregations.

### Benefits

- Handles late-arriving records
- Prevents unbounded state growth
- Improves memory management

---

# Version 5 – Dead Letter Queue

During testing, malformed telemetry records caused failures within the streaming pipeline.
Instead of discarding invalid messages or terminating processing, a dedicated Dead Letter Queue (DLQ) was introduced.

### Processing Flow

```
Kafka

      │

      ▼

Schema Validation

      │

 ┌────┴────┐

 ▼         ▼

Valid   Invalid

 │         │

 ▼         ▼

Spark     DLQ
```

### Benefits

- Pipeline continues processing valid data
- Invalid records are isolated
- Simplifies debugging and auditing

---

# Version 6 – Automatic Recovery Pipeline

Many invalid records contained minor formatting issues that could be corrected automatically.
A recovery service was introduced to inspect DLQ messages, repair supported errors, and replay corrected records back into Kafka.

### Recovery Workflow

```
DLQ

 │
 ▼

Recovery Rules

 │
 ├──────────────┐
 ▼              ▼

Recovered   Unrecoverable

 │              │
 ▼              ▼
Retry Topic  Failure Topic
```

### Supported Recoveries

- Socket count datatype conversion
- Timestamp normalization

### Benefits

- Reduces manual intervention
- Improves data retention
- Enables automatic replay

---

# Version 7 – Parallel Multi-Worker Processing

The initial streaming implementation relied on a single Spark consumer, which limited throughput under increasing workloads.

The architecture was redesigned to use multiple Spark workers consuming Kafka partitions in parallel.

### Previous Architecture

```
Kafka

 │

 ▼

Worker 1

 │

 ▼

Parquet
```

### Current Architecture

```
Kafka

 │

 ├─────────────┐

 ▼             ▼

Worker 1    Worker 2

 │             │

 └──────┬──────┘

        ▼

Shared Output
```

### Benefits

- Increased throughput
- Better CPU utilization
- Parallel Kafka partition consumption
- Independent checkpoints per worker
- Improved scalability

---

# Current Architecture

The final Spark Processing Engine combines all previous improvements into a unified processing pipeline.

Major capabilities include:

- Apache Kafka integration
- Spark Structured Streaming
- Historical batch processing
- Schema validation
- Watermark-based event processing
- Checkpoint-based recovery
- Dead Letter Queue
- Automatic record recovery
- Multi-worker parallel processing
- Analytics-ready Parquet generation
- Lakehouse integration through shared volumes

---

# Key Design Decisions

| Decision | Motivation |
|----------|------------|
| Apache Kafka | Reliable event streaming and decoupled ingestion |
| Structured Streaming | Continuous telemetry processing |
| Watermarking | Late event handling and state cleanup |
| Checkpointing | Fault tolerance and recovery |
| Dead Letter Queue | Isolation of invalid records |
| Recovery Pipeline | Automatic correction of recoverable errors |
| Multi-worker Architecture | Improved scalability and throughput |
| Shared Volume Output | Seamless integration with the Delta Lakehouse |

---

# Conclusion

The Spark Processing Engine evolved from a simple batch-processing application into a resilient, scalable, and fault-tolerant telemetry processing service. Each architectural revision addressed practical limitations observed during development and testing, resulting in a modular pipeline capable of supporting both real-time and historical analytics while integrating seamlessly with the ATLAS Lakehouse.