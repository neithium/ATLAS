# ATLAS Spark Processing Engine – Architecture Evolution
**Author:** Sanjula S

## Overview

The ATLAS Spark Processing Engine was developed iteratively over multiple phases. Rather than implementing the complete architecture at once, new capabilities were introduced as the ATLAS platform expanded, allowing the processor to evolve from a simple batch application into a scalable, fault-tolerant streaming engine.

Each iteration addressed limitations identified during integration, testing, and performance evaluation.

---

# Evolution Timeline

```text
Static Batch Processing
        │
        ▼
Structured Streaming
        │
        ▼
Kafka Integration
        │
        ▼
Checkpointing & Watermarking
        │
        ▼
Dead Letter Queue
        │
        ▼
Automatic Recovery
        │
        ▼
Parallel Spark Workers
        │
        ▼
Lakehouse Integration
```

---

# Phase 1 – Static Batch Processing

The initial implementation focused on processing archived telemetry stored as static JSON datasets.

```text
Raw JSON
    │
    ▼
Spark Batch Job
    │
    ▼
Aggregation
    │
    ▼
Parquet
```

### Characteristics

- Batch-only execution
- Single Spark application
- Historical analytics
- Local file processing

### Limitation

The architecture was unable to process continuously arriving telemetry.

---

# Phase 2 – Structured Streaming

To support continuous telemetry ingestion, Spark Structured Streaming replaced the static batch workflow for real-time processing.

```text
Kafka
   │
   ▼
Spark Structured Streaming
   │
   ▼
Parquet
```

### Improvements

- Continuous processing
- Lower processing latency
- Micro-batch execution model
- Real-time analytics

---

# Phase 3 – Kafka-Based Integration

As the ingestion service matured, telemetry generation shifted from exported files to Apache Kafka.

The Spark processor was redesigned to consume telemetry directly from Kafka, allowing ingestion and processing services to operate independently.

```text
Ingestion
    │
    ▼
Kafka
    │
    ▼
Spark
```

### Architectural Changes

- Decoupled producer-consumer communication
- Standardized telemetry schema
- Support for continuous event streaming

---

# Phase 4 – Reliable Streaming

Long-running streaming jobs required mechanisms to recover from failures while handling delayed telemetry.

Checkpointing and event-time watermarking were incorporated into the processing pipeline.

### Improvements

- Stateful recovery
- Kafka offset persistence
- Late-event handling
- Controlled streaming state

---

# Phase 5 – Dead Letter Queue

Schema validation introduced a new challenge -> invalid telemetry should not interrupt processing.

Instead of rejecting entire batches, invalid records were redirected to a dedicated Dead Letter Queue.

```text
Kafka
   │
Validation
   │
 ┌──────────┐
 ▼          ▼
Valid     Invalid
 │          │
 ▼          ▼
Spark      DLQ
```

### Improvements

- Continuous stream execution
- Invalid record isolation
- Simplified debugging

---

# Phase 6 – Self-Healing Recovery Pipeline

Many DLQ records contained recoverable formatting errors rather than corrupted telemetry.

A dedicated recovery service was introduced to inspect failed records, apply recovery rules, and automatically replay corrected messages into Kafka.

```text
DLQ
 │
 ▼
Recovery Service
 │
 ├──────────────┐
 ▼              ▼
Retry        Failure
 │
 ▼
Spark
```

### Supported Recovery

- Datatype normalization
- Timestamp correction

This transformed the DLQ into a self-healing processing pipeline instead of a simple error repository.

---

# Phase 7 – Parallel Processing

The initial streaming implementation relied on a single Spark worker, which limited throughput as telemetry volume increased.

The architecture was redesigned to launch multiple Spark workers consuming Kafka partitions independently.

```text
Kafka
 │
 ├───────────────┐
 ▼               ▼
Worker 1     Worker 2
 │               │
 └──────┬────────┘
        ▼
Shared Output
```

### Improvements

- Better CPU utilization
- Parallel Kafka consumption
- Independent checkpoints
- Improved scalability

---

# Final Architecture

The current Spark Processing Engine combines all previous improvements into a unified processing platform capable of supporting both streaming and historical analytics.

### Current Capabilities

- Apache Kafka integration
- Spark Structured Streaming
- Historical batch processing
- Schema validation
- Event-time watermarking
- Worker-level checkpointing
- Dead Letter Queue
- Automatic recovery pipeline
- Parallel Spark workers
- Analytics-ready Parquet generation
- Shared-volume Lakehouse integration

---

# Key Architectural Decisions

| Decision | Purpose |
|----------|---------|
| Apache Kafka | Decouple ingestion from processing |
| Structured Streaming | Continuous event processing |
| Watermarking | Handle delayed telemetry |
| Checkpointing | Recover streaming state |
| Dead Letter Queue | Isolate invalid records |
| Recovery Pipeline | Automatically repair recoverable telemetry |
| Multi-worker Processing | Improve throughput and scalability |
| Shared Parquet Output | Simplify Lakehouse integration |

---

# Evolution Summary

The Spark Processing Engine evolved from a standalone batch processor into a distributed streaming platform capable of processing high-volume telemetry while maintaining reliability and scalability.

The final architecture combines real-time streaming, historical batch processing, automatic error recovery, parallel execution, and seamless integration with the ATLAS Lakehouse, providing a robust foundation for the platform's analytics pipeline.