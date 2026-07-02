# Engineering Journey
**Author:** Sanjula S

## Overview

The Spark Processing Engine underwent several architectural revisions before reaching its current design. The primary focus throughout development was to build a processing engine capable of handling continuous telemetry ingestion while remaining fault tolerant, scalable and compatible with the other ATLAS microservices.

This document summarizes the major engineering challenges, architectural decisions and optimizations introduced during development.

---

# Challenge 1 – Building a Unified Data Pipeline

Initially, the ingestion service and Spark processor evolved independently, resulting in incompatible data formats and processing assumptions.

### Solution

The ingestion service was refined to generate production-like telemetry and publish standardized JSON messages to Kafka. The Spark processor was updated to consume the same schema for both streaming and batch workflows.

### Outcome

- Standardized telemetry schema
- Simplified downstream processing
- Seamless integration with Kafka and Lakehouse

---

# Challenge 2 – Moving from File-Based Processing to Kafka Streaming

Early iterations relied on processing exported files directly.

As the architecture matured, Kafka became the central event bus for telemetry ingestion.

### Design Decision

Replacing direct file ingestion with Kafka decoupled producers from consumers and enabled continuous event processing using Spark Structured Streaming.

### Outcome

- Near real-time processing
- Better scalability
- Independent producer and consumer services

---

# Challenge 3 – Scaling Beyond a Single Spark Worker

The first streaming implementation used a single Spark consumer, which became a bottleneck as telemetry volume increased.

### Solution

The processor was redesigned to launch multiple Spark workers, each operating with independent Kafka consumer groups, checkpoints and output directories.

### Outcome

- Parallel Kafka partition consumption
- Improved CPU utilization
- Higher processing throughput

---

# Challenge 4 – Handling Invalid Telemetry

Malformed records previously interrupted streaming jobs and required manual inspection.

### Solution

A Dead Letter Queue (DLQ) and a dedicated recovery service were introduced. Recoverable records are repaired and replayed through a retry topic, while unrecoverable messages are isolated in a failure topic.

### Outcome

- Continuous stream execution
- Automated recovery workflow
- Reduced operational overhead

---

# Challenge 5 – Reliable Long-Running Streams

Streaming applications must survive container restarts and delayed telemetry without losing processing state.

### Solution

Worker-specific checkpoint directories and event-time watermarking were incorporated into the streaming pipeline.

### Outcome

- Reliable recovery after failures
- Controlled streaming state
- Support for late-arriving events

---

# Challenge 6 – Integration with the Lakehouse

The processor needed to integrate seamlessly with the Delta Lakehouse without duplicating storage responsibilities.

### Design Decision

The Spark processor focuses only on validation, transformation and aggregation, writing analytics-ready Parquet files to a shared volume. Deduplication, ACID transactions and partition management remain the responsibility of the Lakehouse service.

### Outcome

- Clear separation of responsibilities
- Loose coupling between services
- Simplified maintenance

---

# Performance Optimizations

Several optimizations were introduced throughout development to improve throughput and resource utilization.

- Transition from single-worker to parallel Spark processing.
- Kafka-based event streaming replacing direct file ingestion.
- Worker-specific checkpoints for independent recovery.
- Repartitioning prior to aggregation to improve workload distribution.
- Snappy-compressed Parquet output to reduce storage overhead.
- Dedicated DLQ reviewer operating independently from the main streaming pipeline.
- Startup synchronization to ensure Kafka availability before launching Spark workers.

---

# Key Architectural Decisions

| Decision | Reason |
|----------|--------|
| Apache Kafka | Decoupled telemetry ingestion and scalable event streaming |
| Spark Structured Streaming | Continuous low-latency processing |
| Multiple Spark Workers | Parallel processing across Kafka partitions |
| Shared Volume Integration | Efficient communication with the Lakehouse |
| DLQ + Retry Pipeline | Isolate and recover invalid telemetry |
| Checkpointing | Preserve streaming state across failures |
| Watermarking | Handle late-arriving telemetry efficiently |

---

# Lessons Learned

Developing the Spark Processing Engine reinforced several important engineering principles:

- Event-driven architectures simplify service integration.
- Schema consistency across services is essential for reliable pipelines.
- Invalid data should be isolated rather than blocking processing.
- Parallel consumers improve scalability when combined with Kafka partitioning.
- Clear separation of processing, storage and analytics responsibilities produces a more maintainable distributed system.