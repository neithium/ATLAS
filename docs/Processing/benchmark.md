# ATLAS Spark Processing Engine - Benchmark Report
**Author:** Sanjula S

## Overview

This document summarizes the benchmarking and performance evaluation of the ATLAS Spark Processing Engine. The objective was to validate the processor under different workloads while measuring scalability, stability, and throughput for both streaming and batch processing.

All benchmarks were executed on a local Docker-based deployment using Apache Spark Structured Streaming and Apache Kafka.

---

# Benchmark Objectives

The benchmarking process focused on validating:

- Streaming throughput
- Batch processing performance
- Multi-worker scalability
- Fault tolerance
- Long-running stability

---

# Test Environment

| Component | Configuration |
|-----------|---------------|
| Processing Engine | Apache Spark 3.5 |
| Streaming Engine | Spark Structured Streaming |
| Messaging | Apache Kafka |
| Deployment | Docker Compose |
| Output Format | Snappy Compressed Parquet |

---

# Benchmark Workloads

The processor was evaluated using multiple telemetry workloads of increasing size.

| Workload | Purpose |
|----------|---------|
| 10 Devices | Functional validation |
| 100 Devices | Streaming validation |
| 1,000 Devices | Medium-scale performance testing |
| 20,000 Devices | Large-scale processing validation |
| 55,000 Devices | Stress testing and scalability evaluation |

---

# Streaming Performance

Streaming benchmarks focused on measuring the processor's ability to continuously consume Kafka messages while maintaining stable execution.

### Observations

- Continuous Kafka consumption remained stable across all benchmark runs.
- Parallel Spark workers significantly improved processing throughput compared to the initial single-worker implementation.
- Worker-specific checkpoints enabled uninterrupted recovery during restart testing.
- Watermarking maintained bounded streaming state during extended execution.

---

# Batch Processing Performance

Historical datasets were processed using the Spark batch pipeline.

The batch processor successfully:

- Read archived telemetry
- Applied schema validation
- Performed aggregations
- Generated analytics-ready Parquet datasets

Batch execution remained consistent across different dataset sizes.

---

# Scalability Evaluation

The processor was gradually evaluated with increasing telemetry volumes.

| Scale | Result |
|--------|--------|
| 10 Devices | Successful |
| 100 Devices | Successful |
| 1,000 Devices | Successful |
| ~20,000 Devices | Successfully processed on local hardware |
| ~55,000 Devices | Stress-tested to evaluate infrastructure limits: Failed |


Metric	                         Value

Maximum devices processed	    20,160
Largest generated dataset	    55,000 devices
Kafka partitions	            12
Output format	                Snappy Parquet
Checkpoint recovery	            Successful
DLQ recovery	                Successful



The 20,000-device workload demonstrated the processor's ability to handle high-volume telemetry on a local development environment. Larger workloads were primarily used to identify hardware bottlenecks and validate architectural scalability.

---

# Architecture Improvements

Benchmarking guided several architectural optimizations throughout development.

| Optimization | Impact |
|--------------|--------|
| Parallel Spark workers | Improved throughput |
| Worker-specific checkpoints | Faster recovery |
| Snappy Parquet output | Reduced storage usage |
| Repartitioning before aggregation | Better workload distribution |
| DLQ recovery pipeline | Continuous processing despite invalid telemetry |

---

# Stability Testing

Long-running execution tests were performed to validate runtime stability.

The processor successfully demonstrated:

- Continuous Spark Structured Streaming
- Stable Kafka consumption
- Reliable checkpoint recovery
- Consistent Parquet generation
- Recovery after service interruptions

No major processing failures were observed during extended execution.

---

# Known Limitations

Benchmarking was performed on a local Docker deployment.

Performance may vary depending on:

- Available CPU cores
- Memory allocation
- Kafka partition count
- Spark executor configuration
- Storage performance

The benchmark results should therefore be interpreted as validation of the processing architecture rather than absolute production throughput.
