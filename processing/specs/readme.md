# 📊 ATLAS Streaming vs Batch Benchmark (Simulated Pipeline)

## ⚙️ System Architecture

```text
Generator → Raw JSON Storage
            ↙           ↘
      Streaming        Batch
   (Hourly Aggregates) (Daily Aggregates)
```

---

## ⏱️ Time Simulation Model

To avoid running for 24 hours, we simulate time:

| Real Time  | Simulated Time |
| ---------- | -------------- |
| 1 minute   | 1 hour         |
| 5 minutes  | 5 hours        |
| 24 minutes | 24 hours       |

### Implementation

```python
TIME_MULTIPLIER = 60  # 1 min = 1 hour

def get_virtual_now():
    elapsed = time.time() - START_REAL
    return VIRTUAL_START + timedelta(seconds=elapsed * TIME_MULTIPLIER)
```

---

## 📦 Data Generation (Generator)

Every **5 minutes (real time)**:

* Generates JSON files for **1000 devices**
* Each file contains:

### ✅ Historical Data

* 6 days of data
* Hourly records (~144)
* `is_fresh = false`

### ⚡ Fresh Data

* Last 1 hour
* 5-minute interval readings (~12)
* `is_fresh = true`

---

## 🔵 Streaming Pipeline

### Input

Reads JSON files from:

```
/app/data/raw
```

---

### Processing Steps

* Flatten nested JSON (`explode`)
* Filter only fresh data:

```python
pd.is_fresh == True
```

* Convert timestamp:

```python
to_timestamp(pd.Time)
```

---

### ⏳ Window Aggregation

```python
window(event_time, "1 hour")
```

* Groups data into 1-hour buckets
* Runs every **5 minutes (trigger interval)**

---

### 📊 Aggregations

```python
avg(power), avg(cpu), avg(temp)
```

---

## 💧 Watermarking

```python
.withWatermark("event_time", "10 minutes")
```

### Purpose

* Handles late-arriving data
* Cleans up state

### Behavior in this simulation

* Data arrives in order
* No late data present
* Watermark is **enabled but not triggered**

---

## 🟡 Batch Pipeline

### Input

Reads full dataset from:

```
/app/data/raw
```

---

### Processing Steps

* Flatten JSON
* Extract date:

```python
to_date(pd.Time)
```

* Group by:

```python
device_id, event_date
```

---

### 📊 Aggregation

```python
avg(power), avg(cpu), avg(temp)
```

---

### Output

* One row per **device per day**
* Stored as partitioned Parquet

---

## 📁 Output Data

### 🔵 Streaming Output

Path:

```
/processed/stream
```

Contains:

```
device_id
window (start, end)
avg(power, cpu, temp)
```

---

### 🟡 Batch Output

Path:

```
/processed/batch
```

Partitioned by:

```
batch_date
```

Contains:

```
device_id
daily averages
```

---

## 📊 Benchmark Results

```
==================================================
📊 ATLAS BENCHMARK REPORT
==================================================

🔵 STREAM PERFORMANCE
- Total Rows Processed : 9886
- Total Time           : 33.63 sec
- Throughput           : 293.96 rows/sec
- Avg Latency          : 4.204 sec

🟡 BATCH PERFORMANCE
- Total Rows Processed : 171000
- Total Time           : 20.3 sec
- Throughput           : 8422.59 rows/sec
- Avg Latency          : 2.538 sec
```

---

## 📈 Interpretation

### 🔵 Streaming

* Processes **only fresh data**
* Uses **stateful window aggregation**
* Lower throughput due to:

  * windowing
  * incremental updates

👉 Optimized for **low-latency insights**

---

### 🟡 Batch

* Processes **entire dataset**
* No state management
* High throughput due to:

  * bulk processing
  * vectorized execution

👉 Optimized for **accuracy and scale**

---

## ⚖️ Streaming vs Batch

| Feature    | Streaming            | Batch        |
| ---------- | -------------------- | ------------ |
| Data scope | Fresh only           | Full dataset |
| Latency    | Low                  | Moderate     |
| Throughput | Lower                | High         |
| Accuracy   | Incremental          | Exact        |
| Use case   | Real-time monitoring | Reporting    |

---


## ⏳ Execution Time

To simulate **1 full day**:

```
Run time required ≈ 25–30 minutes
```

---
Note: Currently the system simulates micro-batch ingestion using files. In the next stage, I will integrate Kafka to transition to true event-driven streaming, enabling lower latency and production-grade scalability
