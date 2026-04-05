# ATLAS Data Generation & Livewire Testing Guide

## 🔄 Complete Data Generation Pipeline

### Overview

The ATLAS system has a **3-stage data generation pipeline** that produces data in different formats for different use cases:

```
┌─────────────────┐
│ json_generator  │  Generates raw JSON with nested structures
│  (continuous)   │  1000 devices × 156 records = 156K records
└────────┬────────┘
         │ /app/data/raw/*.json
         │
         ├─────────────────────┬──────────────────────┐
         │                     │                      │
         ▼                     ▼                      ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ streaming_job.py │  │  batch_job.py    │  │  (Can be used    │
│  (real-time)     │  │  (daily)         │  │   separately)    │
└────────┬─────────┘  └────────┬─────────┘  └──────────────────┘
         │                     │
    /stream                /batch
    (hourly)              (daily)
    aggregates            aggregates
         │                     │
         ▼                     ▼
    1,000 rows/h          1,000 rows/day
    (1 per device         (1 per device
     per 1-hour            per day)
     window)
     
         └─────────┬───────────┘
                   │
                   ▼
         ┌──────────────────────┐
         │  LIVEWIRE MODE       │
         │  MERGE Deduplication │
         │  (tests both paths)  │
         └────────┬─────────────┘
                  │
                  ▼
            /refined (output)
            Deduplicated Delta Lake
```

---

## 📊 Data Generation Details

### 1️⃣ **json_generator.py** - Raw Data Generator

**Location:** `processing/jobs/json_generator.py`

**Configuration:**
```python
OUTPUT_DIR = "/app/data/raw"
DEVICE_COUNT = 1000                 # Devices per batch
VIRTUAL_START = datetime.utcnow()
TIME_MULTIPLIER = 60                # 1 real minute = 1 virtual hour
```

**Generation Frequency:**
- Every `300 seconds` (5 real-time minutes)
- Generates 1,000 devices per batch
- File pattern: `data_{device_id}_{timestamp}.json`

**Data Structure per Device:**
```python
{
  "application_customer_id": "APPCUST0001",
  "device_count": 1,
  "devices": {
    "PLAT1-DEV-0000": {
      "device_id": "PLAT1-DEV-0000",
      "platform_customer_id": "PLATCUST001",
      "application_customer_id": "APPCUST0001",
      "report_type": "power",
      "data": {
        "PowerDetail": [
          {
            "Time": "2026-03-06T15:34:00Z",
            "Average": 250.5,
            "CpuUtil": 45,
            "AmbTemp": 25.3,
            "Minimum": 175.0,
            "Peak": 400.0,
            "is_fresh": True   # ← Key for filtering
          },
          ...156 records total per device
          (144 historical with is_fresh=False, 12 fresh with is_fresh=True)
        ]
      }
    }
  }
}
```

**Time Window Logic:**
```
With TIME_MULTIPLIER=60:
- 1 real second  = 60 virtual seconds
- 1 real minute  = 1 virtual hour  ← KEY!
- 1 real hour    = 1 virtual day
- 24 real minutes = 1 virtual day

So:
- Historical: 6 days of virtual data (is_fresh=False)
- Fresh: 1 hour of virtual data, 5-minute intervals (is_fresh=True)
```

**Output:**
- Location: `/app/data/raw/`
- Files: `data_PLAT1-DEV-{device_id}_{unix_timestamp}.json`
- New batch every 5 real minutes with 1000 devices

---

### 2️⃣ **streaming_job.py** - Real-Time Streaming Processor

**Location:** `processing/jobs/streaming_job.py`

**What it Does:**
```
Spark Structured Streaming:
1. Reads JSON from /app/data/raw
2. Filters only records where is_fresh=True  ← Important filter!
3. Creates 1-hour tumbling windows
4. Groups by: window + device_id
5. Aggregates: avg(power), avg(cpu), avg(temp)
6. Writes to /app/data/processed/stream every 5-minute batch
```

**Configuration:**
```python
TRIGGER_INTERVAL_SECONDS = 5 * 60  # Process every 5 minutes
WINDOW_SIZE = "1 hour"              # 1-hour tumbling window
```

**Key Filter:**
```python
.filter(col("pd.is_fresh") == True)  # Only processes fresh records
```

**Output Schema:**
```python
[
  device_id          (String)
  event_time         (Timestamp) ← from pd.Time
  power              (Double)    ← from pd.Average
  cpu                (Long)      ← from pd.CpuUtil
  temp               (Double)    ← from pd.AmbTemp
]
```

**Output Details:**
- Location: `/app/data/processed/stream/`
- Format: Parquet files (one per 5-minute batch)
- Rows per batch: ~1,000 (one per device per 1-hour window)
- With 60x multiplier: New batch appears every 5 real minutes

**Example Timeline:**
```
Real Time   Virtual Time   Window Generated
────────    ────────────   ──────────────────
T+0:00      T+0:00         (data generation starts)
T+5:00      T+5:00         (still accumulating window 0-1h)
...
T+60:00     T+60:00 (1h)   ✓ WINDOW COMPLETE → Output 1000 rows
T+65:00     T+65:00 (1h5m) (now accumulating window 1-2h)
...
T+120:00    T+120:00 (2h)  ✓ WINDOW COMPLETE → Output 1000 rows
```

---

### 3️⃣ **batch_job.py** - Daily Batch Processor

**Location:** `processing/jobs/batch_job.py`

**What it Does:**
```
Spark Batch Job (Polling Loop):
1. Reads ALL JSON from /app/data/raw (no is_fresh filter)
2. Finds unique dates across all records
3. Identifies "completed days" (day < max_day)
4. For each completed day:
   - Filters records for that day
   - Groups by: device_id + event_date
   - Aggregates: avg(power), avg(cpu), avg(temp)
   - Writes to /app/data/processed/batch
5. Marks day as processed
6. Sleeps 60 seconds, loops again
```

**Key Logic:**
```python
all_days = [r[0] for r in flat.select("event_date").distinct().collect()]
max_day = max(all_days)
days_to_process = [day for day in all_days 
                   if day < max_day and day not in processed_days]
```

**Why Skip Current Day?**
- Current day is still being written by json_generator
- Would create inconsistent/partial aggregates
- Only processes "completed" days (days < max_day)

**Output Details:**
- Location: `/app/data/processed/batch/`
- Format: Parquet files (one per day)
- Rows per day: ~1,000 (one per device)
- With 60x multiplier: One new day appears every ~24 real minutes

**Example Timeline:**
```
Real Time    Virtual Days Exist   Days to Process   Output
────────     ──────────────────   ───────────────   ──────
T+0:30       Day 0, Day 1         (not yet)         (waiting)
T+1:00       Day 0, Day 1, Day 2  Day 0, Day 1      ✓ Day 0 batch
T+24:00      Day 0-6, Day 7, Day8 Day 0-6           ✓ Day 7 batch (if not done)
T+25:00      ...                  ...               ✓ Day 8 batch
```

---

## 🧪 Livewire Mode Testing

### Two Test Scenarios

Livewire mode can accept data from **either** `/stream` **or** `/batch`:

#### **Scenario A: LIVEWIRE WITH /STREAM (Real-Time)**

**Input Path:** `/app/data/processed/stream/`

**Data Characteristics:**
- Source: streaming_job.py output
- Trigger: Every 5 real minutes (~1 hour virtual)
- Rows per batch: ~1,000 (1 per device per 1-hour window)
- Schema: 5 columns (device_id, event_time, power, cpu, temp)
- Time window: 1-hour tumbling windows

**Livewire Processing:**
```
1. readStream monitors /stream for new Parquet files
2. Every 60 seconds (configurable), checks for new files
3. If files exist:
   a. Load Parquet(s) into DataFrame
   b. Validate schema (expect 35 Refined Layer fields)
   c. Align schema (map 5 columns → 35 fields with NULLs)
   d. Execute MERGE on triple-hash key
   e. Record metrics (rows processed, merged, inserted)
4. Checkpoint progress
5. Loop
```

**Expected Output:**
- Deduplicated rows written to `/refined`
- Low deduplication ratio (fresh data, minimal overlap)
- High throughput (100s of rows/sec)

---

#### **Scenario B: LIVEWIRE WITH /BATCH (Daily)**

**Input Path:** `/app/data/processed/batch/`

**Data Characteristics:**
- Source: batch_job.py output
- Trigger: When daily aggregates complete (~24 real min = 1 virtual day)
- Rows per batch: ~1,000 (1 per device per day)
- Schema: 5 columns (device_id, event_date, power, cpu, temp)
- Time window: Daily aggregates

**Livewire Processing:**
```
1. readStream monitors /batch for new Parquet files
2. Every 60 seconds (configurable), checks for new files
3. If new daily batch:
   a. Load Parquet into DataFrame
   b. Validate schema (expect 35 fields)
   c. Align schema (map 5 columns → 35 fields)
   d. Execute MERGE on triple-hash key
   e. Record metrics
4. Checkpoint progress
5. Loop
```

**Expected Output:**
- Deduplicated rows written to `/refined`
- **Higher deduplication ratio** (7-day rolling windows, more overlap)
- Medium throughput (10s-100s of rows/sec depending on batch size)

---

## 🚀 Running Livewire Tests

### Option 1: Automated Python Test Suite

```bash
cd /path/to/ATLAS

# Run comprehensive test
python3 test_livewire_comprehensive.py

# This will:
# 1. Validate processor setup
# 2. Start processor container
# 3. Generate data in /raw
# 4. Monitor /stream and /batch folders
# 5. Test livewire with /stream input
# 6. Test livewire with /batch input
# 7. Generate report
```

### Option 2: Docker Compose (Full Stack)

```bash
# Terminal 1: Start all services
docker-compose up -d

# Terminal 2: Monitor processor
docker logs -f atlas-processor

# Terminal 3: Monitor lakehouse
docker logs -f atlas-lakehouse

# Terminal 4: Check data
docker exec atlas-processor bash -c "
  echo 'Raw JSON:' && ls -l /app/data/raw/ | wc -l
  echo 'Stream Parquet:' && ls -l /app/data/processed/stream/ | wc -l
  echo 'Batch Parquet:' && ls -l /app/data/processed/batch/ | wc -l
"
```

### Option 3: Manual Testing

```bash
# 1. Start processor to generate data
docker-compose up -d atlas-processor

# 2. Wait for data generation
sleep 120

# 3. Start livewire lakehouse with /stream input
LIVEWIRE_INPUT=/app/data/processed/stream \
docker-compose run -e PIPELINE_MODE=livewire atlas-lakehouse

# Or with /batch input
LIVEWIRE_INPUT=/app/data/processed/batch \
docker-compose run -e PIPELINE_MODE=livewire atlas-lakehouse
```

---

## 📈 Expected Data Volumes

### Per Batch Generation (Every 5 Real Minutes)

```
Devices:           1,000
Records/Device:    156 (144 hist + 12 fresh)
Total Records:     156,000
File Size:         ~15-20 MB JSON
Generated Freq:    Every 5 real minutes
```

### Stream Output (Hourly Virtual)

```
Trigger:           Every 1-hour virtual window (≈ 5 min window)
Output Rows:       ~1,000 (1 per device)
Output File Size:  ~100 KB Parquet
Dedup Ratio:       ~0% (first time data)
```

### Batch Output (Daily Virtual)

```
Trigger:           When 1 virtual day completes
Output Rows:       ~1,000 (1 per device per day)
Output File Size:  ~100 KB Parquet per day
Dedup Ratio:       Variable (depends on window overlap)
Accumulation:      Grows with each completed day
```

### Refined Layer Output (Livewire MERGE)

```
Input Rows:        ~1,000 from /stream or /batch
Merge Condition:   (device_id, event_time, app_customer_id)
Expected Output:   
  - Stream input:  ~1,000 new rows (low dedup)
  - Batch input:   ~700-800 rows (70% dedup from 7-day overlap)
```

---

## 🔍 Debugging & Monitoring

### Check Data Generation Progress

```bash
# Raw JSON files
docker exec atlas-processor bash -c "
  echo '=== Raw JSON Files ==='
  find /app/data/raw -name '*.json' | wc -l
  echo 'Size:' && du -sh /app/data/raw/
"

# Stream output
docker exec atlas-processor bash -c "
  echo '=== Stream Parquet Files ==='
  find /app/data/processed/stream -name '*.parquet' | wc -l
  echo 'Size:' && du -sh /app/data/processed/stream/
"

# Batch output
docker exec atlas-processor bash -c "
  echo '=== Batch Parquet Files ==='
  find /app/data/processed/batch -name '*.parquet' | wc -l
  echo 'Size:' && du -sh /app/data/processed/batch/
"
```

### Check Processor Job Status

```bash
# View processor logs
docker logs -f atlas-processor | grep -E "(STARTED|Batch|Window|✓|✅)"

# Check which jobs are running
docker exec atlas-processor ps aux | grep spark
```

### Inspect Parquet Schema

```bash
docker exec atlas-processor python3 <<'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InspectSchema").getOrCreate()

# Check stream schema
stream_df = spark.read.parquet("/app/data/processed/stream")
print("=== Stream Schema ===")
stream_df.printSchema()
print(f"Row count: {stream_df.count()}")

# Check batch schema  
batch_df = spark.read.parquet("/app/data/processed/batch")
print("\n=== Batch Schema ===")
batch_df.printSchema()
print(f"Row count: {batch_df.count()}")
EOF
```

---

## 🎯 Comparison: Stream vs Batch Livewire

| Aspect | Stream Mode | Batch Mode |
|--------|-------------|-----------|
| **Input** | `/stream` (hourly) | `/batch` (daily) |
| **Data Freshness** | ~1 hour virtual | ~1 day virtual |
| **Arrival Pattern** | Every 5 min real | Every 24 min real |
| **Rows/Batch** | ~1,000 | ~1,000 |
| **Time Window** | 1-hour tumbling | 1-day aggregates |
| **Expected Dedup** | 0-20% | 60-80% (7-day overlap) |
| **Use Case** | Real-time dashboards | Historical analysis |
| **Latency** | Minutes | Hours |
| **Throughput** | Sustained 100s/sec | Bursty 1000s/batch |

---

## 🚨 Troubleshooting

### "No /stream data after 2 minutes"

**Cause:** Streaming windows haven't completed yet (need 1-hour virtual window)

**Solution:**
- Wait longer (1 real hour = 1 virtual hour with 60x multiplier)
- Or temporarily reduce `TIME_MULTIPLIER` for faster testing

### "No /batch data after 5 minutes"

**Cause:** Batch processor only processes completed days (not current day)

**Solution:**
- Wait for day boundary (~24 real minutes)
- Or modify batch_job.py to process current day (for testing only)

### "Schema mismatch" in Livewire logs

**Cause:** Stream/batch output has different schema than expected

**Solution:**
- Livewire schema validator automatically maps 5-column → 35-field
- Check COLUMN_NAME_MAPPING in livewire_schema_validator.py
- Add new mappings if needed

---

## 🔧 Customization

### Scale Testing (More Devices)

Edit `processing/jobs/json_generator.py`:
```python
DEVICE_COUNT = 10000      # 10x more devices
TIME_MULTIPLIER = 120     # 2x faster virtual time
```

Then restart processor:
```bash
docker-compose restart atlas-processor
```

### Change Window Configuration

Edit `processing/jobs/streaming_job.py`:
```python
# 2-hour windows instead of 1-hour
agg = flat.withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "2 hours"), col("device_id")) \
    .avg(...)
```

Edit `processing/jobs/batch_job.py`:
```python
# Group by hour instead of day
.groupBy("device_id", to_hour("pd.Time")) \
```

---

## 📝 Summary

**Data Pipeline Flow:**
1. `json_generator` → Creates raw JSON continuously
2. `streaming_job` → Processes fresh data in 1-hour windows
3. `batch_job` → Processes completed days
4. `livewire mode` → Can consume either stream or batch output
5. `refined layer` → Final deduplicated MERGE output

**Livewire Testing:**
- Tests MERGE deduplication with both real-time and batch data
- Validates schema alignment (5 columns → 35 fields)
- Measures deduplication ratios and performance
- Demonstrates exactly-once delivery semantics
