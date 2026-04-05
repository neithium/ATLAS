# ATLAS Livewire Testing - Quick Reference

## 🚀 Quick Start (3 Steps)

### Step 1: Start Data Generation
```bash
docker-compose up -d atlas-processor
```

### Step 2: Run Tests
```bash
# Automated comprehensive test
python3 test_livewire_comprehensive.py

# OR manual testing
docker exec atlas-processor bash -c "
  echo 'Files in /stream:' && ls -1 /app/data/processed/stream/*.parquet | wc -l
  echo 'Files in /batch:' && ls -1 /app/data/processed/batch/*.parquet | wc -l
"
```

### Step 3: Start Livewire Mode
```bash
# Test with stream input (real-time, hourly data)
docker-compose up atlas-lakehouse -e PIPELINE_MODE=livewire

# OR test with batch input (daily data)
docker-compose up atlas-lakehouse -e PIPELINE_MODE=livewire -e STREAM_INPUT=/app/data/processed/batch
```

---

## 📊 Data Pipeline Status

### Check Generation Progress

```bash
# All in one command
docker exec atlas-processor bash -c "
echo '════════════════════════════════════════'
echo '  ATLAS Data Pipeline Status'
echo '════════════════════════════════════════'
echo ''
echo 'Raw JSON Files:'
ls -1 /app/data/raw/*.json 2>/dev/null | wc -l
echo 'Size:' && du -sh /app/data/raw/
echo ''
echo 'Stream Parquet Files:'
ls -1 /app/data/processed/stream/*.parquet 2>/dev/null | wc -l
echo 'Size:' && du -sh /app/data/processed/stream/
echo ''
echo 'Batch Parquet Files:'
ls -1 /app/data/processed/batch/*.parquet 2>/dev/null | wc -l
echo 'Size:' && du -sh /app/data/processed/batch/
echo ''
echo 'Metrics:'
tail -5 /app/data/metrics/stream_metrics.json 2>/dev/null || echo '(none yet)'
"
```

### Monitor Container Logs

```bash
# Processor logs (data generation)
docker logs -f atlas-processor

# Lakehouse logs (livewire mode)
docker logs -f atlas-lakehouse

# Both in split terminal:
# Terminal 1:
docker logs -f atlas-processor | grep -E "(Generated|Batch|Window|✅|✓)"

# Terminal 2:
docker logs -f atlas-lakehouse | grep -E "(MERGE|Batch|Match|Insert|✓|Throughput)"
```

---

## ⏱️ Expected Timeline

With **60x time multiplier** (standard):

| Real Time | Virtual Time | Expected Event |
|-----------|--------------|---|
| T+0:00    | T+0:00       | Generator starts |
| T+5:00    | T+5:00       | First batch generated |
| T+10:00   | T+10:00      | More data flowing |
| T+1:00    | T+60:00      | ✓ Streaming window 1 complete → /stream output |
| T+5:00    | T+5:00       | ✓ Streaming window 2 complete → /stream output |
| T+24:00   | T+24h        | ✓ Batch job processes completed days → /batch output |
| T+2:00    | T+120:00     | Multiple streaming windows available |
| T+48:00   | T+2 days     | Multiple daily batches available |

---

## 📝 Testing Scenarios

### Scenario 1: Minimal Latency Test (Stream Input)

Perfect for **real-time validation** of deduplication logic:

```bash
# Start processor
docker-compose up -d atlas-processor

# Wait for 1st streaming window (≈1-2 real minutes)
sleep 120

# Start livewire with stream
docker-compose run --rm \
  -e PIPELINE_MODE=livewire \
  -e RUN_GENERATOR=n \
  -e RUN_PIPELINE=y \
  atlas-lakehouse

# Expected: ~1000 rows processed per micro-batch, low dedup ratio
```

### Scenario 2: Rolling Window Test (Batch Input)

Perfect for **7-day rolling window** deduplication:

```bash
# Start processor
docker-compose up -d atlas-processor

# Wait for daily batches to accumulate (≈30+ real minutes)
sleep 1800

# Start livewire with batch
docker-compose run --rm \
  -e PIPELINE_MODE=livewire \
  -e RUN_GENERATOR=n \
  -e RUN_PIPELINE=y \
  -e STREAM_INPUT=/app/data/processed/batch \
  atlas-lakehouse

# Expected: ~70% dedup ratio (7-day rolling window overlap)
```

### Scenario 3: Full Stack Integration

Production-like setup with generator + processor + livewire:

```bash
# Start all services
docker-compose up -d

# Monitor all three
docker logs -f atlas-ingestion &
docker logs -f atlas-processor &
docker logs -f atlas-lakehouse &

# Expected: Continuous data flow through entire pipeline
```

---

## 🔧 Configuration Options

### Environment Variables

```bash
# Pipeline mode
-e PIPELINE_MODE=livewire              # or 'benchmark'

# Execution control
-e RUN_GENERATOR=n                     # Skip generator
-e RUN_PIPELINE=y                      # Run processing
-e RUN_VACUUM=n                        # Skip cleanup

# Spark configuration
-e SPARK_EXECUTOR_CORES=6              # Default: 6
-e SPARK_EXECUTOR_MEMORY=4g            # Default: 4g
-e SPARK_DYNAMIC_ALLOCATION=false      # Enable scaling

# Livewire-specific
-e STREAM_INPUT=/app/data/processed/stream   # Default
-e STREAM_INPUT=/app/data/processed/batch    # Alternative
```

### Livewire Config (in livewire_streaming.py)

```python
LivewireConfig.TRIGGER_INTERVAL_SECONDS = 60     # Micro-batch window
LivewireConfig.VALIDATE_SCHEMA = True            # Enable validation
LivewireConfig.COMPRESSION_CODEC = "zstd"        # Compression
LivewireConfig.ZORDER_COLUMN = "metric_time"    # Clustering
```

---

## 📊 Monitoring Metrics

### Stream Processing Metrics

```bash
# Check streaming job metrics
docker exec atlas-processor tail -20 /app/data/metrics/stream_metrics.json
```

Output format:
```json
{
  "batch_id": 0,
  "rows": 1234,
  "duration": 2.45,
  "throughput": 503.5
}
```

### Batch Processing Metrics

```bash
# Check batch job metrics
docker exec atlas-processor tail -20 /app/data/metrics/batch_metrics.json
```

Output format:
```json
{
  "run_id": 0,
  "event_date": "2026-03-01",
  "rows": 1000,
  "duration": 1.23,
  "throughput": 813.0
}
```

### Livewire MERGE Metrics

During livewire execution, watch for:
```
Batch #: 1000 rows | Merged: 500 | Inserted: 500 | 0.34s
Throughput: 2941 rows/sec
Match Ratio: 50% (duplicates detected)
```

---

## 🐛 Troubleshooting

### "No data in /stream folder"

```bash
# Check processor is running
docker ps | grep atlas-processor

# Check logs
docker logs atlas-processor | grep -i "streaming"

# Reason: Streaming needs 1-hour window (≈1 real minute)
# Solution: Wait longer OR reduce TIME_MULTIPLIER
```

### "Only raw JSON, no Parquet output"

```bash
# Check spark jobs are running
docker exec atlas-processor ps aux | grep spark

# Check metrics
docker exec atlas-processor ls -la /app/data/metrics/

# Reason: Jobs haven't completed processing yet
# Solution: Wait for windows/days to complete
```

### "Livewire not processing?"

Check livewire config in docker-compose.yml:
```bash
# Verify environment variables
docker ps --format "table {{.Names}}\t{{.Env}}" | grep lakehouse

# Check logs
docker logs atlas-lakehouse | tail -50
```

---

## 🔄 Cleanup & Reset

### Stop All Services

```bash
docker-compose down
```

### Remove Data & Restart Fresh

```bash
# Stop
docker-compose down -v

# Clean volumes
docker volume rm atlas_raw-volume atlas_refined-volume atlas_spark-checkpoint

# Restart
docker-compose up -d
```

### Keep Processor Running (Stop Lakehouse Only)

```bash
docker-compose down atlas-lakehouse

# Data continues flowing to /stream and /batch
# Then restart lakehouse when ready:
docker-compose up atlas-lakehouse
```

---

## 📚 Documentation

- **Full Pipeline Guide:** `ATLAS_DATA_PIPELINE_GUIDE.md`
- **Livewire Mode Docs:** `delta_lake/LIVEWIRE_MODE.md`
- **Implementation Summary:** `LIVEWIRE_IMPLEMENTATION_SUMMARY.md`

---

## 🎯 Success Criteria

✅ **Processor Running:**
- [ ] Raw JSON files in `/app/data/raw`
- [ ] Job logs show "BATCH STARTED" and "STREAMING STARTED"

✅ **Stream Processing:**
- [ ] Parquet files in `/app/data/processed/stream`
- [ ] New files appear every ~5 real minutes
- [ ] ~1,000 rows per file

✅ **Batch Processing:**
- [ ] Parquet files in `/app/data/processed/batch`
- [ ] New files appear every ~24 real minutes
- [ ] ~1,000 rows per day

✅ **Livewire Mode:**
- [ ] Container starts with PIPELINE_MODE=livewire
- [ ] Logs show "MERGE COMPLETE" messages
- [ ] Refined output files created
- [ ] Metrics showing rows processed/merged/inserted

---

## 💡 Pro Tips

1. **Speed Up Testing:** Reduce `TIME_MULTIPLIER` in `json_generator.py`
   ```python
   TIME_MULTIPLIER = 120  # 1 real minute = 2 virtual hours
   ```

2. **Increase Data Volume:** Increase `DEVICE_COUNT`
   ```python
   DEVICE_COUNT = 10000  # 10,000 devices per batch
   ```

3. **Monitor in Real Time:** Use `watch` command
   ```bash
   watch -n 5 'docker exec atlas-processor bash -c "ls -1 /app/data/processed/stream/*.parquet 2>/dev/null | wc -l"'
   ```

4. **Extract Data for Analysis:** Copy to host
   ```bash
   docker cp atlas-processor:/app/data/refined ./atlas_refined_output
   ```

5. **Check Data Quality:**
   ```bash
   docker exec atlas-processor python3 -c "
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName('Inspect').getOrCreate()
   df = spark.read.parquet('/app/data/processed/stream')
   df.show()
   df.describe().show()
   "
   ```

---

## 📞 Common Commands Cheat Sheet

```bash
# Start all
docker-compose up -d

# Monitor processor
docker logs -f atlas-processor

# Check stream data
docker exec atlas-processor ls -la /app/data/processed/stream/ | tail

# Check batch data
docker exec atlas-processor ls -la /app/data/processed/batch/ | tail

# Count files
docker exec atlas-processor bash -c "find /app/data -name '*.parquet' | wc -l"

# Stop everything
docker-compose down

# Clean and restart
docker-compose down -v && docker-compose up -d

# Run livewire test with stream
docker-compose run --rm -e PIPELINE_MODE=livewire -e RUN_PIPELINE=y atlas-lakehouse

# View container state
docker ps

# Get full logs
docker logs atlas-processor > processor.log
docker logs atlas-lakehouse > lakehouse.log

# Execute Python in processor
docker exec -it atlas-processor python3
```

---

**Ready to test?** Start with: `python3 test_livewire_comprehensive.py`
