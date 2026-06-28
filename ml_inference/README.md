# ATLAS ML Inference Pipeline

> **Owner**: Knsrikanta (ML Inference sub-team)  
> **Branch**: `feature/ml-inference-pipeline`  
> **Status**: Implementation complete — awaiting `isolation_forest.pkl` from training teammate

---

## What This Does

This module reads live telemetry Parquet files, runs Isolation Forest anomaly
detection, computes a health score (0–100), and writes enriched Parquet files
to `/data/ml_predictions` for the ClickHouse loader (PR #74) to ingest.

```
Live Parquet                  Enriched Parquet
/data/live/*.parquet   →   inference.py   →   /data/ml_predictions/
                                ↑
                        isolation_forest.pkl
```

Three columns are appended to every row:

| Column         | Type  | Meaning                              |
|----------------|-------|--------------------------------------|
| `prediction`   | int   | `+1` = Normal, `-1` = Anomaly        |
| `anomaly_score`| float | Raw decision_function score (higher = more normal) |
| `health_score` | float | Composite score in [0, 100]          |

Health score classification:

| Score   | Status     |
|---------|------------|
| 90–100  | ✅ Healthy  |
| 70–89   | ⚠️ Warning  |
| 50–69   | 🟠 Degraded |
| 0–49    | 🔴 Critical |

---

## File Structure

```
ml_inference/
├── config.py         ← ALL configurable constants (paths, weights, features)
├── health_score.py   ← Health score formula — edit only this to change AHC/DHC/TCC
├── inference.py      ← Main pipeline entry point
├── requirements.txt  ← Python dependencies
├── Dockerfile        ← Container definition
├── models/
│   └── .gitkeep      ← Place isolation_forest.pkl here
└── README.md         ← This file
```

---

## Prerequisites

### 1. Get the trained model

Your ML training teammate must provide:
```
isolation_forest.pkl
```
Place it at:
```
ml_inference/models/isolation_forest.pkl
```

### 2. Verify the feature columns match training

Open `config.py` and check `FEATURE_COLUMNS`:
```python
FEATURE_COLUMNS = [
    "MetricValue",
    "avg_metric_value",
    "max_metric_value",
    "min_metric_value",
    "hour_of_day",    # auto-derived from metric_time
    "day_of_week",    # auto-derived from metric_time
]
```
This **must exactly match** the column order used during training. If the
training teammate used a different order or set, update this list in `config.py`.

---

## Running Locally (Without Docker)

```bash
# Install dependencies
cd ATLAS/ml_inference
pip install -r requirements.txt

# Set paths (optional — defaults shown below)
set ML_INPUT_DIR=/data/live
set ML_OUTPUT_DIR=/data/ml_predictions
set ML_MODEL_PATH=./models/isolation_forest.pkl

# Run once
python inference.py

# Run continuously (poll every 60 s)
set ML_WATCH_MODE=true
python inference.py

# Custom poll interval
python inference.py --watch --interval 30
```

### Quick Verification Test (no model needed — uses a toy model)

```python
# Run this from ml_inference/ directory to verify the pipeline works end-to-end
import pandas as pd
import numpy as np
import pickle
from sklearn.ensemble import IsolationForest
from pathlib import Path

# 1. Create a toy model
model = IsolationForest(n_estimators=10, random_state=42, contamination=0.1)

# 2. Toy training data (6 features matching FEATURE_COLUMNS)
X_train = np.random.rand(200, 6)
model.fit(X_train)

# 3. Save the toy model
Path("models").mkdir(exist_ok=True)
with open("models/isolation_forest.pkl", "wb") as f:
    pickle.dump(model, f)
print("Toy model saved.")

# 4. Create a toy live parquet
df = pd.DataFrame({
    "device_id":       [f"SRV-{i:06d}" for i in range(50)],
    "server_name":     ["test-server"] * 50,
    "metric_time":     pd.date_range("2026-06-01", periods=50, freq="5min", tz="UTC"),
    "MetricValue":     np.random.rand(50) * 300,
    "avg_metric_value": np.random.rand(50) * 300,
    "max_metric_value": np.random.rand(50) * 300 + 100,
    "min_metric_value": np.random.rand(50) * 100,
    "location_name":   ["DataCenter-A"] * 50,
    "processor_vendor": ["Intel", "AMD"] * 25,
    "server_generation": ["Gen10 Plus"] * 50,
    "tags":            ["production"] * 50,
})
Path("/data/live").mkdir(parents=True, exist_ok=True)
df.to_parquet("/data/live/test_telemetry.parquet", index=False)
print("Toy parquet written.")

# 5. Now run the pipeline
import subprocess, sys
result = subprocess.run([sys.executable, "inference.py"], capture_output=True, text=True)
print(result.stdout)
if result.returncode != 0:
    print("STDERR:", result.stderr)
```

---

## Running With Docker Compose

The `atlas-ml` service is defined in `docker-compose.yml` (commented out by default
until the model is ready). To enable it:

1. Open `docker-compose.yml`
2. Find the `# atlas-ml:` section and uncomment it
3. Place `isolation_forest.pkl` in `ml_inference/models/`
4. Start the service:

```bash
docker compose up -d atlas-ml
docker compose logs -f atlas-ml
```

---

## Environment Variable Reference

| Variable          | Default                               | Description                         |
|-------------------|---------------------------------------|-------------------------------------|
| `ML_INPUT_DIR`    | `/data/live`                          | Live telemetry Parquet directory     |
| `ML_OUTPUT_DIR`   | `/data/ml_predictions`                | Enriched output Parquet directory    |
| `ML_MODEL_PATH`   | `/app/models/isolation_forest.pkl`    | Path to serialised model             |
| `ML_WATCH_MODE`   | `false`                               | Set `true` for continuous mode       |
| `ML_POLL_INTERVAL`| `60`                                  | Seconds between polls (watch mode)   |
| `ML_SCORE_MIN`    | `-0.5`                                | Anomaly score lower bound for AHC    |
| `ML_SCORE_MAX`    | `0.5`                                 | Anomaly score upper bound for AHC    |
| `ML_W1`           | `0.5`                                 | AHC weight in health score           |
| `ML_W2`           | `0.3`                                 | DHC weight in health score           |
| `ML_W3`           | `0.2`                                 | TCC weight in health score           |

---

## How to Change the Health Score Formula

> The team has not finalised the health score formula.

The formula lives exclusively in `health_score.py` → `calculate_health_score()`.

**To change only the formula:**
1. Edit `health_score.py`
2. Do NOT touch `inference.py` or `config.py`

**To change weights:**
1. Edit `config.py` → `WEIGHT_AHC`, `WEIGHT_DHC`, `WEIGHT_TCC`  
   (or set `ML_W1`, `ML_W2`, `ML_W3` environment variables)

---

## Integration with the Rest of the Pipeline

```
live_data_gen.py  →  /data/live/*.parquet
                           ↓
                     inference.py
                           ↓
              /data/ml_predictions/ml_predictions_YYYYMMDDTHHMMSSZ.parquet
                           ↓
              storage/clickhouse/delta_loader.py  (PR #74)
                           ↓
                       ClickHouse
                           ↓
                    Streamlit Dashboard
```

The ClickHouse loader (teammate's code) will automatically pick up any
Parquet file written to `/data/ml_predictions`.  No coordination needed —
extra columns (`prediction`, `anomaly_score`, `health_score`) that are not
in the ClickHouse schema are safely ignored by the loader.

> **Note**: The ClickHouse `init.sql` may need a schema update to store
> the new ML columns. That is the responsibility of the analytics teammate
> (PR #74 owner).

---

## Error Handling Reference

| Error                         | Cause                              | Behaviour                        |
|-------------------------------|------------------------------------|----------------------------------|
| Missing input directory       | `/data/live` doesn't exist         | Logs warning, skips cycle        |
| No Parquet files              | Directory empty                    | Logs warning, skips cycle        |
| Unreadable Parquet file       | Corrupt file                       | Logs warning, skips that file    |
| Missing `metric_time`         | Schema change upstream             | Raises `ValueError`, exits (1)   |
| Invalid timestamps            | Bad data                           | Drops rows, logs count           |
| Missing FEATURE_COLUMNS       | Schema drift or config mismatch    | Raises `ValueError`, exits (1)   |
| NaN in feature matrix         | Missing sensor readings            | Fills with column median, warns  |
| Model not found               | `.pkl` not placed in `models/`     | Raises `FileNotFoundError`, exits(1) |
| Feature count mismatch        | Training vs inference config diff  | Raises `ValueError`, exits (1)   |
| Empty DataFrame after filter  | All rows dropped                   | Logs warning, skips cycle        |
