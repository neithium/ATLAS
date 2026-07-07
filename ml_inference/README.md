# ATLAS ML Inference Pipeline

> **Ownership**: S Nandini (Streaming & Orchestration sub-team)

This module implements the machine learning inference pipeline for the ATLAS telemetry platform. It consumes live telemetry data in real-time, performs unsupervised anomaly detection using an Isolation Forest model, computes a composite device health score, and exports enriched records for downstream ingestion into ClickHouse.

---

## Architecture and Flow

The inference pipeline reads live Parquet files, processes them through the pre-trained Isolation Forest model, runs health calculations, and saves the enriched predictions.

```text
Live Parquet                  Enriched Parquet
/data/live/*.parquet   →   inference.py   →   /data/ml_predictions/
                                 ↑
                        isolation_forest.pkl
```

### Appended Columns
For every processed record, the following metadata columns are appended:

| Column | Type | Description |
| :--- | :--- | :--- |
| `prediction` | `int` | `+1` = Normal metric behaviour, `-1` = Anomalous metric behaviour |
| `anomaly_score` | `float` | Raw decision function score (higher value indicates more normal behavior) |
| `health_score` | `float` | Composite score ranging in `[0, 100]` indicating overall server health |
| `health_status` | `string` | Human-readable severity tag derived from the health score |

### Health Score Classification

| Score Range | Status | Description |
| :--- | :--- | :--- |
| **90 – 100** | ✅ Healthy | Server operating normally across all monitored metrics. |
| **70 – 89** | ⚠️ Warning | Minor deviations in behavior detected; requires passive observation. |
| **50 – 69** | 🟠 Degraded | Persistent anomalies detected; potential hardware/software warning. |
| **0 – 49** | 🔴 Critical | High-severity anomaly flags; immediate mitigation recommended. |

---

## File Structure

```text
ml_inference/
├── config.py         # Configuration constants (directories, feature columns, weights)
├── health_score.py   # Composite health score algorithms (AHC, DHC, TCC)
├── inference.py      # Main pipeline execution entrypoint
├── requirements.txt  # Python package dependencies
├── Dockerfile        # Container recipe definition
├── models/
│   └── .gitkeep      # Directory where isolation_forest.pkl should be placed
└── README.md         # This documentation
```

---

## Prerequisites and Setup

### 1. Model Deployment
Ensure the pre-trained model file `isolation_forest.pkl` is placed at:
```text
ml_inference/models/isolation_forest.pkl
```

### 2. Feature Column Verification
Inspect `config.py` to ensure that `FEATURE_COLUMNS` matches the exact order of features used during model training:
```python
FEATURE_COLUMNS = [
    "MetricValue",
    "avg_metric_value",
    "max_metric_value",
    "min_metric_value",
    "hour_of_day",    # Auto-derived from metric_time
    "day_of_week",    # Auto-derived from metric_time
]
```

---

## Execution Guide

### Local Execution (Without Docker)

1. Install required packages:
   ```bash
   cd ml_inference
   pip install -r requirements.txt
   ```
2. Configure directories and model path:
   ```bash
   export ML_INPUT_DIR="/data/live"
   export ML_OUTPUT_DIR="/data/ml_predictions"
   export ML_MODEL_PATH="./models/isolation_forest.pkl"
   ```
3. Run inference once:
   ```bash
   python inference.py
   ```
4. Run inference continuously in watch mode (polls for new files every 60 seconds):
   ```bash
   export ML_WATCH_MODE="true"
   python inference.py
   ```

### Execution With Docker Compose

1. Build and start the inference pipeline:
   ```bash
   docker compose up -d atlas-ml
   ```
2. Watch execution logs:
   ```bash
   docker compose logs -f atlas-ml
   ```

---

## Configuration Parameter Reference

The following environment variables can be set to override default configuration parameters:

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `ML_INPUT_DIR` | `/data/live` | Ingress directory containing raw telemetry Parquet files |
| `ML_OUTPUT_DIR` | `/data/ml_predictions` | Egress directory where enriched Parquet files are written |
| `ML_MODEL_PATH` | `/app/models/isolation_forest.pkl` | Path to the serialized Isolation Forest model file |
| `ML_WATCH_MODE` | `false` | Enable continuous monitoring and polling mode |
| `ML_POLL_INTERVAL` | `60` | Check interval in seconds (used in watch mode) |
| `ML_W1` / `ML_W2` / `ML_W3` | `0.5` / `0.3` / `0.2` | Weights for Anomaly Health, Deviation Health, and Temp Health |

---

## Integration Architecture

```text
[Telemetry Generator] ──► /data/live/*.parquet
                                 │
                                 ▼
                          [ml_inference]
                                 │
                                 ▼
                     /data/ml_predictions/*.parquet
                                 │
                                 ▼
                          [delta_loader]
                                 │
                                 ▼
                            [ClickHouse]
```

The ClickHouse loader automatically polls and processes any Parquet files written to `/data/ml_predictions`. The extra ML metadata columns are parsed and populated in ClickHouse tables.
