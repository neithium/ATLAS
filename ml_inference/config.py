"""
ATLAS ML Inference Pipeline — Central Configuration
====================================================
All runtime constants are defined here.

To change paths, feature columns, or health-score weights,
edit ONLY this file.  Nothing else in the pipeline should
contain hard-coded values.

Environment-variable overrides are supported for Docker
deployments — see the comment next to each constant.

Owner: Knsrikanta (ML Inference)
"""

import os

# =============================================================================
# Directory Paths
# =============================================================================

# Directory containing live telemetry Parquet files produced by live_data_gen.py
# Override via env: ML_INPUT_DIR
INPUT_DIRECTORY: str = os.getenv("ML_INPUT_DIR", "/data/live")

# Directory where enriched Parquet files will be written.
# PR #74 (ClickHouse loader) polls this path — do NOT change without
# coordinating with the analytics teammate.
# Override via env: ML_OUTPUT_DIR
OUTPUT_DIRECTORY: str = os.getenv("ML_OUTPUT_DIR", "/data/ml_predictions")

# Path to the trained Isolation Forest model serialised with pickle.
# The model must be provided by the ML training teammate.
# Override via env: ML_MODEL_PATH
MODEL_PATH: str = os.getenv("ML_MODEL_PATH", "/app/models/isolation_forest.pkl")

# =============================================================================
# Feature Engineering
# =============================================================================

# IMPORTANT: The order of columns here must exactly match the order used
# during training.  If the training teammate changes the feature set, update
# this list accordingly.
#
# Current default matches the architecture diagram (6 features):
#   MetricValue, avg_metric_value, max_metric_value, min_metric_value,
#   hour_of_day (derived), day_of_week (derived)
#
# The pipeline will raise a clear ValueError if any column is missing from
# the incoming Parquet so the mismatch is immediately obvious.
FEATURE_COLUMNS: list[str] = [
    "MetricValue",
    "avg_metric_value",
    "max_metric_value",
    "min_metric_value",
    "hour_of_day",    # Derived from metric_time in feature_engineering()
    "day_of_week",    # Derived from metric_time in feature_engineering()
]

# =============================================================================
# Anomaly Score Normalisation Bounds (for AHC)
# =============================================================================

# Isolation Forest decision_function scores are negative for anomalies.
# These bounds are used to normalise the raw score to [0, 100].
# Calibrate these values after observing the score distribution on real data.
# Override via env: ML_SCORE_MIN / ML_SCORE_MAX
SCORE_MIN: float = float(os.getenv("ML_SCORE_MIN", "-0.5"))
SCORE_MAX: float = float(os.getenv("ML_SCORE_MAX", "0.5"))

# =============================================================================
# Health Score Weights
# =============================================================================

# AHC  — Anomaly Health Component     (isolation-forest based)
# DHC  — Deviation Health Component   (how far current value is from avg)
# TCC  — Temporal Consistency Component (matches expected pattern for this hour)
#
# Weights must sum to 1.0.
# Override via env: ML_W1 / ML_W2 / ML_W3
WEIGHT_AHC: float = float(os.getenv("ML_W1", "0.5"))
WEIGHT_DHC: float = float(os.getenv("ML_W2", "0.3"))
WEIGHT_TCC: float = float(os.getenv("ML_W3", "0.2"))

# =============================================================================
# Health Score Classification Thresholds (informational — used in logging)
# =============================================================================

HEALTH_THRESHOLD_HEALTHY: float = 90.0   # 90–100  → Healthy
HEALTH_THRESHOLD_WARNING: float = 70.0   # 70–89   → Warning
HEALTH_THRESHOLD_DEGRADED: float = 50.0  # 50–69   → Degraded
                                          # 0–49    → Critical

# =============================================================================
# Execution Mode
# =============================================================================

# When True, inference.py will run in a continuous loop, polling INPUT_DIRECTORY
# every POLL_INTERVAL_SECONDS for new files.
# Set via env: ML_WATCH_MODE=true
WATCH_MODE: bool = os.getenv("ML_WATCH_MODE", "false").lower() == "true"

# Seconds between directory polls when WATCH_MODE is True.
# Override via env: ML_POLL_INTERVAL
POLL_INTERVAL_SECONDS: int = int(os.getenv("ML_POLL_INTERVAL", "60"))
