"""
ATLAS ML Inference Pipeline — Central Configuration
====================================================
All runtime constants are defined here.

To change paths, feature columns, or health-score weights,
edit ONLY this file.  Nothing else in the pipeline should
contain hard-coded values.

Environment-variable overrides are supported for Docker
deployments — see the comment next to each constant.

Owner: S Nandini (ML Inference)
"""

import os

# =============================================================================
# Directory Paths
# =============================================================================

# Directory containing live telemetry Parquet files produced by live_data_gen.py
# Override via env: ML_INPUT_DIR
INPUT_DIRECTORY: str = os.getenv("ML_INPUT_DIR", "/data/live")

# Directory where enriched Parquet files will be written.
# atlas-analytics (ClickHouse loader) polls this path — do NOT change without
# coordinating with the analytics teammate.
# Override via env: ML_OUTPUT_DIR
OUTPUT_DIRECTORY: str = os.getenv("ML_OUTPUT_DIR", "/data/ml_predictions")

# Path to the trained Isolation Forest model serialised with joblib.
# Trained by ML training teammate (Sanjula) via train_model.py.
# Override via env: ML_MODEL_PATH
MODEL_PATH: str = os.getenv("ML_MODEL_PATH", "/app/models/isolation_forest.pkl")

# Path to the sklearn ColumnTransformer preprocessor serialised with joblib.
# Must match the preprocessor used during training — same feature columns, same order.
# Override via env: ML_PREPROCESSOR_PATH
PREPROCESSOR_PATH: str = os.getenv("ML_PREPROCESSOR_PATH", "/app/models/preprocessor.pkl")

# Path to the health score config dict serialised with joblib.
# Contains: {"min_score": float, "max_score": float, "threshold": float}
# Override via env: ML_HEALTH_CONFIG_PATH
HEALTH_CONFIG_PATH: str = os.getenv("ML_HEALTH_CONFIG_PATH", "/app/models/health_score_config.pkl")

# =============================================================================
# Feature Engineering
# =============================================================================
#
# IMPORTANT: This list must exactly match the features produced by engineer_features()
# in train_model.py (Sanjula's training pipeline).  The sklearn preprocessor handles
# the actual column-to-index mapping, so order here is just for documentation.
#
# Categorical columns — processed by OrdinalEncoder inside the preprocessor:
#   tags, processor_vendor, server_generation, location_name
#
# Numeric columns — processed by StandardScaler inside the preprocessor:
#   All remaining columns after metadata drop and feature derivation.
#
# We do NOT pass a flat numpy array to the model directly; we pass a DataFrame
# to preprocessor.transform() which replicates the training-time column handling.

# Columns to DROP before calling preprocessor.transform() — these are metadata /
# identity columns that were also dropped during training.  The preprocessor was
# fit on the DataFrame AFTER these were removed, so they must not be present.
METADATA_COLUMNS: list[str] = [
    "report_id",
    "device_id",
    "server_name",
    "application_customer_id",
    "platform_customer_id",
    "location_city",
    "location_state",
    "location_country",
    "cpu_inventory",
    "memory_inventory",
    "metric_time",
    "last_boot_time",
    "last_maintenance_date",
    "is_anomaly",  # label — only present in test splits
]

# =============================================================================
# Anomaly Score Normalisation Bounds (for AHC)
# =============================================================================
#
# By default these are loaded from health_score_config.pkl at runtime
# (trained min/max scores from Sanjula's train_model.py).
# These env-var overrides allow manual override if needed.
#
# Leave as empty string to always load from the .pkl file (recommended).
SCORE_MIN_OVERRIDE: str = os.getenv("ML_SCORE_MIN", "")
SCORE_MAX_OVERRIDE: str = os.getenv("ML_SCORE_MAX", "")

# =============================================================================
# Health Score Weights
# =============================================================================
#
# AHC  — Anomaly Health Component     (isolation-forest based)
# DHC  — Deviation Health Component   (how far avg_metric_value is from batch avg)
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
