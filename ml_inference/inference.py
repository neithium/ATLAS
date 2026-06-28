"""
ATLAS ML Inference Pipeline — Main Entry Point
===============================================
Reads live telemetry Parquet files produced by ML-Model/live_data_gen.py,
runs Isolation Forest inference via Sanjula's trained sklearn preprocessor
and model, computes a composite health score, and writes enriched Parquet
to the output directory for downstream ClickHouse ingestion.

Pipeline steps
--------------
1. load_data()            → Read newest Parquet file from INPUT_DIRECTORY
2. engineer_features()    → Derive all features used during training
                            (matches train_model.py exactly)
3. preprocess_features()  → Run sklearn ColumnTransformer (preprocessor.pkl)
                            to scale numeric + encode categorical columns
4. load_models()          → Deserialise all 3 artifacts with joblib (once)
5. predict()              → model.decision_function() + threshold classification
6. calculate_health_score()→ AHC + DHC + TCC → [0, 100]
7. save_predictions()     → Write enriched Parquet to OUTPUT_DIRECTORY

Running
-------
    # One-shot (default):
    python inference.py

    # Continuous watch mode (polls INPUT_DIRECTORY every 60 s):
    ML_WATCH_MODE=true python inference.py

    # Override paths via environment:
    ML_INPUT_DIR=/data/live ML_OUTPUT_DIR=/data/ml_predictions python inference.py

Owner: Knsrikanta (ML Inference)
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import joblib
import numpy as np
import pandas as pd

import config
from health_score import calculate_health_score

# =============================================================================
# Logging Setup — stdout so Docker logs picks everything up
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("atlas.ml.inference")


# =============================================================================
# Step 1 — Load Data (newest file only, matching predict.py behaviour)
# =============================================================================

def load_data(input_dir: str) -> pd.DataFrame:
    """
    Read the most-recently-modified Parquet file from *input_dir*.

    Matching Sanjula's predict.py which processes only the latest live file
    (files sorted by mtime descending, first file used).

    Returns an empty DataFrame (with a warning) when:
        - The directory does not exist
        - No .parquet files are found
        - The selected file cannot be read
    """
    data_path = Path(input_dir)

    if not data_path.exists():
        log.warning("Input directory does not exist: %s", input_dir)
        return pd.DataFrame()

    parquet_files = sorted(
        data_path.rglob("*.parquet"),
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )

    if not parquet_files:
        log.warning("No Parquet files found in: %s", input_dir)
        return pd.DataFrame()

    # Use the newest file (live_data_gen.py writes one file per snapshot)
    newest = parquet_files[0]
    log.info(
        "Found %d Parquet file(s) — processing newest: %s",
        len(parquet_files),
        newest.name,
    )

    try:
        df = pd.read_parquet(newest)
    except Exception as exc:  # noqa: BLE001
        log.error("Cannot read %s: %s", newest, exc)
        return pd.DataFrame()

    log.info("Loaded %d rows from %s", len(df), newest.name)
    return df


# =============================================================================
# Step 2 — Feature Engineering (mirrors train_model.py exactly)
# =============================================================================

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive all features that were created during training.

    This function is a faithful copy of train_model.py::engineer_features()
    (and predict.py::engineer_features()) so that the feature matrix passed
    to preprocessor.transform() has exactly the same columns as were seen
    at fit-time.

    New derived columns:
        hour_of_day          int  [0, 23]
        day_of_week          int  [0=Mon … 6=Sun]
        uptime_hours         float (hours since last boot)
        days_since_maintenance int (days since last maintenance)
        memory_capacity_gb   float (extracted from memory_inventory if needed)
        temperature_delta    float (cpu_temperature − amb_temp)
        power_range          float (max_metric_value − min_metric_value)
        fan_temp_ratio       float (fan_speed_rpm / (cpu_temperature + 1))
        power_per_socket     float (avg_metric_value / socket_count)
        cpu_memory_ratio     float (cpu_utilization / (memory_utilization + 1))
        cpu_disk_ratio       float (cpu_utilization / (disk_utilization + 1))

    Metadata columns (report_id, device_id, server_name, etc.) are kept here
    so that they can be re-attached to the output later.  They are stripped
    ONLY when building the preprocessor input in preprocess_features().

    Raises:
        ValueError: If a mandatory column is missing.
    """
    mandatory = ["metric_time", "last_boot_time"]
    missing = [c for c in mandatory if c not in df.columns]
    if missing:
        raise ValueError(
            f"Mandatory columns missing from telemetry data: {missing}. "
            "Check that the upstream data generator produces these columns."
        )

    df = df.copy()

    # ---- Timestamp parsing ----
    df["metric_time"] = pd.to_datetime(df["metric_time"], utc=True, errors="coerce")
    df["last_boot_time"] = pd.to_datetime(df["last_boot_time"], utc=True, errors="coerce")

    nat_count = df["metric_time"].isna().sum()
    if nat_count > 0:
        log.warning("Dropping %d row(s) with invalid metric_time.", nat_count)
        df = df.dropna(subset=["metric_time"])

    if df.empty:
        log.warning("DataFrame is empty after timestamp parsing.")
        return df

    if "last_maintenance_date" in df.columns:
        df["last_maintenance_date"] = pd.to_datetime(
            df["last_maintenance_date"], utc=True, errors="coerce"
        )

    # ---- Time-based features ----
    df["hour_of_day"] = df["metric_time"].dt.hour.astype(int)
    df["day_of_week"] = df["metric_time"].dt.dayofweek.astype(int)

    df["uptime_hours"] = (
        df["metric_time"] - df["last_boot_time"]
    ).dt.total_seconds() / 3600

    if "last_maintenance_date" in df.columns:
        df["days_since_maintenance"] = (
            df["metric_time"] - df["last_maintenance_date"]
        ).dt.days

    # ---- Memory capacity (extract from string if numeric column absent) ----
    if "memory_capacity_gb" not in df.columns and "memory_inventory" in df.columns:
        df["memory_capacity_gb"] = (
            df["memory_inventory"]
            .astype(str)
            .str.extract(r"(\d+)")
            .astype(float)
        )

    # ---- Derived sensor features ----
    df["temperature_delta"] = df["cpu_temperature"] - df["amb_temp"]

    df["power_range"] = df["max_metric_value"] - df["min_metric_value"]

    df["fan_temp_ratio"] = df["fan_speed_rpm"] / (df["cpu_temperature"] + 1)

    if "socket_count" in df.columns:
        df["power_per_socket"] = df["avg_metric_value"] / df["socket_count"]
    else:
        df["power_per_socket"] = df["avg_metric_value"]

    df["cpu_memory_ratio"] = df["cpu_utilization"] / (df["memory_utilization"] + 1)
    df["cpu_disk_ratio"] = df["cpu_utilization"] / (df["disk_utilization"] + 1)

    log.info(
        "Feature engineering complete. Rows: %d | Columns now: %d",
        len(df),
        len(df.columns),
    )
    return df


# =============================================================================
# Step 3 — Preprocess Features (sklearn ColumnTransformer)
# =============================================================================

def preprocess_features(df: pd.DataFrame, preprocessor) -> np.ndarray:
    """
    Strip metadata columns (as done during training) and apply the fitted
    sklearn ColumnTransformer (StandardScaler + OrdinalEncoder).

    The preprocessor was fit on the DataFrame AFTER metadata columns were
    removed.  We replicate that exact drop step here before calling
    preprocessor.transform().

    Returns:
        2D numpy array suitable for model.decision_function().

    Raises:
        ValueError: If the transformed matrix has unexpected shape.
    """
    # Drop metadata columns that were absent during training
    drop_cols = [c for c in config.METADATA_COLUMNS if c in df.columns]
    feature_df = df.drop(columns=drop_cols)

    log.info(
        "Preprocessing %d rows × %d columns via sklearn ColumnTransformer...",
        len(feature_df),
        len(feature_df.columns),
    )

    try:
        X = preprocessor.transform(feature_df)
    except Exception as exc:
        raise ValueError(
            f"preprocessor.transform() failed: {exc}\n"
            f"Feature columns sent: {list(feature_df.columns)}\n"
            "Ensure engineer_features() matches train_model.py exactly."
        ) from exc

    log.info("Preprocessing complete. Output shape: %s", X.shape)
    return X


# =============================================================================
# Step 4 — Load Models (once at startup)
# =============================================================================

def load_models() -> Tuple[object, object, dict]:
    """
    Deserialise all three model artifacts using joblib (same serialiser used
    by Sanjula's train_model.py).

    Returns:
        (model, preprocessor, health_cfg)
            model        – fitted sklearn IsolationForest
            preprocessor – fitted sklearn ColumnTransformer
            health_cfg   – dict with min_score / max_score / threshold

    Raises:
        FileNotFoundError: If any artifact file is missing.
        RuntimeError:      If any file cannot be deserialised.
    """
    def _load(path: str, label: str):
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(
                f"{label} not found: {path}\n"
                "Run train_model.py inside the atlas-ml container first."
            )
        try:
            obj = joblib.load(p)
            log.info("Loaded %s from: %s", label, path)
            return obj
        except Exception as exc:
            raise RuntimeError(f"Failed to load {label} from {path}: {exc}") from exc

    model = _load(config.MODEL_PATH, "IsolationForest model")
    preprocessor = _load(config.PREPROCESSOR_PATH, "sklearn preprocessor")
    health_cfg = _load(config.HEALTH_CONFIG_PATH, "health score config")

    log.info(
        "All models loaded. Score range: [%.4f, %.4f] | Threshold: %.4f",
        health_cfg.get("min_score", float("nan")),
        health_cfg.get("max_score", float("nan")),
        health_cfg.get("threshold", float("nan")),
    )
    return model, preprocessor, health_cfg


# =============================================================================
# Step 5 — Run Inference
# =============================================================================

def predict(
    model, preprocessor, health_cfg: dict, X: np.ndarray, df_engineered: pd.DataFrame
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Run Isolation Forest inference on the preprocessed feature matrix.

    Uses the trained threshold from health_cfg (20th percentile of training
    scores) to classify predictions, matching Sanjula's predict.py logic.

    Returns:
        (predictions, anomaly_scores)
            predictions   – int array: +1 (normal) or -1 (anomaly)
            anomaly_scores– float array: raw decision_function scores
    """
    log.info("Running inference on %d rows...", len(X))
    anomaly_scores = model.decision_function(X)  # higher = more normal

    threshold = health_cfg.get("threshold", np.percentile(anomaly_scores, 20))
    predictions = np.where(anomaly_scores < threshold, -1, 1)

    n_anomalies = int((predictions == -1).sum())
    n_normal = int((predictions == 1).sum())
    log.info(
        "Inference complete — normal: %d | anomalies: %d (%.1f%%)",
        n_normal,
        n_anomalies,
        100.0 * n_anomalies / len(predictions) if len(predictions) > 0 else 0.0,
    )
    return predictions, anomaly_scores


# =============================================================================
# Step 7 — Save Predictions
# =============================================================================

def save_predictions(df: pd.DataFrame, output_dir: str) -> Path:
    """
    Write the enriched DataFrame as a Parquet file to *output_dir*.

    Output schema keeps all original telemetry columns plus three new ones:
        prediction    : int  (+1 = normal, -1 = anomaly)
        anomaly_score : float (raw decision_function output — higher = healthier)
        health_score  : float (composite score in [0, 100])

    Filename encodes UTC timestamp so consecutive runs don't overwrite.

    Returns:
        Path to the written Parquet file.
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_file = out_path / f"ml_predictions_{ts}.parquet"

    df.to_parquet(out_file, index=False, compression="zstd")

    size_kb = out_file.stat().st_size / 1024
    log.info(
        "Wrote %d enriched rows -> %s (%.1f KB)",
        len(df),
        out_file,
        size_kb,
    )
    return out_file


# =============================================================================
# Main Orchestrator
# =============================================================================

def run_once(model, preprocessor, health_cfg: dict) -> bool:
    """
    Execute one full inference cycle.

    Returns:
        True  if predictions were written successfully.
        False if there was no data to process.

    Raises:
        ValueError / RuntimeError on unrecoverable errors.
    """
    log.info("=" * 60)
    log.info(
        "Starting inference cycle at %s",
        datetime.now(tz=timezone.utc).isoformat(),
    )
    log.info("=" * 60)

    # --- Step 1: Load newest live snapshot ---
    raw_df = load_data(config.INPUT_DIRECTORY)
    if raw_df.empty:
        log.info("No data available — inference cycle skipped.")
        return False

    # --- Step 2: Feature Engineering ---
    engineered_df = engineer_features(raw_df)
    if engineered_df.empty:
        log.warning("Empty DataFrame after feature engineering. Skipping.")
        return False

    # --- Step 3: Preprocess (sklearn ColumnTransformer) ---
    X = preprocess_features(engineered_df, preprocessor)

    # --- Steps 4+5: Predict ---
    predictions, anomaly_scores = predict(
        model, preprocessor, health_cfg, X, engineered_df
    )

    # --- Step 6: Composite Health Score ---
    score_min = health_cfg.get("min_score", -0.5)
    score_max = health_cfg.get("max_score", 0.5)

    health_scores = calculate_health_score(
        df=engineered_df,
        anomaly_scores=pd.Series(anomaly_scores, index=engineered_df.index),
        score_min=score_min,
        score_max=score_max,
        # Use current batch as provisional history for TCC.
        # When a dedicated historical store is available, pass it here.
        historical_df=engineered_df,
    )

    # --- Append output columns to ORIGINAL raw_df (preserves all metadata) ---
    # We use raw_df (not engineered_df) so timestamp/id columns are preserved
    # in their original form for ClickHouse ingestion.
    output_df = raw_df.copy()
    output_df = output_df.loc[engineered_df.index]   # align to rows kept after NaT drops

    # Add uptime_hours and memory_capacity_gb to output schema (like predict.py)
    output_df["uptime_hours"] = engineered_df["uptime_hours"]
    if "memory_capacity_gb" in engineered_df.columns and "memory_capacity_gb" not in output_df.columns:
        output_df["memory_capacity_gb"] = engineered_df["memory_capacity_gb"]

    output_df["prediction"] = predictions       # +1 normal, -1 anomaly
    output_df["anomaly_score"] = anomaly_scores
    output_df["health_score"] = health_scores.round(2)

    log.info(
        "Output columns added: prediction, anomaly_score, health_score. "
        "Total columns: %d",
        len(output_df.columns),
    )

    # --- Step 7: Save ---
    save_predictions(output_df, config.OUTPUT_DIRECTORY)
    return True


def main() -> None:
    """
    Entry point — handles one-shot and watch-mode execution.

    One-shot (default):
        Runs a single inference cycle then exits with code 0 (success)
        or 1 (unrecoverable error).

    Watch mode (ML_WATCH_MODE=true):
        Runs inference repeatedly, sleeping ML_POLL_INTERVAL seconds between
        cycles.  Intended for long-running container deployments where
        live_data_gen.py continuously writes new snapshots.
    """
    parser = argparse.ArgumentParser(
        description="ATLAS ML Inference Pipeline — Isolation Forest anomaly detection"
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        default=config.WATCH_MODE,
        help="Run continuously, polling INPUT_DIRECTORY for new files.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=config.POLL_INTERVAL_SECONDS,
        metavar="SECONDS",
        help="Seconds between polls in watch mode (default: %(default)s).",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  ATLAS ML Inference Pipeline")
    log.info("=" * 60)
    log.info("  Input directory  : %s", config.INPUT_DIRECTORY)
    log.info("  Output directory : %s", config.OUTPUT_DIRECTORY)
    log.info("  Model path       : %s", config.MODEL_PATH)
    log.info("  Preprocessor     : %s", config.PREPROCESSOR_PATH)
    log.info("  Health config    : %s", config.HEALTH_CONFIG_PATH)
    log.info("  Watch mode       : %s", args.watch)
    if args.watch:
        log.info("  Poll interval    : %ds", args.interval)
    log.info("=" * 60)

    # Load all three model artifacts ONCE at startup
    try:
        model, preprocessor, health_cfg = load_models()
    except FileNotFoundError as exc:
        log.error("STARTUP FAILED (model file missing): %s", exc)
        sys.exit(1)
    except RuntimeError as exc:
        log.error("STARTUP FAILED (model deserialisation): %s", exc)
        sys.exit(1)

    if not args.watch:
        # --- One-shot mode ---
        try:
            run_once(model, preprocessor, health_cfg)
        except ValueError as exc:
            log.error("INFERENCE FAILED (data / feature error): %s", exc)
            sys.exit(1)
        except Exception as exc:  # noqa: BLE001
            log.error("INFERENCE FAILED (unexpected): %s", exc, exc_info=True)
            sys.exit(1)
    else:
        # --- Watch / continuous mode ---
        log.info(
            "Watch mode active — polling every %ds. Press Ctrl+C to stop.",
            args.interval,
        )
        try:
            while True:
                try:
                    run_once(model, preprocessor, health_cfg)
                except ValueError as exc:
                    log.error("Inference cycle failed (will retry): %s", exc)
                except Exception as exc:  # noqa: BLE001
                    log.error(
                        "Unexpected error in inference cycle (will retry): %s",
                        exc,
                        exc_info=True,
                    )
                log.info("Sleeping %ds before next cycle...", args.interval)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            log.info("Watch mode stopped by user.")


if __name__ == "__main__":
    main()
