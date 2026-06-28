"""
ATLAS ML Inference Pipeline — Main Entry Point
===============================================
Reads live telemetry Parquet files, runs Isolation Forest inference,
computes a health score, and writes enriched Parquet to the output
directory for downstream ClickHouse ingestion.

Pipeline steps
--------------
1. load_data()          → Read incoming Parquet files with pandas
2. feature_engineering()→ Derive hour_of_day, day_of_week from metric_time
3. prepare_features()   → Extract & validate the FEATURE_COLUMNS matrix
4. load_model()         → Deserialise isolation_forest.pkl (once, at startup)
5. predict()            → model.predict() + model.decision_function()
6. calculate_health_score() → AHC + DHC + TCC → [0, 100]
7. save_predictions()   → Write enriched Parquet to OUTPUT_DIRECTORY

Running
-------
    # One-shot:
    python inference.py

    # Continuous watch mode (polls INPUT_DIRECTORY every 60 s):
    ML_WATCH_MODE=true python inference.py

    # Override paths via environment:
    ML_INPUT_DIR=/data/live ML_OUTPUT_DIR=/data/ml_predictions python inference.py

Owner: Knsrikanta (ML Inference)
"""

import argparse
import logging
import os
import pickle
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd

import config
from health_score import calculate_health_score

# =============================================================================
# Logging Setup
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("atlas.ml.inference")


# =============================================================================
# Step 1 — Load Data
# =============================================================================

def load_data(input_dir: str) -> pd.DataFrame:
    """
    Read all Parquet files from *input_dir* into a single DataFrame.

    Handles:
        - Missing / empty directory  → returns empty DataFrame with a warning
        - Unreadable files           → skips with a warning, continues
        - Empty resulting DataFrame  → returns empty DataFrame with a warning

    Args:
        input_dir: Path to the directory containing live telemetry Parquet files.

    Returns:
        Concatenated DataFrame, or an empty DataFrame on failure.
    """
    data_path = Path(input_dir)

    if not data_path.exists():
        log.warning("Input directory does not exist: %s", input_dir)
        return pd.DataFrame()

    parquet_files = sorted(data_path.rglob("*.parquet"))

    if not parquet_files:
        log.warning("No Parquet files found in: %s", input_dir)
        return pd.DataFrame()

    log.info("Found %d Parquet file(s) in %s", len(parquet_files), input_dir)

    frames: list[pd.DataFrame] = []
    for fp in parquet_files:
        try:
            df = pd.read_parquet(fp)
            frames.append(df)
            log.debug("  Loaded %s — %d rows", fp.name, len(df))
        except Exception as exc:  # noqa: BLE001
            log.warning("  Skipping unreadable file %s: %s", fp.name, exc)

    if not frames:
        log.warning("All Parquet files were unreadable — nothing to process.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    log.info("Total rows loaded: %d", len(combined))

    if combined.empty:
        log.warning("Combined DataFrame is empty after loading all files.")

    return combined


# =============================================================================
# Step 2 — Feature Engineering
# =============================================================================

def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive temporal features from *metric_time* and add them as new columns.

    New columns added (originals are NOT modified):
        hour_of_day  : int [0, 23]
        day_of_week  : int [0 (Monday) … 6 (Sunday)]

    Handles:
        - Missing metric_time column → raises ValueError
        - Unparseable timestamps     → rows set to NaT, then dropped with warning
        - Already datetime column    → used directly (no re-parse overhead)

    Args:
        df: Raw telemetry DataFrame.

    Returns:
        DataFrame with hour_of_day and day_of_week columns appended.
        Original columns are unchanged.
    """
    if "metric_time" not in df.columns:
        raise ValueError(
            "Required column 'metric_time' not found in the DataFrame. "
            "Check that the live data generator is producing this column."
        )

    df = df.copy()

    # Parse timestamp safely — coerce bad values to NaT
    if not pd.api.types.is_datetime64_any_dtype(df["metric_time"]):
        df["metric_time"] = pd.to_datetime(df["metric_time"], errors="coerce", utc=True)

    nat_count = df["metric_time"].isna().sum()
    if nat_count > 0:
        log.warning(
            "Dropping %d row(s) with invalid / unparseable metric_time.", nat_count
        )
        df = df.dropna(subset=["metric_time"])

    if df.empty:
        log.warning("DataFrame is empty after dropping invalid timestamps.")
        return df

    df["hour_of_day"] = df["metric_time"].dt.hour.astype(int)
    df["day_of_week"] = df["metric_time"].dt.dayofweek.astype(int)  # 0=Mon … 6=Sun

    log.info(
        "Feature engineering complete — hour_of_day and day_of_week added. "
        "Rows remaining: %d",
        len(df),
    )
    return df


# =============================================================================
# Step 3 — Prepare Feature Matrix
# =============================================================================

def prepare_features(df: pd.DataFrame) -> np.ndarray:
    """
    Extract the FEATURE_COLUMNS subset and return a numpy matrix for inference.

    Validates:
        - All FEATURE_COLUMNS exist in df (raises ValueError listing missing ones)
        - No NaN values in the feature matrix (fills with column median, logs warning)

    Args:
        df: DataFrame after feature_engineering().

    Returns:
        2D numpy array of shape (n_rows, n_features), dtype float64.

    Raises:
        ValueError: If any FEATURE_COLUMNS are absent from df.
    """
    missing_cols = [c for c in config.FEATURE_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Feature mismatch — the following columns required by FEATURE_COLUMNS "
            f"are not present in the data:\n  {missing_cols}\n"
            f"Either update FEATURE_COLUMNS in config.py or check that the upstream "
            f"data generator produces these columns."
        )

    X = df[config.FEATURE_COLUMNS].copy()

    nan_counts = X.isna().sum()
    if nan_counts.any():
        log.warning(
            "NaN values found in feature matrix — filling with column medians:\n%s",
            nan_counts[nan_counts > 0].to_string(),
        )
        X = X.fillna(X.median())

    log.info(
        "Feature matrix prepared: %d rows × %d features — %s",
        len(X),
        len(config.FEATURE_COLUMNS),
        config.FEATURE_COLUMNS,
    )
    return X.to_numpy(dtype=np.float64)


# =============================================================================
# Step 4 — Load Model (once)
# =============================================================================

def load_model(model_path: str):
    """
    Deserialise the trained Isolation Forest model from disk.

    The model is loaded ONCE at startup and kept in memory for the lifetime
    of the process.  Do NOT call this inside a loop.

    Args:
        model_path: Absolute path to isolation_forest.pkl.

    Returns:
        Fitted sklearn IsolationForest instance.

    Raises:
        FileNotFoundError: If the model file does not exist.
        RuntimeError:      If the file exists but cannot be unpickled.
    """
    mp = Path(model_path)

    if not mp.exists():
        raise FileNotFoundError(
            f"Model file not found: {model_path}\n"
            f"Place the trained isolation_forest.pkl at this path before running."
        )

    try:
        with mp.open("rb") as fh:
            model = pickle.load(fh)  # noqa: S301 — trusted internal artifact
        log.info("Model loaded from: %s", model_path)
        return model
    except Exception as exc:
        raise RuntimeError(
            f"Failed to deserialise model at {model_path}: {exc}"
        ) from exc


# =============================================================================
# Step 5 — Run Inference
# =============================================================================

def predict(model, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Run Isolation Forest inference on the feature matrix.

    Output:
        predictions   : array of int  → +1 (normal) or -1 (anomaly)
        anomaly_scores: array of float → raw decision_function scores
                        (more negative = more anomalous)

    Args:
        model: Fitted IsolationForest instance (from load_model()).
        X:     Feature matrix from prepare_features().

    Returns:
        Tuple of (predictions, anomaly_scores), both shape (n_rows,).

    Raises:
        ValueError: If the model's expected feature count doesn't match X.
    """
    # Validate feature count against model expectation
    expected_n_features = getattr(model, "n_features_in_", None)
    if expected_n_features is not None and X.shape[1] != expected_n_features:
        raise ValueError(
            f"Feature mismatch: model expects {expected_n_features} features "
            f"but the data has {X.shape[1]}. "
            f"Update FEATURE_COLUMNS in config.py to match the training pipeline."
        )

    log.info("Running inference on %d rows...", len(X))
    predictions = model.predict(X)      # +1 = normal, -1 = anomaly
    anomaly_scores = model.decision_function(X)  # higher = more normal

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

    The filename encodes the current UTC timestamp so consecutive runs
    do not overwrite each other.

    Original telemetry columns are preserved.  Three new columns are appended:
        prediction    : int  (+1 = normal, -1 = anomaly)
        anomaly_score : float (raw decision_function output)
        health_score  : float (composite score in [0, 100])

    Args:
        df:         Enriched DataFrame (must contain the three new columns).
        output_dir: Directory path where the Parquet file will be written.

    Returns:
        Path to the written Parquet file.

    Raises:
        OSError: If the output directory cannot be created or the file cannot
                 be written.
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_file = out_path / f"ml_predictions_{ts}.parquet"

    df.to_parquet(out_file, index=False, compression="zstd")

    log.info(
        "Wrote %d enriched rows → %s (%.1f KB)",
        len(df),
        out_file,
        out_file.stat().st_size / 1024,
    )
    return out_file


# =============================================================================
# Main Orchestrator
# =============================================================================

def run_once(model) -> bool:
    """
    Execute one full inference cycle (load → engineer → predict → score → save).

    Args:
        model: Pre-loaded IsolationForest (from load_model() called at startup).

    Returns:
        True  if predictions were written successfully.
        False if there was no data to process (not an error).

    Raises:
        ValueError / RuntimeError on unrecoverable errors (feature mismatch, etc.)
    """
    log.info("=" * 60)
    log.info("Starting inference cycle at %s", datetime.now(tz=timezone.utc).isoformat())
    log.info("=" * 60)

    # --- Step 1: Load ---
    df = load_data(config.INPUT_DIRECTORY)
    if df.empty:
        log.info("No data available. Inference cycle skipped.")
        return False

    # --- Step 2: Feature Engineering ---
    df = feature_engineering(df)
    if df.empty:
        log.warning("Empty DataFrame after feature engineering. Skipping.")
        return False

    # --- Step 3: Prepare Feature Matrix ---
    X = prepare_features(df)

    # --- Steps 4+5: Predict (model already loaded) ---
    predictions, anomaly_scores = predict(model, X)

    # --- Step 6: Health Score ---
    # Pass the current batch as the historical reference for TCC.
    # When a dedicated historical dataset is available, load it here
    # and pass it as historical_df instead.
    health_scores = calculate_health_score(
        df=df,
        anomaly_scores=pd.Series(anomaly_scores, index=df.index),
        historical_df=df,   # Using current batch as provisional history
        metric_col="MetricValue",
    )

    # --- Append output columns ---
    df = df.copy()
    df["prediction"] = predictions       # +1 normal, -1 anomaly
    df["anomaly_score"] = anomaly_scores
    df["health_score"] = health_scores

    log.info(
        "Output columns added: prediction, anomaly_score, health_score. "
        "Total columns: %d",
        len(df.columns),
    )

    # --- Step 7: Save ---
    save_predictions(df, config.OUTPUT_DIRECTORY)
    return True


def main() -> None:
    """
    Entry point — handles one-shot and watch-mode execution.

    One-shot (default):
        Runs a single inference cycle then exits with code 0 (success)
        or 1 (unrecoverable error).

    Watch mode (ML_WATCH_MODE=true):
        Runs inference repeatedly, sleeping ML_POLL_INTERVAL seconds between
        cycles.  Intended for long-running container deployments.
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

    log.info("━" * 60)
    log.info("  ATLAS ML Inference Pipeline")
    log.info("━" * 60)
    log.info("  Input directory : %s", config.INPUT_DIRECTORY)
    log.info("  Output directory: %s", config.OUTPUT_DIRECTORY)
    log.info("  Model path      : %s", config.MODEL_PATH)
    log.info("  Feature columns : %s", config.FEATURE_COLUMNS)
    log.info("  Watch mode      : %s", args.watch)
    if args.watch:
        log.info("  Poll interval   : %ds", args.interval)
    log.info("━" * 60)

    # Load model ONCE at startup — kept in memory for all cycles
    try:
        model = load_model(config.MODEL_PATH)
    except FileNotFoundError as exc:
        log.error("STARTUP FAILED: %s", exc)
        sys.exit(1)
    except RuntimeError as exc:
        log.error("STARTUP FAILED (model deserialisation): %s", exc)
        sys.exit(1)

    if not args.watch:
        # --- One-shot mode ---
        try:
            run_once(model)
        except ValueError as exc:
            log.error("INFERENCE FAILED (data / feature error): %s", exc)
            sys.exit(1)
        except Exception as exc:  # noqa: BLE001
            log.error("INFERENCE FAILED (unexpected): %s", exc, exc_info=True)
            sys.exit(1)
    else:
        # --- Watch / continuous mode ---
        log.info("Watch mode active — polling every %ds. Press Ctrl+C to stop.", args.interval)
        try:
            while True:
                try:
                    run_once(model)
                except ValueError as exc:
                    # Feature / data errors are logged but don't crash the loop
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
