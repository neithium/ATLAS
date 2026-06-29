"""
ATLAS ML Predictions Loader — Inference Parquet → ClickHouse
==============================================================
Reads Parquet files output by the Isolation Forest inference pipeline
and batch-inserts them into ClickHouse atlas.telemetry_ml_predictions.

Architecture:
    This loader mirrors the existing delta_loader.py pattern but is purpose-
    built for the ML inference output path. It reads schema-compliant Parquet
    files, applies type conversions, and inserts into ClickHouse in configurable
    batches.

Scheduler Mode (ML_SCHEDULE_INTERVAL_SECONDS):
    0    → One-shot: run once and exit.
    300  → Demo: poll every 5 minutes.
    3600 → Production: poll every 1 hour (matches upstream batch cadence).

Owner : Varna (ML Storage Layer)
Reads : /data/ml_predictions (Inference Layer output) — READ-ONLY
Writes: ClickHouse atlas.telemetry_ml_predictions

Credentials:
    All DB credentials are read from environment variables.
    In Docker they flow from .env → docker-compose env_file →
    container environment → this loader.

    CLICKHOUSE_HOST                default: 127.0.0.1
    CLICKHOUSE_PORT                default: 8123
    CLICKHOUSE_USER                default: atlas
    CLICKHOUSE_PASSWORD            default: atlas_secure_pwd
    ML_PREDICTIONS_PATH            default: /data/ml_predictions
    ML_BATCH_SIZE                  default: 10000
    ML_SCHEDULE_INTERVAL_SECONDS   default: 0 (one-shot)
"""

import os
import time
import logging
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import clickhouse_connect

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("atlas.ml_loader")

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
CH_HOST = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "atlas")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "atlas_secure_pwd")

ML_PREDICTIONS_PATH = os.getenv("ML_PREDICTIONS_PATH", "/data/ml_predictions")
BATCH_SIZE = int(os.getenv("ML_BATCH_SIZE", "10000"))
SCHEDULE_INTERVAL = int(os.getenv("ML_SCHEDULE_INTERVAL_SECONDS", "0"))

# ---------------------------------------------------------------------------
# Column order matching the ClickHouse telemetry_ml_predictions table
# Must match init.sql definition exactly (21 columns, excluding insertion_time)
# ---------------------------------------------------------------------------
CH_ML_COLUMNS = [
    "device_id",
    "server_name",
    "tags",
    "location_name",
    "metric_time",
    "avg_metric_value",
    "cpu_utilization",
    "memory_utilization",
    "disk_utilization",
    "network_throughput",
    "cpu_temperature",
    "amb_temp",
    "fan_speed_rpm",
    "gpu_utilization",
    "uptime_hours",
    "processor_vendor",
    "server_generation",
    "memory_capacity_gb",
    "prediction",
    "anomaly_score",
    "health_score",
    "health_status",
    # insertion_time is DEFAULT now(), not supplied
]


# =========================================================================
# Parquet reading
# =========================================================================
def read_ml_parquet(path: str) -> pd.DataFrame:
    """
    Read inference Parquet files from the ML predictions output directory.

    Scans recursively for .parquet files, concatenates them into a single
    DataFrame, and validates column presence against the ClickHouse schema.
    """
    predictions_dir = Path(path)
    if not predictions_dir.exists():
        log.warning("ML predictions path does not exist: %s", path)
        return pd.DataFrame()

    parquet_files = sorted(
        str(f) for f in predictions_dir.rglob("*.parquet")
        if not f.name.startswith(".")
    )

    if not parquet_files:
        log.info("No parquet files found in %s", path)
        return pd.DataFrame()

    log.info("Found %d parquet file(s) in %s", len(parquet_files), path)

    dfs = []
    for pf in parquet_files:
        try:
            df = pq.read_table(pf).to_pandas()
            if "prediction" not in df.columns:
                log.warning("Skipping file %s: Missing 'prediction' column (likely not an ML output file)", pf)
                continue
            dfs.append(df)
            log.info("  Read %d rows from %s", len(df), Path(pf).name)
        except Exception as e:
            log.error("  Failed to read %s: %s", pf, e)

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    log.info("Total rows read: %d", len(combined))

    # Log actual columns from the parquet for debugging
    log.info(
        "Parquet columns (%d): %s",
        len(combined.columns),
        sorted(combined.columns.tolist()),
    )

    # Check for missing columns expected by ClickHouse
    missing = [c for c in CH_ML_COLUMNS if c not in combined.columns]
    if missing:
        log.warning("Missing columns in parquet (will be filled with defaults): %s", missing)
        for col_name in missing:
            combined[col_name] = None

    # Check for extra columns not in CH_ML_COLUMNS (informational)
    extra = [c for c in combined.columns if c not in CH_ML_COLUMNS and c != "insertion_time"]
    if extra:
        log.info("Extra columns in parquet (ignored by loader): %s", extra)

    return combined


# =========================================================================
# Type conversions
# =========================================================================
def prepare_for_clickhouse(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert DataFrame types to match ClickHouse schema expectations.

    Key conversions:
        metric_time  : str   → datetime (DateTime64)
        uptime_hours : any   → int (UInt32)
        prediction   : any   → int (Int8)
        health_score : any   → int (UInt8, clipped 0-255)
        floats       : NaN   → 0.0
        strings      : NaN   → empty string

    Follows the same pattern as the existing delta_loader.py.
    """
    df = df.copy()

    # metric_time: ISO-8601 string → datetime
    if "metric_time" in df.columns:
        df["metric_time"] = pd.to_datetime(df["metric_time"], utc=True, errors="coerce")
        nat_count = df["metric_time"].isna().sum()
        if nat_count > 0:
            log.warning(
                "Found %d rows with unparseable metric_time — filling with epoch",
                nat_count,
            )
        epoch = pd.Timestamp("1970-01-01", tz="UTC")
        df["metric_time"] = df["metric_time"].fillna(epoch)

    # Integer columns
    if "uptime_hours" in df.columns:
        df["uptime_hours"] = (
            pd.to_numeric(df["uptime_hours"], errors="coerce").fillna(0).astype(int)
        )

    # prediction: Int8 (1 = Normal, -1 = Anomaly)
    if "prediction" in df.columns:
        df["prediction"] = (
            pd.to_numeric(df["prediction"], errors="coerce").fillna(1).astype(int)
        )

    # health_score: UInt8 (0-100, clipped to 0-255 for safety)
    if "health_score" in df.columns:
        df["health_score"] = (
            pd.to_numeric(df["health_score"], errors="coerce")
            .fillna(0)
            .astype(int)
            .clip(0, 255)
        )

    # Float columns — fill NaNs with 0.0
    float_cols = [
        "avg_metric_value",
        "cpu_utilization",
        "memory_utilization",
        "disk_utilization",
        "network_throughput",
        "cpu_temperature",
        "amb_temp",
        "fan_speed_rpm",
        "gpu_utilization",
        "memory_capacity_gb",
        "anomaly_score",
    ]
    for col_name in float_cols:
        if col_name in df.columns:
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce").fillna(0.0)

    # String columns (non-nullable in ClickHouse)
    str_cols = [
        "device_id",
        "server_name",
        "tags",
        "location_name",
        "processor_vendor",
        "server_generation",
    ]
    for col_name in str_cols:
        if col_name in df.columns:
            df[col_name] = df[col_name].apply(lambda x: "" if pd.isna(x) else str(x))

    return df


# =========================================================================
# ClickHouse insertion
# =========================================================================
def insert_into_clickhouse(ch_client, df: pd.DataFrame) -> int:
    """
    Insert DataFrame rows into atlas.telemetry_ml_predictions in batches.

    Uses insert_df() for direct columnar binary serialization (no Python
    row-level iteration). Falls back to list-based insert() if the driver
    version is incompatible with insert_df() for this table schema.

    Returns total rows inserted.
    """
    if df.empty:
        log.info("No rows to insert — skipping ClickHouse write")
        return 0

    # Reorder columns to match the table definition
    insert_cols = [c for c in CH_ML_COLUMNS if c in df.columns]
    df_ordered = df[insert_cols]

    total = len(df_ordered)
    inserted = 0

    for start in range(0, total, BATCH_SIZE):
        batch = df_ordered.iloc[start : start + BATCH_SIZE].copy()
        try:
            # Fast path: direct columnar binary insert (no Python row loop)
            ch_client.insert_df(
                table="atlas.telemetry_ml_predictions",
                df=batch,
                column_names=insert_cols,
            )
        except TypeError:
            # Fallback for older clickhouse-connect versions that have
            # ColumnDef parsing issues with DESCRIBE TABLE output
            log.warning("insert_df() failed — falling back to list-based insert")
            data = [
                [_native_val(v) for v in row] for row in batch.values.tolist()
            ]
            ch_client.insert(
                table="atlas.telemetry_ml_predictions",
                data=data,
                column_names=insert_cols,
            )
        inserted += len(batch)
        log.info("  Inserted batch %d–%d / %d", start + 1, start + len(batch), total)

    return inserted


def _native_val(val):
    """Convert numpy / pandas scalars to plain Python types.

    Only used as a fallback when insert_df() is unavailable.
    """
    if val is None:
        return None
    if hasattr(val, "item"):
        return val.item()
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    return val


# =========================================================================
# Main
# =========================================================================
def main():
    log.info("=" * 60)
    log.info("ATLAS ML Loader — Inference Parquet → ClickHouse")
    log.info("=" * 60)
    log.info("ClickHouse  : %s:%s  user=%s", CH_HOST, CH_PORT, CH_USER)
    log.info("Predictions : %s", ML_PREDICTIONS_PATH)
    log.info("Batch size  : %d", BATCH_SIZE)
    log.info(
        "Scheduler   : %s",
        f"every {SCHEDULE_INTERVAL}s" if SCHEDULE_INTERVAL > 0 else "one-shot",
    )

    # --- Check predictions path exists before connecting to DB ---------------
    predictions_dir = Path(ML_PREDICTIONS_PATH)
    if not predictions_dir.exists():
        log.warning("ML predictions path does not exist yet: %s", ML_PREDICTIONS_PATH)
        log.warning("Inference layer may not have produced data. Nothing to load.")
        return

    parquet_count = sum(
        1
        for f in predictions_dir.rglob("*.parquet")
        if not f.name.startswith(".")
    )
    if parquet_count == 0:
        log.warning("No parquet files found in %s — nothing to load.", ML_PREDICTIONS_PATH)
        return
    log.info("Found %d parquet file(s) in predictions path", parquet_count)

    # --- Connect to ClickHouse -----------------------------------------------
    ch_client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASS,
    )
    log.info("Connected to ClickHouse (server version %s)", ch_client.server_version)

    # --- Verify table exists -------------------------------------------------
    try:
        count = ch_client.command("SELECT count() FROM atlas.telemetry_ml_predictions")
        log.info(
            "Verified: atlas.telemetry_ml_predictions accessible (%s existing rows)",
            count,
        )
    except Exception as exc:
        log.error("atlas.telemetry_ml_predictions not accessible: %s", exc)
        log.error("Ensure init.sql has been executed with the ML table DDL.")
        ch_client.close()
        raise

    # --- Read inference Parquet files ----------------------------------------
    df = read_ml_parquet(ML_PREDICTIONS_PATH)
    if df.empty:
        log.info("Nothing to load. Exiting.")
        ch_client.close()
        return

    # --- Prepare types for ClickHouse ----------------------------------------
    df = prepare_for_clickhouse(df)

    # --- Insert into ClickHouse ----------------------------------------------
    try:
        rows_inserted = insert_into_clickhouse(ch_client, df)
        log.info("Inserted %d rows into atlas.telemetry_ml_predictions", rows_inserted)
    except Exception as exc:
        log.error("ClickHouse insert failed: %s", exc)
        log.error("Column dtypes:")
        for col_name in df.columns:
            nan_count = df[col_name].isna().sum()
            sample = df[col_name].iloc[0] if len(df) > 0 else "N/A"
            log.error(
                "  %-25s dtype=%-15s NaNs=%-6d sample=%s (type=%s)",
                col_name,
                str(df[col_name].dtype),
                nan_count,
                repr(sample),
                type(sample).__name__,
            )
        ch_client.close()
        raise

    # --- Summary -------------------------------------------------------------
    anomaly_count = (
        len(df[df["prediction"] == -1]) if "prediction" in df.columns else 0
    )
    device_count = df["device_id"].nunique() if "device_id" in df.columns else 0

    log.info("-" * 60)
    log.info("ML LOAD SUMMARY")
    log.info("  Rows inserted       : %d", rows_inserted)
    log.info("  Anomalies detected  : %d", anomaly_count)
    log.info("  Unique devices      : %d", device_count)
    if "health_score" in df.columns:
        log.info("  Avg health score    : %.1f", df["health_score"].mean())
        critical = len(df[df["health_score"] < 50])
        if critical > 0:
            log.warning("  ⚠ Critical devices  : %d (health_score < 50)", critical)
    log.info("-" * 60)

    # --- Cleanup -------------------------------------------------------------
    try:
        deleted_count = 0
        for pf in Path(ML_PREDICTIONS_PATH).rglob("*.parquet"):
            if not pf.name.startswith("."):
                os.remove(pf)
                deleted_count += 1
        log.info("Deleted %d processed parquet files.", deleted_count)
    except Exception as e:
        log.warning("Failed to delete processed parquet files: %s", e)
        
    ch_client.close()
    log.info("Done.")


if __name__ == "__main__":
    if SCHEDULE_INTERVAL > 0:
        log.info("=" * 60)
        log.info(
            "ML LOADER PERSISTENT SCHEDULER — interval: %ds (%d min)",
            SCHEDULE_INTERVAL,
            SCHEDULE_INTERVAL // 60,
        )
        log.info("=" * 60)
        cycle = 0
        while True:
            cycle += 1
            log.info("--- ML Loader cycle %d ---", cycle)
            try:
                main()
            except Exception as exc:
                log.error("ML Loader cycle %d failed: %s", cycle, exc)
                log.error("Will retry in %ds...", SCHEDULE_INTERVAL)
            log.info("Sleeping %ds until next run...", SCHEDULE_INTERVAL)
            time.sleep(SCHEDULE_INTERVAL)
    else:
        log.info("One-shot mode (ML_SCHEDULE_INTERVAL_SECONDS=0)")
        main()
