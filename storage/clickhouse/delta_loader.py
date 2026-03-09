"""
ATLAS Analytics Layer — Delta Lake → ClickHouse Loader
=======================================================
Reads deduplicated Parquet data from the Delta Lake Refined Layer
and batch-inserts it into ClickHouse atlas.telemetry_refined.

Materialized Views (hourly_mv, daily_mv) fire automatically on insert.

Supports incremental loading via a PostgreSQL watermark table so that
re-runs only process new data.

Supports persistent scheduler mode via SCHEDULE_INTERVAL_SECONDS env var.
When set to 0 (default), runs once and exits. When > 0, loops forever.

Partition-Aware Reading:
    The Refined Layer uses a 5-level Hive partition scheme:
        report_type / partition_date / platform_customer_id /
        application_customer_id / device_id
    When a watermark exists, the loader prunes partitions by partition_date
    so only new/updated date directories are scanned.

Owner : Varna (Analytics Layer)
Reads : refined-volume (Lakehouse Refined Layer) — READ-ONLY
Writes: ClickHouse atlas.* tables, PostgreSQL metadata tables

Environment Variables (set via .env.example + docker-compose overrides):
    CLICKHOUSE_HOST          default: 127.0.0.1
    CLICKHOUSE_PORT          default: 8123
    CLICKHOUSE_USER          default: atlas
    CLICKHOUSE_PASSWORD      default: atlas_secure_pwd
    POSTGRES_HOST            default: 127.0.0.1
    POSTGRES_PORT            default: 5432
    POSTGRES_USER            default: atlas
    POSTGRES_PASSWORD        default: atlas_secure_pwd
    POSTGRES_DB              default: atlas_metadata
    REFINED_DATA_PATH        default: /data/refined
    BATCH_SIZE               default: 10000
    SCHEDULE_INTERVAL_SECONDS    default: 0 (one-shot)
"""

import os
import sys
import time
import uuid
import logging
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq
import clickhouse_connect
import psycopg

try:
    from deltalake import DeltaTable
    HAS_DELTALAKE = True
except ImportError:
    HAS_DELTALAKE = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("atlas.delta_loader")

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
CH_HOST = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "atlas")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "atlas_secure_pwd")

PG_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "atlas")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "atlas_secure_pwd")
PG_DB = os.getenv("POSTGRES_DB", "atlas_metadata")

REFINED_PATH = os.getenv("REFINED_DATA_PATH", "/data/refined")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))
SCHEDULE_INTERVAL = int(os.getenv("SCHEDULE_INTERVAL_SECONDS", "0"))
WATERMARK_SOURCE = "delta_refined"

# ---------------------------------------------------------------------------
# Column order matching the ClickHouse telemetry_refined table definition
# Must match output_schema.py exactly (36 columns)
# ---------------------------------------------------------------------------
CH_COLUMNS = [
    "report_id",
    "device_id",
    "application_customer_id",
    "platform_customer_id",
    "status",
    "report_type",
    "error_reason",
    "MetricValue",
    "model",
    "tags",
    "location_state",
    "location_country",
    "processor_vendor",
    "server_generation",
    "location_id",
    "location_name",
    "location_city",
    "server_name",
    "metric_id",
    "cpu_inventory",
    "memory_inventory",
    "pcie_devices_count",
    "socket_count",
    "avg_metric_value",
    "max_metric_value",
    "min_metric_value",
    "metric_time",
    "datetime",
    "timeRangeEnd",
    "amb_temp",
    "Insertiontime",
    # insertion_time is DEFAULT now64(3), not supplied
    "co2_factor",
    "energy_cost_factor",
    "max_metric_time",
    "location_date",
    "inventory_date",
]


# =========================================================================
# PostgreSQL helpers
# =========================================================================
def pg_connect():
    """Return a psycopg connection to the metadata database."""
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        dbname=PG_DB,
    )


def get_watermark(pg_conn) -> Optional[str]:
    """Return the last loaded metric_time ISO string, or None."""
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT last_metric_time FROM data_load_watermarks WHERE source = %s",
            (WATERMARK_SOURCE,),
        )
        row = cur.fetchone()
        return row[0].isoformat() if row and row[0] else None


def update_watermark(pg_conn, last_metric_time: str, rows_loaded: int):
    """Upsert the watermark row."""
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_load_watermarks (source, last_metric_time, last_loaded_at, rows_loaded)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (source) DO UPDATE
                SET last_metric_time = EXCLUDED.last_metric_time,
                    last_loaded_at   = EXCLUDED.last_loaded_at,
                    rows_loaded      = data_load_watermarks.rows_loaded + EXCLUDED.rows_loaded
            """,
            (WATERMARK_SOURCE, last_metric_time, datetime.now(timezone.utc), rows_loaded),
        )
    pg_conn.commit()


def log_pipeline_run(pg_conn, status: str, records_processed: int, error_message: Optional[str] = None):
    """Insert a row into pipeline_runs for audit."""
    run_id = f"load-{uuid.uuid4().hex[:12]}"
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (run_id, pipeline_name, status, records_processed, started_at, completed_at, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                "delta_to_clickhouse",
                status,
                records_processed,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc) if status != "running" else None,
                error_message,
            ),
        )
    pg_conn.commit()
    return run_id


def upsert_device_registry(pg_conn, df: pd.DataFrame):
    """Upsert unique devices into the device_registry table."""
    device_cols = [
        "device_id", "platform_customer_id", "application_customer_id",
        "server_name", "model", "processor_vendor", "server_generation", "socket_count",
    ]
    # Only process columns that exist in the DataFrame
    available_cols = [c for c in device_cols if c in df.columns]
    if not all(c in available_cols for c in ["device_id", "platform_customer_id", "application_customer_id"]):
        log.warning("Missing key device columns, skipping device registry upsert")
        return

    devices = df[available_cols].drop_duplicates(subset=["device_id", "platform_customer_id", "application_customer_id"])
    with pg_conn.cursor() as cur:
        for _, row in devices.iterrows():
            cur.execute(
                """
                INSERT INTO device_registry (device_id, platform_customer_id, application_customer_id,
                                             server_name, model, processor_vendor, server_generation, socket_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (device_id, platform_customer_id, application_customer_id) DO UPDATE
                    SET server_name       = EXCLUDED.server_name,
                        model             = EXCLUDED.model,
                        processor_vendor  = EXCLUDED.processor_vendor,
                        server_generation = EXCLUDED.server_generation,
                        socket_count      = EXCLUDED.socket_count,
                        updated_at        = CURRENT_TIMESTAMP
                """,
                (
                    row.get("device_id"), row.get("platform_customer_id"), row.get("application_customer_id"),
                    row.get("server_name", ""), row.get("model", ""), row.get("processor_vendor", ""),
                    row.get("server_generation", ""),
                    int(row["socket_count"]) if "socket_count" in row and pd.notna(row["socket_count"]) else None,
                ),
            )
    pg_conn.commit()
    log.info("Upserted %d device(s) into device_registry", len(devices))


def upsert_location_registry(pg_conn, df: pd.DataFrame):
    """Upsert unique locations into the location_registry table."""
    loc_cols = ["location_id", "location_name", "location_city", "location_state", "location_country"]
    available_cols = [c for c in loc_cols if c in df.columns]
    if "location_id" not in available_cols:
        log.warning("Missing location_id column, skipping location registry upsert")
        return

    locations = df[available_cols].drop_duplicates(subset=["location_id"])
    with pg_conn.cursor() as cur:
        for _, row in locations.iterrows():
            cur.execute(
                """
                INSERT INTO location_registry (location_id, location_name, location_city, location_state, location_country)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (location_id) DO UPDATE
                    SET location_name    = EXCLUDED.location_name,
                        location_city    = EXCLUDED.location_city,
                        location_state   = EXCLUDED.location_state,
                        location_country = EXCLUDED.location_country
                """,
                (
                    row.get("location_id"), row.get("location_name", ""),
                    row.get("location_city", ""), row.get("location_state", ""),
                    row.get("location_country", ""),
                ),
            )
    pg_conn.commit()
    log.info("Upserted %d location(s) into location_registry", len(locations))


# =========================================================================
# Partition-aware data reading
# =========================================================================
def _compute_partition_cutoff(watermark: Optional[str]) -> Optional[str]:
    """
    Convert a watermark (metric_time ISO string) to a partition_date cutoff
    string (YYYY-MM-DD) for Hive partition pruning.

    The Refined Layer uses 5-level partitions:
        report_type / partition_date / platform_customer_id /
        application_customer_id / device_id

    By filtering on partition_date >= cutoff, we skip scanning old date
    directories entirely.
    """
    if not watermark:
        return None
    try:
        cutoff = pd.to_datetime(watermark).strftime("%Y-%m-%d")
        log.info("Partition pruning: will scan partition_date >= %s", cutoff)
        return cutoff
    except Exception:
        log.warning("Could not parse watermark '%s' for partition pruning, reading all partitions", watermark)
        return None


def read_refined_parquet(path: str, watermark: Optional[str] = None) -> pd.DataFrame:
    """
    Read data from the refined Delta Lake path.

    Uses partition_date pruning when a watermark exists to avoid scanning
    old partitions. Prefers the deltalake (delta-rs) library which reads the
    Delta transaction log correctly. Falls back to pyarrow if unavailable.
    """
    refined = Path(path)
    if not refined.exists():
        log.error("Refined data path does not exist: %s", path)
        return pd.DataFrame()

    partition_cutoff = _compute_partition_cutoff(watermark)

    if HAS_DELTALAKE:
        log.info("Reading Delta table via deltalake (delta-rs)...")
        try:
            dt = DeltaTable(str(refined))
            ds = dt.to_pyarrow_dataset()

            if partition_cutoff:
                import pyarrow.compute as pc
                table = ds.to_table(
                    filter=pc.field("partition_date") >= partition_cutoff
                )
                log.info(
                    "Read %d rows from Delta table (version %d) with partition_date >= %s",
                    len(table), dt.version(), partition_cutoff,
                )
            else:
                table = ds.to_table()
                log.info("Read %d rows from Delta table (version %d)", len(table), dt.version())

            df = table.to_pandas()
        except Exception as exc:
            log.warning("deltalake read failed (%s), falling back to pyarrow", exc)
            df = _read_parquet_fallback(refined, partition_cutoff)
    else:
        log.info("deltalake not installed — using pyarrow fallback")
        df = _read_parquet_fallback(refined, partition_cutoff)

    if df.empty:
        return df

    # ---- Log actual columns from the parquet for debugging ----
    log.info("Parquet columns found (%d): %s", len(df.columns), sorted(df.columns.tolist()))

    # ---- Check for missing columns expected by ClickHouse ----
    missing = [c for c in CH_COLUMNS if c not in df.columns]
    if missing:
        log.warning("Missing columns in parquet (will be filled with defaults): %s", missing)
        for col in missing:
            df[col] = None  # Handled in prepare_for_clickhouse

    # ---- Check for extra columns not in CH_COLUMNS (informational) ----
    extra = [c for c in df.columns if c not in CH_COLUMNS and c != "insertion_time"]
    if extra:
        log.info("Extra columns in parquet (ignored by loader): %s", extra)

    return df


def _read_parquet_fallback(refined: Path, partition_cutoff: Optional[str] = None) -> pd.DataFrame:
    """
    Fallback: read Parquet files using pyarrow.dataset with Hive partitioning
    so that partition columns (report_type, partition_date, etc.) are materialised.
    Applies partition_date filter if a cutoff is provided.
    NOTE: this reads ALL parquet files and may include superseded Delta rows.
    """
    import pyarrow.dataset as pads

    try:
        dataset = pads.dataset(
            str(refined), format="parquet", partitioning="hive"
        )

        if partition_cutoff:
            import pyarrow.compute as pc
            table = dataset.to_table(
                filter=pc.field("partition_date") >= partition_cutoff
            )
            log.info(
                "Read %d rows via pyarrow.dataset with partition_date >= %s",
                len(table), partition_cutoff,
            )
        else:
            table = dataset.to_table()
            log.info("Read %d rows via pyarrow.dataset (Hive partitioning)", len(table))

        df = table.to_pandas()
    except Exception:
        # Last resort: direct parquet file read (no partition columns)
        parquet_files = sorted(
            str(f)
            for f in refined.rglob("*.parquet")
            if "_delta_log" not in str(f) and not f.name.startswith(".")
        )
        if not parquet_files:
            log.warning("No parquet files found in %s", refined)
            return pd.DataFrame()
        log.info("Found %d parquet file(s) in %s", len(parquet_files), refined)
        df = pq.read_table(parquet_files).to_pandas()
        log.info("Read %d total rows from parquet files", len(df))

    return df


# =========================================================================
# Type conversions
# =========================================================================
def prepare_for_clickhouse(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert DataFrame types to match ClickHouse schema expectations.

    Key conversions:
        status       : bool  → int (UInt8)
        metric_time  : str   → datetime (DateTime64)
        location_date: any   → string (kept as String per output_schema.py)
        NULLs        : fill defaults for non-nullable columns
        error_reason : NaN   → Python None (Nullable(String))
    """
    df = df.copy()

    # Boolean → UInt8
    if "status" in df.columns:
        df["status"] = df["status"].apply(lambda x: int(bool(x)) if pd.notna(x) else 0)

    # metric_time: ISO-8601 string → datetime
    if "metric_time" in df.columns:
        df["metric_time"] = pd.to_datetime(df["metric_time"], utc=True, errors="coerce")
        nat_count = df["metric_time"].isna().sum()
        if nat_count > 0:
            log.warning("Found %d rows with unparseable metric_time — filling with epoch", nat_count)
        epoch = pd.Timestamp("1970-01-01", tz="UTC")
        df["metric_time"] = df["metric_time"].fillna(epoch)

    # Ensure integer columns (native Python int)
    for col in ("pcie_devices_count", "socket_count"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Fill NaNs in float columns with 0.0
    float_cols = [
        "MetricValue", "avg_metric_value", "max_metric_value", "min_metric_value",
        "amb_temp", "Insertiontime", "datetime", "timeRangeEnd",
        "co2_factor", "energy_cost_factor",
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # String columns (non-nullable in ClickHouse)
    str_cols = [
        "report_id", "device_id", "application_customer_id", "platform_customer_id",
        "report_type", "model", "tags", "location_state", "location_country",
        "location_id", "location_name", "location_city", "processor_vendor",
        "server_generation", "server_name", "metric_id", "cpu_inventory",
        "memory_inventory", "max_metric_time", "inventory_date",
        "location_date",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: "" if pd.isna(x) else str(x))

    # error_reason is Nullable(String) — must be Python None, NOT float NaN
    if "error_reason" in df.columns:
        df["error_reason"] = [
            None if pd.isna(v) else str(v) for v in df["error_reason"]
        ]

    return df


# =========================================================================
# ClickHouse insertion
# =========================================================================
def _native(val):
    """Convert numpy / pandas scalars to plain Python types."""
    if val is None:
        return None
    if hasattr(val, "item"):
        return val.item()
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    return val


def insert_into_clickhouse(ch_client, df: pd.DataFrame) -> int:
    """
    Insert DataFrame rows into atlas.telemetry_refined in batches.
    Returns total rows inserted.
    """
    if df.empty:
        log.info("No rows to insert — skipping ClickHouse write")
        return 0

    # Reorder columns to match the table definition
    insert_cols = [c for c in CH_COLUMNS if c in df.columns]
    df_ordered = df[insert_cols]

    total = len(df_ordered)
    inserted = 0

    for start in range(0, total, BATCH_SIZE):
        batch = df_ordered.iloc[start : start + BATCH_SIZE]
        data = [
            [_native(v) for v in row]
            for row in batch.values.tolist()
        ]
        ch_client.insert(
            table="atlas.telemetry_refined",
            data=data,
            column_names=insert_cols,
        )
        inserted += len(batch)
        log.info("  Inserted batch %d–%d / %d", start + 1, start + len(batch), total)

    return inserted


# =========================================================================
# Main
# =========================================================================
def main():
    log.info("=" * 60)
    log.info("ATLAS Delta Loader — Refined → ClickHouse")
    log.info("=" * 60)
    log.info("ClickHouse : %s:%s", CH_HOST, CH_PORT)
    log.info("PostgreSQL : %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
    log.info("Refined    : %s", REFINED_PATH)

    # --- Connect ---------------------------------------------------------
    pg_conn = pg_connect()
    log.info("Connected to PostgreSQL")

    ch_client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS,
    )
    log.info("Connected to ClickHouse (server version %s)", ch_client.server_version)

    # --- Read watermark --------------------------------------------------
    watermark = get_watermark(pg_conn)
    if watermark:
        log.info("Incremental mode — watermark: %s", watermark)
    else:
        log.info("Full load — no previous watermark found")

    # --- Read refined data (partition-aware) ------------------------------
    df = read_refined_parquet(REFINED_PATH, watermark)
    if df.empty:
        log.info("Nothing to load. Exiting.")
        log_pipeline_run(pg_conn, "completed", 0)
        pg_conn.close()
        ch_client.close()
        return

    # --- Prepare types for ClickHouse ------------------------------------
    df = prepare_for_clickhouse(df)

    # --- Apply watermark filter AFTER type conversion --------------------
    # (metric_time is now a proper datetime, safe for comparison)
    if watermark and not df.empty:
        watermark_dt = pd.to_datetime(watermark, utc=True)
        original_count = len(df)
        df = df[df["metric_time"] > watermark_dt]
        log.info("After watermark filter (> %s): %d / %d rows", watermark, len(df), original_count)

    if df.empty:
        log.info("All rows already loaded (watermark filter). Exiting.")
        log_pipeline_run(pg_conn, "completed", 0)
        pg_conn.close()
        ch_client.close()
        return

    # --- Insert into ClickHouse ------------------------------------------
    try:
        rows_inserted = insert_into_clickhouse(ch_client, df)
        log.info("Inserted %d rows into atlas.telemetry_refined", rows_inserted)
    except Exception as exc:
        log.error("ClickHouse insert failed: %s", exc)
        log.error("Column dtypes:")
        for col in df.columns:
            nan_count = df[col].isna().sum()
            sample = df[col].iloc[0] if len(df) > 0 else "N/A"
            log.error("  %-30s dtype=%-15s NaNs=%-6d sample=%s (type=%s)",
                      col, str(df[col].dtype), nan_count, repr(sample), type(sample).__name__)
        log_pipeline_run(pg_conn, "failed", 0, str(exc))
        pg_conn.close()
        ch_client.close()
        raise

    # --- Update metadata -------------------------------------------------
    max_metric_time = df["metric_time"].max().isoformat()
    update_watermark(pg_conn, max_metric_time, rows_inserted)
    log.info("Watermark updated to %s", max_metric_time)

    upsert_device_registry(pg_conn, df)
    upsert_location_registry(pg_conn, df)

    log_pipeline_run(pg_conn, "completed", rows_inserted)
    log.info("Pipeline run logged successfully")

    # --- Summary ---------------------------------------------------------
    log.info("-" * 60)
    log.info("LOAD SUMMARY")
    log.info("  Rows inserted      : %d", rows_inserted)
    log.info("  Max metric_time    : %s", max_metric_time)
    log.info("  ClickHouse MVs     : hourly_mv, daily_mv (auto-populated)")
    log.info("-" * 60)

    # --- Cleanup ---------------------------------------------------------
    pg_conn.close()
    ch_client.close()
    log.info("Done.")


if __name__ == "__main__":
    if SCHEDULE_INTERVAL > 0:
        log.info("=" * 60)
        log.info("PERSISTENT SCHEDULER MODE — interval: %ds", SCHEDULE_INTERVAL)
        log.info("=" * 60)
        while True:
            try:
                main()
            except Exception as exc:
                log.error("Scheduler iteration failed: %s", exc)
                log.error("Will retry in %ds...", SCHEDULE_INTERVAL)
            log.info("Sleeping %ds until next run...", SCHEDULE_INTERVAL)
            time.sleep(SCHEDULE_INTERVAL)
    else:
        log.info("One-shot mode (SCHEDULE_INTERVAL_SECONDS=0)")
        main()
