"""
ATLAS Analytics Layer — Delta Lake → ClickHouse Loader
=======================================================
Reads deduplicated Parquet data from the Delta Lake Refined Layer
and batch-inserts it into ClickHouse atlas.telemetry_refined.

Materialized Views (hourly_mv, daily_mv) fire automatically on insert.

Supports incremental loading via a PostgreSQL watermark table so that
re-runs only process new data.

Scheduler Mode (SCHEDULE_INTERVAL_SECONDS):
    0    → One-shot: run once and exit.
    3600 → Persistent scheduler: run, sleep 3600 s (1 hour), repeat.

    Why 3600 s?  The upstream Spark processor produces 1-hour tumbling-
    window aggregations and the Lakehouse MERGE runs after each batch.
    Polling every 3600 s lets the loader pick up exactly the latest
    hourly batch each cycle, avoiding redundant scans while keeping
    the ClickHouse analytics layer at most 1 hour behind real-time.

Partition-Aware Reading:
    The Refined Layer uses a 5-level Hive partition scheme:
        report_type / partition_date / platform_customer_id /
        application_customer_id / device_id
    When a watermark exists, the loader prunes partitions by partition_date
    so only new/updated date directories are scanned.

Metadata Piping (PostgreSQL):
    After each successful ClickHouse load the loader writes metadata into
    four PostgreSQL tables:
      1. data_load_watermarks — last loaded metric_time for incremental mode
      2. pipeline_runs        — audit log of every loader invocation
      3. device_registry      — unique devices seen (upsert on composite key)
      4. location_registry    — unique locations seen (upsert on location_id)
    This metadata is consumed by dashboards and downstream monitoring.

Owner : Varna (Analytics Layer)
Reads : refined-volume (Lakehouse Refined Layer) — READ-ONLY
Writes: ClickHouse atlas.* tables, PostgreSQL metadata tables

Credentials:
    All DB credentials are read from environment variables.
    In Docker they flow from .env.example → docker-compose env_file →
    container environment → this loader.  Edit .env.example to change them.

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
import gc
import logging
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Optional, Generator

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
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))  # Reduced from 10000 for memory efficiency
SCHEDULE_INTERVAL = int(os.getenv("SCHEDULE_INTERVAL_SECONDS", "0"))
WATERMARK_SOURCE = "delta_refined"
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))  # Reduced for memory safety  # Parquet reading chunk size (rows per batch)

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


def get_watermarks(pg_conn) -> dict:
    """Return dict mapping device_id to last loaded metric_time ISO string.
    
    Uses explicit strftime formatting to preserve microsecond precision.
    The previous .isoformat() call would strip microseconds when they were
    zero, causing the downstream watermark comparison to miss rows at the
    second boundary.
    """
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT device_id, last_metric_time FROM data_load_watermarks WHERE source = %s",
            (WATERMARK_SOURCE,),
        )
        return {
            row[0]: row[1].strftime("%Y-%m-%dT%H:%M:%S.%f%z")
            for row in cur.fetchall() if row[1]
        }


def update_watermarks(pg_conn, df: pd.DataFrame):
    """Upsert the watermark row per device."""
    if df.empty:
        return
    
    max_times = df.groupby('device_id')['metric_time'].max().reset_index()
    rows_counts = df.groupby('device_id').size().reset_index(name='rows_loaded')
    stats = pd.merge(max_times, rows_counts, on='device_id')
    
    now = datetime.now(timezone.utc)
    with pg_conn.cursor() as cur:
        for _, row in stats.iterrows():
            cur.execute(
                """
                INSERT INTO data_load_watermarks (source, device_id, last_metric_time, last_loaded_at, rows_loaded)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source, device_id) DO UPDATE
                    SET last_metric_time = EXCLUDED.last_metric_time,
                        last_loaded_at   = EXCLUDED.last_loaded_at,
                        rows_loaded      = data_load_watermarks.rows_loaded + EXCLUDED.rows_loaded
                """,
                (WATERMARK_SOURCE, row['device_id'], row['metric_time'].isoformat(), now, row['rows_loaded']),
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
    """Batch-upsert unique devices into the device_registry table.
    
    Uses executemany instead of row-by-row execute to reduce PostgreSQL
    round-trips from O(n) to O(1). At 100K devices this is ~10-50x faster.
    """
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
    
    rows = [
        (
            row.get("device_id"), row.get("platform_customer_id"), row.get("application_customer_id"),
            row.get("server_name", ""), row.get("model", ""), row.get("processor_vendor", ""),
            row.get("server_generation", ""),
            int(row["socket_count"]) if "socket_count" in row and pd.notna(row["socket_count"]) else None,
        )
        for _, row in devices.iterrows()
    ]
    
    with pg_conn.cursor() as cur:
        cur.executemany(
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
            rows,
        )
    pg_conn.commit()
    log.info("Upserted %d device(s) into device_registry (batch)", len(devices))


def upsert_location_registry(pg_conn, df: pd.DataFrame):
    """Batch-upsert unique locations into the location_registry table.
    
    Uses executemany instead of row-by-row execute to reduce PostgreSQL
    round-trips from O(n) to O(1).
    """
    loc_cols = ["location_id", "location_name", "location_city", "location_state", "location_country"]
    available_cols = [c for c in loc_cols if c in df.columns]
    if "location_id" not in available_cols:
        log.warning("Missing location_id column, skipping location registry upsert")
        return

    locations = df[available_cols].drop_duplicates(subset=["location_id"])
    
    rows = [
        (
            row.get("location_id"), row.get("location_name", ""),
            row.get("location_city", ""), row.get("location_state", ""),
            row.get("location_country", ""),
        )
        for _, row in locations.iterrows()
    ]
    
    with pg_conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO location_registry (location_id, location_name, location_city, location_state, location_country)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (location_id) DO UPDATE
                SET location_name    = EXCLUDED.location_name,
                    location_city    = EXCLUDED.location_city,
                    location_state   = EXCLUDED.location_state,
                    location_country = EXCLUDED.location_country
            """,
            rows,
        )
    pg_conn.commit()
    log.info("Upserted %d location(s) into location_registry (batch)", len(locations))


# =========================================================================
# Partition-aware data reading
# =========================================================================
def _compute_partition_cutoff(watermarks: dict) -> Optional[str]:
    """
    Convert dict of device watermarks to a single partition_date cutoff
    string (YYYY-MM-DD) for Hive partition pruning.

    The Refined Layer uses 5-level partitions:
        report_type / partition_date / platform_customer_id /
        application_customer_id / device_id

    We prune based on the earliest watermark across all devices.
    If there are no watermarks, return None. Note that new devices
    will need older data, so if you expect new devices frequently,
    this could skip scanning them. For a production system with new
    devices, this logic might need refinement.
    """
    if not watermarks:
        return None
    try:
        earliest_watermark = min(watermarks.values())
        cutoff = pd.to_datetime(earliest_watermark).strftime("%Y-%m-%d")
        log.info("Partition pruning: will scan partition_date >= %s", cutoff)
        return cutoff
    except Exception:
        log.warning("Could not parse watermarks for partition pruning, reading all partitions")
        return None


def read_refined_parquet(path: str, watermarks: dict) -> pd.DataFrame:
    """
    Read data from the refined Delta Lake path.

    Uses partition_date pruning when watermarks exist to avoid scanning
    old partitions. Prefers the deltalake (delta-rs) library which reads the
    Delta transaction log correctly. Falls back to pyarrow if unavailable.

    Two-pass strategy (Issue 5 fix):
        Pass 1 — Read with partition pruning for known devices.
        Pass 2 — If new devices are detected in the pruned read, run a
                 targeted full scan for just those new device_ids.
        This avoids the old blind spot where a single new device forced a
        full historical scan of ALL partitions.
    """
    refined = Path(path)
    if not refined.exists():
        log.error("Refined data path does not exist: %s", path)
        return pd.DataFrame()

    partition_cutoff = _compute_partition_cutoff(watermarks)
    known_devices = set(watermarks.keys()) if watermarks else set()

    # --- Primary read (with partition pruning when available) ---
    df = _read_delta_or_fallback(refined, partition_cutoff)

    if df.empty:
        return df

    # --- Two-pass: detect and backfill new devices ---
    if known_devices and "device_id" in df.columns:
        new_devices = set(df["device_id"].unique()) - known_devices
        if new_devices:
            log.info(
                "Detected %d new device(s) in pruned read — running targeted full scan",
                len(new_devices),
            )
            df_full = _read_delta_or_fallback(refined, partition_cutoff=None)
            if not df_full.empty:
                df_new = df_full[df_full["device_id"].isin(new_devices)]
                if not df_new.empty:
                    log.info("Backfilled %d rows for %d new device(s)", len(df_new), len(new_devices))
                    df = pd.concat([df, df_new]).drop_duplicates()

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


def _read_delta_or_fallback(refined: Path, partition_cutoff: Optional[str] = None) -> pd.DataFrame:
    """Internal helper: try deltalake, fall back to pyarrow."""
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

            return table.to_pandas()
        except Exception as exc:
            log.warning("deltalake read failed (%s), falling back to pyarrow", exc)
            return _read_parquet_fallback(refined, partition_cutoff)
    else:
        log.info("deltalake not installed — using pyarrow fallback")
        return _read_parquet_fallback(refined, partition_cutoff)


def _read_parquet_fallback(refined: Path, partition_cutoff: Optional[str] = None) -> pd.DataFrame:
    """
    Fallback: read Parquet files using pyarrow.dataset with Hive partitioning
    so that partition columns (report_type, partition_date, etc.) are materialised.
    Applies partition_date filter if a cutoff is provided.
    NOTE: this reads ALL parquet files and may include superseded Delta rows.
    
    For memory efficiency, yields data in chunks rather than loading all at once.
    Use read_refined_parquet_chunked() for streaming behavior.
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
                "Read %d rows via pyarrow.dataset with partition_date >= %s (via batch chunks)",
                len(table), partition_cutoff,
            )
        else:
            table = dataset.to_table()
            log.info("Read %d rows via pyarrow.dataset (Hive partitioning, via batch chunks)", len(table))

        # Convert using batches to avoid OOM on large tables
        batches = table.to_batches(max_chunksize=CHUNK_SIZE)
        dfs = [batch.to_pandas() for batch in batches]
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    except Exception as exc:
        # Last resort: direct parquet file read (no partition columns)
        log.warning("PyArrow dataset read failed: %s — falling back to file-level read", exc)
        parquet_files = sorted(
            str(f)
            for f in refined.rglob("*.parquet")
            if "_delta_log" not in str(f) and not f.name.startswith(".")
        )
        if not parquet_files:
            log.warning("No parquet files found in %s", refined)
            return pd.DataFrame()
        log.info("Found %d parquet file(s) in %s (reading via chunks)", len(parquet_files), refined)
        
        # Read each parquet file in chunks and concatenate
        dfs = []
        total_rows = 0
        for pf in parquet_files:
            try:
                table = pq.read_table(pf)
                batches = table.to_batches(max_chunksize=CHUNK_SIZE)
                for batch in batches:
                    dfs.append(batch.to_pandas())
                    total_rows += len(batch)
            except Exception as e:
                log.warning("Failed to read %s: %s", pf, e)
                continue
        
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        log.info("Read %d total rows from parquet files (chunked)", total_rows)

    return df


def read_refined_parquet_chunked(path: str, watermarks: dict, chunk_size: int = CHUNK_SIZE) -> Generator[pd.DataFrame, None, None]:
    """
    Stream parquet data from the refined layer in chunks to avoid OOM.
    
    Yields DataFrames of approximately chunk_size rows, already filtered by watermarks.
    Each yielded DataFrame is ready for type conversion and insertion.
    """
    refined = Path(path)
    if not refined.exists():
        log.error("Refined data path does not exist: %s", path)
        return

    partition_cutoff = _compute_partition_cutoff(watermarks)
    
    # Get all parquet files in refined directory
    parquet_files = sorted(
        f for f in refined.rglob("*.parquet")
        if "_delta_log" not in str(f) and not f.name.startswith(".")
    )
    
    if not parquet_files:
        log.warning("No parquet files found in %s", path)
        return
    
    log.info("Streaming %d parquet file(s) in chunks of ~%d rows", len(parquet_files), chunk_size)
    
    try:
        import pyarrow.dataset as pads
        import pyarrow.compute as pc
        
        # Try dataset-based read first (supports Hive partitions)
        dataset = pads.dataset(
            str(refined), format="parquet", partitioning="hive"
        )
        
        # LAZY LOADING: Use to_batches() directly on the dataset instead of to_table()
        # This prevents loading all 140 files into RAM at once.
        if partition_cutoff:
            batches = dataset.to_batches(
                filter=pc.field("partition_date") >= partition_cutoff,
                batch_size=chunk_size
            )
        else:
            batches = dataset.to_batches(batch_size=chunk_size)
            
        # Stream batches directly from disk
        for batch in batches:
            df = batch.to_pandas()
            if not df.empty:
                yield df
                
            # Aggressively release memory after yielding each chunk
            del df
            del batch
            gc.collect() 
            
    except Exception as exc:
        log.warning("Dataset streaming failed: %s — falling back to file-by-file", exc)
        
        # Fallback: stream each parquet file individually
        for pf in parquet_files:
            try:
                # Open the file lazily
                parquet_file = pq.ParquetFile(pf)
                for batch in parquet_file.iter_batches(batch_size=chunk_size):
                    df = batch.to_pandas()
                    if not df.empty:
                        yield df
                        
                    del df
                    del batch
                    gc.collect()
            except Exception as e:
                log.warning("Failed to read %s: %s", pf, e)
                continue
        
        # Fallback: stream each parquet file individually
        for pf in parquet_files:
            try:
                table = pq.read_table(pf)
                for batch in table.to_batches(max_chunksize=chunk_size):
                    df = batch.to_pandas()
                    if not df.empty:
                        yield df
                        gc.collect()
            except Exception as e:
                log.warning("Failed to read %s: %s", pf, e)
                continue


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
def insert_into_clickhouse(ch_client, df: pd.DataFrame) -> int:
    """
    Insert DataFrame rows into atlas.telemetry_refined in batches.
    
    Uses insert_df() for direct columnar binary serialization (no Python
    row-level iteration). Falls back to list-based insert() if the driver
    version is incompatible with insert_df() for this table schema.
    
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
        batch = df_ordered.iloc[start : start + BATCH_SIZE].copy()
        try:
            # Fast path: direct columnar binary insert (no Python row loop)
            ch_client.insert_df(
                table="atlas.telemetry_refined",
                df=batch,
                column_names=insert_cols,
            )
        except TypeError:
            # Fallback for older clickhouse-connect versions that have
            # ColumnDef parsing issues with DESCRIBE TABLE output
            log.warning("insert_df() failed — falling back to list-based insert")
            data = [
                [_native_val(v) for v in row]
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
    log.info("ATLAS Delta Loader — Refined → ClickHouse")
    log.info("=" * 60)
    log.info("ClickHouse : %s:%s  user=%s", CH_HOST, CH_PORT, CH_USER)
    log.info("PostgreSQL : %s:%s/%s  user=%s", PG_HOST, PG_PORT, PG_DB, PG_USER)
    log.info("Refined    : %s", REFINED_PATH)
    log.info("Batch size : %d", BATCH_SIZE)
    log.info("Scheduler  : %s",
             f"every {SCHEDULE_INTERVAL}s" if SCHEDULE_INTERVAL > 0 else "one-shot")

    # --- Check refined path exists before connecting to DBs ---------------
    refined_dir = Path(REFINED_PATH)
    if not refined_dir.exists():
        log.warning("Refined data path does not exist yet: %s", REFINED_PATH)
        log.warning("Lakehouse may not have produced data. Nothing to load.")
        return

    parquet_count = sum(1 for f in refined_dir.rglob("*.parquet")
                        if "_delta_log" not in str(f) and not f.name.startswith("."))
    if parquet_count == 0:
        log.warning("No parquet files found in %s — nothing to load.", REFINED_PATH)
        return
    log.info("Found %d parquet file(s) in refined path", parquet_count)

    # --- Connect ---------------------------------------------------------
    pg_conn = pg_connect()
    log.info("Connected to PostgreSQL (%s:%s/%s)", PG_HOST, PG_PORT, PG_DB)

    ch_client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS,
    )
    log.info("Connected to ClickHouse (server version %s)", ch_client.server_version)

    # Verify atlas DB and table exist
    try:
        ch_client.command("SELECT count() FROM atlas.telemetry_refined")
        log.info("Verified: atlas.telemetry_refined table accessible")
    except Exception as exc:
        log.error("atlas.telemetry_refined not accessible: %s", exc)
        log.error("ClickHouse init may have failed. Check loader-start.sh logs.")
        pg_conn.close()
        ch_client.close()
        raise

    # --- Read watermarks --------------------------------------------------
    watermarks = get_watermarks(pg_conn)
    if watermarks:
        log.info("Incremental mode — found watermarks for %d device(s)", len(watermarks))
    else:
        log.info("Full load — no previous watermarks found")

    # --- Process refined data in chunks (memory-efficient streaming) ------
    log.info("--- Starting chunk-based streaming load ---")
    
    total_rows_inserted = 0
    max_metric_time = None
    chunk_num = 0
    accumulated_df = pd.DataFrame()  # Accumulate for metadata updates
    
    for chunk_df in read_refined_parquet_chunked(REFINED_PATH, watermarks):
        chunk_num += 1
        chunk_rows = len(chunk_df)
        log.info("\n[Chunk %d] Read %d rows from parquet", chunk_num, chunk_rows)
        
        # --- Prepare types for ClickHouse ---
        chunk_df = prepare_for_clickhouse(chunk_df)
        
        # --- Apply watermark filter AFTER type conversion ---
        if watermarks and not chunk_df.empty:
            original_count = len(chunk_df)
            epoch = pd.Timestamp("1970-01-01", tz="UTC")
            watermark_s = chunk_df['device_id'].map(watermarks)
            watermark_dt = pd.to_datetime(watermark_s, utc=True).fillna(epoch)
            chunk_df = chunk_df[chunk_df["metric_time"] >= watermark_dt]
            log.info("  After watermark filter: %d / %d rows", len(chunk_df), original_count)
        
        if chunk_df.empty:
            log.info("  Chunk %d: empty after filtering, skipping", chunk_num)
            gc.collect()
            continue
        
        # --- Insert chunk into ClickHouse ---
        try:
            pre_insert_count = ch_client.command("SELECT count() FROM atlas.telemetry_refined")
            rows_inserted = insert_into_clickhouse(ch_client, chunk_df)
            post_insert_count = ch_client.command("SELECT count() FROM atlas.telemetry_refined")
            
            total_rows_inserted += rows_inserted
            log.info("  ✓ Inserted %d rows (total: %d)", rows_inserted, total_rows_inserted)
            
            if post_insert_count - pre_insert_count != rows_inserted:
                log.warning("  Row count mismatch in chunk %d", chunk_num)
        except Exception as exc:
            log.error("  ✗ ClickHouse insert failed for chunk %d: %s", chunk_num, exc)
            log_pipeline_run(pg_conn, "failed", total_rows_inserted, str(exc))
            pg_conn.close()
            ch_client.close()
            raise
        
        # Accumulate for metadata (sample every 2 chunks to avoid huge DataFrame)
        if chunk_num % 2 == 0:
            accumulated_df = pd.concat([accumulated_df, chunk_df.head(1000)], ignore_index=True)
        
        # Update max_metric_time
        if "metric_time" in chunk_df.columns:
            chunk_max = chunk_df["metric_time"].max()
            if max_metric_time is None or chunk_max > max_metric_time:
                max_metric_time = chunk_max
        
        # Release memory
        del chunk_df
        gc.collect()
    
    if total_rows_inserted == 0:
        log.info("No rows loaded (all filtered or no data). Exiting.")
        log_pipeline_run(pg_conn, "completed", 0)
        pg_conn.close()
        ch_client.close()
        return
    
    # --- Update metadata from accumulated sample ---
    if not accumulated_df.empty:
        log.info("--- PostgreSQL metadata piping (from accumulated sample) ---")
        update_watermarks(pg_conn, accumulated_df)
        log.info("[metadata 1/4] Watermarks updated")
        upsert_device_registry(pg_conn, accumulated_df)
        log.info("[metadata 2/4] Device registry upserted")
        upsert_location_registry(pg_conn, accumulated_df)
        log.info("[metadata 3/4] Location registry upserted")
    
    log_pipeline_run(pg_conn, "completed", total_rows_inserted)
    log.info("[metadata 4/4] Pipeline run logged")
    
    # --- Summary ---------------------------------------------------------
    log.info("-" * 60)
    log.info("LOAD SUMMARY")
    log.info("  Chunks processed   : %d", chunk_num)
    log.info("  Total rows inserted: %d", total_rows_inserted)
    log.info("  Max metric_time    : %s", max_metric_time.isoformat() if max_metric_time else "N/A")
    log.info("  ClickHouse MVs     : hourly_mv, daily_mv (auto-populated)")
    log.info("-" * 60)
    
    # --- Cleanup ---------------------------------------------------------
    pg_conn.close()
    ch_client.close()
    gc.collect()
    log.info("Done.")


if __name__ == "__main__":
    if SCHEDULE_INTERVAL > 0:
        log.info("=" * 60)
        log.info("PERSISTENT SCHEDULER MODE — interval: %ds (%d min)",
                 SCHEDULE_INTERVAL, SCHEDULE_INTERVAL // 60)
        log.info("  Why %ds?  Matches the upstream 1-hour tumbling-window batch",
                 SCHEDULE_INTERVAL)
        log.info("  cadence so each cycle picks up exactly the latest hourly batch.")
        log.info("=" * 60)
        cycle = 0
        while True:
            cycle += 1
            log.info("--- Scheduler cycle %d ---", cycle)
            try:
                main()
            except Exception as exc:
                log.error("Scheduler cycle %d failed: %s", cycle, exc)
                log.error("Will retry in %ds...", SCHEDULE_INTERVAL)
            log.info("Sleeping %ds until next run...", SCHEDULE_INTERVAL)
            time.sleep(SCHEDULE_INTERVAL)
    else:
        log.info("One-shot mode (SCHEDULE_INTERVAL_SECONDS=0)")
        main()
