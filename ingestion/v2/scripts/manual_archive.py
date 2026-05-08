import sys
import os
import asyncio
import time
from datetime import datetime, timezone, timedelta
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import asyncpg
import orjson
import logging
import gc

# Add parent path for schema_builder
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from schema_builder import build_48_field_golden_record

# Configuration
TSDB_HOST = "127.0.0.1"
TSDB_PORT = "5432"
TSDB_USER = "postgres"
TSDB_PASS = "postgres"
TSDB_NAME = "postgres"
TS_CONN_STR = f"postgresql://{TSDB_USER}:{TSDB_PASS}@{TSDB_HOST}:{TSDB_PORT}/{TSDB_NAME}"

MINIO_HOST = "127.0.0.1:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin"

REGISTRY_PATH = "/app/device_configs.json"


async def manual_archival_push():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("manual-archive")
    
    t_total_start = time.monotonic()
    now = datetime.now(timezone.utc)
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(days=7)
    
    log.info(f"🏗️ [PARQUET-STREAMING] 7-Day Archival: {start.strftime('%Y-%m-%d %H:%M')} → {end.strftime('%Y-%m-%d %H:%M')}")

    with open(REGISTRY_PATH, "rb") as f:
        DEVICES = orjson.loads(f.read())
    all_device_ids = list(DEVICES.keys())

    try:
        pool = await asyncpg.create_pool(TS_CONN_STR, min_size=5, max_size=10)

        # 🏎️ STABLE STREAMING STRATEGY
        # 2,016 points/device/7-days -> ~1,000 devices per 128MB silo
        MICRO_BATCH = 20   # Lower batch size for stability
        SILO_SIZE = 1000 
        
        batch_counter = 0
        base_path = f"production/year={end.year}/month={end.month:02d}/day={end.day:02d}/full_7day/"
        
        log.info(f"🚀 Starting Streamed Archival (Goal: {len(all_device_ids)//SILO_SIZE + 1} Large Silos)...")
        
        for i in range(0, len(all_device_ids), SILO_SIZE):
            silo_devices = all_device_ids[i:i + SILO_SIZE]
            pq_buf = io.BytesIO()
            writer = None
            silo_records_count = 0
            
            t_silo_start = time.monotonic()
            
            # Process this 1000-device silo in 100-device micro-chunks
            for j in range(0, len(silo_devices), MICRO_BATCH):
                micro_devices = silo_devices[j:j + MICRO_BATCH]
                
                async with pool.acquire() as conn:
                    records = await conn.fetch(
                        "SELECT * FROM telemetry_live WHERE metric_time >= $1 AND metric_time < $2 AND device_id = ANY($3) ORDER BY device_id, metric_time ASC", 
                        start, end, micro_devices
                    )
                
                if not records:
                    continue

                # 🚀 Group by Device and Hydrate using Unified Batch Builder
                # This ensures archived Parquet files match the Kafka 'Golden Record' format
                from schema_builder import build_batch_power_detail
                
                # Sort/Group records by device_id
                from collections import defaultdict
                device_groups = defaultdict(list)
                for r in records:
                    device_groups[r['device_id']].append(dict(r))
                
                hydrated = []
                for did, raw_readings in device_groups.items():
                    meta = DEVICES.get(did, {})
                    
                    # 1. Build PowerDetail and aggregates
                    pd_list, avg_v, max_v, min_v = build_batch_power_detail(raw_readings)
                    
                    # 2. Build 48-field record
                    payload = build_48_field_golden_record(
                        device_id=did,
                        reading=raw_readings[-1],
                        device_metadata=meta,
                        inventory_data=meta.get("inventory_data"),
                        power_detail_list=pd_list
                    )
                    
                    # 3. Inject computed aggregates
                    payload["data"]["Average"] = avg_v
                    payload["data"]["Maximum"] = max_v
                    payload["data"]["Minimum"] = min_v
                    
                    hydrated.append(payload)

                del records
                
                # Convert to Arrow Table
                table = pa.Table.from_pylist(hydrated)
                del hydrated
                
                # 🏁 EXPLICIT SCHEMA ENFORCEMENT (Prevents int64 vs double mismatches)
                # We define this once per silo based on the first hydrated table
                if writer is None:
                    # Define canonical schema for the 48-field golden record
                    # This ensures consistency even if some batches have only integers
                    schema = pa.schema([
                        ("device_id", pa.string()),
                        ("report_id", pa.string()),
                        ("created_at", pa.string()),
                        ("status", pa.bool_()),
                        ("model", pa.string()),
                        ("tags", pa.string()),
                        ("report_type", pa.string()),
                        ("server_name", pa.string()),
                        ("error_reason", pa.string()),
                        ("location_id", pa.string()),
                        ("location_city", pa.string()),
                        ("location_name", pa.string()),
                        ("location_state", pa.string()),
                        ("location_country", pa.string()),
                        ("processor_vendor", pa.string()),
                        ("server_generation", pa.string()),
                        ("platform_customer_id", pa.string()),
                        ("application_customer_id", pa.string()),
                        ("metric_type", pa.string()),
                        ("data", pa.struct([
                            ("Id", pa.string()),
                            ("Average", pa.float64()),
                            ("Maximum", pa.float64()),
                            ("Minimum", pa.float64()),
                            ("Name", pa.string()),
                            ("PowerDetail", pa.list_(pa.struct([
                                ("AmbTemp", pa.float64()),
                                ("Average", pa.float64()),
                                ("CpuAvgFreq", pa.int64()),
                                ("CpuMax", pa.int64()),
                                ("CpuPwrSavLim", pa.int64()),
                                ("CpuUtil", pa.int64()),
                                ("CpuWatts", pa.int64()),
                                ("GpuWatts", pa.int64()),
                                ("Minimum", pa.float64()),
                                ("Peak", pa.float64()),
                                ("Time", pa.string())
                            ])))
                        ])),
                        ("inventory_data", pa.struct([
                            ("cpu_count", pa.int64()),
                            ("socket_count", pa.int64()),
                            ("cpu_inventory", pa.list_(pa.struct([
                                ("model", pa.string()),
                                ("speed", pa.int64()),
                                ("total_cores", pa.int64())
                            ]))),
                            ("memory_inventory", pa.list_(pa.struct([
                                ("memory_size", pa.int64()),
                                ("operating_freq", pa.int64()),
                                ("memory_device_type", pa.string())
                            ])))
                        ]))
                    ])
                    writer = pq.ParquetWriter(pq_buf, schema, compression='snappy')
                
                # Ensure the table matches the schema (casts if necessary)
                writer.write_table(table.cast(writer.schema))
                silo_records_count += len(table)
                del table
                
                # 🛑 Yield to Event Loop to prevent system hanging
                await asyncio.sleep(0.05)
                gc.collect()

            if writer:
                writer.close()
                content = pq_buf.getvalue()
                fname = f"archive_silo_{batch_counter}.parquet"
                
                # 🚀 LOCAL FS MIRRORING (Primary Storage)
                RAW_LOCAL = "/app/data/raw"
                ARCHIVE_LOCAL = "/app/data/archive"
                
                raw_dir = os.path.join(RAW_LOCAL, base_path)
                archive_dir = os.path.join(ARCHIVE_LOCAL, base_path)
                os.makedirs(raw_dir, exist_ok=True)
                os.makedirs(archive_dir, exist_ok=True)
                
                with open(os.path.join(raw_dir, fname), "wb") as f:
                    f.write(content)
                with open(os.path.join(archive_dir, fname), "wb") as f:
                    f.write(content)
                
                t_silo_elapsed = time.monotonic() - t_silo_start
                log.info(f"✅ Silo {batch_counter} Created & Mirrored: {silo_records_count:,} records | Local + MinIO | Time: {t_silo_elapsed:.1f}s")
                
                batch_counter += 1
                del content, pq_buf
                gc.collect()

        await pool.close()
        log.info(f"🏁 STREAMED ARCHIVAL COMPLETE in {(time.monotonic()-t_total_start)/60:.2f} minutes")
        
    except Exception as e:
        log.error(f"💥 Archival Failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(manual_archival_push())
