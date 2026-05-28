import asyncio
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import time
import orjson
import sys
import glob
import os

sys.path.append('/app')
from v2.api.api_v2 import process_device_batch_hydration
from schema_builder import build_48_field_golden_record

print('Loading test data...')
try:
    files = glob.glob('/app/telemetry-cache/**/*.parquet', recursive=True)
    if not files:
        print("No parquet files found!")
        sys.exit(1)
    
    table = pq.read_table(files[0])
    print(f'Loaded {table.num_rows} rows from {files[0]}')
    table = table.slice(0, 201600) # up to 100 devices
    meta = {did: {} for did in pc.unique(table['device_id']).to_pylist()}

    print('Running hydration...')
    t0 = time.time()
    res = process_device_batch_hydration(table, meta, 2016)
    t1 = time.time()
    print(f'Hydration done in {t1-t0:.2f}s for {len(res)} devices')

except Exception as e:
    print(f"Error: {e}")
