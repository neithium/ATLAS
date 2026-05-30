import pyarrow.parquet as pq
import pyarrow as pa
import os
from datetime import datetime
import random
import time

# Create multiple test batches to trigger streaming processing
for batch_num in range(3):
    data = {
        'device_id': [f'test_device_{i}' for i in range(100)],
        'metric_time': [datetime.now()] * 100,
        'cpu_usage': [random.uniform(10, 90) for _ in range(100)],
        'memory_usage': [random.uniform(20, 80) for _ in range(100)],
        'avg_cpu': [random.uniform(10, 90) for _ in range(100)],
        'avg_mem': [random.uniform(20, 80) for _ in range(100)],
    }

    table = pa.table(data)

    # Write to stream directory
    stream_dir = '/stream_raw/stream'
    os.makedirs(stream_dir, exist_ok=True)
    filepath = os.path.join(stream_dir, f'test_batch_{batch_num}_{int(time.time())}.parquet')
    pq.write_table(table, filepath)
    print(f'Created test file: {filepath}')
    time.sleep(1)
