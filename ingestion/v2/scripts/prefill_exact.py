
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import orjson
import io
import multiprocessing as mp

TSDB_DSN = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
REGISTRY_PATH = "/app/device_configs.json"

def worker(hours_chunk, device_ids, devices):
    conn = psycopg2.connect(TSDB_DSN)
    cur = conn.cursor()
    n = len(device_ids)
    
    for dt_hour in hours_chunk:
        print(f"Injecting {dt_hour}...")
        for s in range(12):
            dt = dt_hour + timedelta(minutes=5*s)
            df = pd.DataFrame({
                "metric_time": [dt.isoformat()] * n,
                "device_id": device_ids,
                "platform_customer_id": [devices[d]["platform_customer_id"] for d in device_ids],
                "application_customer_id": [devices[d]["application_customer_id"] for d in device_ids],
                "avg_watts": np.random.uniform(100, 300, n),
                "peak_watts": np.random.randint(300, 450, n),
                "min_watts": np.random.randint(50, 150, n),
                "cpu_util": np.random.randint(10, 90, n),
                "cpu_watts": np.random.randint(50, 150, n),
                "gpu_watts": np.random.randint(0, 100, n),
                "amb_temp": 25.0,
                "cpu_avg_freq": 3000000,
                "cpu_max": 4200000,
                "cpu_pwr_sav_lim": 250,
                "status": "t",
                "report_type": "telemetry_live",
                "metric_type": "power_metrics"
            })
            buf = io.StringIO()
            df.to_csv(buf, sep='\t', index=False, header=False)
            buf.seek(0)
            cols = df.columns
            cur.copy_expert(f"COPY telemetry_live ({','.join(cols)}) FROM STDIN WITH DELIMITER E'\\t'", buf)
        conn.commit()
    conn.close()

def prefill_exact():
    with open(REGISTRY_PATH, "rb") as f:
        devices = orjson.loads(f.read())
    device_ids = list(devices.keys())
    
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    all_hours = [now - timedelta(hours=i) for i in range(192)] # 8 days
    
    # Split into 4 chunks
    chunks = [all_hours[i::4] for i in range(4)]
    
    processes = []
    for chunk in chunks:
        p = mp.Process(target=worker, args=(chunk, device_ids, devices))
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()
    
    print("🏁 Exact 8-day prefill DONE!")

if __name__ == "__main__":
    prefill_exact()
