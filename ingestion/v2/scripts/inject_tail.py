
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import orjson
import io

TSDB_DSN = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
REGISTRY_PATH = "/app/device_configs.json"

def inject_recent():
    with open(REGISTRY_PATH, "rb") as f:
        devices = orjson.loads(f.read())
    
    device_ids = list(devices.keys())
    n = len(device_ids)
    
    conn = psycopg2.connect(TSDB_DSN)
    cur = conn.cursor()
    
    now = datetime.now(timezone.utc)
    # Inject for the last 12 hours
    for h in range(12):
        dt_hour = (now - timedelta(hours=h)).replace(minute=0, second=0, microsecond=0)
        print(f"Injecting {dt_hour}...")
        
        for s in range(12): # 12 points per hour
            dt = dt_hour + timedelta(minutes=5*s)
            
            # Simple randomized data
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
            
            # Use COPY for speed
            buf = io.StringIO()
            df.to_csv(buf, sep='\t', index=False, header=False)
            buf.seek(0)
            cols = df.columns
            cur.copy_expert(f"COPY telemetry_live ({','.join(cols)}) FROM STDIN WITH DELIMITER E'\\t'", buf)
            
        conn.commit()
    
    print("✅ Recent tail injected!")

if __name__ == "__main__":
    inject_recent()
