import time
import numpy as np
import os
import sys
import glob
import psycopg
import clickhouse_connect
import pandas as pd

# --- Configuration ---
import argparse 
parser = argparse.ArgumentParser(description="ClickHouse Storage Benchmark")
parser.add_argument("--devices", type=int, default=2000, help="Number of devices")
parser.add_argument("--days", type=int, default=3, help="Number of days to simulate")
parser.add_argument("--batch-size", type=int, default=1000, help="Devices per batch")
args = parser.parse_args()

DEVICES = args.devices
DAYS = args.days
BATCH_SIZE = args.batch_size
# Assuming 1 metric every 5 minutes per device -> 288 metrics per day
METRICS_PER_DAY = 288
ROWS_PER_DEVICE = DAYS * METRICS_PER_DAY
TOTAL_ROWS = DEVICES * ROWS_PER_DEVICE
BATCH_COUNT = max(1, DEVICES // BATCH_SIZE) * DAYS # One batch per day per chunk of devices? Or perhaps batching by day and device chunk
ROWS_PER_BATCH = BATCH_SIZE * METRICS_PER_DAY

env_path = os.path.join(os.path.dirname(__file__), '../../../../.env.example')
if os.path.exists(env_path):
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, val = line.split('=', 1)
                os.environ.setdefault(key.strip(), val.strip())

CH_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
if CH_HOST == 'analytics':
    CH_HOST = 'localhost'
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASS = os.getenv('CLICKHOUSE_PASSWORD', '')
CH_DB = os.getenv('CLICKHOUSE_DB', 'atlas')
CH_PORT_HTTP = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CH_PORT_NATIVE = int(os.getenv('CLICKHOUSE_NATIVE_PORT', '9000'))

# PG settings mapped from delta_loader logic
PG_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
if PG_HOST == 'analytics':
    PG_HOST = '127.0.0.1'
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "postgres")

class DualLogger:
    """Intercepts print statements and writes them to both console and a dynamic log file."""
    def __init__(self, filepath):
        self.terminal = sys.stdout
        self.filepath = filepath
        # Initialize the file
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.write("") 

    def write(self, message):
        self.terminal.write(message)
        with open(self.filepath, "a", encoding="utf-8") as f:
            f.write(message)

    def flush(self):
        self.terminal.flush()

def setup_logger():
    """Finds the next available log file name and sets up the DualLogger."""
    test_dir = os.path.dirname(os.path.abspath(__file__))
    existing_tests = glob.glob(os.path.join(test_dir, "test_storage_*.txt"))
    
    test_nums = []
    for t in existing_tests:
        try:
            num = int(os.path.basename(t).replace("test_storage_", "").replace(".txt", ""))
            test_nums.append(num)
        except ValueError:
            pass
            
    next_num = max(test_nums) + 1 if test_nums else 1
    log_path = os.path.join(test_dir, f"test_storage_{next_num}.txt")
    
    sys.stdout = DualLogger(log_path)
    return log_path

# --- Real Benchmarking Logic ---
def setup_test_tables(ch_client, pg_conn):
    ch_client.command("CREATE DATABASE IF NOT EXISTS atlas")
    ch_client.command("DROP TABLE IF EXISTS atlas.test_telemetry_daily_mv_benchmark")
    ch_client.command("DROP TABLE IF EXISTS atlas.test_mv_target_daily_benchmark")
    ch_client.command("DROP TABLE IF EXISTS atlas.test_telemetry_hourly_mv_benchmark")
    ch_client.command("DROP TABLE IF EXISTS atlas.test_mv_target_hourly_benchmark")
    ch_client.command("DROP TABLE IF EXISTS atlas.test_telemetry_benchmark")
    
    ch_client.command("""
        CREATE TABLE atlas.test_telemetry_benchmark (
            report_id String,
            device_id String,
            metric_time DateTime,
            MetricValue Float64,
            avg_metric_value Float64,
            max_metric_value Float64,
            min_metric_value Float64,
            insertion_time DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(insertion_time)
        ORDER BY (device_id, metric_time)
    """)
    ch_client.command("""
        CREATE TABLE atlas.test_mv_target_hourly_benchmark (
            device_id String,
            hour DateTime,
            avg_val AggregateFunction(avg, Float64),
            max_val AggregateFunction(max, Float64),
            min_val AggregateFunction(min, Float64),
            count_val AggregateFunction(count, Float64)
        ) ENGINE = AggregatingMergeTree()
        ORDER BY (device_id, hour)
        PARTITION BY toYYYYMM(hour)
    """)
    ch_client.command("""
        CREATE MATERIALIZED VIEW atlas.test_telemetry_hourly_mv_benchmark TO atlas.test_mv_target_hourly_benchmark AS
        SELECT 
            device_id, 
            toStartOfHour(metric_time) AS hour, 
            avgState(MetricValue) AS avg_val,
            maxState(MetricValue) AS max_val,
            minState(MetricValue) AS min_val,
            countState(MetricValue) AS count_val
        FROM atlas.test_telemetry_benchmark 
        GROUP BY device_id, hour
    """)
    ch_client.command("""
        CREATE TABLE atlas.test_mv_target_daily_benchmark (
            device_id String,
            day Date,
            avg_val AggregateFunction(avg, Float64),
            max_val AggregateFunction(max, Float64),
            total_count AggregateFunction(count, Float64)
        ) ENGINE = AggregatingMergeTree()
        ORDER BY (device_id, day)
        PARTITION BY toYYYYMM(day)
    """)
    ch_client.command("""
        CREATE MATERIALIZED VIEW atlas.test_telemetry_daily_mv_benchmark TO atlas.test_mv_target_daily_benchmark AS
        SELECT 
            device_id, 
            toDate(metric_time) AS day, 
            avgState(MetricValue) AS avg_val,
            maxState(MetricValue) AS max_val,
            countState(MetricValue) AS total_count
        FROM atlas.test_telemetry_benchmark 
        GROUP BY device_id, day
    """)

    with pg_conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_metadata_benchmark")
        cur.execute("""
            CREATE TABLE test_metadata_benchmark (
                device_id VARCHAR PRIMARY KEY,
                last_metric_time TIMESTAMP,
                last_loaded_at TIMESTAMP,
                rows_loaded INT
            )
        """)
    pg_conn.commit()

def teardown_test_tables(ch_client, pg_conn):
    try:
        ch_client.command("DROP TABLE IF EXISTS atlas.test_telemetry_daily_mv_benchmark")
        ch_client.command("DROP TABLE IF EXISTS atlas.test_mv_target_daily_benchmark")
        ch_client.command("DROP TABLE IF EXISTS atlas.test_telemetry_hourly_mv_benchmark")
        ch_client.command("DROP TABLE IF EXISTS atlas.test_mv_target_hourly_benchmark")
        ch_client.command("DROP TABLE IF EXISTS atlas.test_telemetry_benchmark")
        with pg_conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_metadata_benchmark")
        pg_conn.commit()
    except:
        pass

def generate_ch_chunk(devices, date_str):
    n_devices = len(devices)
    n_rows = n_devices * METRICS_PER_DAY
    device_ids = np.repeat(devices, METRICS_PER_DAY)
    
    # Vectorized fast mock UUIDs using integers (fitting within 32-bit limit for Windows)
    report_ids = np.random.randint(100000000, 999999999, size=n_rows).astype(str)
    
    # Vectorized fast datetimes (1 metric every 5 minutes = 288/day)
    base_time = pd.Timestamp(date_str)
    minutes_offset = np.tile(np.arange(METRICS_PER_DAY) * 5, n_devices)
    times = base_time + pd.to_timedelta(minutes_offset, unit='m')
    # Pandas naturally outputs standard ISO strings in to_csv for datetime columns
    
    vals = np.random.normal(loc=50.0, scale=10.0, size=n_rows)
    
    df = pd.DataFrame({
        "report_id": report_ids,
        "device_id": device_ids,
        "metric_time": times,
        "MetricValue": vals,
        "avg_metric_value": vals,
        "max_metric_value": vals + 5.0,
        "min_metric_value": vals - 5.0
    })
    return df

def execute_clickhouse_insert_native(ch_client, device_chunk, date_str):
    import time
    import requests
    start = time.time()
    
    df = generate_ch_chunk(device_chunk, date_str)
    csv_data = df.to_csv(index=False, header=False)
    
    session = requests.Session()
    for attempt in range(5):
        try:
            r = session.post(
                f"http://{CH_HOST}:{CH_PORT_HTTP}/?query=INSERT INTO atlas.test_telemetry_benchmark (report_id, device_id, metric_time, MetricValue, avg_metric_value, max_metric_value, min_metric_value) FORMAT CSV",
                data=csv_data,
                auth=(CH_USER, CH_PASS),
                timeout=30
            )
            if r.status_code != 200:
                raise Exception(f"ClickHouse insert failed: {r.text}")
            break
        except Exception as e:
            if attempt == 4:
                raise e
            time.sleep(1 + attempt)
    session.close()
    
    return time.time() - start

def execute_postgres_upsert(pg_conn, devices):
    start = time.time()
    from datetime import datetime
    now = datetime.now()
    
    with pg_conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO test_metadata_benchmark (device_id, last_metric_time, last_loaded_at, rows_loaded)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (device_id) DO UPDATE 
            SET last_metric_time = EXCLUDED.last_metric_time,
                last_loaded_at = EXCLUDED.last_loaded_at,
                rows_loaded = test_metadata_benchmark.rows_loaded + EXCLUDED.rows_loaded
            """,
            [(d, now, now, METRICS_PER_DAY) for d in devices]
        )
    pg_conn.commit()
    return time.time() - start

def execute_mv_lag_check(ch_client):
    start = time.time()
    res = ch_client.query("SELECT count() FROM atlas.test_mv_target_hourly_benchmark")
    _ = res.result_rows
    return time.time() - start

def measure_final_compaction(ch_client):
    start = time.time()
    ch_client.command("OPTIMIZE TABLE atlas.test_telemetry_benchmark FINAL")
    return time.time() - start

def run_benchmark():
    log_path = setup_logger()
    
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  ATLAS Storage Layer - ClickHouse Ingestion Engine")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
    print("Configuration:")
    print(f"  DEVICES={DEVICES}")
    print(f"  DAYS={DAYS}")
    print(f"  BATCH_SIZE={BATCH_SIZE}")
    print("  CLICKHOUSE_ENGINE=ReplacingMergeTree")
    print("  POSTGRES_METRICS=Enabled\n")
    
    print("[1/2] Connecting to Databases & Setting up schema...")
    ch_client = None
    pg_conn = None
    try:
        ch_client = clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT_HTTP, username=CH_USER, password=CH_PASS, send_receive_timeout=600
        )
        pg_conn = psycopg.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB
        )
        setup_test_tables(ch_client, pg_conn)
        print("  ✓ Connections established and tables created.\n")
    except Exception as e:
        print(f"  ✗ Connection/Setup failed: {e}")
        return

    print("================================================================================")
    print("ATLAS Storage Benchmark - Batch Ingestion Execution")
    print("================================================================================")
    print("Expected Results:")
    print(f"- Total raw input rows:   {TOTAL_ROWS:,}\n")

    ch_latencies = []
    pg_latencies = []
    mv_lags = []
    
    global_start = time.time()

    print("[INIT] Generating base device list...")
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime, timedelta
    
    all_device_ids = [f"dev_{i}" for i in range(1, DEVICES + 1)]
    device_batches = [all_device_ids[i:i + BATCH_SIZE] for i in range(0, len(all_device_ids), BATCH_SIZE)]
    start_date = datetime(2026, 2, 20)
    dates = [(start_date + timedelta(days=d)).strftime('%Y-%m-%d') for d in range(DAYS)]
    
    tasks = [(d_batch, date_str) for date_str in dates for d_batch in device_batches]
    actual_batch_count = len(tasks)
    batch_idx = 1
    
    N_WORKERS = 4
    clients = [
        clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT_HTTP, username=CH_USER, password=CH_PASS, send_receive_timeout=600)
        for _ in range(N_WORKERS)
    ]

    try:
        print("[START] Submitting batches to worker pool...")
        with ThreadPoolExecutor(max_workers=N_WORKERS) as executor:
            futures = {}
            for i, (d_batch, date_str) in enumerate(tasks):
                print(f"  -> Queuing BATCH {i+1}/{actual_batch_count} - {date_str} ({len(d_batch)} devices)")
                future = executor.submit(execute_clickhouse_insert_native, clients[i % N_WORKERS], d_batch, date_str)
                futures[future] = (d_batch, date_str)
            
            print("\n[WAIT] Processing batches concurrently (waiting for results)...")
            for future in as_completed(futures):
                d_batch, date_str = futures[future]
                b_start = time.time()
                
                # 1. ClickHouse Insert Result
                ch_lat = future.result()
                ch_latencies.append(ch_lat)
                
                # 2. Postgres Upsert Overhead (Serialized in main thread)
                pg_lat = execute_postgres_upsert(pg_conn, d_batch)
                pg_latencies.append(pg_lat)
                
                # 3. MV Lag Check (Read only)
                mv_lag = execute_mv_lag_check(ch_client)
                mv_lags.append(mv_lag)
                
                # We also factor latency measurements into batch throughput.
                b_elapsed = time.time() - b_start + ch_lat
                rows_this_batch = len(d_batch) * METRICS_PER_DAY
                throughput = rows_this_batch / max(b_elapsed, 1e-4)
                
                print(f"BATCH {batch_idx}/{actual_batch_count} - {date_str} ({len(d_batch)} devices) - CH: {ch_lat:.2f}s | PG: {pg_lat:.2f}s | MV Lag: {mv_lag:.2f}s | Throughput: {throughput:,.0f} rows/s")
                batch_idx += 1
                
        global_elapsed = time.time() - global_start
        overall_throughput = TOTAL_ROWS / global_elapsed

        print("--------------------------------------------------------------------------------")
        print("[FINAL COMPACTION] Running OPTIMIZE TABLE FINAL...")
        compaction_time = measure_final_compaction(ch_client)
        print(f"  ✓ Compaction completed in {compaction_time:.2f}s\n")

        print("--------------------------------------------------------------------------------")
        print("[FINAL] Verifying storage results & MV Lag...")
        print("--------------------------------------------------------------------------------")
        print("  Running Validation Queries...")
        
        ch_count = ch_client.query("SELECT count() FROM atlas.test_telemetry_benchmark").result_rows[0][0]
        hourly_mv_count = ch_client.query("SELECT countMerge(count_val) FROM atlas.test_mv_target_hourly_benchmark").result_rows[0][0]
        daily_mv_count = ch_client.query("SELECT countMerge(total_count) FROM atlas.test_mv_target_daily_benchmark").result_rows[0][0]
        
        pg_count = 0
        with pg_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM test_metadata_benchmark")
            pg_count = cur.fetchone()[0]
            
        print(f"  ✓ ClickHouse Telemetry Check: {ch_count:,} rows inserted")
        print(f"  ✓ ClickHouse Hourly MV Check: {hourly_mv_count:,} grouped rows")
        print(f"  ✓ ClickHouse Daily MV Check:  {daily_mv_count:,} grouped rows")
        print(f"  ✓ PostgreSQL Metadata Check: {pg_count:,} unique devices updated\n")

        print("================================================================================")
        print("  LATENCY & THROUGHPUT REPORT")
        print("================================================================================")
        
        print("  Overall Performance:")
        print(f"  - Total batches processed: {actual_batch_count}")
        print(f"  - Total rows processed:    {TOTAL_ROWS:,}")
        print(f"  - Total elapsed time:      {global_elapsed:.2f}s")
        print(f"  - Throughput:              {overall_throughput:,.1f} rows/sec\n")

        print("  ClickHouse Insert Latency (seconds):")
        print(f"  - Min: {np.min(ch_latencies):.2f}s, Max: {np.max(ch_latencies):.2f}s, Mean: {np.mean(ch_latencies):.2f}s, P50: {np.percentile(ch_latencies, 50):.2f}s, P95: {np.percentile(ch_latencies, 95):.2f}s, P99: {np.percentile(ch_latencies, 99):.2f}s\n")

        print("  PostgreSQL Upsert Latency (seconds):")
        print(f"  - Min: {np.min(pg_latencies):.2f}s, Max: {np.max(pg_latencies):.2f}s, Mean: {np.mean(pg_latencies):.2f}s, P50: {np.percentile(pg_latencies, 50):.2f}s, P95: {np.percentile(pg_latencies, 95):.2f}s, P99: {np.percentile(pg_latencies, 99):.2f}s\n")
        
        print("  Materialized View Optimization (seconds):")
        print(f"  - Min: {np.min(mv_lags):.2f}s, Max: {np.max(mv_lags):.2f}s, Mean: {np.mean(mv_lags):.2f}s\n")

        print("================================================================================")
        print("  ATLAS STORAGE LAYER BENCHMARK COMPLETE")
        print("================================================================================")
        
    finally:
        teardown_test_tables(ch_client, pg_conn)
        if ch_client:
            ch_client.close()
        if pg_conn:
            pg_conn.close()
        # Restore stdout
        sys.stdout = sys.stdout.terminal

if __name__ == "__main__":
    run_benchmark()

