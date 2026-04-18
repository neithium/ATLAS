import psycopg2
import os

def init():
    conn = psycopg2.connect(
        host=os.getenv('TSDB_HOST', 'localhost'),
        port=int(os.getenv('TSDB_PORT', '5432')),
        user=os.getenv('TSDB_USER', 'postgres'),
        password=os.getenv('TSDB_PASS', 'postgres'),
        dbname=os.getenv('TSDB_NAME', 'postgres')
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # 1. Create Base Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_live (
            metric_time TIMESTAMPTZ NOT NULL,
            device_id TEXT NOT NULL,
            platform_customer_id TEXT,
            application_customer_id TEXT,
            amb_temp FLOAT,
            avg_watts FLOAT,
            cpu_avg_freq BIGINT,
            cpu_max INT,
            cpu_pwr_sav_lim INT,
            cpu_util INT,
            cpu_watts INT,
            gpu_watts INT,
            min_watts INT,
            peak_watts INT,
            server_name TEXT,
            model TEXT,
            processor_vendor TEXT,
            server_generation TEXT,
            report_type TEXT,
            metric_type TEXT,
            status BOOLEAN,
            error_reason TEXT,
            tags TEXT,
            location_id TEXT,
            location_city TEXT,
            location_state TEXT,
            location_country TEXT,
            location_name TEXT
        );
    """)
    
    # 2. Convert to Hypertable (TimescaleDB)
    try:
        cur.execute("SELECT create_hypertable('telemetry_live', 'metric_time', if_not_exists => TRUE);")
    except Exception as e:
        print(f"Hypertable exists or error: {e}")
        
    # 3. Create Optimized Hierarchical Indexes
    # Enables alternative query paths: filter by device_id or by application_customer_id
    # We include platform_customer_id as the leading column to speed up PCID/ACID API lookups
    cur.execute("DROP INDEX IF EXISTS idx_device_time;")
    cur.execute("DROP INDEX IF EXISTS idx_acid_time;")
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_device_time ON telemetry_live (device_id, metric_time DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_acid_time ON telemetry_live (application_customer_id, metric_time DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pcid_acid_time ON telemetry_live (platform_customer_id, application_customer_id, metric_time DESC);")
    
    print("V2 TSDB (Scale-Ready) Initialized!")
    cur.close()
    conn.close()

if __name__ == "__main__":
    init()

