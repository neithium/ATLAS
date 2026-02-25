"""
TVMJNS — PostgreSQL Connection Test & Database Initialization

Tests connection to PostgreSQL and optionally initializes the database schema.

Usage:
    python scripts/test_db.py           # Test connection only
    python scripts/test_db.py --init    # Test connection and initialize schema
"""

import argparse
import sys
from pathlib import Path

import psycopg
from psycopg import sql

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "streaming_user",
    "password": "streaming_pass",
    "dbname": "streaming_db",
}


def test_connection() -> bool:
    """Test PostgreSQL connection and print server info."""
    print("=" * 60)
    print("TVMJNS — PostgreSQL Connection Test")
    print("=" * 60)
    print(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"Database: {DB_CONFIG['dbname']}")
    print(f"User: {DB_CONFIG['user']}")
    print("=" * 60)

    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Get PostgreSQL version
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                print(f"\n✓ Connected successfully!\n")
                print(f"PostgreSQL Version:\n  {version}\n")

                # Get current database info
                cur.execute("""
                    SELECT 
                        current_database() as db,
                        current_user as user,
                        inet_server_addr() as server,
                        inet_server_port() as port
                """)
                info = cur.fetchone()
                print(f"Connection Info:")
                print(f"  Database: {info[0]}")
                print(f"  User: {info[1]}")
                print(f"  Server: {info[2]}:{info[3]}")

                # List existing tables
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                tables = cur.fetchall()
                print(f"\nExisting tables ({len(tables)}):")
                if tables:
                    for t in tables:
                        cur.execute(f"SELECT COUNT(*) FROM {t[0]}")
                        count = cur.fetchone()[0]
                        print(f"  • {t[0]} ({count} rows)")
                else:
                    print("  (none)")

        return True

    except psycopg.OperationalError as e:
        print(f"\n✗ Connection failed!\n")
        print(f"Error: {e}")
        print("\nTroubleshooting:")
        print("  1. Is Docker running? → docker compose ps")
        print("  2. Is PostgreSQL container healthy? → docker logs postgres")
        print("  3. Check credentials in .env file")
        return False

    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        return False


def initialize_database() -> bool:
    """Run the init_db.sql script to create tables."""
    print("\n" + "=" * 60)
    print("Initializing Database Schema...")
    print("=" * 60)

    # Find the SQL file
    script_dir = Path(__file__).parent
    sql_file = script_dir / "init_db.sql"

    if not sql_file.exists():
        print(f"✗ SQL file not found: {sql_file}")
        return False

    print(f"Running: {sql_file.name}")

    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Read and execute the SQL file
                sql_content = sql_file.read_text(encoding="utf-8")
                cur.execute(sql_content)
                conn.commit()

        print("✓ Database initialized successfully!\n")

        # Verify tables were created
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                tables = cur.fetchall()
                print(f"Tables created ({len(tables)}):")
                for t in tables:
                    cur.execute(f"SELECT COUNT(*) FROM {t[0]}")
                    count = cur.fetchone()[0]
                    print(f"  ✓ {t[0]} ({count} rows)")

        return True

    except psycopg.errors.DuplicateTable:
        print("⚠ Tables already exist (skipping)")
        return True

    except Exception as e:
        print(f"✗ Failed to initialize database: {e}")
        return False


def insert_sample_data() -> bool:
    """Insert some sample telemetry readings for testing."""
    print("\n" + "=" * 60)
    print("Inserting Sample Data...")
    print("=" * 60)

    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Insert sample telemetry readings
                cur.execute("""
                    INSERT INTO telemetry_readings 
                        (sensor_id, recorded_at, temperature, humidity, pressure, battery_level)
                    VALUES 
                        ('sensor_001', NOW() - INTERVAL '5 minutes', 22.5, 45.0, 1013.25, 85.0),
                        ('sensor_001', NOW() - INTERVAL '4 minutes', 22.7, 44.8, 1013.30, 84.9),
                        ('sensor_001', NOW() - INTERVAL '3 minutes', 22.8, 44.5, 1013.28, 84.8),
                        ('sensor_002', NOW() - INTERVAL '5 minutes', 24.1, 52.0, 1012.50, 92.0),
                        ('sensor_002', NOW() - INTERVAL '4 minutes', 24.3, 51.8, 1012.55, 91.9),
                        ('sensor_003', NOW() - INTERVAL '3 minutes', 19.5, 60.0, 1014.00, 78.0)
                    ON CONFLICT DO NOTHING
                """)

                # Insert sample alert
                cur.execute("""
                    INSERT INTO alerts 
                        (sensor_id, alert_type, severity, message, triggered_at)
                    VALUES 
                        ('sensor_003', 'low_battery', 'warning', 
                         'Battery level dropped below 80%', NOW() - INTERVAL '1 hour')
                    ON CONFLICT DO NOTHING
                """)

                conn.commit()

                # Count inserted rows
                cur.execute("SELECT COUNT(*) FROM telemetry_readings")
                readings = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM alerts")
                alerts = cur.fetchone()[0]

                print(f"✓ Sample data inserted:")
                print(f"  • {readings} telemetry readings")
                print(f"  • {alerts} alerts")

        return True

    except Exception as e:
        print(f"✗ Failed to insert sample data: {e}")
        return False


def query_sample_data():
    """Run some sample queries to demonstrate the data."""
    print("\n" + "=" * 60)
    print("Sample Queries")
    print("=" * 60)

    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Latest readings per sensor
                print("\n  Latest Reading Per Sensor:")
                print("-" * 70)
                cur.execute("""
                    SELECT sensor_id, recorded_at, temperature, humidity, battery_level
                    FROM v_latest_readings
                    ORDER BY sensor_id
                """)
                for row in cur.fetchall():
                    print(f"  {row[0]}: {row[2]}°C, {row[3]}% humidity, {row[4]}% battery")

                # Sensor list
                print("\n📍 Registered Sensors:")
                print("-" * 70)
                cur.execute("SELECT sensor_id, name, location, status FROM sensors ORDER BY sensor_id")
                for row in cur.fetchall():
                    print(f"  {row[0]}: {row[1]} @ {row[2]} [{row[3]}]")

                # Active alerts
                print("\n🚨 Active Alerts:")
                print("-" * 70)
                cur.execute("SELECT * FROM v_active_alerts")
                alerts = cur.fetchall()
                if alerts:
                    for row in alerts:
                        print(f"  [{row[4]}] {row[1]}: {row[6]}")
                else:
                    print("  (no active alerts)")

    except Exception as e:
        print(f"Query error: {e}")


def main():
    parser = argparse.ArgumentParser(description="PostgreSQL connection test and initialization")
    parser.add_argument("--init", action="store_true", help="Initialize database schema")
    parser.add_argument("--sample", action="store_true", help="Insert sample data")
    args = parser.parse_args()

    # Test connection
    if not test_connection():
        sys.exit(1)

    # Initialize if requested
    if args.init:
        if not initialize_database():
            sys.exit(1)

    # Insert sample data if requested
    if args.sample:
        if not insert_sample_data():
            sys.exit(1)

    # Show sample queries if we have data
    if args.init or args.sample:
        query_sample_data()

    print("\n" + "=" * 60)
    print("✓ All tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
