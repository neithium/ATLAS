"""
ATLAS Delta Lake Module - Streaming Data Producer
================================================================================
Real-time telemetry data producer for Spark Structured Streaming integration.

Producer Modes:
1. kafka  - Produce to Kafka topic (production)
2. socket - Produce to TCP socket (testing)
3. file   - Produce to streaming file sink (testing)

Data Pattern: Same 7-day rolling window pattern as batch generator
- Simulates real-time arrival of telemetry data
- Pre-flattened schema matching downstream expectations
- Configurable throughput and device count

Usage:
    # Kafka producer
    python streaming_data_producer.py --mode kafka --topic atlas.telemetry.flattened
    
    # Socket producer (for testing)
    python streaming_data_producer.py --mode socket --port 9999
    
    # File producer (for testing)
    python streaming_data_producer.py --mode file --output /raw/streaming
"""

import argparse
import json
import math
import os
import random
import signal
import socket
import sys
import time
from datetime import datetime, timedelta
from threading import Thread, Event
from typing import Optional, Generator, List, Dict, Any

# Optional Kafka import
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    KafkaProducer = None

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, pmod, element_at, array, format_string,
    to_timestamp, date_format, floor, hour, minute, sin,
    broadcast, dayofyear, explode, sequence, expr, to_date
)


# =============================================================================
# CONFIGURATION
# =============================================================================

class ProducerConfig:
    """Configuration for streaming data producer."""
    
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "atlas.telemetry.flattened")
    
    # Socket settings
    SOCKET_HOST = os.getenv("SOCKET_HOST", "0.0.0.0")
    SOCKET_PORT = int(os.getenv("SOCKET_PORT", "9999"))
    
    # File settings
    FILE_OUTPUT_PATH = os.getenv("FILE_OUTPUT_PATH", "/raw/streaming")
    
    # Data generation settings
    TOTAL_DEVICES = int(os.getenv("TOTAL_DEVICES", "1000"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
    RECORDS_PER_SECOND = int(os.getenv("RECORDS_PER_SECOND", "1000"))
    
    # Simulation settings
    SIMULATE_7DAY_WINDOW = os.getenv("SIMULATE_7DAY_WINDOW", "false").lower() == "true"


# =============================================================================
# SPARK SESSION FOR DATAFRAME GENERATION
# =============================================================================

def create_spark_session() -> SparkSession:
    """Create SparkSession for DataFrame-based generation."""
    spark = (
        SparkSession.builder
        .appName("ATLAS-StreamingDataProducer")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# DATAFRAME-BASED DATA GENERATOR
# =============================================================================

def build_metric_profile(spark: SparkSession):
    """Create reusable 288-point daily profile (5-min slots)."""
    return (
        spark.range(288)
        .withColumnRenamed("id", "slot_index")
        .withColumn(
            "base_metric",
            (lit(220.0) + lit(45.0) * sin(col("slot_index") / lit(14.0)) + 
             (pmod(col("slot_index"), lit(17)) * lit(0.85))).cast("double"),
        )
    )


def generate_streaming_dataframe(
    spark: SparkSession,
    device_start: int,
    device_end: int,
    base_time: datetime,
    profile_df
) -> DataFrame:
    """
    Generate a DataFrame of telemetry records for streaming.
    
    This creates a batch of records that can be:
    - Sent to Kafka
    - Written to file
    - Sent over socket
    
    Returns a DataFrame with the flattened telemetry schema.
    """
    # Build device dimension
    device_df = (
        spark.range(device_start, device_end)
        .withColumnRenamed("id", "device_num")
        .withColumn("device_id", format_string("SRV-%06d", col("device_num") + lit(1)))
        .withColumn(
            "application_customer_id",
            element_at(
                array(
                    lit("APP-001"), lit("APP-017"), lit("APP-113"),
                    lit("APP-226"), lit("APP-67890"),
                ),
                (pmod(col("device_num"), lit(5)) + lit(1)).cast("int"),
            ),
        )
        .withColumn(
            "platform_customer_id",
            element_at(
                array(
                    lit("PLAT-001"), lit("PLAT-021"), lit("PLAT-101"),
                    lit("PLAT-12345"), lit("PLAT-907"),
                ),
                (pmod(col("device_num"), lit(5)) + lit(1)).cast("int"),
            ),
        )
        .withColumn(
            "report_type",
            element_at(
                array(
                    lit("power_metrics"), lit("thermal_metrics"),
                    lit("cpu_metrics"), lit("sustainability_metrics"),
                ),
                (pmod(col("device_num"), lit(4)) + lit(1)).cast("int"),
            ),
        )
    )

    # Single timestamp for this batch (current time)
    base_ts = base_time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Calculate slot index for metric profile
    slot_index = (base_time.hour * 12 + base_time.minute // 5) % 288

    # Add metric time and value
    result_df = (
        device_df
        .withColumn("metric_time", to_timestamp(lit(base_ts)))
        .withColumn("slot_index", lit(slot_index))
        .join(broadcast(profile_df), on="slot_index", how="left")
        .withColumn(
            "MetricValue",
            (col("base_metric") + 
             (pmod(col("device_num"), lit(19)) * lit(0.22)) +
             (pmod(lit(dayofyear(to_date(lit(base_ts)))), lit(7)) * lit(0.31))
            ).cast("double"),
        )
        .withColumn("partition_date", date_format(col("metric_time"), "yyyy-MM-dd"))
        .select(
            "device_id", "metric_time", "application_customer_id",
            "platform_customer_id", "report_type", "MetricValue", "partition_date"
        )
    )
    
    return result_df


def generate_batch_dataframe(
    spark: SparkSession,
    total_devices: int,
    batch_size: int,
    file_date: datetime
) -> DataFrame:
    """
    Generate a complete batch DataFrame for batch processing.
    
    This is for the batch pipeline that now accepts DataFrames instead of files.
    Creates a 7-day rolling window of data (2016 rows per device).
    
    Args:
        spark: SparkSession
        total_devices: Total number of devices
        batch_size: Devices per processing batch
        file_date: The date this batch was "received"
    
    Returns:
        DataFrame with complete batch data
    """
    profile_df = build_metric_profile(spark)
    
    # 7-day window
    window_start = file_date - timedelta(days=6)
    window_end = file_date + timedelta(hours=23, minutes=55)
    
    start_ts = window_start.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = window_end.strftime("%Y-%m-%d %H:%M:%S")
    
    # Build device dimension
    device_df = (
        spark.range(0, total_devices)
        .withColumnRenamed("id", "device_num")
        .withColumn("device_id", format_string("SRV-%06d", col("device_num") + lit(1)))
        .withColumn(
            "application_customer_id",
            element_at(
                array(
                    lit("APP-001"), lit("APP-017"), lit("APP-113"),
                    lit("APP-226"), lit("APP-67890"),
                ),
                (pmod(col("device_num"), lit(5)) + lit(1)).cast("int"),
            ),
        )
        .withColumn(
            "platform_customer_id",
            element_at(
                array(
                    lit("PLAT-001"), lit("PLAT-021"), lit("PLAT-101"),
                    lit("PLAT-12345"), lit("PLAT-907"),
                ),
                (pmod(col("device_num"), lit(5)) + lit(1)).cast("int"),
            ),
        )
        .withColumn(
            "report_type",
            element_at(
                array(
                    lit("power_metrics"), lit("thermal_metrics"),
                    lit("cpu_metrics"), lit("sustainability_metrics"),
                ),
                (pmod(col("device_num"), lit(4)) + lit(1)).cast("int"),
            ),
        )
    )

    # Build time dimension - 7 days at 5-minute intervals = 2016 timestamps
    time_df = spark.range(1).select(
        explode(
            sequence(
                to_timestamp(lit(start_ts)),
                to_timestamp(lit(end_ts)),
                expr("INTERVAL 5 MINUTES"),
            )
        ).alias("metric_time")
    )

    # Cartesian product: devices × timestamps
    expanded_df = device_df.crossJoin(time_df)

    # Calculate slot index for metric profile lookup
    slot_index_expr = (hour(col("metric_time")) * lit(12) + 
                       floor(minute(col("metric_time")) / lit(5))).cast("long")

    # Join with metric profile and compute values
    result_df = (
        expanded_df
        .withColumn("slot_index", slot_index_expr)
        .join(broadcast(profile_df), on="slot_index", how="left")
        .withColumn(
            "MetricValue",
            (col("base_metric") +
             (pmod(col("device_num"), lit(19)) * lit(0.22)) +
             (pmod(dayofyear(col("metric_time")), lit(7)) * lit(0.31))
            ).cast("double"),
        )
        .withColumn("file_date", lit(file_date.strftime("%Y-%m-%d")))
        .withColumn("partition_date", date_format(to_date(col("metric_time")), "yyyy-MM-dd"))
        .select(
            "device_id", "metric_time", "application_customer_id",
            "platform_customer_id", "report_type", "file_date",
            "partition_date", "MetricValue"
        )
    )
    
    return result_df


# =============================================================================
# PURE PYTHON RECORD GENERATOR (For Socket/Kafka)
# =============================================================================

def generate_telemetry_record(
    device_num: int,
    metric_time: datetime
) -> Dict[str, Any]:
    """Generate a single telemetry record as a dictionary."""
    
    apps = ["APP-001", "APP-017", "APP-113", "APP-226", "APP-67890"]
    platforms = ["PLAT-001", "PLAT-021", "PLAT-101", "PLAT-12345", "PLAT-907"]
    report_types = ["power_metrics", "thermal_metrics", "cpu_metrics", "sustainability_metrics"]
    
    # Deterministic selection based on device_num
    app_id = apps[device_num % 5]
    platform_id = platforms[device_num % 5]
    report_type = report_types[device_num % 4]
    
    # Compute metric value (simplified sine wave pattern)
    slot_index = metric_time.hour * 12 + metric_time.minute // 5
    base_metric = 220.0 + 45.0 * math.sin(slot_index / 14.0) + (slot_index % 17) * 0.85
    metric_value = base_metric + (device_num % 19) * 0.22 + (metric_time.timetuple().tm_yday % 7) * 0.31
    
    return {
        "device_id": f"SRV-{device_num + 1:06d}",
        "metric_time": metric_time.isoformat(),
        "application_customer_id": app_id,
        "platform_customer_id": platform_id,
        "report_type": report_type,
        "MetricValue": round(metric_value, 2),
        "partition_date": metric_time.strftime("%Y-%m-%d")
    }


def record_generator(
    total_devices: int,
    records_per_second: int
) -> Generator[Dict[str, Any], None, None]:
    """
    Infinite generator yielding telemetry records at specified rate.
    
    Cycles through devices and generates current-time metrics.
    """
    device_idx = 0
    interval = 1.0 / records_per_second if records_per_second > 0 else 0.001
    
    while True:
        metric_time = datetime.now()
        record = generate_telemetry_record(device_idx, metric_time)
        
        yield record
        
        device_idx = (device_idx + 1) % total_devices
        time.sleep(interval)


# =============================================================================
# KAFKA PRODUCER
# =============================================================================

class KafkaStreamProducer:
    """Kafka producer for streaming telemetry data."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        total_devices: int,
        records_per_second: int
    ):
        if not KAFKA_AVAILABLE:
            raise ImportError("kafka-python not installed. Run: pip install kafka-python")
        
        self.topic = topic
        self.total_devices = total_devices
        self.records_per_second = records_per_second
        self.stop_event = Event()
        self.records_sent = 0
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            compression_type='gzip',
            batch_size=16384,
            linger_ms=10,
            acks='all'
        )
        
        print(f"[Kafka] Connected to {bootstrap_servers}")
        print(f"[Kafka] Topic: {topic}")
    
    def produce(self):
        """Start producing records to Kafka."""
        print(f"[Kafka] Starting producer: {self.records_per_second} records/sec")
        
        gen = record_generator(self.total_devices, self.records_per_second)
        
        start_time = time.time()
        last_report = start_time
        
        try:
            for record in gen:
                if self.stop_event.is_set():
                    break
                
                # Use device_id as key for partition affinity
                self.producer.send(
                    self.topic,
                    key=record["device_id"],
                    value=record
                )
                self.records_sent += 1
                
                # Report progress every 10 seconds
                now = time.time()
                if now - last_report >= 10:
                    elapsed = now - start_time
                    rate = self.records_sent / elapsed if elapsed > 0 else 0
                    print(f"[Kafka] Sent {self.records_sent:,} records | Rate: {rate:.1f}/s")
                    last_report = now
                    
        except Exception as e:
            print(f"[Kafka] Error: {e}")
        finally:
            self.producer.flush()
            self.producer.close()
            print(f"[Kafka] Producer stopped. Total sent: {self.records_sent:,}")
    
    def stop(self):
        """Signal producer to stop."""
        self.stop_event.set()


# =============================================================================
# SOCKET PRODUCER
# =============================================================================

class SocketStreamProducer:
    """Socket server for streaming telemetry data via TCP."""
    
    def __init__(
        self,
        host: str,
        port: int,
        total_devices: int,
        records_per_second: int
    ):
        self.host = host
        self.port = port
        self.total_devices = total_devices
        self.records_per_second = records_per_second
        self.stop_event = Event()
        self.server_socket = None
        self.client_socket = None
        self.records_sent = 0
    
    def start_server(self):
        """Start the socket server and begin producing."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(1)
        self.server_socket.settimeout(5.0)
        
        print(f"[Socket] Server listening on {self.host}:{self.port}")
        print(f"[Socket] Waiting for client connection...")
        
        while not self.stop_event.is_set():
            try:
                self.client_socket, addr = self.server_socket.accept()
                print(f"[Socket] Client connected from {addr}")
                self._produce_to_client()
            except socket.timeout:
                continue
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"[Socket] Error: {e}")
    
    def _produce_to_client(self):
        """Send records to connected client."""
        print(f"[Socket] Starting producer: {self.records_per_second} records/sec")
        
        gen = record_generator(self.total_devices, self.records_per_second)
        
        start_time = time.time()
        last_report = start_time
        
        try:
            for record in gen:
                if self.stop_event.is_set():
                    break
                
                # Send as newline-delimited JSON
                data = json.dumps(record) + "\n"
                self.client_socket.sendall(data.encode('utf-8'))
                self.records_sent += 1
                
                # Report progress every 10 seconds
                now = time.time()
                if now - last_report >= 10:
                    elapsed = now - start_time
                    rate = self.records_sent / elapsed if elapsed > 0 else 0
                    print(f"[Socket] Sent {self.records_sent:,} records | Rate: {rate:.1f}/s")
                    last_report = now
                    
        except (BrokenPipeError, ConnectionResetError):
            print(f"[Socket] Client disconnected")
        except Exception as e:
            print(f"[Socket] Error: {e}")
        finally:
            if self.client_socket:
                self.client_socket.close()
            print(f"[Socket] Session ended. Records sent: {self.records_sent:,}")
    
    def stop(self):
        """Signal server to stop."""
        self.stop_event.set()
        if self.server_socket:
            self.server_socket.close()


# =============================================================================
# FILE STREAM PRODUCER (For Spark File Source Testing)
# =============================================================================

class FileStreamProducer:
    """File-based producer for Spark file source streaming."""
    
    def __init__(
        self,
        output_path: str,
        total_devices: int,
        batch_size: int,
        interval_seconds: int = 30
    ):
        self.output_path = output_path
        self.total_devices = total_devices
        self.batch_size = batch_size
        self.interval_seconds = interval_seconds
        self.stop_event = Event()
        self.batches_written = 0
        self.spark = None
    
    def start(self):
        """Start producing file batches."""
        os.makedirs(self.output_path, exist_ok=True)
        
        self.spark = create_spark_session()
        profile_df = build_metric_profile(self.spark)
        
        print(f"[File] Output path: {self.output_path}")
        print(f"[File] Writing batches every {self.interval_seconds}s")
        
        device_idx = 0
        
        while not self.stop_event.is_set():
            batch_start = time.time()
            
            # Generate batch DataFrame
            device_end = min(device_idx + self.batch_size, self.total_devices)
            batch_time = datetime.now()
            
            batch_df = generate_streaming_dataframe(
                self.spark,
                device_idx,
                device_end,
                batch_time,
                profile_df
            )
            
            # Write as JSON for easy parsing
            output_file = f"{self.output_path}/batch_{self.batches_written:06d}.json"
            batch_df.write.mode("overwrite").json(output_file)
            
            row_count = batch_df.count()
            self.batches_written += 1
            
            print(f"[File] Batch {self.batches_written}: {row_count} rows -> {output_file}")
            
            # Cycle through devices
            device_idx = device_end % self.total_devices
            
            # Wait for next interval
            elapsed = time.time() - batch_start
            sleep_time = max(0, self.interval_seconds - elapsed)
            
            if sleep_time > 0 and not self.stop_event.is_set():
                time.sleep(sleep_time)
        
        self.spark.stop()
        print(f"[File] Producer stopped. Batches written: {self.batches_written}")
    
    def stop(self):
        """Signal producer to stop."""
        self.stop_event.set()


# =============================================================================
# SPARK STREAMING DATA SOURCE (DataFrame API)
# =============================================================================

def create_streaming_rate_dataframe(
    spark: SparkSession,
    rows_per_second: int = 1000,
    num_partitions: int = 4
) -> DataFrame:
    """
    Create a Spark Structured Streaming DataFrame using rate source.
    
    This is a DataFrame-based streaming source that generates synthetic
    telemetry data for testing the streaming merge pipeline.
    
    Args:
        spark: SparkSession
        rows_per_second: Data generation rate
        num_partitions: Number of output partitions
    
    Returns:
        Streaming DataFrame with telemetry schema
    """
    from pyspark.sql.functions import (
        when, concat_ws, current_timestamp
    )
    
    rate_df = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", rows_per_second)
        .option("numPartitions", num_partitions)
        .load()
    )
    
    # Transform rate output to telemetry schema
    telemetry_df = (
        rate_df
        .withColumn("device_id", 
                    concat_ws("-", lit("SRV"), 
                             format_string("%06d", (col("value") % 1000) + lit(1))))
        .withColumn("metric_time", col("timestamp"))
        .withColumn("application_customer_id",
                    when(col("value") % 5 == 0, lit("APP-001"))
                    .when(col("value") % 5 == 1, lit("APP-017"))
                    .when(col("value") % 5 == 2, lit("APP-113"))
                    .when(col("value") % 5 == 3, lit("APP-226"))
                    .otherwise(lit("APP-67890")))
        .withColumn("platform_customer_id",
                    when(col("value") % 5 == 0, lit("PLAT-001"))
                    .when(col("value") % 5 == 1, lit("PLAT-021"))
                    .when(col("value") % 5 == 2, lit("PLAT-101"))
                    .when(col("value") % 5 == 3, lit("PLAT-12345"))
                    .otherwise(lit("PLAT-907")))
        .withColumn("report_type",
                    when(col("value") % 4 == 0, lit("power_metrics"))
                    .when(col("value") % 4 == 1, lit("thermal_metrics"))
                    .when(col("value") % 4 == 2, lit("cpu_metrics"))
                    .otherwise(lit("sustainability_metrics")))
        .withColumn("MetricValue", 
                    (220.0 + (col("value") % 100) * 0.5).cast("double"))
        .withColumn("partition_date", date_format(col("timestamp"), "yyyy-MM-dd"))
        .select(
            "device_id", "metric_time", "application_customer_id",
            "platform_customer_id", "report_type", "MetricValue", "partition_date"
        )
    )
    
    return telemetry_df


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="ATLAS Streaming Data Producer")
    parser.add_argument("--mode", type=str, choices=["kafka", "socket", "file", "dataframe"],
                        default="socket", help="Producer mode")
    parser.add_argument("--devices", type=int, default=1000, help="Total devices to simulate")
    parser.add_argument("--rate", type=int, default=1000, help="Records per second")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size (for file mode)")
    
    # Kafka options
    parser.add_argument("--kafka-servers", type=str, 
                        default=ProducerConfig.KAFKA_BOOTSTRAP_SERVERS,
                        help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default=ProducerConfig.KAFKA_TOPIC,
                        help="Kafka topic")
    
    # Socket options
    parser.add_argument("--host", type=str, default=ProducerConfig.SOCKET_HOST,
                        help="Socket host")
    parser.add_argument("--port", type=int, default=ProducerConfig.SOCKET_PORT,
                        help="Socket port")
    
    # File options
    parser.add_argument("--output", type=str, default=ProducerConfig.FILE_OUTPUT_PATH,
                        help="Output path for file mode")
    parser.add_argument("--interval", type=int, default=30,
                        help="Batch interval in seconds (file mode)")
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    print("\n" + "=" * 80)
    print("  ATLAS - STREAMING DATA PRODUCER")
    print("=" * 80)
    print(f"\n  Mode: {args.mode.upper()}")
    print(f"  Devices: {args.devices:,}")
    print(f"  Rate: {args.rate:,} records/sec")
    
    producer = None
    
    def signal_handler(signum, frame):
        print("\n\nReceived shutdown signal...")
        if producer:
            producer.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        if args.mode == "kafka":
            producer = KafkaStreamProducer(
                bootstrap_servers=args.kafka_servers,
                topic=args.topic,
                total_devices=args.devices,
                records_per_second=args.rate
            )
            producer.produce()
            
        elif args.mode == "socket":
            producer = SocketStreamProducer(
                host=args.host,
                port=args.port,
                total_devices=args.devices,
                records_per_second=args.rate
            )
            producer.start_server()
            
        elif args.mode == "file":
            producer = FileStreamProducer(
                output_path=args.output,
                total_devices=args.devices,
                batch_size=args.batch_size,
                interval_seconds=args.interval
            )
            producer.start()
            
        elif args.mode == "dataframe":
            # Demo DataFrame generation
            spark = create_spark_session()
            print("\n[DataFrame] Generating sample batch...")
            
            df = generate_batch_dataframe(
                spark=spark,
                total_devices=10,  # Small demo
                batch_size=10,
                file_date=datetime.now()
            )
            
            print(f"\n[DataFrame] Generated {df.count()} rows")
            print("[DataFrame] Schema:")
            df.printSchema()
            print("\n[DataFrame] Sample data:")
            df.show(5, truncate=False)
            
            spark.stop()
            
    except Exception as e:
        print(f"\nError: {e}")
        raise


if __name__ == "__main__":
    main()
