import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, min, max
import time

def create_spark_session():
    """Builds a Spark Session pre-configured for MinIO (S3A) Connectivity"""
    return SparkSession.builder \
        .appName("PowerPulse-MinIO-Analytics") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://ingestion:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def run_7day_analysis():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("\n" + "="*50)
    print("🚀 STORAGE ANALYTICS: 7-Day Rolling Analysis")
    print("="*50)
    
    start_time = time.time()
    
    # 1. FETCH: Spark reads from MinIO
    try:
        # Note: We use the endpoint 'ingestion' as defined in docker-compose
        path = "s3a://telemetry-archive/production/"
        print(f"📡 Fetching data from: {path}")
        
        df = spark.read.parquet(path)
        
        # COLUMN PRUNING: Only fetch the 3 columns we need
        df_minimal = df.select("device_id", col("data.Average").alias("watts"), "created_at")
        
        total_count = df_minimal.count()
        print(f"📊 Total Records Scanned: {total_count:,}")
        
        if total_count > 0:
            # Simple aggregation to prove it's working
            df_minimal.groupBy("device_id").agg(avg("watts").alias("avg_pwr")).show(5)
            
        end_time = time.time()
        print(f"\n✅ Total Fetch + Aggregate Time: {end_time - start_time:.2f} seconds")
        print("="*50 + "\n")

    except Exception as e:
        print(f"❌ Error during Spark Fetch: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_7day_analysis()
