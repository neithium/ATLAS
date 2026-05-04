import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, min, max
import time

def create_spark_session():
    """Builds a Spark Session pre-configured for Local Filesystem Analytics"""
    return SparkSession.builder \
        .appName("PowerPulse-Local-Analytics") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.driver.memory", "4g") \
        .master("local[*]") \
        .getOrCreate()

def run_7day_analysis():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("\n" + "="*50)
    print("🚀 LOCAL ANALYTICS: Batch Pull from /app/data/raw")
    print("="*50)
    
    start_time = time.monotonic()
    
    try:
        path = "/app/data/raw/"
        processed_output = "/app/data/processed/batch/"
        
        print(f"📡 Fetching PARQUET data from: {path}")
        
        # 1. FETCH: Read Parquet (Daily Archived Data)
        df = spark.read.parquet(path)
        
        if df.rdd.isEmpty():
            print("⚠️ No data found in raw directory.")
            return

        total_count = df.count()
        print(f"📊 Total Records Scanned: {total_count:,}")

        # 2. PROCESS: Aggregate
        print("\n📈 Sample Analysis (Average Watts per Device):")
        df.groupBy("device_id").agg(avg("data.Average").alias("avg_pwr")).show(10)
        
        # 3. SAVE PROCESSED: Route to /app/data/processed/batch
        print(f"💾 Saving processed results to: {processed_output}")
        df.write.mode("append").parquet(processed_output)
            
        end_time = time.monotonic()
        print(f"\n✅ Total Process Time: {end_time - start_time:.2f} seconds")
        print("="*50 + "\n")

    except Exception as e:
        print(f"❌ Error during Spark Batch Process: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    from pyspark.sql.functions import explode
    run_7day_analysis()
