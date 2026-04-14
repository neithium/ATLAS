from pyspark.sql import SparkSession
from pyspark.sql.functions import max

def verify():
    spark = SparkSession.builder \
        .appName("Archive-Verification") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://ingestion:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    print("\n" + "="*50)
    print("📋 MINIO ARCHIVE TIMESTAMP CHECK")
    print("="*50)
    
    path = "s3a://telemetry-archive/production/"
    df = spark.read.parquet(path)
    
    print("\n📅 LATEST DATA FOUND IN ARCHIVE:")
    df.select(max("created_at")).show()
    
    print("✅ Verification Complete.")
    print("="*50 + "\n")
    spark.stop()

if __name__ == "__main__":
    verify()
