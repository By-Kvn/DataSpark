import os
from  pathlib import Path

from dotenv import load_dotenv
from minio import Minio

load_dotenv()

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"

# Database configuration
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./data/database/analytics.db")

# Prefect configuration
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")

# Buckets
BUCKET_SOURCES = "sources"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"

def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key = MINIO_ACCESS_KEY,
        secret_key = MINIO_SECRET_KEY,
        secure = MINIO_SECURE
    )

def configure_prefect() -> None:
    os.environ["PREFECT_API_URL"] = PREFECT_API_URL

# Spark configuration
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://localhost:7077")

def get_spark_session(app_name: str = "DataSpark"):
    """
    Create and return a SparkSession configured for the cluster.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession instance
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_MASTER_URL) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    return spark

if __name__ == "__main__":
    client = get_minio_client()
    print(client.list_buckets())

    configure_prefect()
    print(os.getenv("PREFECT_API_URL"))
