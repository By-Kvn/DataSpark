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

def _setup_java_home():
    """
    Automatically configure JAVA_HOME if not set.
    Tries to find Java installed via Homebrew.
    """
    if not os.getenv("JAVA_HOME"):
        try:
            # Try to find Java via Homebrew
            import subprocess
            result = subprocess.run(
                ["brew", "--prefix", "openjdk@17"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                java_path = result.stdout.strip()
                java_home = f"{java_path}/libexec/openjdk.jdk/Contents/Home"
                if os.path.exists(java_home):
                    os.environ["JAVA_HOME"] = java_home
                    print(f"✅ JAVA_HOME auto-configured: {java_home}")
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
            # If brew is not available or fails, try system Java
            try:
                result = subprocess.run(
                    ["/usr/libexec/java_home", "-v", "17"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    java_home = result.stdout.strip()
                    os.environ["JAVA_HOME"] = java_home
                    print(f"✅ JAVA_HOME auto-configured: {java_home}")
            except Exception:
                pass

def get_spark_session(app_name: str = "DataSpark", use_cluster: bool = False):
    """
    Create and return a SparkSession configured for the cluster or local mode.
    
    Args:
        app_name: Name of the Spark application
        use_cluster: If True, try to connect to cluster. If False or cluster unavailable, use local mode.
        
    Returns:
        SparkSession instance
    """
    # Auto-configure JAVA_HOME if not set
    _setup_java_home()
    
    from pyspark.sql import SparkSession
    
    builder = SparkSession.builder.appName(app_name)
    
    if use_cluster:
        # Try to connect to cluster
        try:
            builder = builder.master(SPARK_MASTER_URL)
        except:
            # Fallback to local mode
            builder = builder.master("local[*]")
    else:
        # Use local mode (default for testing)
        builder = builder.master("local[*]")
    
    spark = builder \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    return spark

if __name__ == "__main__":
    client = get_minio_client()
    print(client.list_buckets())

    configure_prefect()
    print(os.getenv("PREFECT_API_URL"))
