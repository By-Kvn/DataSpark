"""
Silver Transformation Flow using PySpark instead of Pandas.
This version uses Spark for distributed processing.
"""
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, count, mean, stddev, min as spark_min, max as spark_max, to_date, date_format, trim, upper, lit
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType
from datetime import datetime

from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client, get_spark_session


@task(name="download_from_bronze_spark", retries=2)
def download_from_bronze_spark(spark: SparkSession, object_name: str) -> DataFrame:
    """
    Download CSV file from bronze bucket and load as Spark DataFrame.

    Args:
        spark: SparkSession instance
        object_name: Name of object in bronze bucket

    Returns:
        Spark DataFrame with raw data
    """
    client = get_minio_client()

    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    # Save to temporary file for Spark to read
    temp_path = f"/tmp/{object_name}"
    with open(temp_path, 'wb') as f:
        f.write(data)

    # Read CSV with Spark
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file://{temp_path}")
    
    print(f"Downloaded {object_name} from {BUCKET_BRONZE}: {df.count()} rows")
    return df


@task(name="clean_null_values_spark", retries=1)
def clean_null_values_spark(df: DataFrame, strategy: str = "drop") -> DataFrame:
    """
    Clean null values and aberrant data using Spark.

    Args:
        df: Input Spark DataFrame
        strategy: Strategy for handling nulls ('drop', 'fill_mean', 'fill_median', 'fill_mode')

    Returns:
        Cleaned Spark DataFrame
    """
    df_cleaned = df
    initial_rows = df_cleaned.count()

    # Identify columns with null values
    null_counts = {col_name: df_cleaned.filter(col(col_name).isNull()).count() 
                   for col_name in df_cleaned.columns}
    null_columns = {k: v for k, v in null_counts.items() if v > 0}

    if null_columns:
        print(f"Found null values in columns: {null_columns}")

        # Drop rows with null values in critical columns
        critical_columns = ['id_achat', 'id_client', 'date_achat', 'montant']
        for col_name in critical_columns:
            if col_name in df_cleaned.columns:
                df_cleaned = df_cleaned.filter(col(col_name).isNotNull())

    # Remove aberrant values
    if 'montant' in df_cleaned.columns:
        # Remove negative amounts
        df_cleaned = df_cleaned.filter(col('montant') > 0)
        
        # Remove outliers (> 3 standard deviations)
        stats = df_cleaned.select(
            mean('montant').alias('mean'),
            stddev('montant').alias('std')
        ).collect()[0]
        
        if stats['std'] is not None:
            threshold = stats['mean'] + 3 * stats['std']
            df_cleaned = df_cleaned.filter(col('montant') <= threshold)

    if 'date_achat' in df_cleaned.columns or 'date_inscription' in df_cleaned.columns:
        # Remove dates in the future
        date_col = 'date_achat' if 'date_achat' in df_cleaned.columns else 'date_inscription'
        # Convert to date and filter
        df_cleaned = df_cleaned.withColumn(
            date_col + '_dt',
            to_date(col(date_col), 'yyyy-MM-dd')
        ).filter(
            col(date_col + '_dt') <= to_date(lit('2025-12-31'), 'yyyy-MM-dd')
        ).drop(date_col + '_dt')

    final_rows = df_cleaned.count()
    removed_rows = initial_rows - final_rows
    if removed_rows > 0:
        print(f"Removed {removed_rows} rows with null/aberrant values ({initial_rows} -> {final_rows})")

    return df_cleaned


@task(name="standardize_dates_spark", retries=1)
def standardize_dates_spark(df: DataFrame) -> DataFrame:
    """
    Standardize date formats to ISO format (YYYY-MM-DD) using Spark.

    Args:
        df: Input Spark DataFrame

    Returns:
        Spark DataFrame with standardized dates
    """
    df_std = df

    date_columns = ['date_achat', 'date_inscription']
    
    for col_name in date_columns:
        if col_name in df_std.columns:
            # Convert to date format
            df_std = df_std.withColumn(
                col_name,
                to_date(col(col_name), 'yyyy-MM-dd')
            )
            # Remove rows where date conversion failed
            df_std = df_std.filter(col(col_name).isNotNull())

    print(f"Standardized date columns: {[col for col in date_columns if col in df_std.columns]}")
    return df_std


@task(name="normalize_data_types_spark", retries=1)
def normalize_data_types_spark(df: DataFrame, table_name: str) -> DataFrame:
    """
    Normalize data types according to schema using Spark.

    Args:
        df: Input Spark DataFrame
        table_name: Name of the table ('achats' or 'clients')

    Returns:
        Spark DataFrame with normalized types
    """
    df_norm = df

    if table_name == 'achats':
        # Normalize achats schema
        if 'id_achat' in df_norm.columns:
            df_norm = df_norm.withColumn('id_achat', col('id_achat').cast(IntegerType()))
        if 'id_client' in df_norm.columns:
            df_norm = df_norm.withColumn('id_client', col('id_client').cast(IntegerType()))
        if 'montant' in df_norm.columns:
            df_norm = df_norm.withColumn('montant', col('montant').cast(DoubleType()))
        if 'produit' in df_norm.columns:
            df_norm = df_norm.withColumn('produit', col('produit').cast(StringType()))

    elif table_name == 'clients':
        # Normalize clients schema
        if 'id_client' in df_norm.columns:
            df_norm = df_norm.withColumn('id_client', col('id_client').cast(IntegerType()))
        if 'nom' in df_norm.columns:
            df_norm = df_norm.withColumn('nom', col('nom').cast(StringType()))
        if 'email' in df_norm.columns:
            df_norm = df_norm.withColumn('email', col('email').cast(StringType()))
        if 'pays' in df_norm.columns:
            df_norm = df_norm.withColumn('pays', col('pays').cast(StringType()))

    print(f"Normalized data types for {table_name}")
    return df_norm


@task(name="deduplicate_records_spark", retries=1)
def deduplicate_records_spark(df: DataFrame, table_name: str) -> DataFrame:
    """
    Remove duplicate records based on primary key using Spark.

    Args:
        df: Input Spark DataFrame
        table_name: Name of the table ('achats' or 'clients')

    Returns:
        Spark DataFrame without duplicates
    """
    df_dedup = df
    initial_rows = df_dedup.count()

    if table_name == 'achats':
        # Remove duplicates based on id_achat (primary key)
        if 'id_achat' in df_dedup.columns:
            df_dedup = df_dedup.dropDuplicates(['id_achat'])
    elif table_name == 'clients':
        # Remove duplicates based on id_client (primary key)
        if 'id_client' in df_dedup.columns:
            df_dedup = df_dedup.dropDuplicates(['id_client'])

    final_rows = df_dedup.count()
    removed_duplicates = initial_rows - final_rows
    if removed_duplicates > 0:
        print(f"Removed {removed_duplicates} duplicate records ({initial_rows} -> {final_rows})")

    return df_dedup


@task(name="quality_checks_spark", retries=1)
def quality_checks_spark(df: DataFrame, table_name: str) -> Dict:
    """
    Perform data quality checks using Spark.

    Args:
        df: Input Spark DataFrame
        table_name: Name of the table

    Returns:
        Dictionary with quality metrics
    """
    total_rows = df.count()
    
    # Count nulls per column
    null_counts = {}
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_counts[col_name] = null_count

    # Count duplicates
    duplicate_count = df.count() - df.dropDuplicates().count()

    quality_report = {
        'table_name': table_name,
        'total_rows': total_rows,
        'null_counts': null_counts,
        'duplicate_count': duplicate_count,
    }

    if table_name == 'achats':
        if 'montant' in df.columns:
            montant_stats = df.agg(
                spark_min('montant').alias('min'),
                spark_max('montant').alias('max'),
                mean('montant').alias('mean')
            ).collect()[0]
            quality_report['montant_stats'] = {
                'min': float(montant_stats['min']) if montant_stats['min'] else None,
                'max': float(montant_stats['max']) if montant_stats['max'] else None,
                'mean': float(montant_stats['mean']) if montant_stats['mean'] else None,
            }
        if 'id_client' in df.columns:
            quality_report['unique_clients'] = int(df.select('id_client').distinct().count())
        if 'produit' in df.columns:
            quality_report['unique_produits'] = int(df.select('produit').distinct().count())

    elif table_name == 'clients':
        if 'pays' in df.columns:
            quality_report['unique_countries'] = int(df.select('pays').distinct().count())
        if 'email' in df.columns:
            quality_report['email_validity'] = int(df.filter(col('email').contains('@')).count())

    print(f"Quality report for {table_name}: {quality_report}")
    return quality_report


@task(name="upload_to_silver_spark", retries=2)
def upload_to_silver_spark(df: DataFrame, object_name: str) -> str:
    """
    Upload cleaned Spark DataFrame to silver bucket as Parquet.

    Args:
        df: Cleaned Spark DataFrame
        object_name: Name of object in silver bucket

    Returns:
        Object name in silver layer
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    # Save to temporary Parquet file
    parquet_name = object_name.replace('.csv', '.parquet')
    temp_path = f"/tmp/{parquet_name}"
    
    # Write as single Parquet file (coalesce to 1 partition)
    df.coalesce(1).write.mode('overwrite').parquet(f"file://{temp_path}")

    # Find and read the Parquet file
    import glob
    import os
    parquet_files = glob.glob(f"{temp_path}/**/*.parquet", recursive=True)
    if parquet_files:
        with open(parquet_files[0], 'rb') as pf:
            parquet_data = pf.read()
            client.put_object(
                BUCKET_SILVER,
                parquet_name,
                BytesIO(parquet_data),
                length=len(parquet_data),
                content_type='application/parquet'
            )

    row_count = df.count()
    print(f"Uploaded {parquet_name} to {BUCKET_SILVER}: {row_count} rows")
    return parquet_name


@flow(name="Silver Transformation Flow (Spark)")
def silver_transformation_flow_spark(bronze_files: Dict[str, str]) -> Dict:
    """
    Main flow: Transform data from bronze to silver layer using Spark.

    Args:
        bronze_files: Dictionary with bronze file names (e.g., {'clients': 'clients.csv', 'achats': 'achats.csv'})

    Returns:
        Dictionary with silver file names and quality reports
    """
    # Create Spark session
    spark = get_spark_session("SilverTransformation")
    
    results = {}

    try:
        for table_name, bronze_file in bronze_files.items():
            print(f"\n=== Processing {table_name} (Spark) ===")

            # Download from bronze
            df = download_from_bronze_spark(spark, bronze_file)

            # Clean null values and aberrant data
            df = clean_null_values_spark(df, strategy="drop")

            # Standardize dates
            df = standardize_dates_spark(df)

            # Normalize data types
            df = normalize_data_types_spark(df, table_name)

            # Deduplicate records
            df = deduplicate_records_spark(df, table_name)

            # Quality checks
            quality_report = quality_checks_spark(df, table_name)

            # Upload to silver
            silver_file = upload_to_silver_spark(df, bronze_file)

            results[table_name] = {
                'silver_file': silver_file,
                'quality_report': quality_report
            }
    finally:
        spark.stop()

    return results


if __name__ == "__main__":
    # Example usage
    bronze_files = {
        "clients": "clients.csv",
        "achats": "achats.csv"
    }
    result = silver_transformation_flow_spark(bronze_files)
    print(f"\nSilver transformation complete (Spark): {result}")
