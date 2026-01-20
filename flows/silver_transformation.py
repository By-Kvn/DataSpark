from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime

from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


@task(name="download_from_bronze", retries=2)
def download_from_bronze(object_name: str) -> pd.DataFrame:
    """
    Download CSV file from bronze bucket and load as DataFrame.

    Args:
        object_name: Name of object in bronze bucket

    Returns:
        DataFrame with raw data
    """
    client = get_minio_client()

    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    df = pd.read_csv(BytesIO(data))
    print(f"Downloaded {object_name} from {BUCKET_BRONZE}: {len(df)} rows")
    return df


@task(name="clean_null_values", retries=1)
def clean_null_values(df: pd.DataFrame, strategy: str = "drop") -> pd.DataFrame:
    """
    Clean null values and aberrant data.

    Args:
        df: Input DataFrame
        strategy: Strategy for handling nulls ('drop', 'fill_mean', 'fill_median', 'fill_mode')

    Returns:
        Cleaned DataFrame
    """
    df_cleaned = df.copy()
    initial_rows = len(df_cleaned)

    # Identify columns with null values
    null_counts = df_cleaned.isnull().sum()
    null_columns = null_counts[null_counts > 0].to_dict()

    if null_columns:
        print(f"Found null values in columns: {null_columns}")

        for col, count in null_columns.items():
            if strategy == "drop":
                # Drop rows with null values in critical columns
                if col in ['id_achat', 'id_client', 'date_achat', 'montant']:
                    df_cleaned = df_cleaned.dropna(subset=[col])
            elif strategy == "fill_mean" and df_cleaned[col].dtype in ['float64', 'int64']:
                df_cleaned[col] = df_cleaned[col].fillna(df_cleaned[col].mean())
            elif strategy == "fill_median" and df_cleaned[col].dtype in ['float64', 'int64']:
                df_cleaned[col] = df_cleaned[col].fillna(df_cleaned[col].median())
            elif strategy == "fill_mode":
                df_cleaned[col] = df_cleaned[col].fillna(df_cleaned[col].mode()[0] if not df_cleaned[col].mode().empty else None)

    # Remove aberrant values
    if 'montant' in df_cleaned.columns:
        # Remove negative amounts or extremely high values (outliers > 3 standard deviations)
        mean = df_cleaned['montant'].mean()
        std = df_cleaned['montant'].std()
        df_cleaned = df_cleaned[
            (df_cleaned['montant'] > 0) & 
            (df_cleaned['montant'] <= mean + 3 * std)
        ]

    if 'date_achat' in df_cleaned.columns or 'date_inscription' in df_cleaned.columns:
        # Remove dates in the future (assuming current date is 2025-12-31)
        date_col = 'date_achat' if 'date_achat' in df_cleaned.columns else 'date_inscription'
        current_date = pd.Timestamp('2025-12-31')
        df_cleaned[date_col] = pd.to_datetime(df_cleaned[date_col], errors='coerce')
        df_cleaned = df_cleaned[df_cleaned[date_col] <= current_date]

    final_rows = len(df_cleaned)
    removed_rows = initial_rows - final_rows
    if removed_rows > 0:
        print(f"Removed {removed_rows} rows with null/aberrant values ({initial_rows} -> {final_rows})")

    return df_cleaned


@task(name="standardize_dates", retries=1)
def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize date formats to ISO format (YYYY-MM-DD).

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with standardized dates
    """
    df_std = df.copy()

    date_columns = ['date_achat', 'date_inscription']
    
    for col in date_columns:
        if col in df_std.columns:
            # Convert to datetime, handling multiple formats
            df_std[col] = pd.to_datetime(df_std[col], errors='coerce')
            # Format as ISO date string (YYYY-MM-DD)
            df_std[col] = df_std[col].dt.strftime('%Y-%m-%d')
            # Remove rows where date conversion failed
            df_std = df_std[df_std[col].notna()]

    print(f"Standardized date columns: {[col for col in date_columns if col in df_std.columns]}")
    return df_std


@task(name="normalize_data_types", retries=1)
def normalize_data_types(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Normalize data types according to schema.

    Args:
        df: Input DataFrame
        table_name: Name of the table ('achats' or 'clients')

    Returns:
        DataFrame with normalized types
    """
    df_norm = df.copy()

    if table_name == 'achats':
        # Normalize achats schema
        if 'id_achat' in df_norm.columns:
            df_norm['id_achat'] = df_norm['id_achat'].astype('int64')
        if 'id_client' in df_norm.columns:
            df_norm['id_client'] = df_norm['id_client'].astype('int64')
        if 'montant' in df_norm.columns:
            df_norm['montant'] = df_norm['montant'].astype('float64')
        if 'produit' in df_norm.columns:
            df_norm['produit'] = df_norm['produit'].astype('string')

    elif table_name == 'clients':
        # Normalize clients schema
        if 'id_client' in df_norm.columns:
            df_norm['id_client'] = df_norm['id_client'].astype('int64')
        if 'nom' in df_norm.columns:
            df_norm['nom'] = df_norm['nom'].astype('string')
        if 'email' in df_norm.columns:
            df_norm['email'] = df_norm['email'].astype('string')
        if 'pays' in df_norm.columns:
            df_norm['pays'] = df_norm['pays'].astype('string')

    print(f"Normalized data types for {table_name}")
    return df_norm


@task(name="deduplicate_records", retries=1)
def deduplicate_records(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Remove duplicate records based on primary key.

    Args:
        df: Input DataFrame
        table_name: Name of the table ('achats' or 'clients')

    Returns:
        DataFrame without duplicates
    """
    df_dedup = df.copy()
    initial_rows = len(df_dedup)

    if table_name == 'achats':
        # Remove duplicates based on id_achat (primary key)
        if 'id_achat' in df_dedup.columns:
            df_dedup = df_dedup.drop_duplicates(subset=['id_achat'], keep='first')
    elif table_name == 'clients':
        # Remove duplicates based on id_client (primary key)
        if 'id_client' in df_dedup.columns:
            df_dedup = df_dedup.drop_duplicates(subset=['id_client'], keep='first')

    final_rows = len(df_dedup)
    removed_duplicates = initial_rows - final_rows
    if removed_duplicates > 0:
        print(f"Removed {removed_duplicates} duplicate records ({initial_rows} -> {final_rows})")

    return df_dedup


@task(name="quality_checks", retries=1)
def quality_checks(df: pd.DataFrame, table_name: str) -> Dict:
    """
    Perform data quality checks.

    Args:
        df: Input DataFrame
        table_name: Name of the table

    Returns:
        Dictionary with quality metrics
    """
    quality_report = {
        'table_name': table_name,
        'total_rows': len(df),
        'null_counts': df.isnull().sum().to_dict(),
        'duplicate_count': df.duplicated().sum(),
    }

    if table_name == 'achats':
        quality_report['montant_stats'] = {
            'min': float(df['montant'].min()) if 'montant' in df.columns else None,
            'max': float(df['montant'].max()) if 'montant' in df.columns else None,
            'mean': float(df['montant'].mean()) if 'montant' in df.columns else None,
        }
        quality_report['unique_clients'] = int(df['id_client'].nunique()) if 'id_client' in df.columns else 0
        quality_report['unique_produits'] = int(df['produit'].nunique()) if 'produit' in df.columns else 0

    elif table_name == 'clients':
        quality_report['unique_countries'] = int(df['pays'].nunique()) if 'pays' in df.columns else 0
        quality_report['email_validity'] = int(df['email'].str.contains('@', na=False).sum()) if 'email' in df.columns else 0

    print(f"Quality report for {table_name}: {quality_report}")
    return quality_report


@task(name="upload_to_silver", retries=2)
def upload_to_silver(df: pd.DataFrame, object_name: str) -> str:
    """
    Upload cleaned DataFrame to silver bucket as Parquet.

    Args:
        df: Cleaned DataFrame
        object_name: Name of object in silver bucket

    Returns:
        Object name in silver layer
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    # Convert DataFrame to Parquet format
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)

    # Upload to MinIO
    parquet_name = object_name.replace('.csv', '.parquet')
    client.put_object(
        BUCKET_SILVER,
        parquet_name,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type='application/parquet'
    )

    print(f"Uploaded {parquet_name} to {BUCKET_SILVER}: {len(df)} rows")
    return parquet_name


@flow(name="Silver Transformation Flow")
def silver_transformation_flow(bronze_files: Dict[str, str]) -> Dict:
    """
    Main flow: Transform data from bronze to silver layer.

    Args:
        bronze_files: Dictionary with bronze file names (e.g., {'clients': 'clients.csv', 'achats': 'achats.csv'})

    Returns:
        Dictionary with silver file names and quality reports
    """
    results = {}

    for table_name, bronze_file in bronze_files.items():
        print(f"\n=== Processing {table_name} ===")

        # Download from bronze
        df = download_from_bronze(bronze_file)

        # Clean null values and aberrant data
        df = clean_null_values(df, strategy="drop")

        # Standardize dates
        df = standardize_dates(df)

        # Normalize data types
        df = normalize_data_types(df, table_name)

        # Deduplicate records
        df = deduplicate_records(df, table_name)

        # Quality checks
        quality_report = quality_checks(df, table_name)

        # Upload to silver
        silver_file = upload_to_silver(df, bronze_file)

        results[table_name] = {
            'silver_file': silver_file,
            'quality_report': quality_report
        }

    return results


if __name__ == "__main__":
    # Example usage
    bronze_files = {
        "clients": "clients.csv",
        "achats": "achats.csv"
    }
    result = silver_transformation_flow(bronze_files)
    print(f"\nSilver transformation complete: {result}")
