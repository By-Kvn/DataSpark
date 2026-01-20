from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime

from prefect import flow, task

from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


@task(name="download_from_silver", retries=2)
def download_from_silver(object_name: str) -> pd.DataFrame:
    """
    Download Parquet file from silver bucket and load as DataFrame.

    Args:
        object_name: Name of object in silver bucket

    Returns:
        DataFrame with cleaned data
    """
    client = get_minio_client()

    response = client.get_object(BUCKET_SILVER, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    df = pd.read_parquet(BytesIO(data))
    print(f"Downloaded {object_name} from {BUCKET_SILVER}: {len(df)} rows")
    return df


@task(name="create_dimension_tables", retries=1)
def create_dimension_tables(df_clients: pd.DataFrame, df_achats: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Create dimension tables (star schema).

    Args:
        df_clients: Clients DataFrame
        df_achats: Achats DataFrame

    Returns:
        Dictionary with dimension tables
    """
    dimensions = {}

    # Dimension: DimClient
    dim_client = df_clients.copy()
    dim_client = dim_client.rename(columns={
        'id_client': 'client_id',
        'nom': 'client_nom',
        'email': 'client_email',
        'date_inscription': 'date_inscription',
        'pays': 'pays'
    })
    # Add surrogate key
    dim_client.insert(0, 'dim_client_id', range(1, len(dim_client) + 1))
    dimensions['dim_client'] = dim_client

    # Dimension: DimProduit
    if 'produit' in df_achats.columns:
        dim_produit = pd.DataFrame({
            'dim_produit_id': range(1, df_achats['produit'].nunique() + 1),
            'produit_nom': df_achats['produit'].unique()
        })
        dimensions['dim_produit'] = dim_produit

    # Dimension: DimTemps
    # Create time dimension from dates in achats
    if 'date_achat' in df_achats.columns:
        dates = pd.to_datetime(df_achats['date_achat'].unique())
        dim_temps = pd.DataFrame({
            'date': dates,
            'annee': dates.year,
            'mois': dates.month,
            'jour': dates.day,
            'semaine': dates.isocalendar().week,
            'trimestre': dates.quarter,
            'jour_semaine': dates.dayofweek,
            'nom_jour': dates.strftime('%A'),
            'nom_mois': dates.strftime('%B')
        })
        dim_temps.insert(0, 'dim_temps_id', range(1, len(dim_temps) + 1))
        dimensions['dim_temps'] = dim_temps

    # Dimension: DimPays
    if 'pays' in df_clients.columns:
        dim_pays = pd.DataFrame({
            'dim_pays_id': range(1, df_clients['pays'].nunique() + 1),
            'pays_nom': df_clients['pays'].unique()
        })
        dimensions['dim_pays'] = dim_pays

    print(f"Created {len(dimensions)} dimension tables")
    return dimensions


@task(name="create_fact_table", retries=1)
def create_fact_table(
    df_achats: pd.DataFrame,
    dim_client: pd.DataFrame,
    dim_produit: pd.DataFrame,
    dim_temps: pd.DataFrame
) -> pd.DataFrame:
    """
    Create fact table (FactAchats) with foreign keys to dimensions.

    Args:
        df_achats: Achats DataFrame
        dim_client: Client dimension
        dim_produit: Produit dimension
        dim_temps: Temps dimension

    Returns:
        Fact table DataFrame
    """
    fact_achats = df_achats.copy()

    # Join with dimensions to get surrogate keys
    fact_achats = fact_achats.merge(
        dim_client[['client_id', 'dim_client_id']],
        left_on='id_client',
        right_on='client_id',
        how='left'
    )

    fact_achats = fact_achats.merge(
        dim_produit,
        left_on='produit',
        right_on='produit_nom',
        how='left'
    )

    fact_achats['date_achat_dt'] = pd.to_datetime(fact_achats['date_achat'])
    fact_achats = fact_achats.merge(
        dim_temps[['date', 'dim_temps_id']],
        left_on='date_achat_dt',
        right_on='date',
        how='left'
    )

    # Select fact table columns
    fact_table = pd.DataFrame({
        'fact_achat_id': fact_achats['id_achat'],
        'dim_client_id': fact_achats['dim_client_id'],
        'dim_produit_id': fact_achats['dim_produit_id'],
        'dim_temps_id': fact_achats['dim_temps_id'],
        'montant': fact_achats['montant'],
        'date_achat': fact_achats['date_achat']
    })

    print(f"Created fact table with {len(fact_table)} rows")
    return fact_table


@task(name="calculate_kpis", retries=1)
def calculate_kpis(
    fact_achats: pd.DataFrame,
    dim_client: pd.DataFrame,
    dim_temps: pd.DataFrame,
    dim_pays: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """
    Calculate key performance indicators (KPIs).

    Args:
        fact_achats: Fact table
        dim_client: Client dimension
        dim_temps: Temps dimension
        dim_pays: Pays dimension

    Returns:
        Dictionary with KPI DataFrames
    """
    kpis = {}

    # Join fact with dimensions for analysis
    fact_with_dims = fact_achats.merge(
        dim_client[['dim_client_id', 'pays']],
        on='dim_client_id',
        how='left'
    )
    fact_with_dims['date_achat_dt'] = pd.to_datetime(fact_with_dims['date_achat'])

    # KPI 1: Volumes par période (jour, semaine, mois)
    fact_with_dims['annee'] = fact_with_dims['date_achat_dt'].dt.year
    fact_with_dims['mois'] = fact_with_dims['date_achat_dt'].dt.month
    fact_with_dims['semaine'] = fact_with_dims['date_achat_dt'].dt.isocalendar().week
    fact_with_dims['jour'] = fact_with_dims['date_achat_dt'].dt.date

    # Volumes par jour
    kpi_volumes_jour = fact_with_dims.groupby('jour').agg({
        'fact_achat_id': 'count',
        'montant': 'sum'
    }).reset_index()
    kpi_volumes_jour.columns = ['date', 'nb_achats', 'ca_total']
    kpis['volumes_jour'] = kpi_volumes_jour

    # Volumes par semaine
    kpi_volumes_semaine = fact_with_dims.groupby(['annee', 'semaine']).agg({
        'fact_achat_id': 'count',
        'montant': 'sum'
    }).reset_index()
    kpi_volumes_semaine.columns = ['annee', 'semaine', 'nb_achats', 'ca_total']
    kpis['volumes_semaine'] = kpi_volumes_semaine

    # Volumes par mois
    kpi_volumes_mois = fact_with_dims.groupby(['annee', 'mois']).agg({
        'fact_achat_id': 'count',
        'montant': 'sum'
    }).reset_index()
    kpi_volumes_mois.columns = ['annee', 'mois', 'nb_achats', 'ca_total']
    kpis['volumes_mois'] = kpi_volumes_mois

    # KPI 2: CA par pays
    kpi_ca_pays = fact_with_dims.groupby('pays').agg({
        'montant': ['sum', 'mean', 'count']
    }).reset_index()
    kpi_ca_pays.columns = ['pays', 'ca_total', 'ca_moyen', 'nb_achats']
    kpi_ca_pays = kpi_ca_pays.sort_values('ca_total', ascending=False)
    kpis['ca_par_pays'] = kpi_ca_pays

    # KPI 3: Taux de croissance (mois par mois)
    ca_mois = fact_with_dims.groupby(['annee', 'mois'])['montant'].sum().reset_index()
    # Create datetime from year, month, day
    ca_mois['periode'] = pd.to_datetime({
        'year': ca_mois['annee'],
        'month': ca_mois['mois'],
        'day': 1
    })
    ca_mois = ca_mois.sort_values('periode')
    ca_mois['ca_prev'] = ca_mois['montant'].shift(1)
    ca_mois['taux_croissance'] = ((ca_mois['montant'] - ca_mois['ca_prev']) / ca_mois['ca_prev'] * 100).round(2)
    kpis['taux_croissance'] = ca_mois[['annee', 'mois', 'montant', 'taux_croissance']].rename(columns={'montant': 'ca'})

    # KPI 4: Distributions statistiques
    distributions = pd.DataFrame({
        'metrique': ['montant'],
        'moyenne': [fact_with_dims['montant'].mean()],
        'mediane': [fact_with_dims['montant'].median()],
        'ecart_type': [fact_with_dims['montant'].std()],
        'min': [fact_with_dims['montant'].min()],
        'max': [fact_with_dims['montant'].max()],
        'q25': [fact_with_dims['montant'].quantile(0.25)],
        'q75': [fact_with_dims['montant'].quantile(0.75)]
    })
    kpis['distributions_statistiques'] = distributions

    print(f"Calculated {len(kpis)} KPIs")
    return kpis


@task(name="create_temporal_aggregations", retries=1)
def create_temporal_aggregations(
    fact_achats: pd.DataFrame,
    dim_temps: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """
    Pre-calculate temporal aggregations (jour, semaine, mois).

    Args:
        fact_achats: Fact table
        dim_temps: Temps dimension

    Returns:
        Dictionary with temporal aggregations
    """
    aggregations = {}

    # Join with time dimension
    fact_with_time = fact_achats.merge(
        dim_temps,
        on='dim_temps_id',
        how='left'
    )

    # Aggregation by day
    agg_jour = fact_with_time.groupby(['date', 'annee', 'mois', 'jour']).agg({
        'montant': ['sum', 'mean', 'count'],
        'fact_achat_id': 'count'
    }).reset_index()
    agg_jour.columns = ['date', 'annee', 'mois', 'jour', 'ca_total', 'ca_moyen', 'nb_transactions', 'nb_achats']
    aggregations['agregation_jour'] = agg_jour

    # Aggregation by week
    agg_semaine = fact_with_time.groupby(['annee', 'semaine']).agg({
        'montant': ['sum', 'mean', 'count'],
        'fact_achat_id': 'count'
    }).reset_index()
    agg_semaine.columns = ['annee', 'semaine', 'ca_total', 'ca_moyen', 'nb_transactions', 'nb_achats']
    aggregations['agregation_semaine'] = agg_semaine

    # Aggregation by month
    agg_mois = fact_with_time.groupby(['annee', 'mois', 'trimestre']).agg({
        'montant': ['sum', 'mean', 'count'],
        'fact_achat_id': 'count'
    }).reset_index()
    agg_mois.columns = ['annee', 'mois', 'trimestre', 'ca_total', 'ca_moyen', 'nb_transactions', 'nb_achats']
    aggregations['agregation_mois'] = agg_mois

    print(f"Created {len(aggregations)} temporal aggregations")
    return aggregations


@task(name="upload_to_gold", retries=2)
def upload_to_gold(df: pd.DataFrame, object_name: str) -> str:
    """
    Upload DataFrame to gold bucket as Parquet.

    Args:
        df: DataFrame to upload
        object_name: Name of object in gold bucket

    Returns:
        Object name in gold layer
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    # Convert DataFrame to Parquet format
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)

    # Upload to MinIO
    client.put_object(
        BUCKET_GOLD,
        object_name,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type='application/parquet'
    )

    print(f"Uploaded {object_name} to {BUCKET_GOLD}: {len(df)} rows")
    return object_name


@flow(name="Gold Aggregation Flow")
def gold_aggregation_flow(silver_files: Dict[str, str]) -> Dict:
    """
    Main flow: Create gold layer with business aggregations.

    Args:
        silver_files: Dictionary with silver file names (e.g., {'clients': 'clients.parquet', 'achats': 'achats.parquet'})

    Returns:
        Dictionary with gold file names
    """
    results = {}

    # Download from silver
    print("\n=== Downloading from Silver ===")
    df_clients = download_from_silver(silver_files['clients'])
    df_achats = download_from_silver(silver_files['achats'])

    # Create dimension tables
    print("\n=== Creating Dimension Tables ===")
    dimensions = create_dimension_tables(df_clients, df_achats)

    # Upload dimensions to gold
    for dim_name, dim_df in dimensions.items():
        object_name = f"{dim_name}.parquet"
        upload_to_gold(dim_df, object_name)
        results[dim_name] = object_name

    # Create fact table
    print("\n=== Creating Fact Table ===")
    fact_achats = create_fact_table(
        df_achats,
        dimensions['dim_client'],
        dimensions['dim_produit'],
        dimensions['dim_temps']
    )
    fact_name = upload_to_gold(fact_achats, "fact_achats.parquet")
    results['fact_achats'] = fact_name

    # Calculate KPIs
    print("\n=== Calculating KPIs ===")
    kpis = calculate_kpis(
        fact_achats,
        dimensions['dim_client'],
        dimensions['dim_temps'],
        dimensions['dim_pays']
    )

    # Upload KPIs to gold
    for kpi_name, kpi_df in kpis.items():
        object_name = f"kpi_{kpi_name}.parquet"
        upload_to_gold(kpi_df, object_name)
        results[f"kpi_{kpi_name}"] = object_name

    # Create temporal aggregations
    print("\n=== Creating Temporal Aggregations ===")
    temporal_aggs = create_temporal_aggregations(fact_achats, dimensions['dim_temps'])

    # Upload temporal aggregations to gold
    for agg_name, agg_df in temporal_aggs.items():
        object_name = f"{agg_name}.parquet"
        upload_to_gold(agg_df, object_name)
        results[agg_name] = object_name

    return results


if __name__ == "__main__":
    # Example usage
    silver_files = {
        "clients": "clients.parquet",
        "achats": "achats.parquet"
    }
    result = gold_aggregation_flow(silver_files)
    print(f"\nGold aggregation complete: {result}")
