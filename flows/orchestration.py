"""
Orchestration flow to run the complete data pipeline:
Bronze -> Silver -> Gold
"""
from pathlib import Path
from prefect import flow

from bronze_ingestion import bronze_ingestion_flow
from silver_transformation import silver_transformation_flow
from gold_aggregation import gold_aggregation_flow


@flow(name="Complete Data Pipeline")
def complete_pipeline_flow(data_dir: str = None) -> dict:
    """
    Complete ETL pipeline: Bronze -> Silver -> Gold

    Args:
        data_dir: Directory containing source CSV files

    Returns:
        Dictionary with results from all layers
    """
    # Determine data directory (relative to project root, not flows/)
    if data_dir is None:
        # Get project root (parent of flows/)
        project_root = Path(__file__).parent.parent
        data_dir = str(project_root / "data" / "sources")
    
    # Step 1: Bronze Ingestion
    print("=" * 50)
    print("STEP 1: BRONZE INGESTION")
    print("=" * 50)
    bronze_results = bronze_ingestion_flow(data_dir)
    print(f"Bronze ingestion complete: {bronze_results}")

    # Step 2: Silver Transformation
    print("\n" + "=" * 50)
    print("STEP 2: SILVER TRANSFORMATION")
    print("=" * 50)
    silver_results = silver_transformation_flow(bronze_results)
    print(f"Silver transformation complete: {silver_results}")

    # Extract silver file names for gold layer
    silver_files = {
        "clients": silver_results["clients"]["silver_file"],
        "achats": silver_results["achats"]["silver_file"]
    }

    # Step 3: Gold Aggregation
    print("\n" + "=" * 50)
    print("STEP 3: GOLD AGGREGATION")
    print("=" * 50)
    gold_results = gold_aggregation_flow(silver_files)
    print(f"Gold aggregation complete: {gold_results}")

    return {
        "bronze": bronze_results,
        "silver": silver_results,
        "gold": gold_results
    }


if __name__ == "__main__":
    # Use absolute path to data directory
    project_root = Path(__file__).parent.parent
    data_dir = str(project_root / "data" / "sources")
    
    result = complete_pipeline_flow(data_dir)
    print("\n" + "=" * 50)
    print("PIPELINE COMPLETE")
    print("=" * 50)
    print(f"Final results: {result}")
