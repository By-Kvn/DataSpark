"""
Benchmark script to compare performance between Pandas and PySpark.
This script measures execution time for the Silver transformation layer.
"""
import time
from pathlib import Path
from typing import Dict
import sys

# Add flows directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "flows"))

from silver_transformation import silver_transformation_flow as pandas_flow
from silver_transformation_spark import silver_transformation_flow_spark as spark_flow


def benchmark_pandas(bronze_files: Dict[str, str]) -> Dict:
    """
    Run Silver transformation using Pandas and measure execution time.
    
    Args:
        bronze_files: Dictionary with bronze file names
        
    Returns:
        Dictionary with results and timing information
    """
    print("\n" + "="*60)
    print("BENCHMARK: Pandas (Local Processing)")
    print("="*60)
    
    start_time = time.time()
    
    try:
        results = pandas_flow(bronze_files)
        execution_time = time.time() - start_time
        
        return {
            'engine': 'Pandas',
            'execution_time': execution_time,
            'results': results,
            'success': True
        }
    except Exception as e:
        execution_time = time.time() - start_time
        return {
            'engine': 'Pandas',
            'execution_time': execution_time,
            'error': str(e),
            'success': False
        }


def benchmark_spark(bronze_files: Dict[str, str]) -> Dict:
    """
    Run Silver transformation using PySpark and measure execution time.
    
    Args:
        bronze_files: Dictionary with bronze file names
        
    Returns:
        Dictionary with results and timing information
    """
    print("\n" + "="*60)
    print("BENCHMARK: PySpark (Distributed Processing)")
    print("="*60)
    
    start_time = time.time()
    
    try:
        results = spark_flow(bronze_files)
        execution_time = time.time() - start_time
        
        return {
            'engine': 'PySpark',
            'execution_time': execution_time,
            'results': results,
            'success': True
        }
    except Exception as e:
        execution_time = time.time() - start_time
        return {
            'engine': 'PySpark',
            'execution_time': execution_time,
            'error': str(e),
            'success': False
        }


def print_comparison(pandas_result: Dict, spark_result: Dict):
    """
    Print comparison results between Pandas and PySpark.
    
    Args:
        pandas_result: Results from Pandas benchmark
        spark_result: Results from PySpark benchmark
    """
    print("\n" + "="*60)
    print("BENCHMARK RESULTS COMPARISON")
    print("="*60)
    
    print(f"\n{'Metric':<30} {'Pandas':<20} {'PySpark':<20}")
    print("-" * 70)
    
    if pandas_result['success']:
        print(f"{'Execution Time (seconds)':<30} {pandas_result['execution_time']:<20.2f} {spark_result['execution_time']:<20.2f}")
        
        # Calculate speedup
        if spark_result['success']:
            speedup = pandas_result['execution_time'] / spark_result['execution_time']
            print(f"{'Speedup (Pandas/Spark)':<30} {'-':<20} {speedup:<20.2f}x")
            
            if speedup > 1:
                print(f"\n✅ PySpark is {speedup:.2f}x FASTER than Pandas")
            elif speedup < 1:
                print(f"\n✅ Pandas is {1/speedup:.2f}x FASTER than PySpark")
            else:
                print(f"\n⚖️  Both engines have similar performance")
        
        # Compare data quality results
        if pandas_result['success'] and spark_result['success']:
            print("\n" + "-" * 70)
            print("DATA QUALITY COMPARISON")
            print("-" * 70)
            
            for table_name in ['clients', 'achats']:
                if table_name in pandas_result['results'] and table_name in spark_result['results']:
                    pandas_rows = pandas_result['results'][table_name]['quality_report']['total_rows']
                    spark_rows = spark_result['results'][table_name]['quality_report']['total_rows']
                    
                    print(f"\n{table_name.upper()}:")
                    print(f"  Pandas rows: {pandas_rows}")
                    print(f"  PySpark rows: {spark_rows}")
                    print(f"  Difference: {abs(pandas_rows - spark_rows)} rows")
    else:
        print(f"❌ Pandas execution failed: {pandas_result.get('error', 'Unknown error')}")
    
    if not spark_result['success']:
        print(f"❌ PySpark execution failed: {spark_result.get('error', 'Unknown error')}")
    
    print("\n" + "="*60)


def main():
    """
    Main function to run the benchmark.
    """
    # Determine data directory
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data" / "sources"
    
    # Bronze files to process
    bronze_files = {
        "clients": "clients.csv",
        "achats": "achats.csv"
    }
    
    print("\n" + "="*60)
    print("BENCHMARK: Pandas vs PySpark - Silver Transformation")
    print("="*60)
    print(f"\nData directory: {data_dir}")
    print(f"Files to process: {list(bronze_files.values())}")
    
    # Run Pandas benchmark
    pandas_result = benchmark_pandas(bronze_files)
    
    # Run PySpark benchmark
    spark_result = benchmark_spark(bronze_files)
    
    # Print comparison
    print_comparison(pandas_result, spark_result)
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"✅ Pandas: {'SUCCESS' if pandas_result['success'] else 'FAILED'} ({pandas_result['execution_time']:.2f}s)")
    print(f"✅ PySpark: {'SUCCESS' if spark_result['success'] else 'FAILED'} ({spark_result['execution_time']:.2f}s)")
    
    if pandas_result['success'] and spark_result['success']:
        if pandas_result['execution_time'] < spark_result['execution_time']:
            print(f"\n🏆 Winner: Pandas (faster for this dataset size)")
            print("💡 Note: PySpark shows its advantage with larger datasets")
        else:
            print(f"\n🏆 Winner: PySpark (distributed processing advantage)")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
