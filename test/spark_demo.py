import pandas as pd
import sys
import os
import json
import sys
import contextlib

from pyspark.sql import SparkSession

from dataquality import validate_data

def create_spark_session():
    """Create a simple local Spark session."""
    spark = SparkSession.builder \
        .appName("DataQualitySparkDemo") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark

def main():
    """
    A clear and concise demonstration of the data quality library's features,
    showing identical usage for a Spark DataFrame.
    """
    print("--- Spark Demo: Data Quality Validation ---")

    # 1. Setup
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    test_data_path = os.path.join(os.path.dirname(__file__), 'HDBResale.csv')
    contract_path = "/Users/dinesh/dqmk/src/dataquality/contracts/hdb_resale_spark.yaml"
    
    # Load data
    df = spark.createDataFrame(pd.read_csv(test_data_path))
    print(f"Loaded {df.count():,} rows from HDB Resale dataset\n")

    # 2. Run validation and get full report
    with validate_data(df, contract_path=contract_path) as result:
        
        # Quick summary
        result.print_summary()
        
        # Check if pipeline should proceed
        if result.passed:
            print("\n All checks passed - pipeline can continue")
        else:
            print("\n Data quality issues found")
            
            # Show sample failures
            result.show_failed_rows(max_rows=3)
            
            # Get metrics for monitoring
            metrics = result.compute_metrics()
            print("\n Data Quality Metrics (Top 5 Failures):")
            metrics.filter(metrics.status == 'FAILED') \
                   .select('dimension', 'dq_rule', 'failed_row_count', 'pass_rate') \
                   .show(5, truncate=False)
            
            # Save results for further analysis
            result.save(output_dir="output", prefix="spark_validation")

    spark.stop()

if __name__ == "__main__":
    main()