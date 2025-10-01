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

    # 1. Create a Spark session and load data
    spark = create_spark_session()
    test_data_path = os.path.join(os.path.dirname(__file__), 'HDBResale.csv')
    contract_path = "/Users/dinesh/dqmk/src/dataquality/contracts/hdb_resale_spark.yaml"
    
    # Load data via pandas and convert to a Spark DataFrame for the demo
    pandas_df = pd.read_csv(test_data_path)
    spark_df = spark.createDataFrame(pandas_df)
    print(f"Loaded {spark_df.count()} rows into a Spark DataFrame.")

    # 2. Run validation to get the result object
    print("\nRunning validation on Spark DataFrame...")
    with validate_data(spark_df, contract_path=contract_path) as result:
        # --- Use Case 1: The Quick & Complete Report ---
        result.display_full_report()

        # --- Use Case 2: Programmatic Check in a Pipeline ---
        if result.passed:
            print("✅ All checks passed. Pipeline can continue.")
        else:
            print("❌ Checks failed. Retrieving failed data for remediation...")
            # Get the actual failed data for programmatic use
            failed_data = result.get_failed_rows()
            if failed_data is not None:
                print(f"   - Found {failed_data.count()} rows to send to a quarantine system.")
                # In a real pipeline, you would now save or process this `failed_data` Spark DataFrame.
            else:
                print("   - Checks failed, but no specific rows were identified. Please review contract queries.")

    # --- Use Case 3: Building a Custom View ---
    result.print_summary().show_failed_rows(max_rows=6)

    # Show Spark execution plan
    print(f"Spark UI available at: http://localhost:4040")

    # Clean up
    spark.stop()
    print("Spark session stopped")

if __name__ == "__main__":
    main()