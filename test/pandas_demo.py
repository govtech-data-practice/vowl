import pandas as pd
import os
from dataquality import validate_data

def main():
    """
    A clear and concise demonstration of the data quality library's features,
    structured by common use cases.
    """
    print("--- Pandas Demo: Data Quality Validation ---")

    # 1. Load data
    test_data_path = os.path.join(os.path.dirname(__file__), 'HDBResale.csv')
    df = pd.read_csv(test_data_path)
    print(f"Loaded {len(df)} rows from {os.path.basename(test_data_path)}")

    # 2. Run validation to get the full result object
    with validate_data(df, contract_path="/Users/dinesh/dqmk/src/dataquality/contracts/hdb_resale_pandas.yaml") as result:
        # Quick summary
        result.display_full_report()

        # Check if pipeline should proceed
        if result.passed:
            print("All checks passed. Pipeline can continue.")
        else:
            print("Checks failed. Retrieving failed data for remediation...")
            failed_data = result.get_failed_rows()
            print(f"   - Found {len(failed_data)} rows that failed tests")

        failed_data.to_csv("/Users/dinesh/dqmk/output/failed_data.csv", index=False)
        print(f"Results saved to output directory.")


        print("\n--- Computing Data Quality Metrics ---")
        
        # Get metrics for monitoring
        metrics_df = result.compute_metrics()
        print("\nRule-Level Metrics:")
        print(metrics_df.to_string(index=False))        

        # Users can aggregate however they want
        # Example 1: Aggregate by dimension
        dimension_summary = metrics_df.groupby('dimension').agg({
            'dq_rule': 'count',
            'failed_row_count': 'sum',
            'pass_rate': 'mean',
            'status': lambda x: (x == 'FAILED').sum()
        }).rename(columns={
            'dq_rule': 'total_rules',
            'failed_row_count': 'total_failure_rows',
            'pass_rate': 'avg_pass_rate',
            'status': 'failed_rules'
        })
        
        print("\nDimension Summary (User Aggregation):")
        print(dimension_summary)

if __name__ == "__main__":
    main()