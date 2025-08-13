import pandas as pd
import sys
import os
import json

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", 'src')))

from dataquality import validate_data

def main():    
    
    test_data = os.path.join(os.path.dirname(__file__), 'HDBResale.csv')
    df = pd.read_csv(test_data)

    # 2. Run validation
    summary, dq_df = validate_data(df, contract_name="hdb_resale")

    # 3. Print a summary
    print("=== Validation Summary ===")
    print(f"Total Checks:     {summary['validation_summary']['total_checks']}")
    print(f"Passed:           {summary['validation_summary']['passed']}")
    print(f"Failed:           {summary['validation_summary']['failed']}")
    print(f"Success Rate:     {summary['validation_summary']['success_rate']:.1f}%")
    print(f"Total Rows:       {summary['validation_summary']['total_rows']:,}")
    print(f"Failed Rows:      {summary['validation_summary']['failed_rows']:,}")



    failed_rows = dq_df[dq_df['dq_validation_status'] == 'FAILED']
    if not failed_rows.empty:
        print("\n--- Sample of Failed Rows ---")
        # Display all columns for the failed rows
        print(failed_rows.head().to_string(index=False))

    # 5. Save the full results for further analysis
    dq_df.to_csv('hdb_data_with_dq_results.csv', index=False)
    with open('validation_summary_report.json', 'w') as f:
        json.dump(summary, f, indent=2)
    print("Validation results saved to 'hdb_data_with_quality_results.csv' and 'validation_summary_report.json'.")

if __name__ == "__main__":
    main()
