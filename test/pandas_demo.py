import pandas as pd
import os
from dataquality import validate_data

def main():
    """
    A clear and concise demonstration of the data quality library's features,
    structured by common use cases.
    """
    print("--- Pandas Demo: Data Quality Validation ---")

    # 1. Define paths and load data
    test_data_path = os.path.join(os.path.dirname(__file__), 'HDBResale.csv')
    df = pd.read_csv(test_data_path)
    print(f"Loaded {len(df)} rows from {os.path.basename(test_data_path)}")

    # 2. Run validation to get the single, powerful result object

    result = validate_data(df, contract_path="/Users/dinesh/dqmk/src/dataquality/contracts/hdb_resale.yaml")


    # --- Use Case 1: The Quick & Complete Report ---
    # This is the most common use case for interactive analysis.
    # For a full overview, just use .display_full_report()
    result.display_full_report()

    # --- Use Case 2: Programmatic Check in a Pipeline ---
    # This shows how to use the library in an automated script.
    # Using the '.passed' property to control a workflow:
    if result.passed:
        print("✅ All checks passed. Pipeline can continue.")
    else:
        print("❌ Checks failed. Retrieving failed data for remediation...")
        # Get the actual failed data for programmatic use
        failed_data = result.get_failed_rows()
        print(f"   - Found {len(failed_data)} rows that failed tests")
        # In a real pipeline, you would now save or process this `failed_data` DataFrame.

    # --- Use Case 3: Building a Custom View ---
    # This shows how to combine methods for a specific, non-saving report.
    result.show_failed_rows(max_rows=3)


if __name__ == "__main__":
    main()