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
        # --- Use Case 1: The Quick & Complete Report ---
        result.display_full_report()

        # --- Use Case 2: Programmatic Check in a Pipeline ---
        if result.passed:
            print("✅ All checks passed. Pipeline can continue.")
        else:
            print("❌ Checks failed. Retrieving failed data for remediation...")
            failed_data = result.get_failed_rows()
            print(f"   - Found {len(failed_data)} rows that failed tests")

        # --- Use Case 3: Building a Custom View ---
        result.show_failed_rows(max_rows=7)


if __name__ == "__main__":
    main()