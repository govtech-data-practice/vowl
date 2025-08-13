import argparse
import sys
import os
import pandas as pd
from pathlib import Path
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dataquality import validate_data

def main():
    parser = argparse.ArgumentParser(description="Validate a data file against a data contract.")
    
    parser.add_argument("data_file", help="Path to the data file (CSV, JSON, Parquet)")
    parser.add_argument("--contract", help="Name of a built-in contract (e.g., 'hdb_resale')")
    parser.add_argument("--contract-file", help="Path to a custom contract YAML file")
    parser.add_argument("--output", "-o", help="Path to save summary JSON file")
    parser.add_argument("--save-data", help="Path to save the enhanced data to a CSV file")
    parser.add_argument("--failures-only", action="store_true", help="Show only rows that failed validation")
    
    args = parser.parse_args()
    
    if not args.contract and not args.contract_file:
        parser.error("Either --contract or --contract-file must be specified.")
    
    # Load data
    try:
        df = load_dataframe(args.data_file)
        print(f"Loaded {len(df):,} rows from '{Path(args.data_file).name}'.")
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Run validation
    try:
        summary, enhanced_df = validate_data(df, contract_name=args.contract, contract_path=args.contract_file)
    except Exception as e:
        print(f"Validation Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Print results
    print_summary(summary)
    
    if args.failures_only:
        show_failed_rows(enhanced_df)
    
    # Save outputs
    if args.output:
        save_json(summary, args.output)
        print(f"Summary report saved to '{args.output}'.")
    
    if args.save_data:
        enhanced_df.to_csv(args.save_data, index=False)
        print(f"Enhanced data saved to '{args.save_data}'.")
    
    if summary['validation_summary']['failed'] > 0:
        sys.exit(1)

def load_dataframe(file_path: str) -> pd.DataFrame:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found at '{file_path}'")
    
    suffix = path.suffix.lower()
    if suffix == '.csv':
        return pd.read_csv(file_path)
    if suffix == '.json':
        return pd.read_json(file_path)
    if suffix in ['.parquet', '.pq']:
        return pd.read_parquet(file_path)
    
    raise ValueError(f"Unsupported file format: '{suffix}'. Use CSV, JSON, or Parquet.")

def print_summary(summary: dict):
    vs = summary['validation_summary']
    print("\n--- Validation Summary ---")
    print(f"  Result: {vs['passed']} / {vs['total_checks']} checks passed ({vs['success_rate']:.1f}%)")
    print(f"  Rows with issues: {vs['failed_rows']:,} / {vs['total_rows']:,}")
    
    if vs['failed'] > 0:
        print("\nFailed Checks:")
        for check in summary['check_results']:
            if check['status'] == 'FAILED':
                print(f"  - {check['name']}: {check['details']} ({check['failed_rows_count']:,} rows)")

def show_failed_rows(enhanced_df: pd.DataFrame):
    failed_rows = enhanced_df[enhanced_df['dq_validation_status'] == 'FAILED']
    if failed_rows.empty:
        return
    
    print("\n--- Sample of Failed Rows ---")
    print(f"Showing first {min(10, len(failed_rows))} of {len(failed_rows)} failed rows:")
    
    display_cols = [c for c in enhanced_df.columns if not c.startswith('dq_')]
    display_cols.append('dq_failed_tests_str')
    
    print(failed_rows[display_cols].head(10).to_string(index=False))

def save_json(data: dict, path: str):
    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        print(f"Error saving to '{path}': {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
