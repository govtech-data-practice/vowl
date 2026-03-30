"""Basic usage example for vowl.

Validates the bundled HDB Resale sample data against a simple ODCS contract.
Run from the project root:

    uv run python examples/basic_usage.py
"""

from pathlib import Path

import pandas as pd

from vowl import validate_data

# Resolve paths relative to the repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
HDB_DIR = REPO_ROOT / "tests" / "hdb_resale"
HDB_CSV = HDB_DIR / "HDBResaleWithErrors.csv"
HDB_CONTRACT = HDB_DIR / "hdb_resale.yaml"

if __name__ == "__main__":
    df = pd.read_csv(HDB_CSV)
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns\n")

    result = validate_data(contract=str(HDB_CONTRACT), df=df)
    result.display_full_report()
