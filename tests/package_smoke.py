"""Smoke-test an installed vowl package, not the repository source.

CI installs the built sdist into a clean environment and runs this from
outside the checkout, so a module or data file left out of the sdist fails
here before it reaches PyPI. It is not collected by pytest.

Usage: python -I package_smoke.py <dir with hdb_resale_simple.yaml and HDBResaleWithErrors.csv>
"""

import json
import sys
import tempfile
from importlib.resources import files
from pathlib import Path

import pandas as pd

import vowl
from vowl import validate_data

data_dir = Path(sys.argv[1])

assert "site-packages" in vowl.__file__, f"imported vowl from {vowl.__file__}, not an installed package"

schemas = sorted(p.name for p in files("vowl.contracts.models").joinpath("schemas").iterdir())
assert "odcs-json-schema-v3.2.0.json" in schemas, f"bundled schemas missing: {schemas}"

df = pd.read_csv(data_dir / "HDBResaleWithErrors.csv")
result = validate_data(str(data_dir / "hdb_resale_simple.yaml"), df=df)

statuses = result.get_check_results_df().to_pandas()["status"].value_counts().to_dict()
assert statuses == {"PASSED": 7, "FAILED": 2}, statuses

metrics = result.get_dq_metrics_df().to_pandas().iloc[0]
assert (metrics["total_rows"], metrics["failed_rows"]) == (201879, 14), metrics.to_dict()

with tempfile.TemporaryDirectory() as out:
    result.save(out, prefix="smoke")
    summary = json.loads((Path(out) / "smoke_summary.json").read_text())
    assert {"all_query_outputs", "consolidated_query_outputs", "annotated_table", "dq_metrics"} <= set(
        summary["saved_outputs"]
    ), summary["saved_outputs"]

import vowl.otel  # noqa: E402,F401  the [otel] extra must import

print(f"vowl {vowl.__version__} package smoke test passed")
