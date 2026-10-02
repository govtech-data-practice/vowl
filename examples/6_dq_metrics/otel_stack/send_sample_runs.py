"""Send vowl validation runs for a small catalog of contracts to the local OTel stack.

Each round validates every contract in CATALOG against a random sample of its
data, so pass rates move between rounds and the dashboard has something to
draw. Run from the repo root (or use `make otel-add-vowl-runs` in this folder):

    uv run python examples/6_dq_metrics/otel_stack/send_sample_runs.py --runs 5
    uv run python examples/6_dq_metrics/otel_stack/send_sample_runs.py --contracts payroll,retail
    uv run python examples/6_dq_metrics/otel_stack/send_sample_runs.py --protocol http/protobuf
"""

import argparse
import time
import warnings
from dataclasses import dataclass
from pathlib import Path

import ibis
import pandas as pd

from vowl import validate_data
from vowl.adapters import IbisAdapter

REPO_ROOT = Path(__file__).resolve().parents[3]
TESTS = REPO_ROOT / "tests"
REL = TESTS / "relationships"

ENDPOINTS = {"grpc": "http://localhost:4317", "http/protobuf": "http://localhost:4318"}


@dataclass
class Entry:
    """One contract in the catalog and the CSV behind each of its schemas."""

    contract: Path
    tables: dict[str, tuple[Path, dict | str | None]]  # schema name -> (CSV, pandas dtype)


# Each contract's `domain` field (hr, retail) is exported as the
# vowl.domain attribute, so the dashboard can group the catalog by domain.
CATALOG = {
    "payroll": Entry(TESTS / "employee" / "employee_payroll_datacontract.yaml",
                     {"demo_employee_payroll": (TESTS / "employee" / "demo_employee_payroll.csv", None),
                      "demo_employee_list": (TESTS / "employee" / "demo_employee_list.csv", None)}),
    "org": Entry(REL / "org_datacontract.yaml",
                 {"employees": (REL / "employees.csv",
                                {"employee_id": "Int64", "manager_id": "Int64", "employee_name": "string"})}),
    "retail": Entry(REL / "retail_datacontract.yaml",
                    {"customers": (REL / "customers.csv", {"customer_id": "Int64", "customer_name": "string"}),
                     "orders": (REL / "orders.csv", {"order_id": "Int64", "customer_id": "Int64", "amount": "Int64"})}),
    "catalog": Entry(REL / "catalog_datacontract.yaml",
                     {"products": (REL / "products.csv", "string"),
                      "order_items": (REL / "order_items.csv",
                                      {"item_id": "Int64", "category": "string", "sku": "string", "qty": "Int64"})}),
}


def load(entry: Entry) -> dict[str, pd.DataFrame]:
    frames = {}
    for schema, (csv, dtype) in entry.tables.items():
        df = pd.read_csv(csv, dtype=dtype, low_memory=False)
        # CSVs without a dtype have mixed-type columns on purpose. Blank them so they load as text.
        frames[schema] = df.fillna("") if dtype is None else df
    return frames


def sample(df: pd.DataFrame, frac: float, seed: int) -> pd.DataFrame:
    return df.sample(n=max(1, round(len(df) * frac)), random_state=seed)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=3, help="rounds, each validates every contract")
    parser.add_argument("--contracts", default="all",
                        help=f"comma-separated subset of {','.join(CATALOG)}, or all")
    parser.add_argument("--protocol", choices=sorted(ENDPOINTS), default="grpc")
    parser.add_argument("--service-name", default="vowl-local-smoke")
    parser.add_argument("--sleep", type=float, default=2.0, help="seconds between rounds")
    args = parser.parse_args()

    names = list(CATALOG) if args.contracts == "all" else args.contracts.split(",")
    unknown = set(names) - set(CATALOG)
    if unknown:
        parser.error(f"unknown contracts: {', '.join(sorted(unknown))}")

    warnings.filterwarnings("ignore", message="Arrow type conversion failed")
    data = {name: load(CATALOG[name]) for name in names}

    for i in range(args.runs):
        frac = 0.5 + 0.5 * (i + 1) / args.runs
        for name in names:
            con = ibis.duckdb.connect()
            for schema, df in data[name].items():
                con.create_table(schema, sample(df, frac, seed=i))
            result = validate_data(contract=str(CATALOG[name].contract),
                                   adapters={schema: IbisAdapter(con) for schema in data[name]})
            run_id = result.export_otel(
                endpoint=ENDPOINTS[args.protocol],
                protocol=args.protocol,
                service_name=args.service_name,
                custom_attributes={"deployment.environment": "local"},
                max_failed_rows_sample=3,
            )
            summary = result.summary["validation_summary"]
            print(f"round {i + 1}/{args.runs} {name:<8} {summary['passed']}/{summary['total_checks']} checks passed, "
                  f"run_id={run_id}")
        if i + 1 < args.runs:
            time.sleep(args.sleep)


if __name__ == "__main__":
    main()
