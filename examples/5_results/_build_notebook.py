"""Generator for results_tour.ipynb.

Kept in-repo so the notebook can be regenerated deterministically rather than
hand-edited as raw JSON. Run: python examples/5_results/_build_notebook.py
"""

from __future__ import annotations

import json
from pathlib import Path

cells: list[dict] = []


def md(text: str) -> None:
    cells.append({"cell_type": "markdown", "metadata": {}, "source": text.strip("\n").splitlines(keepends=True)})


def code(text: str) -> None:
    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": text.strip("\n").splitlines(keepends=True),
        }
    )


md(
    """
# vowl · Results Tour

A finished validation is only useful once you can *read* it. This notebook is a
tour of every shape a `ValidationResult` can take once the checks have run,
using the HDB Resale dataset that ships with the repo (a copy seeded with
deliberate errors so some checks fail and there is something to look at).

Everything here reads an already-finished result. Nothing is re-run against your
data between sections, and each output is just a different view of the same run.

What this notebook covers:

1. Setup and a validation run to work with
2. **Annotated output**: your full table with a `check_info` column, flagged rows first
3. **Residual rows**: failed rows for checks that cannot attach to a single table
4. **Saving to disk**: the annotated tables and residues as CSV plus a JSON summary

Everything here needs only a base `vowl` install. The run's DQ metrics, as
`dq_metrics.json` and as OpenTelemetry, have their own notebook in
[`6_dq_metrics/`](../6_dq_metrics/dq_metrics.ipynb). The files this notebook
writes land in the `outputs/` folder beside it, kept in the repo as reference so
you can see the shapes without running anything.
"""
)

md(
    """
<a id="1-setup"></a>
## 1. Setup and a validation run

Resolve the dataset paths, load the HDB CSV, and run `validate_data`. Some checks
fail, which is exactly what we want: the outputs below are only interesting when
there is something flagged.
"""
)

code(
    """
# Install vowl (skipped during execution)
# %pip install vowl
"""
)

code(
    """
from pathlib import Path

# Walk up to the repo root (works no matter how deep this notebook sits)
REPO_ROOT = Path.cwd()
while not (REPO_ROOT / "tests" / "hdb_resale").exists() and REPO_ROOT != REPO_ROOT.parent:
    REPO_ROOT = REPO_ROOT.parent

# Single-source: HDB Resale dataset (used for sections 2 and 4)
HDB_DIR = REPO_ROOT / "tests" / "hdb_resale"
HDB_CSV = HDB_DIR / "HDBResaleWithErrors.csv"
HDB_CONTRACT = HDB_DIR / "hdb_resale_simple.yaml"  # Switch to "hdb_resale.yaml" for a more complex contract

# Multi-source: Employee dataset (payroll + employee list), used for residues in section 3
EMPLOYEE_DIR = REPO_ROOT / "tests" / "employee"
EMPLOYEE_PAYROLL_CSV = EMPLOYEE_DIR / "demo_employee_payroll.csv"
EMPLOYEE_LIST_CSV = EMPLOYEE_DIR / "demo_employee_list.csv"
EMPLOYEE_CONTRACT = EMPLOYEE_DIR / "employee_payroll_datacontract.yaml"

print(f"HDB CSV exists:      {HDB_CSV.exists()}")
print(f"HDB contract exists: {HDB_CONTRACT.exists()}")
"""
)

code(
    """
import warnings

import pandas as pd

from vowl import validate_data

# vowl surfaces informational UserWarnings on this demo dataset (Arrow coercion,
# adapter reuse). Quieten them so the output below stays readable.
warnings.filterwarnings("ignore", category=UserWarning, module=r"vowl\\.validation\\.runner")

df = pd.read_csv(HDB_CSV, low_memory=False).fillna("")
result = validate_data(contract=str(HDB_CONTRACT), df=df)

print(f"passed:        {result.passed}")
print(f"total checks:  {result._vs.get('total_checks')}")
print(f"failed:        {result._vs.get('failed')}")
print(f"errors:        {result._vs.get('errors')}")
"""
)

# --- Section 2: Annotated output (migrated from the Basic Tutorial) ---

md(
    """
<a id="2-annotated"></a>
## 2. Annotated Output: Your Full Table, Flagged

The main accessor here is **`get_annotated_output()`**. It returns your full table with one extra column, `check_info`, so you can see failures in the context of every row instead of on their own.

It returns a dict with two keys:

- **`"annotated"`**: `{schema: table}`. Each table is your full data plus a `check_info` column. Every original row is kept. `check_info` is `null` for rows that passed every check, and a list of the failing check(s) for rows that didn't.
- **`"residues"`**: failed rows for checks that can't be attached to a single table (cross-table, aggregation, and column-subset checks). The single-table HDB contract here has none; [section 3](#3-residual-non-mergeable-rows) shows them using a multi-source contract.

> Not every check can be merged into the annotated table. For the full rules and worked examples, see [Known Issues: Annotated Output](../../docs/known-issues.md#annotated-output-not-all-checks-can-be-merged).
"""
)

md(
    """
### Choosing How Much Detail: the `check_info` Options

`check_info` is a **list of objects, one per failing check** (stored as a JSON string in the column). The `check_info=` argument sets how many fields each object carries:

| `check_info` | Each object contains | Use it when |
|---|---|---|
| `"names"` *(default)* | `check_name` | You only need to know *which* checks a row failed |
| `"summary"` | `check_name`, `dimension`, `tags`, `target` | You want each failure's quality dimension, tags, and column (e.g. to split completeness vs. accuracy) |
| `"full"` | the entire check definition (`type`, `description`, `query`, `mustBe`, `dimension`, `tags`) plus `check_name` and `target` | You want everything, including the underlying rule/SQL, on every row |

All three return the **same shape** (a list of objects) so you always read a value the same way (`json.loads(cell)[0]["check_name"]`); they differ only in how many fields each object has. A row failing two checks gets a two-object list.

```text
"names"    [{"check_name": "Month"}]
"summary"  [{"check_name": "Month", "dimension": "conformity",
             "tags": null, "target": "hdb_resale_prices.month"}]
"full"     [{"type": "sql", "name": "Month", "description": "...", "mustBe": 0,
             "query": "SELECT COUNT(*) ...",
             "dimension": "conformity", "check_name": "Month",
             "target": "hdb_resale_prices.month"}]
```

The examples below use `"summary"`.
"""
)

code(
    """
# "annotated" -> {schema: your full table + a check_info column}
# "residues"  -> failed rows for checks that can't attach to one table (shown later)
# check_info="summary" -> each object also carries dimension, tags, and target.
output = result.get_annotated_output(check_info="summary")
annotated = output["annotated"]["hdb_resale_prices"].to_pandas()
"""
)

code(
    """
# Inspect the structure of the annotated output
print("Top-level keys:    ", list(output.keys()))
print("Annotated schemas: ", list(output["annotated"].keys()))
print("Residue keys:      ", list(output["residues"].keys()))
print(f"\\nAnnotated table: {annotated.shape[0]:,} rows x {annotated.shape[1]} columns")
print("Columns:", list(annotated.columns))
"""
)

md(
    """
### Inspecting the Full Table, Flagged Rows First

Every original row is present. `check_info` is `null` for rows that passed and a JSON list for rows that failed, one object per failing check (here carrying `dimension`, `tags`, and `target` from the `"summary"` preset). Sorting by `check_info` (nulls last) floats the flagged rows to the top while still showing the whole table.
"""
)

code(
    """
# Sort the full table so flagged rows surface first; passing rows (check_info = null) sink.
flagged_first = annotated.sort_values("check_info", na_position="last").reset_index(drop=True)
n_flagged = annotated["check_info"].notna().sum()
"""
)

code(
    """
print(f"{n_flagged} flagged rows at the top, {len(annotated) - n_flagged:,} passing rows below\\n")
flagged_first[["month", "town", "block", "floor_area_sqm",
               "lease_commence_date", "check_info"]].head(20)
"""
)

md(
    """
### Keeping the Clean Rows for Downstream Use

Because the annotation lives on the full table, separating the good rows from the bad is a one-liner. Filter to where `check_info` is null, drop the annotation column, and you have a clean dataset ready to feed downstream -- no separate join back to the source needed.
"""
)

code(
    """
# Keep only the rows that passed every check, then drop the annotation column
clean = annotated[annotated["check_info"].isna()].drop(columns=["check_info"])
"""
)

code(
    """
print(f"Clean rows ready for downstream use: {len(clean):,} of {len(annotated):,}")
clean.head()
"""
)

# --- Section 3: Residual rows (migrated from the Basic Tutorial) ---

md(
    """
<a id="3-residues"></a>
<a id="residual-rows"></a>
## 3. Residual (Non-Mergeable) Rows

The HDB examples above are single-table, so `residues` is empty. Residues appear when a check can't be attached to one table: **aggregation** checks, **column-subset** checks, and **cross-table** checks whose failed rows carry columns from more than the anchor table.

Residues are **per-check**: `get_annotated_output()` returns one entry for each non-mergeable check, keyed `"<schema>::<check_name>"`. They are never grouped together, so a row that fails two such checks appears once under each check's entry. Each entry carries the failed rows plus the same `check_info` column the annotated tables use (a single-element JSON array, shaped by the `check_info` preset) and `tables_in_query`, so everything `get_annotated_output()` returns is read the same way.

> **A cross-table check can *merge* instead of becoming a residue.** If you shape its row query to project only the anchor table's columns (e.g. `SELECT payroll.*` inside a subquery), the orphan rows match that schema and land directly in its `check_info` column, with no residue. The Employee contract below carries both shapes: `orphan_payroll_rows_merge_onto_payroll` (subquery-projected, merges onto `demo_employee_payroll`) and `employee_id_exists_in_master_list` / `phone_number_exists_in_master_list` (bare JOINs, stay residues). See [Known Issues: Annotated Output](../../docs/known-issues.md#annotated-output-not-all-checks-can-be-merged) for the rules.

Below is a quick multi-source run on the Employee dataset, whose contract has cross-table checks. (Multi-source validation is covered in the [Multiple Sources notebook](../2_multiple_sources/multiple_sources.ipynb); we borrow it here just to produce residues.)
"""
)

code(
    """
import ibis
from vowl.adapters import IbisAdapter

# Load the two Employee tables onto one DuckDB connection (cross-table contract)
con = ibis.duckdb.connect()
con.create_table("demo_employee_payroll", pd.read_csv(EMPLOYEE_PAYROLL_CSV))
con.create_table("demo_employee_list", pd.read_csv(EMPLOYEE_LIST_CSV))

mt_result = validate_data(contract=str(EMPLOYEE_CONTRACT), adapter=IbisAdapter(con))
mt_output = mt_result.get_annotated_output()
"""
)

code(
    """
# Each residue is the failed rows for ONE non-mergeable check, keyed "<schema>::<check>"
print("Annotated schemas:", list(mt_output["annotated"].keys()))
print("Residue keys:     ", list(mt_output["residues"].keys()))

for key, residue in mt_output["residues"].items():
    residue_df = residue.to_pandas()
    print(f"\\nResidue '{key}': {len(residue_df)} failed row(s)")
    display(residue_df[["employee_id", "payroll_id", "month",
                        "check_info", "tables_in_query"]])
"""
)

md(
    """
### The Other Side: a Cross-Table Check That *Merges*

Whether a cross-table check merges is decided entirely by **the columns its failed rows come back with**: they have to match the anchor table's columns exactly. The two checks above join `payroll` against a reference table with a bare `SELECT *`, so their failed rows carry columns from *both* tables and can't land on any single table, so they become a residue.

The *same* referential question **merges** when you wrap it so the inner query projects only the payroll columns (`SELECT payroll.* ...`). The contract's `orphan_payroll_rows_merge_onto_payroll` check does exactly that, so its failed rows come back with exactly the payroll columns and are annotated **directly onto the payroll table's `check_info` column** (notice it's absent from the residue keys above).

> For the full mechanics (how vowl derives the scalar and row queries, and why the `COUNT(*)` -> `SELECT *` rewrite only touches the outer projection), see [Known Issues: Cross-table checks, mergeable when the failed rows match the home schema](../../docs/known-issues.md#1-cross-table-checks-mergeable-when-the-failed-rows-match-the-home-schema).
"""
)

code(
    """
# The subquery-projected cross-table check lands on the payroll annotated table,
# not in residues. Find the payroll rows it flagged.
payroll_annotated = mt_output["annotated"]["demo_employee_payroll"].to_pandas()

MERGED_CHECK = "orphan_payroll_rows_merge_onto_payroll"
flagged = payroll_annotated[
    payroll_annotated["check_info"].fillna("").str.contains(MERGED_CHECK)
]

print(f"'{MERGED_CHECK}' in residues? "
      f"{any(MERGED_CHECK in k for k in mt_output['residues'])}")
print(f"Payroll rows it annotated: {len(flagged)}\\n")
display(flagged[["employee_id", "month", "check_info"]])
"""
)

# --- Section 4: Saving to disk (migrated from the Basic Tutorial) ---

md(
    """
<a id="4-saving"></a>
## 4. Saving Outputs to Disk

`result.save(...)` always writes `<prefix>_check_results.csv` and `<prefix>_summary.json`. The `outputs` argument lists the other files to write. By default it writes every output except `"all_query_outputs"`:

| Output | Files written | Default |
|--------|---------------|---------|
| `"failed_query_outputs"` | One `<prefix>_checks/<schema>__<check>.csv` per failed check, with that check's own columns | ✓ |
| `"all_query_outputs"` | The same folder, one file per row-level check whatever its status, with a `status` column. Can't be combined with `"failed_query_outputs"`. | |
| `"consolidated_query_outputs"` | One grouped failed-rows CSV per set of tables, e.g. `<prefix>_<table>.csv` | ✓ |
| `"annotated_table"` | One `<prefix>_<schema>_annotated.csv` per schema (full table + `check_info`), **plus** one `<prefix>_<schema>__<check>_residue.csv` per non-mergeable check | ✓ |
| `"dq_metrics"` | The run's [DQ metrics](../6_dq_metrics/dq_metrics.ipynb) as `<prefix>_dq_metrics.json` | ✓ |

`<prefix>_summary.json` lists every file under `saved_outputs`, with its row count. A check with no rows writes no file and is listed with `"rows": 0`.

The `check_info` argument (`"names"` / `"summary"` / `"full"`, described above) sets how much detail the saved `check_info` column carries. To set the `check_info` detail for every `save()`, pass `ValidationConfig(annotated_check_info="summary")` to `validate_data(config=...)`.

`"annotated_table"` and `"dq_metrics"` attribute failed rows to each table and may download it, which can be slow on a large table. They share that work, so writing both costs no more than writing one. The other outputs never download a table, so pick `outputs=["failed_query_outputs"]` or `["consolidated_query_outputs"]` when you only need the failed rows.

Single-table contracts (like the HDB example) have no non-mergeable checks, so they never produce `_residue` files. The multi-source run below does, so let's save both to see the difference. Both saves below keep to the annotated tables and the DQ metrics. The files land in the `outputs/` folder beside this notebook.
"""
)

code(
    """
# Write the annotated table(s) to disk
result.save(output_dir="outputs", prefix="vowl_demo_annotated", outputs=["annotated_table", "dq_metrics"])
"""
)

md(
    """
### Saving a Run That Has Residues

`result` above is single-table, so its save wrote only `*_annotated.csv` files. Saving the multi-source `mt_result` from the residue example instead also writes one `*_residue.csv` per non-mergeable check, named `<schema>__<check>` (the schema and the check name are cleaned on their own and joined with `__`). Each residue CSV carries that one check's failed rows plus the same `check_info` column as the annotated tables and `tables_in_query`.
"""
)

code(
    """
import os

# Save the multi-source run. The annotated output writes residue CSVs for the
# cross-table checks that can't be merged onto a single table.
mt_result.save(output_dir="outputs", prefix="vowl_demo_residues", outputs=["annotated_table", "dq_metrics"])

print("\\nFiles written:")
for fname in sorted(f for f in os.listdir("outputs") if f.startswith("vowl_demo_residues")):
    tag = "  <- residue" if fname.endswith("_residue.csv") else ""
    print(f"   {fname}{tag}")
"""
)

md(
    """
### Where the Full Check Definitions Live

To keep the annotated table readable, you'll usually pick `"names"` or `"summary"` rather than `"full"`. You don't lose anything: the complete definition of **every** check (its `dimension`, `tags`, `description`, `query`, and more) is always written to the `*_summary.json` file that `save()` produces, keyed by check name. Keep the table light, and look up the rest here when you need it.
"""
)

code(
    """
import json

# save(prefix="vowl_demo_annotated", ...) wrote this alongside the CSVs.
with open("outputs/vowl_demo_annotated_summary.json") as f:
    summary = json.load(f)

# Every check's resolved definition lives under "check_results", keyed by name.
month_check = next(c for c in summary["check_results"] if c["name"] == "Month")

print("Target:    ", month_check["target"])
print("Dimension: ", month_check["check_definition"]["dimension"])
print("\\nFull check_definition:")
print(json.dumps(month_check["check_definition"], indent=2))
"""
)

md(
    """
---

### Recap

- `get_annotated_output()` returns your full table with a `check_info` column, so
  failed rows read in context and the clean rows fall out with a single filter.
- Checks that can't attach to one table come back as **residues**, one entry per
  non-mergeable check.
- `result.save(outputs=["annotated_table", ...])` writes those same shapes to disk as CSV,
  plus a `*_summary.json` that always carries every full check definition and a
  `*_dq_metrics.json` with the run's DQ metrics.
- The `outputs/` folder holds a saved example of each output for reference.

Next, [`6_dq_metrics/dq_metrics.ipynb`](../6_dq_metrics/dq_metrics.ipynb) reads the
same run as DQ metrics: the `*_dq_metrics.json` file written above, and the
OpenTelemetry export for your monitoring tools.
"""
)

notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "vowl", "language": "python", "name": "python3"},
        "language_info": {
            "codemirror_mode": {"name": "ipython", "version": 3},
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

out = Path(__file__).with_name("results_tour.ipynb")
out.write_text(json.dumps(notebook, indent=1) + "\n")
print(f"wrote {out} ({len(cells)} cells)")
