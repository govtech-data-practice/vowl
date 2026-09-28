"""Generator for outputs_tour.ipynb.

Kept in-repo so the notebook can be regenerated deterministically rather than
hand-edited as raw JSON. Run: python examples/5_outputs/_build_notebook.py
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
# vowl · Outputs Tour

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
5. **OpenTelemetry**: the same run as metrics, traces, and logs

Sections 2 to 4 need only a base `vowl` install. Section 5 is optional and lives
behind the `[otel]` extra. The files this notebook writes land in the `outputs/`
folder beside it, kept in the repo as reference so you can see the shapes without
running anything.
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
# Install vowl with the OpenTelemetry extra used in section 5 (skipped during execution)
# %pip install 'vowl[otel]'
"""
)

code(
    """
from pathlib import Path

# Walk up to the repo root (works no matter how deep this notebook sits)
REPO_ROOT = Path.cwd()
while not (REPO_ROOT / "tests" / "hdb_resale").exists() and REPO_ROOT != REPO_ROOT.parent:
    REPO_ROOT = REPO_ROOT.parent

# Single-source: HDB Resale dataset (used for sections 2, 4, and 5)
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
             "tags": ["SG-DRM v5.0"], "target": "hdb_resale_prices.month"}]
"full"     [{"type": "sql", "name": "Month", "description": "...", "mustBe": 0,
             "query": "SELECT COUNT(*) ...", "tags": ["SG-DRM v5.0"],
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

> **A cross-table check can *merge* instead of becoming a residue.** If you shape its failed-rows query to project only the anchor table's columns (e.g. `SELECT payroll.*` inside a subquery), the orphan rows match that schema and land directly in its `check_info` column, with no residue. The Employee contract below carries both shapes: `orphan_payroll_rows_merge_onto_payroll` (subquery-projected, merges onto `demo_employee_payroll`) and `employee_id_exists_in_master_list` / `phone_number_exists_in_master_list` (bare JOINs, stay residues). See [Known Issues: Annotated Output](../../docs/known-issues.md#annotated-output-not-all-checks-can-be-merged) for the rules.

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

> For the full mechanics (how vowl derives the scalar and failed-rows queries, and why the `COUNT(*)` -> `SELECT *` rewrite only touches the outer projection), see [Known Issues: Cross-table checks, mergeable when the failed rows match the home schema](../../docs/known-issues.md#1-cross-table-checks-mergeable-when-the-failed-rows-match-the-home-schema).
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

`result.save(...)` can write the annotated tables instead of (or alongside) the failed-rows CSVs. The `output_mode` argument controls the layout (every mode also writes `<prefix>_check_results.csv` and `<prefix>_summary.json`):

| Mode | Files written |
|------|----------------|
| `"failed_rows"` (default) | One grouped failed-rows CSV per table key, e.g. `<prefix>_<table>.csv` |
| `"annotated"` | One `<prefix>_<schema>_annotated.csv` per schema (full table + `check_info`), **plus** one `<prefix>_<schema>_<check>_residue.csv` per non-mergeable check |
| `"both"` | The failed-rows CSVs **and** the annotated CSVs (residues are already covered by the grouped failed-rows CSVs, so no separate `_residue` files are written) |

The `check_info` argument (`"names"` / `"summary"` / `"full"`, described above) sets how much detail the saved `check_info` column carries. To make annotated output the default for every `save()`, pass `ValidationConfig(output_mode="annotated", annotated_check_info="summary")` to `validate_data(config=...)`.

Single-table contracts (like the HDB example) have no non-mergeable checks, so they never produce `_residue` files. The multi-source run below does, so let's save both to see the difference. The files land in the `outputs/` folder beside this notebook.
"""
)

code(
    """
# Write the annotated table(s) to disk
result.save(output_dir="outputs", prefix="vowl_demo_annotated", output_mode="annotated")
"""
)

md(
    """
### Saving a Run That Has Residues

`result` above is single-table, so its `save(output_mode="annotated")` wrote only `*_annotated.csv` files. Saving the multi-source `mt_result` from the residue example instead also writes one `*_residue.csv` per non-mergeable check, keyed `<schema>_<check>` (the `::` in the residue key becomes `_`). Each residue CSV carries that one check's failed rows plus the same `check_info` column as the annotated tables and `tables_in_query`.
"""
)

code(
    """
import os

# Save the multi-source run; annotated mode emits residue CSVs for the
# cross-table checks that can't be merged onto a single table.
mt_result.save(output_dir="outputs", prefix="vowl_demo_residues", output_mode="annotated")

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
print("Tags:      ", month_check["check_definition"]["tags"])
print("\\nFull check_definition:")
print(json.dumps(month_check["check_definition"], indent=2))
"""
)

# --- Section 5: OpenTelemetry (condensed from the former OTEL notebook) ---

md(
    """
<a id="5-otel"></a>
## 5. OpenTelemetry: Metrics, Traces, and Logs

This last output shape sends the finished run to your monitoring tools, so
data-quality results show up on the same dashboards and alerts as everything else.
`export_otel()` reuses the `result` from section 1, so nothing re-runs. It is fully
optional: the feature lives behind the `[otel]` extra, and `import vowl` never pulls
in `opentelemetry`.

In production you point vowl at an OTLP endpoint:

```python
run_id = result.export_otel(
    endpoint="http://localhost:4317",   # or leave unset to use OTEL_EXPORTER_OTLP_* env vars
    signals=("metrics", "traces", "logs"),  # logs are opt-in, default is metrics + traces
    service_name="hdb-resale-dq",
    custom_attributes={
        "deployment.environment": "prod",
        "vowl.artifact.uri": "s3://dq/run=0f2c9e1a/",  # link alerts to the rows you saved
    },
)
```

That needs a live collector, so we do not run it here. Instead we hand `export_otel`
some **in-memory providers** so everything runs offline. The call returns a run id
now, and at the end of this section we serialize the signals it emitted to files you
can open.

> **Demo only.** These in-memory providers hold each signal in memory and lose it
> when the kernel stops. In a real pipeline you export to your own collector (the
> `endpoint` call above) and let it handle storage, dashboards, and alerts.
"""
)

md(
    """
`export_otel` also takes explicit `metric_provider` / `tracer_provider` /
`logger_provider`. When you pass your own, vowl emits into them but does not flush
or shut them down, so we can read them back afterwards. Here we use the in-memory
ones the OpenTelemetry SDK ships.
"""
)

code(
    """
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter, SimpleLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


def in_memory_providers(resource=None):
    \"\"\"A meter/tracer/logger trio wired to in-memory readers we can inspect.

    Pass ``resource`` to stamp every signal with a specific OTEL resource. Leave
    it unset to get the SDK default (fine when we only care about the signals).
    \"\"\"
    metric_reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[metric_reader], resource=resource)

    span_exporter = InMemorySpanExporter()
    tracer = TracerProvider(resource=resource)
    tracer.add_span_processor(SimpleSpanProcessor(span_exporter))

    log_exporter = InMemoryLogRecordExporter()
    logger = LoggerProvider(resource=resource)
    logger.add_log_record_processor(SimpleLogRecordProcessor(log_exporter))

    return meter, metric_reader, tracer, span_exporter, logger, log_exporter


meter, metric_reader, tracer, span_exporter, logger, log_exporter = in_memory_providers()

run_id = result.export_otel(
    signals=("metrics", "traces", "logs"),
    service_name="hdb-resale-dq",
    custom_attributes={"deployment.environment": "demo", "team": "housing-data"},
    metric_provider=meter,
    tracer_provider=tracer,
    logger_provider=logger,
)
print(f"exported run id: {run_id}")
"""
)

md(
    """
### How the signals flow through your stack

The three signals do different jobs. `export_otel` hands them to an OpenTelemetry
Collector, which sends each one where it belongs. **Metrics** feed dashboards you
watch over time. **Logs and traces** carry the per-check detail (the SQL, the full
definition, a row sample), so an alert can open a data incident with everything an
on-call needs already attached.

```mermaid
flowchart LR
    vowl["vowl validation run"] -->|"export_otel(...)"| collector["OpenTelemetry Collector"]
    collector --> metrics["Metrics"]
    collector --> logstraces["Logs & Traces"]
    metrics --> agg["Aggregation"] --> dash["DQ reporting & dashboards"]
    logstraces --> errors["Listen for errors"] --> incident["Automated data-incident reporting"]
```

Metrics tell you how quality is trending. Logs and traces tell you what broke and
where. The table below lays out who carries what.
"""
)

md(
    """
### What each signal carries

Same run, three signals, split by job. Metrics stay lightweight so they can feed
dashboards. Traces and logs carry the heavy detail (the SQL, the full definition, a
row sample) for digging into what broke.

| Signal | One per | Carries | Reach for it to |
| ------ | ------- | ------- | --------------- |
| **Metrics** | run (counts) | a few safe labels only: `schema`, `dimension`, `severity`, `engine`, `check_name` | watch quality trends and alert on rates |
| **Traces** | check | those same labels, plus the SQL that ran and the full check definition (`check.definition.*`) | see exactly what one check did |
| **Logs** | failing check only | severity (`WARN` for a data failure, `ERROR` when vowl itself broke), the SQL, the `trace_id`/`span_id`, and a short message | fire error alerts and jump straight to the trace |

You can see all three for real in the `metrics.json`, `traces.json`, and `logs.json`
files written at the end of this section.
"""
)

md(
    """
### Context attributes on every signal

vowl attaches a set of **context attributes** to every metric data point, span, and
log record, so each signal says which run and contract it came from. You never build
them by hand. They carry:

- `service.name` and `vowl.version`
- `vowl.run.id`, the same id `export_otel(...)` returned above
- `vowl.contract.*` pulled from your contract (id, version, status, and so on)
- anything you passed in `custom_attributes`, added as-is

These attributes are signal-level in all three provider modes (explicit, global, and
self-contained), so they are always present regardless of how the providers are set
up. In self-contained mode they additionally go on the OTEL Resource for backends
that surface resource metadata separately.
"""
)

md(
    """
### Linking an alert back to the rows

vowl exports **how many** rows failed, never the rows themselves. Three ways to
get from an alert to the actual data, weakest to strongest:

- **`failed_rows_count`** rides on every check, always on. No cell values leave the
  process.
- **An inline sample** via `max_failed_rows_sample`. Off by default. Set a positive
  number and each failing check attaches that many rows to its log and span. Mind
  PII, since these are real cell values.
- **A pointer** to wherever you saved the rows yourself.

The pointer goes in `custom_attributes`, which vowl copies onto every signal
untouched. The suggested key is `vowl.artifact.uri`, pointing at the output
`result.save(...)` wrote for this run:

```python
result.save("s3://dq/run=0f2c9e1a/")  # your failed rows and annotated tables land here
result.export_otel(
    custom_attributes={"vowl.artifact.uri": "s3://dq/run=0f2c9e1a/"},  # alerts deep-link to them
)
```

Use `vowl.link.<name>` (for example `vowl.link.runbook` or `vowl.link.ticket`) for
any other link you want on the run. vowl always sets `vowl.run.id`, so a run still
matches an artifact saved under that id even with no pointer. These are naming
suggestions, not special keys, so a team spells the same pointer the same way. See
the [OpenTelemetry Export guide](../../docs/otel-export.md) for the full list.

### The signals as files you can eyeball

This last cell writes one file per signal into the `outputs/` folder beside this
notebook: `metrics.json`, `traces.json`, and `logs.json`, each as the OpenTelemetry
SDK serializes it. Open them to see the exact shape a collector receives.

Here we set `max_failed_rows_sample` to `2` (it is `0` by default) so each failure
log and span carries a couple of the offending rows, which is what you would attach
for triage. Timestamps and the run, trace, and span ids vary from run to run.
"""
)

code(
    """
ometer, oreader, otracer, ospans, ologger, ologs = in_memory_providers()
result.export_otel(
    signals=("metrics", "traces", "logs"),
    max_failed_rows_sample=2,
    metric_provider=ometer,
    tracer_provider=otracer,
    logger_provider=ologger,
)

OUTPUTS = REPO_ROOT / "examples" / "5_outputs" / "outputs"
OUTPUTS.mkdir(exist_ok=True)

(OUTPUTS / "metrics.json").write_text(
    json.dumps(json.loads(oreader.get_metrics_data().to_json()), indent=2) + "\\n"
)
(OUTPUTS / "traces.json").write_text(
    json.dumps([json.loads(s.to_json()) for s in ospans.get_finished_spans()], indent=2) + "\\n"
)
(OUTPUTS / "logs.json").write_text(
    json.dumps([json.loads(e.to_json()) for e in ologs.get_finished_logs()], indent=2) + "\\n"
)

for path in sorted(OUTPUTS.glob("*.json")):
    print(f"{path.name:14} {path.stat().st_size:>7,} bytes")
"""
)

md(
    """
---

### Recap

- `get_annotated_output()` returns your full table with a `check_info` column, so
  flagged rows read in context and the clean rows fall out with a single filter.
- Checks that can't attach to one table come back as **residues**, one entry per
  non-mergeable check.
- `result.save(output_mode="annotated")` writes those same shapes to disk as CSV
  plus a `*_summary.json` that always carries every full check definition.
- `result.export_otel(...)` sends the run to OpenTelemetry: lightweight metrics
  for dashboards, and traces and logs carrying the SQL and full definition for
  digging into what broke.
- The `outputs/` folder holds a serialized sample of each shape for reference.

The full parameters, signal schema, and correlation keys live in the
[OpenTelemetry Export guide](../../docs/otel-export.md).
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

out = Path(__file__).with_name("outputs_tour.ipynb")
out.write_text(json.dumps(notebook, indent=1) + "\n")
print(f"wrote {out} ({len(cells)} cells)")
