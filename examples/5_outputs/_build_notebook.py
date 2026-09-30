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

`result.save(...)` can write the annotated tables instead of (or alongside) the failed-rows CSVs. The `output_mode` argument controls the layout (every mode also writes `<prefix>_check_results.csv`, `<prefix>_summary.json`, and `<prefix>_dq_metrics.json`, the run's [DQ metrics](../../docs/dq-metrics/index.md)):

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

This last output sends the finished run to your monitoring tools, so data quality
results show up on the same dashboards and alerts as everything else.
`export_otel()` reuses the `result` from section 1, so nothing runs again. It is
optional: install it with the `[otel]` extra. `import vowl` never loads
OpenTelemetry on its own.

In a real pipeline you point vowl at your OpenTelemetry Collector:

```python
run_id = result.export_otel(
    endpoint="http://localhost:4317",  # or leave it out and set OTEL_EXPORTER_OTLP_ENDPOINT
    service_name="hdb-resale-dq",
    custom_attributes={"deployment.environment": "prod"},
)
```

vowl sends all three signals (metrics, traces, and logs) by default. It waits until
the data is sent before returning, and warns you if the collector could not be
reached.

That needs a running collector, so we skip it here. Instead we give `export_otel`
some **in-memory providers**, so everything runs offline. The call still returns a
run ID, and at the end of this section we write what it sent to files you can open.

> **Demo only.** In-memory providers keep the data in memory and lose it when the
> kernel stops. In a real pipeline you send to your own collector (the `endpoint`
> call above), and it handles storage, dashboards, and alerts.
"""
)

md(
    """
`export_otel` also accepts your own `metric_provider`, `tracer_provider`, and
`logger_provider`. When you pass your own, vowl writes into them but leaves them
open, so we can read the data back afterwards. Here we use the in-memory ones that
come with the OpenTelemetry SDK.
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
    \"\"\"A meter, tracer, and logger that keep everything in memory for us to read.

    Pass ``resource`` to stamp every signal with a specific OTel resource. Leave
    it unset to get the SDK default, which is fine when we only look at the signals.
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
    service_name="hdb-resale-dq",
    custom_attributes={"deployment.environment": "demo", "team": "housing-data"},
    metric_provider=meter,
    tracer_provider=tracer,
    logger_provider=logger,
)
print(f"exported run ID: {run_id}")
"""
)

md(
    """
### How the signals flow through your stack

The three signals do different jobs. `export_otel` sends them to an OpenTelemetry
Collector (or straight to your monitoring tool), which sends each one where it
belongs:

- **Metrics** are numbers over time, such as "how many rows failed". They feed
  dashboards and alerts.
- **Traces** are a timeline of one run, with one bar per check. They show what ran,
  how long it took, and what failed.
- **Logs** are one message per failed or broken check. They carry the SQL and the
  links you need, so an alert can reach the right person with the details attached.

```mermaid
flowchart LR
    vowl["vowl validation run"] -->|"export_otel(...)"| collector["OpenTelemetry Collector<br/>or your monitoring tool"]
    collector --> metrics["Metrics:<br/>dashboards and alerts"]
    collector --> traces["Traces:<br/>a timeline of each run"]
    collector --> logs["Logs:<br/>one message per failed check"]
```

Metrics tell you how quality is trending. Logs and traces tell you what broke and
where. The table below shows what each one carries.
"""
)

md(
    """
### What each signal carries

Same run, three signals, each with its own job. Metrics stay small so dashboards
stay fast. Traces and logs carry the detail (the SQL, the full check definition, and
optionally a few failed rows) for working out what broke.

| Signal | What you get | Carries | Use it to |
| ------ | ------------ | ------- | --------- |
| **Metrics** | the [DQ metrics](../../docs/dq-metrics/index.md): check and row counts and pass rates at check, dimension, schema, and run level, plus durations (Histograms) | a few short labels only: `status`, `schema_name`, `dimension`, `severity`, `engine`, `check_name` | watch quality trends and alert on rates |
| **Traces** | one span for the run, and one child span per check | those same labels, plus the SQL that ran and the full check definition (`check.definition.*`) | see exactly what one check did |
| **Logs** | one record per failed or broken check | a level (`WARN` when the data failed a check, `ERROR` when the check itself could not run), the SQL, the `trace_id` and `span_id`, and a short message | send alerts and jump straight to the trace |

You can see all three in the `metrics.json`, `traces.json`, and `logs.json` files
written at the end of this section.
"""
)

md(
    """
### Run details on every signal

An **attribute** is a `key=value` label on a piece of OTel data. vowl adds the same
set of attributes to every metric, span, and log record, so each one says which run
and contract it came from. You never build them by hand. They are:

- `service.name` and `vowl.version`
- `vowl.contract.*`, read from your contract (id, version, status, and a few more)
- anything you passed in `custom_attributes`, copied as-is
- `vowl.run.id`, the same ID `export_otel(...)` returned above. It is
  `result.run_id`, made when the run ran, and `dq_metrics.json` carries it too.
  This one goes on spans and logs only. A new value every run would make every metric series new,
  which many metric backends charge for.

vowl adds these attributes however the providers were set up (your own, the global
ones, or ones vowl builds). When vowl builds the providers itself, it also puts them
on the OTel resource, for tools that show resource details separately.
"""
)

md(
    """
### Linking an alert back to the rows

By default vowl sends **how many** rows failed, not the rows themselves. There are
three ways to get from an alert to the actual data:

- **`row.count.failed`** is on every check that reports failing rows. No cell
  values leave your process.
- **A small sample** with `max_failed_rows_sample`. This is off by default. Set a
  positive number and each failed check adds up to that many rows to its log and
  span. These are real cell values, so think about personal data before you turn
  it on.
- **A link** to wherever you saved the rows yourself.

The link goes in `custom_attributes`, which vowl copies onto every signal as-is. The
suggested key is `vowl.artifact.uri`. Every run already has an ID in
`result.run_id`, which the saved files and the telemetry share, so save the rows
under it:

```python
output_dir = f"s3://my-bucket/dq-results/{result.run_id}/"

result.save(output_dir)  # your failed rows, annotated tables, and dq_metrics.json go here
result.export_otel(
    custom_attributes={"vowl.artifact.uri": output_dir},  # alerts link straight to the files
)
```

Use `vowl.link.<name>` (for example `vowl.link.runbook` or `vowl.link.ticket`) for
any other link you want on the run. These are naming ideas, not special keys. They
help a team spell the same link the same way. See the
[Exporting to OpenTelemetry guide](../../docs/dq-metrics/otel-export.md) for the full list.

### The signals as files you can eyeball

This last cell writes one file per signal into the `outputs/` folder next to this
notebook: `metrics.json`, `traces.json`, and `logs.json`, in the JSON form the
OpenTelemetry SDK produces. Open them to see what a collector receives.

Here we set `max_failed_rows_sample` to `2` (it is `0` by default), so each failed
check's log and span carries a couple of the failed rows. Timestamps and the run,
trace, and span IDs change on every run.
"""
)

code(
    """
ometer, oreader, otracer, ospans, ologger, ologs = in_memory_providers()
result.export_otel(
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
- `result.save(output_mode="annotated")` writes those same shapes to disk as CSV,
  plus a `*_summary.json` that always carries every full check definition and a
  `*_dq_metrics.json` with the run's DQ metrics.
- `result.export_otel(...)` sends the run to OpenTelemetry: small metrics for
  dashboards, plus traces and logs that carry the SQL and full check definition
  for working out what broke.
- The `outputs/` folder holds a saved example of each output for reference.

Every parameter, metric, and attribute is listed in the
[Exporting to OpenTelemetry guide](../../docs/dq-metrics/otel-export.md).
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
