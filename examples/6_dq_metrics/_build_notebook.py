"""Generator for dq_metrics.ipynb.

Kept in-repo so the notebook can be regenerated deterministically rather than
hand-edited as raw JSON. Run: python examples/6_dq_metrics/_build_notebook.py
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
# vowl · DQ Metrics

Every vowl run is summed up as **DQ metrics**: check counts, row counts, and pass
rates at four levels (check, dimension, schema, and run). This notebook reads one
run's metrics in each form vowl gives them to you, using the HDB Resale dataset
that ships with the repo (a copy seeded with deliberate errors so some checks
fail).

What this notebook covers:

1. Setup and a validation run to work with
2. **Understanding the metrics**: the four levels and how to read them
3. **`dq_metrics.json`**: the metrics as a file, loaded back with pandas
4. **OpenTelemetry**: the same run as metrics, traces, and logs for your monitoring tools
5. **A real backend**: where to go next to see it all in Grafana

Sections 2 and 3 need only a base `vowl` install. Section 4 needs the `[otel]`
extra but runs fully offline. The file and the OpenTelemetry metrics come from one
calculation, with the same names, attributes, and values, so whatever you learn in
section 2 reads the same in both.

The full reference is in the docs:
[Understanding DQ Metrics](../../docs/dq-metrics/understanding-metrics.md),
[Exporting to dq_metrics.json](../../docs/dq-metrics/json-export.md), and
[Exporting to OpenTelemetry](../../docs/dq-metrics/otel-export.md).
"""
)

md(
    """
<a id="1-setup"></a>
## 1. Setup and a validation run

Resolve the dataset paths, load the HDB CSV, and run `validate_data`. Some checks
fail, so the metrics below have something to count.
"""
)

code(
    """
# Install vowl with the OpenTelemetry extra used in section 4 (skipped during execution)
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

HDB_DIR = REPO_ROOT / "tests" / "hdb_resale"
HDB_CSV = HDB_DIR / "HDBResaleWithErrors.csv"
HDB_CONTRACT = HDB_DIR / "hdb_resale_simple.yaml"  # Switch to "hdb_resale.yaml" for a more complex contract

OUTPUTS = REPO_ROOT / "examples" / "6_dq_metrics" / "outputs"
OUTPUTS.mkdir(exist_ok=True)

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

# --- Section 2: Understanding the metrics ---

md(
    """
<a id="2-understanding"></a>
## 2. Understanding the Metrics

`result.get_dq_metrics()` returns the run's metrics as a Python `dict`. It is the
same document that `save()` writes to `dq_metrics.json` (section 3), and the same
numbers that `export_otel()` sends (section 4).

Every metric name follows `vowl.<level>.<unit>.<measure>`:

| Part | Values | Meaning |
|---|---|---|
| level | `check`, `dimension`, `schema`, `run` | what the reading is about: one check, one quality dimension of a table, one table, or the whole run |
| unit | `check`, `row`, `schema` | what is counted |
| measure | `count`, `pass_rate`, `duration` | a count (split by a `status` attribute), a rate from 0 to 1, or a timing in ms |

So `vowl.schema.row.pass_rate` is "the share of a table's rows that passed every
check", and `vowl.run.check.count` with `status="FAILED"` is "how many checks
failed in this run".
"""
)

code(
    """
metrics = result.get_dq_metrics()

print("Keys:  ", list(metrics.keys()))
print("Run:   ", metrics["run"]["vowl.contract.name"], "/", metrics["run"]["vowl.run.id"])
print("Points:", len(metrics["points"]))
print("\\nOne point:")
metrics["points"][0]
"""
)

md(
    """
Each point is one reading: a `name`, a `type` (`counter`, `gauge`, or
`histogram`), a `unit`, a `value`, and the `attributes` that say what it is for.
`pd.json_normalize` turns the list into a table, one row per point and one
`attributes.<key>` column per attribute. The rest of this section reads that table
from the top level down.
"""
)

code(
    """
points = pd.json_normalize(metrics["points"])
points["name"].value_counts().rename("points").to_frame()
"""
)

md(
    """
### Run level: the one-line answer

The `vowl.run.*` metrics answer "how did this run go?" Counts carry a `status` of
`PASSED`, `FAILED`, or `ERROR`, zeros included, so a dashboard always has a value
to show. Pass rates and durations have no `status`.
"""
)

code(
    """
points[points["name"].str.startswith("vowl.run.")][["name", "attributes.status", "value"]]
"""
)

md(
    """
### Schema and dimension level: rows, counted once

Row counts at schema level count **attributed rows**, every copy counted. A row that fails two checks is
one failed row, not two, so the schema's failed rows can be fewer than the
check-level failures added up. In this run no row fails more than one check, so the
two numbers below match. The
[worked example](../../docs/dq-metrics/understanding-metrics.md#worked-example) in the
docs shows a run where they differ. The dimension level does the same within each
quality dimension (completeness, conformity, and so on), so it tells you *what
kind* of problem the failing rows have.

The metrics do not say whether a row count is approximate. The `vowl.validate` and
`vowl.check` spans do, through `row.approximate`.
"""
)

code(
    """
check_failed = points[
    (points["name"] == "vowl.check.row.count") & (points["attributes.status"] == "FAILED")
]["value"].sum()
schema_rows = points[points["name"] == "vowl.schema.row.count"]
schema_failed = schema_rows[schema_rows["attributes.status"] == "FAILED"]["value"].sum()

print(f"Failed rows, added up over checks: {check_failed:,.0f}")
print(f"Failed rows, counted once (schema): {schema_failed:,.0f}\\n")
display(schema_rows[["attributes.schema_name", "attributes.status", "value"]])

points[points["name"] == "vowl.dimension.row.pass_rate"][
    ["attributes.schema_name", "attributes.dimension", "value"]
]
"""
)

md(
    """
### Check level: which checks to look at

The check level has one reading per check. Pivot `vowl.check.row.count` to see each
check's passed and failed attributed rows side by side, worst first. Checks that do
not count rows, such as aggregate checks, have a `vowl.check.check.count` but no row
count. `vowl.check.row.scalar_count` holds the number each check itself reported,
its scalar count.
"""
)

code(
    """
(
    points[points["name"] == "vowl.check.row.count"]
    .pivot_table(index="attributes.check_name", columns="attributes.status", values="value")
    .sort_values("FAILED", ascending=False)
    .head(10)
)
"""
)

md(
    """
> **Which numbers add up?** Counters (`*.check.count`, `*.schema.count`) add up
> across runs, so summing failed checks over a week is fine. Gauges (row counts and
> pass rates) are readings of one run. For a pass rate over many runs, add up the
> `PASSED` and `FAILED` row counts and divide, rather than averaging the rates. See
> [Which numbers add up](../../docs/dq-metrics/understanding-metrics.md#which-numbers-add-up).
"""
)

# --- Section 3: dq_metrics.json ---

md(
    """
<a id="3-json"></a>
## 3. `dq_metrics.json`: the Metrics as a File

`result.save(...)` writes `<prefix>_dq_metrics.json` next to the annotated tables.
The annotated tables already attribute failed rows, so the file costs nothing more.
Leave `"dq_metrics"` out of `save(outputs=[...])` to skip it.
Use it when you want the metrics in a data warehouse, a notebook, or
anywhere without an OpenTelemetry backend. It needs no extra install.
"""
)

code(
    """
import json

result.save(output_dir=str(OUTPUTS), prefix="vowl_demo")

with open(OUTPUTS / "vowl_demo_dq_metrics.json") as f:
    document = json.load(f)

print("\\nSame content as get_dq_metrics():", document == json.loads(json.dumps(metrics)))
"""
)

md(
    """
The file holds the run identity once, under `run`, and the readings under `points`.
The run identity is not repeated on every point, so the file stays small.

| Key | What it holds |
|---|---|
| `schema_version` | The version of this layout. It changes only when the layout changes in a way that could break a reader. |
| `run` | The vowl version, `vowl.run.id`, and the contract fields (id, name, version, status, domain). |
| `run_started_at`, `run_finished_at` | When the run started and when its last check finished, in UTC. |
| `points` | One entry per metric reading, read exactly as in section 2. |
"""
)

code(
    """
{k: v for k, v in document.items() if k != "points"}
"""
)

md(
    """
### Loading many runs

Each run writes its own file. Add the run ID and start time to every point, and the
files of many runs load into one table you can chart or query. Here the "many runs"
are the same file read twice, standing in for a folder of saved runs.
"""
)

code(
    """
def load_run(path):
    \"\"\"One row per point, tagged with the run it came from.\"\"\"
    with open(path) as f:
        doc = json.load(f)
    frame = pd.json_normalize(doc["points"])
    frame["run_id"] = doc["run"]["vowl.run.id"]
    frame["run_started_at"] = doc["run_started_at"]
    return frame


run_files = [OUTPUTS / "vowl_demo_dq_metrics.json"] * 2  # in practice: sorted(folder.glob("*_dq_metrics.json"))
history = pd.concat([load_run(p) for p in run_files])

# A pass rate over many runs: add up the row counts, then divide
rows = history[history["name"] == "vowl.run.row.count"].groupby("attributes.status")["value"].sum()
print(f"Row pass rate over {len(run_files)} runs: {rows['PASSED'] / (rows['PASSED'] + rows['FAILED']):.4%}")
"""
)

md(
    """
> **`dq_metrics.json` or `summary.json`?** `summary.json` is the record of the run:
> its settings, every check, and what each check found. `dq_metrics.json` is the
> numbers computed from it. Some `summary.json` fields look like metrics but follow
> older rules (`success_rate` is 0 to 100, and `failed_rows` counts a row once per
> failing check). Prefer `dq_metrics.json` for numbers. See
> [dq_metrics.json and summary.json](../../docs/dq-metrics/json-export.md#dq_metricsjson-and-summaryjson).
"""
)

# --- Section 4: OpenTelemetry ---

md(
    """
<a id="4-otel"></a>
## 4. OpenTelemetry: Metrics, Traces, and Logs

OpenTelemetry sends the finished run to your monitoring tools, so data quality
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
| **Metrics** | the [DQ metrics](#2-understanding): check and row counts and pass rates at check, dimension, schema, and run level, plus durations (Histograms) | a few short labels only: `status`, `schema_name`, `dimension`, `severity`, `engine`, `check_name` | watch quality trends and alert on rates |
| **Traces** | one span for the run, and one child span per check | those same labels, plus the SQL that ran and the full check definition (`check.definition.*`) | see exactly what one check did |
| **Logs** | one record per failed or broken check | a level (`WARN` when the data failed a check, `ERROR` when the check itself could not run), the SQL, the `trace_id` and `span_id`, and a short message | send alerts and jump straight to the trace |

You can see all three in the `metrics.json`, `traces.json`, and `logs.json` files
written at the end of this section.
"""
)

md(
    """
### The same numbers as `dq_metrics.json`

The metrics `export_otel` sent are the points from section 2, under the same names
with the same values. Read them back from the in-memory reader and compare one of
them with the JSON.
"""
)

code(
    """
otel_metrics = {
    m.name: m
    for rm in metric_reader.get_metrics_data().resource_metrics
    for sm in rm.scope_metrics
    for m in sm.metrics
}
json_names = set(points["name"])

print("Same metric names:", set(otel_metrics) == json_names)

otel_rate = next(iter(otel_metrics["vowl.run.row.pass_rate"].data.data_points)).value
json_rate = points[points["name"] == "vowl.run.row.pass_rate"]["value"].item()
print(f"vowl.run.row.pass_rate  OTel: {otel_rate:.6f}  JSON: {json_rate:.6f}")
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

result.save(output_dir)  # check results, summary.json, failed rows, annotated tables and dq_metrics.json
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

(OUTPUTS / "metrics.json").write_text(
    json.dumps(json.loads(oreader.get_metrics_data().to_json()), indent=2) + "\\n"
)
(OUTPUTS / "traces.json").write_text(
    json.dumps([json.loads(s.to_json()) for s in ospans.get_finished_spans()], indent=2) + "\\n"
)
(OUTPUTS / "logs.json").write_text(
    json.dumps([json.loads(e.to_json()) for e in ologs.get_finished_logs()], indent=2) + "\\n"
)

for name in ["metrics.json", "traces.json", "logs.json"]:
    print(f"{name:14} {(OUTPUTS / name).stat().st_size:>7,} bytes")
"""
)

# --- Section 5: A real backend ---

md(
    """
<a id="5-backend"></a>
## 5. Seeing It in a Real Backend

The in-memory providers above show what vowl sends. To see it land in real
monitoring tools, the [`otel_stack/`](otel_stack/README.md) folder next to this
notebook runs an OpenTelemetry Collector, Prometheus, Tempo, Loki, and Grafana on
your machine with Docker, sends it runs for a small catalog of four contracts, and
shows them on a DQ dashboard:

```bash
cd examples/6_dq_metrics/otel_stack
make otel-up              # start the stack
make otel-add-vowl-runs   # send sample validation runs
# then open http://localhost:3000/d/vowl-dq
```

![vowl DQ metrics dashboard](otel_stack/images/dashboard-overview.png)
"""
)

md(
    """
---

### Recap

- Every run is summed up as DQ metrics named `vowl.<level>.<unit>.<measure>`, at
  check, dimension, schema, and run level. Row counts above check level count
  attributed rows, every copy counted.
- `result.get_dq_metrics()` returns them as a `dict`, and `result.save(...)` writes
  the same document to `<prefix>_dq_metrics.json`. `pd.json_normalize` turns it into
  a table, and tagging each file with its run ID loads many runs together.
- `result.export_otel(...)` sends the same metrics to OpenTelemetry, plus traces and
  logs that carry the SQL and full check definition for working out what broke.
- [`otel_stack/`](otel_stack/README.md) shows it all end to end in Grafana.
- The `outputs/` folder holds a saved example of `dq_metrics.json` and of each
  OpenTelemetry signal for reference.

Every metric, attribute, and parameter is listed in
[Understanding DQ Metrics](../../docs/dq-metrics/understanding-metrics.md),
[Exporting to dq_metrics.json](../../docs/dq-metrics/json-export.md), and
[Exporting to OpenTelemetry](../../docs/dq-metrics/otel-export.md).
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

out = Path(__file__).with_name("dq_metrics.ipynb")
out.write_text(json.dumps(notebook, indent=1) + "\n")
print(f"wrote {out} ({len(cells)} cells)")
