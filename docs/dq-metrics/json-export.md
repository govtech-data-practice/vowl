---
description: How vowl writes a run's DQ metrics to dq_metrics.json, and how to load them.
---

# Exporting to dq_metrics.json

`result.save(...)` writes the run's [DQ metrics](understanding-metrics.md) to
`<prefix>_dq_metrics.json`, next to the check results and `summary.json`. The
file holds the same readings that
[Exporting to OpenTelemetry](otel-export.md) sends, with the same names,
attributes and values. Use it when you want the metrics in a data warehouse,
a notebook, or anywhere without an OpenTelemetry backend. It needs no extra
install.

```python
from vowl import validate_data

result = validate_data("contract.yaml", df=df)
result.save("dq-results/", prefix="orders", output_mode="annotated")
# writes dq-results/orders_dq_metrics.json, among the other files
```

To get the same content without writing a file, call
`result.get_dq_metrics()`. It returns the document as a Python `dict`.

## Worked example

This is the run from the [Understanding DQ Metrics worked example](understanding-metrics.md#worked-example).
The contract has two schemas:

- `orders` has 100 rows. `email_required_check` (completeness) fails on 5 rows
  and `order_id_unique_check` (consistency) fails on 3 rows. One row has both
  a missing email and a duplicate `order_id`. `order_id_required_check`
  (completeness) passes.
- `refunds` has 20 rows. `refund_positive` (consistency) fails on 2 rows.

vowl also adds a `<column>_column_exists_check` (conformity) for each column
in the contract, and they all pass. So the run has 7 checks: 4 passed and 3
failed. Everything below is the real output of this run.

### The file

The file starts with the run identity and times, then holds 109 points. Here
are three of them, one of each type:

```json
{
  "schema_version": 1,
  "run": {
    "vowl.version": "0.0.7",
    "vowl.run.id": "0f2c9e1a-6b1d-4c1e-9d59-2f1f3f3c8a10",
    "vowl.contract.id": "customer_orders",
    "vowl.contract.version": "3.0.0",
    "vowl.contract.api_version": "v3.1.0",
    "vowl.contract.status": "active",
    "vowl.domain": "sales"
  },
  "run_started_at": "2026-09-30T02:00:00.307761+00:00",
  "run_finished_at": "2026-09-30T02:00:00.707886+00:00",
  "points": [
    {
      "name": "vowl.check.check.count",
      "type": "counter",
      "unit": "{check}",
      "value": 1,
      "attributes": {
        "check_name": "email_required_check",
        "schema_name": "orders",
        "dimension": "completeness",
        "engine": "sql",
        "status": "FAILED"
      }
    },
    {
      "name": "vowl.schema.row.count",
      "type": "gauge",
      "unit": "{row}",
      "value": 7,
      "attributes": {
        "schema_name": "orders",
        "status": "FAILED"
      }
    },
    {
      "name": "vowl.run.duration",
      "type": "histogram",
      "unit": "ms",
      "value": 400.125,
      "attributes": {}
    }
  ]
}
```

### Every metric in the file

Every metric on the [All metrics](understanding-metrics.md#all-metrics) list is in the file.
This table shows how many points each one has in this run, and one reading
from each. Counts have one point per status, zeros included, so
`vowl.check.check.count` has 7 checks times 3 statuses, which is 21 points.

| Metric                           | Type      | Unit       | Points | One reading from this run                         |
| -------------------------------- | --------- | ---------- | -----: | ------------------------------------------------- |
| `vowl.check.check.count`         | counter   | `{check}`  |     21 | `1` for `email_required_check`, `status="FAILED"` |
| `vowl.check.row.count`           | gauge     | `{row}`    |     14 | `5` for `email_required_check`, `status="FAILED"` |
| `vowl.check.row.pass_rate`       | gauge     | `1`        |      7 | `0.95` for `email_required_check`                 |
| `vowl.check.duration`            | histogram | `ms`       |      7 | `11.6` for `email_required_check`                 |
| `vowl.dimension.check.count`     | counter   | `{check}`  |     15 | `1` for `orders` completeness, `status="FAILED"`  |
| `vowl.dimension.check.pass_rate` | gauge     | `1`        |      5 | `0.5` for `orders` completeness                   |
| `vowl.dimension.row.count`       | gauge     | `{row}`    |     10 | `5` for `orders` completeness, `status="FAILED"`  |
| `vowl.dimension.row.pass_rate`   | gauge     | `1`        |      5 | `0.95` for `orders` completeness                  |
| `vowl.schema.check.count`        | counter   | `{check}`  |      6 | `2` for `orders`, `status="FAILED"`               |
| `vowl.schema.check.pass_rate`    | gauge     | `1`        |      2 | `0.6` for `orders`                                |
| `vowl.schema.row.count`          | gauge     | `{row}`    |      4 | `7` for `orders`, `status="FAILED"`               |
| `vowl.schema.row.pass_rate`      | gauge     | `1`        |      2 | `0.93` for `orders`                               |
| `vowl.run.schema.count`          | counter   | `{schema}` |      3 | `2` with `status="FAILED"`                        |
| `vowl.run.check.count`           | counter   | `{check}`  |      3 | `3` with `status="FAILED"`                        |
| `vowl.run.check.pass_rate`       | gauge     | `1`        |      1 | `0.571`, which is 4 of 7 checks                   |
| `vowl.run.row.count`             | gauge     | `{row}`    |      2 | `9` with `status="FAILED"`                        |
| `vowl.run.row.pass_rate`         | gauge     | `1`        |      1 | `0.925`, which is 111 of 120 rows                 |
| `vowl.run.duration`              | histogram | `ms`       |      1 | `400.1`                                           |

A few things to notice:

- Row counts carry a `status` of `PASSED` or `FAILED` only. Check and schema
  counts also have `ERROR`.
- 7 rows of `orders` failed at schema level, not 5 + 3 = 8, because one row
  failed both checks. See [How rows are counted](understanding-metrics.md#how-failed-rows-are-counted).
- The run level adds up the schemas: 7 + 2 = 9 rows failed.
- The points do not say whether a row count is approximate. Use
  `get_row_quality_df()` or the [traces](otel-export.md#traces) for that.

### Reading it with pandas

Each point becomes one row, and each attribute becomes an `attributes.<key>`
column:

```python
import json

import pandas as pd

with open("dq-results/orders_dq_metrics.json") as f:
    document = json.load(f)

points = pd.json_normalize(document["points"])
```

Pick out one metric to get a small table. The row counts of each schema:

```python
points[points["name"] == "vowl.schema.row.count"][
    ["attributes.schema_name", "attributes.status", "value"]
]
```

```
attributes.schema_name attributes.status  value
                orders            PASSED   93.0
                orders            FAILED    7.0
               refunds            PASSED   18.0
               refunds            FAILED    2.0
```

Pivot the check level to see each check's rows side by side:

```python
points[points["name"] == "vowl.check.row.count"].pivot_table(
    index=["attributes.schema_name", "attributes.check_name"],
    columns="attributes.status",
    values="value",
)
```

```
attributes.status                                    FAILED  PASSED
attributes.schema_name attributes.check_name
orders                 email_column_exists_check        0.0   100.0
                       email_required_check             5.0    95.0
                       order_id_column_exists_check     0.0   100.0
                       order_id_required_check          0.0   100.0
                       order_id_unique_check            3.0    97.0
refunds                amount_column_exists_check       0.0    20.0
                       refund_positive                  2.0    18.0
```

Or take everything at run level for a one-line summary of the run:

```python
points[points["name"].str.startswith("vowl.run.")][
    ["name", "attributes.status", "value"]
]
```

```
                    name attributes.status      value
   vowl.run.schema.count            PASSED   0.000000
   vowl.run.schema.count            FAILED   2.000000
   vowl.run.schema.count             ERROR   0.000000
    vowl.run.check.count            PASSED   4.000000
    vowl.run.check.count            FAILED   3.000000
    vowl.run.check.count             ERROR   0.000000
vowl.run.check.pass_rate               NaN   0.571429
      vowl.run.row.count            PASSED 111.000000
      vowl.run.row.count            FAILED   9.000000
  vowl.run.row.pass_rate               NaN   0.925000
       vowl.run.duration               NaN 400.125000
```

## What is in the file

| Key               | What it holds                                                                                                                                                                                                                            |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `schema_version`  | The version of this layout. It changes only when the layout changes in a way that could break a reader.                                                                                                                                  |
| `run`             | The run identity: the vowl version, `vowl.run.id` and the contract fields. The keys are the same as the [run identity attributes](otel-export.md#all-run-identity-attributes), apart from `service.name`, which only OpenTelemetry uses. |
| `run_started_at`  | When the run started, in UTC. `null` when vowl did not record it.                                                                                                                                                                        |
| `run_finished_at` | When the last check finished, in UTC. `null` when vowl did not record it.                                                                                                                                                                |
| `points`          | One entry per metric reading, at check, dimension, schema and run level.                                                                                                                                                                 |

Each entry in `points` has:

| Key          | What it holds                                                                                                                                                                                  |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`       | The metric name, for example `vowl.schema.row.pass_rate`. [All metrics](understanding-metrics.md#all-metrics) lists them.                                                                      |
| `type`       | `counter`, `gauge` or `histogram`. It says whether the value adds up. See [Which numbers add up](understanding-metrics.md#which-numbers-add-up).                                               |
| `unit`       | `{check}`, `{row}` or `{schema}` for counts, `1` for pass rates, `ms` for durations.                                                                                                           |
| `value`      | The reading for this run. For a counter, it is this run's count. For a histogram, it is one timing.                                                                                            |
| `attributes` | What the reading is for, such as `schema_name` and `status`. The run identity is in `run` instead, not repeated. [Attributes](understanding-metrics.md#attributes) lists them for each metric. |

The metric names always start with `vowl`. The `prefix` of `save` only names
the files.

Two readings with the same name and attributes, such as two checks with the
same name in one schema, are merged the way OpenTelemetry merges them. Counters
add up and the last gauge reading wins. Every timing is kept. Give checks
unique names to see each one on its own.

## Loading many runs

Add the run ID and start time to every row, and you can load the files of
many runs into one table:

```python
points = pd.json_normalize(document["points"])
points["run_id"] = document["run"]["vowl.run.id"]
points["run_started_at"] = document["run_started_at"]
```

The rules for combining runs are the same as on a dashboard:

- **Counters** add up across runs. Summing `vowl.run.check.count` with
  `status = "FAILED"` over a week gives the failed checks of that week.
- **Gauges** are readings of one run. To combine row counts, see
  [Adding row counts across runs](understanding-metrics.md#adding-row-counts-across-runs). For
  a pass rate over many runs, add up the `PASSED` and `FAILED` row counts and
  divide, rather than averaging the rates.

## dq_metrics.json and summary.json

`summary.json` is unchanged. It is the record of the run: its settings, every
check and what each check found. `dq_metrics.json` is the numbers computed
from it, in the vocabulary on [Understanding DQ Metrics](understanding-metrics.md).

Some fields in `summary.json` look like DQ metrics but follow older rules.
Prefer `dq_metrics.json` for these:

| In `summary.json`                            | Use instead                | Why                                                                                                 |
| -------------------------------------------- | -------------------------- | --------------------------------------------------------------------------------------------------- |
| `success_rate`                               | `vowl.run.check.pass_rate` | `success_rate` is a percentage from 0 to 100. Pass rates are from 0 to 1.                           |
| `failed_rows`                                | `vowl.run.row.count`       | `failed_rows` adds up the scalar counts of each check, so a row that fails two checks counts twice. |
| `total_checks`, `passed`, `failed`, `errors` | `vowl.run.check.count`     | The same numbers, as one metric with a `status` attribute.                                          |
