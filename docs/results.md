---
description: How to use the ValidationResult that validate_data returns. Check whether the run passed, print reports, find failed rows, read row counts, export DQ metrics and save the results.
---

# The Results Object

`validate_data` returns a `ValidationResult`. This page calls it **the
result**. It holds the status of every check, the failed rows of each
check, and the row counts for each schema.

!!! tip "Interactive Demo"

    The [Results Tour notebook](https://github.com/govtech-data-practice/vowl/blob/main/examples/5_results/results_tour.ipynb) walks through annotated output, residues and saved files on a real dataset.

```python
from vowl import validate_data

result = validate_data("contract.yaml", df=df)

result.passed                                   # True when no check failed
result.print_summary()                          # a report in the console
output = result.get_annotated_output()          # your tables, with the failed rows annotated
result.save("dq-results/")
```

The checks run once, inside `validate_data`. The result fetches failed rows,
counts rows and downloads tables only when a method first needs them, and then
keeps them. Calling a method twice does not query your data twice. Keep the
connection to your data open until you are done with the result.

The words on this page, such as _failed rows_, _row counts_ and _residue_,
mean the same as in the [Glossary](glossary.md).

## Two tiers of results

The result has two tiers. The basic tier is cheap. The DQ metrics tier does
more work. `save()` is in the DQ metrics tier by default. Pass
`outputs=["consolidated_query_outputs"]` or `outputs=["failed_query_outputs"]`
to keep it in the basic tier.

| Tier       | Methods                                                                                                                       | Cost                                                                                                                                                                                    | Counts                                                                                      |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Basic      | `print_summary()`, `get_check_results_df()`, `result.summary`, `get_output_dfs()`, `save()` without `annotated_table` or `dq_metrics` | Runs the check queries only. Never attributes rows to the table.                                                                                                                        | Each check's scalar `failed_rows_count`. A sum across checks is marked approximate.         |
| DQ metrics | `get_dq_metrics()`, `get_dq_metrics_df(by=...)`, `export_otel(...)`, `get_annotated_output()`, `save()` with `annotated_table` or `dq_metrics` | [Attributes failed rows to the table](design-considerations/checks/how-attributed-rows-work.md) once and reuses that work. Runs a `COUNT(*)` on each table the first time it is needed. | Rows that failed at least one check, rows that passed, pass rates and an `approximate` flag |

A sum of scalar counts across checks is approximate. A row that fails two
checks counts twice, and `DISTINCT` or a join can change a check's count. The
DQ metrics count each failed row once. Only the DQ metrics tier counts the rows
in each table.

## Getting results as DataFrames

The methods below return [Narwhals](https://narwhals-dev.github.io/narwhals/)
DataFrames. Call `.to_pandas()` or `.to_polars()` to work with them in the
library you use.

### Check results

`get_check_results_df()` returns one row per check. The main columns are:

| Column                                       | What it holds                                                                                                                                  |
| -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `check_name`                                 | The check's name                                                                                                                               |
| `schema_name`                                | The schema the check belongs to                                                                                                                |
| `dimension`                                  | The check's dimension, such as `completeness`                                                                                                  |
| `status`                                     | `PASSED`, `FAILED` or `ERROR`                                                                                                                  |
| `operator`, `expected_value`, `actual_value` | What the check compared, for example `mustBe`, `0` and `3`                                                                                     |
| `failed_rows_count`                          | The check's scalar count when it failed, `0` when it passed. `actual_value` always holds the scalar. For `COUNT(DISTINCT x)` it counts values. |
| `message`                                    | Why the check ended in `ERROR`. Empty otherwise.                                                                                               |
| `execution_time_ms`                          | How long the check took                                                                                                                        |

Pass `include_check_definition=True` to add each check's definition as JSON,
and `include_contract_definition=True` to add the part of the contract it came
from.

### Failed rows

There are two ways to get failed rows. Choose by what you want to do with them.

| You want to                                              | Use                      |
| -------------------------------------------------------- | ------------------------ |
| See every row of a table, with the failed rows annotated | `get_annotated_output()` |
| Look at one check's failed rows on their own             | `get_output_dfs()`       |

**The annotated output** has one annotated table per schema: your full table
with a `check_info` column. On a failed row, `check_info` lists the checks the
row failed. On every other row it is empty. A check whose failed rows cannot be
annotated on the table, for example because they have different columns, becomes
a residue instead.

```python
output = result.get_annotated_output()
output["annotated"]["orders"]                   # the orders table, with check_info
output["residues"]                              # {"<schema>::<check_name>": failed rows}
```

Pass `check_info="summary"` or `"full"` for more detail in `check_info`.
[What the annotated output holds](design-considerations/checks/annotating-the-source-table.md#what-the-annotated-output-holds)
describes each option, and
[Where each failed check ends up](design-considerations/checks/annotating-the-source-table.md#where-each-failed-check-ends-up)
explains which checks are annotated and which become residues.

**`get_output_dfs()`** returns each check's failed rows, keyed
`"<schema>::<check_name>"`. Each DataFrame has a `check_id` column (the check's
name) and a `tables_in_query` column. Checks that ended in `ERROR` are left
out, and so are checks that passed, unless
[`fetch_tolerated_rows=True`](run-settings.md#fetch_tolerated_rows)
puts the tolerated ones in with a `tolerated` column. A failed check whose
operator sets no upper limit, such as `mustBeGreaterThan`, is left out too,
because the rows it returns are the ones that passed. This method does not
download your tables, so it is the better choice on large tables.

```python
failed = result.get_output_dfs()
failed["orders::price_must_be_positive"]
```

`get_output_dfs(scope="all")` returns the rows of every row-level check that
did not error, whatever its status, with a `status` column. It runs the row
query of each check that passed, and it ignores `fetch_tolerated_rows`.

Both methods take `checks=["check_a", "check_b"]` to return only those checks.

### Row counts

`get_dq_metrics_df()` returns, for each schema, how many rows failed at least
one row-level check and how many passed them all. A row that fails two checks counts
once. These are the row counts in the
[DQ metrics](dq-metrics/understanding-metrics.md). The summary does not show
them. It shows **Failed Rows (approximate)**, a sum of the scalar counts of
the failed checks. A check that passed within its tolerance adds nothing.

```python
result.get_dq_metrics_df()                     # one row per schema
result.get_dq_metrics_df(by="dimension")       # one row per schema and dimension
result.get_dq_metrics_df(by="check")           # how each check was counted, and why
```

The columns for `by="schema"` and `by="dimension"` are:

| Column                                     | What it holds                                                                          |
| ------------------------------------------ | -------------------------------------------------------------------------------------- |
| `total_rows`                               | The rows in the table                                                                  |
| `failed_rows`                              | Rows that failed at least one row-level check                                          |
| `passed_rows`                              | Rows that failed no row-level check                                                    |
| `pass_rate`                                | `passed_rows` divided by `total_rows`, from 0 to 1                                     |
| `approximate`                              | `True` when a number could be off                                                      |
| `checks_row_level`, `checks_not_row_level` | How many checks are and are not row-level                                              |
| `checks_not_attributable`                  | How many row-level checks are not attributable, so their rows are not in these numbers |

The row counts hold attributed rows only. A row-level check that vowl could not
attribute adds nothing, and is flagged in `checks_not_attributable`. If such a
check failed and may have rows, `approximate` is `True`.

When every row-level check of a schema or dimension is not attributable, its
numbers are `None`.

`by="check"` has one row per check, with these columns:

| Column            | What it holds                                                                                                           |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `row_level`       | Whether the check is a row-level check                                                                                  |
| `route`           | How vowl counted the check. Empty when it is not row-level, not attributable or passed.                                 |
| `reason`          | Why the check is not row-level or not attributable. A passed check has `passed, not attributed`.                        |
| `scalar_count`    | The check's scalar count, the number that decides pass or fail                                                          |
| `attributed_rows` | The rows of the table its failed rows are attributed to, every copy counted. `None` when the check is not attributable. |
| `approximate`     | `True` when this check's rows are incomplete or approximate                                                             |

`scalar_count` and `attributed_rows` differ for a check that uses `DISTINCT`
or a join, or that counts distinct values. See
[How Attributed Rows Work](design-considerations/checks/how-attributed-rows-work.md#checks-can-change-the-rows-they-return).

vowl counts rows inside the data source where it can, so the row counts stay
exact on large tables and do not depend on `max_failed_rows`. Where the data
source can't attribute a check's failed rows, vowl downloads the table and
attributes them on your machine. `get_annotated_output()` reuses that
download. This work runs only when you ask for DQ metrics or annotated
output.
[How Attributed Rows Work](design-considerations/checks/how-attributed-rows-work.md)
explains which checks are row-level and how they are counted.

## Print a report

These methods print to the console. Each returns the result, so you can chain
them, for example `result.print_summary().show_failed_checks()`.

| Method                            | What it prints                                                                                    |
| --------------------------------- | ------------------------------------------------------------------------------------------------- |
| `print_summary()`                 | The summary: check counts and approximate failed rows for each schema, and a table of every check |
| `show_failed_checks()`            | Each failed check with its operator, expected value and actual value                              |
| `show_failed_rows(max_rows=5)`    | Up to `max_rows` failed rows for each failed check. `max_rows=-1` prints all.                     |
| `display_full_report(max_rows=5)` | `print_summary()` followed by `show_failed_rows()`                                                |

[Reading the summary](getting-started.md#reading-the-summary) explains each
line of the summary.

## Saving results

`save()` writes the result to a folder, prints each file it writes and returns
the result.

```python
result.save("dq-results/", prefix="orders")
```

File names start with `prefix`, or with `vowl_results` if you leave it out.
`outputs` picks the files to write. See [Outputs](#outputs). `save()` also
takes the `check_info`, `include_check_definition` and
`include_contract_definition` options of the methods above.

To write to cloud storage, pass a URI instead of a folder:

```python
result.save("s3://my-bucket/dq-results/run-1/")
```

| Storage          | Example                                                     |
| ---------------- | ----------------------------------------------------------- |
| Amazon S3        | `s3://my-bucket/dq-results/`                                |
| Google Cloud     | `gs://my-bucket/dq-results/`                                |
| Azure Data Lake  | `abfs://container@account.dfs.core.windows.net/dq-results/` |
| HDFS             | `hdfs://namenode:8020/dq-results/`                          |
| A local file URI | `file:///shared/dq-results/`                                |

vowl writes through the filesystems built into pyarrow, so there is nothing
extra to install. Credentials come from the usual place for each cloud. For S3,
that is environment variables such as `AWS_ACCESS_KEY_ID`, the `~/.aws` files
or an IAM role. For Google Cloud, it is your default application credentials.

For an S3-compatible store such as MinIO, set `AWS_ENDPOINT_URL` to its
address. [Loading contracts from S3](usage-patterns.md#from-s3) reads the same
variable.

To pass credentials yourself, pass a pyarrow filesystem as `filesystem=`. The
path is then inside that filesystem and starts with the bucket name:

```python
import pyarrow.fs as pafs

minio = pafs.S3FileSystem(
    endpoint_override="http://minio.internal:9000",
    access_key="...",
    secret_key="...",
)
result.save("my-bucket/dq-results/run-1/", filesystem=minio)
```

!!! note

    Some pyarrow builds, mostly from conda, leave out S3 or Google Cloud
    support. If `save()` says the filesystem is not supported, install pyarrow
    from PyPI with `pip install --force-reinstall pyarrow`.

### Outputs

`outputs` is a list of the files `save()` writes. `<prefix>_check_results.csv`
and `<prefix>_summary.json` are always written.

| Output                         | Files                                                                                  | Default |
| ------------------------------ | -------------------------------------------------------------------------------------- | ------- |
| (always)                       | `orders_check_results.csv` with the [check results](#check-results), `orders_summary.json` with the numbers behind the summary | Yes |
| `"failed_query_outputs"`       | `orders_checks/<schema>__<check>.csv`, the rows of one failed check                    | Yes     |
| `"all_query_outputs"`          | `orders_checks/<schema>__<check>.csv`, the rows of every row-level check, with `status` | No      |
| `"consolidated_query_outputs"` | `orders_<tables>.csv`, a [failed-rows CSV](#failed-rows-csvs)                          | Yes     |
| `"annotated_table"`            | `orders_<schema>_annotated.csv`, the annotated table of one schema, and `orders_<schema>__<check>_residue.csv`, the residue of one check | Yes |
| `"dq_metrics"`                 | `orders_dq_metrics.json`, the [DQ metrics](dq-metrics/json-export.md)                  | Yes     |

```python
result.save("dq-results/", prefix="orders")  # every output but "all_query_outputs"
result.save("dq-results/", prefix="orders", outputs=["consolidated_query_outputs"])
result.save("dq-results/", prefix="orders", outputs=[])  # the summary only
```

`"annotated_table"` and `"dq_metrics"` attribute failed rows to each table,
which can download the table and is slow on a large one. They share that work,
so writing both costs no more than writing one. The other outputs run no extra
queries, except `"all_query_outputs"`, which runs the row query of each check
that passed. You can't pick both `"failed_query_outputs"` and
`"all_query_outputs"`, as they write the same folder. You can set the outputs
for every run with [`ValidationConfig(outputs=...)`](run-settings.md#outputs).
`check_info` warns when `"annotated_table"` is not in the list, as only the
annotated tables have a `check_info` column.

### Per-check files

A per-check file holds the rows one check's query returned, with its own
columns plus `check_id` and `tables_in_query`. `"failed_query_outputs"` writes
the rows `get_output_dfs()` returns and `"all_query_outputs"` writes the rows
`get_output_dfs(scope="all")` returns.

The file name is the schema and the check name, each cleaned for use in a file
name and joined by `__`. A check `amount > 0` on schema `orders` writes
`orders_checks/orders__amount_0.csv`. Two checks that clean to the same name,
compared without case, such as `amount > 0` and `amount_0`, make `save()` raise
a `ValueError` before it writes anything. So do two checks with the same name
in one schema. Rename one of them. Residue files are named the same way.

A check with no rows writes no file.

### The saved_outputs list

`orders_summary.json` has a `saved_outputs` key that lists the files of each
output. A check with no rows is listed with `"rows": 0` and no `"file"`.

```json
"saved_outputs": {
  "failed_query_outputs": [
    {"schema": "orders", "check": "amount > 0", "rows": 3, "file": "orders_checks/orders__amount_0.csv"},
    {"schema": "orders", "check": "id_unique", "rows": 0}
  ],
  "consolidated_query_outputs": [
    {"tables": "orders", "rows": 3, "file": "orders_orders.csv"}
  ],
  "dq_metrics": [{"file": "orders_dq_metrics.json"}]
}
```

### Deprecated output_mode

`output_mode` still works with a `FutureWarning` and writes the files it wrote
in v0.0.6. It will be removed in v0.1.0. Passing it with `outputs` raises a
`ValueError`.

| `output_mode`   | Use instead                                                            |
| --------------- | ---------------------------------------------------------------------- |
| `"failed_rows"` | `outputs=["consolidated_query_outputs"]`                               |
| `"annotated"`   | `outputs=["annotated_table", "dq_metrics"]`                            |
| `"both"`        | `outputs=["consolidated_query_outputs", "annotated_table", "dq_metrics"]` |

### Failed-rows CSVs

A failed-rows CSV holds each failed row once, with the checks it failed in
`check_ids`. vowl writes one CSV for each set of tables the checks read. These
are the tables `get_consolidated_output_dfs()` returns.

- `<tables>` is the tables the checks read, joined by `_`. A check on `orders`
  writes `orders_orders.csv`, and a check that joins `orders` and `customers`
  writes `orders_orders_customers.csv`.
- Checks that read the same tables but return different columns share one CSV.
  A check that did not return a column has nulls in it, so its row does not
  merge with the full row of the same record from another check. Use the
  [per-check files](#per-check-files) to see each check's own columns.

A check that fails because too few rows matched, such as `mustBeGreaterThan`,
is left out. The rows it returns are the ones that passed.

With [`fetch_tolerated_rows=True`](run-settings.md#fetch_tolerated_rows)
the CSVs also hold the rows of checks that passed within their threshold. A
CSV such a check contributed to gets a `tolerated_check_ids` column next to
`check_ids`, listing those checks. A row picked out by a failed check A and a
tolerated check B has `check_ids` `A, B` and `tolerated_check_ids` `B`. See
[Tolerated rows](design-considerations/checks/check-results.md#tolerated-rows).

## DQ metrics and OpenTelemetry

The DQ metrics are the counts and pass rates at check, dimension, schema and
run level, ready for a dashboard.

| Method or property | What it does                                                                                                                                  |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `get_dq_metrics()` | Returns the DQ metrics as a `dict`. See [Exporting to dq_metrics.json](dq-metrics/json-export.md).                                            |
| `save(...)`        | Writes the DQ metrics to `<prefix>_dq_metrics.json`, next to the annotated tables. See [Saving results](#saving-results).                     |
| `export_otel(...)` | Sends the DQ metrics, traces and logs to OpenTelemetry. See [Exporting to OpenTelemetry](dq-metrics/otel-export.md).                          |
| `run_id`           | The run ID. `save()` and `export_otel()` both use it. You can set your own. See [The run ID](dq-metrics/understanding-metrics.md#the-run-id). |

## Deprecated

These still work but will be removed in a future release.

| Deprecated                          | Use instead                                       |
| ----------------------------------- | ------------------------------------------------- |
| `ValidationResult.save_dataframe()` | `pyarrow.parquet.write_table(df.to_arrow(), ...)` |
