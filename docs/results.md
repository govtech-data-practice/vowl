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
`output_mode="as_is"` to keep it in the basic tier.

| Tier       | Methods                                                                                                                       | Cost                                                                                                                                                                                    | Counts                                                                                      |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Basic      | `print_summary()`, `get_check_results_df()`, `result.summary`, `save(output_mode="as_is")`                                    | Runs the check queries only. Never attributes rows to the table.                                                                                                                        | Each check's scalar `failed_rows_count`. A sum across checks is marked approximate.         |
| DQ metrics | `get_dq_metrics()`, `get_dq_metrics_df(by=...)`, `export_otel(...)`, `get_annotated_output()`, `save()` (`"attributed"`, `"both"`) | [Attributes failed rows to the table](design-considerations/checks/how-attributed-rows-work.md) once and reuses that work. Runs a `COUNT(*)` on each table the first time it is needed. | Rows that failed at least one check, rows that passed, pass rates and an `approximate` flag |

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
puts the tolerated ones in with a `tolerated` column. This method does not download your tables, so it is the better choice on
large tables.

```python
failed = result.get_output_dfs()
failed["orders::price_must_be_positive"]
```

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
`save()` also takes the `check_info`, `include_check_definition` and
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

### Output modes

`output_mode` sets which files `save()` writes. The modes are named for what
they do to the rows:

- `"as_is"` is cheap and does nothing to the rows. It writes each check's rows
  as its query returned them, grouped by table.
- `"attributed"` attributes the rows onto their table and writes the DQ
  metrics.
- `"both"` writes everything the other two write.

| File                                  | What it holds                               | Written by                   |
| ------------------------------------- | ------------------------------------------- | ---------------------------- |
| `orders_check_results.csv`            | The [check results](#check-results)         | Every mode                   |
| `orders_summary.json`                 | The numbers behind the summary              | Every mode                   |
| `orders_<schema>_annotated.csv`       | The annotated table of one schema           | `"attributed"` and `"both"`  |
| `orders_<schema>_<check>_residue.csv` | The residue of one check                    | `"attributed"` and `"both"`  |
| `orders_dq_metrics.json`              | The [DQ metrics](dq-metrics/json-export.md) | `"attributed"` and `"both"`  |
| `orders_<tables>.csv`                 | A [failed-rows CSV](#failed-rows-csvs)      | `"as_is"` and `"both"`       |

`"attributed"` is the default. It and `"both"` attribute failed rows to each
table, which can download the table and is slow on a large one. The annotated
tables and the DQ metrics share that work, so writing both costs no more than
writing one. `"as_is"` runs no extra queries. Use it when you only need
the failed rows.

The old names `"failed_rows"` (now `"as_is"`) and `"annotated"` (now
`"attributed"`) still work with a `FutureWarning`. They will be removed in
v0.1.0.

### Failed-rows CSVs

A failed-rows CSV holds each failed row once, with the checks it failed in
`check_ids`. vowl writes one CSV for each group of checks that read the same
tables and return the same columns. These are the tables
`get_consolidated_output_dfs()` returns.

- `<tables>` is the tables the checks read, joined by `_`. A check on `orders`
  writes `orders_orders.csv`, and a check that joins `orders` and `customers`
  writes `orders_orders_customers.csv`.
- Checks that read the same tables but return different columns get separate
  CSVs, ending in `_1`, `_2` and so on. The matching
  `get_consolidated_output_dfs()` keys end in `__1`, `__2`.

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
