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

## Did the run pass?

`result.passed` is `True` when no check has the status `FAILED`.

A check that ends in `ERROR` could not run, so it neither passed nor failed,
and it does not make `passed` `False`. To stop a pipeline on errors as well,
check the statuses yourself:

```python
import narwhals as nw

checks = result.get_check_results_df()
errored = checks.filter(nw.col("status") == "ERROR")

if not result.passed or len(errored) > 0:
    raise SystemExit("Data quality checks did not pass")
```

## Print a report

These methods print to the console. Each returns the result, so you can chain
them, for example `result.print_summary().show_failed_checks()`.

| Method                            | What it prints                                                                |
| --------------------------------- | ----------------------------------------------------------------------------- |
| `print_summary()`                 | The summary: check and row counts for each schema, and a table of every check |
| `show_failed_checks()`            | Each failed check with its operator, expected value and actual value          |
| `show_failed_rows(max_rows=5)`    | Up to `max_rows` failed rows for each failed check. `max_rows=-1` prints all. |
| `display_full_report(max_rows=5)` | `print_summary()` followed by `show_failed_rows()`                            |

[Reading the summary](getting-started.md#reading-the-summary) explains each
line of the summary.

## Getting results as DataFrames

The methods below return [Narwhals](https://narwhals-dev.github.io/narwhals/)
DataFrames. Call `.to_pandas()` or `.to_polars()` to work with them in the
library you use.

### Check results

`get_check_results_df()` returns one row per check. The main columns are:

| Column                                       | What it holds                                                       |
| -------------------------------------------- | ------------------------------------------------------------------- |
| `check_name`                                 | The check's name                                                    |
| `schema_name`                                | The schema the check belongs to                                     |
| `dimension`                                  | The check's dimension, such as `completeness`                       |
| `status`                                     | `PASSED`, `FAILED` or `ERROR`                                       |
| `operator`, `expected_value`, `actual_value` | What the check compared, for example `mustBe`, `0` and `3`          |
| `failed_rows_count`                          | The check's scalar count. For `COUNT(DISTINCT x)` it counts values. |
| `message`                                    | Why the check ended in `ERROR`. Empty otherwise.                    |
| `execution_time_ms`                          | How long the check took                                             |

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
out. This method does not download your tables, so it is the better choice on
large tables.

```python
failed = result.get_output_dfs()
failed["orders::price_must_be_positive"]
```

Both methods take `checks=["check_a", "check_b"]` to return only those checks.

### Row counts

`get_dq_metrics_df()` returns, for each schema, how many rows failed at least
one row-level check and how many passed them all. A row that fails two checks counts
once. These are the same numbers as **Passed Rows** in the summary and the row
counts in the [DQ metrics](dq-metrics/understanding-metrics.md).

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
check failed and may have rows, `approximate` is `True` and the summary marks **Passed Rows**
as approximate:

```text
Passed Rows: 4 / 5 (80.0%) (approx., 2 checks not attributable)
```

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
download. This work runs only when you ask for DQ metrics.
[How Attributed Rows Work](design-considerations/checks/how-attributed-rows-work.md)
explains which checks are row-level and how they are counted.

## DQ metrics and OpenTelemetry

The DQ metrics are the counts and pass rates at check, dimension, schema and
run level, ready for a dashboard.

| Method or property     | What it does                                                                                                                                  |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `get_dq_metrics()`     | Returns the DQ metrics as a `dict`. See [Exporting to dq_metrics.json](dq-metrics/json-export.md).                                            |
| `save_dq_metrics(...)` | Writes the DQ metrics to `<prefix>_dq_metrics.json`. It takes the same `output_dir`, `prefix` and `filesystem` as `save()`.                   |
| `export_otel(...)`     | Sends the DQ metrics, traces and logs to OpenTelemetry. See [Exporting to OpenTelemetry](dq-metrics/otel-export.md).                          |
| `run_id`               | The run ID. `save()` and `export_otel()` both use it. You can set your own. See [The run ID](dq-metrics/understanding-metrics.md#the-run-id). |

## Saving results

`save()` writes the result to a folder. It returns the result and prints the
files it wrote.

```python
result.save("dq-results/", prefix="orders")
```

| File                                  | What it holds                       |
| ------------------------------------- | ----------------------------------- |
| `orders_check_results.csv`            | The [check results](#check-results) |
| `orders_<schema>_annotated.csv`       | One annotated table per schema      |
| `orders_<schema>_<check>_residue.csv` | One file per residue                |
| `orders_summary.json`                 | The numbers behind the summary      |

Without `prefix`, the files start with `vowl_results`.

`save()` writes no DQ metrics, so it never attributes rows. To save them too,
call `result.save_dq_metrics("dq-results/", prefix="orders")`, which writes
`orders_dq_metrics.json`. See [Exporting to dq_metrics.json](dq-metrics/json-export.md).

`save()` takes the same `check_info`, `include_check_definition` and
`include_contract_definition` options as the methods above.

### Saving to cloud storage

Give `save()` a URI instead of a folder to write straight to cloud storage:

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

vowl writes through the filesystems that come with pyarrow, so there is
nothing extra to install. Credentials come from the usual place for each
cloud. For S3 that means environment variables such as `AWS_ACCESS_KEY_ID`,
the `~/.aws` files, or an IAM role. For Google Cloud it means your default
application credentials.

To use an S3-compatible store such as MinIO, set the `AWS_ENDPOINT_URL`
environment variable to its address. [Loading contracts from S3](usage-patterns.md#from-s3)
reads the same variable.

To pass credentials yourself, build a pyarrow filesystem and pass it as
`filesystem=`. The folder is then a path inside that filesystem, starting with
the bucket name:

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

### Saving one DataFrame

To write one check's failed rows on their own, convert the frame to Arrow and
write it with pyarrow. pyarrow takes the same URIs and `filesystem=` as
`save()`:

```python
import pyarrow.parquet as pq

failed = result.get_output_dfs()["orders::price_must_be_positive"]
pq.write_table(failed.to_arrow(), "s3://my-bucket/price_failures.parquet")
```

You can also use your own library's writer on `failed.to_native()`, such as
`to_parquet()` in pandas or `write_parquet()` in polars.

## Contract details

| Property        | What it holds                    |
| --------------- | -------------------------------- |
| `contract_id`   | The contract's `id`              |
| `api_version`   | The contract's ODCS `apiVersion` |
| `contract_data` | The whole contract               |

## Deprecated

These still work but will be removed in a future release.

| Deprecated                          | Use instead                                       |
| ----------------------------------- | ------------------------------------------------- |
| `save(output_mode="failed_rows")`   | `save()`                                          |
| `save(output_mode="both")`          | `save()`                                          |
| `get_consolidated_output_dfs()`     | `get_annotated_output()`                          |
| `ValidationResult.save_dataframe()` | `pyarrow.parquet.write_table(df.to_arrow(), ...)` |

`output_mode="failed_rows"` writes the older set of files, which group the
failed rows of several checks together. `output_mode="both"` writes the
annotated tables and the older files together, which can help while you move
over.
