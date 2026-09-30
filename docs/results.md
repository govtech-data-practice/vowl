---
description: What you can do with the ValidationResult that validate_data returns. Print reports, get DataFrames, save to disk or cloud storage, and export DQ metrics.
---

# Reading Results

`validate_data` returns a `ValidationResult`. It holds every check's outcome
and the rows that failed, and it has methods to print, query, save and export
them. Nothing runs again when you call them.

```python
from vowl import validate_data

result = validate_data("contract.yaml", df=df)

result.print_summary()                        # check and row counts
annotated = result.get_annotated_output()     # your tables, with failed rows marked
result.save("dq-results/", output_mode="annotated")
```

## All methods

### Print to the console

These return the result itself, so you can chain them.

| Method                            | What it prints                                                                |
| --------------------------------- | ----------------------------------------------------------------------------- |
| `print_summary()`                 | Check and row counts for each schema, and a table of every check              |
| `show_failed_checks()`            | Each failed check with its operator, expected value and actual value          |
| `show_failed_rows(max_rows=5)`    | Up to `max_rows` failed rows per failed check. `max_rows=-1` prints them all. |
| `display_full_report(max_rows=5)` | `print_summary()` followed by `show_failed_rows()`                            |

[Quick start](getting-started.md#reading-the-summary) explains each line of the
summary.

### Get the results as data

The DataFrame methods return [Narwhals](https://narwhals-dev.github.io/narwhals/)
DataFrames. Call `.to_native()` to get the pandas, Polars or other DataFrame
underneath.

| Method or property                            | Returns                                                                                                                                    |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `passed`                                      | `True` when every check passed                                                                                                             |
| `get_check_results_df()`                      | One row per check: name, status, expected and actual values, timing. `include_check_definition=True` adds each check's definition as JSON. |
| `get_annotated_output(check_info=None)`       | Your tables with failed rows marked, plus the failed rows that could not be marked. See [Failed rows](failed-rows.md).                     |
| `get_row_quality_df(by="schema")`             | Row counts: how many rows failed and passed. See [Row counts](#row-counts).                                                                |
| `get_output_dfs(checks=None)`                 | Each check's failed rows on their own, keyed `"<schema>::<check_name>"`                                                                    |
| `get_dq_metrics()`                            | The [DQ metrics](dq-metrics/index.md) as a `dict`, the same content as `dq_metrics.json`                                                   |
| `export_otel(...)`                            | Sends the results to OpenTelemetry. See [Exporting to OpenTelemetry](dq-metrics/otel-export.md).                                           |
| `run_id`                                      | This run's ID. You can set it to your own value. See [The run ID](dq-metrics/index.md#the-run-id).                                         |
| `contract_id`, `api_version`, `contract_data` | The contract's `id`, its ODCS `apiVersion`, and the whole contract                                                                         |
| `get_consolidated_output_dfs(checks=None)`    | _Deprecated._ Use `get_annotated_output()`.                                                                                                |

## Saving results

`save()` writes the run to a folder:

```python
result.save("dq-results/", prefix="orders", output_mode="annotated")
```

| File                                  | What it holds                                                                                                                        |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `orders_check_results.csv`            | One row per check, the same as `get_check_results_df()`                                                                              |
| `orders_<schema>_annotated.csv`       | One per schema: the full table with a `check_info` column on each failed row                                                         |
| `orders_<schema>_<check>_residue.csv` | One per check whose failed rows could not be marked on the table. See [Failed rows](failed-rows.md#where-each-failed-check-ends-up). |
| `orders_summary.json`                 | The numbers behind `print_summary()`                                                                                                 |
| `orders_dq_metrics.json`              | The [DQ metrics](dq-metrics/json-export.md)                                                                                          |

!!! warning "Pass `output_mode=\"annotated\"`"

    `save()` still defaults to `output_mode="failed_rows"`, which writes the
    older grouped failed-rows CSVs and is deprecated. The default will change
    to `"annotated"` in a future release. Pass `output_mode="annotated"` now to
    get the files above and avoid the warning. `output_mode="both"` writes both
    kinds while you move over.

Other options:

- `check_info="names"`, `"summary"` or `"full"` sets how much detail the
  `check_info` column holds. See [Failed rows](failed-rows.md).
- `include_check_definition=True` and `include_contract_definition=True` add
  each check's definition to the check results CSV.
- `filesystem=` saves through a pyarrow filesystem you set up. See
  [Custom endpoints and explicit credentials](#custom-endpoints-and-explicit-credentials).

### Saving results to cloud storage

Give `save()` a URI instead of a folder and it writes straight to cloud
storage:

```python
result.save("s3://my-bucket/dq-results/run-1/", output_mode="annotated")
```

| Location         | Example                                                     |
| ---------------- | ----------------------------------------------------------- |
| Amazon S3        | `s3://my-bucket/dq-results/`                                |
| Google Cloud     | `gs://my-bucket/dq-results/`                                |
| Azure Data Lake  | `abfs://container@account.dfs.core.windows.net/dq-results/` |
| HDFS             | `hdfs://namenode:8020/dq-results/`                          |
| A local file URI | `file:///shared/dq-results/`                                |

vowl uses the filesystems that come with pyarrow, which vowl already depends
on, so there is nothing extra to install. Credentials come from the usual place
for each cloud. For S3 that means environment variables such as
`AWS_ACCESS_KEY_ID`, the `~/.aws` files, or an IAM role. For Google Cloud it
means your default application credentials.

`ValidationResult.save_dataframe(df, "s3://my-bucket/out.parquet", "parquet")`
writes any DataFrame to a path or URI the same way. It supports `csv`,
`parquet` and `json`.

#### Custom endpoints and explicit credentials

To save to an S3-compatible store such as MinIO, or to pass credentials
yourself, build a pyarrow filesystem and pass it as `filesystem=`.
`output_dir` is then a path inside that filesystem, starting with the bucket
name:

```python
import pyarrow.fs as pafs

minio = pafs.S3FileSystem(
    endpoint_override="http://minio.internal:9000",
    access_key="...",
    secret_key="...",
)
result.save("my-bucket/dq-results/run-1/", output_mode="annotated", filesystem=minio)
```

To keep plain `s3://` URIs instead, set the `AWS_ENDPOINT_URL` environment
variable to the store's address. Both saving and
[loading contracts from S3](loading-contracts.md#loading-contracts-from-s3)
pick it up.

!!! note

    Some pyarrow builds, mostly from conda, leave out S3 or Google Cloud
    support. If `save()` says the filesystem is not supported, install pyarrow
    from PyPI with `pip install --force-reinstall pyarrow`.

## Row counts

`result.get_row_quality_df()` returns, for each schema, how many rows failed
at least one check and how many passed them all. These are the same numbers as
**Passed Rows** in `print_summary()` and the row counts in the
[DQ metrics](dq-metrics/index.md).

```python
result.get_row_quality_df()                  # one row per schema
result.get_row_quality_df(by="dimension")    # one row per schema and dimension
result.get_row_quality_df(by="check")        # how each check was counted, and why
```

By default only checks that failed add failed rows. A check can pass with a
few failed rows when its threshold allows them, for example `mustBeLessThan: 10`.
Those rows are reported as `tolerated_rows`. To count them as failed rows too:

```python
from vowl import ValidationConfig

config = ValidationConfig(row_issue_scope="all_violations")
result = validate_data("contract.yaml", df=df, config=config)
```

vowl counts the rows inside the data source where it can, so the numbers stay
exact on large tables and do not depend on `max_failed_rows`. The `exact`
column is `False` when a number could be off.
[How vowl counts failed rows](failed-rows.md#how-vowl-counts-failed-rows)
explains which checks are counted and how.

## Run settings

`ValidationConfig` holds the settings that apply to a whole run. Pass it to
`validate_data` as `config=`.

| Setting                               | Default           | What it does                                                                                                                           |
| ------------------------------------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `max_failed_rows`                     | `-1` (no cap)     | The most failed rows kept per check. See [Capping failed rows](failed-rows.md#capping-failed-rows).                                    |
| `row_issue_scope`                     | `"failed_checks"` | `"all_violations"` also counts the failed rows of checks that passed within their threshold. See [Row counts](#row-counts).            |
| `use_try_cast`                        | `True`            | Turns `CAST` into `TRY_CAST` in check queries, so a value that cannot be converted becomes a failed row instead of stopping the check. |
| `output_mode`                         | `"failed_rows"`   | What `save()` writes when you do not pass `output_mode`. Set it to `"annotated"`. See [Saving results](#saving-results).               |
| `annotated_check_info`                | `"names"`         | How much detail the `check_info` column holds when you do not pass `check_info`                                                        |
| `enable_additional_schema_statistics` | `True`            | Counts the rows in each table for the summary. `False` skips the count.                                                                |
| `max_rows_for_statistics`             | `-1` (no cap)     | The most rows counted per table for the summary                                                                                        |
