---
description: How to use the ValidationResult that validate_data returns. Check whether the run passed, print reports, find failed rows, read row counts, export DQ metrics and save the results.
---

# The Results Object

`validate_data` returns a `ValidationResult`, called **the result** on this
page. It holds the status of every check. It fetches failed rows, row counts
and tables only when a method first needs them, and keeps them for later
calls. Keep the connection to your data open until you are done with the
result.

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

Three methods return failed rows. They differ in how the rows are arranged and
in how much data they read.

|                     | [`get_output_dfs()`](#failed-rows-per-check) | [`get_consolidated_output_dfs()`](#consolidated-failed-rows) | [`get_annotated_output()`](#annotated-output) |
| ------------------- | -------------------------------------------- | ------------------------------------------------------------ | --------------------------------------------- |
| Keyed by            | Check                                        | Tables the checks read                                       | Schema                                        |
| One row per         | Row the check's row query returned           | Distinct row across checks, with `check_ids`                 | Row of the table, with `check_info`           |
| Columns             | The check's own                              | Every column the checks returned                             | The table's                                   |
| Attributes rows     | No                                           | No                                                           | Yes                                           |
| Downloads tables    | No                                           | No                                                           | Every table in the contract                   |
| Saved by `outputs=` | `"failed_query_outputs"`                     | `"consolidated_query_outputs"`                               | `"annotated_table"`                           |

All three take `checks=["check_a", "check_b"]` to return only those checks, and
share the failed rows they fetch, so a second call queries nothing again. They
include the same checks. [`get_output_dfs(scope="all")`](#every-checks-rows)
includes more:

| Check                                                                            | Included                                                                                                                                                         |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FAILED`                                                                         | Yes                                                                                                                                                              |
| `FAILED`, with an operator that sets no upper limit, such as `mustBeGreaterThan` | No. Its rows are the ones that passed.                                                                                                                           |
| `PASSED`                                                                         | Only with [`fetch_tolerated_rows=True`](run-settings.md#fetch_tolerated_rows), as [tolerated rows](design-considerations/checks/check-results.md#tolerated-rows) |
| `ERROR`                                                                          | No                                                                                                                                                               |

Tolerated rows are marked by a `tolerated` column in `get_output_dfs()`, a
`tolerated_check_ids` column in `get_consolidated_output_dfs()` and
`"tolerated": true` in `check_info`.

The examples below use one `orders` table, with `order_id` as its primary key,
and three checks.

??? example "The example table and checks"

    | order_id | price | quantity |
    | -------- | ----- | -------- |
    | 1        | 9.5   | 2        |
    | 2        | -1.0  | 0        |
    | 3        | 0.0   | 1        |
    | 4        | 12.0  | 3        |

    ```yaml
    schema:
      - name: orders
        properties:
          - name: order_id
            logicalType: integer
            primaryKey: true
          - name: price
            logicalType: number
            quality:
              - type: sql
                name: price_must_be_positive
                query: "SELECT COUNT(*) FROM orders WHERE price <= 0"
                mustBe: 0
              - type: sql
                name: price_below_one
                query: "SELECT COUNT(*) FROM (SELECT order_id, price FROM orders WHERE price < 1) AS low"
                mustBe: 0
          - name: quantity
            logicalType: integer
            quality:
              - type: sql
                name: quantity_must_be_positive
                query: "SELECT COUNT(*) FROM orders WHERE quantity <= 0"
                mustBe: 0
    ```

    Orders 2 and 3 fail `price_must_be_positive` and `price_below_one`. Order 2
    also fails `quantity_must_be_positive`. `price_below_one` returns only
    `order_id` and `price`.

#### Failed rows per check

`get_output_dfs()` returns one DataFrame per check, keyed by the check's
`target` in `check_results.csv` and its name:

| Check on         | Key                                 |
| ---------------- | ----------------------------------- |
| A column         | `"<schema>.<column>::<check_name>"` |
| The whole schema | `"<schema>::<check_name>"`          |

Each DataFrame holds the columns the row query returned, plus `check_id` and
`tables_in_query`. The rows are exactly what the query returned, so use this
view to cross-check the other two.

```python
failed = result.get_output_dfs()
failed["orders.price::price_must_be_positive"]
```

??? example "Example output"

    ```python
    failed = result.get_output_dfs()
    list(failed)
    # ['orders.price::price_below_one',
    #  'orders.price::price_must_be_positive',
    #  'orders.quantity::quantity_must_be_positive']
    ```

    `failed["orders.price::price_must_be_positive"]`

    | order_id | price | quantity | check_id               | tables_in_query |
    | -------- | ----- | -------- | ---------------------- | --------------- |
    | 2        | -1.0  | 0        | price_must_be_positive | orders          |
    | 3        | 0.0   | 1        | price_must_be_positive | orders          |

    `failed["orders.price::price_below_one"]` has only the columns its query
    returned:

    | order_id | price | check_id        | tables_in_query |
    | -------- | ----- | --------------- | --------------- |
    | 2        | -1.0  | price_below_one | orders          |
    | 3        | 0.0   | price_below_one | orders          |

    `failed["orders.quantity::quantity_must_be_positive"]`

    | order_id | price | quantity | check_id                  | tables_in_query |
    | -------- | ----- | -------- | ------------------------- | --------------- |
    | 2        | -1.0  | 0        | quantity_must_be_positive | orders          |

##### Every check's rows

`get_output_dfs(scope="all")` returns what the row query of every row-level
check returned, whatever its status, with a `status` column in place of
`tolerated`. It runs the row query of each passed check and ignores
`fetch_tolerated_rows`. It differs from the default scope in which checks it
includes:

| Check                                                                            | `scope="failed"` (default)            | `scope="all"`                          |
| -------------------------------------------------------------------------------- | ------------------------------------- | -------------------------------------- |
| `FAILED`                                                                         | Yes                                   | Yes                                    |
| `FAILED`, with an operator that sets no upper limit, such as `mustBeGreaterThan` | No                                    | Yes. Its rows are the ones that passed |
| `PASSED`, with rows within its limit                                             | Only with `fetch_tolerated_rows=True` | Yes                                    |
| `PASSED`, with no rows                                                           | No                                    | Yes, as an empty DataFrame             |
| `ERROR`, or not row-level                                                        | No                                    | No                                     |

Use `get_consolidated_output_dfs()` with `fetch_tolerated_rows=True` for every
row that broke a limit, even under a check that passed. Use `scope="all"` for
exactly what each check's query returned.

#### Consolidated failed rows

`get_consolidated_output_dfs()` stacks the failed rows of every check and keys
them by `tables_in_query`, such as `"orders"` or `"customers, orders"`.
Identical rows merge into one, with every check that returned them in
`check_ids`. The merge runs on your machine, so memory grows with the number of
failed rows, not with the size of your tables.

```python
consolidated = result.get_consolidated_output_dfs()
consolidated["orders"]                          # each failed row once, with check_ids
```

??? example "Example output"

    `result.get_consolidated_output_dfs()["orders"]`

    | order_id | price | quantity | check_ids                                         | tables_in_query |
    | -------- | ----- | -------- | ------------------------------------------------- | --------------- |
    | 2        | -1.0  | 0        | price_must_be_positive, quantity_must_be_positive | orders          |
    | 3        | 0.0   | 1        | price_must_be_positive                            | orders          |
    | 2        | -1.0  | null     | price_below_one                                   | orders          |
    | 3        | 0.0   | null     | price_below_one                                   | orders          |

    Orders 2 and 3 each appear twice. `price_below_one` did not return
    `quantity`, so its rows do not merge with the full rows of the same
    orders. Counting this table gives 4 failed rows, but only 2 orders failed.
    See [Limits of consolidated failed rows](#limits-of-consolidated-failed-rows).

??? example "With tolerated rows"

    With `fetch_tolerated_rows=True`, a row returned by failed check `A` and
    tolerated check `B` has `check_ids` `A, B` and `tolerated_check_ids` `B`.

##### Limits of consolidated failed rows

Rows merge by value. They are not attributed to the table, as they are in the
annotated output and the row counts, so the output can differ from the table:

| Cause                                                     | Effect                                                                                                                                                                                |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Identical rows in the table, or a join that repeats a row | They merge into one row                                                                                                                                                               |
| A check that returns fewer columns                        | Its rows have nulls in the other columns and do not merge with the full row of the same record                                                                                        |
| `DISTINCT`, a join or an aggregate in the row query       | The rows may not be rows of the table. See [Checks can change the rows they return](design-considerations/checks/how-attributed-rows-work.md#checks-can-change-the-rows-they-return). |
| A cross-table check                                       | Its rows are keyed by every table it reads, such as `"customers, orders"`, even when they hold only `orders` columns                                                                  |
| [`max_failed_rows`](run-settings.md#max_failed_rows)      | Each check holds at most that many rows                                                                                                                                               |

Use `len()` on this output as a rough guide only. For exact counts, use the
[row counts](#row-counts).

#### Annotated output

`get_annotated_output()` returns a `dict` with two keys:

| Key           | Holds                                                                                                                                |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `"annotated"` | One table per schema: the full table with a `check_info` column, listing the checks each row failed. Empty on rows that failed none. |
| `"residues"`  | The failed rows of each check that could not be annotated on its table, keyed like `get_output_dfs()`                                |

```python
output = result.get_annotated_output()
output["annotated"]["orders"]                   # the orders table, with check_info
output["residues"]                              # {"<schema>.<column>::<check_name>": failed rows}
```

??? example "Example output"

    `result.get_annotated_output()["annotated"]["orders"]`

    | order_id | price | quantity | check_info                                                                                                              |
    | -------- | ----- | -------- | ----------------------------------------------------------------------------------------------------------------------- |
    | 1        | 9.5   | 2        | null                                                                                                                    |
    | 2        | -1.0  | 0        | `[{"check_name": "price_must_be_positive"}, {"check_name": "price_below_one"}, {"check_name": "quantity_must_be_positive"}]` |
    | 3        | 0.0   | 1        | `[{"check_name": "price_must_be_positive"}, {"check_name": "price_below_one"}]`                                         |
    | 4        | 12.0  | 3        | null                                                                                                                    |

    Every order is here, and each failed order appears once. `price_below_one`
    returned only `order_id` and `price`, but `order_id` is the primary key,
    so its rows merge onto the table. `output["residues"]` is empty.

    With `check_info="summary"`, each item also has the check's `dimension`,
    `tags` and `target`:

    ```json
    [{"check_name": "price_must_be_positive", "dimension": null, "tags": null, "target": "orders.price"},
     {"check_name": "price_below_one", "dimension": null, "tags": null, "target": "orders.price"}]
    ```

`check_info` takes `"names"` (the default), `"summary"` or `"full"`.
[What the annotated output holds](design-considerations/checks/annotating-the-source-table.md#what-the-annotated-output-holds)
describes each option, and
[Where each failed check ends up](design-considerations/checks/annotating-the-source-table.md#where-each-failed-check-ends-up)
explains which checks become residues.

This is the most complete view and the most costly, as it downloads every
table in the contract.

## DQ metrics and OpenTelemetry

The DQ metrics are the counts and pass rates at check, dimension, schema and
run level, ready for a dashboard. vowl attributes failed rows to each table to
compute them, so each failed row counts once.

| Method or property    | What it does                                                                                                                                  |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `get_dq_metrics_df()` | Returns the row counts as a DataFrame. See [Row counts](#row-counts).                                                                         |
| `get_dq_metrics()`    | Returns the DQ metrics as a `dict`. See [Exporting to dq_metrics.json](dq-metrics/json-export.md).                                            |
| `save(...)`           | Writes the DQ metrics to `<prefix>_dq_metrics.json`. See [Outputs](#outputs).                                                                 |
| `export_otel(...)`    | Sends the DQ metrics, traces and logs to OpenTelemetry. See [Exporting to OpenTelemetry](dq-metrics/otel-export.md).                          |
| `run_id`              | The run ID. `save()` and `export_otel()` both use it. You can set your own. See [The run ID](dq-metrics/understanding-metrics.md#the-run-id). |

### Row counts

`get_dq_metrics_df()` returns, for each schema, how many rows failed at least
one row-level check and how many passed them all. A row that fails two checks
counts once. The summary does not show these. Its **Failed Rows
(approximate)** is a sum of the scalar counts of the failed checks.

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

Only attributed rows count. A row-level check that is not attributable adds
nothing and is counted in `checks_not_attributable`. If it failed and may have
rows, `approximate` is `True`. When no row-level check of a schema or dimension
is attributable, its numbers are `None`. vowl attributes inside the data
source where it can, so the row counts do not depend on `max_failed_rows`.

`by="check"` has one row per check, with these columns:

| Column               | What it holds                                                                                                           |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `row_level`          | Whether the check is a row-level check                                                                                  |
| `attribution_method` | How vowl counted the check. Empty when it is not row-level, not attributable or passed.                                 |
| `attribution_note`   | Why the check is not row-level or not attributable. A passed check has `passed, not attributed`.                        |
| `scalar_count`       | The check's scalar count, the number that decides pass or fail                                                          |
| `attributed_rows`    | The rows of the table its failed rows are attributed to, every copy counted. `None` when the check is not attributable. |
| `approximate`        | `True` when this check's rows are incomplete or approximate                                                             |

`scalar_count` and `attributed_rows` differ for a check that uses `DISTINCT`
or a join, or that counts distinct values.
[How Attributed Rows Work](design-considerations/checks/how-attributed-rows-work.md)
explains which checks are row-level and how they are counted.

## Saving results

`save()` writes the result to a folder or to cloud storage, prints each file it
writes and returns the result.

```python
result.save("dq-results/", prefix="orders")
```

File names start with `prefix`, or with `vowl_results` if you leave it out.
`save()` also takes the `check_info`, `include_check_definition` and
`include_contract_definition` options of the methods above.

### Outputs

`outputs` is a list of the files `save()` writes. `<prefix>_check_results.csv`
and `<prefix>_summary.json` are always written.

| Output                         | Files                                                                                                                                              | Default |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| (always)                       | `orders_check_results.csv` with the [check results](#check-results), `orders_summary.json` with the numbers behind the summary                     | Yes     |
| `"failed_query_outputs"`       | `orders_checks/<schema>__<column>__<check>.csv`, the rows of one failed check                                                                      | No      |
| `"all_query_outputs"`          | `orders_checks/<schema>__<column>__<check>.csv`, the rows of every row-level check, with `status`                                                  | Yes     |
| `"consolidated_query_outputs"` | `orders_<tables>.csv`, a [consolidated file](#consolidated-files)                                                                                  | Yes     |
| `"annotated_table"`            | `orders_<schema>_annotated.csv`, the annotated table of one schema, and `orders_<schema>__<column>__<check>_residue.csv`, the residue of one check | Yes     |
| `"dq_metrics"`                 | `orders_dq_metrics.json`, the [DQ metrics](dq-metrics/json-export.md)                                                                              | Yes     |

```python
result.save("dq-results/", prefix="orders")  # every output but "failed_query_outputs"
result.save("dq-results/", prefix="orders", outputs=["consolidated_query_outputs"])
result.save("dq-results/", prefix="orders", outputs=[])  # the summary only
```

- `"failed_query_outputs"` and `"all_query_outputs"` write the same folder, so
  you can pick only one.
- [`ValidationConfig(outputs=...)`](run-settings.md#outputs) sets the outputs
  for every run.
- `check_info` warns when `"annotated_table"` is not in the list, as only the
  annotated tables have a `check_info` column.
- [What each method costs](#what-each-method-costs) lists the work each output
  does.

### Per-check files

`"failed_query_outputs"` writes the rows of
[`get_output_dfs()`](#failed-rows-per-check), and `"all_query_outputs"` the
rows of `get_output_dfs(scope="all")`, one CSV per check. A check with no rows
writes no file. Residue files are named the same way.

| Check on         | File                                              |
| ---------------- | ------------------------------------------------- |
| A column         | `<prefix>_checks/<schema>__<column>__<check>.csv` |
| The whole schema | `<prefix>_checks/<schema>__<check>.csv`           |

Each part is cleaned for use in a file name. Comparison operators are spelled
out (`>` as `gt`, `>=` as `gte`, `<` as `lt`, `<=` as `lte`, `=` and `==` as
`eq`, `!=` and `<>` as `ne`) and any other character that is not a letter,
digit, `.`, `_` or `-` becomes `_`. So check `amount > 0` on column `amount` of
schema `orders` writes `orders_checks/orders__amount__amount_gt_0.csv`.

If two checks still clean to the same name, compared without case, such as
`Amount` and `amount` on one column, the later one in the contract gets `_2`,
the next `_3` and so on: `orders__amount__Amount.csv` and
`orders__amount__amount_2.csv`. Every check holds its name, even one that
writes no file, so a check's file name does not depend on how the others did.
The `saved_outputs` list in the summary says which file is which check.

### Consolidated files

`"consolidated_query_outputs"` writes one CSV per key of
[`get_consolidated_output_dfs()`](#consolidated-failed-rows), with the tables
joined by `_`. The files have the same
[limits](#limits-of-consolidated-failed-rows).

| Checks read              | File                          |
| ------------------------ | ----------------------------- |
| `orders`                 | `orders_orders.csv`           |
| `orders` and `customers` | `orders_orders_customers.csv` |

### The saved_outputs list

`orders_summary.json` has a `saved_outputs` key that lists the files of each
output. A check with no rows has `"rows": 0` and no `"file"`. A check on the
whole schema has no `"column"`.

??? example "Example saved_outputs"

    ```json
    "saved_outputs": {
      "failed_query_outputs": [
        {"schema": "orders", "column": "amount", "check": "amount > 0", "rows": 3,
         "file": "orders_checks/orders__amount__amount_gt_0.csv"},
        {"schema": "orders", "column": "id", "check": "id_unique", "rows": 0}
      ],
      "consolidated_query_outputs": [
        {"tables": "orders", "rows": 3, "file": "orders_orders.csv"}
      ],
      "dq_metrics": [{"file": "orders_dq_metrics.json"}]
    }
    ```

### Cloud storage

Pass a URI instead of a folder to write to cloud storage:

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
extra to install. Credentials come from the usual place for each cloud, such
as `AWS_ACCESS_KEY_ID`, the `~/.aws` files or an IAM role for S3, and the
default application credentials for Google Cloud. For an S3-compatible store
such as MinIO, set `AWS_ENDPOINT_URL`, the same variable
[Loading contracts from S3](usage-patterns.md#from-s3) reads.

??? example "Pass credentials yourself"

    Pass a pyarrow filesystem as `filesystem=`. The path is then inside that
    filesystem and starts with the bucket name:

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

## What each method costs

The checks run once, inside `validate_data`. Each method then does only the
work it needs, and keeps it, so a later method that needs the same work pays
nothing more. The groups run from the cheapest to the most costly, and each
group also does the work it needs from the groups above.

<table>
  <thead>
    <tr><th>Method or <code>save()</code> output</th><th>What it costs</th></tr>
  </thead>
  <tbody>
    <tr class="table-group"><th colspan="2">No extra queries<small>Uses what the checks already returned.</small></th></tr>
    <tr><td><code>result.passed</code>, <code>result.summary</code>, <code>print_summary()</code>, <code>show_failed_checks()</code>, <code>get_check_results_df()</code></td><td>Nothing more.</td></tr>
    <tr><td><code>save()</code> with <code>outputs=[]</code>: <code>check_results.csv</code> and <code>summary.json</code></td><td>Writing two small files. They are written by every <code>save()</code>.</td></tr>
    <tr class="table-group"><th colspan="2">Failed query outputs<small>Runs the row query of each failed check.</small></th></tr>
    <tr><td><code>show_failed_rows()</code>, <code>get_output_dfs()</code>, <code>get_consolidated_output_dfs()</code></td><td>One query per failed check, the first time one of these is called. Each returns only that check's failed rows, at most <code>max_failed_rows</code>. <code>get_consolidated_output_dfs()</code> then groups them on your machine.</td></tr>
    <tr><td><code>save()</code> with <code>"failed_query_outputs"</code> or <code>"consolidated_query_outputs"</code></td><td>The same queries, plus writing the files.</td></tr>
    <tr class="table-group"><th colspan="2">All query outputs<small>Runs the row query of every row-level check, passed or failed.</small></th></tr>
    <tr><td><code>get_output_dfs(scope="all")</code></td><td>One query per row-level check that did not error. A check that passed can return many rows, so this can fetch far more than the failed rows.</td></tr>
    <tr><td><code>save()</code> with <code>"all_query_outputs"</code></td><td>The same queries, plus writing the files.</td></tr>
    <tr class="table-group"><th colspan="2">Row attribution<small>Matches failed rows to the rows of each table, and counts the rows of each table.</small></th></tr>
    <tr><td><code>get_dq_metrics()</code>, <code>get_dq_metrics_df(by=...)</code>, <code>export_otel(...)</code></td><td>A <code>COUNT(*)</code> on each table, and queries that count failed rows inside the data source. Where the data source can't count a check, vowl fetches its failed rows and downloads the table to count them on your machine.</td></tr>
    <tr><td><code>save()</code> with <code>"dq_metrics"</code></td><td>The same work, plus writing <code>dq_metrics.json</code>.</td></tr>
    <tr class="table-group"><th colspan="2">Full source tables<small>Downloads every table in the contract, and the failed rows of each failed check.</small></th></tr>
    <tr><td><code>get_annotated_output()</code></td><td>Every table in full, even one whose checks all passed, plus the row attribution above. Memory grows with the size of your tables. The most costly method.</td></tr>
    <tr><td><code>save()</code> with <code>"annotated_table"</code></td><td>The same work, plus writing each table. Writing <code>"dq_metrics"</code> as well costs nothing more.</td></tr>
  </tbody>
</table>

- `save()` with no `outputs` writes every output but `"failed_query_outputs"`.
  That includes `"all_query_outputs"`, which runs the row query of every
  passed check, and `"annotated_table"`, which downloads every table, so it
  does the most costly work. Pass `outputs=["consolidated_query_outputs"]` or
  `outputs=["failed_query_outputs"]` to stay in the failed query outputs
  group.
- [`fetch_tolerated_rows=True`](run-settings.md#fetch_tolerated_rows) also
  runs the row query of each check that passed within its limit, for every
  method that reads failed rows.

## Deprecated

These still work but will be removed in a future release.

| Deprecated                          | Use instead                                       |
| ----------------------------------- | ------------------------------------------------- |
| `ValidationResult.save_dataframe()` | `pyarrow.parquet.write_table(df.to_arrow(), ...)` |

### output_mode { #deprecated-output_mode }

`output_mode` still works with a `FutureWarning` and writes the files it wrote
in v0.0.6. It will be removed in a future release. Passing it with
`outputs` raises a `ValueError`.

| `output_mode`   | Use instead                                                               |
| --------------- | ------------------------------------------------------------------------- |
| `"failed_rows"` | `outputs=["consolidated_query_outputs"]`                                  |
| `"annotated"`   | `outputs=["annotated_table", "dq_metrics"]`                               |
| `"both"`        | `outputs=["consolidated_query_outputs", "annotated_table", "dq_metrics"]` |
