---
description: The DQ metrics vowl reports for every run, at check, dimension, schema and run level.
---

# Understanding DQ Metrics

!!! tip "Interactive Demo"

    The [DQ Metrics notebook](https://github.com/govtech-data-practice/vowl/blob/main/examples/6_dq_metrics/dq_metrics.ipynb) reads one run's metrics at every level, loads `dq_metrics.json` with pandas and exports the same run to OpenTelemetry.

From the results of a run, vowl can work out a set of numbers that describe
the quality of your data, such as "how many checks failed" or "what share of
rows passed". These are the **DQ metrics**. vowl calculates them when you ask
for them, not during the run. You can get them in two ways:

- [Exporting to OpenTelemetry](otel-export.md) sends them to your monitoring
  tool, such as Grafana or Datadog, for dashboards and alerts.
- [Exporting to dq_metrics.json](json-export.md) writes them to a file next to
  the rest of the run's output, for a data warehouse or a notebook.

Both come from the same calculation. The names, attributes and values are
the same in both, so a number on a dashboard always matches the number in
the file. This page describes the metrics once for both.

## The four levels

Every metric is a reading at one of four **levels**. The level says what one
reading covers.

| Level         | One reading per      | Answers questions like                         |
| ------------- | -------------------- | ---------------------------------------------- |
| **check**     | Check                | "How many rows failed `email_required_check`?" |
| **dimension** | Schema and dimension | "What share of `orders` rows are complete?"    |
| **schema**    | Schema               | "How many `orders` rows have any problem?"     |
| **run**       | Run                  | "How did last night's run do overall?"         |

A **schema** is one table in the contract. A **dimension** is the kind of
quality a check measures, such as `completeness` or `consistency`. A check
with no dimension is put under `unknown`.

## Metric names

Every metric is named `vowl.<level>.<unit>.<measure>`:

- **level** is `check`, `dimension`, `schema` or `run`.
- **unit** is what is counted: `check`, `row` or `schema`.
- **measure** is `count` or `pass_rate`. Timings are `duration`. The check
  level also has `scalar_count` and `scalar_pass_rate`, which use the
  check's scalar count (see [How rows are counted](#how-failed-rows-are-counted)).

So `vowl.schema.row.pass_rate` is "at schema level, the share of rows that
passed", and `vowl.check.check.count` is "at check level, the number of
checks". Every name follows the pattern, so you can build one from its parts.

The `prefix` option of `export_otel` replaces `vowl` at the start of every name.

## All metrics

| Level     | Check count                  | Check pass rate                  | Row count                  | Row pass rate                  | Other                                                                     |
| --------- | ---------------------------- | -------------------------------- | -------------------------- | ------------------------------ | ------------------------------------------------------------------------- |
| check     | `vowl.check.check.count`     |                                  | `vowl.check.row.count`     | `vowl.check.row.pass_rate`     | `vowl.check.row.scalar_count`, `vowl.check.row.scalar_pass_rate`, `vowl.check.duration` |
| dimension | `vowl.dimension.check.count` | `vowl.dimension.check.pass_rate` | `vowl.dimension.row.count` | `vowl.dimension.row.pass_rate` |                                                                           |
| schema    | `vowl.schema.check.count`    | `vowl.schema.check.pass_rate`    | `vowl.schema.row.count`    | `vowl.schema.row.pass_rate`    |                                                                           |
| run       | `vowl.run.check.count`       | `vowl.run.check.pass_rate`       | `vowl.run.row.count`       | `vowl.run.row.pass_rate`       | `vowl.run.schema.count`, `vowl.run.duration`                              |

There is no check pass rate at check level. For one check it would always be
0 or 1, which `vowl.check.check.count` already says.

## Statuses

A count is split into parts by its `status` attribute. Add the parts to get
the total. Which statuses a count has depends on the **unit** in its name,
`vowl.<level>.<unit>.<measure>`: `check`, `row` or `schema`.

!!! example

    The examples below use the `orders` table from the
    [worked example](#worked-example): 100 rows and 5 checks.

### Rows: `PASSED` or `FAILED`

A row is `FAILED` if it has a problem, and `PASSED` if it has none. A row with
several problems is still one failed row. Which checks count towards the row
numbers is explained in [How rows are counted](#how-failed-rows-are-counted).

!!! example

    In `orders`, 5 rows have no email, so `email_required_check` reports 5
    failed rows:

    ```
    vowl.check.row.count:  95  {status="PASSED", check_name="email_required_check"}
    vowl.check.row.count:  5   {status="FAILED", check_name="email_required_check"}
    ```

    Another 3 rows have a duplicate `order_id`. One of them also has no email,
    so it has two problems. At schema level each row is counted once, so 7 rows
    of `orders` failed, not 8:

    ```
    vowl.schema.row.count:  93  {status="PASSED", schema_name="orders"}
    vowl.schema.row.count:  7   {status="FAILED", schema_name="orders"}
    ```

### Checks: `PASSED`, `FAILED` or `ERROR`

`FAILED` means the check ran and found bad data. `ERROR` means the check could
not run, for example because its query names a column that does not exist.

!!! example

    3 of the 5 checks on `orders` pass and 2 find bad data:

    ```
    vowl.schema.check.count:  3  {status="PASSED", schema_name="orders"}
    vowl.schema.check.count:  2  {status="FAILED", schema_name="orders"}
    vowl.schema.check.count:  0  {status="ERROR",  schema_name="orders"}
    ```

### Schemas: `PASSED`, `FAILED` or `ERROR`

A schema is `FAILED` when any of its checks failed. Otherwise it is `ERROR`
when any of its checks could not run. Otherwise it is `PASSED`. A table with
both a failed check and a check that could not run is `FAILED`, because a
known data problem outranks a check that could not say.

!!! example

    A run covers three tables. `customers` passes everything, `orders` has a
    failed check and `refunds` has a check that could not run:

    ```
    vowl.run.schema.count:  1  {status="PASSED"}
    vowl.run.schema.count:  1  {status="FAILED"}
    vowl.run.schema.count:  1  {status="ERROR"}
    ```

## Working with the numbers

### Pass rates

A pass rate is a share from 0 to 1.

- A **check pass rate** is passed checks over all checks. Checks that could
  not run (`ERROR`) count against it, because they did not pass.
- A **row pass rate** is passed rows over all rows in the table.

### Zeros and missing values

A count always sends every status, even when it is 0. A clean run sends
`FAILED` as 0 instead of sending nothing, so a dashboard never keeps showing
the failures of an earlier run.

A pass rate is different. It is left out when there is nothing to divide by,
for example a schema with no checks or an empty table. A missing rate means
"no rate", not 100%.

### Which numbers add up

Each check belongs to exactly one dimension and one schema, so check counts
always add up from one level to the next. Rows are different. One row can
fail several checks, so it can show up once per check or dimension it fails.

| From, to            | Checks | Rows                                                |
| ------------------- | ------ | --------------------------------------------------- |
| Check to dimension  | Yes    | No. A row can fail several checks in one dimension. |
| Dimension to schema | Yes    | No. A row can fail checks in several dimensions.    |
| Schema to run       | Yes    | Yes. Each table has different rows.                 |

The run level is there for convenience. You could add up the schema levels
yourself, but you do not have to.

### Metric types

Whether a number adds up decides its metric type:

- **Counters** hold numbers that add up: every `check.count` and
  `vowl.run.schema.count`. Adding a counter over a week gives the number of
  checks run that week.
- **Gauges** hold readings that do not add up: every `row.count` and every
  `pass_rate`. A gauge is a reading of one run, like a thermometer.
- **Histograms** hold timings: `vowl.check.duration` and `vowl.run.duration`,
  in milliseconds.

### Adding row counts across runs

A row count is a reading of one table in one run. Adding readings across
runs only makes sense when each run checks new rows, such as a daily load of
new records. When each run checks the whole table again, the same rows are
counted every run.

To get a pass rate over a week, add up `PASSED` and `FAILED` over the week
and divide, rather than averaging the daily rates. That way a small run
counts for less than a large one.

## How rows are counted {#how-failed-rows-are-counted}

vowl has two numbers for the rows a check failed:

- **[Scalar count](../glossary.md#results)**: the number the check reports,
  such as the result of `SELECT COUNT(*) FROM orders WHERE email IS NULL`. It
  decides whether the check passes. Every row-level check has one, but it is
  not always the number of rows that failed. A `DISTINCT` lowers it, a join
  can raise it, and `COUNT(DISTINCT x)` counts values instead of rows.
- **[Attributed rows](../glossary.md#results)**: the rows of the table that
  failed the check. vowl finds them by matching the check's failed rows back
  to the table. Only [attributable](../design-considerations/checks/check-results.md#counted-checks)
  checks have them.

Every level counts attributed rows:

| Metric                                                               | Counts                                 | Clamped |
| -------------------------------------------------------------------- | -------------------------------------- | ------- |
| `row.count` and `row.pass_rate`, at every level                      | Attributed rows, each row counted once | Yes     |
| `vowl.check.row.scalar_count` and `vowl.check.row.scalar_pass_rate`  | The scalar count of one check          | No      |

!!! warning "Breaking change"

    `vowl.check.row.count` and `vowl.check.row.pass_rate` used to count the
    scalar count. They now count attributed rows, like the other levels. The
    scalar count moved to `vowl.check.row.scalar_count` and
    `vowl.check.row.scalar_pass_rate`. Update any dashboard or alert that reads
    the old meaning.

### What the scalar count is for

The scalar count decides whether the check passes, so it always agrees with
the check's status. Use `vowl.check.row.scalar_count` to see the number the
check itself reported, for example to compare it with the check's limit. Use
`vowl.check.row.count` to see how many rows of the table failed.

The two differ when the check changes the rows it returns. A `DISTINCT`
lowers the scalar count. A join that returns one row twice raises it.

A check's attributed rows are also in the `attributed_rows` column of
`get_row_quality_df(by="check")` and the `vowl.row_quality.attributed_rows`
attribute of its [`vowl.check` span](otel-export.md#traces).

For the details, see
[Attributed rows](../design-considerations/checks/check-results.md#from-query-output-to-row-counts)
and
[Checks can change the rows they return](../design-considerations/checks/how-attributed-rows-work.md#checks-can-change-the-rows-they-return).

### What the row counts add up to

At every level, `PASSED` plus `FAILED` is the number of rows in the table, or
in all tables at run level.

Row counts stay between 0 and the table's rows. Above check level they match
`get_row_quality_df()` and **Passed Rows** in `print_summary()`.

The scalar count can go outside that range. When it is higher than the
table's rows, `PASSED` and `scalar_pass_rate` go negative. vowl does not
clamp them, so the overcount stays visible.

### Which checks have row counts

Only [row-level checks](../design-considerations/checks/check-results.md#counted-checks)
have row counts. A check that is not row-level, such as an average, has check
counts only.

| Check                                  | `scalar_count` | `row.count` at check level                                      |
| -------------------------------------- | -------------- | --------------------------------------------------------------- |
| Failed, attributable                   | Yes            | Its attributed rows                                             |
| Passed                                 | Yes            | `FAILED` is 0, or its attributed rows with `attribute_tolerated_rows=True` |
| Failed, not attributable               | Yes            | None                                                            |

A passed check follows the same rule as [tolerated rows](../design-considerations/checks/check-results.md#tolerated-rows).
By default it adds no rows, so its `FAILED` is 0. With
`attribute_tolerated_rows=True`, its attributed rows count at every level.

At dimension, schema and run level, only attributed rows are counted. A
dimension or schema with no attributable row-level check has no row counts.

### Approximate row counts

The row counts do not say whether they are
[approximate](../design-considerations/checks/counting-mechanisms.md#exact-numbers).
That is on the [traces](otel-export.md#traces). The `vowl.validate` span says
whether the run's row counts are approximate, and each `vowl.check` span says
which check made them so.

## Attributes

Each reading carries attributes that say what it is for. All levels also
carry the [run identity attributes](otel-export.md#run-identity-attributes),
such as `vowl.contract.id`.

| Metric                           | Attributes                                                               |
| -------------------------------- | ------------------------------------------------------------------------ |
| `vowl.check.check.count`         | `status`, `check_name`, `schema_name`, `dimension`, `severity`, `engine` |
| `vowl.check.row.count`           | `status`, `check_name`, `schema_name`, `dimension`, `severity`, `engine` |
| `vowl.check.row.pass_rate`       | `check_name`, `schema_name`, `dimension`, `severity`, `engine`           |
| `vowl.check.row.scalar_count`    | `status`, `check_name`, `schema_name`, `dimension`, `severity`, `engine` |
| `vowl.check.row.scalar_pass_rate`| `check_name`, `schema_name`, `dimension`, `severity`, `engine`           |
| `vowl.check.duration`            | `check_name`, `schema_name`, `dimension`, `severity`, `engine`           |
| `vowl.dimension.check.count`     | `status`, `schema_name`, `dimension`                                     |
| `vowl.dimension.check.pass_rate` | `schema_name`, `dimension`                                               |
| `vowl.dimension.row.count`       | `status`, `schema_name`, `dimension`                                     |
| `vowl.dimension.row.pass_rate`   | `schema_name`, `dimension`                                               |
| `vowl.schema.check.count`        | `status`, `schema_name`                                                  |
| `vowl.schema.check.pass_rate`    | `schema_name`                                                            |
| `vowl.schema.row.count`          | `status`, `schema_name`                                                  |
| `vowl.schema.row.pass_rate`      | `schema_name`                                                            |
| `vowl.run.schema.count`          | `status`                                                                 |
| `vowl.run.check.count`           | `status`                                                                 |
| `vowl.run.check.pass_rate`       | none                                                                     |
| `vowl.run.row.count`             | `status`                                                                 |
| `vowl.run.row.pass_rate`         | none                                                                     |
| `vowl.run.duration`              | none                                                                     |

Every check-level metric carries the same attributes, apart from `status` on
the counts, so you can filter and join them the same way. `severity` is left
out when the contract does not set one. The
[traces and logs](otel-export.md#traces) carry the same numbers under the same
names, without the level.

## The run ID

Every run gets an ID when it runs, a new UUID in `result.run_id`.
`dq_metrics.json` and the OpenTelemetry traces and logs all carry it as
`vowl.run.id`, so you can go from a dashboard to the saved files of the same
run. To use your own ID, such as your orchestrator's run ID, set it before
saving or exporting:

```python
result = validate_data("contract.yaml", df=df)
result.run_id = "nightly-2026-09-30"
```

## Worked example

A contract has two schemas:

- `orders` has 100 rows. `email_required_check` (completeness) fails on 5
  rows and `order_id_unique_check` (consistency) fails on 3 rows. One row has
  both a missing email and a duplicate `order_id`. `order_id_required_check`
  (completeness) passes.
- `refunds` has 20 rows. `refund_positive` (consistency) fails on 2 rows.

vowl also generates a `<column>_column_exists_check` (conformity) for each of
the 3 columns in the contract, and they all pass. So the run has 7 checks: 4
passed and 3 failed. The numbers below are the real output of this run.

To keep it short, the check counts leave out statuses with a count of 0 (for
example `status="ERROR"`), and every reading leaves out the durations and the run identity attributes. The
check lines show only the attributes that tell them apart. Every other reading
the run sends is shown.

```
# Check level: each check on its own
vowl.check.check.count:    1     {status="PASSED", schema_name="orders",  check_name="order_id_column_exists_check"}
vowl.check.check.count:    1     {status="PASSED", schema_name="orders",  check_name="email_column_exists_check"}
vowl.check.check.count:    1     {status="PASSED", schema_name="orders",  check_name="order_id_required_check"}
vowl.check.check.count:    1     {status="FAILED", schema_name="orders",  check_name="email_required_check"}
vowl.check.check.count:    1     {status="FAILED", schema_name="orders",  check_name="order_id_unique_check"}
vowl.check.check.count:    1     {status="PASSED", schema_name="refunds", check_name="amount_column_exists_check"}
vowl.check.check.count:    1     {status="FAILED", schema_name="refunds", check_name="refund_positive"}
vowl.check.row.count:      100   {status="PASSED", check_name="order_id_column_exists_check"}
vowl.check.row.count:      0     {status="FAILED", check_name="order_id_column_exists_check"}
vowl.check.row.count:      100   {status="PASSED", check_name="email_column_exists_check"}
vowl.check.row.count:      0     {status="FAILED", check_name="email_column_exists_check"}
vowl.check.row.count:      100   {status="PASSED", check_name="order_id_required_check"}
vowl.check.row.count:      0     {status="FAILED", check_name="order_id_required_check"}
vowl.check.row.count:      95    {status="PASSED", check_name="email_required_check"}
vowl.check.row.count:      5     {status="FAILED", check_name="email_required_check"}
vowl.check.row.count:      97    {status="PASSED", check_name="order_id_unique_check"}
vowl.check.row.count:      3     {status="FAILED", check_name="order_id_unique_check"}
vowl.check.row.count:      20    {status="PASSED", check_name="amount_column_exists_check"}
vowl.check.row.count:      0     {status="FAILED", check_name="amount_column_exists_check"}
vowl.check.row.count:      18    {status="PASSED", check_name="refund_positive"}
vowl.check.row.count:      2     {status="FAILED", check_name="refund_positive"}
vowl.check.row.pass_rate:  1.0   {check_name="order_id_column_exists_check"}
vowl.check.row.pass_rate:  1.0   {check_name="email_column_exists_check"}
vowl.check.row.pass_rate:  1.0   {check_name="order_id_required_check"}
vowl.check.row.pass_rate:  0.95  {check_name="email_required_check"}
vowl.check.row.pass_rate:  0.97  {check_name="order_id_unique_check"}
vowl.check.row.pass_rate:  1.0   {check_name="amount_column_exists_check"}
vowl.check.row.pass_rate:  0.9   {check_name="refund_positive"}
vowl.check.row.scalar_count:      100   {status="PASSED", check_name="order_id_column_exists_check"}
vowl.check.row.scalar_count:      0     {status="FAILED", check_name="order_id_column_exists_check"}
vowl.check.row.scalar_count:      100   {status="PASSED", check_name="email_column_exists_check"}
vowl.check.row.scalar_count:      0     {status="FAILED", check_name="email_column_exists_check"}
vowl.check.row.scalar_count:      100   {status="PASSED", check_name="order_id_required_check"}
vowl.check.row.scalar_count:      0     {status="FAILED", check_name="order_id_required_check"}
vowl.check.row.scalar_count:      95    {status="PASSED", check_name="email_required_check"}
vowl.check.row.scalar_count:      5     {status="FAILED", check_name="email_required_check"}
vowl.check.row.scalar_count:      97    {status="PASSED", check_name="order_id_unique_check"}
vowl.check.row.scalar_count:      3     {status="FAILED", check_name="order_id_unique_check"}
vowl.check.row.scalar_count:      20    {status="PASSED", check_name="amount_column_exists_check"}
vowl.check.row.scalar_count:      0     {status="FAILED", check_name="amount_column_exists_check"}
vowl.check.row.scalar_count:      18    {status="PASSED", check_name="refund_positive"}
vowl.check.row.scalar_count:      2     {status="FAILED", check_name="refund_positive"}
vowl.check.row.scalar_pass_rate:  1.0   {check_name="order_id_column_exists_check"}
vowl.check.row.scalar_pass_rate:  1.0   {check_name="email_column_exists_check"}
vowl.check.row.scalar_pass_rate:  1.0   {check_name="order_id_required_check"}
vowl.check.row.scalar_pass_rate:  0.95  {check_name="email_required_check"}
vowl.check.row.scalar_pass_rate:  0.97  {check_name="order_id_unique_check"}
vowl.check.row.scalar_pass_rate:  1.0   {check_name="amount_column_exists_check"}
vowl.check.row.scalar_pass_rate:  0.9   {check_name="refund_positive"}

# Dimension level: each row once per dimension
vowl.dimension.check.count:      2     {status="PASSED", schema_name="orders",  dimension="conformity"}
vowl.dimension.check.count:      1     {status="PASSED", schema_name="orders",  dimension="completeness"}
vowl.dimension.check.count:      1     {status="FAILED", schema_name="orders",  dimension="completeness"}
vowl.dimension.check.count:      1     {status="FAILED", schema_name="orders",  dimension="consistency"}
vowl.dimension.check.count:      1     {status="PASSED", schema_name="refunds", dimension="conformity"}
vowl.dimension.check.count:      1     {status="FAILED", schema_name="refunds", dimension="consistency"}
vowl.dimension.check.pass_rate:  1.0   {schema_name="orders",  dimension="conformity"}
vowl.dimension.check.pass_rate:  0.5   {schema_name="orders",  dimension="completeness"}
vowl.dimension.check.pass_rate:  0.0   {schema_name="orders",  dimension="consistency"}
vowl.dimension.check.pass_rate:  1.0   {schema_name="refunds", dimension="conformity"}
vowl.dimension.check.pass_rate:  0.0   {schema_name="refunds", dimension="consistency"}
vowl.dimension.row.count:        100   {status="PASSED", schema_name="orders",  dimension="conformity"}
vowl.dimension.row.count:        0     {status="FAILED", schema_name="orders",  dimension="conformity"}
vowl.dimension.row.count:        95    {status="PASSED", schema_name="orders",  dimension="completeness"}
vowl.dimension.row.count:        5     {status="FAILED", schema_name="orders",  dimension="completeness"}
vowl.dimension.row.count:        97    {status="PASSED", schema_name="orders",  dimension="consistency"}
vowl.dimension.row.count:        3     {status="FAILED", schema_name="orders",  dimension="consistency"}
vowl.dimension.row.count:        20    {status="PASSED", schema_name="refunds", dimension="conformity"}
vowl.dimension.row.count:        0     {status="FAILED", schema_name="refunds", dimension="conformity"}
vowl.dimension.row.count:        18    {status="PASSED", schema_name="refunds", dimension="consistency"}
vowl.dimension.row.count:        2     {status="FAILED", schema_name="refunds", dimension="consistency"}
vowl.dimension.row.pass_rate:    1.0   {schema_name="orders",  dimension="conformity"}
vowl.dimension.row.pass_rate:    0.95  {schema_name="orders",  dimension="completeness"}
vowl.dimension.row.pass_rate:    0.97  {schema_name="orders",  dimension="consistency"}
vowl.dimension.row.pass_rate:    1.0   {schema_name="refunds", dimension="conformity"}
vowl.dimension.row.pass_rate:    0.9   {schema_name="refunds", dimension="consistency"}

# Schema level: each row once
# 7 rows of orders failed, not 5 + 3 = 8, because 1 row failed in both dimensions
vowl.schema.check.count:      3      {status="PASSED", schema_name="orders"}
vowl.schema.check.count:      2      {status="FAILED", schema_name="orders"}
vowl.schema.check.count:      1      {status="PASSED", schema_name="refunds"}
vowl.schema.check.count:      1      {status="FAILED", schema_name="refunds"}
vowl.schema.check.pass_rate:  0.6    {schema_name="orders"}
vowl.schema.check.pass_rate:  0.5    {schema_name="refunds"}
vowl.schema.row.count:        93     {status="PASSED", schema_name="orders"}
vowl.schema.row.count:        7      {status="FAILED", schema_name="orders"}
vowl.schema.row.count:        18     {status="PASSED", schema_name="refunds"}
vowl.schema.row.count:        2      {status="FAILED", schema_name="refunds"}
vowl.schema.row.pass_rate:    0.93   {schema_name="orders"}
vowl.schema.row.pass_rate:    0.9    {schema_name="refunds"}

# Run level: the schemas added up
# 9 rows failed = 7 + 2, because the two tables hold different rows
vowl.run.schema.count:      2      {status="FAILED"}
vowl.run.check.count:       4      {status="PASSED"}
vowl.run.check.count:       3      {status="FAILED"}
vowl.run.check.pass_rate:   0.571  {}
vowl.run.row.count:         111    {status="PASSED"}
vowl.run.row.count:         9      {status="FAILED"}
vowl.run.row.pass_rate:     0.925  {}
```

Every level has 7 checks in total, because check counts add up. Rows do not
add up until the run level: 5 + 3 failed rows at dimension level become 7 at
schema level for `orders`, and the run level adds `orders` and `refunds`.
[Exporting to dq_metrics.json](json-export.md#worked-example) shows the same
run as it appears in the file.
