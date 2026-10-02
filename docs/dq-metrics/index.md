---
description: The DQ metrics vowl reports for every run, at check, dimension, schema and run level.
---

# DQ Metrics

!!! tip "Interactive Demo"

    The [DQ Metrics notebook](https://github.com/govtech-data-practice/vowl/blob/main/examples/6_dq_metrics/dq_metrics.ipynb) reads one run's metrics at every level, loads `dq_metrics.json` with pandas and exports the same run to OpenTelemetry.

After every run, vowl works out a set of numbers that describe the quality of
your data, such as "how many checks failed" or "what share of rows passed".
These are the **DQ metrics**. You can get them in two ways:

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
- **measure** is `count` or `pass_rate`. Timings are `duration`.

So `vowl.schema.row.pass_rate` is "at schema level, the share of rows that
passed", and `vowl.check.check.count` is "at check level, the number of
checks". Every name follows the pattern, so you can build one from its parts.

The `prefix` option of `export_otel` replaces `vowl` at the start of every name.

## All metrics

| Level     | Check count                  | Check pass rate                  | Row count                  | Row pass rate                  | Other                                        |
| --------- | ---------------------------- | -------------------------------- | -------------------------- | ------------------------------ | -------------------------------------------- |
| check     | `vowl.check.check.count`     |                                  | `vowl.check.row.count`     | `vowl.check.row.pass_rate`     | `vowl.check.duration`                        |
| dimension | `vowl.dimension.check.count` | `vowl.dimension.check.pass_rate` | `vowl.dimension.row.count` | `vowl.dimension.row.pass_rate` |                                              |
| schema    | `vowl.schema.check.count`    | `vowl.schema.check.pass_rate`    | `vowl.schema.row.count`    | `vowl.schema.row.pass_rate`    |                                              |
| run       | `vowl.run.check.count`       | `vowl.run.check.pass_rate`       | `vowl.run.row.count`       | `vowl.run.row.pass_rate`       | `vowl.run.schema.count`, `vowl.run.duration` |

There is no check pass rate at check level. For one check it would always be
0 or 1, which `vowl.check.check.count` already says.

## Statuses

Counts carry a `status` attribute that says which part of the total a reading
is:

- **Checks** are `PASSED`, `FAILED` or `ERROR`. `FAILED` means the check ran
  and found bad data. `ERROR` means the check could not run, for example
  because its query names a column that does not exist.
- **Rows** are `PASSED` or `FAILED`. A row is `FAILED` at a level when it
  failed at least one counted check at that level.
- **Schemas** (`vowl.run.schema.count`) are `FAILED` when any of their checks
  failed, else `ERROR` when any check could not run, else `PASSED`. A known
  data problem outranks a check that could not say.

Every status is always sent, zeros included. A clean run sends `FAILED` as 0
instead of sending nothing, so a dashboard never keeps showing the failures of
an earlier run. Add up the statuses of one reading to get the total.

## Pass rates

A pass rate is a share from 0 to 1.

- A **check pass rate** is passed checks over all checks. Checks that could
  not run (`ERROR`) count against it, because they did not pass.
- A **row pass rate** is passed rows over all rows in the table.

A pass rate is left out when there is nothing to divide by, for example a
schema with no checks or an empty table. A missing rate means "no rate", not
100%.

## Which numbers add up

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

The same idea decides the metric **type**:

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

## How failed rows are counted

The level decides how often one failing row is counted:

- **`vowl.check.row.count`** counts each check on its own. A row that fails
  two checks is `FAILED` for each check.
- **`vowl.dimension.row.count`** counts each row once per dimension. A row
  that fails two completeness checks counts once for completeness. A row that
  fails one completeness check and one consistency check counts once for
  each.
- **`vowl.schema.row.count`** counts each row once, however many checks it
  fails.
- **`vowl.run.row.count`** adds up the schemas.

At every level, `PASSED` plus `FAILED` is the number of rows in the table, or
in all tables at run level. Every copy of a duplicated row counts. With
`ValidationConfig(row_issue_scope="all_violations")`, rows caught by a check
that passed within its tolerance count as `FAILED` too.

Row counts are only sent for checks that look at rows one by one, such as
"`email` must not be empty". A check on the whole table, such as `rowCount`,
has no failing rows to count. Checks that could not run are skipped too. The
dimension, schema and run row counts are left out when no check in them was
counted. [Which Checks Contribute Failed Rows](../design-considerations/failed-rows/which-checks.md)
has the full rules.

The dimension, schema and run row counts are the same numbers as
`result.get_row_quality_df()` and **Passed Rows** in `print_summary()`. They
carry a `vowl.row_quality.exact` attribute, which is `false` when a number
could be off, for example because a check that would have been counted ended
in `ERROR`. At run level it is `true` only when every schema is exact. See
[Exact numbers](../design-considerations/failed-rows/levels.md#exact-numbers).

The check row count is the check's own count, not merged with other checks.
So it can differ from the check's `failed_rows` in
`get_row_quality_df(by="check")`. For example, a `DISTINCT` check that returns
one row for three identical rows shows 1 here and 3 there.
[Failed Rows at Each Level](../design-considerations/failed-rows/levels.md) walks through every
step behind the dimension, schema and run numbers.

## Attributes

Each reading carries attributes that say what it is for. All levels also
carry the [run identity attributes](otel-export.md#run-identity-attributes),
such as `vowl.contract.id`.

| Metric                           | Attributes                                                               |
| -------------------------------- | ------------------------------------------------------------------------ |
| `vowl.check.check.count`         | `status`, `check_name`, `schema_name`, `dimension`, `severity`, `engine` |
| `vowl.check.row.count`           | `status`, `check_name`, `schema_name`, `dimension`, `severity`, `engine` |
| `vowl.check.row.pass_rate`       | `check_name`, `schema_name`, `dimension`, `severity`, `engine`           |
| `vowl.check.duration`            | `check_name`, `schema_name`, `dimension`, `severity`, `engine`           |
| `vowl.dimension.check.count`     | `status`, `schema_name`, `dimension`                                     |
| `vowl.dimension.check.pass_rate` | `schema_name`, `dimension`                                               |
| `vowl.dimension.row.count`       | `status`, `schema_name`, `dimension`, `vowl.row_quality.exact`           |
| `vowl.dimension.row.pass_rate`   | `schema_name`, `dimension`, `vowl.row_quality.exact`                     |
| `vowl.schema.check.count`        | `status`, `schema_name`                                                  |
| `vowl.schema.check.pass_rate`    | `schema_name`                                                            |
| `vowl.schema.row.count`          | `status`, `schema_name`, `vowl.row_quality.exact`                        |
| `vowl.schema.row.pass_rate`      | `schema_name`, `vowl.row_quality.exact`                                  |
| `vowl.run.schema.count`          | `status`                                                                 |
| `vowl.run.check.count`           | `status`                                                                 |
| `vowl.run.check.pass_rate`       | none                                                                     |
| `vowl.run.row.count`             | `status`, `vowl.row_quality.exact`                                       |
| `vowl.run.row.pass_rate`         | `vowl.row_quality.exact`                                                 |
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
example `status="ERROR"`), and every reading leaves out
`vowl.row_quality.exact`, the durations and the run identity attributes. The
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
# orders has 7 failed rows, not 5 + 3 = 8, because 1 row failed in both dimensions
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
# 9 failed rows = 7 + 2, because the two tables hold different rows
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
