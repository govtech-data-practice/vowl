---
title: Handling of Failed Rows
---

# Handling of Failed Rows

This page explains what vowl does with the rows that fail your checks: how it
counts them, and how it marks them on your tables. You don't need to know SQL
to follow it.

## Words used on this page

Each word below means one thing, and this page uses no other word for it. [Key terms](key-terms.md) has the words used across the
docs.

| Word                | Meaning                                                                                                                                                 |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **check**           | One test from your contract, such as "price must be positive".                                                                                          |
| **failed rows**     | The rows a check caught.                                                                                                                                |
| **counted check**   | A check whose failed rows count toward the row counts. Most checks are counted. [Which checks are counted](#which-checks-are-counted) lists the others. |
| **row counts**      | How many rows of a table failed at least one counted check, and how many passed them all.                                                               |
| **mark**            | To write a check's name into a row's `check_info` column.                                                                                               |
| **annotated table** | Your table with one extra column, `check_info`. Every row is kept.                                                                                      |
| **clean row**       | A row in the annotated table that no check marked. Its `check_info` is empty.                                                                           |
| **residue**         | Failed rows that vowl could not mark on a table. vowl keeps them in a separate list.                                                                    |
| **summary**         | The report printed by `print_summary()`, and saved as `summary.json`.                                                                                   |

## The short version

1. vowl runs each check. The check counts the rows that break it. If the count
   is too high, the check fails.
2. vowl counts the rows of each table that failed at least one counted check.
   A row that fails two checks counts once. Every copy of a duplicated row
   counts.
3. vowl finds each failed row in your table and marks it. A row that fails two
   checks is marked with both.
4. If vowl can't find a check's failed rows in your table, it keeps them as a
   residue.

The row counts are in the summary as **Passed Rows**, and in
`result.get_row_quality_df()`. `result.get_annotated_output()` returns the
annotated tables and the residues. The row counts and the annotated tables
use the same checks, so the number of failed rows usually equals the number of
marked rows. [Counting and marking compared](#counting-and-marking-compared)
explains how the two work and the few cases where they differ.

## A small example

Here is an `orders` table and three checks:

- `price_must_be_positive`: no order has a price of zero or less.
- `quantity_is_filled`: every order has a quantity.
- `average_price_in_range`: the average price is between 1 and 5.

| order_id | item  | price | quantity |
| -------- | ----- | ----- | -------- |
| 1        | apple | 1.50  | 3        |
| 2        | bread | -2.00 | 1        |
| 3        | milk  | 0.00  |          |
| 4        | eggs  | 3.20  | 2        |
| 2        | bread | -2.00 | 1        |

The last row is an exact copy of the second.

```python
from vowl import validate_data

result = validate_data("orders.yaml", df=df)
result.get_row_quality_df().to_pandas()
```

Some of its columns:

| schema_name | total_rows | failed_rows | passed_rows | pass_rate | exact |
| ----------- | ---------- | ----------- | ----------- | --------- | ----- |
| orders      | 5          | 3           | 2           | 0.4       | True  |

The summary shows the same numbers:

```text
     Overall:
       Passed Rows:            2 / 5 (40.0%)
```

This is the annotated table:

```python
output = result.get_annotated_output()
annotated = output["annotated"]["orders"].to_pandas()
```

| order_id | item  | price | quantity | check_info                                                                         |
| -------- | ----- | ----- | -------- | ---------------------------------------------------------------------------------- |
| 1        | apple | 1.50  | 3        |                                                                                    |
| 2        | bread | -2.00 | 1        | `[{"check_name": "price_must_be_positive"}]`                                       |
| 3        | milk  | 0.00  |          | `[{"check_name": "price_must_be_positive"}, {"check_name": "quantity_is_filled"}]` |
| 4        | eggs  | 3.20  | 2        |                                                                                    |
| 2        | bread | -2.00 | 1        | `[{"check_name": "price_must_be_positive"}]`                                       |

- The milk row failed two checks. It is marked with both, and counts once.
- Both copies of the bread row are marked, and both count.
- `average_price_in_range` failed too (the average is 0.14), but it marks no
  row and is not counted. An average is one number for the whole table, so it
  has no failed rows.
- 3 rows failed and 2 passed, in the row counts and in the annotated table.

To keep only the clean rows:

```python
clean = annotated[annotated["check_info"].isna()].drop(columns=["check_info"])
# 2 clean rows: apple and eggs
```

## How vowl counts failed rows

### Which checks are counted

A check is counted when all of these hold:

| Rule                                      | Why                                                                  | Example of a check that is not counted                         |
| ----------------------------------------- | -------------------------------------------------------------------- | -------------------------------------------------------------- |
| It points at rows                         | An average, a sum or a `rowCount` is one number for the whole table. | `average_price_in_range`                                       |
| Its failed rows are the bad rows          | Some checks count good rows, so the rows they catch are fine.        | "at least 100 orders are paid" (`mustBeGreaterThan: 100`)      |
| It ran                                    | A check that hit an error has no failed rows.                        | A check with a typo in its SQL                                 |
| Its failed rows can be found in the table | vowl needs to recognise the same row across checks.                  | A check that returns only the town names with an unusual price |

The second rule looks at the check's operator. The rows a check catches are
bad rows when the check sets an upper limit on them:

| Operator                                                                                            | Counted                                              |
| --------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `mustBeLessThan`, `mustBeLessOrEqualTo`                                                             | Yes                                                  |
| `mustBe: 0`                                                                                         | Yes                                                  |
| `mustBeBetween: [0, n]`                                                                             | Yes                                                  |
| `mustBeGreaterThan`, `mustBeGreaterOrEqualTo`                                                       | No. The check counts good rows.                      |
| `mustBe: n` with n above 0, `mustNotBe`, `mustBeBetween: [a, b]` with a above 0, `mustNotBeBetween` | No. No single row is at fault when the total is off. |

A check that is not counted still passes or fails as usual. It just adds no
rows to the row counts, and marks no rows on the annotated table.

For the last rule, the failed rows must have exactly the same columns as your
table. If the table has a declared primary key (`primaryKey: true` in the
contract), no value of it appears twice, and the data source is DuckDB,
SQLite, Spark or Postgres, failed rows that hold the primary key columns are
enough.

### Checks that pass with some failed rows

A check can pass and still catch rows. For example, "fewer than 100 orders
have no quantity" (`mustBeLessThan: 100`) passes with 50 such orders. Whether
those 50 rows count as failed depends on what you report, so it is a setting:

```python
from vowl import ValidationConfig

config = ValidationConfig(row_issue_scope="all_violations")
```

| `row_issue_scope`           | Rows counted as failed                                                                                                |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `"failed_checks"` (default) | Only rows caught by checks that failed. Rows caught by a check that passed are shown separately, as `tolerated_rows`. |
| `"all_violations"`          | Every row that any counted check caught, whether the check passed or failed.                                          |

The setting applies to the annotated tables too. Under `"all_violations"`,
rows caught by a check that passed are marked, and the check's entry in
`check_info` carries `"tolerated": true`.

### Where vowl counts

vowl counts failed rows in the fastest way that gives the right number:

| Route          | Used for                                                                                        | How it works                                                              |
| -------------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `pushdown`     | Most checks on one data source                                                                  | The data source counts the rows itself. No rows are sent to vowl.         |
| `table_match`  | Checks on one data source whose query is not a plain filter, such as a query with `DISTINCT`    | The data source counts the table rows that match the check's failed rows. |
| `fetched_rows` | Checks that read tables from different data sources, and data sources that can't count for vowl | vowl gets the check's failed rows and counts them itself.                 |

`result.get_row_quality_df(by="check")` shows the route each check took, and
why a check was not counted.

### Is the number exact?

Every row of `get_row_quality_df()` has an `exact` column. It is `False` when the number could be
off:

- A check that would have been counted hit an error, so it could be hiding bad
  rows.
- A check's rows were fetched, and `max_failed_rows` cut them short.
- A check's rows were fetched, and its query is not a plain filter of the table
  (for example it uses `DISTINCT` or a join), so its rows may hold fewer or
  more copies than the table has.
- The data source could not run a check's rows as part of the count, so vowl
  left the check out.
- The data source is not one vowl has tested for counting. DuckDB, SQLite,
  Spark and Postgres are tested. Databricks uses the Spark method but has not been tested on
  its own.
- The contract and the data source don't say what columns the table has, so
  vowl can't tell whether two checks caught the same row.
- The table's total row count is lower than the rows a check found, so the count
  itself is off.

`get_row_quality_df(by="check")` has an `exact` column too, which shows which
check made a number not exact.

The summary adds **(approx.)** after a number that is not exact.

### The three views

```python
result.get_row_quality_df(by="schema")     # one row per table
result.get_row_quality_df(by="dimension")  # one row per table and dimension
result.get_row_quality_df(by="check")      # one row per check
```

The table and dimension views have these columns:

| Column                                 | Meaning                                                                                                                        |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `total_rows`                           | Rows in the table, after your filter conditions.                                                                               |
| `failed_rows`                          | Rows that failed at least one counted check.                                                                                   |
| `tolerated_rows`                       | Rows caught only by checks that passed. See [Checks that pass with some failed rows](#checks-that-pass-with-some-failed-rows). |
| `passed_rows`                          | `total_rows` minus `failed_rows`.                                                                                              |
| `pass_rate`                            | `passed_rows` divided by `total_rows`, from 0 to 1.                                                                            |
| `exact`                                | Whether the numbers are exact.                                                                                                 |
| `checks_counted`, `checks_not_counted` | How many checks were counted, and how many were not.                                                                           |

A missing value means there is no number to give. A dimension where no check
was counted has no pass rate, and neither has an empty table. vowl shows N/A
for them rather than 100%.

## Where each failed check ends up

Every failed check ends up in exactly one of three places.

| Where                                  | Which checks                                                                                                                                                                                                                                      | Example                                                            |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| **Marked on the annotated table**      | Counted checks whose failed rows have exactly the same columns as your table. Most checks are like this, including many that vowl creates for you, such as unique-value and foreign-key checks.                                                   | `price_must_be_positive`                                           |
| **A residue**, in `output["residues"]` | Checks whose failed rows have different columns from your table, either fewer columns or extra columns from a second table. Each residue is stored under the name `"<table>::<check>"`.                                                           | A check that returns only the names of towns with an unusual price |
| **The summary only**                   | Checks that produce one number instead of rows, such as an average, a maximum or `rowCount`. Checks whose failed rows are good rows (see [Which checks are counted](#which-checks-are-counted)). Also checks that hit an error and could not run. | `average_price_in_range`                                           |

!!! tip "Read the summary for the final result"

    A check in the last group marks no row and creates no residue. It appears
    only in the summary and in `result.get_check_results_df()`. Looking at the
    annotated tables alone, you would miss it.

A table can have marked rows and residues at the same time. A check is never in
both places.

If vowl cannot get your whole table (for example, because no connection was
given for it), it logs a warning and makes no annotated table for it. That
table's failed rows become residues instead.

## How vowl finds a failed row in your table

This section is about marking rows on the annotated table. Counting compares
rows in a different place, see
[Counting and marking compared](#counting-and-marking-compared).

To mark a row, vowl compares values. A row in your table matches a failed row
when every column holds the same value. This means:

- **Identical rows are all marked.** If a row appears twice and one copy fails a
  check, the other copy holds the same data, so it fails the same check. That
  is why both bread rows are marked.
- **Empty values match each other.** An empty value (`NULL`) matches another
  empty value.
- **"Not a number" values match each other.** A `NaN` matches another `NaN`.
- **`-0.0` and `0.0` are different values.** Some databases treat them as
  different, so a check can catch one and not the other. vowl marks only the one
  the check caught.
- **Lists and nested values are compared item by item.**
- **Times are compared to the nanosecond.**

vowl can't get tables that hold certain DuckDB column types: `INTERVAL`, `BIT`
and `UNION`. For those tables vowl logs "Annotated export failed", makes no
annotated table, and their failed rows become residues. The row counts are
not affected when the data source counts them.

## Counting and marking compared

vowl works out failed rows twice, for two different jobs:

- **Counting** gives the row counts: **Passed Rows**, `get_row_quality_df()`
  and the OpenTelemetry row metrics. It runs inside your data source where it
  can, and only the counts come back.
- **Marking** gives the annotated table. vowl downloads your whole table and the
  failed rows of each check that marks rows, and matches them on your machine.

Both start from the same list of counted checks, and both count every copy of a
duplicated row. So in most runs `failed_rows` equals the number of marked rows,
as in the [small example](#a-small-example). vowl never uses the annotated
table to work out the row counts.

### How the two work

|                             | Counting (the row counts)                                                                                                                                                                                                                                         | Marking (the annotated table)                                                                                    |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Where it runs               | In your data source, for the `pushdown` and `table_match` routes. On your machine, for `fetched_rows`.                                                                                                                                                            | On your machine.                                                                                                 |
| What vowl downloads         | Counts. For a `fetched_rows` check, its failed rows.                                                                                                                                                                                                              | Your whole table, plus the failed rows of every check that marks rows.                                           |
| How it tells two rows apart | In DuckDB, SQLite, Spark and Postgres, the data source compares the stored values exactly, so `a` and `A`, or `-0.0` and `0.0`, stay apart. In other data sources it uses the data source's own comparison, see below. For fetched rows, vowl's value comparison. | vowl's value comparison, described in [How vowl finds a failed row](#how-vowl-finds-a-failed-row-in-your-table). |
| Which checks it can use     | Checks whose failed rows have the table's columns, or its primary key.                                                                                                                                                                                            | The same checks as counting.                                                                                     |
| Effect of `max_failed_rows` | None for `pushdown` and `table_match`. A `fetched_rows` check that was cut short makes the number not exact.                                                                                                                                                      | Stops with an error when a check that marks rows was cut short.                                                  |
| Cost                        | Grows with the number of checks, not with the size of the table.                                                                                                                                                                                                  | Downloads the whole table every time.                                                                            |

A check on the `table_match` route is counted the way marking works: the data
source counts the table rows that match the check's failed rows. So a check
with `DISTINCT`, which returns one row for three identical copies, counts three
and marks three.

### When the two differ

| Situation                                                                                                                                                     | Row counts                                                                                                 | Annotated table                                               |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| A check had more failed rows than `max_failed_rows`                                                                                                           | Exact, when the data source counts. Marked not exact when the check's rows were fetched.                   | `get_annotated_output()` stops with an error.                 |
| The table has a column vowl can't download (`INTERVAL`, `BIT`, `UNION`)                                                                                       | Counted as usual.                                                                                          | No annotated table. The failed rows become residues.          |
| The data source is not DuckDB, SQLite, Spark or Postgres, and it treats different values as equal, for example `a` and `A` under a case-insensitive collation | Two rows caught by different checks can count as one. The number is marked not exact.                      | Both rows are marked.                                         |
| A check's query is not a plain filter, and its rows had to be fetched, for example `DISTINCT` on a data source that can't count for vowl                      | Counted from the fetched rows, so three identical copies can count as one. The number is marked not exact. | All three copies are marked, since they hold the same values. |
| The table changes while vowl reads it                                                                                                                         | Counting reads the table at one moment, or at a few moments for large contracts.                           | Marking reads it again later, so it can see different rows.   |

In the first three rows of the table, the row counts are the right ones and
`exact` stays `True`. In the next two, `exact` is `False` on the number. vowl
can't detect the last one, which only matters for a table that is written to
during the run.

### Which one to use

- For how many rows have issues, and for pass rates, use the row counts. They
  are cheap on large tables and don't depend on `max_failed_rows`.
- For which rows have issues, use the annotated table, or the residues for
  checks that can't be marked. On a large table, `get_output_dfs()` gives each
  check's failed rows without downloading the whole table.

## Why the numbers don't always agree

vowl shows failed rows in three places. In the example:

| Where you look                                             | What it counts                                                                                          | In the example              |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- | --------------------------- |
| A check's count, shown as `actual` in the summary          | Rows that one check caught, every copy included                                                         | `price_must_be_positive`: 3 |
| **Passed Rows** in the summary, and `get_row_quality_df()` | Rows that failed no counted check. A row that fails two checks counts once. Every copy of a row counts. | 2 of 5                      |
| The annotated table                                        | Clean rows                                                                                              | 2 clean                     |

The row counts and the annotated table agree. The checks' own counts add up to
more than the failed rows (3 + 1 = 4 against 3), because the milk row failed two
checks.

The numbers per dimension overlap in the same way. A row that fails a
completeness check and a conformity check counts once in each dimension, but once
in total. So the dimensions can add up to more than the table.

Some more details about the summary:

- **Passed Rows** is under **Overall**, and includes checks that read more than
  one table when their failed rows can be found in the table.
- **Non-unique Failed Rows** is under **Multi Table**, for checks that look at
  more than one table. It adds up each of those checks' counts, so a row caught
  by two such checks counts twice.
- The row counts and the annotated table agree in most runs.
  [When the two differ](#when-the-two-differ) lists the exceptions.

## Capping failed rows

By default vowl gets every failed row. On very large tables you can cap how many
it gets for each check:

```python
from vowl import ValidationConfig

config = ValidationConfig(max_failed_rows=100)
result = validate_data("orders.yaml", df=df, config=config)
```

With a cap:

- **Pass or fail does not change.** It comes from each check's count, which is
  never capped.
- **The row counts do not change** for checks the data source counts itself
  (the `pushdown` and `table_match` routes). A check whose rows vowl had to get
  (`fetched_rows`) can be cut short. Its numbers are then marked not exact.
- **`get_annotated_output()` stops with an error** if a check that marks rows
  had more failed rows than the cap. Otherwise the rows past the cap would look
  clean. Remove the cap (`max_failed_rows=-1`, the default) to get annotated
  tables.

`max_rows_for_statistics` no longer caps the row counts. vowl always counts
the whole table.

## Writing a cross-table check that marks rows

A cross-table check reads two tables, for example "every payroll row has an
employee in the master list". It marks rows like any other check, as long as
its failed rows hold only the columns of the schema the check belongs to.

Each SQL check runs two queries built from the one you write: a count that
decides pass or fail, and a second query that fetches the failed rows. vowl
writes the second one by swapping the outer `SELECT COUNT(*)` for `SELECT *`
(see [the two-query model](design-considerations.md#the-two-query-model)). So
the outer `FROM` decides which columns the failed rows have.

This check marks rows. The inner query returns only payroll columns, so each
failed row matches a row of `demo_employee_payroll`:

```yaml
quality:
  - type: sql
    name: employee_id_exists_in_master_list
    query: >-
      SELECT COUNT(*)
      FROM (
        SELECT payroll.*
        FROM demo_employee_payroll payroll
        LEFT JOIN demo_employee_list ref
          ON payroll.employee_id = ref.employee_id
        WHERE ref.employee_id IS NULL
      ) AS orphaned_payroll
    mustBe: 0
```

This one becomes a residue. A bare join returns the columns of both tables, so
its failed rows don't match either table:

```yaml
quality:
  - type: sql
    name: employee_id_exists_in_master_list
    query: >-
      SELECT COUNT(*) FROM demo_employee_payroll p
      LEFT JOIN demo_employee_list e ON p.employee_id = e.employee_id
      WHERE e.employee_id IS NULL
    mustBe: 0
```

Its failed rows are kept under
`"demo_employee_payroll::employee_id_exists_in_master_list"`.

!!! note "vowl goes by the columns, not by what the check means"

    A check is only ever matched against its own schema's table, and its failed
    rows must have exactly that table's columns. A query that returns rows of
    the right shape for the wrong reason will still mark them, the same as a
    single-table SQL check would.

## What the annotated output holds

```python
output = result.get_annotated_output()
output["annotated"]   # {"<schema>": your table + check_info}
output["residues"]    # {"<schema>::<check_name>": failed rows + check_info + tables_in_query}
```

<!-- prettier-ignore-start -->
<!-- Zensical needs the table indented 4 spaces to stay inside the list item. -->

- **Every schema gets an annotated table**, even when nothing failed. Its
  `check_info` column is then empty on every row.
- **`check_info` is a JSON list** with one item per check the row failed. Pick
  how much each item holds with `check_info=` (or `annotated_check_info` in
  `ValidationConfig`):

    | `check_info`        | Each item holds                                                  |
    | ------------------- | ---------------------------------------------------------------- |
    | `"names"` (default) | `check_name`                                                     |
    | `"summary"`         | `check_name`, `dimension`, `tags` and `target` (the column)      |
    | `"full"`            | The whole check definition, plus `check_name` and `target`       |

- **Each residue holds one check.** It has that check's failed rows, with
  duplicates removed, the same `check_info` column (with one item), and a
  `tables_in_query` column naming the tables the check read.
- **Unique-value, primary key and `duplicateValues` checks mark rows.** vowl
  fetches every row whose value appears more than once (and, for a primary
  key, every row where it is empty), so the count and the marked rows agree.
  The exception is `duplicateValues` with `unit: percent`, whose result is a
  share and not a number of rows.

<!-- prettier-ignore-end -->

## Other ways to see failed rows

| Method                                 | What you get                                                                           |
| -------------------------------------- | -------------------------------------------------------------------------------------- |
| `result.get_row_quality_df(by=...)`    | The row counts per table, per dimension or per check.                                  |
| `result.show_failed_rows(max_rows=5)`  | Prints a few failed rows for each failed check. `max_rows=-1` prints them all.         |
| `result.get_output_dfs()`              | Each check's failed rows as a separate table, stored under `"<schema>::<check_name>"`. |
| `result.get_annotated_output()`        | The annotated tables and residues.                                                     |
| `result.save(output_mode="annotated")` | Saves the annotated tables, residues and `summary.json` as files.                      |

`output_mode="failed_rows"` and `get_consolidated_output_dfs()` are the older
way of saving failed rows and will be removed. They group the failed rows of
several checks together, with a comma-separated `check_ids` column, so they
look different from residues. Use `"annotated"` instead.
