---
title: Failed Rows at Each Level
description: >-
  How vowl turns each check's failed rows into row counts per check,
  dimension, schema and run, and how it annotates your table with them.
---

# Failed Rows at Each Level

Each counted check returns its own failed rows. vowl reports failed rows at
four levels, and does two things with them:

- **Counting** gives the row counts: how many rows of each table failed, per
  dimension and per schema. A row that fails two checks counts once.
- **Annotating** gives the annotated table: your table with each failed row
  annotated with the checks it failed.

This page uses the [example](queries.md#the-example-used-in-this-section)
from the first page.

## The four levels

| Level         | What it counts                                                                   | In the example                                                                | Where to see it                                           |
| ------------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | --------------------------------------------------------- |
| **Check**     | The rows one check caught, every copy included. Not merged with other checks.    | `price_must_be_positive`: 3, `quantity_is_filled`: 1, `order_id_is_unique`: 2 | `actual` in the summary, `get_row_quality_df(by="check")` |
| **Dimension** | The rows that failed at least one counted check of the dimension. Each row once. | conformity: 3, completeness: 1, uniqueness: 2                                 | `get_row_quality_df(by="dimension")`                      |
| **Schema**    | The rows of the table that failed at least one counted check. Each row once.     | orders: 3 of 5 failed, 2 passed                                               | **Passed Rows** in the summary, `get_row_quality_df()`    |
| **Run**       | The schema numbers added up. Different tables hold different rows.               | 3 of 5                                                                        | the run-level [DQ metrics](../../dq-metrics/index.md)     |

Two things to note:

- **Check numbers add up to more than the schema number.** The milk row failed
  two checks, so the checks add up to 3 + 1 + 2 = 6, while the table has 3
  failed rows.
- **Dimension numbers overlap the same way.** The milk row failed a
  conformity check and a completeness check. It counts once in each
  dimension, and once for the table.

The [DQ metrics](../../dq-metrics/index.md#how-failed-rows-are-counted) send
the same four levels, under the names listed there. The dimension, schema
and run numbers are worked out once and cached on the result, so the summary,
`get_row_quality_df()` and the DQ metrics always agree.

!!! note "The check level uses each check's own count"

    The check level comes from each check's count query, not from the merge
    below. For a check that uses `DISTINCT` and returns one row for three
    copies, the check-level DQ metric shows 1, while
    `get_row_quality_df(by="check")` shows 3, the number of copies in the
    table. **Non-unique Failed Rows**, under **Multi Table** in the summary,
    adds up the check-level numbers of the cross-table checks, so a row caught
    by two such checks counts twice.

## How counting and annotating work

Counting and annotating start with the same two steps, then run separately:

```text
                    ┌────────────────────────────────────┐
                    │ Pick the counted checks            │  shared
                    │ Pick the match key                 │  shared
                    └──────────────────┬─────────────────┘
                     ┌─────────────────┴──────────────────┐
                     ▼                                    ▼
      ┌─────────────────────────────┐      ┌─────────────────────────────┐
      │ COUNTING                    │      │ ANNOTATING                  │
      │ C1. Collect each check's    │      │ A1. Download the whole      │
      │     failed rows (3 routes)  │      │     table                   │
      │ C2. Merge into one entry    │      │ A2. Keep the checks whose   │
      │     per row                 │      │     rows can be annotated   │
      │ C3. Count the table's rows  │      │ A3. Stop if a check was cut │
      │ C4. Add up per dimension    │      │     short by max_failed_rows│
      │     and per schema          │      │ A4. Merge into one entry    │
      │ C5. Flag numbers that are   │      │     per row                 │
      │     not exact               │      │ A5. Annotate matching rows  │
      │                             │      │ A6. Keep the rest as        │
      │                             │      │     residues                │
      └──────────────┬──────────────┘      └──────────────┬──────────────┘
                     ▼                                    ▼
      Passed Rows, get_row_quality_df(),      get_annotated_output(),
      DQ metrics                              save(output_mode="annotated")
```

The first shared step is described in
[Which Checks Contribute Failed Rows](which-checks.md). The second follows.

|                             | Counting                                                              | Annotating                                                      |
| --------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------- |
| Where it runs               | Inside your data source where it can, otherwise on your machine       | On your machine                                                 |
| What vowl downloads         | Counts, and only the failed rows the data source can't count          | Your whole table, plus the failed rows of every annotated check |
| Effect of `max_failed_rows` | None where the data source counts. Otherwise the number is not exact. | Stops with an error. See [Capping Failed Rows](capping.md).     |
| Cost                        | Grows with the number of checks, not with the size of the table       | Downloads the whole table every time                            |

vowl never uses the annotated table to work out the row counts, so the row
counts don't depend on downloading anything.

## The match key

To merge the failed rows of several checks, vowl must know when two failed
rows are the same row. It compares the values in the **match key** columns.

The match key is **every column of the table**, unless vowl can use the
**primary key**. vowl uses the primary key when all of these hold:

1. The contract declares one (`primaryKey: true`), and it leaves out at least
   one column of the table.
2. The data source can count for vowl. This means vowl knows the table's
   column types and the data source runs a test counting query without an
   error. This is not limited to particular data sources, but the numbers
   are exact only on DuckDB, SQLite, Spark, Databricks and PostgreSQL.
3. vowl asks the data source whether any key value appears twice, and the
   answer is no.

A primary key with no repeated value groups the rows the same way as every
column does, so the numbers don't change. It is faster, avoids comparing
values that are awkward to compare (such as floats, lists and timestamps
outside the key), and lets a check that returns only some of the columns be
merged, as long as it returns the key.

A check whose failed rows don't hold the match key columns can't be merged.
It is not counted, and its failed rows become a residue. Its `reason` in
`get_row_quality_df(by="check")` says why:

| Reason                                                       | When                                                                                                                                                                                   |
| ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `failed rows do not have the table's columns or primary key` | The failed rows hold neither all of the table's columns nor the primary key. Or they hold the primary key, but the data source can't count for vowl, so the match key is every column. |
| `primary key has duplicate values`                           | The failed rows hold the primary key, but a key value appears twice in the table.                                                                                                      |
| `primary key uniqueness could not be checked`                | The failed rows hold the primary key, but the data source could not answer.                                                                                                            |

### How values are compared

Inside DuckDB, SQLite, Spark and PostgreSQL, vowl compares the stored values
exactly. So `a` and `A`, or `-0.0` and `0.0`, stay different even when the
column ignores case. On your machine, two values are the same when:

- **They hold the same data.** So every copy of a failed row matches. That is
  why both bread rows are annotated.
- **Both are empty.** An empty value (`NULL`) matches another empty value.
- **Both are "not a number".** A `NaN` matches another `NaN`.

And they differ when:

- **One is `-0.0` and the other `0.0`.** Some databases keep them apart, so a
  check can catch one and not the other.
- **Any item of a list or nested value differs.** They are compared item by
  item.
- **Two times differ by a nanosecond or more.**

## Counting the row counts

Counting runs once per table.

### C1. Collect each check's failed rows

vowl collects every copy of every failed row of each counted check. For each
check it picks the fastest **route** that gives the right number:

| Route          | When                                                                                                                                | What happens                                                                     |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `pushdown`     | The failed rows query is a plain filter of its table ("rows of `orders` where price is 0 or less"), on the table's own data source. | The data source finds the rows itself. No rows are sent to vowl.                 |
| `table_match`  | The query is not a plain filter, for example it uses `DISTINCT` or a join, but it runs on the table's own data source.              | The data source counts the rows of the table that match the check's failed rows. |
| `fetched_rows` | The check reads tables on more than one data source, or the data source can't count for vowl.                                       | vowl downloads the check's failed rows and counts them itself.                   |

`table_match` exists because a query that is not a plain filter can return the
wrong number of copies. A `DISTINCT` query returns one row for three copies. A
join can return one row twice. Matching back onto the table gives the true
number of copies.

A check that caught no rows needs no route. `get_row_quality_df(by="check")`
shows the route each check took.

### C2. Merge into one entry per row

For each distinct row, by the match key, vowl keeps:

- **the checks** that caught it,
- **the copies**: the largest number of copies any one check returned.

Taking the largest, not the sum, is the key rule. Both
`price_must_be_positive` and `order_id_is_unique` return the 2 bread rows.
Adding them up would give 4. Taking the largest gives 2, which is right. A
plain filter always returns all copies of a row or none, so the largest is the
true number.

| Row   | Checks that caught it                          | Copies |
| ----- | ---------------------------------------------- | ------ |
| bread | `price_must_be_positive`, `order_id_is_unique` | 2      |
| milk  | `price_must_be_positive`, `quantity_is_filled` | 1      |

When every check took `pushdown` or `table_match`, the data source does the
whole merge and sends back only the counts. Large contracts are split into
batches of up to 250 checks, which vowl merges with the same rule. When some
checks took `fetched_rows`, the data source merges its own checks and sends
back one entry per failed row, and vowl merges those with the fetched rows on
your machine.

In the rare case where two rows the data source kept apart look the same on
your machine (SQLite `1` and `1.0` in one column, for example), vowl keeps
them as two rows and adds their copies.

### C3. Count the table's rows

The total is the number of rows in the table after your filter conditions. It
is never capped. vowl takes the first of these that is available:

1. The count from the same query as the merge, when everything ran in one
   query.
2. The count vowl already took during the run, when it was not capped.
3. A fresh count from the data source.
4. The capped count from the run. The numbers are then not exact.

A total of 0 is counted again, because a failed count also comes back as 0.

### C4. Add up per dimension and per schema

From the merged entries:

- **Failed rows** of a schema are the copies of every entry caught by at
  least one check in scope. That means failed checks, or every counted check
  under `row_issue_scope="all_violations"`.
- **Tolerated rows** are the copies of every entry caught by any counted
  check, minus the failed rows.
- **Failed rows of a dimension** are worked out the same way, using only the
  checks of that dimension.
- **Passed rows** are the total rows minus the failed rows.
- **Pass rate** is the passed rows divided by the total rows. Every dimension
  uses its table's total rows.

| Level        | Entries     | Failed rows | Pass rate |
| ------------ | ----------- | ----------- | --------- |
| orders       | bread, milk | 2 + 1 = 3   | 2 / 5     |
| conformity   | bread, milk | 3           | 2 / 5     |
| completeness | milk        | 1           | 4 / 5     |
| uniqueness   | bread       | 2           | 3 / 5     |

A dimension with no counted checks, and an empty table, have no pass rate.
vowl shows N/A for them rather than 100%.

`get_row_quality_df()` returns these columns, by schema or by dimension:

| Column                                 | Meaning                                              |
| -------------------------------------- | ---------------------------------------------------- |
| `total_rows`                           | Rows in the table, after your filter conditions.     |
| `failed_rows`                          | Rows that failed at least one counted check.         |
| `tolerated_rows`                       | Rows caught only by checks that passed.              |
| `passed_rows`                          | `total_rows` minus `failed_rows`.                    |
| `pass_rate`                            | `passed_rows` divided by `total_rows`, from 0 to 1.  |
| `exact`                                | Whether the numbers are exact.                       |
| `checks_counted`, `checks_not_counted` | How many checks were counted, and how many were not. |

An empty value means there is no number to give.

### C5. Exact numbers {#exact-numbers}

Every number carries an `exact` flag. It is `False` when the number could be
off:

- A check that would have been counted ended in `ERROR`, so it could be
  hiding bad rows.
- A check's failed rows were downloaded, and `max_failed_rows` cut them short.
- A check's failed rows were downloaded, and its query is not a plain filter
  (for example it uses `DISTINCT` or a join), so it may return more or fewer
  copies than the table has.
- The data source could not run a check's failed rows query as part of the
  count, so vowl left the check out.
- The data source is not one vowl has tested for counting. DuckDB, SQLite,
  Spark and PostgreSQL are tested. Databricks uses the Spark method but has
  not been tested on its own.
- The contract and the data source don't say what columns the table has, so
  vowl can't tell whether two checks caught the same row.
- A value could not be compared during the merge.
- The total was capped, or is lower than the rows one check found.

`get_row_quality_df(by="check")` has an `exact` column too, which shows which
check made a number not exact. The summary adds **(approx.)** after a number
that is not exact, and the DQ metrics carry `vowl.row_quality.exact`.

## Annotating your table

Annotating runs once per table, on your machine, from the failed rows each check
returned. It does not use the counting routes.

### Where each failed check ends up

Every failed check ends up in exactly one of three places:

| Where                                  | Which checks                                                                                                                                                                              | Example                                                            |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| **Annotated on the table**             | Counted checks whose failed rows hold the match key. Most checks, including generated unique-value and foreign-key checks.                                                                | `price_must_be_positive`                                           |
| **A residue**, in `output["residues"]` | Checks that returned failed rows that can't be annotated, such as rows with fewer columns, or extra columns from a second table. Each residue is stored under `"<schema>::<check_name>"`. | A check that returns only the names of towns with an unusual price |
| **The summary only**                   | Checks that are not counted and return no rows: one number such as an average or `rowCount`, failed rows that are good rows, or a check that ended in `ERROR`.                            | `average_price_in_range`                                           |

!!! tip "Read the summary for the final result"

    A check in the last group annotates no row and creates no residue. It appears
    only in the summary and in `get_check_results_df()`. Looking at the
    annotated table alone, you would miss it.

A table can have annotated rows and residues at the same time. A check is never
in both places.

### A1. Download the whole table

vowl downloads the table, with your filter conditions applied. If it can't,
for example because no adapter was given for it, it logs a warning and makes
no annotated table. That table's failed rows become residues.

vowl can't download tables with these DuckDB column types: `INTERVAL`, `BIT`
and `UNION`. For those tables vowl logs "Annotated export failed", makes no
annotated table, and their failed rows become residues. The row counts are
not affected when the data source counts them.

### A2. Keep the checks whose rows can be annotated

From the counted checks, vowl keeps those whose failed rows hold the match
key: exactly the table's columns when the match key is every column, or the
key columns when it is the primary key. Counting uses the same rule, so both
keep the same checks.

### A3. Stop if a check was cut short

If a kept check caught more rows than `max_failed_rows` let vowl download,
`get_annotated_output()` stops with an error. Otherwise the rows past the cap
would look clean. See [Capping Failed Rows](capping.md).

### A4. Merge into one entry per row

vowl stacks the failed rows of all kept checks, each tagged with its check's
name, and groups them by the match key. Each entry keeps the list of check
names, in the order they were first seen. Copies don't matter here, so both
bread rows become one entry.

### A5. Annotate matching rows

vowl goes through every row of the table. When its match key values match an
entry, as described in [How values are compared](#how-values-are-compared),
vowl writes the entry's check names into the row's `check_info` column.
Otherwise `check_info` is empty, and the row is a **clean row**.

Because vowl matches by value, every copy of a failed row is annotated. In the
example, both bread rows are annotated, so the annotated table has 3 annotated rows,
the same as the 3 failed rows from counting:

| order_id | item  | price | quantity | check_info                                                                         |
| -------- | ----- | ----- | -------- | ---------------------------------------------------------------------------------- |
| 1        | apple | 1.50  | 3        |                                                                                    |
| 2        | bread | -2.00 | 1        | `[{"check_name": "price_must_be_positive"}, {"check_name": "order_id_is_unique"}]` |
| 3        | milk  | 0.00  |          | `[{"check_name": "price_must_be_positive"}, {"check_name": "quantity_is_filled"}]` |
| 4        | eggs  | 3.20  | 2        |                                                                                    |
| 2        | bread | -2.00 | 1        | `[{"check_name": "price_must_be_positive"}, {"check_name": "order_id_is_unique"}]` |

To keep only the clean rows:

```python
annotated = result.get_annotated_output()["annotated"]["orders"].to_pandas()
clean = annotated[annotated["check_info"].isna()].drop(columns=["check_info"])
# 2 clean rows: apple and eggs
```

### A6. Keep the rest as residues

vowl keeps a residue for every check that returned failed rows but was not
annotated:

- counted checks left out in A2,
- counted checks of a table with no annotated table (A1),
- other failed checks that returned rows, except checks whose failed rows are
  the good rows.

Each residue holds one check, with duplicate rows removed. Residues are never
grouped across checks.

### What the annotated output holds

```python
output = result.get_annotated_output()
output["annotated"]   # {"<schema>": your table + check_info}
output["residues"]    # {"<schema>::<check_name>": failed rows + check_info + tables_in_query}
```

<!-- prettier-ignore-start -->
<!-- Zensical needs the table indented 4 spaces to stay inside the list item. -->

- **Every schema gets an annotated table**, even when nothing failed. Its
  `check_info` column is then empty on every row.
- **`check_info` is a JSON list** with one item per check the row failed.
  Choose how much each item holds with `check_info=` (or
  `annotated_check_info` in `ValidationConfig`):

    | `check_info`        | Each item holds                                             |
    | ------------------- | ----------------------------------------------------------- |
    | `"names"` (default) | `check_name`                                                |
    | `"summary"`         | `check_name`, `dimension`, `tags` and `target` (the column) |
    | `"full"`            | The whole check definition, plus `check_name` and `target`  |

- **Each residue holds one check.** It has that check's failed rows, the same
  `check_info` column (with one item), and a `tables_in_query` column naming
  the tables the check read.
- **Unique-value, primary key and `duplicateValues` checks annotate rows.** vowl
  fetches every row whose value appears more than once (and, for a primary
  key, every row where it is empty), so the row counts and the annotated rows
  agree. The exception is `duplicateValues` with `unit: percent`, whose result
  is a share and not a number of rows.

<!-- prettier-ignore-end -->

## When counting and annotating differ

In most runs, `failed_rows` equals the number of annotated rows. These are the
exceptions:

| Situation                                                                                                                                    | Row counts                                                                          | Annotated table                                      |
| -------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ---------------------------------------------------- |
| A check had more failed rows than `max_failed_rows`                                                                                          | Exact where the data source counts. Not exact when the failed rows were downloaded. | `get_annotated_output()` stops with an error.        |
| The table has a column vowl can't download (`INTERVAL`, `BIT`, `UNION`)                                                                      | Counted as usual.                                                                   | No annotated table. The failed rows become residues. |
| The data source is not DuckDB, SQLite, Spark or PostgreSQL, and treats different values as equal (`a` and `A` in a column that ignores case) | Two rows caught by different checks can count as one. Not exact.                    | Both rows are annotated.                             |
| A check's query is not a plain filter, and its failed rows had to be downloaded, for example `DISTINCT` on a data source that can't count    | Three copies can count as one. Not exact.                                           | All three copies are annotated.                      |
| The table changes while vowl reads it                                                                                                        | Counting reads the table at one moment, or a few moments for large contracts.       | Annotating reads it again later.                     |

In the first two rows, the row counts are right and `exact` stays `True`. In
the next two, `exact` is `False`. vowl can't detect the last one, which only
matters for a table that is written to during the run.

Use the row counts for how many rows have issues and for pass rates. They are
cheap on large tables. Use the annotated table, or the residues, for which
rows have issues. On a large table, `get_output_dfs()` gives each check's
failed rows without downloading the whole table.

## Other ways to see failed rows

| Method                                 | What you get                                                                    |
| -------------------------------------- | ------------------------------------------------------------------------------- |
| `result.show_failed_rows(max_rows=5)`  | Prints a few failed rows for each failed check. `max_rows=-1` prints them all.  |
| `result.get_output_dfs()`              | Each check's failed rows as a separate table, under `"<schema>::<check_name>"`. |
| `result.save(output_mode="annotated")` | Saves the annotated tables, residues and `summary.json` as files.               |

`save()` uses `output_mode="annotated"` by default.
`output_mode="failed_rows"` and `get_consolidated_output_dfs()` are the older
way of saving failed rows and will be removed. They group the failed rows of
several checks together, with a comma-separated `check_ids` column. Use
`"annotated"` instead.

## Where to look in the code

| Step                             | Code in `src/vowl/validation/`                                                      |
| -------------------------------- | ----------------------------------------------------------------------------------- |
| Pick the counted checks          | `row_quality/selection.py`                                                          |
| Pick the match key               | `_choose_key` in `row_quality/__init__.py`, `row_quality/mergeable.py`              |
| C1                               | `_assign_route` and `_fetch` in `row_quality/__init__.py`, `row_quality/certify.py` |
| C2                               | `row_quality/pushdown.py` (data source), `row_quality/merge.py` (your machine)      |
| C3                               | `_total_rows` in `row_quality/__init__.py`                                          |
| C4, C5                           | `row_quality/rollup.py`                                                             |
| DQ metrics                       | `dq_metrics.py`                                                                     |
| A1 to A6                         | `get_annotated_output` in `result.py`                                               |
| Comparing values on your machine | `row_keys` in `result_row_quality.py`                                               |

The full design, with the reasoning and measurements, is in
`design/row-quality-statistics.md`.
