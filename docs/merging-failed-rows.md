---
title: How Failed Rows Are Merged
description: >-
  Step by step, how vowl merges the failed rows of many checks into one set of
  rows per table. Once for the row counts and DQ metrics, and once for the
  annotated table.
---

# How Failed Rows Are Merged

A table usually has many checks, and one row can fail several of them. To say
"how many rows have issues" or to mark each bad row once, vowl has to merge
the failed rows of all the checks into one list per table. This page walks
through how it does that.

vowl merges failed rows twice, for two different jobs:

- **Counting** gives the row counts and the row-level DQ metrics.
- **Marking** gives the annotated table.

Both start the same way, then go their own way. This page follows that order.
[Handling of Failed Rows](failed-rows.md) covers the same ground from the
user's side, with less detail.

## Words used on this page

The words below come from [Key Terms](key-terms.md), plus a few that only
this page needs. Each one means one thing on this page.

| Word              | Meaning                                                                                                                            |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| **check**         | One test from your contract.                                                                                                       |
| **failed rows**   | The rows a check caught.                                                                                                           |
| **counted check** | A check whose failed rows take part in the merge.                                                                                  |
| **copy**          | One of several identical rows in a table. A table where the same row appears twice has two copies of it.                           |
| **match key**     | The columns vowl compares to decide whether two rows are the same row. Every column of the table, or its primary key.              |
| **merge**         | Putting the failed rows of all counted checks together, so each row appears once with the list of checks it failed and its copies. |
| **route**         | How vowl collects a check's failed rows for counting: `pushdown`, `table_match` or `fetched_rows`.                                 |
| **mark**          | To write a check's name into a row's `check_info` column.                                                                          |
| **residue**       | The failed rows of one check that vowl could not mark on its table.                                                                |
| **exact**         | Whether a number is exact. `False` when it could be off.                                                                           |

## The whole process at a glance

```text
                    ┌────────────────────────────────────┐
                    │ Step 1. Pick the counted checks     │  shared
                    │ Step 2. Pick the match key          │  shared
                    └──────────────────┬─────────────────┘
                     ┌─────────────────┴──────────────────┐
                     ▼                                    ▼
      ┌─────────────────────────────┐      ┌─────────────────────────────┐
      │ COUNTING                    │      │ MARKING                     │
      │ C1. Collect each check's    │      │ M1. Download the whole      │
      │     failed rows (3 routes)  │      │     table                   │
      │ C2. Merge: one entry per    │      │ M2. Keep the checks whose   │
      │     row, checks + copies    │      │     rows can be marked      │
      │ C3. Count the table's rows  │      │ M3. Stop if a check was cut │
      │ C4. Add up per table and    │      │     short by max_failed_rows│
      │     per dimension           │      │ M4. Merge: one entry per    │
      │ C5. Flag numbers that are   │      │     row, list of checks     │
      │     not exact               │      │ M5. Mark matching rows      │
      │                             │      │ M6. Everything else becomes │
      │                             │      │     a residue               │
      └──────────────┬──────────────┘      └──────────────┬──────────────┘
                     ▼                                    ▼
      Passed Rows, get_row_quality_df(),      get_annotated_output(),
      the dimension, schema and run            save(output_mode="annotated")
      row metrics
```

vowl never uses the annotated table to work out the row counts. The two paths
only share steps 1 and 2.

## The example used on this page

The `orders` table from [Handling of Failed Rows](failed-rows.md#a-small-example),
with a fourth check added:

| order_id | item  | price | quantity |
| -------- | ----- | ----- | -------- |
| 1        | apple | 1.50  | 3        |
| 2        | bread | -2.00 | 1        |
| 3        | milk  | 0.00  |          |
| 4        | eggs  | 3.20  | 2        |
| 2        | bread | -2.00 | 1        |

The last row is a copy of the second.

| Check                    | Dimension    | Operator        | Rows it catches              | Result |
| ------------------------ | ------------ | --------------- | ---------------------------- | ------ |
| `price_must_be_positive` | conformity   | `mustBe: 0`     | bread, milk, bread (3 rows)  | FAILED |
| `quantity_is_filled`     | completeness | `mustBe: 0`     | milk (1 row)                 | FAILED |
| `order_id_is_unique`     | uniqueness   | `mustBe: 0`     | bread, bread (2 rows)        | FAILED |
| `average_price_in_range` | conformity   | `mustBeBetween` | none, it returns one average | FAILED |

## Shared step 1: pick the counted checks

vowl looks at every check once and decides whether its failed rows take part.
Both counting and marking use this same decision.

A check is counted when all of these hold:

1. **It belongs to one table.** Its failed rows are rows of that table.
2. **It ran.** Its result is `PASSED` or `FAILED`. A check that ended in
   `ERROR` has no failed rows. If it would have been counted, the numbers of
   its table and dimension are marked not exact, since it could be hiding bad
   rows.
3. **It points at rows.** An average, a sum or `rowCount` is one number for
   the whole table, so it has no failed rows.
4. **Its failed rows are the bad rows.** The operator must set an upper limit
   on the rows: `mustBeLessThan`, `mustBeLessOrEqualTo`, `mustBe: 0` or
   `mustBeBetween: [0, n]`. With any other operator the caught rows are the
   good rows, or no single row is at fault. See
   [Which checks are counted](failed-rows.md#which-checks-are-counted).

A counted check that **passed** but still caught rows is a **tolerated**
check, for example `mustBeLessThan: 100` with 50 rows caught. The
`row_issue_scope` setting decides what happens to its rows:

| `row_issue_scope`           | Rows of tolerated checks                                                        |
| --------------------------- | ------------------------------------------------------------------------------- |
| `"failed_checks"` (default) | Not in the failed rows, and not marked. Counted separately as `tolerated_rows`. |
| `"all_violations"`          | In the failed rows, and marked with `"tolerated": true` in `check_info`.        |

In the example, the first three checks are counted. `average_price_in_range`
is not, because it returns one number.

## Shared step 2: pick the match key

To merge, vowl must know when two failed rows are the same row. It compares
the values in the **match key** columns.

The match key is **every column of the table**, unless vowl can use the
**primary key**. Two rows are then the same row when their key values are the
same.

vowl uses the primary key whenever all of these hold:

1. The contract declares one (`primaryKey: true`), and it leaves out at least
   one column of the table.
2. The data source can count for vowl (DuckDB, SQLite, Spark, Databricks or
   Postgres).
3. vowl asks the data source whether any key value appears twice, and the
   answer is no.

A key with no repeated value puts rows into the same groups as every column
does, so the numbers don't change. Grouping on fewer columns is faster. It
also avoids comparing values that are awkward to compare, such as floats,
lists and timestamps outside the key. And a check that returns only some of
the table's columns can still be merged, as long as it returns the key.

A check whose failed rows lack the match key columns cannot be merged. It is
left out of the row counts, and becomes a residue on the annotated side. Its
`reason` in `get_row_quality_df(by="check")` says why:

| Reason                                                       | When                                                                              |
| ------------------------------------------------------------ | --------------------------------------------------------------------------------- |
| `failed rows do not have the table's columns or primary key` | The rows have neither all of the table's columns nor the primary key.             |
| `primary key has duplicate values`                           | The rows hold the primary key, but a key value appears twice in the table.        |
| `primary key uniqueness could not be checked`                | The rows hold the primary key, but the data source could not answer the question. |

Counting and marking use the same match key, so they merge the same checks.

## Counting: the row counts and DQ metrics

Counting runs once per table.

### C1. Collect each check's failed rows

vowl collects every copy of every failed row of each counted check. It picks
one of three routes per check:

| Route          | When                                                                                                                            | What happens                                                                     |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `pushdown`     | The check's query is a plain filter of its table ("rows of `orders` where price is 0 or less"), on the table's own data source. | The data source finds the rows itself. No rows are sent to vowl.                 |
| `table_match`  | The query is not a plain filter, for example it uses `DISTINCT` or a join, but it runs on the table's own data source.          | The data source counts the rows of the table that match the check's failed rows. |
| `fetched_rows` | The check reads tables on more than one data source, or the data source can't count for vowl.                                   | vowl downloads the check's failed rows and counts them itself.                   |

A check that caught no rows needs no route. It adds nothing.

`table_match` exists because a query that is not a plain filter can return
the wrong number of copies. A `DISTINCT` query returns one row for three
copies. A join can return one row twice. Matching back onto the table gives
the true number of copies.

Two things can make a check's numbers not exact here:

- On `fetched_rows`, `max_failed_rows` cut the check's rows short.
- On `fetched_rows`, the check's query is not a plain filter, so the copies
  can be off.

Under the default `row_issue_scope`, a tolerated check on the `fetched_rows`
route is skipped, because its rows are not needed for the failed rows and can
be large. `tolerated_rows` is then missing.

### C2. Merge into one entry per row

For each distinct row (by the match key), vowl keeps two things:

- **the checks** that caught it,
- **the copies**: the largest number of copies any single check returned.

Taking the largest, not the sum, is the key rule. The two bread rows in the
example show why:

| Check                    | Copies of the bread row it returned |
| ------------------------ | ----------------------------------- |
| `price_must_be_positive` | 2                                   |
| `order_id_is_unique`     | 2                                   |

The table holds 2 bread rows. Adding the checks up would give 4. Taking the
largest gives 2, which is right. A plain filter always returns all copies of a
row or none of them, so the largest count is the true count.

After the merge, the example has three entries:

| Row   | Checks that caught it                          | Copies |
| ----- | ---------------------------------------------- | ------ |
| bread | `price_must_be_positive`, `order_id_is_unique` | 2      |
| milk  | `price_must_be_positive`, `quantity_is_filled` | 1      |

Where the merge happens depends on the routes:

- **Every check on `pushdown` or `table_match`.** The data source does the
  whole merge and sends back only how many rows failed each combination of
  checks. Large contracts are split into batches of up to 250 checks. vowl
  merges the batches with the same "largest copies" rule.
- **Some checks on `fetched_rows`.** The data source merges its own checks and
  sends back one entry per failed row, with its checks, its copies and its
  values. vowl then merges those entries with the fetched rows on your
  machine, again taking the largest copies per row.

When the data source compares values, it compares the stored bytes, so `a`
and `A`, or `-0.0` and `0.0`, stay apart even under a case-insensitive
setting. On your machine vowl compares values the way
[How vowl finds a failed row](failed-rows.md#how-vowl-finds-a-failed-row-in-your-table)
describes. In the rare case where two rows the data source kept apart look
the same on your machine (SQLite `1` and `1.0` in one column, for example),
vowl keeps them as two rows and adds their copies.

If a value can't be compared at all on your machine, the numbers of the table
are marked not exact.

### C3. Count the table's rows

The total is the number of rows in the table after your filter conditions. It
is never capped. vowl takes the first of these that is available:

1. The count from the same query as the merge, when everything ran in one
   query.
2. The count vowl already took during the run, when it was not capped.
3. A fresh count from the data source.
4. The capped count from the run. The numbers are then marked not exact.

A total of 0 is counted once more, because a failed count also comes back as 0.

### C4. Add up per table and per dimension

From the merged entries:

- **Failed rows of a table** = the copies of every entry caught by at least one
  check in scope. In scope means failed checks by default, or every counted
  check under `"all_violations"`.
- **Tolerated rows** = the copies of every entry caught by any counted check,
  minus the failed rows.
- **Failed rows of a dimension** = the same, using only the checks of that
  dimension.
- **Passed rows** = total rows minus failed rows.
- **Pass rate** = passed rows divided by total rows. Every dimension of a table
  uses the table's total rows.

In the example, with 5 total rows:

| Level        | Entries counted | Failed rows | Pass rate |
| ------------ | --------------- | ----------- | --------- |
| orders       | bread, milk     | 2 + 1 = 3   | 2 / 5     |
| conformity   | bread, milk     | 3           | 2 / 5     |
| completeness | milk            | 1           | 4 / 5     |
| uniqueness   | bread           | 2           | 3 / 5     |

The dimensions add up to 6, more than the 3 failed rows of the table, because
milk and bread each failed checks in two dimensions. That is expected.

A dimension with no counted checks, and an empty table, have no pass rate.
vowl shows N/A for them rather than 100%.

### C5. Flag numbers that are not exact

A table or dimension number is marked not exact when any of these hold:

- a check in it is not exact (see C1, or a check ended in `ERROR`),
- a value could not be compared during the merge,
- the total was capped, or is lower than the rows one check found.

`get_row_quality_df(by="check")` shows which check caused it.

### How the DQ metrics use the counts

| Metric                                   | Where the number comes from                                                                           |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `vowl.check.row.count` and its pass rate | Each check's own count of rows. **Not merged.** A row that fails two checks shows up in both.         |
| `vowl.dimension.row.count` and pass rate | The dimension numbers from C4.                                                                        |
| `vowl.schema.row.count` and pass rate    | The table numbers from C4.                                                                            |
| `vowl.run.row.count` and pass rate       | The table numbers added up. Different tables hold different rows, so nothing is merged across tables. |

The dimension, schema and run metrics carry `vowl.row_quality.exact`. They are
left out when no check in them was counted.

The check level uses the check's own count query, so it can differ from the
check's `failed_rows` in `get_row_quality_df(by="check")`. For a `DISTINCT`
check that returns one row for three copies, the check metric shows 1 and
`get_row_quality_df(by="check")` shows 3.

## Marking: the annotated table

Marking runs once per table, entirely on your machine, from the failed rows
each check fetched. It does not use the counting routes.

### M1. Download the whole table

vowl downloads the table, with your filter conditions applied. If it can't
(no connection for the table, or a column type it can't download such as
DuckDB `INTERVAL`), there is no annotated table for it. Its checks' failed
rows become residues.

### M2. Keep the checks whose rows can be marked

From the counted checks in scope (step 1), vowl keeps those whose failed rows
can be found in the table:

- with every column as the match key, the failed rows must have exactly the
  table's columns,
- with the primary key as the match key, they must hold the key columns.

Counting uses the same rule, so both keep the same checks. Marking checks it
against the columns of the table it downloaded in M1.

A cross-table check can be marked if its failed rows hold only its own table's
columns, for example `SELECT payroll.* FROM payroll LEFT JOIN ...`. See
[Writing a cross-table check that marks rows](failed-rows.md#writing-a-cross-table-check-that-marks-rows).

### M3. Stop if a check was cut short

If a kept check caught more rows than `max_failed_rows` let vowl fetch,
`get_annotated_output()` stops with an error. Otherwise the rows past the cap
would look clean.

### M4. Merge into one entry per row

vowl stacks the failed rows of all kept checks, each row tagged with its
check's name. It then groups identical rows by the match key. Each distinct
row keeps the list of check names, in the order they were first seen, with no
name twice.

Copies don't matter here. Both bread rows turn into one entry:

| Row   | Checks                                         |
| ----- | ---------------------------------------------- |
| bread | `price_must_be_positive`, `order_id_is_unique` |
| milk  | `price_must_be_positive`, `quantity_is_filled` |

### M5. Mark matching rows

vowl goes through every row of the whole table. If its match key values match
an entry from M4, it writes that entry's check list into `check_info`.
Otherwise `check_info` is empty.

Because vowl matches by value, every copy of a failed row is marked. So both
bread rows are marked, and the annotated table has 3 marked rows, the same as
the 3 failed rows from counting.

### M6. Everything else becomes a residue

A residue is kept for every check that caught rows but was not marked:

- counted checks left out in M2,
- counted checks of a table with no annotated table (M1),
- other failed checks that returned rows, except checks whose caught rows are
  the good rows.

Each residue holds one check only, with duplicate rows removed. Residues are
never grouped across checks.

## Counting and marking side by side

| Step                        | Counting                                                                          | Marking                                               |
| --------------------------- | --------------------------------------------------------------------------------- | ----------------------------------------------------- |
| Which checks                | Step 1 counted checks, merged on the step 2 match key                             | The same checks, on the same match key                |
| Where failed rows come from | The data source for `pushdown` and `table_match`. Fetched rows for `fetched_rows` | Always each check's fetched failed rows               |
| How it keeps the copies     | The largest copies any one check returned                                         | Marks every copy of a matching row in the whole table |
| How it compares rows        | Stored bytes in the data source. vowl's value comparison for fetched rows         | vowl's value comparison                               |
| Checks it can't merge       | Left out of the counts                                                            | Kept as residues                                      |
| `max_failed_rows`           | No effect on `pushdown` and `table_match`. Makes `fetched_rows` not exact         | Stops with an error                                   |
| Result for the example      | 3 failed rows                                                                     | 3 marked rows                                         |

The two agree whenever both are exact. The few cases where they differ are
listed in [When the two differ](failed-rows.md#when-the-two-differ).

## Where to look in the code

| Step                           | Code in `src/vowl/validation/`                                                      |
| ------------------------------ | ----------------------------------------------------------------------------------- |
| 1                              | `row_quality/selection.py`                                                          |
| 2                              | `_choose_key` in `row_quality/__init__.py`, `row_quality/mergeable.py`              |
| C1                             | `_assign_route` and `_fetch` in `row_quality/__init__.py`, `row_quality/certify.py` |
| C2                             | `row_quality/pushdown.py` (data source side), `row_quality/merge.py` (your machine) |
| C3                             | `_total_rows` in `row_quality/__init__.py`                                          |
| C4, C5                         | `row_quality/rollup.py`                                                             |
| DQ metrics                     | `dq_metrics.py`                                                                     |
| M1 to M6                       | `get_annotated_output` in `result.py`                                               |
| Row comparison on your machine | `row_keys` in `result_row_quality.py`                                               |

The full design, with the reasoning and measurements, is in
`design/row-quality-statistics.md`.
