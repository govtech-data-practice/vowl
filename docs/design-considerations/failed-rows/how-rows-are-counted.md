---
title: How Rows Are Counted
description: >-
  Why counting the rows that failed is harder than adding up checks, the
  grains vowl counts at, and why counts are exact only when each failed row is
  attributed to the source table.
---

# How Rows Are Counted

[How Failed Rows Are Derived](how-failed-rows-are-derived.md) showed how each
check returns its failed rows. This page covers
[attributed rows](failed-row-results.md#failed-row-results): how vowl
attributes the failed rows to the source table and turns them into row
counts, such as **Passed Rows** in the summary and `get_row_quality_df()`.

## Why counting the rows that failed is hard {#why-counting-failed-rows-is-hard}

Adding up each check's failed rows seems enough, but three things get in the
way:

- **One row can fail several checks.** In a table with 5 rows, no more than
  5 rows can fail, so vowl counts each row once within each grain: the
  dimension, the schema and the run.
- **Some checks return rows that can't be traced back to the source table.**
  A `DISTINCT` drops copies, a join can repeat rows, and a check may return
  only some columns. Such rows can't be counted as they are.
- **Exact counts cost extra work.** Tracing each failed row back to the
  source table takes extra queries, or a download of the table. vowl offers
  several [counting mechanisms](counting-mechanisms.md) that trade this cost
  against exactness.

## Grains of row counts {#grains-of-failed-row-counts}

vowl counts the rows that failed at four grains. This page uses the `orders` table
from the [example](failed-row-results.md#the-example-used-in-this-section)
and its three counted checks. A ✗ marks a row the check catches:

| Row | order_id | item  | price | quantity | `price_must_be_positive` | `quantity_is_filled` | `order_id_is_unique` |
| --- | -------- | ----- | ----- | -------- | ------------------------ | -------------------- | -------------------- |
| 1   | 1        | apple | 1.50  | 3        |                          |                      |                      |
| 2   | 2        | bread | -2.00 | 1        | ✗                        |                      | ✗                    |
| 3   | 3        | milk  | 0.00  |          | ✗                        | ✗                    |                      |
| 4   | 4        | eggs  | 3.20  | 2        |                          |                      |                      |
| 5   | 2        | bread | -2.00 | 1        | ✗                        |                      | ✗                    |

Rows 2 and 5 are exact copies. Rows 2, 3 and 5 fail at least one check. Rows
1 and 4 are clean. The fourth check, `average_price_in_range`, is not counted,
so it has no row counts at any grain.

| Grain         | What it counts                                      | In the example                                                                | Where to see it                                                                                                                                                                                |
| ------------- | --------------------------------------------------- | ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Check**     | The rows one counted check caught                   | `price_must_be_positive`: 3, `quantity_is_filled`: 1, `order_id_is_unique`: 2 | `scalar_count` in `get_row_quality_df(by="check")` and the check-level [DQ metrics](../../dq-metrics/understanding-metrics.md#how-failed-rows-are-counted). `attributed_rows` sits next to it. |
| **Dimension** | The rows that failed any check of the dimension     | conformity: 3, completeness: 1, uniqueness: 2                                 | `get_row_quality_df(by="dimension")`                                                                                                                                                           |
| **Schema**    | The rows of the table that failed any counted check | 3 of 5                                                                        | **Passed Rows** in the summary, `get_row_quality_df()`                                                                                                                                         |
| **Run**       | The schema numbers added up                         | 3 of 5                                                                        | the run-level [DQ metrics](../../dq-metrics/understanding-metrics.md)                                                                                                                          |

The check numbers add up to 3 + 1 + 2 = 6, but only 3 rows failed. Each of
them was caught by two checks:

| Row       | Caught by                                      |
| --------- | ---------------------------------------------- |
| 2 (bread) | `price_must_be_positive`, `order_id_is_unique` |
| 3 (milk)  | `price_must_be_positive`, `quantity_is_filled` |
| 5 (bread) | `price_must_be_positive`, `order_id_is_unique` |

So above the check grain, each row counts once. The summary,
`get_row_quality_df()` and the
[DQ metrics](../../dq-metrics/understanding-metrics.md#how-failed-rows-are-counted)
all show these same numbers.

## Counts are exact only when attributed to the source table

To count a row once, vowl must know which row of the source table each failed
row stands for. This is called **attributing** the failed row to the source
table, and the rows it finds are the check's **attributed rows**. The failed
rows are not always the rows of the table, so counting them as they
are can give the wrong answer.

### Checks can change the rows they return

Take this check:

```sql
SELECT DISTINCT * FROM orders WHERE price <= 0
```

Three rows have a price of zero or less, but `DISTINCT` keeps only one of
the two bread rows:

| Row | item  | price | In the table | Returned by the query |
| --- | ----- | ----- | ------------ | --------------------- |
| 2   | bread | -2.00 | ✗            | ✗                     |
| 3   | milk  | 0.00  | ✗            | ✗                     |
| 5   | bread | -2.00 | ✗            | dropped               |

So the check has three numbers. The first two are its query output, and the
third is worked out from it:

| Number              | What it is                                                       | Shows | Where you see it                                                                                                           |
| ------------------- | ---------------------------------------------------------------- | ----- | -------------------------------------------------------------------------------------------------------------------------- |
| **Scalar count**    | The number the count query returned. It decides pass or fail.    | 2     | `actual`, `failed_rows_count`, `scalar_count` in `get_row_quality_df(by="check")` and the check-level DQ metrics           |
| **Failed rows**     | The rows the failed rows query returned                          | 2     | `show_failed_rows()`, `get_output_dfs()` and residues                                                                      |
| **Attributed rows** | The rows of the table they are attributed to, every copy counted | 3     | `attributed_rows` in `get_row_quality_df(by="check")`, the annotated output and every count above the check grain[^scalar] |

[^scalar]:
    Except with
    [`disable_table_attributed_counts`](../../run-settings.md#disable_table_attributed_counts)
    set. The counts above the check grain then add up the scalar counts.

For a plain filter such as `WHERE price <= 0` without `DISTINCT`, all three
show 3. A join can do the opposite and return one row twice, so the scalar
count and the failed rows are higher than the attributed rows.

A `COUNT(DISTINCT ...)` check has a scalar count that differs by design,
because it counts values, not rows:

```sql
SELECT COUNT(DISTINCT item) FROM orders WHERE price <= 0
```

Its scalar count is 2, for bread and milk. Its attributed rows are 3, the
rows that hold them. A check with no `WHERE`, such as
`SELECT COUNT(DISTINCT item) FROM orders` with `mustBeLessThan: 3`, fails
because the table has too many distinct items, not because any one row is
bad. vowl still counts every row with an `item` as failed, so the table's
row counts and its annotated output treat every such row as bad.

### Counts from separate checks don't add up

When each check is only counted on its own, the counts can't be added up,
because they don't say which rows overlap. The summary shows one such number.
It splits each table's checks into two groups:

- **Single Table**: checks that read only this table, like every check in
  the example.
- **Multi Table**: checks that also read another table, such as "every
  order's customer exists in `customers`".

Under **Multi Table**, the summary shows **Non-unique Failed Rows**. It is
the scalar counts (`actual`) of the failed Multi Table checks, simply added up.
Unlike **Passed Rows**, it doesn't check whether two checks caught the same
row.

Say `orders` has two Multi Table checks, and `customers` holds customer 10
(in `SG`) and customer 30 (in `XX`, an unknown country):

| order_id | customer_id | `customer_must_exist` | `customer_country_known` |
| -------- | ----------- | --------------------- | ------------------------ |
| 1        | 10          |                       |                          |
| 2        | 20          | ✗ (no customer 20)    | ✗ (no country)           |
| 3        | 30          |                       | ✗ (`XX`)                 |
| 4        | 10          |                       |                          |

```text
orders:
  Overall:
    Passed Rows:            2 / 4 (50.0%)
  Multi Table:
    Non-unique Failed Rows: 3
```

**Passed Rows** says 2 rows failed, orders 2 and 3. **Non-unique Failed
Rows** says 3, because order 2 is counted once for each check. Use **Passed
Rows** for how many rows failed, and **Non-unique Failed Rows** only as a
rough size of the Multi Table problems.

### The match key

vowl attributes a failed row by comparing a set of columns called the
**match key**. A failed row stands for every row of the table with the same
values in every match key column.

By default the match key is **every column**. It is the **primary key**
instead when all three of these are true:

1. The contract declares a primary key (`primaryKey: true`), and the key is
   not every column.
2. The data source can count for vowl. That means vowl knows the column
   types, and a test query runs without an error.
3. No key value appears twice in the table. vowl asks the data source.

Both choices give the same numbers. The primary key is faster, and it lets
vowl attribute checks that return only some columns.

#### Why the primary key helps

The example table has no primary key: `order_id` is 2 on both bread rows.
Now imagine it gets a `line_id` column with no repeats, declared as the
primary key, and a new check that returns only two columns:

```sql
SELECT line_id, price FROM orders WHERE price < 0
```

| line_id | price |
| ------- | ----- |
| 2       | -2.00 |
| 5       | -2.00 |

- **If the match key is every column**, these rows lack `order_id`, `item`
  and `quantity`, so vowl can't attribute them. The check is counted but
  not attributed.
- **If the match key is `line_id`**, vowl knows these are rows 2 and 5. The
  check is attributed, and the bread rows still count once each.

A check whose failed rows lack the match key columns is never attributable.
It stays counted but is not attributed. It adds nothing to the row counts,
which are marked not exact when it caught rows, and its failed rows become a
[residue](annotating-the-source-table.md#residues). See
[From query output to row counts](failed-row-results.md#from-query-output-to-row-counts).
The `reason` column of `get_row_quality_df(by="check")` says why:

| `reason`                                                     | Meaning                                                                                                                                                              |
| ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `failed rows do not have the table's columns or primary key` | The failed rows lack some columns. Or they hold the primary key, but the data source can't count for vowl (the second condition in [The match key](#the-match-key)). |
| `primary key has duplicate values`                           | A primary key value appears twice in the table.                                                                                                                      |
| `primary key uniqueness could not be checked`                | The data source could not say whether a key value repeats.                                                                                                           |

#### When two values count as the same {#how-values-are-compared}

Most values are simply equal or not. These are the cases that may surprise
you:

| Values in a match key column      | Same? | Why                                                       |
| --------------------------------- | ----- | --------------------------------------------------------- |
| empty (`NULL`) and empty (`NULL`) | Yes   | So two copies of a row with no quantity, like milk, match |
| `NaN` and `NaN`                   | Yes   | Same reason as empty values                               |
| `a` and `A`                       | No    | Even in a column set to ignore case                       |
| `0.0` and `-0.0`                  | No    | Some databases keep them apart                            |
| `[1, 2]` and `[1, 3]`             | No    | Lists and nested values must match item by item           |
| two times a nanosecond apart      | No    | Times must match exactly                                  |

vowl compares values this way both in the data source and on your machine.
This is tested on DuckDB, SQLite, Spark, Databricks and PostgreSQL, called
the **tested sources** in these pages. Another data source may treat `a` and
`A` as the same, so its numbers can be marked not exact.

### Attribution isn't always possible, and it costs extra compute

Attributing every failed row is not always possible. A check may return rows
without the match key, or values that no table row holds. And where it is
possible, it costs extra work: either the data source looks the failed rows
up in the table, or vowl downloads the table and looks them up on your
machine. A plain filter, which only keeps or drops rows, needs neither.

So vowl attributes the rows in the cheapest way it can, lets you turn
attribution off, and marks every number that could be off. A counted check
it can't attribute is left out of the row counts and flagged in
`checks_not_attributed`. See
[Counting Mechanisms](counting-mechanisms.md).

## How the numbers are put together

Once each check's failed rows are attributed, vowl counts each table once, in
five steps:

1. **Collect each check's attributed rows.** For each counted check, vowl
   finds how many copies of each failed row the table holds, using one of the
   [routes](counting-mechanisms.md#the-four-routes). A check that is not
   attributed adds nothing.
2. **Merge them into one list.** Using the match key, vowl lists each
   attributed row once, with the checks that caught it.
3. **Count the table's rows**, to get the total.
4. **Add up** the rows that failed per dimension and per schema.
5. **Flag numbers that may be off** as not
   [exact](counting-mechanisms.md#exact-numbers).

### Merging the attributed rows {#merging-the-failed-rows}

A **row** is one line of your table. A **distinct row** stands for all the
rows that hold the same match key values. Rows 2 and 5 are two rows but one
distinct row:

```text
 Rows of the table                        Distinct rows
 ─────────────────────────────            ──────────────────────
 row 2   2  bread  -2.00  1   ─┐
                               ├──────▶   bread    2 copies
 row 5   2  bread  -2.00  1   ─┘
 row 3   3  milk    0.00      ────────▶   milk     1 copy
```

vowl works with distinct rows because a database can't point to "row 2" or
"row 5" when they hold the same values. It can only say "this bread row
appears twice".

vowl puts the attributed rows of all checks into one list, with each distinct row
once. For each one it keeps the checks that caught it and its number of
copies. For the copies it takes the highest number any check found, not the
sum. Two checks found the 2 bread rows, and adding them would give 4:

| Check                    | bread        | milk       |
| ------------------------ | ------------ | ---------- |
| `price_must_be_positive` | 2 copies     | 1 copy     |
| `quantity_is_filled`     |              | 1 copy     |
| `order_id_is_unique`     | 2 copies     |            |
| **Merged**               | **2 copies** | **1 copy** |

The merged list:

| Distinct row | Checks that caught it                          | Copies |
| ------------ | ---------------------------------------------- | ------ |
| bread        | `price_must_be_positive`, `order_id_is_unique` | 2      |
| milk         | `price_must_be_positive`, `quantity_is_filled` | 1      |

So 3 rows of the table failed: 2 copies of bread and 1 of milk.

??? note "Where the merge runs"

    - If every check was counted in the data source, the data source merges
      everything and sends back only numbers. Very large contracts are split
      into batches of 250 checks.
    - Otherwise the data source merges its checks, and vowl merges the rest
      on your machine with the same rule.
    - Rarely, two rows that the data source kept apart look equal on your
      machine, such as `1` and `1.0` in SQLite. vowl keeps them apart and
      adds their copies.

### Counting the table's rows

The total is the number of rows in the table, after your filter conditions.
It is never capped by `max_failed_rows`.

??? note "Where the total comes from"

    vowl uses the first of these it has:

    1. The count from the merge query, if everything ran in one query.
    2. The row count of the downloaded table, if vowl downloaded it.
    3. A count taken earlier in the run, if `max_rows_for_statistics` did not
       cap it.
    4. A new count from the data source.
    5. The capped count from earlier in the run. The numbers are then not
       exact.

    A total of 0 is counted again, because a failed count also returns 0.

### Adding up per dimension and schema

From the merged list:

- **`failed_rows`** is the copies of every distinct row caught by a failed
  check.
- **Tolerated rows** are the copies of every distinct row caught only by
  checks that passed. See
  [Tolerated rows](failed-row-results.md#tolerated-rows).
- **Passed rows** are the total minus `failed_rows`.
- **Pass rate** is the passed rows divided by the total.
- **Per dimension**, the same, using only that dimension's checks.

| Grain        | Checks used              | Rows that failed | Pass rate |
| ------------ | ------------------------ | ---------------- | --------- |
| orders       | all three                | 2, 3, 5 → 3      | 2 / 5     |
| conformity   | `price_must_be_positive` | 2, 3, 5 → 3      | 2 / 5     |
| completeness | `quantity_is_filled`     | 3 → 1            | 4 / 5     |
| uniqueness   | `order_id_is_unique`     | 2, 5 → 2         | 3 / 5     |

A dimension with no counted checks, a dimension whose counted checks are all
not attributed, or an empty table, shows N/A, not 100%.

`get_row_quality_df()` has these columns, per schema or per dimension:

| Column                                 | Meaning                                         |
| -------------------------------------- | ----------------------------------------------- |
| `total_rows`                           | Rows in the table, after your filter conditions |
| `failed_rows`                          | Rows that failed at least one counted check     |
| `tolerated_rows`                       | Rows caught only by checks that passed          |
| `passed_rows`                          | `total_rows` minus `failed_rows`                |
| `pass_rate`                            | `passed_rows` divided by `total_rows`, 0 to 1   |
| `exact`                                | Whether the numbers are exact                   |
| `checks_counted`, `checks_not_counted` | How many checks were counted, and how many not  |
| `checks_not_attributed`                | How many counted checks were not attributed     |

An empty value means there is no number to give.
