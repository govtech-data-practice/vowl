---
title: Counting Mechanisms
description: >-
  How vowl attributes each check's failed rows to the source table, the four
  routes it uses, what each costs, and when the numbers are exact.
---

# Counting Mechanisms

This page is about [attributed rows](failed-row-results.md#failed-row-results).
[How Rows Are Counted](how-rows-are-counted.md) explained that
counts are exact only when each failed row is attributed to the source table,
and that this costs extra work. This page explains how vowl does that work.
vowl picks a **route** for each check.

By default vowl attributes the failed rows of every counted check. To skip
that work, for example on very large tables, set
[`disable_table_attributed_counts`](../../run-settings.md#disable_table_attributed_counts):

```python
from vowl import ValidationConfig

config = ValidationConfig(disable_table_attributed_counts=True)
```

Every counted check then takes `server_scalar` and is not attributed, and
the numbers can be approximate. The check's status and scalar count stay the
same, and so does the annotated output.

## Which route a check takes

vowl asks up to three questions about each check:

1. **Can the data source count for this check?** vowl must know the table's
   column types, a test query must run without an error, and the check must
   read only this table's data source. When it can't, the `reason` is
   `data source does not support pushdown` or
   `checks tables from more than one data source`.
2. **Is the query a plain filter?** A plain filter only keeps or drops rows,
   like `SELECT * FROM orders WHERE price <= 0`, so every row it returns is
   already a row of the table. `DISTINCT`, `GROUP BY`, `LIMIT`, a join or
   `RANDOM()` make a query not a plain filter. The `reason` then starts with
   `not certified for pushdown`.
3. **Is the data source a tested source?** `server_lookup` attributes rows by
   value, which is only safe where values are compared exactly (see
   [When two values count as the same](how-rows-are-counted.md#how-values-are-compared)).
   The tested sources are DuckDB, SQLite, Spark, Databricks and PostgreSQL.

```text
 1. Can the data source count?
    │
    ├── No ──────────────────────────────────▶  client_lookup
    │
    Yes
    ▼
 2. Is the query a plain filter?
    │
    ├── Yes ─────────────────────────────────▶  server_predicate
    │
    No
    ▼
 3. Is the data source a tested source?
    │
    ├── Yes ─────────────────────────────────▶  server_lookup
    │
    No ──────────────────────────────────────▶  client_lookup
```

With `disable_table_attributed_counts=True`, vowl asks no questions:

```text
 Any counted check  ─────────────────────────▶  server_scalar
```

One more rule applies. When the table is downloaded anyway and the data
source is not a tested source, the plain filters are attributed in the
downloaded table too, and take `client_lookup`.

A check can still end with no route, not attributed, when something goes
wrong. See [When something goes wrong](#fallbacks).

## Trade-offs {#when-exact}

| `disable_table_attributed_counts` | What vowl downloads                                                  | Extra work in the data source                                         | Memory on your machine | Exact                                                  |
| --------------------------------- | -------------------------------------------------------------------- | --------------------------------------------------------------------- | ---------------------- | ------------------------------------------------------ |
| `False` (default)                 | The whole table, only where the data source can't attribute the rows | A counting query that also attributes the checks that are not filters | Low on a tested source | Yes[^7]                                                |
| `True`                            | Nothing                                                              | None                                                                  | None                   | Approximate once two checks of a table caught rows[^8] |

A table whose checks are all plain filters on a source that can count is
never downloaded to count it.

**Whether each check's number is exact, by route:**

| Route              | Plain filter | `DISTINCT` or join | Returns changed values | Untested source | Failed rows cut short by `max_failed_rows` |
| ------------------ | ------------ | ------------------ | ---------------------- | --------------- | ------------------------------------------ |
| `server_predicate` | Yes          | Not used           | Yes[^1]                | Approx[^2]      | Yes[^3]                                    |
| `server_lookup`    | Yes          | Yes                | Approx[^4]             | Not used        | Yes[^3]                                    |
| `client_lookup`    | Yes          | Yes                | Approx[^4]             | Yes             | Not attributed[^5]                         |
| `server_scalar`    | Yes          | Approx[^6]         | Approx[^6]             | Yes             | Yes[^3]                                    |

[^1]:
    It counts the table rows the filter keeps, so the values the check
    returns don't matter.

[^2]:
    The data source may treat different values as equal, such as `a` and
    `A`. Exact only when every counted check of the table takes
    `server_predicate`. Once the table is downloaded, the plain filters move
    to `client_lookup` instead.

[^3]:
    It counts in the data source or uses the scalar count, so the cap
    doesn't apply.

[^4]:
    Marked approximate whenever the check's SELECT list holds anything other
    than plain table columns. Its rows are still attributed and counted.

[^5]:
    The check is left out of the row counts, which become approximate. See
    [When something goes wrong](#fallbacks).

[^6]:
    Only a plain filter's scalar count matches the rows it caught. A
    `COUNT(DISTINCT ...)` check counts values, not rows, so it is not exact
    either, with the `reason`
    `the check counts distinct values, not rows`.

[^7]:
    Unless one of the table's checks is approximate, or a counted check that
    caught rows is not attributed. Then the table, its dimensions and the run
    total are approximate too.

[^8]:
    Scalar counts can't tell which rows overlap. The sum is capped at the
    table's row count. With only one check that caught rows, the table is
    exact if that check is.

### Every case {#route-scenarios}

Each cell is the route, then whether the check's number is exact. "Can
count" refers to question 1. Under `disable_table_attributed_counts=True`
every counted check is not attributed, and "exact" refers to its scalar
count. A scalar count of 0 is always exact.

| Check                                    | Data source                 | Default                                                                                                            | `disable_table_attributed_counts=True` |
| ---------------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------ | -------------------------------------- |
| Plain filter                             | Tested, can count           | `server_predicate`, exact                                                                                          | `server_scalar`, exact                 |
| Plain filter                             | Not tested, can count       | `server_predicate`, exact only if every counted check takes it. `client_lookup`, exact, if the table is downloaded | `server_scalar`, exact                 |
| Not a plain filter (`DISTINCT`, join...) | Tested, can count           | `server_lookup`, exact                                                                                             | `server_scalar`, not exact             |
| Not a plain filter                       | Not tested, can count       | `client_lookup`, exact                                                                                             | `server_scalar`, not exact             |
| Plain filter                             | Can't count                 | `client_lookup`, exact                                                                                             | `server_scalar`, exact                 |
| Not a plain filter, or reads two sources | Can't count, or two sources | `client_lookup`, exact                                                                                             | `server_scalar`, not exact             |

## The four routes

A route is how vowl finds how many copies of each failed row the table holds.
For `price_must_be_positive`, that is "bread 2 copies, milk 1 copy".

| Route                                         | When it's used                                                                                   | What vowl downloads                 | Exact                                        |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------ | ----------------------------------- | -------------------------------------------- |
| [`server_predicate`](#route-server-predicate) | The query is a plain filter, so the data source just counts                                      | Only numbers                        | Yes on a tested source                       |
| [`server_lookup`](#route-server-lookup)       | Not a plain filter. The data source attributes the rows to the table                             | Only numbers                        | Yes, unless the check returns changed values |
| [`client_lookup`](#route-client-lookup)       | Not a plain filter. vowl attributes the rows in the downloaded table                             | The whole table and the failed rows | Yes, unless the check returns changed values |
| [`server_scalar`](#route-server-scalar)       | Only with `disable_table_attributed_counts=True`. vowl uses the count the check already returned | Nothing                             | Only for a plain filter, and see the warning |

The `route` column of `get_row_quality_df(by="check")` shows each check's
route. It is empty for a check that is not counted, or not attributed on a
default run. The `reason` column says why the check is not on
`server_predicate`.

The routes below use `price_must_be_positive`, a plain filter, and one extra
check, `distinct_bad_prices`, which is not:

```sql
-- price_must_be_positive
SELECT * FROM orders WHERE price <= 0

-- distinct_bad_prices
SELECT DISTINCT * FROM orders WHERE price <= 0
```

| Row | item  | price | Returned by `price_must_be_positive` | Returned by `distinct_bad_prices` |
| --- | ----- | ----- | ------------------------------------ | --------------------------------- |
| 2   | bread | -2.00 | ✗                                    | ✗                                 |
| 3   | milk  | 0.00  | ✗                                    | ✗                                 |
| 5   | bread | -2.00 | ✗                                    | dropped by `DISTINCT`             |

Both checks should count 3 rows.

### `server_predicate` {#route-server-predicate}

The fastest route. A plain filter already returns every copy, so its rows are
already attributed and the data source only has to count them.

```text
 IN THE DATA SOURCE
 ┌──────────────────────────────────────────────────┐
 │ 1. Run the failed rows query on orders           │
 │      price <= 0   →   bread, milk, bread         │
 │                                                  │
 │ 2. Group the failed rows by the match key        │
 │      bread   2 copies                            │
 │      milk    1 copy                              │
 └─────────────────────────┬────────────────────────┘
                           │  only the counts
                           ▼
 ON YOUR MACHINE      bread 2, milk 1
```

- All three counted checks of the example take this route.
- On a source that is not tested, the numbers are exact only when every
  counted check of the table takes `server_predicate`.

### `server_lookup` {#route-server-lookup}

`DISTINCT` dropped a copy, so the query's rows can't be counted as they are.
Instead, the data source uses the query only to learn _which_ rows failed. It
then attributes them by looking them up in the table, and counts the real
copies. Still only numbers come back.

```text
 IN THE DATA SOURCE
 ┌──────────────────────────────────────────────────┐
 │ 1. Run the failed rows query                     │
 │      SELECT DISTINCT ...   →   bread, milk       │
 │      (only 1 bread)                              │
 │                                                  │
 │ 2. Look each one up in orders by the match key   │
 │      bread   →   bread, bread                    │
 │      milk    →   milk                            │
 │                                                  │
 │ 3. Group by the match key                        │
 │      bread   2 copies                            │
 │      milk    1 copy                              │
 └─────────────────────────┬────────────────────────┘
                           │  only the counts
                           ▼
 ON YOUR MACHINE      bread 2, milk 1
```

- On a tested source, `distinct_bad_prices` takes this route, with the
  `reason` `not certified for pushdown: uses DISTINCT`.
- It only runs on a tested source.
- The failed rows must hold the
  [match key](how-rows-are-counted.md#the-match-key), or they can't be
  attributed.
- Values that are equal under their column type but print differently, such
  as `-0.0` and `0.0`, or the intervals `1 month` and `30 days`, are kept
  apart.

### `client_lookup` {#route-client-lookup}

The same idea as `server_lookup`, but on your machine. vowl downloads the
table once and attributes each failed row to it. It works on any data source,
and for checks that read two data sources.

```text
 IN THE DATA SOURCE
 ┌──────────────────────────────────────────────────┐
 │ 1. The check already ran and returned its        │
 │    failed rows                                   │
 │      SELECT DISTINCT ...   →   bread, milk       │
 │                                                  │
 │ 2. vowl downloads orders once                    │
 └─────────────────────────┬────────────────────────┘
                           │  the failed rows and the table
                           ▼
 ON YOUR MACHINE
 ┌──────────────────────────────────────────────────┐
 │ Look each failed row up in orders by the         │
 │ match key                                        │
 │      bread   →   rows 2 and 5                    │
 │      milk    →   row 3                           │
 └─────────────────────────┬────────────────────────┘
                           ▼
                      bread 2, milk 1
```

- On a source that is not tested, `distinct_bad_prices` takes this route.
- A join that returns a row twice counts it once, because the table holds it
  once.
- When the table is downloaded, the table's total is the downloaded row count.
- The annotated output reuses the same download, so vowl downloads each table
  at most once per run, and counting and annotating flag the same rows.
- The whole table is held in memory. At 6 columns this took about 3 s and 1
  to 1.5 GiB for 1 million rows, and about 13 s and 3.5 GiB for 5 million
  rows. On large tables, set `disable_table_attributed_counts=True`.

!!! note "Checks that return changed values"

    On both lookup routes, a check whose SELECT list holds anything other than
    plain table columns or `*`, such as `price * 1.5` or `upper(item)`, is
    marked not exact. So is a query vowl can't parse. Such a check may return
    values that no table row holds, or that happen to equal another row's
    values, as `lower('A')` matches a row holding `a`. vowl still counts the
    rows that can be attributed. When some failed rows match no table row,
    the `reason` is `some failed rows match no table row`. On
    `server_lookup`, finding them costs one more query per table.

### `server_scalar` {#route-server-scalar}

The cheapest route. It is used only with
`disable_table_attributed_counts=True`, and then every counted check takes
it. vowl uses each check's scalar count, the number that decided pass or
fail, as it is, without attributing any row. Every counted check is then not
attributed, so `checks_not_attributed` equals `checks_counted`.

```text
 IN THE DATA SOURCE
 ┌──────────────────────────────────────────────────┐
 │ The check already ran and returned its           │
 │ scalar count                                     │
 │      SELECT DISTINCT ...   →   2                 │
 └─────────────────────────┬────────────────────────┘
                           │  nothing more
                           ▼
 ON YOUR MACHINE      2 rows   (the table has 3)   →   not exact
```

- A plain filter's scalar count is every row it keeps, so it is exact. So is
  a scalar count of 0. `distinct_bad_prices` counted 2, not 3, so it is
  marked not exact.
- A `COUNT(DISTINCT item)` check counts values, not the rows that hold them,
  so it is marked not exact, with the `reason`
  `the check counts distinct values, not rows`.
- The scalar count is never cut short by `max_failed_rows`.

!!! warning "Scalar counts can't tell which rows overlap"

    A scalar count says how many rows failed, not which ones. When two checks
    of a table caught rows, vowl can't tell whether they caught the same
    rows. It adds the scalar counts, caps the sum at the table's row count and
    marks the table not exact. With only one check that caught rows, the
    table is exact if that check is.

## When something goes wrong {#fallbacks}

Sometimes vowl can't attribute a counted check on this run. The check is then
**not attributed**: its `route` is empty and it adds nothing to the row
counts, and `checks_not_attributed` counts it. When its scalar count is
above 0 and its rows are in scope of
[`row_issue_scope`](../../run-settings.md#row_issue_scope), the numbers of its
dimension, its schema and the run are marked not exact. A passed check that
is left out leaves `tolerated_rows` empty instead.
The other checks of the table keep their routes. The `reason` says what
happened:

| What happened                                                                                    | `reason`                                         |
| ------------------------------------------------------------------------------------------------ | ------------------------------------------------ |
| The table could not be downloaded                                                                | `the table could not be exported`                |
| The table's values could not be turned into match keys                                           | `the failed rows could not be keyed`             |
| The check's failed rows could not be fetched                                                     | `the failed rows could not be fetched`           |
| `max_failed_rows` cut the check's failed rows short                                              | `truncated by max_failed_rows`                   |
| The check passed within its tolerance, so under `"failed_checks"` vowl skipped fetching its rows | `tolerated rows not fetched under failed_checks` |
| The counting query failed in the data source                                                     | `the counting query failed in the data source`   |

The first five mostly happen to a check meant for `client_lookup`.
`the failed rows could not be keyed` can also happen to a `server_predicate`
check that is matched onto the table on a source that is not tested. The last
can also happen when the data source counts.

These checks are not attributed on this run only. Their failed rows are still
annotated where vowl can download the table, so they are not residues. A
check that can never be attributed is a different case. See
[From query output to row counts](failed-row-results.md#from-query-output-to-row-counts).

## The exact flag {#exact-numbers}

Every number has an `exact` flag. It is `False` when the number could be off.
The summary then shows **(approx.)** after it, with the number of checks not
attributed when there are any, and the DQ metrics set
`vowl.row_quality.exact`. To find the check that caused it, look at the
`exact` and `attributed` columns of `get_row_quality_df(by="check")`.

A number is not exact when:

| Cause                                                                                                                                                              |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| A counted check ended in `ERROR`, so it may hide bad rows                                                                                                          |
| A counted check that may have caught rows was not attributed, so its rows are left out                                                                             |
| With `disable_table_attributed_counts=True`, a scalar count is not a plain filter's row count, or two checks overlap (see [`server_scalar`](#route-server-scalar)) |
| A check returns changed values, such as `price * 1.5`, that may match no row of the table                                                                          |
| The data source is not a tested source and may treat different values as equal                                                                                     |
| vowl doesn't know the table's columns, or a value could not be compared during the merge                                                                           |
| The total was capped, or is lower than the rows one check found                                                                                                    |

## Where to look in the code

| Step                             | Code in `src/vowl/validation/`                                                                                             |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Pick the counted checks          | `row_quality/selection.py`                                                                                                 |
| Pick the match key               | `_choose_key` in `row_quality/__init__.py`, `row_quality/mergeable.py`                                                     |
| Pick the route                   | `_assign_route` in `row_quality/__init__.py`, `row_quality/certify.py`                                                     |
| `client_lookup`                  | `_fetch`, `_export_table` and `_run_onto_table` in `row_quality/__init__.py`, `merge_onto_table` in `row_quality/merge.py` |
| `server_scalar`, not attributed  | `_run_scalars`, `_leave_unattributed`, `_leave_lookup_unattributed` and `_check_mergeable` in `row_quality/__init__.py`    |
| Merging the attributed rows      | `row_quality/pushdown.py` (data source), `row_quality/merge.py` (your machine)                                             |
| Counting the table's rows        | `_total_rows` in `row_quality/__init__.py`                                                                                 |
| Adding up, exact flags           | `row_quality/rollup.py`                                                                                                    |
| DQ metrics                       | `dq_metrics.py`                                                                                                            |
| Annotating                       | `get_annotated_output` in `result.py`                                                                                      |
| Comparing values on your machine | `row_keys` in `result_row_quality.py`                                                                                      |

The full design, with the reasoning and measurements, is in
`design/row-quality-statistics.md`.
