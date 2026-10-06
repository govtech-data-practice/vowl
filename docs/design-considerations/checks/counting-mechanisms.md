---
title: Counting Mechanisms
description: >-
  How vowl attributes each check's failed rows to the source table, the three
  routes it uses, what each costs, and when the numbers are exact.
---

# Counting Mechanisms

This page explains how vowl attributes each row-level check's failed rows, and
what each way costs. vowl picks a **route** for each check.

By default vowl attributes the failed rows of every row-level check that
failed. A check that passed takes no route and runs no extra query, unless
[`attribute_tolerated_rows`](../../run-settings.md#attribute_tolerated_rows)
is set. This work runs only when you ask for DQ metrics, through
`get_dq_metrics()`, `get_dq_metrics_df()`, `save_dq_metrics()` or
`export_otel()`.

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
   [When two values count as the same](how-attributed-rows-work.md#how-values-are-compared)).
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

One more rule applies. When the table is downloaded anyway and the data
source is not a tested source, the plain filters are attributed in the
downloaded table too, and take `client_lookup`.

Under `attribute_tolerated_rows=True`, a passed check is routed like a failed
one. If it needs the table downloaded, the failed checks of that table can
move to `client_lookup` too.

A check can still end with no route, not attributable, when something goes
wrong. See [When something goes wrong](#fallbacks).

## Trade-offs {#when-exact}

| What vowl downloads                                                  | Extra work in the data source                                               | Memory on your machine | Exact   |
| -------------------------------------------------------------------- | --------------------------------------------------------------------------- | ---------------------- | ------- |
| The whole table, only where the data source can't attribute the rows | An attribution query, which also attributes the checks that are not filters | Low on a tested source | Yes[^6] |

A table whose checks are all plain filters on a source that can count is
never downloaded to count it.

**Whether each check's number is exact, by route:**

| Route              | Plain filter | `DISTINCT` or join | Returns changed values | Untested source | Failed rows cut short by `max_failed_rows` |
| ------------------ | ------------ | ------------------ | ---------------------- | --------------- | ------------------------------------------ |
| `server_predicate` | Yes          | Not used           | Yes[^1]                | Approx[^2]      | Yes[^3]                                    |
| `server_lookup`    | Yes          | Yes                | Approx[^4]             | Not used        | Yes[^3]                                    |
| `client_lookup`    | Yes          | Yes                | Approx[^4]             | Yes             | Not attributable[^5]                       |

[^1]:
    It counts the table rows the filter keeps, so the values the check
    returns don't matter.

[^2]:
    The data source may treat different values as equal, such as `a` and
    `A`. Exact only when every row-level check of the table takes
    `server_predicate`. Once the table is downloaded, the plain filters move
    to `client_lookup` instead.

[^3]: It counts in the data source, so the cap doesn't apply.

[^4]:
    Marked approximate whenever the check's SELECT list holds anything other
    than plain table columns. Its rows are still attributed and counted.

[^5]:
    The check is left out of the row counts, which become approximate. See
    [When something goes wrong](#fallbacks).

[^6]:
    Unless one of the table's checks is approximate, or a row-level check with
    failed rows is not attributable. Then the table, its dimensions and the run
    total are approximate too.

### Every case {#route-scenarios}

Each cell is the route, then whether the check's number is exact. "Can
count" refers to question 1.

| Check                                    | Data source                 | Default                                                                                                              |
| ---------------------------------------- | --------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| Plain filter                             | Tested, can count           | `server_predicate`, exact                                                                                            |
| Plain filter                             | Not tested, can count       | `server_predicate`, exact only if every row-level check takes it. `client_lookup`, exact, if the table is downloaded |
| Not a plain filter (`DISTINCT`, join...) | Tested, can count           | `server_lookup`, exact                                                                                               |
| Not a plain filter                       | Not tested, can count       | `client_lookup`, exact                                                                                               |
| Plain filter                             | Can't count                 | `client_lookup`, exact                                                                                               |
| Not a plain filter, or reads two sources | Can't count, or two sources | `client_lookup`, exact                                                                                               |

## The three routes {#the-four-routes}

A route is how vowl finds how many copies of each failed row the table holds.
For `price_must_be_positive`, that is "bread 2 copies, milk 1 copy".

On `server_predicate` and `server_lookup`, vowl puts the row queries of a
table's checks into one **attribution query** and runs it in the data source.
It attributes the rows there, and only the numbers come back.

| Route                                         | When it's used                                                       | What vowl downloads                 | Exact                                        |
| --------------------------------------------- | -------------------------------------------------------------------- | ----------------------------------- | -------------------------------------------- |
| [`server_predicate`](#route-server-predicate) | The query is a plain filter, so the data source just counts          | Only numbers                        | Yes on a tested source                       |
| [`server_lookup`](#route-server-lookup)       | Not a plain filter. The data source attributes the rows to the table | Only numbers                        | Yes, unless the check returns changed values |
| [`client_lookup`](#route-client-lookup)       | Not a plain filter. vowl attributes the rows in the downloaded table | The whole table and the failed rows | Yes, unless the check returns changed values |

The `route` column of `get_dq_metrics_df(by="check")` shows each check's
route. It is empty for a check that is not row-level, or not attributable on a
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
 │ 1. Run the row query on orders                   │
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

- All three row-level checks of the example take this route.
- On a source that is not tested, the numbers are exact only when every
  row-level check of the table takes `server_predicate`.

### `server_lookup` {#route-server-lookup}

`DISTINCT` dropped a copy, so the query's rows can't be counted as they are.
Instead, the data source uses the query only to learn _which_ rows failed. It
then attributes them by looking them up in the table, and counts the real
copies. Still only numbers come back.

```text
 IN THE DATA SOURCE
 ┌──────────────────────────────────────────────────┐
 │ 1. Run the row query                             │
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
  [match key](how-attributed-rows-work.md#the-match-key), or they can't be
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
  rows. The download happens only when you ask for DQ metrics.

!!! note "Checks that return changed values"

    On both lookup routes, a check whose SELECT list holds anything other than
    plain table columns or `*`, such as `price * 1.5` or `upper(item)`, is
    marked not exact. So is a query vowl can't parse. Such a check may return
    values that no table row holds, or that happen to equal another row's
    values, as `lower('A')` matches a row holding `a`. vowl still counts the
    rows that can be attributed. When some failed rows can't be attributed,
    the `reason` is `some failed rows could not be attributed to a table row`. On
    `server_lookup`, finding them costs one more query per table.

## When something goes wrong {#fallbacks}

Sometimes vowl can't attribute a row-level check on this run. The check is then
**not attributable**: its `route` is empty and it adds nothing to the row
counts, and `checks_not_attributable` counts it. When its scalar count is
above 0, the numbers of its dimension, its schema and the run are marked not
exact. The other checks of the table keep their routes. The `reason` says what
happened:

| What happened                                          | `reason`                                              |
| ------------------------------------------------------ | ----------------------------------------------------- |
| The table could not be downloaded                      | `the table could not be exported`                     |
| The table's values could not be turned into match keys | `the failed rows could not be turned into match keys` |
| The check's failed rows could not be fetched           | `the failed rows could not be fetched`                |
| `max_failed_rows` cut the check's failed rows short    | `truncated by max_failed_rows`                        |
| The check's row query failed in the data source        | `its row query failed in the data source`             |

The first four mostly happen to a check meant for `client_lookup`.
`the failed rows could not be turned into match keys` can also happen to a `server_predicate`
check that is attributed to the table on a source that is not tested. The last
can also happen when the data source attributes the rows.

These checks are not attributable on this run only. Their failed rows are still
annotated where vowl can download the table, so they are not residues. A
check that is never attributable is a different case. See
[Attributed rows](check-results.md#from-query-output-to-row-counts).

## The approximate flag {#exact-numbers}

Every number has an `approximate` flag. It is `True` when the number could be off.
The summary then shows **(approx.)** after it, with the number of checks not
attributable when there are any, and the OTEL `vowl.validate` span sets
`vowl.row_quality.approximate`. To find the check that caused it, look at the
`approximate` and `reason` columns of `get_dq_metrics_df(by="check")`, or the
same attributes on its `vowl.check` span. The DQ metrics do not carry the flag.

A number is approximate when:

| Cause                                                                                      |
| ------------------------------------------------------------------------------------------ |
| A row-level check ended in `ERROR`, so it may hide failed rows                             |
| A row-level check that may have failed rows was not attributable, so its rows are left out |
| A check returns changed values, such as `price * 1.5`, that may match no row of the table  |
| The data source is not a tested source and may treat different values as equal             |
| vowl doesn't know the table's columns, or a value could not be compared during the merge   |
| The total was capped, or is lower than the rows one check found                            |

## Where to look in the code

| Step                             | Code in `src/vowl/validation/`                                                                                             |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Pick the row-level checks        | `row_quality/selection.py`                                                                                                 |
| Pick the match key               | `_choose_key` in `row_quality/__init__.py`, `row_quality/match_key.py`                                                     |
| Pick the route                   | `_assign_route` in `row_quality/__init__.py`, `row_quality/certify.py`                                                     |
| `client_lookup`                  | `_fetch`, `_export_table` and `_run_onto_table` in `row_quality/__init__.py`, `merge_onto_table` in `row_quality/merge.py` |
| Not attributable                 | `_leave_unattributed`, `_leave_lookup_unattributed` and `_check_match_key` in `row_quality/__init__.py`                    |
| Merging the attributed rows      | `row_quality/pushdown.py` (data source), `row_quality/merge.py` (your machine)                                             |
| Counting the table's rows        | `_total_rows` in `row_quality/__init__.py`                                                                                 |
| Adding up, approximate flags     | `row_quality/rollup.py`                                                                                                    |
| DQ metrics                       | `dq_metrics.py`                                                                                                            |
| Annotating                       | `get_annotated_output` in `result.py`                                                                                      |
| Comparing values on your machine | `row_keys` in `result_row_quality.py`                                                                                      |

The full design, with the reasoning and measurements, is in
`design/row-quality-statistics.md`.
