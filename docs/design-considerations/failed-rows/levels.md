---
title: Failed Rows at Each Level
description: >-
  How vowl turns each check's failed rows into row counts per check,
  dimension, schema and run, and how it annotates your table with them.
---

# Failed Rows at Each Level

!!! abstract "In short"

    - Each check finds its own **failed rows**.
    - vowl **counts** them. A row that fails two checks counts once.
    - vowl can also **annotate** your table. Each failed row gets a list of
      the checks it failed.
    - To know that two checks caught the same row, vowl compares a set of
      columns called the **match key**.

## The example on this page

The whole page uses one small `orders` table and three checks, the same
[example](queries.md#the-example-used-in-this-section) as the first page. A
✗ marks a row the check catches:

| Row | order_id | item  | price | quantity | `price_must_be_positive` | `quantity_is_filled` | `order_id_is_unique` |
| --- | -------- | ----- | ----- | -------- | ------------------------ | -------------------- | -------------------- |
| 1   | 1        | apple | 1.50  | 3        |                          |                      |                      |
| 2   | 2        | bread | -2.00 | 1        | ✗                        |                      | ✗                    |
| 3   | 3        | milk  | 0.00  |          | ✗                        | ✗                    |                      |
| 4   | 4        | eggs  | 3.20  | 2        |                          |                      |                      |
| 5   | 2        | bread | -2.00 | 1        | ✗                        |                      | ✗                    |

- `price_must_be_positive` (conformity): no price is zero or less.
- `quantity_is_filled` (completeness): every order has a quantity.
- `order_id_is_unique` (uniqueness): no `order_id` appears twice.

Rows 2 and 5 are exact copies of each other. Rows 2, 3 and 5 fail at least
one check. Rows 1 and 4 are clean.

The first page has a fourth check, `average_price_in_range`. It returns one
average, not rows, so it plays no part here.

## The four levels

| Level         | What it counts                                      | In the example                                                                | Where to see it                                                       |
| ------------- | --------------------------------------------------- | ----------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| **Check**     | The rows one check caught                           | `price_must_be_positive`: 3, `quantity_is_filled`: 1, `order_id_is_unique`: 2 | `actual` in the summary, `get_row_quality_df(by="check")`             |
| **Dimension** | The rows that failed any check of the dimension     | conformity: 3, completeness: 1, uniqueness: 2                                 | `get_row_quality_df(by="dimension")`                                  |
| **Schema**    | The rows of the table that failed any counted check | orders: 3 of 5 failed                                                         | **Passed Rows** in the summary, `get_row_quality_df()`                |
| **Run**       | The schema numbers added up                         | 3 of 5                                                                        | the run-level [DQ metrics](../../dq-metrics/understanding-metrics.md) |

The check numbers add up to 3 + 1 + 2 = 6, but only 3 rows failed (rows 2,
3 and 5). Every failed row was caught by two checks:

| Row       | Caught by                                      |
| --------- | ---------------------------------------------- |
| 2 (bread) | `price_must_be_positive`, `order_id_is_unique` |
| 3 (milk)  | `price_must_be_positive`, `quantity_is_filled` |
| 5 (bread) | `price_must_be_positive`, `order_id_is_unique` |

Above the check level, each row counts once.

The summary, `get_row_quality_df()` and the
[DQ metrics](../../dq-metrics/understanding-metrics.md#how-failed-rows-are-counted)
all show the same numbers.

??? note "Two numbers that can look wrong"

    **1. A check that uses `DISTINCT` can show two different numbers.**

    Take this check:

    ```sql
    SELECT DISTINCT * FROM orders WHERE price <= 0
    ```

    Three rows have a price of zero or less. `DISTINCT` keeps only one of
    the two identical bread rows:

    | Row | item  | price | In the table | Returned by the query |
    | --- | ----- | ----- | ------------ | --------------------- |
    | 2   | bread | -2.00 | ✗            | ✗                     |
    | 3   | milk  | 0.00  | ✗            | ✗                     |
    | 5   | bread | -2.00 | ✗            | dropped               |

    | Where you look                                     | Shows | Why                                       |
    | -------------------------------------------------- | ----- | ----------------------------------------- |
    | `actual` in the summary, and the check's DQ metric | 2     | The rows the query returned               |
    | `get_row_quality_df(by="check")`                   | 3     | The rows in the table, every copy counted |

    For a plain filter such as `WHERE price <= 0` without `DISTINCT`, both
    show 3.

    **2. "Non-unique Failed Rows" in the summary can count a row twice.**

    The summary splits each table's checks into two groups:

    - **Single Table**: checks that read only this table, like every check
      in the example above.
    - **Multi Table**: checks that also read another table, such as "every
      order's customer exists in `customers`".

    Under **Multi Table**, the summary shows **Non-unique Failed Rows**. It
    is the `actual` numbers of the failed Multi Table checks, simply added
    up. Unlike **Passed Rows**, it does not check whether two checks caught
    the same row.

    Say `orders` has two Multi Table checks, and a `customers` table holds
    customers 10 (in `SG`) and 30 (in `XX`, an unknown country):

    | order_id | customer_id | `customer_must_exist` | `customer_country_known` |
    | -------- | ----------- | --------------------- | ------------------------ |
    | 1        | 10          |                       |                          |
    | 2        | 20          | ✗ (no customer 20)    | ✗ (no country)           |
    | 3        | 30          |                       | ✗ (`XX`)                 |
    | 4        | 10          |                       |                          |

    The summary then shows:

    ```text
    orders:
      Overall:
        Passed Rows:            2 / 4 (50.0%)
      Multi Table:
        Non-unique Failed Rows: 3
    ```

    - **Passed Rows** says 2 rows failed: orders 2 and 3.
    - **Non-unique Failed Rows** says 3. That is 1 from
      `customer_must_exist` plus 2 from `customer_country_known`. Order 2 is
      counted twice, once for each check.

    So use **Passed Rows** for how many rows failed. Use **Non-unique Failed
    Rows** only as a rough size of the Multi Table problems.

## How counting and annotating work

Both start with the same two steps. Then they go their own way. Every step
marked ◆ uses the match key.

```text
     ┌─────────────────────────────────────────────────────────────┐
     │ SHARED, once per table                                      │
     │ 1. Pick the counted checks                                  │
     │ 2. Pick the match key: how vowl tells two failed rows       │
     │    are the same row                                         │
     │      • the primary key, if it is declared and unique        │
     │      • otherwise every column                               │
     └──────────────────────────────┬──────────────────────────────┘
                 ┌──────────────────┴───────────────────┐
                 ▼                                      ▼
  ┌─────────────────────────────┐        ┌─────────────────────────────┐
  │ COUNTING                    │        │ ANNOTATING                  │
  │ C1. Collect each check's    │        │ A1. Download the whole      │
  │     failed rows ◆           │        │     table                   │
  │ C2. List each distinct      │        │ A2. Keep the checks whose   │
  │     failed row once ◆       │        │     rows hold the key ◆     │
  │ C3. Count the table's rows  │        │ A3. Stop if a check was cut │
  │ C4. Add up per dimension    │        │     short by max_failed_rows│
  │     and per schema          │        │ A4. List each distinct      │
  │ C5. Flag numbers that are   │        │     failed row once ◆       │
  │     not exact               │        │ A5. Annotate the rows found │
  │                             │        │     in the list ◆           │
  │                             │        │ A6. Keep the rest as        │
  │                             │        │     residues                │
  └──────────────┬──────────────┘        └──────────────┬──────────────┘
                 ▼                                      ▼
  Passed Rows, get_row_quality_df(),      get_annotated_output(),
  DQ metrics                              save(output_mode="annotated")

  ◆ uses the match key
```

Step 1 is explained in [Which Checks Contribute Failed Rows](which-checks.md).
Step 2 is next.

|                        | Counting                                                                                                  | Annotating                                      |
| ---------------------- | --------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| Runs                   | In your data source if it can, else on your machine                                                       | On your machine                                 |
| Downloads              | Only numbers for plain filters. The whole table for other checks, unless `row_count_accuracy` says not to | Your whole table, plus each check's failed rows |
| With `max_failed_rows` | Can become not exact                                                                                      | Stops with an error ([Capping](capping.md))     |
| Cost on a large table  | Small when every check is a plain filter, or under `row_count_accuracy="fast"`                            | Large                                           |

When counting downloads the table, it is the same download annotating uses.
vowl downloads each table at most once per run, and counting and annotating
then compare the same columns. So they flag the same rows. See
[Row count accuracy](../../run-settings.md#row-count-accuracy).

## The match key

The match key is the set of columns vowl compares to tell whether two failed
rows are the same row.

By default, the match key is **every column**. It is the **primary key**
instead when all three of these are true:

1. The contract declares a primary key (`primaryKey: true`), and the key is
   not every column.
2. The data source can count for vowl. That means vowl knows the column
   types, and a test query runs without an error.
3. No key value appears twice in the table. vowl asks the data source.

Both choices give the same numbers. The primary key is faster, and it lets
vowl use checks that return only some columns.

!!! example "Why the primary key helps"

    The example `orders` table has no primary key. `order_id` is 2 on both
    bread rows, so it can't be one.

    Now imagine `orders` gets a new column, `line_id`, with no repeats, and
    it is declared as the primary key:

    | line_id | order_id | item  | price | quantity |
    | ------- | -------- | ----- | ----- | -------- |
    | 1       | 1        | apple | 1.50  | 3        |
    | 2       | 2        | bread | -2.00 | 1        |
    | 3       | 3        | milk  | 0.00  |          |
    | 4       | 4        | eggs  | 3.20  | 2        |
    | 5       | 2        | bread | -2.00 | 1        |

    Imagine also a new check, `price_is_negative`, that returns only two
    columns:

    ```sql
    SELECT line_id, price FROM orders WHERE price < 0
    ```

    | line_id | price |
    | ------- | ----- |
    | 2       | -2.00 |
    | 5       | -2.00 |

    What happens depends on the match key:

    - **If the match key is every column**, these rows lack `order_id`,
      `item` and `quantity`. vowl can't tell which rows they are. The check
      is not counted.
    - **If the match key is `line_id`**, vowl knows these are rows 2 and 5,
      the two bread rows. The check is counted, and the bread rows still
      count once each.

A check whose failed rows lack the match key columns is not counted. Its
failed rows become a **residue** (see [A6](#a6-keep-the-rest-as-residues)).
The `reason` column of `get_row_quality_df(by="check")` says why:

| Reason                                                       | Meaning                                                                                                 |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| `failed rows do not have the table's columns or primary key` | The failed rows lack some columns. Or they have the primary key, but vowl can't use it (point 2 above). |
| `primary key has duplicate values`                           | A primary key value appears twice in the table.                                                         |
| `primary key uniqueness could not be checked`                | The data source could not say whether a key value repeats.                                              |

### When two rows count as the same {#how-values-are-compared}

Two rows are the same row when every match key column holds the same value.
That is how vowl knows rows 2 and 5 are copies, and that two checks caught
the same bread row.

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
On DuckDB, SQLite, Spark, Databricks and PostgreSQL, this is tested. On other
data sources, the data source may treat `a` and `A` as the same, so the
numbers are marked not exact.

## Counting the row counts

Counting runs once per table, in five steps.

### C1. Collect each check's failed rows

For each counted check, vowl needs one thing: how many copies of each failed
row the table holds. For `price_must_be_positive`, that is "bread 2 copies,
milk 1 copy".

The cheapest way is to let the data source count and send back only the
numbers. Two things can get in the way:

- **The check's query may return the wrong number of copies.** A plain
  filter returns every copy. But a query with `DISTINCT` drops copies, and a
  join can repeat them. Then its rows can't be counted as they are.
- **The data source may not be able to do the counting.** For example, the
  check reads tables from two different data sources, so neither one holds
  all the data.

Each route is the answer to one of these cases:

| Route                                                 | Why it exists                                                               | What vowl downloads                 | Exact                                                                                                                  |
| ----------------------------------------------------- | --------------------------------------------------------------------------- | ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| [`server_predicate`](#route-server-predicate)         | The fast path. The query returns every copy, so just count them.            | Only numbers                        | Yes on the 5 tested sources. Elsewhere the data source may treat different values as equal, so see the exception below |
| [`client_lookup`](#route-client-lookup)               | The query may return the wrong copies, so recount from the downloaded table | The whole table and the failed rows | Yes, unless the check returns changed values, such as `price * 1.5`, that no table row holds                           |
| [`server_lookup`](#route-server-lookup)               | The query may return the wrong copies, so recount from the table            | Only numbers                        | Yes, unless the check returns changed values that no table row holds. It only runs on the 5 tested sources             |
| [`client_returned_rows`](#route-client-returned-rows) | The data source can't count, so vowl counts on your machine                 | The failed rows                     | Only if the query returned every copy, once                                                                            |

The 5 tested sources are DuckDB, SQLite, Spark, Databricks and PostgreSQL.

#### Which route a check takes

vowl asks up to three questions about each check. The questions are the same
at every level. Only some answers lead to a different route:

- **Question 1. Can the data source do the counting for this check?** This
  means vowl knows the table's column types, a test query runs without an
  error, and the check reads only this table's data source. See
  [the match key](#the-match-key), point 2.
- **Question 2. Is the check's query a plain filter?** A plain filter only
  keeps or drops rows, like `SELECT * FROM orders WHERE price <= 0`.
  `DISTINCT`, `GROUP BY`, `LIMIT`, a join or `RANDOM()` make a query not a
  plain filter.
- **Question 3. Is the data source one of the 5 tested ones?**
  `server_lookup` looks rows up by value. That is only safe where values are
  compared exactly, so `a` never matches `A` (see
  [When two rows count as the same](#how-values-are-compared)).

`"accurate"` (the default) downloads the table for every check the data source
can't count as a plain filter. It never asks question 3:

```text
 1. Can the data source do the counting?
    │
    ├── No ──────────────────────────────────▶  client_lookup
    │
    Yes
    ▼
 2. Is the query a plain filter?
    │
    ├── Yes ─────────────────────────────────▶  server_predicate
    │
    No ──────────────────────────────────────▶  client_lookup
```

`"balanced"` counts in the data source where it safely can, and downloads the
table only where it can't:

```text
 1. Can the data source do the counting?
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
 3. Is the data source one of the 5 tested ones?
    │
    ├── Yes ─────────────────────────────────▶  server_lookup
    │
    No ──────────────────────────────────────▶  client_lookup
```

`"fast"` never downloads the table. It asks the same questions as
`"balanced"`, but where `"balanced"` downloads the table, it counts the rows the
check returned:

```text
 1. Can the data source do the counting?
    │
    ├── No ──────────────────────────────────▶  client_returned_rows,
    │                                           exact only for a plain filter
    Yes
    ▼
 2. Is the query a plain filter?
    │
    ├── Yes ─────────────────────────────────▶  server_predicate
    │
    No
    ▼
 3. Is the data source one of the 5 tested ones?
    │
    ├── Yes ─────────────────────────────────▶  server_lookup
    │
    No ──────────────────────────────────────▶  client_returned_rows,
                                                not exact
```

So `"balanced"` and `"fast"` differ only at the two "No" answers that end in
`client_lookup` or `client_returned_rows`. On a tested data source that can
count, they take the same routes.

When the table is downloaded on a data source that is not one of the 5 tested
ones, the plain filters are matched onto the downloaded table too, and take
`client_lookup`.

A check meant for `client_lookup` can still end on another route, when the
download or the match fails. See
[Every scenario, by level](#route-scenarios) for the full map.

The `route` column of `get_row_quality_df(by="check")` shows each check's
route. The `reason` column says why it is not `server_predicate`.

#### The two checks used below

The diagrams below use `price_must_be_positive`, which is a plain filter,
and one extra check, `distinct_bad_prices`, which is not:

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

Both checks should count 3 rows. The routes below show how vowl gets there.

#### `server_predicate` {#route-server-predicate}

**Why it exists:** it is the fastest route. A plain filter already returns
every copy, so the data source only has to count them. Nothing but numbers
is downloaded.

**Used when:** the data source can count, and the query is a plain filter.

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
 ON YOUR MACHINE      bread 2, milk 1   →  C2
```

- The three example checks take this route.
- On a data source that is not one of the 5 tested ones, the numbers are
  exact only when every counted check of the table takes `server_predicate`.

#### `server_lookup` {#route-server-lookup}

**Why it exists:** the query's rows can't be counted as they are, because
`DISTINCT` dropped a copy (or a join added one). Instead of trusting the
query, the data source uses it only to learn _which_ rows failed. Then it
looks those rows up in the table and counts the real copies. Still nothing
but numbers is downloaded.

**Used when:** the data source can count, the query is not a plain filter,
and the data source is one of the 5 tested ones.

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
 ON YOUR MACHINE      bread 2, milk 1   →  C2
```

- Under `"balanced"` and `"fast"`, `distinct_bad_prices` takes this route,
  with the `reason`
  `not certified for pushdown: uses a FROM that is not the table itself`.
- The failed rows must hold the match key, or they can't be looked up.
- A check that returns changed values, such as `price * 1.5` or
  `upper(item)`, can return rows that no table row holds. vowl counts the rows
  that do match and marks the check not exact, with the `reason`
  `some failed rows match no table row`, as `client_lookup` does. This costs one
  more query per table that has `server_lookup` checks. A changed value that
  happens to equal another row's values matches that row. vowl can't detect
  this, so the check stays exact.
- Values that are equal under their column type but print apart, such as
  `-0.0` and `0.0`, NaN and `-NaN`, or the intervals `1 month` and `30 days`,
  are kept apart.

#### `client_lookup` {#route-client-lookup}

**Why it exists:** it is the exact route for checks that are not plain
filters. vowl downloads the table once, then looks each failed row up in it on
your machine. Every table row that holds a failed row's values is counted, so
the copies come from the table, not from the query. It works on any data
source, and for checks that read two data sources.

**Used when:** the query is not a plain filter, under `"accurate"`. Under
`"balanced"`, only where the check would otherwise take `client_returned_rows`.

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
                      bread 2, milk 1   →  C2
```

- Under `"accurate"`, `distinct_bad_prices` takes this route.
- A join that returns a row twice counts it once, because the table holds it
  once.
- A check that returns changed values, such as `price * 1.5` or `upper(item)`,
  can return rows that no table row holds. vowl counts the rows that do match
  and marks the check not exact, with the `reason`
  `some failed rows match no table row`. A changed value that happens to equal
  another row's values matches that row. For example `lower('A')` matches a
  row that holds `a`. vowl can't detect this, so the check stays exact.
- When the table is downloaded, the total in [C3](#c3-count-the-tables-rows)
  is the downloaded table's row count.
- The whole table is held in memory on your machine. At 6 columns this took
  about 3 s and 1 to 1.5 GiB for 1 million rows, and about 13 s and 3.5 GiB for
  5 million rows. Use `row_count_accuracy="balanced"` or `"fast"` on large
  tables.

#### `client_returned_rows` {#route-client-returned-rows}

**Why it exists:** it is the fallback when the data source can't do the
counting. vowl downloads the failed rows the check already returned, and
counts them on your machine. This always works, but it downloads rows, and it
can only count the copies the query returned.

**Used when:** the data source can't count for vowl, or the check reads
tables from two data sources. Also when the query is not a plain filter on a
data source that is not one of the 5 tested ones. Under `"accurate"` and
`"balanced"` these checks take `client_lookup` instead, unless the table can't be
downloaded or the check was cut short by `max_failed_rows`.

```text
 IN THE DATA SOURCE
 ┌──────────────────────────────────────────────────┐
 │ The check already ran and returned its           │
 │ failed rows                                      │
 │      SELECT DISTINCT ...   →   bread, milk       │
 └─────────────────────────┬────────────────────────┘
                           │  the failed rows
                           ▼
 ON YOUR MACHINE
 ┌──────────────────────────────────────────────────┐
 │ Group the failed rows by the match key           │
 │      bread   1 copy     (orders has 2)           │
 │      milk    1 copy                              │
 └─────────────────────────┬────────────────────────┘
                           ▼
                      bread 1, milk 1   →  C2,  exact = False
```

- The count is only right if the check returned every copy. Here it
  returned 1 bread, not 2, so the number is marked not exact.
- `max_failed_rows` can also cut the rows short (see
  [Capping Failed Rows](capping.md)).
- When two checks return the same row, it is counted once, with the larger of
  the two copy counts.

#### Every scenario, by level {#route-scenarios}

| Route                  | Previously called | Where the matching runs                                       |
| ---------------------- | ----------------- | ------------------------------------------------------------- |
| `server_predicate`     | `pushdown`        | The data source counts the rows the filter keeps              |
| `server_lookup`        | `table_match`     | The data source looks the failed rows up in the table         |
| `client_lookup`        | `full_table`      | Your machine looks the failed rows up in the downloaded table |
| `client_returned_rows` | `fetched_rows`    | Your machine counts the rows the check returned, as they are  |

**The normal routes.** Each cell is the route, then whether the number is
exact.

| Check                                      | Data source                 | `"accurate"`                                                                                                                                          | `"balanced"`              | `"fast"`                                                                                      |
| ------------------------------------------ | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------- | --------------------------------------------------------------------------------------------- |
| Plain filter                               | Tested, can count           | `server_predicate`, exact                                                                                                                             | `server_predicate`, exact | `server_predicate`, exact                                                                     |
| Plain filter                               | Not tested, can count       | `server_predicate`, exact only when every counted check of the table takes it. `client_lookup`, exact, when the table is downloaded for another check | Same as `"accurate"`      | `server_predicate`. Exact only when every counted check of the table takes `server_predicate` |
| Not a plain filter (`DISTINCT`, join, ...) | Tested, can count           | `client_lookup`, exact                                                                                                                                | `server_lookup`, exact    | `server_lookup`, exact                                                                        |
| Not a plain filter                         | Not tested, can count       | `client_lookup`, exact                                                                                                                                | `client_lookup`, exact    | `client_returned_rows`, not exact                                                             |
| Plain filter                               | Can't count for vowl        | `client_lookup`, exact                                                                                                                                | `client_lookup`, exact    | `client_returned_rows`, exact, because a plain filter returns every copy                      |
| Not a plain filter, or reads two sources   | Can't count, or two sources | `client_lookup`, exact                                                                                                                                | `client_lookup`, exact    | `client_returned_rows`, not exact                                                             |

So `"fast"` never downloads a table, `"balanced"` downloads it only where
`"fast"` would take `client_returned_rows` for a check that is not a plain
filter, and `"accurate"` downloads it for every check that is not a plain
filter. A table whose checks are all plain filters on a source that can count
is never downloaded.

**When something goes wrong.** These only apply under `"accurate"` and
`"balanced"`, because `"fast"` never downloads the table.

| What happened                                    | A check meant for `client_lookup` that `"fast"` would put on `server_lookup`     | Any other check meant for `client_lookup`                                                                                                                                                        |
| ------------------------------------------------ | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| The table could not be downloaded                | `server_lookup`, exact                                                           | `client_returned_rows`, `reason` `the table could not be exported`, exact only where `"fast"` is                                                                                                 |
| The table's values could not be turned into keys | `server_lookup`, exact                                                           | `client_returned_rows`, `reason` `the failed rows could not be keyed`, exact only where `"fast"` is                                                                                              |
| `max_failed_rows` cut the check's rows short     | `server_lookup`, exact. It counts in the data source, so the cap does not matter | `client_returned_rows`, `reason` `truncated by max_failed_rows`, not exact. If the table was downloaded for other checks, the rows that came back are looked up in it, which gives a lower bound |

The other checks of the table keep their own routes.

**At every level:**

- A check that returns changed values, such as `price * 1.5` or `upper(item)`,
  on `server_lookup` or `client_lookup` is not exact, with the `reason`
  `some failed rows match no table row`. The rows that do match are counted.
- If a counting query fails in the data source, the check is left out of the
  row counts, with the `reason` `dropped at run time (probe failure)`.
- Under the default `row_issue_scope="failed_checks"`, the tolerated rows of
  a check that passed are not fetched or counted at any level.

### C2. List each distinct failed row once

A **row** is one line of your table. A **distinct row** is a set of rows
that hold exactly the same match key values. Rows 2 and 5 are two rows, but
one distinct row, because they are identical:

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

In C2, vowl puts the results of all checks into one list, with each
distinct failed row once. For each one, it keeps:

- **the checks** that caught it,
- **the copies**: how many rows of the table it stands for.

| Check                    | bread        | milk       |
| ------------------------ | ------------ | ---------- |
| `price_must_be_positive` | 2 copies     | 1 copy     |
| `quantity_is_filled`     |              | 1 copy     |
| `order_id_is_unique`     | 2 copies     |            |
| **After C2**             | **2 copies** | **1 copy** |

For the copies, vowl takes the highest number any check found, not the sum.
Two checks found the 2 bread rows. Adding them would give 4, but the table
has 2 (rows 2 and 5).

The list after C2:

| Distinct failed row | Checks that caught it                          | Copies |
| ------------------- | ---------------------------------------------- | ------ |
| bread               | `price_must_be_positive`, `order_id_is_unique` | 2      |
| milk                | `price_must_be_positive`, `quantity_is_filled` | 1      |

The table has 3 failed rows: 2 copies of bread and 1 of milk.

??? note "Where the merge runs"

    - If every check took `server_predicate` or `server_lookup`, the data source merges
      everything and sends back only numbers. Very large contracts are split
      into batches of 250 checks.
    - Otherwise the data source merges its checks, and vowl merges the rest
      on your machine with the same rule.
    - Rarely, two rows that the data source kept apart look equal on your
      machine, such as `1` and `1.0` in SQLite. vowl keeps them apart and
      adds their copies.

### C3. Count the table's rows

The total is the number of rows in the table, after your filter conditions.
It is never capped.

??? note "Where the total comes from"

    vowl uses the first of these it has:

    1. The count from the merge query, if everything ran in one query.
    2. The row count of the downloaded table, if a check took `client_lookup`.
    3. A count taken earlier in the run, if it was not capped.
    4. A new count from the data source.
    5. The capped count from the run. The numbers are then not exact.

    A total of 0 is counted again, because a failed count also returns 0.

### C4. Add up per dimension and per schema

From the list in C2:

- **Failed rows**: the copies of every distinct row caught by a failed check.
- **Tolerated rows**: the copies of every distinct row caught only by checks
  that passed.
- **Passed rows**: the total minus the failed rows.
- **Pass rate**: the passed rows divided by the total.
- **Per dimension**: the same, using only that dimension's checks.

The table has 5 rows.

| Level        | Checks used              | Failed rows | Pass rate |
| ------------ | ------------------------ | ----------- | --------- |
| orders       | all three                | 2, 3, 5 → 3 | 2 / 5     |
| conformity   | `price_must_be_positive` | 2, 3, 5 → 3 | 2 / 5     |
| completeness | `quantity_is_filled`     | 3 → 1       | 4 / 5     |
| uniqueness   | `order_id_is_unique`     | 2, 5 → 2    | 3 / 5     |

A dimension with no counted checks, or an empty table, shows N/A, not 100%.

With `row_issue_scope="all_violations"`, failed rows also include rows caught
only by checks that passed.

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

An empty value means there is no number to give.

### C5. Exact numbers {#exact-numbers}

Every number has an `exact` flag. It is `False` when the number could be off.
The summary then shows **(approx.)** after it, and the DQ metrics set
`vowl.row_quality.exact`. To see which check caused it, look at the `exact`
column of `get_row_quality_df(by="check")`.

A number is not exact when:

- a counted check ended in `ERROR`, so it may hide bad rows,
- a check took `client_returned_rows`, and `max_failed_rows` cut its rows short,
- a check took `client_returned_rows`, and its query is not a plain filter. Under
  `"accurate"` this happens only when the table could not be downloaded or
  keyed,
- a check took `client_lookup` or `server_lookup`, and some of its failed rows
  match no table row,
- the data source could not run a check's query for the count, so vowl left
  the check out,
- the data source is not one vowl has tested (DuckDB, SQLite, Spark and
  PostgreSQL are tested, and Databricks uses the Spark method),
- vowl doesn't know the table's columns, so it can't tell whether two checks
  caught the same row,
- a value could not be compared during the merge,
- the total was capped, or is lower than the rows one check found.

## Annotating your table

Annotating runs on your machine, once per table. It uses the failed rows each
check returned. It does not use the routes. It shares the downloaded table
with counting, so a table is downloaded at most once per run.

### Where each failed check ends up

Each failed check ends up in exactly one place:

| Where                                  | Which checks                                                                            | Example                              |
| -------------------------------------- | --------------------------------------------------------------------------------------- | ------------------------------------ |
| **Annotated on the table**             | Counted checks whose failed rows hold the match key. This is most checks.               | `price_must_be_positive`             |
| **A residue**, in `output["residues"]` | Checks whose failed rows can't be matched to the table, such as rows with fewer columns | A check that returns only town names |
| **The summary only**                   | Checks that return no rows, such as an average, `rowCount`, or a check in `ERROR`       | `average_price_in_range`             |

!!! tip "Read the summary for the final result"

    A check in the last group leaves no mark on the table and has no residue.
    You would miss it if you only looked at the annotated table.

### A1. Download the whole table

vowl downloads the table, with your filter conditions applied. If counting
already downloaded it, vowl reuses that copy.

If it can't, it logs a warning and makes no annotated table. The failed rows
become residues. This happens, for example, when no adapter was given for the
table (the log says "No adapter for schema ..., so its table cannot be
exported."), or the table has a DuckDB `INTERVAL`, `BIT` or `UNION` column (the
log says "Could not export the table of ..."). Checks that would take
`client_lookup` then fall back to their `"fast"` route.

When two checks return different types for the same column, such as `int32`
and `int64`, vowl casts each check's rows to the downloaded table's types
before it merges them.

### A2. Keep the checks whose rows can be annotated

vowl keeps the counted checks whose failed rows hold the match key. Counting
uses the same rule.

### A3. Stop if a check was cut short

If a kept check caught more rows than `max_failed_rows`,
`get_annotated_output()` stops with an error. Otherwise the rows past the cap
would look clean. See [Capping Failed Rows](capping.md).

### A4. List each distinct failed row once

As in [C2](#c2-list-each-distinct-failed-row-once), vowl puts the failed rows
of all kept checks into one list, with each distinct row once. Each keeps the
names of the checks that caught it. Copies don't matter here:

| Distinct failed row | Checks that caught it                          |
| ------------------- | ---------------------------------------------- |
| bread               | `price_must_be_positive`, `order_id_is_unique` |
| milk                | `price_must_be_positive`, `quantity_is_filled` |

### A5. Annotate matching rows

vowl goes through every row of the table. If the row's match key values match
a distinct row in the list, vowl writes that one's check names into the row's
`check_info` column. Otherwise `check_info` is empty, and the row is a **clean row**.

Every copy of a failed row is annotated. The bread row in the list matches both rows
2 and 5, so there are 3 annotated rows, the same as the 3 failed rows from counting:

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

A **residue** holds the failed rows of one check that could not be annotated.
vowl makes one for:

- counted checks left out in A2,
- counted checks of a table with no annotated table (A1),
- other failed checks that returned rows, unless those rows are the good
  rows.

Each residue holds one check, without duplicate rows. Residues of different
checks are never merged.

### What the annotated output holds

```python
output = result.get_annotated_output()
output["annotated"]   # {"<schema>": your table + check_info}
output["residues"]    # {"<schema>::<check_name>": failed rows + check_info + tables_in_query}
```

<!-- prettier-ignore-start -->
<!-- Zensical needs the table indented 4 spaces to stay inside the list item. -->

- **Every schema gets an annotated table**, even when nothing failed. Then
  `check_info` is empty on every row.
- **`check_info` is a JSON list**, with one item per check the row failed.
  Choose what each item holds with `check_info=` (or
  `annotated_check_info` in `ValidationConfig`):

    | `check_info`        | Each item holds                                             |
    | ------------------- | ----------------------------------------------------------- |
    | `"names"` (default) | `check_name`                                                |
    | `"summary"`         | `check_name`, `dimension`, `tags` and `target` (the column) |
    | `"full"`            | The whole check definition, plus `check_name` and `target`  |

- **Each residue holds one check**: its failed rows, a `check_info` column,
  and a `tables_in_query` column naming the tables the check read.
- **Unique-value, primary key and `duplicateValues` checks annotate rows.**
  vowl fetches every row whose value repeats (and, for a primary key, every
  row where it is empty). So the row counts and the annotated rows agree. The
  exception is `duplicateValues` with `unit: percent`, which gives a share,
  not rows.

<!-- prettier-ignore-end -->

## When counting and annotating differ

Usually `failed_rows` equals the number of annotated rows. Here is when it
doesn't:

| Situation                                                                          | Row counts                                                                                                                       | Annotated table                              |
| ---------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| A check caught more rows than `max_failed_rows`                                    | Right, unless the check took `client_returned_rows`                                                                              | `get_annotated_output()` stops with an error |
| The table has a column vowl can't download (`INTERVAL`, `BIT`, `UNION`)            | Right for plain filters. Other checks fall back to their `"fast"` route.                                                         | None. The failed rows become residues.       |
| An untested data source treats different values as equal (`a` and `A`)             | Two rows can count as one, marked not exact. When the table was downloaded, both rows count.                                     | Both rows are annotated                      |
| A query that is not a plain filter took `client_returned_rows`, such as `DISTINCT` | Three copies can count as one. Marked not exact. Only under `"balanced"` or `"fast"`, or when the table could not be downloaded. | All three copies are annotated               |
| The table changes while vowl reads it                                              | Read at one moment                                                                                                               | Read again later                             |

vowl can't detect the last case. It only matters if the table is written to
during the run.

Use the **row counts** for how many rows have issues, and for pass rates.
They are cheap on large tables under `row_count_accuracy="fast"`, or when every
check is a plain filter. Use the **annotated table** or the
**residues** for which rows have issues. On a large table,
`get_output_dfs()` gives each check's failed rows without downloading the
whole table.

## Other ways to see failed rows

| Method                                 | What you get                                                                    |
| -------------------------------------- | ------------------------------------------------------------------------------- |
| `result.show_failed_rows(max_rows=5)`  | Prints a few failed rows for each failed check. `max_rows=-1` prints them all.  |
| `result.get_output_dfs()`              | Each check's failed rows as a separate table, under `"<schema>::<check_name>"`. |
| `result.save(output_mode="annotated")` | Saves the annotated tables, residues and `summary.json` as files.               |

`save()` uses `output_mode="annotated"` by default. The older
`output_mode="failed_rows"` and `get_consolidated_output_dfs()` will be
removed. Use `"annotated"` instead.

## Where to look in the code

| Step                             | Code in `src/vowl/validation/`                                                                                                      |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Pick the counted checks          | `row_quality/selection.py`                                                                                                          |
| Pick the match key               | `_choose_key` in `row_quality/__init__.py`, `row_quality/mergeable.py`                                                              |
| C1                               | `_assign_route` and `_fetch` in `row_quality/__init__.py`, `row_quality/certify.py`                                                 |
| `client_lookup`                  | `_mark_want_full`, `_export_table` and `_run_onto_table` in `row_quality/__init__.py`, `merge_onto_table` in `row_quality/merge.py` |
| C2                               | `row_quality/pushdown.py` (data source), `row_quality/merge.py` (your machine)                                                      |
| C3                               | `_total_rows` in `row_quality/__init__.py`                                                                                          |
| C4, C5                           | `row_quality/rollup.py`                                                                                                             |
| DQ metrics                       | `dq_metrics.py`                                                                                                                     |
| A1 to A6                         | `get_annotated_output` in `result.py`                                                                                               |
| Comparing values on your machine | `row_keys` in `result_row_quality.py`                                                                                               |

The full design, with the reasoning and measurements, is in
`design/row-quality-statistics.md`.
