---
title: Failed Row Results
description: >-
  What a check reports about the rows it caught, the example used in this
  section, and which checks contribute to the row counts and the annotated
  output.
---

# Failed Row Results

A check tells you whether your data passed. Often you also want to know which
rows broke it, and how many rows of the table are affected overall. This
section explains how vowl gets from one check to those answers:

1. **This page.** What a check reports, how vowl turns it into row counts,
   and which checks count.
2. [How Failed Rows Are Derived](how-failed-rows-are-derived.md). The query
   output: the count query and the failed rows query.
3. [How Rows Are Counted](how-rows-are-counted.md). Attributed
   rows, and how vowl adds them up per dimension, table and run.
4. [Counting Mechanisms](counting-mechanisms.md). The routes vowl uses to
   attribute rows, and what each one costs and gets right.
5. [Annotating the Source Table](annotating-the-source-table.md). How vowl
   writes the attributed rows into your table.
6. [Capping Failed Rows](capping-failed-rows.md). What changes when you limit
   how many failed rows vowl downloads.

Words such as **failed rows** and **row counts** are defined in the
[Glossary](../../glossary.md).

## Failed row results

The **failed row results** are everything vowl reports about the rows a check
caught. They come in two groups:

| Group               | Result              | What it is                                                                       |
| ------------------- | ------------------- | -------------------------------------------------------------------------------- |
| **Query output**    | **Scalar count**    | The number the count query returns. It decides pass or fail.                     |
| **Query output**    | **Failed rows**     | The rows the failed rows query returns, such as orders with a price of 0 or less |
| **Attributed rows** | **Attributed rows** | The rows of the source table that the failed rows stand for, every copy counted  |

The query output is what the check's own queries return. vowl works out the
attributed rows from the failed rows. See
[Counts are exact only when attributed to the source table](how-rows-are-counted.md#counts-are-exact-only-when-attributed-to-the-source-table).

For most checks all three give the same number. They differ when a check
changes the rows it returns, for example with `DISTINCT`. See
[Checks can change the rows they return](how-rows-are-counted.md#checks-can-change-the-rows-they-return).

Each output of vowl uses one group:

| Output                                                                                                        | Uses            |
| ------------------------------------------------------------------------------------------------------------- | --------------- |
| Pass or fail, `actual`, `failed_rows_count`, `scalar_count`, check-level DQ metrics                           | Scalar count    |
| `show_failed_rows()`, `get_output_dfs()`, residues                                                            | Failed rows     |
| `attributed_rows`, the row counts, **Passed Rows**, dimension, schema and run DQ metrics, the annotated table | Attributed rows |

`scalar_count`, `attributed_rows` and `attributed` are columns of
`get_row_quality_df(by="check")`. Per schema and per dimension, the
`failed_rows` column holds the rows of the table that failed, counted from
the attributed rows.

Not every failed check has failed rows. Aggregation checks, such as "the
average price is between 1 and 5", return a number and no rows.

Not every passed check has zero failed rows. A check with a failure threshold
above 0, such as "fewer than 100 orders have no quantity", can pass and still
catch some rows. These are [tolerated rows](#tolerated-rows).

So a check's status (`PASSED`, `FAILED` or `ERROR`) doesn't settle whether its
rows count. The [Counted checks](#counted-checks) section shows the rules.

## From query output to row counts

vowl gets from a check to the row counts in four steps:

1. **Query output.** The check returns its scalar count and its failed rows,
   as they are.
2. **Attribute.** For a [counted check](#counted-checks), vowl finds the rows
   of the table that the failed rows stand for. These are its **attributed
   rows**.
3. **Not attributed.** When vowl can't attribute a counted check, the check
   is **not attributed**. Its failed rows become a
   [residue](annotating-the-source-table.md#residues) when they can never be
   attributed.
4. **Row counts.** vowl adds up the attributed rows per dimension, schema and
   run, counting each row once.

```text
 query output ──▶ attribute ──┬──▶ attributed rows ──▶ row counts
                              │
                              └──▶ not attributed  ──▶ left out, flagged
                                     (never attributable: residues)
```

There are two kinds of not attributed:

| Kind                        | `reason`                                                                                                                                                                                                                                          | Row counts                                                                  | Annotated output                                                                             |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **Never attributable**      | `failed rows do not have the table's columns or primary key`, `primary key has duplicate values`, `primary key uniqueness could not be checked`                                                                                                   | Left out. See [The match key](how-rows-are-counted.md#the-match-key)        | The failed rows become a residue                                                             |
| **Not attributed this run** | `truncated by max_failed_rows`, `the table could not be exported`, `the failed rows could not be fetched`, `the failed rows could not be keyed`, `the counting query failed in the data source`, `tolerated rows not fetched under failed_checks` | Left out. See [When something goes wrong](counting-mechanisms.md#fallbacks) | Annotated as usual. When the table could not be downloaded, the failed rows become a residue |

A check that is not attributed has an empty `route`, `attributed` set to
`False` and no `attributed_rows`. It adds nothing to the row counts. When its
scalar count says it may have caught rows, the numbers of its dimension, its
schema and the run are marked not [exact](counting-mechanisms.md#exact-numbers).
A dimension or schema whose counted checks are all not attributed shows N/A.

Each grain of the [DQ metrics](../../dq-metrics/understanding-metrics.md) uses
one number:

| Grain                     | Uses                                                              |
| ------------------------- | ----------------------------------------------------------------- |
| Check                     | The scalar count, for every counted check                         |
| Dimension, schema and run | Attributed rows only. Checks that are not attributed are left out |

The number of counted checks left out is `checks_not_attributed`. It is a
column of `get_row_quality_df()` per schema and per dimension, and the
`vowl.row_quality.checks_not_attributed` attribute of the dimension, schema
and run DQ metrics and of the OTEL root span. The summary shows it after the numbers, for example
`Passed Rows: 4 / 5 (80.0%) (approx., 2 checks not attributed)`.

With [`disable_table_attributed_counts`](../../run-settings.md#disable_table_attributed_counts)
set, vowl attributes no rows. Every counted check is then not attributed, so
`checks_not_attributed` equals `checks_counted`, and the row counts add up the
scalar counts instead. See [`server_scalar`](counting-mechanisms.md#route-server-scalar).

## The example used in this section {#the-example-used-in-this-section}

Every page uses one `orders` table and four checks:

| order_id | item  | price | quantity |
| -------- | ----- | ----- | -------- |
| 1        | apple | 1.50  | 3        |
| 2        | bread | -2.00 | 1        |
| 3        | milk  | 0.00  |          |
| 4        | eggs  | 3.20  | 2        |
| 2        | bread | -2.00 | 1        |

The last row is an exact copy of the second.

| Check                    | Dimension    | Rule                                 | Rows it catches              | Status |
| ------------------------ | ------------ | ------------------------------------ | ---------------------------- | ------ |
| `price_must_be_positive` | conformity   | no price is zero or less             | bread, milk, bread (3 rows)  | FAILED |
| `quantity_is_filled`     | completeness | every order has a quantity           | milk (1 row)                 | FAILED |
| `order_id_is_unique`     | uniqueness   | no `order_id` appears twice          | bread, bread (2 rows)        | FAILED |
| `average_price_in_range` | conformity   | the average price is between 1 and 5 | none, it returns one average | FAILED |

## Counted checks

A check whose rows count toward the row counts is a **counted check**. vowl
decides this once per check, and the row counts and the annotated output both
use that decision, so they always work from the same checks. A check is
counted when it meets the first three conditions:

1. [It points at rows](#condition-1).
2. [It sets an upper limit on the rows it catches](#condition-2).
3. [It ran](#condition-3).

A counted check is then attributed when it also meets the fourth:

4. [Its failed rows can be attributed to the source table](#condition-4).

A check that is not counted still passes or fails as usual. It just adds no
rows to the row counts and annotates no rows of the table.
`get_row_quality_df(by="check")` has one row per check. Its `counted` and
`attributed` columns show whether the check was counted and attributed, and
its `reason` column says why not. Each condition below gives its `reason`. A
check gets only one: a check that ended in `ERROR` always gets the reason of
condition 3, and any other check gets the reason of the first condition it
fails.

### Condition 1: It points at rows {#condition-1}

The check must catch individual rows, such as "no price is zero or less".
vowl tests this by asking whether the number the check returns is a count of
rows. For a SQL check, that means the query holds one `COUNT`, such as
`COUNT(*)`, `COUNT(quantity)` or `COUNT(DISTINCT item)`, or lists rows with no
aggregate at all.

A `COUNT(DISTINCT item)` check counts values, not rows, but its failed rows
are the rows that hold those values. Say a check allows fewer than 2 distinct
items with a price of zero or less:

```sql
SELECT COUNT(DISTINCT item) FROM orders WHERE price <= 0
```

It counts 2 values, bread and milk, so it fails. Its failed rows are the 3
rows that hold them, both bread rows and the milk row. Like `COUNT(quantity)`,
it skips empty values, so a row with no `item` is not one of its failed rows.
`COUNT(DISTINCT (item, price))` and `COUNT(DISTINCT ROW(item, price))` count
combined values in a way that differs between databases, so they are not
counted.

A check that works out one number for the whole table has no rows to count.
That is why `average_price_in_range` fails but is not counted. These checks
are not counted:

| Check                                       | Example                                                 |
| ------------------------------------------- | ------------------------------------------------------- |
| An aggregate other than `COUNT`             | `SELECT AVG(price) FROM orders`, or `SUM`, `MIN`, `MAX` |
| More than one aggregate                     | `SELECT COUNT(*), MAX(price) FROM orders`               |
| A distinct count of combined values         | `SELECT COUNT(DISTINCT (item, price)) FROM orders`      |
| A query vowl can't parse, or with no `FROM` | `SELECT 1`                                              |
| A `unit` other than `rows`                  | `unit: percent`                                         |
| A check type with no SQL behind it          | Custom and unsupported check types                      |

`reason`: `table-level or not a row filter`

### Condition 2: It sets an upper limit on the rows it catches {#condition-2}

A check counts the rows it catches and compares that number with a limit. The
caught rows are bad rows only when the limit is an upper limit, meaning "no
more than this many". vowl reads this from the check's operator:

| Operator                                                                                            | Counted                                                           |
| --------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `mustBe: 0`                                                                                         | Yes. No caught row is allowed.                                    |
| `mustBeLessThan`, `mustBeLessOrEqualTo`, `mustBeBetween: [0, n]`                                    | Yes. Some caught rows are allowed.                                |
| `mustBeGreaterThan`, `mustBeGreaterOrEqualTo`                                                       | No. The check catches good rows, such as "at least 100 are paid". |
| `mustBe: n` with n above 0, `mustNotBe`, `mustBeBetween: [a, b]` with a above 0, `mustNotBeBetween` | No. When the total is off, no single row is at fault.             |

`reason`: `operator does not identify bad rows`

#### Tolerated rows

An upper limit above 0 allows some bad rows, so a counted check can pass and
still catch rows. For example, "fewer than 100 orders have no quantity"
(`mustBeLessThan: 100`) passes with 50 such orders. Those 50 rows are
**tolerated rows**. Whether they count as rows that failed depends on what you want
to report, so you choose with
[`row_issue_scope`](../../run-settings.md#row_issue_scope):

| `row_issue_scope`           | Which rows count as rows that failed                                                                 |
| --------------------------- | ---------------------------------------------------------------------------------------------------- |
| `"failed_checks"` (default) | Only rows caught by checks that failed. Tolerated rows are reported separately, as `tolerated_rows`. |
| `"all_violations"`          | Every row that any counted check caught, whether the check passed or failed.                         |

You set it for one run, in a `ValidationConfig`. It applies to every check in
that run, and it is not part of the contract:

```python
from vowl import ValidationConfig, validate_data

config = ValidationConfig(row_issue_scope="all_violations")
result = validate_data("orders.yaml", df=df, config=config)

result.get_row_quality_df().to_pandas()  # failed_rows now includes the 50 tolerated rows
```

The setting applies to the annotated output too. Under `"all_violations"`,
tolerated rows are annotated, and their `check_info` item carries
`"tolerated": true`.

`tolerated_rows` and `failed_rows` come from the same pass over the data, so
you see both without running the checks again. Under the default setting,
vowl does not download a passed check's rows just to count its tolerated
rows, because they can be large. Where that download would be needed, the
check is not attributed, and `tolerated_rows` is left empty (see
[When something goes wrong](counting-mechanisms.md#fallbacks)).

### Condition 3: It ran {#condition-3}

A check that ended in `ERROR`, for example because of a typo in its SQL, has
no query output. If it would otherwise have been counted, the row counts of its
table and dimension are marked [not exact](counting-mechanisms.md#exact-numbers),
because it could be hiding bad rows.

`reason`: `check ended in ERROR`

### Condition 4: Its failed rows can be attributed to the source table {#condition-4}

To count a row once when several checks catch it, vowl must tell which row of
the source table each failed row stands for. This is called **attributing**
the failed row to the source table. vowl compares the
[match key](how-rows-are-counted.md#the-match-key): every column of the
table, or its primary key. A check whose failed rows don't hold the match key
columns is never attributable. It stays counted, but it is not attributed, so
it adds nothing to the row counts and they are marked not exact. For example,
a check that returns only the names of towns with an unusual price can't be
attributed to rows of the table. Its failed rows are kept as a
[residue](annotating-the-source-table.md#residues) instead.

vowl checks this condition later than the other three, once it has picked the
match key. The `reason` values are listed in
[The match key](how-rows-are-counted.md#the-match-key).
