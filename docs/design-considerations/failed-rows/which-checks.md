---
title: Which Checks Contribute Failed Rows
description: >-
  Which checks add their failed rows to the row counts and the annotated
  output, and how row_issue_scope treats checks that pass with some failed
  rows.
---

# Which Checks Contribute Failed Rows

A check's status (`PASSED`, `FAILED` or `ERROR`) and whether it adds failed
rows are two separate things. A check can fail and add no rows, such as
`average_price_in_range` in the [example](queries.md#the-example-used-in-this-section).
A check can also pass and still catch rows. This page explains both.

vowl makes this decision once per check. The row counts and the annotated
output both use it, so they always work from the same checks.

## Counted checks

A check whose failed rows count toward the row counts is a **counted check**.
A check is counted when it meets all four conditions:

1. [It points at rows](#condition-1).
2. [It sets an upper limit on the rows it catches](#condition-2).
3. [It ran](#condition-3).
4. [Its failed rows can be found in the table](#condition-4).

A check that is not counted still passes or fails as usual. It adds no rows to
the row counts and annotates no rows of the table.
`get_row_quality_df(by="check")` shows, for each check, whether it was counted
and the `reason` when it was not. Each condition below gives the `reason` shown when a check does not meet it.

### Condition 1: It points at rows {#condition-1}

The check must catch individual rows, such as "no price is zero or less". A
check that works out one number for the whole table, such as an average, a sum
or a `rowCount`, has no rows to count.

In the [example](queries.md#the-example-used-in-this-section),
`average_price_in_range` fails, but it is not counted, because it returns one
average and not rows.

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

An upper limit above 0 allows some bad rows. So a counted check can pass and
still catch rows. For example, "fewer than 100 orders have no quantity"
(`mustBeLessThan: 100`) passes with 50 such orders. Those 50 rows are
**tolerated rows**. Whether they count as failed rows depends on what you
report, so you choose it with `row_issue_scope`.

`row_issue_scope` is set for one run. You pass it in a `ValidationConfig` to
`validate_data`, and it applies to every check in that run. It is not set in
the contract, and it does not change any later run:

```python
from vowl import ValidationConfig, validate_data

config = ValidationConfig(row_issue_scope="all_violations")
result = validate_data("orders.yaml", df=df, config=config)

result.get_row_quality_df().to_pandas()  # failed_rows now includes the 50 tolerated rows
```

A run without `config=` uses the default, `"failed_checks"`.

| `row_issue_scope`           | Which rows count as failed rows                                                                      |
| --------------------------- | ---------------------------------------------------------------------------------------------------- |
| `"failed_checks"` (default) | Only rows caught by checks that failed. Tolerated rows are reported separately, as `tolerated_rows`. |
| `"all_violations"`          | Every row that any counted check caught, whether the check passed or failed.                         |

The setting applies to the annotated output too. Under `"all_violations"`,
tolerated rows are annotated, and their entry in `check_info` carries
`"tolerated": true`.

`tolerated_rows` and `failed_rows` come from the same pass over the data, so
you can see both without running the checks again. One exception: under the
default setting, vowl does not download the failed rows of a passed check only
to count its tolerated rows, because they are not needed for `failed_rows` and
can be large. When a passed check's rows would have to be downloaded,
`tolerated_rows` is left empty.

### Condition 3: It ran {#condition-3}

A check that ended in `ERROR` has no failed rows, for example because of a
typo in its SQL. If it would otherwise have been counted, the row counts of
its table and dimension are [not exact](levels.md#exact-numbers), because it
could be hiding bad rows.

`reason`: `check ended in ERROR`

### Condition 4: Its failed rows can be found in the table {#condition-4}

To count a row once when several checks catch it, vowl must recognise the
same row across checks. It does this with the
[match key](levels.md#the-match-key): every column of the table, or its
primary key. A check whose failed rows don't hold the match key columns is not
counted. For example, a check that returns only the names of towns with an
unusual price can't be matched to rows of the table. Its failed rows are kept
as a residue instead.

vowl checks this condition later than the other three, when it picks the match
key. The `reason` values are listed in [The match key](levels.md#the-match-key).
