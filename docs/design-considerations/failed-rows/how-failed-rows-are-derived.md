---
title: How Failed Rows Are Derived
description: >-
  Every SQL check gives vowl two queries. The count query decides pass or
  fail. The failed rows query returns the rows behind that count.
---

# How Failed Rows Are Derived

[Failed Row Results](failed-row-results.md) set out which checks have
failed rows. This page covers the
[query output](failed-row-results.md#failed-row-results): the scalar count
and the failed rows, and where they come from. You write one query
per SQL check, and vowl gets two queries out of it: one that counts the failed
rows and one that returns them.

## Two queries from one

| Query                 | What it returns                      | When it runs                                      |
| --------------------- | ------------------------------------ | ------------------------------------------------- |
| **Count query**       | The number that decides pass or fail | Always                                            |
| **Failed rows query** | The rows behind that number          | Only when the check fails and the rows are needed |

You can write either one, and vowl builds the other:

=== "You write `COUNT(*)`"

    ```sql
    -- Your query (count query): decides pass or fail
    SELECT COUNT(*) FROM orders WHERE price <= 0

    -- vowl builds the failed rows query by replacing COUNT(*) with *
    SELECT * FROM orders WHERE price <= 0
    ```

=== "You write `SELECT *`"

    ```sql
    -- Your query (failed rows query): lists the rows
    SELECT * FROM orders WHERE price <= 0

    -- vowl builds the count query by wrapping it in COUNT(*)
    SELECT COUNT(*) FROM (SELECT * FROM orders WHERE price <= 0)
    ```

So a check written as `COUNT(*)` with `mustBe: 0` still gives you the rows
behind the count. This works for a check that joins two tables too, such as
"every payroll row has a matching employee":

```sql
SELECT COUNT(*)
FROM demo_employee_payroll payroll
LEFT JOIN demo_employee_list ref
  ON payroll.employee_id = ref.employee_id
WHERE ref.employee_id IS NULL
```

The count query tells you that _some_ payroll rows have no matching employee.
The failed rows query tells you _which_ ones. The columns those rows hold
depend on how the query is written, and decide whether vowl can attribute
them to the source table. See
[Annotating the failed rows of a cross-table check](../cross-table/how-it-works.md#annotating-the-failed-rows-of-a-cross-table-check).

!!! info "How vowl builds the other query"

    vowl does not search and replace the text `COUNT(*)`. It reads your query
    with [sqlglot](https://github.com/tobymao/sqlglot), a Python library that
    parses and translates SQL, and swaps the selected columns in the parsed
    query. Subqueries, `WITH` clauses and syntax specific to one database come
    through unchanged.

## When each query runs

The count query always runs, because vowl needs it to pass or fail the check.
A check that passes costs one query.

The failed rows query runs when a check fails and something needs its rows:

- The [row counts](how-rows-are-counted.md), such as **Passed Rows**
  in the summary and `get_row_quality_df()`. Where it can, vowl runs the
  failed rows queries inside the data source as part of one counting query,
  so only numbers come back. With
  [`disable_table_attributed_counts`](../../run-settings.md#disable_table_attributed_counts)
  set, the row counts use the count query only.
- The [annotated output](annotating-the-source-table.md), from
  `get_annotated_output()` or `save()`.
- `show_failed_rows()` and `get_output_dfs()`.

!!! info "Query performance"

    The wrapping vowl adds, a subquery or `SELECT *` in place of `COUNT(*)`,
    does not slow the query down. Databases flatten these standard shapes
    when they plan the query. A `LEFT JOIN ... WHERE ref.key IS NULL`, which
    finds rows with no match in the other table, usually runs as one pass
    over the join key. By default the failed rows query returns every failed
    row. On a very large table with many failed rows, see
    [Capping Failed Rows](capping-failed-rows.md).
