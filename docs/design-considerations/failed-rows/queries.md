---
title: The Count Query and the Failed Rows Query
description: >-
  Every SQL check gives vowl two queries. The count query decides pass or
  fail. The failed rows query returns the rows behind that count.
---

# The Count Query and the Failed Rows Query

vowl runs each check as a query that counts the rows breaking it, and passes
or fails the check on that count. It can then fetch those rows, count them per
table and annotate your table with them. The pages in this section explain each of
these steps.

## The example used in this section

The pages in this section use one `orders` table and four checks:

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

The words used here, such as **failed rows** and **row counts**, are defined
in [Glossary](../../glossary.md).

## Two queries from one

You write one query per SQL check. vowl builds the second one from it:

| Query                 | What it returns                      | When it runs                                      |
| --------------------- | ------------------------------------ | ------------------------------------------------- |
| **Count query**       | The number that decides pass or fail | Always                                            |
| **Failed rows query** | The rows behind that number          | Only when the check fails and the rows are needed |

You can write either one:

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
behind the count. This holds for a check that joins two tables too, such as
"every payroll row has a matching employee":

```sql
SELECT COUNT(*)
FROM demo_employee_payroll payroll
LEFT JOIN demo_employee_list ref
  ON payroll.employee_id = ref.employee_id
WHERE ref.employee_id IS NULL
```

The count query tells you that _some_ payroll rows have no matching employee.
The failed rows query tells you _which_ ones. Which columns those rows hold
depends on how the query is written, and decides whether vowl can annotate your
table with them. See
[Annotating the failed rows of a cross-table check](../cross-table/how-it-works.md#annotating-the-failed-rows-of-a-cross-table-check).

!!! info "How vowl builds the second query"

    vowl does not search and replace the text `COUNT(*)`. It reads your query
    with [sqlglot](https://github.com/tobymao/sqlglot), a Python library that
    parses and translates SQL, and swaps the selected columns in the parsed
    query. Subqueries, `WITH` clauses and syntax specific to one database come
    through unchanged.

## When each query runs

The count query always runs, because vowl needs it to pass or fail the check.
A check that passes costs one query.

The failed rows query runs when a check fails and something needs its rows:

- the [row counts](levels.md#counting-the-row-counts), such as **Passed Rows**
  in the summary and `get_row_quality_df()`. Where it can, vowl runs the
  failed rows queries inside the data source as part of one counting query,
  so only the counts come back.
- the [annotated output](levels.md#annotating-your-table), from
  `get_annotated_output()` or `save(output_mode="annotated")`.
- `show_failed_rows()` and `get_output_dfs()`.

!!! info "Query performance"

    The extra wrapping vowl adds, a subquery or swapping `COUNT(*)` for
    `SELECT *`, does not slow the query down. Databases flatten these standard
    shapes when they plan the query. A `LEFT JOIN ... WHERE ref.key IS NULL`,
    which finds rows with no match in the other table, usually runs as one
    pass over the join key. By default the failed rows query returns every
    failed row. On a very large table with many failed rows, see
    [Capping Failed Rows](capping.md).
