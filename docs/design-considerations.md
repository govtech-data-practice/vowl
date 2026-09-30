---
title: Design Decisions
---

# Design Decisions

This page explains design decisions behind vowl's internals: how checks are
executed, how queries are derived, and how results flow into output.

## The two-query model

Every SQL check derives **two** queries from the single `query:` you write:

| Query                 | Purpose                                    | When it runs                                      |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| **Count query**       | Produces the number that decides pass/fail | Always                                            |
| **Failed-rows query** | Returns the actual offending rows          | Only on failure, and only when rows are requested |

You only write one of them. vowl derives the other automatically:

=== "You write `COUNT(*)`"

    ```sql
    -- Your query (count): decides pass/fail
    SELECT COUNT(*) FROM orders WHERE total < 0

    -- vowl derives the failed-rows query by rewriting COUNT(*) to *
    SELECT * FROM orders WHERE total < 0
    ```

=== "You write `SELECT *`"

    ```sql
    -- Your query (failed rows): lists the offending rows
    SELECT * FROM orders WHERE total < 0

    -- vowl derives the count query by wrapping in COUNT(*)
    SELECT COUNT(*) FROM (SELECT * FROM orders WHERE total < 0)
    ```

This means a check like `COUNT(*) mustBe 0` is not limited to a single number.
vowl can always recover the individual rows behind that number by running the
derived failed-rows query. Consider a check that every payroll row has a
matching employee:

```sql
SELECT COUNT(*)
FROM demo_employee_payroll payroll
LEFT JOIN demo_employee_list ref
  ON payroll.employee_id = ref.employee_id
WHERE ref.employee_id IS NULL
```

The count query tells you _some_ payroll rows have no matching master record.
The derived failed-rows query (`COUNT(*)` rewritten to `SELECT *`) tells you
exactly _which_ ones. The join is how you express the condition. It does not
stop vowl from recovering individual offending rows.

This is what lets vowl mark a cross-table check's failed rows on one table
(next section).

!!! info "How the rewrite works"
    vowl does not search and replace the text `COUNT(*)`. It parses your query
    with [sqlglot](https://github.com/tobymao/sqlglot), a Python library that
    reads and translates SQL, and swaps the select list in the parsed query.
    So nested subqueries, `WITH` clauses and dialect-specific syntax come
    through intact.

!!! note "The failed-rows query runs only when needed"
    The count query always runs, because vowl needs it to pass or fail the
    check. The failed-rows query runs only when a check **fails** and something
    asks for the rows, such as `get_annotated_output()`, `show_failed_rows()`
    or `save()`. A passing check costs one query.

    The row counts (`print_summary()`, `get_row_quality_df()`) also use the
    failed-rows queries. Where they can, they run them inside the data source
    as part of one counting query, so no rows are sent back. See
    [Handling of Failed Rows](failed-rows.md#how-vowl-counts-failed-rows).

!!! info "A note on query performance"
    The extra wrapping vowl adds (subqueries, and swapping `COUNT(*)` for
    `SELECT *`) does not slow the query down. Databases flatten these standard
    shapes when they plan the query. A `LEFT JOIN ... WHERE ref.key IS NULL`,
    which finds rows with no match in the other table, usually runs as one
    pass over the join key, not a row-by-row comparison. The failed-rows query
    returns every failed row by default. On a very large table with many
    failures, set `max_failed_rows` to cap how many it fetches (see
    [Capping failed rows](failed-rows.md#capping-failed-rows)).

## Making a cross-table check mark rows

By default a cross-table check's failed rows have columns from both tables, so
they match neither table and become a residue. To have them marked on one
table, wrap the join in a subquery that selects only that table's columns
(`SELECT payroll.*`). The rewrite only swaps the outer select list, so the
inner `SELECT payroll.*` decides the columns of the failed rows. `payroll.*`
picks which table's columns to return. It does not add `payroll.` to their
names, so they match the table.
[Writing a cross-table check that marks rows](failed-rows.md#writing-a-cross-table-check-that-marks-rows)
has the full example.

| Query shape                                           | Failed-rows columns                     | Marked on payroll?           |
| ----------------------------------------------------- | --------------------------------------- | ---------------------------- |
| Bare `SELECT COUNT(*) FROM payroll LEFT JOIN ref ...` | Both tables' columns (`ref.*` all NULL) | No, the columns don't match |
| Subquery with `SELECT payroll.*`                      | Payroll columns only                    | Yes                          |
| `WHERE NOT EXISTS (SELECT 1 FROM ref ...)`            | Payroll columns only                    | Yes                          |

`NOT EXISTS` needs no wrapper, because the reference table appears only inside
the `WHERE`:

```sql
SELECT COUNT(*)
FROM demo_employee_payroll payroll
WHERE NOT EXISTS (
  SELECT 1 FROM demo_employee_list ref
  WHERE ref.employee_id = payroll.employee_id
)
```

## Reference resolution for relationships

`relationships` targets are found the way JSON Schema and OpenAPI find a
`$ref`, following the standard rules for relative URLs
([RFC 3986 §5](https://www.rfc-editor.org/rfc/rfc3986#section-5)):

- **Shorthand** `object.property` resolves by schema/property `name`.
- **Fully-qualified** `/schema/<id>/properties/<id>` matches by `id` and returns
  the target's `name`.
- **External** `file.yaml#/schema/<id>/properties/<id>` loads `file.yaml`
  **relative to the referencing contract's own retrieval location** (its
  `origin`), then finds the part after `#` within it. A contract built from
  in-memory data has no location, so vowl does not guess where a relative
  external reference points. External files are fetched the same way as
  contracts, so the same
  [public address rule](loading-contracts.md#loading-contracts-from-git-github-or-gitlab)
  applies.

Shorthand and fully-qualified references that point at the same property produce
the same SQL. The notation only changes how vowl finds the property.
