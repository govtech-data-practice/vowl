---
title: Design Considerations
---

# Design Considerations

This page explains design decisions behind vowl's internals: how checks are
executed, how queries are derived, and how results flow into output.

## The two-query model

Every SQL check derives **two** queries from the single `query:` you write:

| Query                 | Purpose                                    | When it runs                                      |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| **Scalar query**      | Produces the number that decides pass/fail | Always                                            |
| **Failed-rows query** | Returns the actual offending rows          | Only on failure, and only when rows are requested |

You only write one of them. vowl derives the other automatically:

=== "You write `COUNT(*)`"

    ```sql
    -- Your query (scalar): decides pass/fail
    SELECT COUNT(*) FROM orders WHERE total < 0

    -- vowl derives the failed-rows query by rewriting COUNT(*) to *
    SELECT * FROM orders WHERE total < 0
    ```

=== "You write `SELECT *`"

    ```sql
    -- Your query (failed rows): lists the offending rows
    SELECT * FROM orders WHERE total < 0

    -- vowl derives the scalar query by wrapping in COUNT(*)
    SELECT COUNT(*) FROM (SELECT * FROM orders WHERE total < 0)
    ```

This means a check like `COUNT(*) mustBe 0` is not limited to a single number.
vowl can always recover the individual rows behind that number by running the
derived failed-rows query. Consider a referential-integrity check:

```sql
SELECT COUNT(*)
FROM demo_employee_payroll payroll
LEFT JOIN demo_employee_list ref
  ON payroll.employee_id = ref.employee_id
WHERE ref.employee_id IS NULL
```

The scalar query tells you _some_ payroll rows have no matching master record.
The derived failed-rows query (`COUNT(*)` rewritten to `SELECT *`) tells you
exactly _which_ ones. The join is how you express the condition; it does not
prevent vowl from recovering individual offending rows.

This is what makes it possible to map a cross-table failure back onto a single
table's annotated output (next section).

!!! info "How the rewrite works: sqlglot, not string manipulation"
vowl does not regex or string-replace `COUNT(*)` with `SELECT *`. It parses
your query into an AST using [sqlglot](https://github.com/tobymao/sqlglot),
a SQL transpiler, and performs the substitution structurally. This means it
correctly handles nested subqueries, CTEs, and dialect-specific syntax
without mangling your SQL.

!!! note "The failed-rows query is lazy"
The scalar query always runs because vowl needs it to decide the verdict.
The failed-rows query only runs when a check **fails** and something
requests the rows (e.g. `get_annotated_output()`, `show_failed_rows()`, or
`output_mode="failed_rows"`). Passing checks cost a single query.

!!! info "A note on query performance"
The syntactic complexity vowl adds (wrapping queries in subqueries,
rewriting between `COUNT(*)` and `SELECT *`) does not degrade execution
plans. Query engines flatten these standard shapes during planning. The
`LEFT JOIN ... WHERE ref.key IS NULL` anti-join pattern is typically
executed as a hash anti-join over the join key, not a row-by-row
comparison. Additionally, the failed-rows query only runs on failure and
is capped at `max_failed_rows`, so its cost is bounded regardless of table
size.

## Making a cross-table check annotate onto a table

For background on annotated output and residues, see
[Annotated Output: Not All Checks Can Be Merged](known-issues.md#annotated-output-not-all-checks-can-be-merged).

By default a cross-table check becomes a **residue**: its failed rows carry
columns from both tables, so they don't match any single schema (see
[condition 3](known-issues.md#annotated-output-not-all-checks-can-be-merged)).
The residue still contains the offending rows and the `check_name`, just as a
separate entry rather than a column on the annotated table.

If you want those failures to appear in a table's `check_info` column, shape
the failed-rows query so it returns **exactly the columns of the table you want
to annotate**. Wrap the join in a subquery that selects only the anchor table's
columns:

```yaml
- name: employee_id_exists_in_master_list
  description: >-
    Referential integrity check: ensures every employee ID in the payroll
    table exists in the reference employee list.
  type: sql
  dimension: consistency
  query: >-
    SELECT COUNT(*)
    FROM (
      SELECT payroll.*
      FROM demo_employee_payroll payroll
      LEFT JOIN demo_employee_list ref
        ON payroll.employee_id = ref.employee_id
      WHERE ref.employee_id IS NULL
    ) AS orphaned_payroll
  mustBe: 0
  tags:
    - Cross-Table Validation
```

When vowl rewrites the outer `COUNT(*)` into `SELECT *`, it only swaps the
**outer** select list. The subquery is left untouched:

```sql
SELECT * FROM (
  SELECT payroll.*
  FROM demo_employee_payroll payroll
  LEFT JOIN demo_employee_list ref
    ON payroll.employee_id = ref.employee_id
  WHERE ref.employee_id IS NULL
) AS orphaned_payroll
```

The inner `SELECT payroll.*` decides the columns, so the failed rows come back
with **only the payroll columns** using their bare names (`employee_id`, `name`,
`salary`, ...). `payroll.*` qualifies _which_ table to expand; it does not
prefix the column names. That column set matches `demo_employee_payroll`, so the
rows map straight back onto its annotated table.

| Query shape                                           | Failed-rows columns                     | Annotates onto payroll?      |
| ----------------------------------------------------- | --------------------------------------- | ---------------------------- |
| Bare `SELECT COUNT(*) FROM payroll LEFT JOIN ref ...` | Both tables' columns (`ref.*` all NULL) | No, column set doesn't match |
| Subquery with `SELECT payroll.*` (above)              | Payroll columns only                    | Yes                          |

!!! warning "Mergeability is decided by column structure, not intent"
vowl decides mergeability by **column structure**. Any failed-rows result
whose columns match the anchor table will be merged onto it. It is on you
to ensure the subquery projects the right columns (`payroll.*`, not a
partial column list, and not both tables' columns). Get this wrong and the
check either won't merge (column mismatch = residue) or could merge rows
you didn't intend. When in doubt, run `get_annotated_output()` and inspect
both `annotated` and `residues`.

!!! note "`NOT EXISTS` is an equivalent, often cleaner form"
The same result with no subquery wrapper. The reference table lives only
inside the `WHERE NOT EXISTS (...)`, so `SELECT *` naturally resolves to
payroll columns:

    ```sql
    SELECT COUNT(*)
    FROM demo_employee_payroll payroll
    WHERE NOT EXISTS (
      SELECT 1 FROM demo_employee_list ref
      WHERE ref.employee_id = payroll.employee_id
    )
    ```
