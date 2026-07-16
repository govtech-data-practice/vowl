---
title: Design Considerations
---

# Design Considerations

This page explains how vowl actually runs your checks under the hood: how a
single check becomes two SQL queries, why the pass/fail verdict is table-level
while the failing rows are still recoverable, and how that shapes what ends up
in annotated output. Understanding this makes it clear *why* some checks
annotate onto a table and others become residues — and how you can influence
that with the way you write a query.

For the annotated-output / residues split itself, see
[Annotated Output: Not All Checks Can Be Merged](known-issues.md#annotated-output-not-all-checks-can-be-merged).

## How a check runs: two queries, not one

Every SQL check derives **two** queries from the single `query:` you write:

- a **scalar query** — produces the one number that decides pass/fail
- a **failed-rows query** — pulls back the actual rows that failed

You only write one of them. vowl derives the other automatically, in whichever
direction is needed:

=== "You write `COUNT(*)`"

    ```sql
    -- Your query (scalar): decides pass/fail
    SELECT COUNT(*) FROM orders WHERE total < 0

    -- vowl rewrites COUNT(*) -> * for the failed-rows query
    SELECT * FROM orders WHERE total < 0
    ```

=== "You write `SELECT *`"

    ```sql
    -- Your query (failed rows): lists the offending rows
    SELECT * FROM orders WHERE total < 0

    -- vowl wraps it in COUNT(*) for the scalar query
    SELECT COUNT(*) FROM (SELECT * FROM orders WHERE total < 0)
    ```

The two are just different views of the same check. The `COUNT(*)` / `SELECT *`
rewrite is what keeps them in sync.

!!! info "How the rewrite works: sqlglot, not string manipulation"
    vowl does not regex or string-replace `COUNT(*)` with `SELECT *`. It parses
    your query into an AST using [sqlglot](https://github.com/tobymao/sqlglot),
    a SQL transpiler, and performs the substitution structurally. This means it
    correctly handles nested subqueries, CTEs, and dialect-specific syntax
    without mangling your SQL.

!!! note "The failed-rows query is lazy"
    The scalar query always runs, because vowl needs it to decide the verdict.
    The failed-rows query only runs when a check **fails** and something asks for
    the rows (e.g. `get_annotated_output()`, `show_failed_rows()`, or
    `output_mode="failed_rows"`). So it's *up to* two queries per check, not
    always two — passing checks cost a single query.

## Row-level or table-level?

Both, at different stages — and this is the distinction that trips people up.

Take a referential-integrity check:

```sql
SELECT COUNT(*)
FROM demo_employee_payroll payroll
LEFT JOIN demo_employee_list ref
  ON payroll.employee_id = ref.employee_id
WHERE ref.employee_id IS NULL
```

- The **verdict is table-level.** `COUNT(*) mustBe 0` is a single number over the
  whole table. On its own it only tells you that *some* payroll rows have no
  matching master record — not which ones.
- The **failing rows are still recoverable.** Because vowl rewrites that
  `COUNT(*)` into `SELECT *`, it can fetch the exact rows behind the number. So a
  count-with-a-join check *can* tell you which rows failed.

In other words, the join is just how you *express* the condition. It doesn't stop
vowl from recovering the individual offending rows — that recovery is exactly
what makes it possible to map a cross-table failure back onto a single table's
annotated output (below).

!!! info "How the database runs it is not your concern"
    A `COUNT(*)` over a `LEFT JOIN` *looks* expensive, but you're describing a
    result, not an execution plan. The database's query optimizer rewrites these
    standard shapes into efficient plans — the `LEFT JOIN ... WHERE ref.key IS
    NULL` anti-join is typically executed as a single-pass hash anti-join over
    the (usually indexed/unique) join key, not a row-by-row comparison. A
    readable, correct query is almost always the right call; if one is ever slow,
    the fix is normally fresh table statistics or an index on the join key, not
    hand-simplifying the SQL.

## Making a cross-table check annotate onto a table

By default a cross-table check becomes a **residue**: it spans two tables, so
vowl has no single table to annotate it onto (this is
[merge condition 2](known-issues.md#annotated-output-not-all-checks-can-be-merged)).
The residue still contains the offending rows and the `check_name`, just as a
separate entry rather than a column on the annotated table.

If you *want* those failures to appear in a table's `check_info` column, you can
shape the failed-rows query so it returns **exactly the columns of the table you
want to annotate**. The trick is to wrap the join in a subquery that selects only
the anchor table's columns:

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

Here's why this works. When vowl rewrites the outer `COUNT(*)` into `SELECT *`,
it only swaps the **outer** select list — the subquery is left untouched:

```sql
SELECT * FROM (
  SELECT payroll.*
  FROM demo_employee_payroll payroll
  LEFT JOIN demo_employee_list ref
    ON payroll.employee_id = ref.employee_id
  WHERE ref.employee_id IS NULL
) AS orphaned_payroll
```

The inner `SELECT payroll.*` is what decides the columns, so the failed rows come
back with **only the payroll columns**, using their bare names (`employee_id`,
`name`, `salary`, …). `payroll.*` qualifies *which* table to expand; it does not
prefix the column names (you'd only get a `payroll_` prefix if you aliased a
column explicitly). That column set matches `demo_employee_payroll`, so the rows
map straight back onto its annotated table.

Compare the two shapes:

| Query shape | Failed-rows columns | Annotates onto payroll? |
| --- | --- | --- |
| Bare `SELECT COUNT(*) FROM payroll LEFT JOIN ref …` | Both tables' columns (`ref.*` all NULL) | No — column set doesn't match |
| Subquery with `SELECT payroll.*` (above) | Payroll columns only | Yes |

!!! warning "This is author-driven, and the merge is by column structure"
    vowl decides mergeability by **column structure**, not by intent. Any
    failed-rows result whose columns match the anchor table will be merged onto
    it — so it's on you to ensure the subquery projects the right columns
    (`payroll.*`, not a partial column list, and not both tables' columns). Get
    this wrong and the check either won't merge (column mismatch → residue) or
    could merge rows you didn't intend. When in doubt, run
    `get_annotated_output()` and inspect both `annotated` and `residues`.

!!! note "`NOT EXISTS` is an equivalent, often cleaner form"
    The same result with no subquery wrapper — the reference table lives only in
    the subquery, so `SELECT *` naturally resolves to payroll columns:

    ```sql
    SELECT COUNT(*)
    FROM demo_employee_payroll payroll
    WHERE NOT EXISTS (
      SELECT 1 FROM demo_employee_list ref
      WHERE ref.employee_id = payroll.employee_id
    )
    ```
