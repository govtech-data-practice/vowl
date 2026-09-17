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

## Design principles for auto-generated checks

vowl derives executable checks from contract metadata (`enum`, `logicalType`,
`required`, `unique`, `primaryKey`, `relationships`, ...) in
`Contract.get_check_references_by_schema()`. These principles keep that
derivation predictable, safe, and version-tolerant.

### Data-driven, not version-branched

Generation keys off the _presence and shape_ of a property attribute, never off
`apiVersion`. A property carrying `enum` produces an enum check whether the
contract declares `v3.1.0` or `v3.2.0`; a newer field simply becomes visible
once contracts start using it. This keeps a single code path valid across the
whole supported ODCS range and avoids a matrix of per-version branches.

### Degrade, don't crash

An attribute vowl cannot turn into a runnable check must never raise out of
generation. Each generator is wrapped so a `ValueError` (unsupported shape,
unresolvable reference, arity mismatch, missing origin) is converted into an
`UnsupportedColumnCheckReference` / `UnsupportedTableCheckReference` plus a
`UserWarning`. The rest of the contract still generates its checks. This mirrors
the existing `logicalTypeOptions` and `enum` handling: a malformed corner of a
contract downgrades that one check, it does not abort validation.

### SQL is built from AST literals, never string interpolation

Contract values (allowed enum values, schema and column names, reference
targets) are attacker-controllable, so generated SQL is assembled as a
[sqlglot](https://github.com/tobymao/sqlglot) expression tree — identifiers via
`exp.to_identifier(..., quoted=True)`, values as `exp.Literal` nodes — and only
then rendered with `.sql(dialect=...)`. No generated query is produced by
f-string or `.format()` concatenation of contract text. A pathological name is
emitted as a single quoted identifier (embedded quotes doubled), so it cannot
break out into a statement; the executor's `validate_query_security` pass is a
second, independent line of defence.

### Dimensions come from the ODCS enum

Generated checks tag themselves with a `dimension` drawn from the ODCS
`quality.dimension` enum. Enum checks use `conformity` (values must conform to
an allowed set). Foreign-key checks use `consistency` (the same dimension
`unique`/`primaryKey` checks use) — deliberately **not** `integrity`, which is
not a member of the ODCS dimension enum and would fail validation.

### Foreign-key checks: `NOT EXISTS` anti-join, MATCH SIMPLE

A `relationships` entry of `type: foreignKey` becomes a referential-integrity
check counting rows in the `from` table whose key has no match in the `to`
table:

```sql
SELECT COUNT(*)
FROM "orders" AS "_vowl_fk_from"
WHERE NOT "_vowl_fk_from"."customer_id" IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM "customers" AS "_vowl_fk_to"
    WHERE "_vowl_fk_to"."id" = "_vowl_fk_from"."customer_id"
  )
```

- **Anti-join, not `LEFT JOIN`.** The `to` table lives only inside the
  `NOT EXISTS`, so the failed-rows rewrite (`COUNT(*)` → `SELECT *`) naturally
  yields **from-table columns only** — mergeable onto the from-table's annotated
  output without a subquery wrapper (see
  [Making a cross-table check annotate onto a table](#making-a-cross-table-check-annotate-onto-a-table)).
- **MATCH SIMPLE null semantics.** A row is skipped if _any_ foreign-key column
  is `NULL` (the `IS NOT NULL` guards). This matches SQL's default composite-FK
  behaviour: a partially-null key is not enforced.
- **Composite keys** are supported at schema level (`from`/`to` as equal-arity
  lists); the equality chain is `AND`-ed across column pairs. Property-level
  relationships are single-column by definition.
- **Self-referential keys** (from and to are the same table) work via distinct
  aliases. Because only one physical table is referenced, such a check stays on
  the single-table execution path; a cross-table FK auto-routes to the
  multi-source executor.
- **Target uniqueness is advisory.** If the target column is not declared
  `unique`/`primaryKey`, vowl still generates the check but emits a `UserWarning`
  — the anti-join is well-defined regardless, but a non-unique target usually
  signals a modelling gap.
- **Nested/array targets are deferred.** A reference into a nested or array
  property degrades to an unsupported reference for now.

### Reference resolution follows RFC 3986

`relationships` targets are resolved the way JSON Schema and OpenAPI resolve
`$ref`, per [RFC 3986 §5](https://www.rfc-editor.org/rfc/rfc3986#section-5):

- **Shorthand** `object.property` resolves by schema/property `name`.
- **Fully-qualified** `/schema/<id>/properties/<id>` matches by `id` and returns
  the target's `name`.
- **External** `file.yaml#/schema/<id>/properties/<id>` loads `file.yaml`
  **relative to the referencing contract's own retrieval location** (its
  `origin`), then resolves the fragment within it. A contract built from
  in-memory data has no origin, so a relative external reference degrades rather
  than guessing a base. All external fetches reuse the SSRF-guarded load path.

Shorthand and fully-qualified references that point at the same property produce
identical SQL — the notation is a lookup convenience, not a semantic
distinction.

### Adapters are caller-supplied, keyed by schema name

vowl does **not** auto-connect to a contract's `servers[]` block to reach an
external foreign-key target. Cross-source checks reach data through adapters the
caller registers under each schema's `name`. An unregistered target hits the
executor's existing "no adapter" degradation. This keeps vowl's data access
explicit and caller-controlled rather than implicitly dialling out to whatever a
contract happens to declare.

!!! note "Tradeoff: external references fetch eagerly at generation time"
Building the anti-join needs the _names_ of the target columns, so an
external foreign key fetches the referenced contract inside
`get_check_references_by_schema()` (cached per reference, SSRF-guarded,
degrading to an unsupported reference on failure). This is the one place
check generation performs network I/O; local and same-contract references
do not.
