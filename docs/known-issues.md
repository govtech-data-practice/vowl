---
description: >-
  Known issues and workarounds for vowl, including MSSQL regex limitations
  and backend-specific behaviours.
---

# Known Issues

## Database Backend Differences

### Null Handling Varies Across Backends

Database backends handle `NULL` differently in aggregate checks like `minimum`, `maximum`, and `mean`. Most backends silently skip nulls, which means a column full of nulls can still pass a `minimum` check (because there are no non-null values to violate the constraint).

If you need to catch nulls, add an explicit `nullValues` check — don't rely on aggregate checks to find them:

```yaml
properties:
  - name: my_column
    quality:
      - id: my_column_no_nulls
        metric: nullValues
        mustBe: 0
        description: "There must be no null values in the column."
```

This catches nulls directly, regardless of which database backend runs the validation.

### MSSQL: No Regex Support

SQL Server does not support regex (`REGEXP_LIKE`). Any check that uses pattern matching will return `ERROR` when run against MSSQL.

**Affected checks:**

- `logicalType` checks that validate string formats (e.g. `date`, `timestamp`, `time`)
- `logicalTypeOptions.pattern` checks
- `logicalTypeOptions.format` checks for string, date, timestamp, and time logical types
- `library` metric `invalidValues` with `arguments.pattern`

**Workaround:** Route queries through DuckDB instead, which has full regex support:

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

con = ibis.duckdb.connect()
con.raw_sql("ATTACH 'mssql://user:pass@host:1433/mydb' AS mssql_db (TYPE sqlserver, READ_ONLY)")
con.raw_sql("USE mssql_db")

result = validate_data("contract.yaml", adapter=IbisAdapter(con))
```

### Oracle: Dialect Differences

Oracle's SQL dialect differs from standard SQL in ways that can cause some checks to `ERROR`:

- **No `LIMIT` clause:** Ibis rewrites this as `FETCH FIRST N ROWS ONLY`, but edge cases may arise.
- **No `!~` regex operator:** vowl rewrites regex checks to use `REGEXP_LIKE`, but complex patterns may not translate cleanly.
- **Case-sensitive identifiers:** Oracle uppercases unquoted identifiers. If your tables were created with quoted lowercase names (e.g. `CREATE TABLE "my_table"`), checks may fail because Oracle looks for `MY_TABLE` instead. vowl applies quoting transforms, but mismatches can still occur.
- **`TEXT`/`CLOB` columns can't use `REGEXP_LIKE`:** vowl auto-casts these to `VARCHAR(4000)`, which means values longer than 4000 characters get truncated before the regex runs.

### SQLite: Regex via User-Defined Function

SQLite has no built-in regex support. vowl works around this by using a Python-side regex function (`_IBIS_REGEX_SEARCH`) that Ibis registers automatically. This works in most cases, but may behave slightly differently from server-side regex (e.g. subtle Unicode or flag differences).

---

## Multi-Source Adapters: Data Materialisation

When using `MultiSourceAdapter` (passing `adapters={}` to `validate_data`), vowl downloads each table into a local DuckDB instance before running checks. This means:

- **Memory usage** grows with table size — large tables may cause out-of-memory errors.
- **Network transfer:** the full table (or filtered subset) is pulled to the client.

For large datasets, prefer the **DuckDB ATTACH** approach which queries data in-place without downloading it. See [Usage Patterns](usage-patterns.md#option-a-duckdb-attach) for details.

### Why Not Use DuckDB ATTACH Internally?

vowl materialises tables via Arrow instead of using DuckDB ATTACH for these reasons:

1. **Table names don't line up.** DuckDB ATTACH puts tables under a qualified path (e.g. `pg_db.public.my_table`), but contract queries use bare names like `my_table`. For cross-database joins (the main multi-source use case), every table reference would need rewriting — which is fragile.

2. **No access to connection credentials.** DuckDB ATTACH needs a connection string with host/port/password, but vowl only receives a live Ibis connection object. There's no reliable way to extract credentials from it.

3. **Limited backend support.** DuckDB ATTACH only works with PostgreSQL, MySQL, and SQLite. vowl supports any Ibis backend, so materialisation is needed anyway for most of them.

4. **Filters can't be pushed down.** With materialisation, vowl applies filter conditions at the source before downloading. With ATTACH, the remote table is exposed raw and pushing per-adapter filters into cross-database joins would require complex query rewriting.

5. **ATTACH opens a separate connection.** This bypasses any session state on the user's Ibis connection (transactions, temp tables, session variables, `search_path`).

---

## Annotated Output: Not All Checks Can Be Merged

`get_annotated_output()` (and `save(output_mode="annotated")`) returns your **full table** with an extra `check_info` column showing which check(s) each row failed. However, not every check can be merged into this table — some checks simply don't produce results that map back to individual rows.

```python
output = result.get_annotated_output()
output["annotated"]   # {schema: full table + check_info}     <- mergeable checks
output["residues"]    # {"<schema>::<check>": failed rows + check_info + tables_in_query}  <- non-mergeable checks that still have offending rows
```

Note that `residues` only holds non-mergeable checks that **still produce offending rows** (column-subset checks, and cross-table checks whose failed rows carry columns from more than the anchor table — see §1 below). A non-mergeable check with no rows to emit (a scalar aggregation like `AVG`/`SUM`/`MIN`/`MAX`, `rowCount`, or an errored check) appears in neither dict; its verdict is recorded only in `summary.json`.

The `check_info` column holds a JSON array of objects (one per failing check), shaped by the `check_info` preset (`"names"` default, `"summary"`, `"full"`). Residues are **per-check** — one entry per non-mergeable check, keyed `"<schema>::<check_name>"`, each carrying its own failed rows plus the **same `check_info` column** the annotated tables use (a single-element JSON array) and `tables_in_query`. Two non-mergeable checks are never grouped into one entry, and a check that was annotated onto a full table never reappears as a residue. So everything `get_annotated_output()` returns — annotated tables and residues alike — is read the same way. (The standalone `failed_rows`/`both` CSVs come from a separate, unchanged path and keep their legacy comma-joined `check_ids` column.)

For example, suppose your full table `hdb_resale_prices` looks like this:

| month   | town       | block | street_name    | flat_type | storey_range | floor_area_sqm | lease_commence_date | remaining_lease | resale_price |
| ------- | ---------- | ----- | -------------- | --------- | ------------ | -------------- | ------------------- | --------------- | ------------ |
| 2024-01 | ANG MO KIO | 123   | ANG MO KIO AVE | 3 ROOM    | 04 TO 06     | 68             | 1980                | 55 years        | 350000       |
| 2024-01 | BEDOK      | 456   | BEDOK NORTH    | 4 ROOM    | 07 TO 09     | 92             | 1995                | 70 years        | 480000       |
| 2024-02 | TAMPINES   | 789   | TAMPINES ST    | 5 ROOM    | 10 TO 12     | 110            | 2000                | 75 years        | 620000       |

A **mergeable** check (e.g. a row-level check like "resale_price must be > 0") can tag individual rows directly, producing an annotated table like:

| month   | town       | block | ... | resale_price | check_info                                  |
| ------- | ---------- | ----- | --- | ------------ | ------------------------------------------- |
| 2024-01 | ANG MO KIO | 123   | ... | 350000       | null                                        |
| 2024-01 | BEDOK      | 456   | ... | 480000       | null                                        |
| 2024-02 | TAMPINES   | 789   | ... | 620000       | `[{"check_name": "resale_price_positive"}]` |

This split is by design. A check can only be merged into the annotated table when **all** of the following are true:

1. **The check didn't error.** An errored check has no usable failed rows.
2. **It produces row-level results** (aggregation type is `count` or `none`). Checks that return a single number (like `mean` or `maximum`) can't point to specific rows.
3. **Its failed rows have the same columns as the full table.** If a check only selects a few columns, we can't match results back to full rows. This is what decides a cross-table check too (see below): the merge is driven purely by the failed-rows column set, not by how many tables the query touches.

When a condition fails, the check is not merged onto the annotated table. What happens next depends on _why_ it failed to merge:

- **It still has offending rows** (condition 3, i.e. column-subset checks, or a cross-table check whose rows carry both tables' columns) → those rows are emitted as a **residue** (returned separately), keyed `"<schema>::<check_name>"`.
- **It has no offending rows to emit** (condition 2, i.e. a scalar aggregation like `AVG`/`SUM`/`MIN`/`MAX`, or an errored check) → there is **nothing to put in a residue either**. The failure appears only in `summary.json` (status, `actual_value`, `expected_value`). It is **not** written to any CSV.

> **Heads-up: a failed scalar aggregation has no CSV footprint in `annotated` mode.**
> It tags no rows in the annotated table (its `check_info` stays `null`) and produces no
> residue file, so the only record of the failure is `summary.json`. Always consult the
> summary for the authoritative pass/fail verdict; the annotated CSVs alone do not surface
> scalar-aggregation or errored-check failures.

The common cases:

### 1. Cross-table checks: mergeable when the failed rows match the home schema

A cross-table check (one that JOINs a table against a reference table) **can** annotate onto its home schema's table — you just have to shape its failed-rows query so it projects **only that schema's columns**. Whether it merges is decided entirely by condition 3 (the failed-rows column set), not by how many tables the query touches.

Every SQL check derives two queries from the single query you write: a **scalar query** (the `SELECT COUNT(*)` that decides pass/fail) and a lazy **failed-rows query** (`SELECT *` over the same `FROM`, run only when the check failed and rows are requested). The `COUNT(*)` → `SELECT *` rewrite only touches the **outer** select list, so a subquery that projects one table's columns still governs the shape of the failed rows.

**Mergeable — project only the anchor table's columns via a wrapping subquery:**

```yaml
# Anchored to demo_employee_payroll; failed rows are payroll rows only.
quality:
  - type: sql
    name: employee_id_exists_in_master_list
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
```

The failed-rows query rewrites to `SELECT * FROM (SELECT payroll.* …)`, which returns **only `demo_employee_payroll`'s columns**. Those rows match the anchor table exactly, so the orphan payroll rows are annotated directly into `demo_employee_payroll`'s `check_info` column — no residue, no downstream mapping.

**Non-mergeable — a bare JOIN returns both tables' columns:**

```yaml
# Failed rows carry columns from BOTH tables (ref.* all NULL) -> residue.
quality:
  - type: sql
    name: employee_id_exists_in_master_list
    query: >-
      SELECT COUNT(*) FROM demo_employee_payroll p
      LEFT JOIN demo_employee_list e ON p.employee_id = e.employee_id
      WHERE e.employee_id IS NULL
    mustBe: 0
```

Here the failed-rows query rewrites to a top-level `SELECT *` over the JOIN, which returns **both** tables' columns. That column set doesn't match `demo_employee_payroll`, so the check stays a **residue** keyed `"demo_employee_payroll::employee_id_exists_in_master_list"` — the same, backward-compatible behaviour existing bare-JOIN checks already have.

> **The merge is decided by column structure, not intent.** A misshaped query that
> happens to return anchor-shaped rows _will_ merge — this is the same class of risk
> single-table custom SQL checks already carry. It is mitigated by two guards: the check
> is only ever considered against **its own declared schema** (a payroll-anchored check
> can never merge onto an unrelated table with a coincidentally-matching shape), and the
> failed-rows column set must match that schema's columns **exactly**.

### 2. Scalar-aggregation checks (fails condition 2): no residue at all

Checks that produce a single number (e.g. `AVG`, `MAX`, `SUM`) can't point to specific rows.

```yaml
properties:
  - name: resale_price
    quality:
      - type: sql
        name: avg_resale_price_in_range
        query: "SELECT AVG(resale_price) FROM hdb_resale_prices"
        mustBeBetween:
          - 100000
          - 2000000
```

The query result is just one number:

| avg       |
| --------- |
| 483333.33 |

There are no individual rows to flag: the result is a single scalar, so it can't be annotated onto the full table. Crucially, there are also no rows to put in a residue: a residue holds _offending rows_, and a scalar verdict has none. So unlike the cross-table and column-subset cases below, a failed scalar aggregation produces **neither an annotated tag nor a residue file**; the failure lives only in `summary.json`.

Note: `rowCount` is an aggregate too. Its query is a bare `SELECT COUNT(*) FROM t` with no failure predicate, so the count measures table cardinality rather than a number of failing rows; there is no per-row failure to annotate. Like `AVG`/`MAX`/`SUM`, it is non-row-level (fails condition 2) and produces no residue; its verdict is summary-only.

### 3. Column-subset checks (fails condition 3)

A check that only returns _some_ columns can't be matched back to full rows. This happens with custom SQL `query:` checks that `SELECT` (or `GROUP BY`) a subset of columns rather than whole rows:

```yaml
properties:
  - name: resale_price
    quality:
      - type: sql
        name: distinct_towns_with_outliers
        query: >-
          SELECT town
          FROM hdb_resale_prices
          GROUP BY town
          HAVING MAX(resale_price) > 2000000
        mustBe: 0
```

The query result might look like:

| town       |
| ---------- |
| ANG MO KIO |

This tells us a town has an outlier, but the result only has 1 column. The full table has 10+ columns, so we can't match this partial result back to specific full rows — it becomes a residue.

> **Auto-generated `unique`, `primaryKey`, and `duplicateValues` checks are mergeable.**
> Although these are implemented with `GROUP BY … HAVING COUNT(*) > 1` internally, vowl
> rewrites their failed-rows query to return the **full participating rows** (every row
> whose value belongs to a duplicate group, plus NULL primary keys) via an `IN`/`EXISTS`
> predicate against the base table. They therefore annotate directly onto the table rather
> than becoming residues. Their reported `failed_rows_count` counts participating _rows_
> (not duplicate _groups_), so it matches the number of annotated rows. The `percent`-unit
> variant of `duplicateValues` stays non-mergeable (its result is a ratio, not a row count).

### Consolidated output groups; annotated residues are per-check

`get_consolidated_output_dfs()` (used by `output_mode="failed_rows"`/`"both"`) **groups** failed rows by `(tables_in_query, column_set)`, deduplicating identical rows and comma-joining the check names that hit them — cross-table failures included, keyed by composite table name (e.g. `"table_a, table_b"`).

`get_annotated_output()`'s `residues` instead emit **one entry per non-mergeable check**, keyed `"<schema>::<check_name>"`, never grouped across checks. So the same non-mergeable failure looks different between the two: grouped (possibly multi-check) rows with a comma-joined `check_ids` column in the failed-rows CSVs, vs. a single-check entry with a `check_info` JSON-array column under annotated residues.

If you rely solely on annotated output, always check `residues` for non-mergeable failures.

### Other things to know

- **A table can have both.** If a table has mergeable _and_ non-mergeable failing checks, you'll get both an annotated table and residue entries for that schema. Mergeable checks are never duplicated into `residues`.
- **Annotated entries exist even when nothing failed.** Every schema with an available adapter gets an annotated table — the `check_info` column is just all null.
- **Missing adapter?** If a schema's adapter is unavailable, that schema is skipped (with a warning) and its failures appear only as residues.
- **`max_failed_rows` raises an error for annotated output.** If you cap failed rows (`max_failed_rows >= 0`) and a mergeable check gets truncated, `get_annotated_output()` raises `ValueError` rather than silently treating un-fetched failures as passing. Use `max_failed_rows=-1` (the default) or switch to `output_mode="failed_rows"`.

- **Identical rows are all flagged.** Rows are matched by their values. If two rows are exactly the same and one fails a check, the other will fail it too, so both are flagged. This is correct. It does mean you may see more flagged rows in the annotated table than the failure count in the summary, which only counts unique failing rows.

---

## Dark Patterns

### Queries Accessing Tables Outside the Contract

SQL checks can reference **any** table the connection can reach, not just those declared in your contract's `schema`. For example:

```yaml
quality:
  - type: sql
    name: "cross_reference_check"
    query: "SELECT COUNT(*) FROM hdb_resale_prices h JOIN audit_log a ON h.id = a.record_id WHERE a.flagged = 1"
    mustBe: 0
```

Here, `audit_log` isn't declared in the contract, but the check runs fine. vowl reports the tables involved via `tables_in_query` but does **not** block undeclared table access.

**Why this matters:**

- The contract is no longer the single source of truth for what's being validated.
- Hidden dependencies on undeclared tables aren't obvious to contract reviewers.
- It may unintentionally expose data the contract author didn't intend to include.

**Backend differences:**

| Adapter                                | Behaviour                                                                                                               |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `IbisAdapter` (native Ibis connection) | Works — the query runs against whatever the connection can reach.                                                       |
| `MultiSourceAdapter`                   | Works — all materialised tables are available in the local DuckDB instance.                                             |
| DuckDB ATTACH                          | **May fail** — only explicitly attached tables are visible. References to undeclared tables give a missing table error. |

!!! warning
Treat SQL checks that reference undeclared tables as a code smell. Declare all referenced tables in your contract's `schema`, even if they're not the primary validation target.
