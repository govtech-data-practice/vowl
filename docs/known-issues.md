---
description: >-
  Known issues and workarounds for vowl, including MSSQL regex limitations
  and backend-specific behaviours.
---

# Known Issues

## MSSQL: No Regex Support

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

## Oracle: Dialect Differences

Oracle's SQL dialect differs from standard SQL in ways that can cause some checks to `ERROR`:

- **No `LIMIT` clause:** Ibis rewrites this as `FETCH FIRST N ROWS ONLY`, but edge cases may arise.
- **No `!~` regex operator:** vowl rewrites regex checks to use `REGEXP_LIKE`, but complex patterns may not translate cleanly.
- **Case-sensitive identifiers:** Oracle uppercases unquoted identifiers. If your tables were created with quoted lowercase names (e.g. `CREATE TABLE "my_table"`), checks may fail because Oracle looks for `MY_TABLE` instead. vowl applies quoting transforms, but mismatches can still occur.
- **`TEXT`/`CLOB` columns can't use `REGEXP_LIKE`:** vowl auto-casts these to `VARCHAR(4000)`, which means values longer than 4000 characters get truncated before the regex runs.

## SQLite: Regex via User-Defined Function

SQLite has no built-in regex support. vowl works around this by using a Python-side regex function (`_IBIS_REGEX_SEARCH`) that Ibis registers automatically. This works in most cases, but may behave slightly differently from server-side regex (e.g. subtle Unicode or flag differences).

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

`get_annotated_output()` (and `save(output_mode="annotated")`) returns your **full table** with an extra `check_ids` column showing which check(s) each row failed. However, not every check can be merged into this table — some checks simply don't produce results that map back to individual rows.

```python
output = result.get_annotated_output()
output["annotated"]   # {schema: full table + check_ids}      <- mergeable checks
output["residues"]    # {key: failed rows + check_ids + tables_in_query}  <- everything else
```

For example, suppose your full table `hdb_resale_prices` looks like this:

| month   | town       | block | street_name    | flat_type | storey_range | floor_area_sqm | lease_commence_date | remaining_lease | resale_price |
|---------|------------|-------|----------------|-----------|--------------|----------------|---------------------|-----------------|--------------|
| 2024-01 | ANG MO KIO | 123   | ANG MO KIO AVE | 3 ROOM    | 04 TO 06     | 68             | 1980                | 55 years        | 350000       |
| 2024-01 | BEDOK      | 456   | BEDOK NORTH    | 4 ROOM    | 07 TO 09     | 92             | 1995                | 70 years        | 480000       |
| 2024-02 | TAMPINES   | 789   | TAMPINES ST    | 5 ROOM    | 10 TO 12     | 110            | 2000                | 75 years        | 620000       |

A **mergeable** check (e.g. a row-level check like "resale_price must be > 0") can tag individual rows directly, producing an annotated table like:

| month   | town       | block | ... | resale_price | check_ids              |
|---------|------------|-------|-----|--------------|------------------------|
| 2024-01 | ANG MO KIO | 123   | ... | 350000       | null                   |
| 2024-01 | BEDOK      | 456   | ... | 480000       | null                   |
| 2024-02 | TAMPINES   | 789   | ... | 620000       | resale_price_positive  |

This split is by design. A check can only be merged into the annotated table when **all** of the following are true:

1. **The check didn't error.** An errored check has no usable failed rows.
2. **It queries exactly one table.** A cross-table check (e.g. a JOIN between two tables) has no single table to annotate onto.
3. **It produces row-level results** (aggregation type is `count` or `none`). Checks that return a single number (like `mean` or `maximum`) can't point to specific rows.
4. **Its failed rows have the same columns as the full table.** If a check only selects a few columns, we can't match results back to full rows.

When any condition fails, the check becomes a **residue** (returned separately). The three common cases:

### 1. Cross-table checks (fails condition 2)

Checks that JOIN multiple tables have no single table to annotate onto.

```yaml
# This check spans two tables — which table should the failure appear in?
quality:
  - type: sql
    name: employee_id_exists_in_master_list
    query: >-
      SELECT COUNT(*) FROM demo_employee_payroll p
      LEFT JOIN demo_employee_list e ON p.employee_id = e.employee_id
      WHERE e.employee_id IS NULL
    mustBe: 0
```

The query result might look like:

| count |
|-------|
| 3     |

This tells us 3 payroll rows have missing employee IDs, but the failure belongs to the _relationship_ between the two tables — there's no single table to annotate it onto. It goes to `residues` keyed by `"demo_employee_list, demo_employee_payroll"`.

### 2. Aggregation checks (fails condition 3)

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

| avg        |
|------------|
| 483333.33  |

There are no individual rows to flag — the result is a single scalar, so it can't be annotated onto the full table. It becomes a residue.

Note: `rowCount` is technically an aggregate too, but it fails at condition 4 instead (no data columns). Either way, it's not merged.

### 3. Column-subset checks (fails condition 4)

Checks that only return _some_ columns can't be matched back to full rows. The most common example is duplicate detection, which groups by a subset of columns:

```yaml
properties:
  - name: resale_price
    quality:
      - type: sql
        name: no_duplicate_listings
        query: >-
          SELECT month, block, street_name, flat_type, storey_range
          FROM hdb_resale_prices
          GROUP BY month, block, street_name, flat_type, storey_range
          HAVING COUNT(*) > 1
        mustBe: 0
```

The query result might look like:

| month   | block | street_name    | flat_type | storey_range |
|---------|-------|----------------|-----------|--------------|
| 2024-01 | 123   | ANG MO KIO AVE | 3 ROOM    | 04 TO 06     |

This tells us there's a duplicate, but the result only has 5 columns. The full table has 10 columns (including `town`, `floor_area_sqm`, `lease_commence_date`, `remaining_lease`, `resale_price`). We can't match this partial row back to a specific full row, so it becomes a residue.

### Consolidated output includes cross-table checks; annotated output does not

`get_consolidated_output_dfs()` includes cross-table failures (keyed by composite table name, e.g. `"table_a, table_b"`). `get_annotated_output()` does not — they only appear in `residues`.

If you rely solely on annotated output, always check `residues` for non-mergeable failures.

### Other things to know

- **A table can have both.** If a table has mergeable _and_ non-mergeable failing checks, you'll get both an annotated table and residue entries for that schema. Mergeable checks are never duplicated into `residues`.
- **Annotated entries exist even when nothing failed.** Every schema with an available adapter gets an annotated table — the `check_ids` column is just all null.
- **Missing adapter?** If a schema's adapter is unavailable, that schema is skipped (with a warning) and its failures appear only as residues.
- **`max_failed_rows` raises an error for annotated output.** If you cap failed rows (`max_failed_rows >= 0`) and a mergeable check gets truncated, `get_annotated_output()` raises `ValueError` rather than silently treating un-fetched failures as passing. Use `max_failed_rows=-1` (the default) or switch to `output_mode="failed_rows"`.
- **Duplicate rows may be over-flagged.** Matching is value-based. If two rows are byte-identical and one failed, both get annotated (the safe direction — false positives, not false negatives). A row-id-based matcher is planned.

---

## Null Handling Varies Across Database Backends

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

| Adapter                                | Behaviour                                                                                                            |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `IbisAdapter` (native Ibis connection) | Works — the query runs against whatever the connection can reach.                                                    |
| `MultiSourceAdapter`                   | Works — all materialised tables are available in the local DuckDB instance.                                          |
| DuckDB ATTACH                          | **May fail** — only explicitly attached tables are visible. References to undeclared tables give a missing table error. |

!!! warning
    Treat SQL checks that reference undeclared tables as a code smell. Declare all referenced tables in your contract's `schema`, even if they're not the primary validation target.
