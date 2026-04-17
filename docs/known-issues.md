---
description: >-
  Known issues and workarounds for vowl, including MSSQL regex limitations
  and backend-specific behaviours.
---

# Known Issues

## MSSQL: No Regex Support

SQL Server does not support `REGEXP_LIKE`. Pattern-based checks, including `logicalType` format validation (e.g. date patterns like `YYYY-MM`) and `logicalTypeOptions.pattern`, will return `ERROR` status when running against MSSQL.

**Affected checks:**

- `logicalType` checks that validate string format via regex (e.g. `date`, `timestamp`, `time`)
- `logicalTypeOptions.pattern` checks
- `library` metric `invalidValues` with `arguments.pattern`

**Workaround:** Use the DuckDB ATTACH compatibility mode to route queries through DuckDB, which has full regex support:

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

Oracle has several SQL dialect differences that can cause some generated checks to `ERROR`:

- **No `LIMIT` clause:** Ibis handles this via `FETCH FIRST N ROWS ONLY`, but edge cases may arise.
- **No `!~` regex operator:** vowl applies an AST transform to use `REGEXP_LIKE` on Oracle, but complex patterns may not translate cleanly.
- **Case-sensitive identifiers:** Oracle folds unquoted identifiers to uppercase. If your source tables were created with quoted lowercase column names, auto-generated checks may fail to match. vowl applies quoting transforms to mitigate this, but mismatches can still occur.
- **`TEXT`/`CLOB` types cannot be used in `REGEXP_LIKE`:** vowl automatically casts these to `VARCHAR(4000)`, which truncates values longer than 4000 characters.

## SQLite: Regex via User-Defined Function

SQLite does not natively support `REGEXP_LIKE`. vowl rewrites regex checks to use Ibis's `_IBIS_REGEX_SEARCH` user-defined function, which Ibis registers automatically when using a DuckDB or SQLite backend. This generally works but may behave differently from server-side regex implementations in edge cases.

## Multi-Source Adapters: Data Materialisation

When using `MultiSourceAdapter` (passing `adapters={}` to `validate_data`), each table is materialised into a local DuckDB instance before running checks. This means:

- **Memory usage** scales with the size of your tables.
- **Network transfer:** the full table (or filtered subset) is downloaded to the client.
- Large tables may cause out-of-memory errors.

For large datasets, prefer the **DuckDB ATTACH** approach which streams data on demand without materialisation. See [Usage Patterns](usage-patterns.md#option-a-duckdb-attach) for details.

### Why Not Use DuckDB ATTACH Internally?

The `MultiSourceSQLExecutor` materialises tables via Arrow instead of using DuckDB's `ATTACH` statement for several technical reasons:

1. **Table namespace mismatch.** DuckDB ATTACH places tables under a qualified namespace (e.g. `pg_db.public.my_table`). User-authored contract queries use bare table names (`my_table`). `USE` or `SET search_path` resolves this for a single attached database, but cross-database joins (the core multi-source use case) require every table reference to be fully qualified. Rewriting arbitrary user SQL to inject per-table namespace prefixes is fragile and error-prone.

2. **No access to connection credentials.** DuckDB ATTACH requires a connection string (`host=... port=... dbname=... user=... password=...`), not a live connection object. vowl receives an Ibis connection from the user. Reconstructing credentials would require accessing private Ibis internals (`_con_kwargs`), would not work for connections created via `from_connection()` or environment-based auth, and would surface passwords in SQL strings.

3. **Limited backend coverage.** DuckDB ATTACH only supports PostgreSQL, MySQL, and SQLite. vowl supports 12+ Ibis backends (Snowflake, Spark, BigQuery, Trino, ClickHouse, Oracle, MSSQL, DataFusion, etc.). The materialisation path would still be needed for every unsupported backend, so ATTACH would only serve as a partial optimisation.

4. **Filter conditions cannot be pushed through ATTACH.** When an adapter has filter conditions, `export_table_as_arrow()` applies them at the source before export. With ATTACH, the remote table is exposed raw, and pushing per-adapter filters into cross-database joins would require deep query rewriting.

5. **ATTACH opens a separate connection.** DuckDB ATTACH creates a new, independent connection to the remote database. This misses any session-level state on the user's Ibis connection (transactions, temp tables, session variables, `search_path`).

---

## Null Handling Varies Across Database Backends

Different database backends handle `NULL` values differently in generic checks such as `minimum`, `maximum`, and `mean`. Some backends silently skip `NULL` rows when computing aggregates, while others may include them or produce unexpected results. This means a column containing `NULL` values might still pass a `minimum` or `maximum` check because the nulls are ignored during evaluation.

If you need to guarantee that a column contains **no null values**, add an explicit `nullValues` library check rather than relying on aggregate checks to catch them:

```yaml
properties:
  - name: my_column
    quality:
      - id: my_column_no_nulls
        metric: nullValues
        mustBe: 0
        description: "There must be no null values in the column."
```

This ensures nulls are caught directly, regardless of which database backend is running the validation.

---

## Dark Patterns

### Queries Accessing Tables Outside the Contract

SQL checks in a contract can reference any table reachable on the connection, not just the tables declared in the contract's `schema` block. For example, a check like this:

```yaml
quality:
  - type: sql
    name: "cross_reference_check"
    query: "SELECT COUNT(*) FROM hdb_resale_prices h JOIN audit_log a ON h.id = a.record_id WHERE a.flagged = 1"
    mustBe: 0
```

will happily query `audit_log` even if it is not declared anywhere in the contract. vowl extracts and reports the tables involved via `tables_in_query` in the results, but it does **not** block execution against undeclared tables.

**Why this matters:**

- It breaks the principle that the contract is the single source of truth for what data is being validated.
- It creates implicit dependencies on tables that may not be obvious to reviewers of the contract.
- It can inadvertently expose data from tables the contract author did not intend to include.

**Backend differences:**

| Adapter | Behaviour |
|---------|-----------|
| `IbisAdapter` (native Ibis connection) | Works: the query runs against whatever the connection can reach. |
| `MultiSourceAdapter` | Works: all materialised tables are available in the local DuckDB instance, so cross-references between them succeed. |
| DuckDB ATTACH | **May not work:** only tables explicitly attached or aliased as views are visible. References to undeclared tables will fail with a missing table error. |

This is one reason the `MultiSourceAdapter` materialisation approach remains available despite its memory cost: it preserves the ability to run cross-table checks that reference tables outside the contract.

!!! warning
    Treat SQL checks that reference tables not in your contract's `schema` as a code smell. Consider declaring all referenced tables explicitly, even if they are not the primary validation target.
