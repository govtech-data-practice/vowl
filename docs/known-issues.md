# Known Issues

## MSSQL: No Regex Support

SQL Server does not support `REGEXP_LIKE`. Pattern-based checks — including `logicalType` format validation (e.g. date patterns like `YYYY-MM`) and `logicalTypeOptions.pattern` — will return `ERROR` status when running against MSSQL.

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

- **No `LIMIT` clause** — Ibis handles this via `FETCH FIRST N ROWS ONLY`, but edge cases may arise.
- **No `!~` regex operator** — Vowl applies an AST transform to use `REGEXP_LIKE` on Oracle, but complex patterns may not translate cleanly.
- **Case-sensitive identifiers** — Oracle folds unquoted identifiers to uppercase. If your source tables were created with quoted lowercase column names, auto-generated checks may fail to match. Vowl applies quoting transforms to mitigate this, but mismatches can still occur.
- **`TEXT`/`CLOB` types cannot be used in `REGEXP_LIKE`** — Vowl automatically casts these to `VARCHAR(4000)`, which truncates values longer than 4000 characters.

## SQLite: Regex via User-Defined Function

SQLite does not natively support `REGEXP_LIKE`. Vowl rewrites regex checks to use Ibis's `_IBIS_REGEX_SEARCH` user-defined function, which Ibis registers automatically when using a DuckDB or SQLite backend. This generally works but may behave differently from server-side regex implementations in edge cases.

## Multi-Source Adapters: Data Materialisation

When using `MultiSourceAdapter` (passing `adapters={}` to `validate_data`), each table is materialised into a local DuckDB instance before running checks. This means:

- **Memory usage** scales with the size of your tables.
- **Network transfer** — the full table (or filtered subset) is downloaded to the client.
- Large tables may cause out-of-memory errors.

For large datasets, prefer the **DuckDB ATTACH** approach which streams data on demand without materialisation. See [Usage Patterns](usage-patterns.md#option-b-duckdb-attach) for details.

---

## Dark Patterns

### Queries Accessing Tables Outside the Contract

SQL checks in a contract can reference any table reachable on the connection — not just the tables declared in the contract's `schema` block. For example, a check like this:

```yaml
quality:
  - type: sql
    name: "cross_reference_check"
    query: "SELECT COUNT(*) FROM hdb_resale_prices h JOIN audit_log a ON h.id = a.record_id WHERE a.flagged = 1"
    mustBe: 0
```

will happily query `audit_log` even if it is not declared anywhere in the contract. Vowl extracts and reports the tables involved via `tables_in_query` in the results, but it does **not** block execution against undeclared tables.

**Why this matters:**

- It breaks the principle that the contract is the single source of truth for what data is being validated.
- It creates implicit dependencies on tables that may not be obvious to reviewers of the contract.
- It can inadvertently expose data from tables the contract author did not intend to include.

**Backend differences:**

| Adapter | Behaviour |
|---------|-----------|
| `IbisAdapter` (native Ibis connection) | Works — the query runs against whatever the connection can reach. |
| `MultiSourceAdapter` | Works — all materialised tables are available in the local DuckDB instance, so cross-references between them succeed. |
| DuckDB ATTACH | **May not work** — only tables explicitly attached or aliased as views are visible. References to undeclared tables will fail with a missing table error. |

This is one reason the `MultiSourceAdapter` materialisation approach remains available despite its memory cost: it preserves the ability to run cross-table checks that reference tables outside the contract.

!!! warning
    Treat SQL checks that reference tables not in your contract's `schema` as a code smell. Consider declaring all referenced tables explicitly, even if they are not the primary validation target.
