---
description: >-
  Known issues and workarounds for vowl, including MSSQL regex limitations
  and backend-specific behaviours.
---

# Known Issues & Caveats

## Database Backend Differences

### Null Handling Varies Across Backends

Database backends handle `NULL` differently in aggregate checks like `minimum`, `maximum`, and `mean`. Most backends silently skip nulls, which means a column full of nulls can still pass a `minimum` check (because there are no non-null values to violate the constraint).

If you need to catch nulls, add an explicit `nullValues` check rather than relying on aggregate checks to find them:

```yaml
properties:
  - name: my_column
    quality:
      - id: my_column_no_nulls
        type: library
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

### SQLite: Parallel Checks Need a Thread-Safe Connection

`PooledAdapter` hands each connection to one worker thread at a time, but not
always the same thread. Python's `sqlite3` refuses by default to use a
connection on a thread other than the one that opened it. So a pooled SQLite
connection opened with `ibis.connect("sqlite://...")` fails some checks with
`ERROR`, depending on which thread picks them up.

SQLite itself is thread-safe, so you can turn the guard off. Open the
connection yourself with `check_same_thread=False` and hand it to Ibis:

```python
import sqlite3
import ibis

raw_con = sqlite3.connect("my.db", check_same_thread=False)
con = ibis.sqlite.from_connection(raw_con)
```

This is safe with `PooledAdapter`, because it never gives one connection to
two threads at once.

### Native Array Checks

Array checks (see [Array Formats](contracts.md#array-formats)) rely on native array SQL. vowl builds them from contract metadata without inspecting the actual column type, so on an engine that doesn't support arrays the check returns `ERROR` rather than silently passing.

**Only DuckDB is tested.** The array checks are verified against DuckDB, where all three constructs execute correctly. For every other engine, the behaviour below is _inferred from the SQL vowl emits_ rather than observed from an actual run, so treat the table as expectations rather than guarantees. Outside DuckDB, validate against your own data or expect an `ERROR` for unsupported constructs.

| Array check                    | SQL construct       | Expected to work (untested)                   | Expected to ERROR                                                     |
| ------------------------------ | ------------------- | --------------------------------------------- | --------------------------------------------------------------------- |
| `minItems` / `maxItems`        | `ARRAY_LENGTH`      | spark, snowflake, trino, bigquery, clickhouse | engines without arrays (sqlite, mysql, tsql, oracle)                  |
| `uniqueItems`                  | `ARRAY_DISTINCT`    | spark, snowflake, trino, clickhouse           | **postgres, bigquery** (no `array_distinct` builtin)                  |
| `items.*` (element validation) | `UNNEST` + `EXISTS` | postgres, trino, bigquery                     | clickhouse, sqlite, mysql, tsql, oracle (spark/snowflake best-effort) |

---

## Multi-Source Adapters: Tables Copied into Memory

<a id="multi-source-adapters-data-materialisation"></a>

With `adapters={...}`, a cross-table check whose tables are on different connections runs on copies of those tables in an in-memory DuckDB on your machine. Large tables can use a lot of memory and network, and may run out of memory. Add filter conditions to copy less, or use [DuckDB ATTACH](usage-patterns.md#option-a-duckdb-attach) if your databases support it. See [Copying tables into memory](design-considerations/cross-table/how-it-works.md#copying-tables-into-memory).

### PooledAdapter: Joins Across Pools Are Copied

A join across two different pools, or across a pool and another adapter, is copied into memory, even when both reach the same database. Pass one pool for every schema a join reads to keep it in the database. Separate pools also run their schemas one at a time. See [PooledAdapter](design-considerations/cross-table/how-it-works.md#pooledadapter).

### Why Not Use DuckDB ATTACH Internally?

vowl can't rely on ATTACH because it has no connection details, most backends don't support it, and table names and filter conditions would have to be rewritten. See [Why vowl copies instead of using ATTACH](design-considerations/cross-table/how-it-works.md#why-vowl-copies-instead-of-using-attach).

---

## Annotated Output: Not All Checks Can Be Merged

<a id="1-cross-table-checks-mergeable-when-the-failed-rows-match-the-home-schema"></a>

`get_annotated_output()` annotates your table with a check's failed rows only when those rows hold the table's columns, or its primary key. Checks that return one number (an average, a `rowCount`), checks whose failed rows are good rows, and checks that ended in `ERROR` annotate nothing and appear only in the summary. Checks whose failed rows have other columns become residues. See:

- [Where each failed check ends up](design-considerations/failed-rows/levels.md#where-each-failed-check-ends-up)
- [Which Checks Contribute Failed Rows](design-considerations/failed-rows/which-checks.md)
- [Annotating the failed rows of a cross-table check](design-considerations/cross-table/how-it-works.md#annotating-the-failed-rows-of-a-cross-table-check)

---

## Queries that Read Tables Outside the Contract

<a id="dark-patterns"></a>
<a id="queries-accessing-tables-outside-the-contract"></a>

A SQL check can read a table the contract doesn't declare, such as a `currencies` lookup table. vowl reads it through the adapter of the schema the check belongs to, and does not block it. If that connection can't see the table, the check ends in `ERROR`. In a run across connections, the same name can mean two different tables. See [Tables outside the contract](design-considerations/cross-table/how-it-works.md#tables-outside-the-contract) for what happens in each setup.

!!! warning

    Avoid SQL checks that read undeclared tables. Declare all referenced tables in your contract's `schema`, even if they're not the primary validation target.
