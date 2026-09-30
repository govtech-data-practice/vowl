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

Array checks (see [Array Checks](contracts.md#array-checks)) rely on native array SQL. vowl builds them from contract metadata without inspecting the actual column type, so on an engine that doesn't support arrays the check returns `ERROR` rather than silently passing.

**Only DuckDB is tested.** The array checks are verified against DuckDB, where all three constructs execute correctly. For every other engine, the behaviour below is _inferred from the SQL vowl emits_ rather than observed from an actual run, so treat the table as expectations rather than guarantees. Outside DuckDB, validate against your own data or expect an `ERROR` for unsupported constructs.

| Array check                    | SQL construct       | Expected to work (untested)                   | Expected to ERROR                                                     |
| ------------------------------ | ------------------- | --------------------------------------------- | --------------------------------------------------------------------- |
| `minItems` / `maxItems`        | `ARRAY_LENGTH`      | spark, snowflake, trino, bigquery, clickhouse | engines without arrays (sqlite, mysql, tsql, oracle)                  |
| `uniqueItems`                  | `ARRAY_DISTINCT`    | spark, snowflake, trino, clickhouse           | **postgres, bigquery** (no `array_distinct` builtin)                  |
| `items.*` (element validation) | `UNNEST` + `EXISTS` | postgres, trino, bigquery                     | clickhouse, sqlite, mysql, tsql, oracle (spark/snowflake best-effort) |

---

## Multi-Source Adapters: Tables Copied into Memory

When you pass `adapters={...}` to `validate_data`, single-table checks run inside each table's own database. A cross-table check whose tables share one connection runs in that database too. A cross-table check across different connections can't, so vowl copies each table it reads into an in-memory DuckDB on your machine and runs the check there. This means:

- **Memory usage** grows with table size, so large tables may cause out-of-memory errors.
- **Network transfer:** the full table, minus rows your filter conditions leave out, travels to your machine.

For large datasets, prefer the **DuckDB ATTACH** approach. It reads the rows each check needs while the check runs, and doesn't copy whole tables into memory first. See [Connecting to Your Data](usage-patterns.md#option-a-duckdb-attach) for details.

### PooledAdapter: Joins Across Pools Are Copied

A cross-table check whose tables are all served by one `PooledAdapter` runs in the database. This includes `adapter=pooled` on a contract with several schemas. vowl takes one of the pool's connections for the check, applies each table's filter conditions, and runs the query there.

vowl copies the tables into memory and runs the join in DuckDB when they come from:

- **Two different pools**, even if both factories open the same database.
- **A pool and another adapter**, such as an `IbisAdapter`, even on the same connection.

vowl can't tell that two factories reach the same database, so it treats each pool as its own source.

- **Results are correct.** Each table keeps its adapter's filter conditions, as in any copy.
- **The cost is memory and network.** It grows with the size of the tables the join reads.
- **Single-table checks are not affected.** They still run in the database on the pool's connections.

To keep a join in the database, pass one pool for every schema it reads, for example `adapter=pooled`.

Separate pools also don't share work. Schemas given the same pool run their single-table checks side by side, up to `max_concurrency`. Schemas on different pools, or on a pool and another adapter, run one schema at a time, and each pool runs its own schema's checks in parallel.

### Why Not Use DuckDB ATTACH Internally?

vowl copies tables into memory (as Arrow tables) instead of using DuckDB ATTACH, for these reasons:

1. **Table names don't line up.** DuckDB ATTACH puts tables under a qualified path (e.g. `pg_db.public.my_table`), but contract queries use bare names like `my_table`. For joins across databases (the main multi-source use case), every table reference would need rewriting, which is fragile.

2. **No access to connection credentials.** DuckDB ATTACH needs a connection string with host/port/password, but vowl only receives a live Ibis connection object. There's no reliable way to extract credentials from it.

3. **Limited backend support.** DuckDB ATTACH only works with PostgreSQL, MySQL, and SQLite. vowl supports any Ibis backend, so it would need to copy tables for most of them anyway.

4. **Filters would be hard to apply at the source.** When copying, vowl applies each adapter's filter conditions in the source database before any rows travel. With ATTACH, the remote table is exposed as is, and adding each adapter's filters to a join across databases would mean rewriting the query.

5. **ATTACH opens a separate connection.** This bypasses any session state on the user's Ibis connection (transactions, temp tables, session variables, `search_path`).

---

## Annotated Output: Not All Checks Can Be Merged

<a id="1-cross-table-checks-mergeable-when-the-failed-rows-match-the-home-schema"></a>

`get_annotated_output()` marks a check's failed rows on your table only when
those rows have the same columns as the table. Checks that return one number
(an average, a `rowCount`), checks whose failed rows are good rows, and checks
that hit an error mark nothing and appear only in the summary. Checks whose
failed rows have other columns become residues.

The rules, and how to write a cross-table check so it marks rows, are in
[Handling of Failed Rows](failed-rows.md):

- [Where each failed check ends up](failed-rows.md#where-each-failed-check-ends-up)
- [Which checks are counted](failed-rows.md#which-checks-are-counted)
- [Writing a cross-table check that marks rows](failed-rows.md#writing-a-cross-table-check-that-marks-rows)
- [What the annotated output holds](failed-rows.md#what-the-annotated-output-holds)

---

## Queries that Read Tables Outside the Contract

<a id="dark-patterns"></a>

### Queries Accessing Tables Outside the Contract

A SQL check can name a table that isn't declared in your contract's `schema`. For example, this check sits under the `orders` schema but reads `currencies`, a lookup table the contract never mentions:

```yaml
quality:
  - type: sql
    name: "unknown_currency"
    query: >
      SELECT COUNT(*) FROM orders o
      LEFT JOIN currencies c ON o.currency = c.code
      WHERE c.code IS NULL
    mustBe: 0
```

vowl reads an undeclared table through the adapter of the schema the check sits under, on that adapter's connection. If that connection can see `currencies`, the check runs and passes or fails like any other. If it can't, the check comes back `ERROR` with the database's own "table not found" message. vowl reports the tables involved via `tables_in_query` but does **not** block undeclared table access.

**Why this matters:**

- The contract is no longer the single source of truth for what's being validated.
- Hidden dependencies on undeclared tables aren't obvious to contract reviewers.
- It may unintentionally expose data the contract author didn't intend to include.

**Joins follow the same rule.** A check under `orders` that joins `orders` to `currencies` reads `currencies` through the `orders` adapter:

- If every table in the join is on that one connection, the query runs there directly and nothing is copied.
- If the join also reads a schema on another connection, vowl copies `currencies` from the `orders` connection into memory and joins the copies in DuckDB, as it does for [cross-table checks across connections](usage-patterns.md#multi-source-validation).
- Two checks under different schemas can both name `currencies`. Each one reads the `currencies` on its own schema's connection, so the same name can mean two different tables in one run.

**What happens in each setup.** The check sits under `orders` and names `currencies`:

| Setup                                                              | Reads only `currencies`       | Joins `currencies` to `orders`                                               | Joins `currencies` to a schema on another connection                                                                                                      |
| ------------------------------------------------------------------ | ----------------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `IbisAdapter` on one connection (`adapter=`)                       | Runs on that connection       | Runs on that connection                                                      | Not applicable, every schema shares the connection                                                                                                        |
| `adapters={...}`, `currencies` on the `orders` connection          | Runs on that connection       | Runs on that connection, nothing copied                                      | Both tables copied into memory, `currencies` from the `orders` connection                                                                           |
| `adapters={...}`, `currencies` only on another schema's connection | `ERROR`, table not found      | `ERROR`, table not found                                                     | `ERROR`, table not found                                                                                                                                  |
| `PooledAdapter` (`adapter=pooled` or in `adapters={...}`)          | Runs on a pooled connection   | Runs on a pooled connection when both schemas share the pool, nothing copied | Both tables copied into memory, `currencies` from the `orders` pool ([joins across pools are copied](#pooledadapter-joins-across-pools-are-copied)) |
| DuckDB ATTACH, bare `currencies` with no view                      | `ERROR`, table not found      | `ERROR`, table not found                                                     | Not applicable, every schema shares the connection                                                                                                        |
| DuckDB ATTACH, attached path (`pg_sales.currencies`) or a view     | Runs on the DuckDB connection | Runs on the DuckDB connection                                                | Not applicable, every schema shares the connection                                                                                                        |

"Table not found" is the database's own error for the connection the table was looked up on. Two more things to know:

- **Filter conditions.** A join that runs on one connection still applies each table's own filter conditions. `currencies` gets the `orders` adapter's conditions that match its name, and every declared schema gets its own adapter's.
- **Schema prefixes.** When a join is copied, vowl drops schema prefixes from table names, so a copied join that names `src1.currencies` can't find it. Use the bare name in joins across connections.

!!! warning
    Avoid SQL checks that read undeclared tables. Declare all referenced tables in your contract's `schema`, even if they're not the primary validation target.
