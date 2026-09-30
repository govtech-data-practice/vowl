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

`PooledAdapter` runs checks across a thread pool, handing each pooled connection to one worker thread at a time. Python's `sqlite3` connections default to `check_same_thread=True`, which forbids using a connection on any thread other than the one that created it. So a pooled SQLite connection that gets handed to a different worker raises a thread error, which surfaces as an intermittent `ERROR` status (it depends on thread scheduling — sometimes every checkout happens to land back on its creating thread, sometimes not).

The underlying SQLite library is thread-safe (CPython compiles it serialized, `sqlite3.threadsafety == 3`), so this is a Python-level guard, not a real engine limitation. To run pooled/parallel checks against SQLite, build the connection with the guard disabled and wrap it for Ibis:

```python
import sqlite3
import ibis

raw_con = sqlite3.connect("my.db", check_same_thread=False)
con = ibis.sqlite.from_connection(raw_con)
```

This is safe with `PooledAdapter` because it hands each connection to only one thread at a time. **vowl's built-in connection-string path (`ibis.connect("sqlite://...")`) does not set this flag**, so a SQLite adapter created that way and then pooled will hit the limitation. Pass a pre-built thread-safe connection as shown above if you need parallel SQLite.

### Native Array Checks

Array checks (see [Array Checks](contracts.md#array-checks)) rely on native array SQL. vowl builds them from contract metadata without inspecting the actual column type, so on an engine that doesn't support arrays the check returns `ERROR` rather than silently passing.

**Only DuckDB is tested.** The array checks are verified against DuckDB, where all three constructs execute correctly. For every other engine, the behaviour below is _inferred from the SQL vowl emits_ rather than observed from an actual run, so treat the table as expectations rather than guarantees. Outside DuckDB, validate against your own data or expect an `ERROR` for unsupported constructs.

| Array check                    | SQL construct       | Expected to work (untested)                   | Expected to ERROR                                                     |
| ------------------------------ | ------------------- | --------------------------------------------- | --------------------------------------------------------------------- |
| `minItems` / `maxItems`        | `ARRAY_LENGTH`      | spark, snowflake, trino, bigquery, clickhouse | scalar-only engines (sqlite, mysql, tsql, oracle)                     |
| `uniqueItems`                  | `ARRAY_DISTINCT`    | spark, snowflake, trino, clickhouse           | **postgres, bigquery** (no `array_distinct` builtin)                  |
| `items.*` (element validation) | `UNNEST` + `EXISTS` | postgres, trino, bigquery                     | clickhouse, sqlite, mysql, tsql, oracle (spark/snowflake best-effort) |

---

## Multi-Source Adapters: Data Materialisation

When you pass `adapters={...}` to `validate_data`, single-table checks run inside each table's own database. A cross-table check whose tables share one connection runs in that database too. A cross-table check across different connections can't, so vowl downloads each table it reads into a local DuckDB instance and runs the check there. This means:

- **Memory usage** grows with table size, so large tables may cause out-of-memory errors.
- **Network transfer:** the full table (or filtered subset) is pulled to the client.

For large datasets, prefer the **DuckDB ATTACH** approach. It streams the rows each check needs, and doesn't download whole tables into memory before the checks run. See [Connecting to Your Data](usage-patterns.md#option-a-duckdb-attach) for details.

### PooledAdapter: Joins Across Pools Are Copied

A cross-table check whose tables are all served by one `PooledAdapter` runs in the database. This includes `adapter=pooled` on a contract with several schemas. vowl takes one of the pool's connections for the check, applies each table's filter conditions, and runs the query there.

vowl copies the join to a local DuckDB when its tables come from:

- **Two different pools**, even if both factories open the same database.
- **A pool and another adapter**, such as an `IbisAdapter`, even on the same connection.

vowl can't tell that two factories reach the same database, so it treats each pool as its own source.

- **Results are correct.** Each table keeps its adapter's filter conditions, as in any download.
- **The cost is memory and network.** It grows with the size of the tables the join reads.
- **Single-table checks are not affected.** They still run in the database on the pool's connections.

To keep a join in the database, pass one pool for every schema it reads, for example `adapter=pooled`.

Separate pools also don't share work. Schemas given the same pool run their single-table checks side by side, up to `max_concurrency`. Schemas on different pools, or on a pool and another adapter, run one schema at a time, and each pool runs its own schema's checks in parallel.

### Why Not Use DuckDB ATTACH Internally?

vowl materialises tables via Arrow instead of using DuckDB ATTACH for these reasons:

1. **Table names don't line up.** DuckDB ATTACH puts tables under a qualified path (e.g. `pg_db.public.my_table`), but contract queries use bare names like `my_table`. For cross-database joins (the main multi-source use case), every table reference would need rewriting, which is fragile.

2. **No access to connection credentials.** DuckDB ATTACH needs a connection string with host/port/password, but vowl only receives a live Ibis connection object. There's no reliable way to extract credentials from it.

3. **Limited backend support.** DuckDB ATTACH only works with PostgreSQL, MySQL, and SQLite. vowl supports any Ibis backend, so materialisation is needed anyway for most of them.

4. **Filters can't be pushed down.** With materialisation, vowl applies filter conditions at the source before downloading. With ATTACH, the remote table is exposed raw and pushing per-adapter filters into cross-database joins would require complex query rewriting.

5. **ATTACH opens a separate connection.** This bypasses any session state on the user's Ibis connection (transactions, temp tables, session variables, `search_path`).

---

## Annotated Output: Not All Checks Can Be Merged

`get_annotated_output()` (and `save(output_mode="annotated")`) returns your **full table** with an extra `check_info` column showing which check(s) each row failed. However, not every check can be merged into this table; some checks simply don't produce results that map back to individual rows.

```python
output = result.get_annotated_output()
output["annotated"]   # {schema: full table + check_info}     <- mergeable checks
output["residues"]    # {"<schema>::<check>": failed rows + check_info + tables_in_query}  <- non-mergeable checks that still have offending rows
```

`residues` only holds non-mergeable checks that **still produce offending rows**. There are two such cases:

- column-subset checks, and
- cross-table checks whose failed rows carry columns from more than the anchor table (see case 1 below).

A non-mergeable check that produces no rows at all still appears in neither dict; its verdict is recorded only in `summary.json`. This covers scalar aggregations (`AVG`/`SUM`/`MIN`/`MAX`), `rowCount`, and errored checks.

The `check_info` column holds a JSON array of objects, one per failing check. Its shape follows the `check_info` preset (`"names"` default, `"summary"`, or `"full"`).

Residues are **per-check**. Each entry:

- covers exactly one non-mergeable check, keyed `"<schema>::<check_name>"`;
- carries that check's own failed rows; and
- uses the **same `check_info` column** as the annotated tables (here a single-element JSON array), plus `tables_in_query`.

Two non-mergeable checks are never combined into one entry, and a check that was merged into a full table never also appears as a residue. So annotated tables and residues are read exactly the same way. (The standalone `failed_rows`/`both` CSVs come from a separate, unchanged path and keep their legacy comma-joined `check_ids` column.)

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
3. **Its failed rows have the same columns as the full table.** If a check only selects a few columns, we can't match its results back to full rows. This condition also decides cross-table checks (see below): the merge depends only on the failed-rows column set, not on how many tables the query touches.

When a condition fails, the check is not merged onto the annotated table. What happens next depends on _why_ it failed to merge:

- **The check still has offending rows** (fails condition 3: column-subset checks, or a cross-table check whose rows carry both tables' columns). Those rows become a **residue**, returned separately and keyed `"<schema>::<check_name>"`.
- **The check has no offending rows to emit** (fails condition 2: a scalar aggregation like `AVG`/`SUM`/`MIN`/`MAX`, or an errored check). There is nothing to put in a residue, so the failure appears only in `summary.json` (status, `actual_value`, `expected_value`) and is never written to a CSV.

> **Heads-up: a failed scalar aggregation has no CSV footprint in `annotated` mode.**
> It tags no rows in the annotated table (its `check_info` stays `null`) and produces no
> residue file, so the only record of the failure is `summary.json`. Always consult the
> summary for the authoritative pass/fail verdict; the annotated CSVs alone do not surface
> scalar-aggregation or errored-check failures.

The common cases:

### 1. Cross-table checks: mergeable when the failed rows match the home schema

A cross-table check (one that JOINs a table against a reference table) **can** annotate onto its home schema's table. You just have to shape its failed-rows query so it projects **only that schema's columns**. As with any check, condition 3 alone decides whether it merges: what matters is the failed-rows column set, not how many tables the query touches.

To see why, note that every SQL check derives two queries from the single query you write:

- a **scalar query**: the `SELECT COUNT(*)` that decides pass/fail; and
- a lazy **failed-rows query**: a `SELECT *` over the same `FROM`, run only when the check fails and rows are requested.

The `COUNT(*)` → `SELECT *` rewrite only touches the **outer** select list. So if a subquery already projects just one table's columns, that projection still governs the shape of the failed rows.

**Mergeable (project only the anchor table's columns via a wrapping subquery):**

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

The failed-rows query rewrites to `SELECT * FROM (SELECT payroll.* …)`, which returns **only `demo_employee_payroll`'s columns**. Those rows match the anchor table exactly, so the orphan payroll rows are annotated directly into `demo_employee_payroll`'s `check_info` column, with no residue and no downstream mapping.

**Non-mergeable (a bare JOIN returns both tables' columns):**

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

Here the failed-rows query rewrites to a top-level `SELECT *` over the JOIN, which returns **both** tables' columns. That column set doesn't match `demo_employee_payroll`, so the check stays a **residue** keyed `"demo_employee_payroll::employee_id_exists_in_master_list"`, the same backward-compatible behaviour existing bare-JOIN checks already have.

> **The merge is decided by column structure, not intent.** A misshaped query that
> happens to return anchor-shaped rows _will_ merge. This is the same class of risk
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

A single scalar has no individual rows to flag, so it can't be annotated onto the full table. It also can't become a residue: a residue holds _offending rows_, and a scalar verdict has none. So a failed scalar aggregation produces **neither an annotated tag nor a residue file**. Unlike the cross-table and column-subset cases, its failure lives only in `summary.json`.

`rowCount` behaves the same way. Its query is a bare `SELECT COUNT(*) FROM t` with no failure predicate, so the count measures table size, not a number of failing rows. There is no per-row failure to annotate, so (like `AVG`/`MAX`/`SUM`) it fails condition 2, produces no residue, and reports its verdict in the summary only.

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

This tells us a town has an outlier, but the result only has 1 column. The full table has 10+ columns, so we can't match this partial result back to specific full rows, and it becomes a residue.

> **Auto-generated `unique`, `primaryKey`, and `duplicateValues` checks are mergeable.**
> Although these are implemented with `GROUP BY … HAVING COUNT(*) > 1` internally, vowl
> rewrites their failed-rows query to return the **full participating rows** (every row
> whose value belongs to a duplicate group, plus NULL primary keys) via an `IN`/`EXISTS`
> predicate against the base table. They therefore annotate directly onto the table rather
> than becoming residues. Their reported `failed_rows_count` counts participating _rows_
> (not duplicate _groups_), so it matches the number of annotated rows. The `percent`-unit
> variant of `duplicateValues` stays non-mergeable (its result is a ratio, not a row count).

### How grouping differs: consolidated output vs. annotated residues

`get_consolidated_output_dfs()` (used by `output_mode="failed_rows"`/`"both"`) **groups** failed rows by `(tables_in_query, column_set)`. Within each group it deduplicates identical rows and comma-joins the names of every check that flagged them. Cross-table failures are included too, keyed by a composite table name (e.g. `"table_a, table_b"`). This method is **deprecated** in favour of `get_annotated_output()` and will be removed in a future release.

By contrast, `get_annotated_output()`'s `residues` emit **one entry per non-mergeable check**, keyed `"<schema>::<check_name>"` and never grouped across checks. So the same non-mergeable failure looks different in each:

- **failed-rows CSVs:** grouped (possibly multi-check) rows with a comma-joined `check_ids` column.
- **annotated residues:** a single-check entry with a `check_info` JSON-array column.

If you rely solely on annotated output, always check `residues` for non-mergeable failures.

### Other things to know

- **A table can have both.** If a table has mergeable _and_ non-mergeable failing checks, you'll get both an annotated table and residue entries for that schema. Mergeable checks are never duplicated into `residues`.
- **Annotated entries exist even when nothing failed.** Every schema with an available adapter gets an annotated table; the `check_info` column is just all null.
- **Missing adapter?** If a schema's adapter is unavailable, that schema is skipped (with a warning) and its failures appear only as residues.
- **`max_failed_rows` raises an error for annotated output.** If you cap failed rows (`max_failed_rows >= 0`) and a mergeable check gets truncated, `get_annotated_output()` raises `ValueError` rather than silently treating un-fetched failures as passing. Use `max_failed_rows=-1` (the default) or switch to `output_mode="failed_rows"`.

- **Identical rows are all flagged.** Rows are matched by their values, so if two rows are identical and one fails a check, the other fails it too. This is correct behaviour, but it means the annotated table can show more flagged rows than the summary's failure count, which only counts unique failing rows.

---

## Dark Patterns

### Queries Accessing Tables Outside the Contract

A SQL check can name a table that isn't declared in your contract's `schema`. For example, this check sits under the `hdb_resale_prices` schema but reads `audit_log`, which the contract never mentions:

```yaml
quality:
  - type: sql
    name: "flagged_audit_entries"
    query: "SELECT COUNT(*) FROM audit_log WHERE flagged = 1"
    mustBe: 0
```

vowl reads an undeclared table through the adapter of the schema the check sits under, on that adapter's connection. If that connection can see `audit_log`, the check runs and passes or fails like any other. If it can't, the check comes back `ERROR` with the database's own "table not found" message. vowl reports the tables involved via `tables_in_query` but does **not** block undeclared table access.

**Why this matters:**

- The contract is no longer the single source of truth for what's being validated.
- Hidden dependencies on undeclared tables aren't obvious to contract reviewers.
- It may unintentionally expose data the contract author didn't intend to include.

**Joins follow the same rule.** A check under `hdb_resale_prices` that joins `hdb_resale_prices` to `audit_log` reads `audit_log` through the `hdb_resale_prices` adapter:

- If every table in the join is on that one connection, the query runs there directly and nothing is copied.
- If the join also reads a schema on another connection, vowl downloads `audit_log` from the `hdb_resale_prices` connection and joins the copies in a local DuckDB, as it does for [cross-table checks across connections](usage-patterns.md#multi-source-validation).
- Two checks under different schemas can both name `audit_log`. Each one reads the `audit_log` on its own schema's connection, so the same name can mean two different tables in one run.

**What happens in each setup.** The check sits under `hdb_resale_prices` and names `audit_log`:

| Setup                                                                  | Reads only `audit_log`         | Joins `audit_log` to `hdb_resale_prices`                                   | Joins `audit_log` to a schema on another connection                                      |
| ---------------------------------------------------------------------- | ------------------------------ | -------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `IbisAdapter` on one connection (`adapter=`)                           | Runs on that connection        | Runs on that connection                                                    | Not applicable, every schema shares the connection                                       |
| `adapters={...}`, `audit_log` on the `hdb_resale_prices` connection    | Runs on that connection        | Runs on that connection, nothing copied                                    | Both tables copied to a local DuckDB, `audit_log` from the `hdb_resale_prices` connection |
| `adapters={...}`, `audit_log` only on another schema's connection      | `ERROR`, table not found       | `ERROR`, table not found                                                   | `ERROR`, table not found                                                                 |
| `PooledAdapter` (`adapter=pooled` or in `adapters={...}`)              | Runs on a pooled connection    | Runs on a pooled connection when both schemas share the pool, nothing copied | Both tables copied to a local DuckDB, `audit_log` from the `hdb_resale_prices` pool ([joins across pools are copied](#pooledadapter-joins-across-pools-are-copied)) |
| DuckDB ATTACH, bare `audit_log` with no view                           | `ERROR`, table not found       | `ERROR`, table not found                                                   | Not applicable, every schema shares the connection                                       |
| DuckDB ATTACH, attached path (`pg_sales.audit_log`) or a view          | Runs on the DuckDB connection  | Runs on the DuckDB connection                                              | Not applicable, every schema shares the connection                                       |

"Table not found" is the database's own error for the connection the table was looked up on. Two more things to know:

- **Filter conditions.** A join that runs on one connection still applies each table's own filter conditions. `audit_log` gets the `hdb_resale_prices` adapter's conditions that match its name, and every declared schema gets its own adapter's.
- **Schema prefixes.** When a join is copied, vowl drops schema prefixes from table names, so a copied join that names `src1.audit_log` can't find it. Use the bare name in joins across connections.

!!! warning
    Treat SQL checks that reference undeclared tables as a code smell. Declare all referenced tables in your contract's `schema`, even if they're not the primary validation target.
