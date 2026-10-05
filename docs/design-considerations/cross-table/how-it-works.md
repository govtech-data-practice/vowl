---
title: How Cross-Server and Cross-Table Checks Work
description: >-
  Where vowl runs a check that reads more than one table, when it copies
  tables into memory, how it reads tables outside the contract, and how the
  failed rows of such a check are annotated.
---

# How Cross-Server and Cross-Table Checks Work

A **cross-table check** reads more than one table, such as a foreign key
("every `transactions.user_id` exists in `users.id`"). When the tables are on
different data sources, no single database can run the check alone, so vowl
has to bring the tables together.

This page explains where vowl runs each check, what it copies, and how it
annotates the failed rows. To set up a run across data sources, see
[Multi-Source Validation](../../usage-patterns.md#multi-source-validation).

## Where each check runs

There are two ways to connect tables on different data sources. They differ
in who brings the tables together.

### Option A: DuckDB ATTACH

You attach each remote database to one DuckDB connection, and give each
attached table the name your contract uses with a view. vowl sees one
connection that holds every table, and runs every check there, as it would on
a single database.

DuckDB reads the rows each check needs from the remote databases while the
check runs, and discards them afterwards. Nothing is copied up front. This
only works with the databases DuckDB can attach: PostgreSQL, MySQL and SQLite.

### Option B: Multi-source adapters

You give vowl one adapter per schema, with `adapters={...}`, and vowl decides
where each check runs:

| Check                                         | Where it runs                                                                                                            |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Reads one table                               | Inside that table's own database. Nothing is copied.                                                                     |
| Reads several tables, all on one connection   | Inside that database. Nothing is copied.                                                                                 |
| Reads several tables on different connections | In DuckDB on your machine, on copies of the tables.                                                                      |
| Reads a table the contract does not declare   | Through the adapter of the schema the check belongs to. See [Tables outside the contract](#tables-outside-the-contract). |

"One connection" means the same connection object. vowl can't tell that two
connections reach the same database, so it treats each as its own data source.
Tables on one `PooledAdapter` count as one connection.

## Copying tables into memory

When a check reads tables on different connections, vowl copies each table
it reads into an in-memory DuckDB on your machine and runs the check there:

- **Filter conditions apply at the source.** Only the rows your
  [filter conditions](../../usage-patterns.md#filter-conditions) keep are
  copied.
- **Each table is copied once per run**, however many checks use it. The
  copies are dropped when the run ends.
- **The cost is memory and network.** The copied rows travel to your machine
  and stay in memory for the run, so a large table can run out of memory.

To copy less, add filter conditions, or use Option A if your databases
support it.

### PooledAdapter

A cross-table check whose tables are all on one `PooledAdapter` runs in the
database. This includes `adapter=pooled` on a contract with several schemas.
vowl takes one of the pool's connections for the check, applies each table's
filter conditions, and runs the query there.

vowl copies the tables into memory when they come from:

- **two different pools**, even if both open the same database,
- **a pool and another adapter**, such as an `IbisAdapter`, even on the same
  database.

To keep a join in the database, pass one pool for every schema it reads.

Separate pools also don't share work. Schemas on the same pool run their
single-table checks side by side, up to `max_concurrency`. Schemas on
different pools, or on a pool and another adapter, run one schema at a time,
and each pool runs its own schema's checks in parallel.

### Why vowl copies instead of using ATTACH

vowl copies tables (as Arrow tables) instead of attaching the databases
itself, for these reasons:

1. **Table names don't line up.** ATTACH puts tables under a longer name,
   such as `pg_db.public.my_table`, while contract queries use `my_table`.
   Every table name in a join would have to be rewritten.
2. **vowl has no connection details.** ATTACH needs a host, port and
   password, but vowl only gets a live connection object, which doesn't
   reliably expose them.
3. **Few databases support it.** ATTACH works with PostgreSQL, MySQL and
   SQLite. vowl supports any Ibis backend, so it would still copy tables for
   most of them.
4. **Filter conditions would be hard to apply.** When copying, vowl applies
   each adapter's filter conditions in the source database. With ATTACH, each
   adapter's conditions would have to be written into the join.
5. **ATTACH opens a separate connection.** It would skip any session state on
   your connection, such as transactions, temporary tables, session variables
   or `search_path`.

## Tables outside the contract

A SQL check can name a table that isn't declared in the contract's `schema`.
This check sits under the `orders` schema but reads `currencies`, a lookup
table the contract never declares:

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

vowl reads an undeclared table through the adapter of the schema the check
belongs to. If that connection can see `currencies`, the check runs as usual.
If it can't, the check ends in `ERROR` with the database's own "table not
found" message. vowl lists the tables a check read in `tables_in_query`, but
does not block undeclared tables.

Joins follow the same rule:

- If every table in the join is on the `orders` connection, the query runs
  there and nothing is copied.
- If the join also reads a schema on another connection, vowl copies
  `currencies` from the `orders` connection into memory, with the other
  tables.
- Two checks under different schemas can both name `currencies`. Each reads
  the `currencies` on its own schema's connection, so one name can mean two
  different tables in one run.

What happens in each setup, for this check under `orders`:

| Setup                                                              | Reads only `currencies`       | Joins `currencies` to `orders`                               | Joins `currencies` to a schema on another connection                      |
| ------------------------------------------------------------------ | ----------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------- |
| `IbisAdapter` on one connection (`adapter=`)                       | Runs on that connection       | Runs on that connection                                      | Not applicable, every schema shares the connection                        |
| `adapters={...}`, `currencies` on the `orders` connection          | Runs on that connection       | Runs on that connection, nothing copied                      | Both tables copied into memory, `currencies` from the `orders` connection |
| `adapters={...}`, `currencies` only on another schema's connection | `ERROR`, table not found      | `ERROR`, table not found                                     | `ERROR`, table not found                                                  |
| `PooledAdapter` (`adapter=pooled` or in `adapters={...}`)          | Runs on a pooled connection   | Runs on a pooled connection when both schemas share the pool | Both tables copied into memory, `currencies` from the `orders` pool       |
| DuckDB ATTACH, bare `currencies` with no view                      | `ERROR`, table not found      | `ERROR`, table not found                                     | Not applicable, every schema shares the connection                        |
| DuckDB ATTACH, attached name (`pg_sales.currencies`) or a view     | Runs on the DuckDB connection | Runs on the DuckDB connection                                | Not applicable, every schema shares the connection                        |

Two more things to know:

- **Filter conditions.** A join that runs on one connection still applies
  each table's own filter conditions. `currencies` gets the `orders`
  adapter's conditions that match its name, and every declared schema gets
  its own adapter's.
- **Schema prefixes.** When a join is copied, vowl drops schema prefixes from
  table names, so a copied join that names `src1.currencies` can't find it.
  Use the bare name in joins across connections.

!!! warning "Declare every table a check reads"

    A check that reads undeclared tables makes the contract an incomplete
    record of what is validated, hides the dependency from reviewers, and can
    read data the contract author did not mean to include. Declare every table
    a check reads in the contract's `schema`, even if it is not the main table
    you are checking.

## Annotating the failed rows of a cross-table check

A cross-table check annotates rows on the [annotated table](../failed-rows/annotating-the-source-table.md)
like any other check, as long as its failed rows hold only the columns of the
schema the check belongs to. vowl goes by the columns, not by what the query
means.

The [failed rows query](../failed-rows/how-failed-rows-are-derived.md#two-queries-from-one)
swaps the outer `SELECT COUNT(*)` for `SELECT *`, so the outer `FROM` decides
which columns the failed rows have. For "every payroll row has an employee in
the master list":

| How the query is written                               | Columns of the failed rows               | Annotated on payroll?       |
| ------------------------------------------------------ | ---------------------------------------- | --------------------------- |
| Plain `SELECT COUNT(*) FROM payroll LEFT JOIN ref ...` | Both tables' columns (`ref.*` all empty) | No, the columns don't match |
| A subquery with `SELECT payroll.*`                     | Payroll columns only                     | Yes                         |
| `WHERE NOT EXISTS (SELECT 1 FROM ref ...)`             | Payroll columns only                     | Yes                         |

This check annotates rows. The subquery returns only payroll columns, so each
failed row matches a row of `demo_employee_payroll`. `payroll.*` picks which
table's columns to return, and keeps their names as they are:

```yaml
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

`NOT EXISTS` needs no subquery, because the other table appears only inside
the `WHERE`:

```sql
SELECT COUNT(*)
FROM demo_employee_payroll payroll
WHERE NOT EXISTS (
  SELECT 1 FROM demo_employee_list ref
  WHERE ref.employee_id = payroll.employee_id
)
```

This one becomes a residue. A plain join returns the columns of both tables,
so its failed rows don't match either table:

```yaml
quality:
  - type: sql
    name: employee_id_exists_in_master_list
    query: >-
      SELECT COUNT(*) FROM demo_employee_payroll p
      LEFT JOIN demo_employee_list e ON p.employee_id = e.employee_id
      WHERE e.employee_id IS NULL
    mustBe: 0
```

Its failed rows are kept under
`"demo_employee_payroll::employee_id_exists_in_master_list"`. The check is
still counted, but it is not attributed, so it adds nothing to the row counts
of `demo_employee_payroll` and is counted in `checks_not_attributed`.

The foreign-key checks vowl generates from `relationships` are already
written the first way, so they annotate rows on the referencing table. A check is
only ever matched against its own schema's table. A query that returns rows
with the right columns for the wrong reason still annotates them.
