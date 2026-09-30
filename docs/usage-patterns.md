---
description: Usage patterns for vowl — local DataFrames, PySpark, Ibis connections, multi-source validation, custom adapters, and loading contracts from Git or S3.
---

# Connecting to Your Data

!!! tip "Interactive Demo"
    Try the [example notebooks](https://github.com/govtech-data-practice/vowl/tree/main/examples) for a hands-on walkthrough of the examples below — start with the [Basic Tutorial](https://github.com/govtech-data-practice/vowl/blob/main/examples/1_basic_tutorial/basic_tutorial.ipynb).

## Local DataFrame (Pandas/Polars)

```python
import pandas as pd
from vowl import validate_data

df = pd.read_csv("data.csv")
result = validate_data("contract.yaml", df=df)
result.display_full_report()
```

## PySpark

```python
from pyspark.sql import SparkSession
from vowl import validate_data

spark = SparkSession.builder.appName("vowl").getOrCreate()

try:
    spark_df = spark.read.table("my_table")
    result = validate_data("contract.yaml", df=spark_df)
    result.display_full_report()
finally:
    spark.stop()
```

!!! note
    The library does **not** manage the SparkSession lifecycle. You must create and stop it yourself. This is by design. SparkSession is a heavy, application-owned resource with specific configuration requirements.

## Ibis Connections (20+ Backends)

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

con = ibis.postgres.connect(...)

result = validate_data("contract.yaml", adapter=IbisAdapter(con))
result.display_full_report()
```

Ibis supports: Amazon Athena, BigQuery, ClickHouse, Dask, Databricks, DataFusion, Druid, DuckDB, Exasol, Flink, Impala, MSSQL, MySQL, Oracle, pandas, Polars, PostgreSQL, PySpark, RisingWave, SingleStoreDB, Snowflake, SQLite, Trino, and more. See [ibis-project/ibis](https://github.com/ibis-project/ibis).

!!! info "MySQL"
    Select the database when you create the connection, for example via `ibis.mysql.connect(..., database="my_db")` or a connection URI that already includes the database name. vowl does not issue `USE database` during validation; it runs read-only `SELECT` queries against the active database on the existing connection.

## Compatibility Mode (DuckDB ATTACH)

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

con = ibis.duckdb.connect()
con.raw_sql("ATTACH 'postgresql://user:pass@host:5432/mydb' AS pg (TYPE postgres, READ_ONLY)")  # trufflehog:ignore
con.raw_sql("USE pg")

result = validate_data("contract.yaml", adapter=IbisAdapter(con))
result.display_full_report()
```

!!! tip "When to use this"
    Your remote backend doesn't support a SQL feature that a check needs, or you want a single local engine for reproducible results regardless of the source database. DuckDB ATTACH supports PostgreSQL, MySQL, and SQLite.

## Explicit Adapter with Filter Conditions

```python
from vowl import validate_data
from vowl.adapters import IbisAdapter
from datetime import datetime, timedelta
import ibis

date_limit = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
con = ibis.postgres.connect(...)

adapter = IbisAdapter(
    con,
    filter_conditions={
        # Exact match
        "TableA": {
            "field": "date_dt",
            "operator": ">=",
            "value": date_limit
        },
        # Wildcard: matches employees, emp_history, emp_details, etc.
        "emp*": {
            "field": "date_dt",
            "operator": ">=",
            "value": date_limit
        },
        # Wildcard: matches orders_archive, customers_archive, etc.
        "*_archive": {
            "field": "is_deleted",
            "operator": "=",
            "value": False
        },
        # Apply to ALL tables
        "*": {
            "field": "tenant_id",
            "operator": "=",
            "value": 123
        },
    }
)

result = validate_data("contract.yaml", adapter=adapter)
result.display_full_report()
```

!!! note
    If multiple patterns match a table, conditions are combined with AND.

### Multiple Filter Conditions on Same Table

```python
adapter = IbisAdapter(
    con,
    filter_conditions={
        "TableA": [
            {"field": "date_dt", "operator": ">=", "value": date_limit},
            {"field": "status", "operator": "=", "value": "active"},
        ]
    }
)
```

## Multi-Source Validation

Use this when one contract covers tables that live in different databases, and
some checks need to read more than one of them at once. The usual case is a
foreign key: every `transactions.user_id` must exist in `users.id`, but
`transactions` is in PostgreSQL and `users` is in SQLite. Neither database can
run that join alone, so something has to bring the two tables together.

vowl gives you two ways to do that. They differ in where the checks run and
how your rows travel to them.

|                                | Option A: DuckDB ATTACH                                      | Option B: Multi-source adapters                                       |
| ------------------------------ | ------------------------------------------------------------ | --------------------------------------------------------------------- |
| Where single-table checks run  | In DuckDB on your machine, reading the remote table          | Inside each table's own database                                      |
| Where cross-table checks run   | In DuckDB on your machine, reading the remote tables         | In a local DuckDB, on downloaded copies of the tables                 |
| How rows reach your machine    | Streamed while each check runs, then discarded               | Downloaded in full before the check, then held in memory for the run  |
| Supported sources              | PostgreSQL, MySQL, SQLite                                    | Any Ibis backend                                                      |
| Who wires up the tables        | You, with `ATTACH` and views                                 | vowl, from one adapter per contract table                             |

Neither option keeps all the work inside your databases. Option A doesn't
download whole tables up front, so it suits large tables, but it only works
with PostgreSQL, MySQL and SQLite. Option B works with any backend, such as
Snowflake, BigQuery, Databricks, Oracle or MSSQL, and keeps single-table checks
inside each database, but every table a cross-table check reads is downloaded
whole.

### Option A: DuckDB ATTACH

You open one DuckDB connection and attach each remote database to it. DuckDB
can read from all of them in a single query, so vowl sees one connection that
holds every table.

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

con = ibis.duckdb.connect()

con.raw_sql("ATTACH 'postgresql://user:pass@host:5432/salesdb' AS pg_sales (TYPE postgres, READ_ONLY)")  # trufflehog:ignore
con.raw_sql("ATTACH 'sqlite:///path/to/users.db' AS sqlite_users (TYPE sqlite, READ_ONLY)")

con.raw_sql("USE memory")

con.raw_sql("CREATE VIEW transactions AS SELECT * FROM pg_sales.transactions")
con.raw_sql("CREATE VIEW users AS SELECT * FROM sqlite_users.users")

result = validate_data("contract.yaml", adapter=IbisAdapter(con))
result.display_full_report()
```

What each step does:

1. `ATTACH ... READ_ONLY` makes a remote database visible inside DuckDB under
   an alias. Its tables are then reachable as `pg_sales.transactions`,
   `sqlite_users.users` and so on.
2. `USE memory` switches back to DuckDB's own in-memory database, so the views
   in the next step are created there and not in a remote database.
3. `CREATE VIEW` gives each attached table the bare name your contract uses.
   Contract queries say `transactions`, not `pg_sales.transactions`, so without
   the views vowl would not find the tables.
4. `validate_data` runs every check, single-table and cross-table, through this
   one DuckDB connection.

!!! note "Streamed, not stored"
    A view is a saved query, not a copy. Each time a check runs, DuckDB reads
    the rows it needs from the remote database and discards them once the
    check finishes. For PostgreSQL and MySQL, DuckDB sends simple column
    filters to the source, so only matching rows travel. Joins and aggregates such as `COUNT(*)` run inside
    DuckDB, so the rows they read still cross the network. A table used by
    five checks is read five times.

### Option B: Multi-Source Adapters

You give vowl one adapter per contract table, and vowl decides where each check
runs.

```python
from vowl import validate_data
from vowl.adapters import IbisAdapter
import ibis

pg_con = ibis.postgres.connect(...)
sqlite_con = ibis.sqlite.connect(...)

adapters = {
    "transactions": IbisAdapter(pg_con),
    "users": IbisAdapter(sqlite_con),
}

result = validate_data("contract.yaml", adapters=adapters)
result.display_full_report()
```

The dictionary keys must match the schema `name`s in your contract. vowl then
routes each check like this:

- **Single-table checks** (for example `required`, `unique`, or a SQL check that
  only reads `users`) run on that table's own adapter, inside its own database.
  Nothing is copied.
- **Cross-table checks on one connection** run directly in that database. This
  applies when every table the check reads shares the same connection object.
  Each table keeps the [filter conditions](#explicit-adapter-with-filter-conditions)
  of the adapter that reads it, so the result matches a run on local copies.
  Tables served by one `PooledAdapter` count as one connection. The check runs
  on one of the pool's connections. A join across two pools, or between a pool
  and another adapter, is copied. See
  [PooledAdapter: Joins Across Pools Are Copied](known-issues.md#pooledadapter-joins-across-pools-are-copied).
- **Cross-table checks across connections** need a local copy. vowl downloads
  each table the check reads, applying that adapter's
  [filter conditions](#explicit-adapter-with-filter-conditions) at the source.
  It loads the copies into a local in-memory DuckDB and runs the check there.
  Each table is downloaded once per run, however many checks use it, and the
  copies are dropped when the run ends.
- **Tables the contract doesn't declare** (a lookup table such as `audit_log`)
  are read through the adapter of the schema the check sits under, in both
  single-table checks and joins. See
  [Queries Accessing Tables Outside the Contract](known-issues.md#queries-accessing-tables-outside-the-contract).

!!! warning "Watch the data volume"
    A table read by a cross-table check is downloaded in full, minus any rows
    your filter conditions exclude. Large tables can use a lot of memory and
    network. Add filter conditions to limit what is pulled, or use Option A if
    your sources support it. See
    [Known Issues & Caveats](known-issues.md#multi-source-adapters-data-materialisation)
    for more, including why vowl doesn't use `ATTACH` internally.

## Custom Adapters and Executors

`BaseAdapter`, `BaseExecutor`, and `SQLExecutor` are intended as extension points for teams building custom integrations.

```python
from typing import Optional

import ibis

from vowl.adapters import BaseAdapter, IbisAdapter
from vowl.executors import BaseExecutor, SQLExecutor


class CustomAdapter(BaseAdapter):
    def __init__(self, con, **kwargs):
        super().__init__(executors={
            "sql": CustomSQLExecutor,
            "xxx": CustomEngineExecutor,
        })
        self._wrapped = IbisAdapter(con, **kwargs)

    def get_connection(self):
        return self._wrapped.get_connection()

    @property
    def filter_conditions(self):
        return self._wrapped.filter_conditions

    def test_connection(self, table_name: str) -> Optional[str]:
        return self._wrapped.test_connection(table_name)


class CustomEngineExecutor(BaseExecutor):
    ...


class CustomSQLExecutor(SQLExecutor):
    ...


con = ibis.duckdb.connect()
adapter = CustomAdapter(con)

executors = adapter.get_executors()
assert "sql" in executors
```

!!! info
    `validate_data` accepts any `BaseAdapter` through `adapter=` or `adapters=`, including `IbisAdapter`, `PooledAdapter` and your own subclasses. A custom adapter runs its checks through the executors it registers. A cross-table check runs in the database only if `is_compatible_with` says the adapters can share a query. The default returns `False`, so vowl copies the tables to a local DuckDB, which needs `export_table_as_arrow`. When the tables in such a join have different filter conditions, the adapter also needs `with_filter_conditions`, or vowl copies the tables.

## Using Servers Defined in Data Contract

```python
from vowl import validate_data
from vowl.contracts import Contract
from vowl.adapters import IbisAdapter
import ibis

contract = Contract.load("contract.yaml")
server = contract.get_server("my-postgres-server")  # Match by server name
# Or: contract.get_server("uat")        # falls back to matching by environment
# Or: contract.get_server()             # returns the first server

con = ibis.postgres.connect(
    host=server["server"],
    port=server.get("port", 5432),
    database=server.get("database", ""),
)

adapter = IbisAdapter(con)
result = validate_data("contract.yaml", adapter=adapter)
result.display_full_report()
```

## Loading Contracts from Git (GitHub/GitLab)

```python
from vowl import validate_data

# GitHub - blob URL (auto-converted to raw)
result = validate_data(
    "https://github.com/org/repo/blob/main/contracts/my_contract.yaml",
    df=df
)

# GitHub - raw URL
result = validate_data(
    "https://raw.githubusercontent.com/org/repo/main/contracts/my_contract.yaml",
    df=df
)

# GitLab - blob URL (auto-converted to raw)
result = validate_data(
    "https://gitlab.com/org/repo/-/blob/main/contracts/my_contract.yaml",
    df=df
)
```

## Loading Contracts from S3

```python
from vowl import validate_data

result = validate_data("s3://my-bucket/contracts/my_contract.yaml", df=df)
result.display_full_report()
```

!!! note
    `boto3` is not included in the base install. Install it with `pip install vowl[all]` or `pip install boto3`. Uses default AWS credentials (environment variables, `~/.aws/credentials`, IAM role, etc.).
