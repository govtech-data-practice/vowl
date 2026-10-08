---
description: Connect vowl to your data and load contracts. Local DataFrames, PySpark, 20+ databases through Ibis, filter conditions, concurrent checks, multi-source runs, custom adapters, and contracts from Git or S3.
---

# Connecting to Your Data

!!! tip "Interactive Demo"

    Try the [example notebooks](https://github.com/govtech-data-practice/vowl/tree/main/examples) for a hands-on walkthrough. Start with the [Basic Tutorial](https://github.com/govtech-data-practice/vowl/blob/main/examples/1_basic_tutorial/basic_tutorial.ipynb).

vowl reads your data through an **adapter**, an object that knows how to run
queries on one data source. Most of the time you do not build one yourself:

| Your data is in                             | Pass to `validate_data`                  | See                                                   |
| ------------------------------------------- | ---------------------------------------- | ----------------------------------------------------- |
| A pandas or Polars DataFrame                | `df=df`                                  | [Local DataFrames](#local-dataframe-pandaspolars)     |
| A Spark DataFrame                           | `df=spark_df`                            | [PySpark](#pyspark)                                   |
| A database                                  | `adapter=IbisAdapter(con)`               | [Ibis connections](#ibis-connections-20-backends)     |
| A database, with many checks to run at once | `adapter=PooledAdapter(...)`             | [Concurrent checks](#concurrent-checks-pooledadapter) |
| Several databases                           | `adapters={"schema_name": adapter, ...}` | [Multi-source validation](#multi-source-validation)   |

## Local DataFrame (Pandas/Polars)

```python
import pandas as pd
from vowl import validate_data

df = pd.read_csv("data.csv")
result = validate_data("contract.yaml", df=df)
result.display_full_report()
```

Any DataFrame that [Narwhals](https://narwhals-dev.github.io/narwhals/)
supports works the same way. vowl loads it into an in-memory DuckDB and runs
the checks there.

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

    vowl does **not** start or stop the SparkSession. You create and stop it
    yourself, because it is a heavy resource your application owns and
    configures.

## Ibis Connections (20+ Backends)

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

con = ibis.postgres.connect(...)

result = validate_data("contract.yaml", adapter=IbisAdapter(con))
result.display_full_report()
```

Checks run inside the database. Only counts and failed rows come back.

Ibis supports Amazon Athena, BigQuery, ClickHouse, Databricks, DataFusion,
Druid, DuckDB, Exasol, Flink, Impala, MSSQL, MySQL, Oracle, pandas, Polars,
PostgreSQL, PySpark, RisingWave, SingleStoreDB, Snowflake, SQLite, Trino, and
more. See [ibis-project/ibis](https://github.com/ibis-project/ibis). Some
databases handle nulls, regex or arrays differently. See
[Known issues](known-issues.md#database-backend-differences).

!!! info "MySQL"

    Select the database when you create the connection, for example
    `ibis.mysql.connect(..., database="my_db")`, or use a connection URI that
    includes the database name. vowl does not run `USE database`. It runs
    read-only `SELECT` queries on the connection's current database.

### Using servers defined in the contract

An ODCS contract can list the servers its data lives on. `get_server` returns
one of them as a `dict`, so you can connect without repeating the details:

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter
from vowl.contracts import Contract

contract = Contract.load("contract.yaml")
server = contract.get_server("my-postgres-server")  # matches the server's `server` field
# contract.get_server("uat")                        # or its `environment`
# contract.get_server()                             # or the first server

con = ibis.postgres.connect(
    host=server["host"],
    port=server.get("port", 5432),
    database=server.get("database", ""),
)

result = validate_data(contract, adapter=IbisAdapter(con))
```

`get_server` looks for a server whose `server` field matches, then for one
whose `environment` matches. It raises a `ValueError` when the contract has no
servers or none match. `contract.get_servers()` returns them all.

## Filter Conditions

Filter conditions limit which rows vowl checks, for example only the last
seven days. Pass them to the adapter. Keys are table names, and `*` matches
any characters.

```python
from datetime import datetime, timedelta

import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

date_limit = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
con = ibis.postgres.connect(...)

adapter = IbisAdapter(
    con,
    filter_conditions={
        # One table
        "TableA": {"field": "date_dt", "operator": ">=", "value": date_limit},
        # employees, emp_history, emp_details, ...
        "emp*": {"field": "date_dt", "operator": ">=", "value": date_limit},
        # orders_archive, customers_archive, ...
        "*_archive": {"field": "is_deleted", "operator": "=", "value": False},
        # Every table
        "*": {"field": "tenant_id", "operator": "=", "value": 123},
    },
)

result = validate_data("contract.yaml", adapter=adapter)
```

When several keys match a table, all their conditions apply (they are joined
with AND). To give one table several conditions, pass a list:

```python
adapter = IbisAdapter(
    con,
    filter_conditions={
        "TableA": [
            {"field": "date_dt", "operator": ">=", "value": date_limit},
            {"field": "status", "operator": "=", "value": "active"},
        ]
    },
)
```

## Concurrent Checks (`PooledAdapter`)

When a contract has many checks and the database can serve several queries at
once, run the checks side by side with a `PooledAdapter`. You give it a
_factory_, a function that returns a new adapter with its own connection. The
pool calls it once per connection it opens. The results are the same as a run
one check at a time.

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter, PooledAdapter

def make_adapter():
    con = ibis.duckdb.connect("my_db.duckdb")
    return IbisAdapter(con)

pooled = PooledAdapter(factory=make_adapter, max_concurrency=4)

result = validate_data("contract.yaml", adapter=pooled)
# or adapters={"orders": pooled, ...} to use the pool for some schemas only
```

- `max_concurrency` (default 4) is the most checks running at once, and so the
  most connections open, for the whole run. Schemas given the same pool share
  that limit.
- A cross-table check between tables on one pool runs in the database. A check
  that joins two pools, or a pool and another adapter, copies the tables into
  memory first. See
  [PooledAdapter](design-considerations/cross-table/how-it-works.md#pooledadapter).
- The pool keeps its connections after the run, so you can reuse it. Call
  `pooled.cleanup()` when you are done. It drops the pooled adapters but does
  not close Ibis connections, so close those yourself if they need it.

!!! warning "SQLite"

    A SQLite connection opened by `ibis.connect("sqlite://...")` cannot be
    shared between threads. See
    [SQLite: Parallel Checks Need a Thread-Safe Connection](known-issues.md#sqlite-parallel-checks-need-a-thread-safe-connection).

## Multi-Source Validation

Use this when one contract covers tables in different databases, and some
checks read more than one of them. The usual case is a foreign key: every
`transactions.user_id` must exist in `users.id`, but `transactions` is in
PostgreSQL and `users` is in SQLite. Neither database can run that join alone,
so something has to bring the two tables together.

There are two ways. They differ in where the checks run and how your rows
reach them.

|                               | Option A: DuckDB ATTACH                              | Option B: Multi-source adapters                               |
| ----------------------------- | ---------------------------------------------------- | ------------------------------------------------------------- |
| Where single-table checks run | In DuckDB on your machine, reading the remote table  | Inside each table's own database                              |
| Where cross-table checks run  | In DuckDB on your machine, reading the remote tables | In DuckDB on your machine, on copies of the tables            |
| How rows reach your machine   | Read while each check runs, then discarded           | Copied into memory in full before the check, kept for the run |
| Supported sources             | PostgreSQL, MySQL, SQLite                            | Any Ibis backend                                              |
| Who connects the tables       | You, with `ATTACH` and views                         | vowl, from one adapter per schema                             |

Option A suits large tables, because it never copies a whole table up front,
but it only works with PostgreSQL, MySQL and SQLite. Option B works with any
backend, such as Snowflake, BigQuery, Databricks, Oracle or MSSQL, and keeps
single-table checks inside each database. But every table a cross-table check
reads is copied into memory whole.

### Option A: DuckDB ATTACH

You open one DuckDB connection and attach each remote database to it. DuckDB
can read from all of them in one query, so vowl sees one connection that holds
every table.

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
   a short name. Its tables are then reachable as `pg_sales.transactions`,
   `sqlite_users.users` and so on.
2. `USE memory` switches back to DuckDB's own in-memory database, so the views
   in the next step are created there and not in a remote database.
3. `CREATE VIEW` gives each attached table the name your contract uses.
   Contract queries say `transactions`, not `pg_sales.transactions`, so
   without the views vowl would not find the tables.
4. `validate_data` runs every check through this one DuckDB connection.

!!! note "Read, not stored"

    A view is a saved query, not a copy. Each time a check runs, DuckDB reads
    the rows it needs from the remote database and discards them when the
    check finishes. For PostgreSQL and MySQL, DuckDB passes simple column
    filters to the source, so only matching rows travel. Joins and counts
    such as `COUNT(*)` run inside DuckDB, so the rows they read still cross
    the network. A table used by five checks is read five times.

!!! tip "Also useful for one database"

    ATTACH helps with a single database too, when that database lacks a SQL
    feature a check needs (for example regex on MSSQL), or when you want the
    same engine to run the checks whatever the source. Attach the database,
    run `USE` on it, and pass the DuckDB connection to `IbisAdapter`:

    ```python
    con = ibis.duckdb.connect()
    con.raw_sql("ATTACH 'postgresql://user:pass@host:5432/mydb' AS pg (TYPE postgres, READ_ONLY)")  # trufflehog:ignore
    con.raw_sql("USE pg")
    result = validate_data("contract.yaml", adapter=IbisAdapter(con))
    ```

### Option B: Multi-Source Adapters

You give vowl one adapter per schema, and vowl decides where each check runs.

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

pg_con = ibis.postgres.connect(...)
sqlite_con = ibis.sqlite.connect(...)

adapters = {
    "transactions": IbisAdapter(pg_con),
    "users": IbisAdapter(sqlite_con),
}

result = validate_data("contract.yaml", adapters=adapters)
result.display_full_report()
```

The keys must match the schema `name`s in your contract. Single-table checks,
and cross-table checks whose tables share one connection, run inside the
database. A cross-table check across connections runs in DuckDB on your
machine, on copies of the tables it reads. The
[filter conditions](#filter-conditions) apply at the source, so only matching
rows are copied. [How Cross-Server and Cross-Table Checks Work](design-considerations/cross-table/how-it-works.md)
explains where each check runs, including `PooledAdapter` and tables the
contract does not declare.

!!! warning "Watch the data volume"

    A table read by a cross-table check across connections is copied in full,
    minus any rows your filter conditions leave out. Large tables can use a lot
    of memory and network. Add filter conditions to limit what is copied, or
    use Option A if your sources support it. See
    [Copying tables into memory](design-considerations/cross-table/how-it-works.md#copying-tables-into-memory).

## Custom Adapters and Executors

`BaseAdapter`, `BaseExecutor` and `SQLExecutor` are extension points for teams
building their own integrations. An adapter connects to a data source. An
executor runs one kind of check on it, keyed by the check's `engine`.

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

`validate_data` accepts any `BaseAdapter` through `adapter=` or `adapters=`.
For cross-table checks, a custom adapter can implement three more methods:

- `is_compatible_with(other)` returns `True` when two adapters can run one
  query together, so the check runs in the database. The default is `False`.
- `export_table_as_arrow(schema_name)` returns a table as a PyArrow table, so vowl can copy it
  into memory when the check cannot run in the database.
- `with_filter_conditions(filter_conditions)` returns a copy of the adapter with other
  filter conditions. vowl needs it to run a join in the database when the
  joined tables have different filter conditions. Without it, vowl copies the
  tables.

## Loading contracts

`validate_data` takes the contract as its first argument. It can be a local
path, a URL, an S3 URI, or a `Contract` you loaded yourself. vowl checks the
contract against the ODCS schema for its `apiVersion` when it loads it.

```python
from vowl import validate_data
from vowl.contracts import Contract

result = validate_data("contracts/orders.yaml", df=df)   # a local file

contract = Contract.load("contracts/orders.yaml")        # or load it first
result = validate_data(contract, df=df)
```

To connect with the servers a contract lists, see
[Using servers defined in the contract](#using-servers-defined-in-the-contract).

### From Git (GitHub or GitLab)

Pass the file's URL. A GitHub or GitLab `blob` URL, the one in your browser's
address bar, is turned into its raw URL for you.

```python
from vowl import validate_data

# GitHub blob URL
result = validate_data(
    "https://github.com/org/repo/blob/main/contracts/my_contract.yaml",
    df=df,
)

# GitHub raw URL
result = validate_data(
    "https://raw.githubusercontent.com/org/repo/main/contracts/my_contract.yaml",
    df=df,
)

# GitLab blob URL
result = validate_data(
    "https://gitlab.com/org/repo/-/blob/main/contracts/my_contract.yaml",
    df=df,
)
```

vowl only fetches contracts over `http` or `https` from public addresses. A
URL whose host resolves to a private, internal or loopback address (such as
`localhost`, `10.x.x.x`, or a cloud metadata address) is refused with a
`ContractURLError`. This stops a contract link from being used to reach
machines inside your network. For a contract on an internal Git server,
download the file and pass its local path.

### From S3

```python
from vowl import validate_data

result = validate_data("s3://my-bucket/contracts/my_contract.yaml", df=df)
```

!!! note

    Loading from S3 needs `boto3`, which the base install leaves out. Install
    it with `pip install 'vowl[all]'` or `pip install boto3`. vowl uses your
    default AWS credentials: environment variables, `~/.aws/credentials`, or an
    IAM role. For an S3-compatible store such as MinIO, set
    `AWS_ENDPOINT_URL` to its address.

To save results, including to
[cloud storage](results.md#saving-results), see
[The Results Object](results.md).
