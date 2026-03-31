# Usage Patterns

!!! tip "Interactive Demo"
    Try the [usage patterns notebook](https://github.com/govtech-data-practice/Vowl/blob/main/examples/vowl_usage_patterns_demo.ipynb) for a hands-on walkthrough of the examples below.

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
    The library does **not** manage the SparkSession lifecycle. You must create and stop it yourself. This is by design — SparkSession is a heavy, application-owned resource with specific configuration requirements.

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
    Select the database when you create the connection, for example via `ibis.mysql.connect(..., database="my_db")` or a connection URI that already includes the database name. Vowl does not issue `USE database` during validation; it runs read-only `SELECT` queries against the active database on the existing connection.

## Compatibility Mode (DuckDB ATTACH)

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

con = ibis.duckdb.connect()
con.raw_sql("ATTACH 'postgresql://user:pass@host:5432/mydb' AS pg (TYPE postgres, READ_ONLY)")
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

There are two ways to validate across tables in different databases.

### Option A: DuckDB ATTACH

Streams data, no materialisation:

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

con = ibis.duckdb.connect()

con.raw_sql("ATTACH 'postgresql://user:pass@host:5432/salesdb' AS pg_sales (TYPE postgres, READ_ONLY)")
con.raw_sql("ATTACH 'sqlite:///path/to/users.db' AS sqlite_users (TYPE sqlite, READ_ONLY)")

con.raw_sql("USE memory")

con.raw_sql("CREATE VIEW transactions AS SELECT * FROM pg_sales.transactions")
con.raw_sql("CREATE VIEW users AS SELECT * FROM sqlite_users.users")

result = validate_data("contract.yaml", adapter=IbisAdapter(con))
result.display_full_report()
```

!!! note
    DuckDB evaluates views dynamically at query time — this does **not** materialise or copy data. It streams live from your attached databases.

### Option B: Multi-Source Adapters

Materialises data locally:

```python
from vowl import validate_data
from vowl.adapters import IbisAdapter
import ibis

con_a = ibis.postgres.connect(...)
con_b = ibis.sqlite.connect(...)

adapters = {
    "table_a": IbisAdapter(con_a),
    "table_b": IbisAdapter(con_b)
}

result = validate_data("contract.yaml", adapters=adapters)
result.display_full_report()
```

!!! warning
    Multi-source adapters **materialise** each table into a local DuckDB instance before running checks. Ensure your local machine can handle the data volume.

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
    For end-to-end validation in the built-in runner today, the supported runtime adapter type is `IbisAdapter`.

## Using Servers Defined in Data Contract

```python
from vowl import validate_data
from vowl.contracts import Contract
from vowl.adapters import IbisAdapter
import ibis

contract = Contract.load("contract.yaml")
server = contract.get_server("my-postgres-server")  # Match by server name
# Or: contract.get_server("uat")        — falls back to matching by environment
# Or: contract.get_server()             — returns the first server

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
