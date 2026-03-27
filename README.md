<div align="center">
  <img src="docs/img/vowl_logo.png" alt="vowl logo" width="400">
</div>

# Vowl

A validation engine for [Open Data Contract Standard (ODCS)](https://github.com/bitol-io/open-data-contract-standard) data contracts. Define your validation rules once in a declarative YAML contract and get rich, actionable reports on your data's quality.

## 🚀 Key Features

*   **Extensible Check Engine**: Ships with a SQL check engine out of the box, with the architecture designed to support custom check types beyond SQL.
*   **Auto-Generated Rules**: Checks are automatically derived from contract metadata (`logicalType`, `logicalTypeOptions`, `required`, `unique`, `primaryKey`) and library metrics (`nullValues`, `missingValues`, `invalidValues`, `duplicateValues`, `rowCount`).
*   **Any DataFrame, Any Backend**: Load any [Narwhals-compatible](https://github.com/narwhals-dev/narwhals) DataFrame type (pandas, Polars, PySpark, etc.) or connect to **20+ backends** via [Ibis](https://github.com/ibis-project/ibis). SQL dialect translation is handled by [SQLGlot](https://github.com/tobymao/sqlglot).
*   **Server-Side Execution**: SQL checks run server-side through Ibis without materialising tables on the client.
*   **Multi-Source Validation**: Validate across tables in different source systems with cross-database joins.
*   **Declarative ODCS Contracts**: Define validation rules in YAML following the [Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard).
*   **Flexible Filtering**: Filter conditions with wildcard pattern matching, ideal for incremental validation of new data.
*   **Rich Reporting**: Detailed summaries, row-level failure analysis, saveable reports, and a chainable `ValidationResult` API.

## 📦 Getting Started

Install from the GitLab Package Registry (keep PyPI as the primary index so public dependencies resolve normally):

### Installation
```bash
pip install vowl \
    --index-url https://pypi.org/simple \
    --extra-index-url https://sgts.gitlab-dedicated.com/api/v4/projects/64873/packages/pypi/simple
```

Optional extras are available: `vowl[spark]`, `vowl[all]`.
For local development, testing, and release workflow, see [CONTRIBUTING.md](CONTRIBUTING.md).

### Validate in 3 lines
```python
import pandas as pd # or any Narwhals-compatible Dataframe
from vowl import validate_data

df = pd.read_csv("data.csv")
result = validate_data("contract.yaml", df=df)
result.display_full_report()
```

<details>
<summary><strong>Output</strong> (click to expand)</summary>

```
=== Data Quality Validation Results ===
   Contract Version:      v3.1.0
   Contract ID:           c11443ee-542f-4442-b28d-2d224342be37
   Schemas:               hdb_resale_prices

 OVERALL DATA QUALITY
   Overall:
     Checks Pass Rate:       18 / 20 (90.0%)

   hdb_resale_prices:
     Overall:
       Checks Pass Rate:       18 / 20 (90.0%)
       ERRORED Checks:         0
     Single Table:
       Checks Pass Rate:       18 / 20 (90.0%)
       ERRORED Checks:         0
       Unique Passed Rows:     195 / 200 (97.5%)
     Multi Table:
       Checks Pass Rate:       0 / 0 (N/A)
       ERRORED Checks:         0
       Non-unique Failed Rows: 0


 CHECK RESULTS
+-----------------------------------------+---------------------------------------+-------------------+--------+---------------+---------------+--------+----------------+
| check_id                                | Target                                | tables_in_query   | status | operator      | expected      | actual | execution time |
+-----------------------------------------+---------------------------------------+-------------------+--------+---------------+---------------+--------+----------------+
| Month                                   | hdb_resale_prices.month               | hdb_resale_prices | FAILED | mustBe        | 0             | 2      | 13.01 ms       |
| Year                                    | hdb_resale_prices.lease_commence_date | hdb_resale_prices | FAILED | mustBe        | 0             | 3      | 9.86 ms        |
+-----------------------------------------+---------------------------------------+-------------------+--------+---------------+---------------+--------+----------------+
| AddressBlockHouseNumber                 | hdb_resale_prices.block               | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 11.21 ms       |
| block_column_exists_check               | hdb_resale_prices.block               | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 2.34 ms        |
| flat_model_column_exists_check          | hdb_resale_prices.flat_model          | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 2.31 ms        |
| flat_type_column_exists_check           | hdb_resale_prices.flat_type           | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 2.23 ms        |
| flat_type_invalidValues                 | hdb_resale_prices.flat_type           | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 11.33 ms       |
| floor_area_must_be_less_than_200        | hdb_resale_prices.floor_area_sqm      | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 9.74 ms        |
| floor_area_sqm_column_exists_check      | hdb_resale_prices.floor_area_sqm      | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 2.14 ms        |
| hdb_resale_prices_rowCount              | hdb_resale_prices                     | hdb_resale_prices | PASSED | mustBeBetween | [0, 30000000] | 200    | 4.78 ms        |
| lease_commence_date_column_exists_check | hdb_resale_prices.lease_commence_date | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 2.45 ms        |
| month_column_exists_check               | hdb_resale_prices.month               | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 3.79 ms        |
| month_logical_type_check                | hdb_resale_prices.month               | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 2.77 ms        |
| remaining_lease_column_exists_check     | hdb_resale_prices.remaining_lease     | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 2.28 ms        |
| resale_price_column_exists_check        | hdb_resale_prices.resale_price        | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 2.28 ms        |
| resale_price_must_not_exceed_2m         | hdb_resale_prices.resale_price        | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 9.91 ms        |
| storey_range_column_exists_check        | hdb_resale_prices.storey_range        | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 2.24 ms        |
| street_name_column_exists_check         | hdb_resale_prices.street_name         | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 3.72 ms        |
| town_column_exists_check                | hdb_resale_prices.town                | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 3.53 ms        |
| town_nullValues                         | hdb_resale_prices.town                | hdb_resale_prices | PASSED | mustBe        | 0             | 0      | 9.04 ms        |
+-----------------------------------------+---------------------------------------+-------------------+--------+---------------+---------------+--------+----------------+
Total Execution:       110.94 ms

=== Failed Checks and Rows (up to 5 row(s) per failed check) ===

  hdb_resale_prices
    Single checks

      [Month]
        Operator:   mustBe
        Expected:   0
        Actual:     2
        Target:   hdb_resale_prices.month
        Details:  Based on ISO 8601, assumed to be in UTC +8 | YYYY-MM
        Rule:     SELECT COUNT(*) FROM `hdb_resale_prices` WHERE NOT ((CAST(month AS CHAR) RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'))
        Rows shown: 2 of 2
+----------+--------+-----------+-------+--------------+--------------+----------------+---------------+---------------------+--------------------+--------------+
| month    | town   | flat_type | block | street_name  | storey_range | floor_area_sqm | flat_model    | lease_commence_date | remaining_lease    | resale_price |
+----------+--------+-----------+-------+--------------+--------------+----------------+---------------+---------------------+--------------------+--------------+
| 2017-jan | BEDOK  | 5 ROOM    | 21    | CHAI CHEE RD | 07 TO 09     | 130.0          | Adjoined flat | 1972                | 54 years 06 months | 530000.0     |
| 2017-jan | BISHAN | 3 ROOM    | 105   | BISHAN ST 12 | 04 TO 06     | 4.0            | Simplified    | 1985                | 67 years 11 months | 395000.0     |
+----------+--------+-----------+-------+--------------+--------------+----------------+---------------+---------------------+--------------------+--------------+

      [Year]
        Operator:   mustBe
        Expected:   0
        Actual:     3
        Target:   hdb_resale_prices.lease_commence_date
        Details:  Based on ISO 8601, assumed to be in UTC +8 | YYYY
        Rule:     SELECT COUNT(*) FROM `hdb_resale_prices` WHERE NOT ((CAST(lease_commence_date AS CHAR) RLIKE '^[0-9]{4}$'))
        Rows shown: 3 of 3
+---------+------------+-----------+-------+------------------+--------------+----------------+----------------+---------------------+--------------------+--------------+
| month   | town       | flat_type | block | street_name      | storey_range | floor_area_sqm | flat_model     | lease_commence_date | remaining_lease    | resale_price |
+---------+------------+-----------+-------+------------------+--------------+----------------+----------------+---------------------+--------------------+--------------+
| 2017-01 | ANG MO KIO | 3 ROOM    | 219   | ANG MO KIO AVE 1 | 07 TO 09     | 67.0           | New Generation | 1977.0              | 59 years 06 months | 297000.0     |
| 2017-01 | ANG MO KIO | 3 ROOM    | 211   | ANG MO KIO AVE 3 | 01 TO 03     | 67.0           | New Generation | abc                 | 59 years 03 months | 325000.0     |
| 2017-01 | ANG MO KIO | 3 ROOM    | 330   | ANG MO KIO AVE 1 | 07 TO 09     | 68.0           | New Generation | nan                 | 63 years           | 338000.0     |
+---------+------------+-----------+-------+------------------+--------------+----------------+----------------+---------------------+--------------------+--------------+
```

</details>

See [Usage Patterns](#️-usage-patterns) for PySpark, Ibis connections, multi-source validation, and more.

## 🎯 The Core Concept: The Data Contract

Instead of writing validation logic in Python, you declare it in a YAML file following the [Open Data Contract Standard (ODCS)](https://github.com/bitol-io/open-data-contract-standard). This separates your rules from your code, making them easier to manage, version, and share.

**Example `hdb_resale.yaml`:**
```yaml
kind: DataContract
apiVersion: v3.1.0
schema:
  - name: hdb_resale_prices # This becomes the table name in your SQL queries
    properties:
      # --- Column-Level Check ---
      - name: resale_price
        quality:
          - type: sql
            name: "resale_price_positive"
            query: "SELECT COUNT(*) FROM hdb_resale_prices WHERE resale_price <= 0"
            mustBe: 0

      - name: flat_type
        quality:
          # --- Library Metric Check (SQL auto-generated) ---
          - type: library
            metric: invalidValues
            mustBe: 0
            dimension: conformity
            arguments:
              validValues:
                - 3 ROOM
                - 4 ROOM
                - 5 ROOM
                - EXECUTIVE

    # --- Table-Level Check ---
    quality:
      - type: sql
        name: "no_null_resale_prices"
        query: "SELECT COUNT(*) FROM hdb_resale_prices WHERE resale_price IS NULL"
        mustBe: 0

      # --- Table-Level Library Metric ---
      - type: library
        metric: rowCount
        mustBeGreaterThan: 0
        dimension: completeness
```

### Automatic `check_references`

When a contract is loaded, `vowl` automatically builds `CheckReference` objects for every executable check in the contract via `Contract.get_check_references_by_schema()`.

This includes both user-authored checks in `quality` blocks and synthetic checks derived from column metadata. The generated references are grouped by schema, and the auto-generated ones run before explicit `quality` checks.

| Reference type | Trigger in contract | JSONPath stored in the reference |
|----------------|---------------------|----------------------------------|
| Table check | Entry under schema-level `quality` | `$.schema[N].quality[M]` |
| Column check | Entry under property-level `quality` | `$.schema[N].properties[M].quality[K]` |
| Library column metric | `type: library` under property-level `quality` | `$.schema[N].properties[M].quality[K]` |
| Library table metric | `type: library` under schema-level `quality` | `$.schema[N].quality[M]` |
| Declared column exists check | Property has a `name` | `$.schema[N].properties[M]` |
| Logical type check | `logicalType` present on a property | `$.schema[N].properties[M].logicalType` |
| Logical type options check | Supported key under `logicalTypeOptions` | `$.schema[N].properties[M].logicalTypeOptions.<optionKey>` |
| Required check | `required: true` | `$.schema[N].properties[M].required` |
| Unique check | `unique: true` | `$.schema[N].properties[M].unique` |
| Primary key check | `primaryKey: true` | `$.schema[N].properties[M].primaryKey` |

The auto-generated check types currently cover:

| Generated from | What `vowl` validates |
|----------------|------------------------|
| `name` | Column declared in the contract exists in the source table |
| `logicalType` | Values can be cast to the declared SQL type for `integer`, `number`, `boolean`, `date`, `timestamp`, and `time` |
| `logicalTypeOptions.minLength` | String length is at least the configured minimum |
| `logicalTypeOptions.maxLength` | String length does not exceed the configured maximum |
| `logicalTypeOptions.pattern` | String values match the configured regex pattern |
| `logicalTypeOptions.minimum` | Value is greater than or equal to the configured minimum |
| `logicalTypeOptions.maximum` | Value is less than or equal to the configured maximum |
| `logicalTypeOptions.exclusiveMinimum` | Value is strictly greater than the configured minimum |
| `logicalTypeOptions.exclusiveMaximum` | Value is strictly less than the configured maximum |
| `logicalTypeOptions.multipleOf` | Value is a multiple of the configured number |
| `required: true` | Column contains no `NULL` values |
| `unique: true` | Non-null values are unique |
| `primaryKey: true` | Values are both unique and non-null |

### Library Metrics (`type: library`)

Instead of writing SQL by hand, you can declare common data quality metrics using `type: library` in your `quality` blocks. `vowl` auto-generates the appropriate SQL at runtime.

**Column-level metrics** (under a property's `quality`):

| `metric` | What it checks | Arguments |
|----------|---------------|-----------|
| `nullValues` | Count of `NULL` values in the column | — |
| `missingValues` | Count of values matching a configurable missing-values list | `arguments.missingValues`: list of sentinel values (use `null` for SQL NULL) |
| `invalidValues` | Count of values that fail valid-value or pattern criteria | `arguments.validValues`: allowed values list and/or `arguments.pattern`: regex |
| `duplicateValues` | Count of duplicate non-NULL values in the column | — |

**Table-level metrics** (under a schema's `quality`):

| `metric` | What it checks | Arguments |
|----------|---------------|-----------|
| `rowCount` | Total number of rows in the table | — |
| `duplicateValues` | Count of duplicate rows across specified columns | `arguments.properties`: list of column names to check |

All library metrics support `unit: "percent"` to return the result as a percentage of total rows instead of an absolute count. They also accept any of the standard check operators (`mustBe`, `mustBeGreaterThan`, etc.).

**Example:**
```yaml
properties:
  - name: town
    quality:
      - type: library
        metric: nullValues
        mustBe: 0
        dimension: completeness

  - name: flat_type
    quality:
      - type: library
        metric: invalidValues
        mustBe: 0
        dimension: conformity
        arguments:
          validValues:
            - 3 ROOM
            - 4 ROOM
            - 5 ROOM
            - EXECUTIVE

quality:
  - type: library
    metric: rowCount
    mustBeGreaterThan: 0
    dimension: completeness

  - type: library
    metric: duplicateValues
    mustBe: 0
    dimension: uniqueness
    arguments:
      properties:
        - month
        - block
        - street_name
```

In practice, a property like this:

```yaml
- name: block
    logicalType: string
    logicalTypeOptions:
        maxLength: 10
    required: true
```

produces three generated check references pointing at:

```text
$.schema[0].properties[...]
$.schema[0].properties[...].logicalTypeOptions.maxLength
$.schema[0].properties[...].required
```

Because `string` does not currently generate a SQL cast-based type check, the `logicalType` entry above contributes metadata for option checks rather than a standalone type-validation query. If you use `integer`, `number`, `boolean`, `date`, `timestamp`, or `time`, `vowl` also generates a `logicalType` SQL check automatically. You only need to define extra `quality` entries when you want custom business rules beyond the contract metadata.

## 🔧 The `ValidationResult` Object: Your Toolkit

The `validate_data` function returns a powerful `ValidationResult` object that provides multiple ways to interact with your validation results.

### Core Methods

| Method/Property | What It Does | Returns |
|-----------------|--------------|---------|
| **`print_summary()`** | Prints high-level statistics (pass/fail counts, success rate, performance) | `self` (chainable) |
| **`show_failed_rows(max_rows=5)`** | Displays sample of failed rows in console | `self` (chainable) |
| **`display_full_report()`** | Prints summary + shows failed rows (convenience method) | `self` (chainable) |
| **`save(output_dir=".", prefix="vowl_results")`** | Saves enhanced CSV and summary JSON to disk | `self` (chainable) |
| **`get_output_dfs(checks=None)`** | Returns per-check failed rows as `{check_id: DataFrame}` | Dict[str, DataFrame] |
| **`get_consolidated_output_dfs(checks=None)`** | Deduplicates failed rows across checks, grouped by table | Dict[str, DataFrame] |
| **`.passed`** (property) | Boolean indicating if all checks passed | `True`/`False` |

---

## 💡 How It Works: Architecture

`vowl` has a modular architecture built around **Ibis** as the universal query layer.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              validate_data()                                │
│                           (Main Entry Point)                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DataSourceMapper                                  │
│              (Auto-detects input type → creates adapter)                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
          ┌──────────────────────────┼──────────────────────────┐
          ▼                          ▼                          ▼
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│   IbisAdapter    │      │ MultiSourceAdapter│      │  Custom Adapter  │
│                  │      │                  │      │                  │
│ • pandas/Polars  │      │ • Cross-database │      │ • Extend         │
│ • PySpark        │      │   validation     │      │   BaseAdapter    │
│ • PostgreSQL     │      │ • Data federation│      │                  │
│ • Snowflake      │      │                  │      │                  │
│ • BigQuery       │      │                  │      │                  │
│ • 20+ backends   │      │                  │      │                  │
└──────────────────┘      └──────────────────┘      └──────────────────┘
          │                          │                          │
          └──────────────────────────┼──────────────────────────┘
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Executors                                      │
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐     │
│  │  IbisSQLExecutor│  │MultiSourceSQLExecutor│  │  Custom Executor   │     │
│  │                 │  │                     │  │                     │     │
│  │ Runs SQL checks │  │ Cross-source SQL    │  │ Extend BaseExecutor │     │
│  │ via Ibis        │  │ via DuckDB          │  │ or SQLExecutor      │     │
│  │ (server-side)   │  │ (client-side)       │  │                     │     │
│  └─────────────────┘  └─────────────────────┘  └─────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ValidationResult                                  │
│                                                                             │
│  • Per-check failed rows with check_id & tables_in_query columns            │
│  • Detailed check results and metrics                                       │
│  • Export to CSV/JSON                                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | Description |
|-----------|-------------|
| **DataSourceMapper** | Auto-detects a single input source (DataFrame, Spark object, Ibis backend, or connection string) and creates the appropriate adapter |
| **IbisAdapter** | Universal adapter supporting 20+ backends via Ibis (pandas, Polars, PySpark, PostgreSQL, Snowflake, BigQuery, etc.) |
| **MultiSourceAdapter** | Enables validation across multiple data sources with data federation |
| **IbisSQLExecutor** | Executes SQL-based quality checks through the Ibis query layer |
| **Contract** | Parses ODCS YAML contracts into executable validation rules |
| **ValidationResult** | Rich result object with enhanced DataFrames, metrics, and export capabilities |

## ⚙️ Usage Patterns

> **Interactive demo:** Try the [usage patterns notebook](docs/demo/test_ibis_validation.ipynb) for a hands-on walkthrough of the examples below.

### Local DataFrame (Pandas/Polars)
```python
import pandas as pd
from vowl import validate_data

df = pd.read_csv("data.csv")
result = validate_data("contract.yaml", df=df)
result.display_full_report()
```

### PySpark
```python
from pyspark.sql import SparkSession
from vowl import validate_data

# Create SparkSession (user-managed)
spark = SparkSession.builder.appName("vowl").getOrCreate()

try:
    spark_df = spark.read.table("my_table")
    result = validate_data("contract.yaml", df=spark_df)
    result.display_full_report()
finally:
    # User is responsible for stopping the SparkSession
    spark.stop()
```

> **Note:** The library does **not** manage the SparkSession lifecycle. You must create and stop it yourself. This is by design - SparkSession is a heavy, application-owned resource with specific configuration requirements.

### Ibis Connections (20+ Backends)
```python
# Ibis supports: Amazon Athena, BigQuery, ClickHouse, Dask, Databricks, DataFusion, 
# Druid, DuckDB, Exasol, Flink, Impala, MSSQL, MySQL, Oracle, pandas, Polars, 
# PostgreSQL, PySpark, RisingWave, SingleStoreDB, Snowflake, SQLite, Trino, ...
# Find out more at https://github.com/ibis-project/ibis

import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

con = ibis.postgres.connect(...)  # Redshift can be supported via Postgres connections too

result = validate_data("contract.yaml", adapter=IbisAdapter(con))
result.display_full_report()
```

For MySQL, select the database when you create the connection, for example via
`ibis.mysql.connect(..., database="my_db")` or a connection URI that already
includes the database name. `vowl` does not issue `USE database` during
validation; it runs read-only `SELECT` queries against the active database on
the existing connection. If you need to avoid relying on the connection's
default database, use qualified table names such as `my_db.my_table` in your
contract queries.

### Explicit Adapter with Filter Conditions
```python
from vowl import validate_data
from vowl.adapters import IbisAdapter
from datetime import datetime, timedelta
import ibis

date_limit = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
con = ibis.postgres.connect()

# Using dict for filter conditions with wildcard patterns
# Wildcards use glob-style matching: * (any chars), ? (single char), [seq] (char in seq)
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
# Note: If multiple patterns match a table, conditions are combined with AND

# Multiple filter conditions on same table (combined with AND)
adapter = IbisAdapter(
    con,
    filter_conditions={
        "TableA": [
            {"field": "date_dt", "operator": ">=", "value": date_limit},
            {"field": "status", "operator": "=", "value": "active"},
        ]
    }
)

result = validate_data("contract.yaml", adapter=adapter)
result.display_full_report()
```

### Multi-Source Adapters
```python
from vowl import validate_data
from vowl.adapters import IbisAdapter
import ibis

con_a = ibis.postgres.connect()
con_b = ibis.sqlite.connect()

adapters = {
    "table_a": IbisAdapter(con_a),
    "table_b": IbisAdapter(con_b)
}

# Multi Adapter usage will cause data to be loaded into local DuckDB before quality 
# checks are evaluated. Ensure that local compute instance is capable of handling 
# all the loaded data.

result = validate_data("contract.yaml", adapters=adapters)
result.display_full_report()
```

### Custom Adapters and Executors

`BaseAdapter`, `BaseExecutor`, and `SQLExecutor` are intended as boilerplate extension points for teams building custom integrations. The typical pattern is to wrap an existing adapter, register custom executors, and then add backend-specific behavior incrementally.

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

This section documents the extension boilerplate rather than a guaranteed drop-in `validate_data(..., adapter=...)` path for arbitrary non-Ibis adapters. For end-to-end validation in the built-in runner today, the supported runtime adapter type is `IbisAdapter`.

### Using Servers Defined in Data Contract
```python
from vowl import validate_data
from vowl.contracts import Contract
from vowl.adapters import IbisAdapter
import ibis

# Load the contract and get server configuration
contract = Contract.load("contract.yaml")
server = contract.get_server("my-postgres-server")  # Match by server name
# Or: contract.get_server("uat")        — falls back to matching by environment
# Or: contract.get_server()             — returns the first server

# Create connection based on server config
con = ibis.postgres.connect(
    host=server["server"],
    port=server.get("port", 5432),
    database=server.get("database", ""),
)

# Create adapter and validate
adapter = IbisAdapter(con)
result = validate_data("contract.yaml", adapter=adapter)
result.display_full_report()
```

### Loading Contracts from Git (GitHub/GitLab)
```python
from vowl import validate_data

# GitHub - blob URL (auto-converted to raw)
result = validate_data(
    "https://github.com/org/repo/blob/main/contracts/my_contract.yaml",
    df=df
)
result.display_full_report()

# GitHub - raw URL
result = validate_data(
    "https://raw.githubusercontent.com/org/repo/main/contracts/my_contract.yaml",
    df=df
)
result.display_full_report()

# GitLab - blob URL (auto-converted to raw)
result = validate_data(
    "https://gitlab.com/org/repo/-/blob/main/contracts/my_contract.yaml",
    df=df
)
result.display_full_report()

# Note: `requests` is included in base install.
```

### Loading Contracts from S3
```python
from vowl import validate_data

# S3 URI format
result = validate_data("s3://my-bucket/contracts/my_contract.yaml", df=df)
result.display_full_report()

# Note: `boto3` is included in base install.
# Uses default AWS credentials (environment variables, ~/.aws/credentials, IAM role, etc.)
```

## 📋 Capabilities Roadmap

### Completed

| Capability | Description |
|------------|-------------|
| ✅ **Ibis Connectors** | Interoperability with 20+ data sources via Ibis (PostgreSQL, Snowflake, BigQuery, Databricks, etc.) |
| ✅ **Remote Contract Loading** | Load contracts from S3 (`s3://`) and Git (GitHub/GitLab URLs) |
| ✅ **JSONPath Navigation** | Navigate contract elements using JSONPath expressions (`contract.resolve("$.schema[0].name")`) |
| ✅ **Static Checks** | Auto-generated checks from contract elements: `logicalType`, `logicalTypeOptions`, `required`, `unique`, `primaryKey` |
| ✅ **Library Metrics** | Declare common data quality metrics (`nullValues`, `missingValues`, `invalidValues`, `duplicateValues`, `rowCount`) with `type: library` — SQL auto-generated at runtime |
| ✅ **ODCS Schema Validation** | Contracts validated against ODCS JSON Schema before execution |
| ✅ **Filter Conditions** | Incremental quality testing with wildcard pattern matching - optimised for append-only data sources |
| ✅ **Multi-Schema Checks** | Cross-table referential checks within a single contract |
| ✅ **Multi-Connection Checks** | Cross-table referential checks between different servers/databases via `MultiSourceAdapter` |
| ✅ **Optional Extras** | Add optional Spark support with `.[spark]` or install `.[all]` |
| ✅ **Custom Adapters & Executors** | Extensible architecture - create custom adapters and executors by extending `BaseAdapter`, `BaseExecutor`, or `SQLExecutor` |

### Planned

| Capability | Description | Status |
|------------|-------------|--------|
| 🔄 **PyPI/Nexus Distribution** | Currently available on the GitLab Package Registry. Publishing to PyPI/Nexus for easier installation is in progress. | In Progress |
| 📅 **Alternative Check Engines** | Support for dqx, dbt, Soda, Great Expectations (subject to licensing review) | Planned |

---

## 📄 License

This project is licensed under the GovTech Public Sector Licence. Usage and modification of the source code are subject to the workflows defined in [CONTRIBUTING.md](CONTRIBUTING.md).

