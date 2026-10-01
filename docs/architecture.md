---
description: How vowl works inside. Adapters connect to your data, executors run the checks, and Ibis lets the SQL run inside your database.
---

# How vowl Works

vowl is built from a few parts. [Ibis](https://github.com/ibis-project/ibis)
lets the same SQL run on 20+ databases, so most checks run inside your
database.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              validate_data()                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DataSourceMapper                                  │
│         Turns df=, connection_str= or spark_session= into an adapter        │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MultiSourceAdapter                                 │
│   Built by vowl for every run. Holds one adapter per schema and decides     │
│   where each check runs.                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
          ┌──────────────────────────┼──────────────────────────┐
          ▼                          ▼                          ▼
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│   IbisAdapter    │      │  PooledAdapter   │      │  Custom Adapter  │
│                  │      │                  │      │                  │
│ • pandas/Polars  │      │ • Several        │      │ • Extend         │
│ • PySpark        │      │   IbisAdapters   │      │   BaseAdapter    │
│ • PostgreSQL     │      │   from a factory │      │                  │
│ • Snowflake      │      │ • Runs checks    │      │                  │
│ • 20+ backends   │      │   side by side   │      │                  │
└──────────────────┘      └──────────────────┘      └──────────────────┘
          │                          │                          │
          └──────────────────────────┼──────────────────────────┘
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Executors                                      │
│                                                                             │
│  ┌─────────────────┐  ┌──────────────────────┐  ┌─────────────────────┐     │
│  │ IbisSQLExecutor │  │MultiSourceSQLExecutor│  │  Custom Executor    │     │
│  │                 │  │                      │  │                     │     │
│  │ Runs a SQL      │  │ Cross-table checks:  │  │ Extend BaseExecutor │     │
│  │ check inside    │  │ in the database when │  │ or SQLExecutor      │     │
│  │ the database    │  │ it can, otherwise on │  │                     │     │
│  │                 │  │ copies in DuckDB     │  │                     │     │
│  └─────────────────┘  └──────────────────────┘  └─────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ValidationResult                                  │
│                                                                             │
│  • Check results, row counts and failed rows                                │
│  • Annotated tables and residues                                            │
│  • Files, DQ metrics and OpenTelemetry export                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Components

| Component                  | What it does                                                                                                                                                                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **DataSourceMapper**       | Looks at what you passed (a DataFrame, a Spark object, an Ibis connection or a connection string) and creates the right adapter                                                                                                             |
| **MultiSourceAdapter**     | vowl builds one for every run from `adapter=` or `adapters={...}`. You don't create it yourself. It sends single-table checks to each schema's adapter, and cross-table checks to `MultiSourceSQLExecutor`                                  |
| **IbisAdapter**            | Connects to any of the 20+ Ibis backends (pandas, Polars, PySpark, PostgreSQL, Snowflake, BigQuery and more)                                                                                                                                |
| **PooledAdapter**          | Opens several adapters from a factory function you give it, and runs up to `max_concurrency` checks at once. See [Concurrent checks](usage-patterns.md#concurrent-checks-pooledadapter)                                                     |
| **IbisSQLExecutor**        | Runs a SQL check inside the database through Ibis                                                                                                                                                                                           |
| **MultiSourceSQLExecutor** | Runs a cross-table check. When every table it reads is on one connection, the check runs in that database. Otherwise vowl copies each table into memory (as an Arrow table), loads it into DuckDB on your machine, and runs the check there |
| **Contract**               | Reads an ODCS YAML contract and turns it into checks                                                                                                                                                                                        |
| **ValidationResult**       | Holds the outcome of the run. See [The Results Object](results.md)                                                                                                                                                                             |
