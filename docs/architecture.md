# Architecture

Vowl has a modular architecture built around **Ibis** as the universal query layer.

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
│  │ Runs SQL checks │  │ Mode 1: delegate to │  │ Extend BaseExecutor │     │
│  │ via Ibis        │  │ backend (same conn) │  │ or SQLExecutor      │     │
│  │ (server-side)   │  │ Mode 2: materialise │  │                     │     │
│  │                 │  │ to DuckDB via Arrow │  │                     │     │
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

## Key Components

| Component | Description |
|-----------|-------------|
| **DataSourceMapper** | Auto-detects a single input source (DataFrame, Spark object, Ibis backend, or connection string) and creates the appropriate adapter |
| **IbisAdapter** | Universal adapter supporting 20+ backends via Ibis (pandas, Polars, PySpark, PostgreSQL, Snowflake, BigQuery, etc.) |
| **MultiSourceAdapter** | Routes checks across multiple data sources, separating single-table checks (delegated to per-schema adapters) from multi-table checks (sent to `MultiSourceSQLExecutor`) |
| **IbisSQLExecutor** | Executes SQL-based quality checks through the Ibis query layer (server-side) |
| **MultiSourceSQLExecutor** | Executes cross-source SQL with two modes: **direct delegation** when all tables share the same compatible backend, or **DuckDB materialisation** when backends differ. Tables are exported as Arrow and loaded into a local DuckDB for cross-database joins |
| **Contract** | Parses ODCS YAML contracts into executable validation rules |
| **ValidationResult** | Rich result object with enhanced DataFrames, metrics, and export capabilities |
