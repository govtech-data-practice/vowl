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

## Key Components

| Component | Description |
|-----------|-------------|
| **DataSourceMapper** | Auto-detects a single input source (DataFrame, Spark object, Ibis backend, or connection string) and creates the appropriate adapter |
| **IbisAdapter** | Universal adapter supporting 20+ backends via Ibis (pandas, Polars, PySpark, PostgreSQL, Snowflake, BigQuery, etc.) |
| **MultiSourceAdapter** | Enables validation across multiple data sources with data federation |
| **IbisSQLExecutor** | Executes SQL-based quality checks through the Ibis query layer |
| **Contract** | Parses ODCS YAML contracts into executable validation rules |
| **ValidationResult** | Rich result object with enhanced DataFrames, metrics, and export capabilities |
