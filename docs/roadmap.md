# Roadmap

## Completed

| Capability | Description |
|------------|-------------|
| **Ibis Connectors** | Interoperability with 20+ data sources via Ibis (PostgreSQL, Snowflake, BigQuery, Databricks, etc.) |
| **Remote Contract Loading** | Load contracts from S3 (`s3://`) and Git (GitHub/GitLab URLs) |
| **JSONPath Navigation** | Navigate contract elements using JSONPath expressions (`contract.resolve("$.schema[0].name")`) |
| **Static Checks** | Auto-generated checks from contract elements: `logicalType`, `logicalTypeOptions`, `required`, `unique`, `primaryKey` |
| **Library Metrics** | Declare common data quality metrics (`nullValues`, `missingValues`, `invalidValues`, `duplicateValues`, `rowCount`) with `type: library` — SQL auto-generated at runtime |
| **ODCS Schema Validation** | Contracts validated against ODCS JSON Schema before execution |
| **Filter Conditions** | Incremental quality testing with wildcard pattern matching — optimised for append-only data sources |
| **Multi-Schema Checks** | Cross-table referential checks within a single contract |
| **Multi-Connection Checks** | Cross-table referential checks between different servers/databases via `MultiSourceAdapter` |
| **Optional Extras** | Add optional Spark support with `.[spark]` or install `.[all]` |
| **Custom Adapters & Executors** | Extensible architecture — create custom adapters and executors by extending `BaseAdapter`, `BaseExecutor`, or `SQLExecutor` |

## Planned

| Capability | Description | Status |
|------------|-------------|--------|
| **PyPI Distribution** | Published to PyPI. Nexus distribution support is in progress. | In Progress |
| **Alternative Check Engines** | Support for dqx, dbt, Soda, Great Expectations (subject to licensing review) | Planned |
