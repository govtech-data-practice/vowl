---
description: >-
  vowl roadmap — completed features and upcoming capabilities for the
  ODCS data contract validation engine.
---

# Roadmap

## Completed

| Capability                      | Description                                                                                                                                                             |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Ibis Connectors**             | Interoperability with 20+ data sources via Ibis (PostgreSQL, Snowflake, BigQuery, Databricks, etc.)                                                                     |
| **Remote Contract Loading**     | Load contracts from S3 (`s3://`) and Git (GitHub/GitLab URLs)                                                                                                           |
| **Remote Result Saving**        | Save results straight to S3, Google Cloud, Azure, or HDFS with `result.save("s3://...")`                                                                                |
| **JSONPath Navigation**         | Navigate contract elements using JSONPath expressions (`contract.resolve("$.schema[0].name")`)                                                                          |
| **Generated Checks**            | Checks built from contract fields: `logicalType`, `logicalTypeOptions`, `required`, `unique`, `primaryKey`                                                              |
| **Library Checks**              | Declare common checks (`nullValues`, `missingValues`, `invalidValues`, `duplicateValues`, `rowCount`) with `type: library`. vowl writes the SQL for you               |
| **ODCS Schema Validation**      | Contracts validated against ODCS JSON Schema before execution                                                                                                           |
| **Filter Conditions**           | Check only new or chosen rows, with wildcard table names. Suits tables that only grow                                                                                   |
| **Cross-Table Checks**          | Checks that compare tables within a single contract, such as foreign keys                                                                                               |
| **Multi-Source Checks**         | Cross-table checks between tables in different databases, with `adapters={...}`                                                                                         |
| **Optional Extras**             | Add Spark with `.[spark]`, OpenTelemetry with `.[otel]`, or everything with `.[all]`                                                                                    |
| **Custom Adapters & Executors** | Extensible architecture. Create custom adapters and executors by extending `BaseAdapter`, `BaseExecutor`, or `SQLExecutor`                                              |
| **Parallel Check Execution**    | Run checks side by side on large contracts with `PooledAdapter`                                                                                                         |
| **OpenTelemetry Export**        | Export validation metrics, traces, and logs via OpenTelemetry (OTLP), behind the optional `[otel]` extra                                                                |

## Planned

| Capability                    | Description                                                               | Status  |
| ----------------------------- | ------------------------------------------------------------------------- | ------- |
| **Alternative Check Engines** | Support for dqx, Soda, Great Expectations (subject to licensing review)   | Planned |
| **CLI Interface**             | Command-line interface for running validations directly from the terminal | Planned |
| **vowl-ui**                   | Web-based validation interface for vowl                                   | Planned |
