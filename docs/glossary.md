---
description: The words the vowl docs use, and what each one means. Schemas, checks, failed rows, row counts, annotated output, adapters and more.
---

# Glossary

The vowl docs use each word below for one thing only. When a page says
"schema" it means the same thing on every page.

## Contracts and checks

| Term                  | Meaning                                                                                                                                                                                                                            |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Contract**          | A YAML file that follows the [Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard). It describes your tables and the checks to run on them. See [Data Quality with Data Contracts](contracts.md). |
| **Schema**            | One entry under `schema` in a contract. It describes one table and has a `name`. Results, adapters and failed rows are all grouped by schema name.                                                                                 |
| **Table**             | The actual table in your database or DataFrame that a schema describes.                                                                                                                                                            |
| **Check**             | One test, such as "price must be positive". Each check has a name, and the check's name is how you find it in every result.                                                                                                        |
| **Generated check**   | A check vowl builds for you from column details such as `logicalType`, `required` or `unique`. See [Auto-generated Checks](contracts.md#auto-generated-checks).                                                                    |
| **Library check**     | A common check you declare with `type: library`, such as `nullValues` or `rowCount`. vowl writes its SQL. See [Library Checks](contracts.md#library-checks).                                                                       |
| **SQL check**         | A check you write yourself with `type: sql` and a `query`.                                                                                                                                                                         |
| **Dimension**         | The group a check belongs to, set with `dimension:`, such as `completeness`, `conformity` or `uniqueness`.                                                                                                                         |
| **Cross-table check** | A check that reads more than one table, such as a foreign key.                                                                                                                                                                     |

## Connecting to data

| Term                   | Meaning                                                                                                                                                                                 |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Data source**        | Where a table lives: a DataFrame, a Spark session or a database.                                                                                                                        |
| **Adapter**            | The object vowl uses to run queries on one data source, such as `IbisAdapter`. `validate_data` makes one for you when you pass `df=`. See [Connecting to Your Data](usage-patterns.md). |
| **Multi-source run**   | A run where schemas are on different data sources, set up with `adapters={"schema_name": adapter, ...}`.                                                                                |
| **Copied into memory** | What vowl does with the tables of a cross-table check that spans data sources. It copies each table to your machine and runs the check in DuckDB there.                                 |
| **Filter conditions**  | Rules that limit which rows vowl checks, such as only the last seven days. See [Filter Conditions](usage-patterns.md#filter-conditions).                                                |

## Results

| Term                   | Meaning                                                                                                                                                                                                                                                                   |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Status**             | Each check ends as `PASSED`, `FAILED` or `ERROR`. `ERROR` means the check could not run, for example because of a typo in its SQL.                                                                                                                                        |
| **Check pass rate**    | Passed checks divided by all checks. Shown as **Checks Pass Rate** in the summary.                                                                                                                                                                                        |
| **Failed rows**        | The rows a check caught.                                                                                                                                                                                                                                                  |
| **Counted check**      | A check whose failed rows count toward the row counts. Checks that return one number, such as an average, are not counted. See [Which Checks Contribute Failed Rows](design-considerations/failed-rows/which-checks.md).                                                  |
| **Row counts**         | How many rows of a table failed at least one counted check, and how many passed them all. A row that fails two checks counts once. From `get_row_quality_df()`.                                                                                                           |
| **Row pass rate**      | Passed rows divided by all rows. Shown as **Passed Rows** in the summary.                                                                                                                                                                                                 |
| **Tolerated rows**     | Failed rows of a check that still passed, because its threshold allows some, such as `mustBeLessThan: 10`.                                                                                                                                                                |
| **Exact**              | Whether a row count is exact. It is `False` when the number could be off, for example because a counted check ended in `ERROR`.                                                                                                                                           |
| **Route**              | How vowl counted a check's failed rows: `server_predicate` (the data source counts), `server_lookup` (the data source matches rows), `client_lookup` (vowl matches the rows onto the downloaded table) or `client_returned_rows` (vowl fetches the rows and counts them). |
| **Row count accuracy** | How exactly the row counts count checks that are not plain row filters: `"accurate"` (default), `"balanced"` or `"fast"`. See [Row count accuracy](run-settings.md#row-count-accuracy).                                                                                   |
| **Annotate**           | To write a check's name into a row's `check_info` column.                                                                                                                                                                                                                 |
| **Annotated output**   | Your full tables with a `check_info` column, plus the residues. From `get_annotated_output()`.                                                                                                                                                                            |
| **Residue**            | The failed rows of one check that vowl could not annotate its table with, because they have different columns. Stored under `"<schema>::<check_name>"`.                                                                                                                   |
| **Summary**            | The report `print_summary()` prints, also saved as `summary.json`.                                                                                                                                                                                                        |
| **DQ metrics**         | The counts and pass rates at check, dimension, schema and run level, for dashboards. See [Understanding DQ Metrics](dq-metrics/understanding-metrics.md).                                                                                                                 |
| **Run ID**             | The ID of one `validate_data` call. It ties the metrics, traces and files of one run together.                                                                                                                                                                            |
