---
description: Install vowl and validate your first dataset in three lines of Python. Supports pandas, Polars, PySpark, and 20+ Ibis backends.
---

# Quick Start

## Installation

```bash
pip install vowl
```

Optional extras:

| Extra                 | What it adds                                            |
| --------------------- | ------------------------------------------------------- |
| `vowl[spark]`         | PySpark support, including Spark Connect                |
| `vowl[spark-classic]` | PySpark 3.0 to 3.3, without Spark Connect               |
| `vowl[otel]`          | [Exporting to OpenTelemetry](dq-metrics/otel-export.md) |
| `vowl[all]`           | Everything: Spark, AWS (`boto3`) and OpenTelemetry      |

For local development, testing, and release workflow, see [CONTRIBUTING.md](https://github.com/govtech-data-practice/vowl/blob/main/CONTRIBUTING.md).

## Validate in 3 Lines

```python
import pandas as pd  # or any Narwhals-compatible DataFrame
from vowl import validate_data

df = pd.read_csv("data.csv")
result = validate_data("contract.yaml", df=df)
result.display_full_report()
```

??? example "Sample Output (click to expand)"

    ```
    === Data Quality Validation Results ===
       Contract Version:      v3.2.0
       Contract ID:           c11443ee-542f-4442-b28d-2d224342be37
       Schemas:               hdb_resale_prices

     OVERALL DATA QUALITY
       Overall:
         Checks Pass Rate:          7 / 9 (77.7%)

       hdb_resale_prices:
         Overall:
           Checks Pass Rate:          7 / 9 (77.7%)
           ERRORED Checks:            0
           Failed Rows (approximate): 14
         Single Table:
           Checks Pass Rate:          7 / 9 (77.7%)
           ERRORED Checks:            0
         Multi Table:
           Checks Pass Rate:          0 / 0 (N/A)
           ERRORED Checks:            0
           Non-unique Failed Rows:    0


     CHECK RESULTS
    +------------------------------------+----------------------------------+-------------------+--------+----------+----------+--------+----------------+
    | check_id                           | Target                           | tables_in_query   | status | operator | expected | actual | execution time |
    +------------------------------------+----------------------------------+-------------------+--------+----------+----------+--------+----------------+
    | Month                              | hdb_resale_prices.month          | hdb_resale_prices | FAILED | mustBe   | 0        | 2      | 18.60 ms       |
    | floor_area_must_be_less_than_200   | hdb_resale_prices.floor_area_sqm | hdb_resale_prices | FAILED | mustBe   | 0        | 12     | 15.28 ms       |
    +------------------------------------+----------------------------------+-------------------+--------+----------+----------+--------+----------------+
    | flat_type_column_exists_check      | hdb_resale_prices.flat_type      | hdb_resale_prices | PASSED | mustBe   | 0        | 0      | 4.44 ms        |
    | flat_type_enum_check               | hdb_resale_prices.flat_type      | hdb_resale_prices | PASSED | mustBe   | 0        | 0      | 8.24 ms        |
    | floor_area_sqm_column_exists_check | hdb_resale_prices.floor_area_sqm | hdb_resale_prices | PASSED | mustBe   | 0        | 0      | 4.43 ms        |
    | month_column_exists_check          | hdb_resale_prices.month          | hdb_resale_prices | PASSED | mustBe   | 0        | 0      | 4.81 ms        |
    | month_logical_type_check           | hdb_resale_prices.month          | hdb_resale_prices | PASSED | mustBe   | 0        | 0      | 10.04 ms       |
    | resale_price_column_exists_check   | hdb_resale_prices.resale_price   | hdb_resale_prices | PASSED | mustBe   | 0        | 0      | 4.32 ms        |
    | resale_price_must_not_exceed_2m    | hdb_resale_prices.resale_price   | hdb_resale_prices | PASSED | mustBe   | 0        | 0      | 14.53 ms       |
    +------------------------------------+----------------------------------+-------------------+--------+----------+----------+--------+----------------+
    Total Execution:       84.69 ms

    === Failed Checks and Rows (up to 5 row(s) per failed check) ===

      hdb_resale_prices
        Single checks

          [Month]
            Operator:   mustBe
            Expected:   0
            Actual:     2
            Target:   hdb_resale_prices.month
            Details:  Based on ISO 8601 | YYYY-MM
            Rule:     SELECT COUNT(*) FROM "hdb_resale_prices" WHERE NOT REGEXP_MATCHES(TRY_CAST(month AS TEXT), '^[0-9]{4}-(0[1-9]|1[0-2])$')
            Rows shown: 2 of 2
    +----------+--------+-----------+-------+--------------+--------------+----------------+---------------+---------------------+--------------------+--------------+
    | month    | town   | flat_type | block | street_name  | storey_range | floor_area_sqm | flat_model    | lease_commence_date | remaining_lease    | resale_price |
    +----------+--------+-----------+-------+--------------+--------------+----------------+---------------+---------------------+--------------------+--------------+
    | 2017-jan | BEDOK  | 5 ROOM    | 21    | CHAI CHEE RD | 07 TO 09     | 130.0          | Adjoined flat | 1972                | 54 years 06 months | 530000.0     |
    | 2017-jan | BISHAN | 3 ROOM    | 105   | BISHAN ST 12 | 04 TO 06     | 4.0            | Simplified    | 1985                | 67 years 11 months | 395000.0     |
    +----------+--------+-----------+-------+--------------+--------------+----------------+---------------+---------------------+--------------------+--------------+

          [floor_area_must_be_less_than_200]
            Operator:   mustBe
            Expected:   0
            Actual:     12
            Target:   hdb_resale_prices.floor_area_sqm
            Details:  Check failed: expected mustBe 0, got 12
            Rule:     SELECT COUNT(*) FROM "hdb_resale_prices" WHERE TRY_CAST(floor_area_sqm AS BIGINT) >= 200
            Rows shown: 5 of 12
    +---------+-----------------+-----------+-------+---------------------+--------------+----------------+--------------------+---------------------+--------------------+--------------+
    | month   | town            | flat_type | block | street_name         | storey_range | floor_area_sqm | flat_model         | lease_commence_date | remaining_lease    | resale_price |
    +---------+-----------------+-----------+-------+---------------------+--------------+----------------+--------------------+---------------------+--------------------+--------------+
    | 2017-06 | KALLANG/WHAMPOA | 3 ROOM    | 38    | JLN BAHAGIA         | 01 TO 03     | 215.0          | Terrace            | 1972                | 54 years 01 month  | 830000.0     |
    | 2017-09 | CHOA CHU KANG   | EXECUTIVE | 641   | CHOA CHU KANG ST 64 | 16 TO 18     | 215.0          | Premium Maisonette | 1998                | 79 years 04 months | 888000.0     |
    | 2017-12 | KALLANG/WHAMPOA | 3 ROOM    | 65    | JLN MA'MOR          | 01 TO 03     | 249.0          | Terrace            | 1972                | 53 years 07 months | 1053888.0    |
    | 2018-01 | CHOA CHU KANG   | EXECUTIVE | 639   | CHOA CHU KANG ST 64 | 10 TO 12     | 215.0          | Premium Maisonette | 1998                | 79 years           | 900000.0     |
    | 2018-09 | KALLANG/WHAMPOA | 3 ROOM    | 41    | JLN BAHAGIA         | 01 TO 03     | 237.0          | Terrace            | 1972                | 52 years 10 months | 1185000.0    |
    +---------+-----------------+-----------+-------+---------------------+--------------+----------------+--------------------+---------------------+--------------------+--------------+
    ```

### Reading the summary

The summary groups the numbers for each schema (one table in the contract):

- **Checks Pass Rate** is passed checks over all checks.
- **ERRORED Checks** are checks that could not run, for example because the
  query names a missing column.
- **Failed Rows (approximate)** adds up the failed rows of each row-level
  check for this schema. A row that fails two checks counts twice, so the
  number is approximate. It shows `N/A` when the schema has no row-level
  checks. For how many rows failed at least once, and the row pass rate, call
  `result.get_dq_metrics_df()`. See
  [How Attributed Rows Work](design-considerations/checks/how-attributed-rows-work.md).
- **Single Table** covers checks that read only this schema's table.
  **Multi Table** covers cross-table checks, which read more than one table.
- **Non-unique Failed Rows** adds up the scalar counts of the failed
  cross-table checks. A row that fails two of them is counted twice.

In the **CHECK RESULTS** table, `check_id` is the check's name, and
`tables_in_query` lists the tables its query reads.

## Next steps

- [Writing contracts](contracts.md) shows how to describe your data and its
  checks.
- [Connecting to data](usage-patterns.md) covers databases, Spark and
  multi-source runs.
- [The Results Object](results.md) shows how to use the `ValidationResult`
  that `validate_data` returns, including saving it.
- [Run Settings](run-settings.md) lists every `ValidationConfig` setting.
