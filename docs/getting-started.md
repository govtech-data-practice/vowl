# Getting Started

## Installation

```bash
pip install vowl
```

Optional extras are available:

| Extra | What it adds |
|-------|-------------|
| `vowl[spark]` | PySpark support |
| `vowl[all]` | Everything (Spark + AWS) |

For local development, testing, and release workflow, see [CONTRIBUTING.md](https://github.com/govtech-data-practice/Vowl/blob/main/CONTRIBUTING.md).

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

## The `ValidationResult` Object

The `validate_data` function returns a powerful `ValidationResult` object that provides multiple ways to interact with your validation results.

### Core Methods

| Method/Property | What It Does | Returns |
|-----------------|--------------|---------|
| **`print_summary()`** | Prints high-level statistics (pass/fail counts, success rate, performance) | `self` (chainable) |
| **`show_failed_rows(max_rows=5)`** | Displays sample of failed rows in console. Use `max_rows=-1` for all rows. | `self` (chainable) |
| **`display_full_report(max_rows=5)`** | Prints summary + shows failed rows (convenience method) | `self` (chainable) |
| **`save(output_dir=".", prefix="vowl_results")`** | Saves enhanced CSV and summary JSON to disk | `self` (chainable) |
| **`get_output_dfs(checks=None)`** | Returns per-check failed rows as `{check_id: DataFrame}` | `Dict[str, DataFrame]` |
| **`get_consolidated_output_dfs(checks=None)`** | Deduplicates failed rows across checks, grouped by table | `Dict[str, DataFrame]` |
| **`.passed`** (property) | Boolean indicating if all checks passed | `True`/`False` |
