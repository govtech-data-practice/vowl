<div align="center">
  <img src="https://raw.githubusercontent.com/govtech-data-practice/vowl/main/docs/img/vowl_logo.png" alt="vowl logo" width="400">

  <br/>

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://govtech-data-practice.github.io/vowl/)
[![PyPI](https://img.shields.io/pypi/v/vowl?color=green)](https://pypi.org/project/vowl/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/govtech-data-practice/vowl/blob/main/LICENSE)
[![CI](https://github.com/govtech-data-practice/vowl/actions/workflows/ci.yml/badge.svg)](https://github.com/govtech-data-practice/vowl/actions/workflows/ci.yml)
[![ODCS Vendor](https://img.shields.io/badge/ODCS-Official%20Vendor-%23FF6B35)](https://github.com/bitol-io/open-data-contract-standard/blob/main/vendors.md)

</div>

# vowl

vowl (vee-owl 🦉) is a validation engine for [Open Data Contract Standard (ODCS)](https://github.com/bitol-io/open-data-contract-standard) data contracts. Define your validation rules once in a declarative YAML contract and get rich, actionable reports on your data's quality.

🏆 **Official ODCS Vendor**: `vowl` is actively maintained and listed on the official [ODCS vendors list](https://github.com/bitol-io/open-data-contract-standard/blob/main/vendors.md) as a natively compatible tool.

## Who it's for

- Data engineers and platform teams that need declarative, standards-based data quality validation
- Teams wanting a deterministic quality gate that guards pipelines against unreliable data, including data produced by increasingly automated or AI-assisted workflows
- Anyone working with ODCS data contracts who wants an automated way to enforce them

## Table of Contents

**Part 1 · Getting Started**

- [Who it's for](#who-its-for)
- [Features](#features)
- [Installation](#installation)
- [Validate in 3 lines](#validate-in-3-lines)

**Part 2 · Core Concepts**

- [Data Contracts](#data-contracts)
- [Auto-Generated Checks](#auto-generated-checks)
- [Library Metrics (`type: library`)](#library-metrics-type-library)
- [Format Checks](#format-checks)
- [Validation Results](#validation-results)
- [Architecture](#architecture)

**Part 3 · Usage Patterns**

- _Common sources_ — [Local DataFrame](#local-dataframe-pandaspolars) · [PySpark](#pyspark) · [Ibis (20+ backends)](#ibis-connections-20-backends) · [Concurrent Checks](#concurrent-checks-pooledadapter)
- _Filtering & cross-source_ — [Filter Conditions](#explicit-adapter-with-filter-conditions) · [Multi-Source](#multi-source-validation) · [Compatibility Mode](#compatibility-mode-duckdb-attach)
- _Advanced & extending_ — [Servers in Contract](#using-servers-defined-in-data-contract) · [Custom Adapters](#custom-adapters-and-executors) · [Remote Contracts (Git/S3)](#loading-contracts-from-remote-sources-gits3)

**More**

- [Roadmap](#roadmap)
- [Status](#status)
- [Contributing](#contributing)
- [Owner](#owner)
- [Security](#security)
- [License](#license)

---

# Part 1 · Getting Started

## Features

- **Extensible Check Engine**: Ships with a SQL check engine out of the box, with the architecture designed to support custom check types beyond SQL.
- **Auto-Generated Rules**: Checks are automatically derived from contract metadata (`logicalType`, `logicalTypeOptions`, `enum`, `required`, `unique`, `primaryKey`) and library metrics (`nullValues`, `missingValues`, `invalidValues`, `duplicateValues`, `rowCount`).
- **Any DataFrame, Any Backend**: Load any [Narwhals-compatible](https://github.com/narwhals-dev/narwhals) DataFrame type (pandas, Polars, PySpark, etc.) or connect to **20+ backends** via [Ibis](https://github.com/ibis-project/ibis). SQL dialect translation is handled by [SQLGlot](https://github.com/tobymao/sqlglot).
- **Server-Side Execution**: SQL checks run server-side through Ibis without materialising tables on the client.
- **Multi-Source Validation**: Validate across tables in different source systems with cross-database joins.
- **Declarative ODCS Contracts**: Define validation rules in YAML following the [Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard).
- **Flexible Filtering**: Filter conditions with wildcard pattern matching, ideal for incremental validation of new data.
- **Rich Reporting**: Detailed summaries, row-level failure analysis, saveable reports, and a chainable `ValidationResult` API.
- **No Silent Gaps**: Unimplemented or unrecognised checks surface as `ERROR`, not quietly skipped, so nothing slips through the cracks.

## Installation

```bash
pip install vowl
```

Or install from source:

```bash
pip install git+https://github.com/govtech-data-practice/vowl.git
```

Optional extras are available: `vowl[spark]`, `vowl[all]`. `vowl[spark]` supports both classic Spark and Spark Connect (`pyspark[connect]`, requires pyspark >= 3.4). On runtimes pinned to pyspark 3.0–3.3, use `vowl[spark-classic]` for classic Spark only (no Spark Connect).
For local development, testing, and release workflow, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Validate in 3 lines

```python
import pandas as pd  # or any Narwhals-compatible DataFrame
from vowl import validate_data

df = pd.read_csv("tests/hdb_resale/HDBResaleWithErrors.csv")
result = validate_data("tests/hdb_resale/hdb_resale_simple.yaml", df=df)
result.display_full_report()
```

<details>
<summary><strong>Output</strong> (click to expand)</summary>

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

</details>

Next, [Part 2 · Core Concepts](#part-2--core-concepts) explains what a contract looks like and what you get back. If you'd rather see runnable code, jump to [Part 3 · Usage Patterns](#part-3--usage-patterns) for PySpark, Ibis connections, multi-source validation, and more.

---

# Part 2 · Core Concepts

This section walks through the four ideas you need: how you **declare** rules ([Data Contracts](#data-contracts)), what vowl **generates for you** ([Auto-Generated Checks](#auto-generated-checks)), the **declarative metrics** you can add ([Library Metrics](#library-metrics-type-library) and [Format Checks](#format-checks)), and what you **get back** ([Validation Results](#validation-results)). It closes with the [Architecture](#architecture).

## Data Contracts

Instead of writing validation logic in Python, you declare it in a YAML file following the [Open Data Contract Standard (ODCS)](https://github.com/bitol-io/open-data-contract-standard). This separates your rules from your code, making them easier to manage, version, and share.

**Example [`hdb_resale_simple.yaml`](tests/hdb_resale/hdb_resale_simple.yaml)**:

```yaml
kind: DataContract
apiVersion: v3.2.0
version: 1.0.0
id: c11443ee-542f-4442-b28d-2d224342be37
name: HDB Resale Flat Prices
schema:
  - name: hdb_resale_prices # This becomes the table name in your SQL queries
    properties:
      # --- SQL Check: regex-based format validation ---
      - name: month
        logicalType: string
        quality:
          - type: sql
            name: Month
            description: Based on ISO 8601 | YYYY-MM
            mustBe: 0
            query: |-
              SELECT COUNT(*)
              FROM "hdb_resale_prices"
              WHERE CAST(month AS TEXT) !~ '^[0-9]{4}-(0[1-9]|1[0-2])$';
            dimension: conformity

      # --- Generated Check: allowed values from enum ---
      - name: flat_type
        enum:
          - value: 1 ROOM
          - value: 2 ROOM
          - value: 3 ROOM
          - value: 4 ROOM
          - value: 5 ROOM
          - value: EXECUTIVE
          - value: MULTI-GENERATION

      # --- SQL Check: business rule ---
      - name: floor_area_sqm
        quality:
          - name: floor_area_must_be_less_than_200
            type: sql
            query: SELECT COUNT(*) FROM "hdb_resale_prices" WHERE floor_area_sqm >= 200
            mustBe: 0
            dimension: consistency

      # --- SQL Check: resale price cap ---
      - name: resale_price
        quality:
          - name: resale_price_must_not_exceed_2m
            type: sql
            query: SELECT COUNT(*) FROM "hdb_resale_prices" WHERE resale_price > 2000000
            mustBe: 0
            dimension: consistency
```

## Auto-Generated Checks

You don't have to write every check by hand. When a contract is loaded, `vowl` automatically derives checks from your column metadata, so simply declaring `logicalType`, `required: true`, `unique: true`, and similar gives you validation for free. These auto-generated checks run before any explicit `quality` checks you've authored.

The check types currently generated:

| Generated from                        | What `vowl` validates                                                                                           |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `name`                                | Column declared in the contract exists in the source table                                                      |
| `logicalType`                         | Values can be cast to the declared SQL type for `integer`, `number`, `boolean`, `date`, `timestamp`, and `time` |
| `logicalTypeOptions.minLength`        | String length is at least the configured minimum                                                                |
| `logicalTypeOptions.maxLength`        | String length does not exceed the configured maximum                                                            |
| `logicalTypeOptions.pattern`          | String values match the configured regex pattern                                                                |
| `logicalTypeOptions.minimum`          | Value is greater than or equal to the configured minimum                                                        |
| `logicalTypeOptions.maximum`          | Value is less than or equal to the configured maximum                                                           |
| `logicalTypeOptions.exclusiveMinimum` | Value is strictly greater than the configured minimum                                                           |
| `logicalTypeOptions.exclusiveMaximum` | Value is strictly less than the configured maximum                                                              |
| `logicalTypeOptions.multipleOf`       | Value is a multiple of the configured number                                                                    |
| `logicalTypeOptions.format`           | Value satisfies the declared format (see [Format Checks](#format-checks))                                       |
| `logicalTypeOptions.minItems`         | Array (`logicalType: array`) contains at least the configured number of items                                   |
| `logicalTypeOptions.maxItems`         | Array contains at most the configured number of items                                                           |
| `logicalTypeOptions.uniqueItems`      | Array (`uniqueItems: true`) contains no duplicate items                                                         |
| `items.logicalType`                   | Every element of an array casts to the declared element type                                                    |
| `items.logicalTypeOptions.*`          | Every element satisfies the element option (`minLength`, `pattern`, `minimum`, `format`, …)                     |
| `items.enum`                          | Every element is within the declared allowed set                                                                |
| `enum`                                | Non-null values are within the declared allowed set (`enum` value list)                                         |
| `required: true`                      | Column contains no `NULL` values                                                                                |
| `unique: true`                        | Non-null values are unique                                                                                      |
| `primaryKey: true`                    | Values are both unique and non-null                                                                             |
| `relationships` (`foreignKey`)        | Every non-null key value exists in the referenced target table (referential integrity)                          |

For example, a property like this:

```yaml
- name: block
    logicalType: string
    logicalTypeOptions:
        maxLength: 10
    required: true
```

produces three generated checks: a column-exists check, a `maxLength` option check, and a `required` (no-NULL) check. Because `string` does not currently generate a SQL cast-based type check, the `logicalType` entry above contributes metadata for option checks rather than a standalone type-validation query. If you use `integer`, `number`, `boolean`, `date`, `timestamp`, or `time`, `vowl` also generates a `logicalType` SQL check automatically. You only need to define extra `quality` entries when you want custom business rules beyond the contract metadata.

<details>
<summary><strong>Reference: how check references are built (JSONPath internals)</strong></summary>

When a contract is loaded, `vowl` builds `CheckReference` objects for every executable check via `Contract.get_check_references_by_schema()`. This includes both user-authored checks in `quality` blocks and synthetic checks derived from column metadata. The generated references are grouped by schema, and the auto-generated ones run before explicit `quality` checks.

| Reference type               | Trigger in contract                            | JSONPath stored in the reference                           |
| ---------------------------- | ---------------------------------------------- | ---------------------------------------------------------- |
| Table check                  | Entry under schema-level `quality`             | `$.schema[N].quality[M]`                                   |
| Column check                 | Entry under property-level `quality`           | `$.schema[N].properties[M].quality[K]`                     |
| Library column metric        | `type: library` under property-level `quality` | `$.schema[N].properties[M].quality[K]`                     |
| Library table metric         | `type: library` under schema-level `quality`   | `$.schema[N].quality[M]`                                   |
| Declared column exists check | Property has a `name`                          | `$.schema[N].properties[M]`                                |
| Logical type check           | `logicalType` present on a property            | `$.schema[N].properties[M].logicalType`                    |
| Logical type options check   | Supported key under `logicalTypeOptions`       | `$.schema[N].properties[M].logicalTypeOptions.<optionKey>` |
| Array items check            | `items` sub-schema on a `logicalType: array`   | `$.schema[N].properties[M].items.<...>`                    |
| Enum check                   | `enum` present on a property                   | `$.schema[N].properties[M].enum`                           |
| Required check               | `required: true`                               | `$.schema[N].properties[M].required`                       |
| Unique check                 | `unique: true`                                 | `$.schema[N].properties[M].unique`                         |
| Primary key check            | `primaryKey: true`                             | `$.schema[N].properties[M].primaryKey`                     |

So the `block` property above produces three generated check references pointing at:

| Check path                                                 | Check type                           |
| ---------------------------------------------------------- | ------------------------------------ |
| `$.schema[0].properties[...]`                              | `DeclaredColumnExistsCheckReference` |
| `$.schema[0].properties[...].logicalTypeOptions.maxLength` | `LogicalTypeOptionsCheckReference`   |
| `$.schema[0].properties[...].required`                     | `RequiredCheckReference`             |

</details>

### Relationships (foreign keys)

A `relationships` entry of type `foreignKey` says every non-`NULL` value in a column must exist in another table's column. `vowl` runs this as a real check, so you get referential integrity from metadata without writing the cross-table SQL by hand (compare the `employee_id_exists_in_master_list` example under [Residues](#residues) below).

Declare the relationship on the property that holds the key. The `to` target uses shorthand `<object>.<property>` notation, resolved by property `name`:

```yaml
schema:
  - name: demo_employee_list # the master list (referenced side)
    properties:
      - name: employee_id
        logicalType: string
        primaryKey: true
  - name: demo_employee_payroll # references the master list
    properties:
      - name: employee_id
        logicalType: string
        relationships:
          - type: foreignKey
            to: demo_employee_list.employee_id
```

This generates a check named `demo_employee_payroll_employee_id_foreign_key_check`. A payroll row whose `employee_id` is `NULL` is **skipped**, so an optional foreign key is allowed. A value fails only when it is present but has no match in the master list. Because failed rows carry only the payroll table's columns, they appear **in the `demo_employee_payroll` annotated table** rather than a separate residue.

Need composite keys, cross-file references, or cross-source joins? See [Data Contracts: Relationships (Foreign Keys)](docs/contracts.md#relationships-foreign-keys).

## Library Metrics (`type: library`)

Instead of writing SQL by hand, you can declare common data quality metrics using `type: library` in your `quality` blocks. `vowl` auto-generates the appropriate SQL at runtime.

**Column-level metrics** (under a property's `quality`):

| `metric`          | What it checks                                              | Arguments                                                                      |
| ----------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `nullValues`      | Count of `NULL` values in the column                        | -                                                                              |
| `missingValues`   | Count of values matching a configurable missing-values list | `arguments.missingValues`: list of sentinel values (use `null` for SQL NULL)   |
| `invalidValues`   | Count of values that fail valid-value or pattern criteria   | `arguments.validValues`: allowed values list and/or `arguments.pattern`: regex |
| `duplicateValues` | Count of duplicate non-NULL values in the column            | -                                                                              |

**Table-level metrics** (under a schema's `quality`):

| `metric`          | What it checks                                   | Arguments                                             |
| ----------------- | ------------------------------------------------ | ----------------------------------------------------- |
| `rowCount`        | Total number of rows in the table                | -                                                     |
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

## Format Checks

The `logicalTypeOptions.format` key validates that column values conform to a declared format. The check generated depends on the column's `logicalType`. In short:

- **Integer formats** (`i8`…`u64`) — range-check a fixed-width integer type.
- **String formats** (`uuid`, `email`, `ipv4`, `ipv6`, `hostname`, `uri`) — match a built-in regex.
- **Date/timestamp/time formats** — a JDK `DateTimeFormatter` pattern (e.g. `yyyy-MM-dd`) is converted to a regex and matched against string-cast values.
- **Number formats** (`f32`, `f64`) — recognised but metadata-only (no check).

```yaml
- name: age
  logicalType: integer
  logicalTypeOptions:
    format: u8 # 0 – 255

- name: request_id
  logicalType: string
  logicalTypeOptions:
    format: uuid

- name: created_at
  logicalType: timestamp
  logicalTypeOptions:
    format: "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
```

<details>
<summary><strong>Reference: supported formats and ranges</strong></summary>

**Integer formats** — validates that values fall within the range of a fixed-width integer type:

| `format` | Min                        | Max                        |
| -------- | -------------------------- | -------------------------- |
| `i8`     | -128                       | 127                        |
| `i16`    | -32,768                    | 32,767                     |
| `i32`    | -2,147,483,648             | 2,147,483,647              |
| `i64`    | -9,223,372,036,854,775,808 | 9,223,372,036,854,775,807  |
| `u8`     | 0                          | 255                        |
| `u16`    | 0                          | 65,535                     |
| `u32`    | 0                          | 4,294,967,295              |
| `u64`    | 0                          | 18,446,744,073,709,551,615 |

`i128` and `u128` are recognised but skipped because their ranges exceed what SQL engines can represent.

**String formats** — validates values against a built-in regex pattern:

| `format`   | What it checks                                                 |
| ---------- | -------------------------------------------------------------- |
| `uuid`     | UUID v1-v5 hex format (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) |
| `email`    | Basic `local@domain.tld` structure                             |
| `ipv4`     | Dotted-decimal IPv4 address (`0.0.0.0` - `255.255.255.255`)    |
| `ipv6`     | Full-form colon-separated IPv6 address                         |
| `hostname` | RFC-952 hostname with TLD                                      |
| `uri`      | URI with a valid scheme prefix (e.g. `https:`, `s3:`)          |

`password`, `byte`, and `binary` are recognised but skipped because they cannot be validated against data.

**Number formats** — `f32` and `f64` are recognised but produce no check (metadata-only).

**Date, timestamp and time formats** — accepts a JDK DateTimeFormatter pattern (e.g. `yyyy-MM-dd`). `vowl` converts the pattern to a regex and validates that string-cast values match. Supported tokens include `yyyy`, `yy`, `MM`, `dd`, `HH`, `mm`, `ss`, `SSS`, and timezone offsets (`X`/`XXX`/`Z`). If a pattern contains tokens `vowl` cannot translate, the check is skipped with a warning.

</details>

## Validation Results

The `validate_data` function returns a powerful `ValidationResult` object that provides multiple ways to interact with your validation results.

#### Core Methods

| Method/Property                                                                      | What It Does                                                                                                                                                                                           | Returns                         |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------- |
| **`print_summary()`**                                                                | Prints high-level statistics (pass/fail counts, success rate, performance)                                                                                                                             | `self` (chainable)              |
| **`show_failed_rows(max_rows=5)`**                                                   | Displays sample of failed rows in console. Use `max_rows=-1` for all rows.                                                                                                                             | `self` (chainable)              |
| **`display_full_report(max_rows=5)`**                                                | Prints summary + shows failed rows (convenience method)                                                                                                                                                | `self` (chainable)              |
| **`save(output_dir=".", prefix="vowl_results", outputs=None, check_info=None)`**     | Saves the results to disk. `outputs` lists the files to write. The default writes everything but `"all_query_outputs"`. `check_info` shapes the `check_info` column | `self` (chainable)              |
| **`get_output_dfs(checks=None, scope="failed")`**                                    | Returns per-check failed rows as `{check_id: DataFrame}`. `scope="all"` returns the rows of every row-level check, with a `status` column | Dict[str, DataFrame]            |
| **`get_annotated_output(checks=None, check_info=None)`**                             | Returns full in-scope tables with a `check_info` column (JSON array of objects) annotating failed rows                                                                                                 | Dict[str, Dict[str, DataFrame]] |
| **`get_dq_metrics_df(by="schema")`**                                                 | Returns how many rows of each table failed at least one check, and the pass rate. `by` can be `"schema"`, `"dimension"` or `"check"`                                                                   | DataFrame                       |
| **`get_dq_metrics()`**                                                               | Returns the run's DQ metrics at check, dimension, schema and run level: the content of `dq_metrics.json`. See [Understanding DQ Metrics](docs/dq-metrics/understanding-metrics.md)                     | dict                            |
| **`.passed`** (property)                                                             | Boolean indicating if all checks passed                                                                                                                                                                | `True`/`False`                  |

#### Row Quality

`get_dq_metrics_df()` returns the row counts. The summary does not show them. It shows **Failed Rows (approximate)**, a sum of the scalar counts of the failed checks. For each table, `get_dq_metrics_df()` gives how many rows failed at least one row-level check, and the share that passed. A row that fails two checks counts once, and every copy of a duplicated row counts. vowl counts inside your data source where it can, so the numbers do not depend on `max_failed_rows`, and each one carries an `approximate` flag for the cases where it could be off. A check whose failed rows vowl can't attribute to the table is left out and counted in `checks_not_attributable`. See [When counting and annotating differ](docs/design-considerations/checks/annotating-the-source-table.md#when-counting-and-annotating-differ) for how the row counts relate to annotated output.

```python
result.get_dq_metrics_df()                  # one row per table
result.get_dq_metrics_df(by="dimension")    # one row per table and dimension
result.get_dq_metrics_df(by="check")        # which checks are row-level, and how they were counted
```

By default only failed checks add rows. A check that passed within its tolerance (for example 50 rows under `mustBeLessThan: 100`) adds nothing. Set [`ValidationConfig(fetch_tolerated_rows=True)`](docs/run-settings.md#fetch_tolerated_rows) to fetch its rows too, so they count in the row counts and appear in every output, marked as tolerated. See [Tolerated rows](docs/design-considerations/checks/check-results.md#tolerated-rows).

Checks that are plain row filters are counted inside your data source wherever it supports it. For other checks, such as ones with `DISTINCT` or a join, vowl attributes each failed row to the source table, so a row that fails several checks counts once. On DuckDB, SQLite, Spark, Databricks and PostgreSQL the data source does this. Elsewhere vowl downloads the table and holds it in memory, about 1 to 1.5 GiB for 1 million rows at 6 columns. Annotated output reuses the same download. This work runs only when you ask for DQ metrics. `print_summary()`, `get_check_results_df()` and `save()` use the scalar counts only.

See [Counting Mechanisms](docs/design-considerations/checks/counting-mechanisms.md).

#### Annotated Output

`get_annotated_output()` returns the **full in-scope table** with a `check_info` column that annotates which rows failed which checks. Passing rows have `null` in the `check_info` column. This is useful when you need to see failures in the context of the full dataset rather than just the isolated failed rows.

> New to this? [Check Results](docs/design-considerations/checks/check-results.md) explains failed rows, annotated output and residues in plain language, with a small worked example.

It returns a nested dict with two reserved keys:

- **`"annotated"`**: a `{schema: table}` dict where each table is your full in-scope data plus a `check_info` column. Every original row is present; `check_info` is `null` for rows that passed everything and holds a JSON array of objects describing the failing check(s) otherwise.
- **`"residues"`**: failed rows for checks that _cannot_ be merged onto a single table (checks whose failed rows lack the table's primary key or, when it declares none, don't have exactly its columns, such as column-subset checks). Checks that return one number, such as an average, a sum, a minimum or a maximum, produce none. Most single-table contracts produce none. Residues are **per-check** (one entry per non-mergeable check, keyed `"<schema>.<column>::<check_name>"`, or `"<schema>::<check_name>"` for a schema-level check) and carry the **same `check_info` column** as the annotated tables (a single-element JSON array, shaped by the same preset) plus `tables_in_query`, so everything `get_annotated_output()` returns is read the same way. (A cross-table check _can_ merge onto its home schema if you shape its row query to project only that schema's columns: see [Annotating the failed rows of a cross-table check](docs/design-considerations/cross-table/how-it-works.md#annotating-the-failed-rows-of-a-cross-table-check).)

The **`check_info`** parameter (`"names"` default, `"summary"`, or `"full"`) shapes each array element. Every preset emits a JSON **array of objects** so consumers parse uniformly via `item["check_name"]`; they differ only in how many keys each object carries:

- **`"names"`** — `[{"check_name": ...}, ...]` (just the failing check name(s)).
- **`"summary"`** — `[{"check_name", "dimension", "tags", "target"}, ...]`.
- **`"full"`** — `[<full check_definition> + "check_name" + "target", ...]`.

```python
import json

result = validate_data("contract.yaml", df=df)
output = result.get_annotated_output(check_info="summary")

annotated = output["annotated"]["hdb_resale_prices"].to_pandas()
# columns: <original columns> + check_info
# output["residues"] — cross-table or non-mergeable failures (empty for single-table contracts)

# check_info is a JSON string; parse it to read each failing check's dimension/tags/target.
failing = annotated[annotated["check_info"].notna()].iloc[0]
print(json.loads(failing["check_info"]))
# [{"check_name": "Year", "dimension": "conformity",
#   "tags": ["SG-DRM v5.0"], "target": "hdb_resale_prices.lease_commence_date"}]
```

#### Annotated Table

<details>
<summary><strong>Output</strong> — full table, flagged rows floated to the top (click to expand)</summary>

```python
# Sort so flagged rows surface first; passing rows (check_info = null) sink to the bottom.
flagged_first = annotated.sort_values("check_info", na_position="last").reset_index(drop=True)

flagged_first[["month", "town", "block", "floor_area_sqm",
               "lease_commence_date", "check_info"]]
```

|     | month    | town            | block | floor_area_sqm | lease_commence_date | check_info                                                                                                                                   |
| --- | -------- | --------------- | ----- | -------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| 0   | 2017-01  | BEDOK           |       | 84.0           | 1986                | `[{"check_name": "AddressBlockHouseNumber", "dimension": "conformity", "tags": [...], "target": "hdb_resale_prices.block"}]`                 |
| 1   | 2017-jan | BEDOK           | 21    | 130.0          | 1972                | `[{"check_name": "Month", "dimension": "conformity", "tags": [...], "target": "hdb_resale_prices.month"}]`                                   |
| 2   | 2017-jan | BISHAN          | 105   | 4.0            | 1985                | `[{"check_name": "Month", "dimension": "conformity", "tags": [...], "target": "hdb_resale_prices.month"}]`                                   |
| 3   | 2017-01  | ANG MO KIO      | 219   | 67.0           | 1977.0              | `[{"check_name": "Year", "dimension": "conformity", "tags": [...], "target": "hdb_resale_prices.lease_commence_date"}]`                      |
| 4   | 2017-01  | ANG MO KIO      | 211   | 67.0           | abc                 | `[{"check_name": "Year", "dimension": "conformity", "tags": [...], "target": "hdb_resale_prices.lease_commence_date"}]`                      |
| 5   | 2017-06  | KALLANG/WHAMPOA | 38    | 215.0          | 1972                | `[{"check_name": "floor_area_must_be_less_than_200", "dimension": "accuracy", "tags": [...], "target": "hdb_resale_prices.floor_area_sqm"}]` |
| ... | ...      | ...             | ...   | ...            | ...                 | ...                                                                                                                                          |
| 8   | 2017-01  | ANG MO KIO      | 406   | 73.0           | 1979                |                                                                                                                                              |
| 9   | 2017-01  | ANG MO KIO      | 108   | 67.0           | 1978                |                                                                                                                                              |
| 10  | 2017-01  | ANG MO KIO      | 602   | 67.0           | 1984                |                                                                                                                                              |
| ... | ...      | ...             | ...   | ...            | ...                 | ...                                                                                                                                          |

_201,879 rows — failed rows floated to the top, passing rows (empty `check_info`) below. Shown with `check_info="summary"`._

Because the annotation lives on the full table, separating the good rows from the bad is a one-liner — filter to where `check_info` is null and drop the annotation column:

```python
clean = annotated[annotated["check_info"].isna()].drop(columns=["check_info"])
# 201,861 clean rows of 201,879, ready for downstream use — no join back to the source needed
```

</details>

A check whose failed rows can't be matched to rows of one table becomes a residue: its rows lack the table's declared primary key or, when the table declares none, don't have exactly the table's columns. Column-subset checks, such as one that returns only distinct values, are the usual case. A cross-table check merges when its failed rows carry the table's primary key, or when its row query projects only its home schema's columns. See [Annotating the failed rows of a cross-table check](docs/design-considerations/cross-table/how-it-works.md#annotating-the-failed-rows-of-a-cross-table-check). Checks that return one number, such as an average, a sum, a minimum or a maximum, have no failed rows and appear only in the summary. Residues are **per-check**, one entry per non-mergeable check, keyed `"<schema>.<column>::<check_name>"` (`"<schema>::<check_name>"` for a schema-level check). Each carries its own failed rows plus the same `check_info` column the annotated tables use (a single-element JSON array) and `tables_in_query`:

#### Residues

<details>
<summary><strong>Output</strong> — a residue from a cross-table (multi-source) contract (click to expand)</summary>

The payroll table declares a primary key, so its bare-JOIN referential checks merge onto it. This check returns only the distinct phone numbers missing from the master list, which hold no key:

```yaml
- name: phone_numbers_missing_from_master_list
  type: sql
  query: >-
    SELECT COUNT(*) FROM (
      SELECT DISTINCT payroll.phone_number
      FROM demo_employee_payroll payroll
      LEFT JOIN demo_employee_list ref ON payroll.phone_number = ref.phone_number
      WHERE payroll.phone_number IS NOT NULL AND ref.phone_number IS NULL
    ) AS missing_numbers
  mustBe: 0
```

```python
output = result.get_annotated_output()

print("Residue keys:", list(output["residues"].keys()))
for key, residue in output["residues"].items():
    df = residue.to_pandas()
    print(f"\nResidue '{key}': {len(df)} failed row(s)")
    print(df)
```

Residue keys: `['demo_employee_payroll::phone_numbers_missing_from_master_list']`

Each non-mergeable check gets its own entry. They are never grouped together, so a row that failed two such checks appears once under each check's residue:

Residue `'demo_employee_payroll::phone_numbers_missing_from_master_list'`: 2 failed row(s)

|     | phone_number | check_info                                                   | tables_in_query                           |
| --- | ------------ | ------------------------------------------------------------ | ----------------------------------------- |
| 0   | 6581234567   | `[{"check_name": "phone_numbers_missing_from_master_list"}]` | demo_employee_list, demo_employee_payroll |
| 1   | 6594327654   | `[{"check_name": "phone_numbers_missing_from_master_list"}]` | demo_employee_list, demo_employee_payroll |

</details>

> For the full eligibility rules and worked examples of each non-mergeable category, see [Where each failed check ends up](docs/design-considerations/checks/annotating-the-source-table.md#where-each-failed-check-ends-up). The [Basic Tutorial notebook](examples/1_basic_tutorial/basic_tutorial.ipynb) walks through these examples end-to-end.

`save()` always writes `<prefix>_check_results.csv` and `<prefix>_summary.json`. The `outputs` argument lists the other files to write:

| Output                         | Files                                                            |
| ------------------------------ | ---------------------------------------------------------------- |
| `"failed_query_outputs"`       | `<prefix>_checks/<schema>__<column>__<check>.csv`, one per failed check |
| `"all_query_outputs"`          | The same folder, one per row-level check whatever its status     |
| `"consolidated_query_outputs"` | `<prefix>_<tables>.csv`, the failed rows grouped per table set   |
| `"annotated_table"`            | `<prefix>_<schema>_annotated.csv` and the residue files          |
| `"dq_metrics"`                 | `<prefix>_dq_metrics.json`                                       |

```python
# Every output but "all_query_outputs". This is the default.
result.save()

# Shape the check_info column: "names" (default), "summary", or "full"
result.save(outputs=["annotated_table"], check_info="summary")

# Only the grouped failed-rows CSVs
result.save(outputs=["consolidated_query_outputs"])
```

> **Cost:** `"annotated_table"` and `"dq_metrics"` attribute failed rows to each table and may download it, which can be slow on a large table. They share that work, so writing both costs no more than writing one. The other outputs do nothing to the rows. `"all_query_outputs"` runs the row query of each check that passed.

`output_mode` is deprecated. It still works with a `FutureWarning` and will be removed in v0.1.0. See [Deprecated output_mode](docs/results.md#deprecated-output_mode).

You can also set the outputs globally via `ValidationConfig` (see [Run Settings](docs/run-settings.md#saving-results)):

```python
from vowl import ValidationConfig, validate_data

config = ValidationConfig(outputs=["annotated_table", "dq_metrics"], annotated_check_info="summary")
result = validate_data("contract.yaml", df=df, config=config)
result.save()  # uses the configured outputs and check_info
```

`save()` can also write straight to cloud storage. Pass a URI instead of a folder:

```python
result.save("s3://my-bucket/dq-results/run-1/")
```

It works for `s3://`, `gs://`, `abfs://` and `hdfs://` using the filesystems built into pyarrow, so there's nothing extra to install. Credentials come from the usual place for each cloud, such as environment variables, `~/.aws`, or an IAM role. To use a custom endpoint or explicit credentials, pass `filesystem=`. See [Saving Results to Cloud Storage](docs/results.md#saving-results).

## Architecture

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

#### Key Components

| Component                  | Description                                                                                                                                                                                                                                                 |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **DataSourceMapper**       | Auto-detects a single input source (DataFrame, Spark object, Ibis backend, or connection string) and creates the appropriate adapter                                                                                                                        |
| **IbisAdapter**            | Universal adapter supporting 20+ backends via Ibis (pandas, Polars, PySpark, PostgreSQL, Snowflake, BigQuery, etc.)                                                                                                                                         |
| **MultiSourceAdapter**     | Routes checks across multiple data sources, separating single-table checks (delegated to per-schema adapters) from multi-table checks (sent to `MultiSourceSQLExecutor`)                                                                                    |
| **IbisSQLExecutor**        | Executes SQL-based quality checks through the Ibis query layer (server-side)                                                                                                                                                                                |
| **MultiSourceSQLExecutor** | Executes cross-source SQL with two modes: **direct delegation** when all tables share the same compatible backend, or **DuckDB materialisation** when backends differ. Tables are exported as Arrow and loaded into a local DuckDB for cross-database joins |
| **Contract**               | Parses ODCS YAML contracts into executable validation rules                                                                                                                                                                                                 |
| **ValidationResult**       | Rich result object with enhanced DataFrames, metrics, and export capabilities                                                                                                                                                                               |

---

# Part 3 · Usage Patterns

> **Interactive demo:** Try the [example notebooks](examples/) for a hands-on walkthrough of the examples below — start with the [Basic Tutorial](examples/1_basic_tutorial/basic_tutorial.ipynb).

The patterns are grouped from most common to most advanced:

- **Common sources** — point vowl at the data you already have: a [local DataFrame](#local-dataframe-pandaspolars), [PySpark](#pyspark), or any of [20+ Ibis backends](#ibis-connections-20-backends).
- **Filtering & cross-source** — validate a [subset of rows](#explicit-adapter-with-filter-conditions), or join across [multiple source systems](#multi-source-validation) — optionally through [DuckDB ATTACH](#compatibility-mode-duckdb-attach).
- **Advanced & extending** — drive connections from [servers in the contract](#using-servers-defined-in-data-contract), build [custom adapters/executors](#custom-adapters-and-executors), or load contracts from [Git and S3](#loading-contracts-from-remote-sources-gits3).

## Common sources

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

### Concurrent Checks (`PooledAdapter`)

When a contract has many independent checks and the backend can serve several
queries at once, run them concurrently by wrapping a connection _factory_ in a
`PooledAdapter`. It keeps a thread-safe pool of connections (one per worker) and
dispatches checks across them; the verdicts are identical to a sequential run.

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter, PooledAdapter

# factory: returns a fresh adapter (new connection) on each call. Called once
# per pooled connection, so the table must be available on every connection.
def make_adapter():
    con = ibis.duckdb.connect("my_db.duckdb")
    return IbisAdapter(con)

pooled = PooledAdapter(factory=make_adapter, max_concurrency=4)

# Pass the pool like any other adapter: adapter=pooled for every schema,
# or adapters={"my_table": pooled, ...} to give each schema its own.
result = validate_data("contract.yaml", adapter=pooled)
result.display_full_report()
```

`max_concurrency` caps the checks in flight, and so the connections open, for
the whole run. Schemas given the same pool share that cap and run side by side.
A join between tables on one pool runs in the database on one of the pool's
connections. A join across two pools, or between a pool and another adapter,
is copied to a local DuckDB. See [PooledAdapter](docs/design-considerations/cross-table/how-it-works.md#pooledadapter).

The pool keeps its adapters after the run, so you can pass it to another
`validate_data` call. Call `pooled.cleanup()` when you are done with it. This
drops the pooled adapters and calls `cleanup()` on each one that defines it. It
does not close Ibis connections, so close those yourself if your factory opens
ones that need closing.

## Filtering & cross-source

### Explicit Adapter with Filter Conditions

```python
from vowl import validate_data
from vowl.adapters import IbisAdapter
from datetime import datetime, timedelta
import ibis

date_limit = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
con = ibis.postgres.connect(...)

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

### Multi-Source Validation

There are two ways to validate across tables in different databases.

#### Option A: DuckDB ATTACH (streams rows per check, no up-front download)

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

con = ibis.duckdb.connect()

# Attach multiple remote databases
con.raw_sql("ATTACH 'postgresql://user:pass@host:5432/salesdb' AS pg_sales (TYPE postgres, READ_ONLY)")  # trufflehog:ignore
con.raw_sql("ATTACH 'sqlite:///path/to/users.db' AS sqlite_users (TYPE sqlite, READ_ONLY)")

# Switch back to local DuckDB so views live in memory
con.raw_sql("USE memory")

# Create views as prefix-free shortcuts to the attached tables
con.raw_sql("CREATE VIEW transactions AS SELECT * FROM pg_sales.transactions")
con.raw_sql("CREATE VIEW users AS SELECT * FROM sqlite_users.users")

# Now vowl (and your contract queries) can reference tables without alias prefixes
result = validate_data("contract.yaml", adapter=IbisAdapter(con))
result.display_full_report()
```

> **Note:** A view is a saved query, not a copy, and it gives your contracts clean, prefix-free table names. DuckDB still runs on your machine, so each check streams the rows it reads from the attached databases, then discards them. For PostgreSQL and MySQL, simple filters are sent to the source. Joins and aggregates such as `COUNT(*)` run inside DuckDB, so the rows they read cross the network, and a table used by several checks is read once per check. DuckDB ATTACH supports PostgreSQL, MySQL, and SQLite.

#### Option B: Multi-Source Adapters (downloads tables for cross-table checks)

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

> **Why this exists:** A fallback for backends that DuckDB ATTACH does not support (e.g. Snowflake, BigQuery, Databricks, Oracle, MSSQL). Single-table checks run inside each table's own database. For a cross-table check across different connections, the `MultiSourceAdapter` **downloads each table it reads in full** (after filter conditions) via Arrow into a local DuckDB instance, so prefer ATTACH for large tables when your sources support it. DuckDB ATTACH only supports PostgreSQL, MySQL, and SQLite. It cannot be used as a general-purpose multi-source strategy because of [namespace, credential, and filter limitations](docs/design-considerations/cross-table/how-it-works.md#why-vowl-copies-instead-of-using-attach). Both options share a [known dark pattern](docs/design-considerations/cross-table/how-it-works.md#tables-outside-the-contract): SQL checks can reference tables not declared in the contract's `schema` block. vowl reads an undeclared table through the connection of the schema the check sits under, whether the check reads it alone or joins it to other tables, and the check runs whenever that connection can see the table.

### Compatibility Mode ([DuckDB](https://github.com/duckdb/duckdb) ATTACH)

```python
import ibis
from vowl import validate_data
from vowl.adapters import IbisAdapter

# ATTACH lets DuckDB query your remote database directly.
# Data is streamed on demand, not materialised locally.
# All SQL is evaluated by DuckDB, so dialect differences are eliminated.
con = ibis.duckdb.connect()
con.raw_sql("ATTACH 'postgresql://user:pass@host:5432/mydb' AS pg (TYPE postgres, READ_ONLY)")  # trufflehog:ignore
con.raw_sql("USE pg")  # Allows querying tables without the pg. alias

result = validate_data("contract.yaml", adapter=IbisAdapter(con))
result.display_full_report()
```

> **When to use this:** Your remote backend doesn't support a SQL feature that a check needs, or you want a single local engine for reproducible results regardless of the source database. DuckDB ATTACH supports PostgreSQL, MySQL, and SQLite.

## Advanced & extending

### Using Servers Defined in Data Contract

```python
from vowl import validate_data
from vowl.contracts import Contract
from vowl.adapters import IbisAdapter
import ibis

# Load the contract and get server configuration
contract = Contract.load("contract.yaml")
server = contract.get_server("my-postgres-server")  # Match by server name
# Or: contract.get_server("uat")        # falls back to matching by environment
# Or: contract.get_server()             # returns the first server

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

`validate_data` accepts any `BaseAdapter` through `adapter=` or `adapters=`, including `IbisAdapter`, `PooledAdapter` and your own subclasses. A custom adapter runs its checks through the executors it registers. A cross-table check runs in the database only if `is_compatible_with` says the adapters can share a query. The default returns `False`, so vowl copies the tables to a local DuckDB, which needs `export_table_as_arrow`. When the tables in such a join have different filter conditions, the adapter also needs `with_filter_conditions`, or vowl copies the tables.

### Loading Contracts from Remote Sources (Git/S3)

Contracts don't have to live on local disk — `validate_data` accepts GitHub/GitLab URLs and S3 URIs directly.

**Git (GitHub/GitLab):**

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

**S3:**

```python
from vowl import validate_data

# S3 URI format
result = validate_data("s3://my-bucket/contracts/my_contract.yaml", df=df)
result.display_full_report()

# Note: `boto3` is not included in the base install.
# Install it with: pip install vowl[all]  or  pip install boto3
# Uses default AWS credentials (environment variables, ~/.aws/credentials, IAM role, etc.)
```

---

## Roadmap

### Completed

| Capability                         | Description                                                                                                                                                             |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ✅ **Ibis Connectors**             | Interoperability with 20+ data sources via Ibis (PostgreSQL, Snowflake, BigQuery, Databricks, etc.)                                                                     |
| ✅ **Remote Contract Loading**     | Load contracts from S3 (`s3://`) and Git (GitHub/GitLab URLs)                                                                                                           |
| ✅ **Remote Result Saving**        | Save results straight to S3, Google Cloud, Azure, or HDFS with `result.save("s3://...")`                                                                                |
| ✅ **JSONPath Navigation**         | Navigate contract elements using JSONPath expressions (`contract.resolve("$.schema[0].name")`)                                                                          |
| ✅ **Static Checks**               | Auto-generated checks from contract elements: `logicalType`, `logicalTypeOptions`, `required`, `unique`, `primaryKey`                                                   |
| ✅ **Library Metrics**             | Declare common data quality metrics (`nullValues`, `missingValues`, `invalidValues`, `duplicateValues`, `rowCount`) with `type: library`. SQL auto-generated at runtime |
| ✅ **ODCS Schema Validation**      | Contracts validated against ODCS JSON Schema before execution                                                                                                           |
| ✅ **Filter Conditions**           | Incremental quality testing with wildcard pattern matching - optimised for append-only data sources                                                                     |
| ✅ **Multi-Schema Checks**         | Cross-table referential checks within a single contract                                                                                                                 |
| ✅ **Multi-Connection Checks**     | Cross-table referential checks between different servers/databases via `MultiSourceAdapter`                                                                             |
| ✅ **Optional Extras**             | Add optional Spark support with `.[spark]` or install `.[all]`                                                                                                          |
| ✅ **Custom Adapters & Executors** | Extensible architecture - create custom adapters and executors by extending `BaseAdapter`, `BaseExecutor`, or `SQLExecutor`                                             |
| ✅ **Parallel Check Execution**    | Run checks in parallel for faster validation across large contracts via the pooled adapter                                                                              |
| ✅ **OpenTelemetry Export**        | Export [DQ metrics](docs/dq-metrics/understanding-metrics.md), traces, and logs via OpenTelemetry (OTLP), behind the optional `[otel]` extra                            |

### Planned

| Capability                       | Description                                                               | Status  |
| -------------------------------- | ------------------------------------------------------------------------- | ------- |
| 🔬 **Alternative Check Engines** | Support for dqx, Soda, Great Expectations (subject to licensing review)   | Planned |
| 📅 **CLI Interface**             | Command-line interface for running validations directly from the terminal | Planned |
| 📅 **vowl-ui**                   | Web-based validation interface for vowl                                   | Planned |

---

## Status

**Active**. Under ongoing development, with issues and PRs monitored.

---

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to get started.

---

## Owner

Maintained by **GovTech's Data Practice** ([`govtech-data-practice`](https://github.com/govtech-data-practice)).

---

## Security

See [`SECURITY.md`](SECURITY.md) for how to report a vulnerability and which versions are supported.

---

## License

This project is licensed under the [MIT License](LICENSE).
