# Data Contracts

## The Core Concept

Instead of writing validation logic in Python, you declare it in a YAML file following the [Open Data Contract Standard (ODCS)](https://github.com/bitol-io/open-data-contract-standard). This separates your rules from your code, making them easier to manage, version, and share.

**Example `hdb_resale.yaml`:**

```yaml
kind: DataContract
apiVersion: v3.1.0
schema:
  - name: hdb_resale_prices
    properties:
      # Column-level SQL check
      - name: resale_price
        quality:
          - type: sql
            name: "resale_price_positive"
            query: "SELECT COUNT(*) FROM hdb_resale_prices WHERE resale_price <= 0"
            mustBe: 0

      - name: flat_type
        quality:
          # Library metric check (SQL auto-generated)
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

    # Table-level checks
    quality:
      - type: sql
        name: "no_null_resale_prices"
        query: "SELECT COUNT(*) FROM hdb_resale_prices WHERE resale_price IS NULL"
        mustBe: 0

      - type: library
        metric: rowCount
        mustBeGreaterThan: 0
        dimension: completeness
```

## Automatic Check References

When a contract is loaded, Vowl automatically builds `CheckReference` objects for every executable check in the contract via `Contract.get_check_references_by_schema()`.

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

## Auto-Generated Checks

| Generated from | What Vowl validates |
|----------------|----------------------|
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

In practice, a property like this:

```yaml
- name: block
    logicalType: string
    logicalTypeOptions:
        maxLength: 10
    required: true
```

produces three generated check references:

| Check path | Check type |
|---|---|
| `$.schema[0].properties[...]` | `DeclaredColumnExistsCheckReference` |
| `$.schema[0].properties[...].logicalTypeOptions.maxLength` | `LogicalTypeOptionsCheckReference` |
| `$.schema[0].properties[...].required` | `RequiredCheckReference` |

!!! note
    Because `string` does not currently generate a SQL cast-based type check, the `logicalType` entry above contributes metadata for option checks rather than a standalone type-validation query. If you use `integer`, `number`, `boolean`, `date`, `timestamp`, or `time`, Vowl also generates a `logicalType` SQL check automatically.

## Library Metrics (`type: library`)

Instead of writing SQL by hand, you can declare common data quality metrics using `type: library` in your `quality` blocks. Vowl auto-generates the appropriate SQL at runtime.

### Column-Level Metrics

Under a property's `quality`:

| `metric` | What it checks | Arguments |
|----------|---------------|-----------|
| `nullValues` | Count of `NULL` values in the column | — |
| `missingValues` | Count of values matching a configurable missing-values list | `arguments.missingValues`: list of sentinel values (use `null` for SQL NULL) |
| `invalidValues` | Count of values that fail valid-value or pattern criteria | `arguments.validValues`: allowed values list and/or `arguments.pattern`: regex |
| `duplicateValues` | Count of duplicate non-NULL values in the column | — |

### Table-Level Metrics

Under a schema's `quality`:

| `metric` | What it checks | Arguments |
|----------|---------------|-----------|
| `rowCount` | Total number of rows in the table | — |
| `duplicateValues` | Count of duplicate rows across specified columns | `arguments.properties`: list of column names to check |

All library metrics support `unit: "percent"` to return the result as a percentage of total rows instead of an absolute count. They also accept any of the standard check operators (`mustBe`, `mustBeGreaterThan`, etc.).

### Example

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
