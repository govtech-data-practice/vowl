---
description: Learn how to define data quality rules in declarative YAML using the Open Data Contract Standard (ODCS) with vowl.
---

# Writing Data Contracts

## The Core Concept

Instead of writing validation logic in Python, you declare it in a YAML file following the [Open Data Contract Standard (ODCS)](https://github.com/bitol-io/open-data-contract-standard). This keeps your rules apart from your code, so they are easier to manage, version and share.

A contract has one entry under `schema` for each table. Each **schema** lists its columns under `properties`. Checks come from two places:

- **Checks you write**, under `quality`, either as SQL (`type: sql`) or as [library checks](#library-checks-type-library) (`type: library`), where vowl writes the SQL for you.
- **[Generated checks](#generated-checks)**, which vowl builds from the column details you declare, such as `logicalType`, `required` or `unique`.

A check under a property's `quality` is about that column. A check under the schema's `quality` is about the whole table. Give each check a `dimension` (such as `completeness` or `conformity`) to group it in the results.

**Example `hdb_resale_simple.yaml`** (trimmed for readability):

```yaml
kind: DataContract
apiVersion: v3.1.0
name: HDB Resale Flat Prices
schema:
  - name: hdb_resale_prices
    properties:
      # SQL check: regex-based format validation
      - name: month
        logicalType: string
        quality:
          - type: sql
            name: Month
            description: Based on ISO 8601, assumed to be in UTC +8 | YYYY-MM
            mustBe: 0
            query: |-
              SELECT COUNT(*)
              FROM "hdb_resale_prices"
              WHERE CAST(month AS TEXT) !~ '^[0-9]{4}-(0[1-9]|1[0-2])$';
            dimension: conformity

      # Library check: null values
      - name: town
        quality:
          - type: library
            metric: nullValues
            mustBe: 0
            dimension: completeness

      # Library check: allowed values
      - name: flat_type
        quality:
          - type: library
            metric: invalidValues
            mustBe: 0
            dimension: conformity
            arguments:
              validValues:
                - 1 ROOM
                - 2 ROOM
                - 3 ROOM
                - 4 ROOM
                - 5 ROOM
                - EXECUTIVE
                - MULTI-GENERATION

      # SQL check: business rule
      - name: floor_area_sqm
        quality:
          - name: floor_area_must_be_less_than_200
            description: Validates that floor area must be less than 200
            type: sql
            dimension: consistency
            query: SELECT COUNT(*) FROM "hdb_resale_prices" WHERE floor_area_sqm >= 200
            mustBe: 0

      # SQL check: resale price cap
      - name: resale_price
        quality:
          - name: resale_price_must_not_exceed_2m
            description: Resale price must not be more than 2 million SGD
            type: sql
            dimension: conformity
            query: >-
              SELECT COUNT(*) FROM "hdb_resale_prices" WHERE resale_price > 2000000
            mustBe: 0

    # Table-level library check
    quality:
      - type: library
        metric: rowCount
        mustBeBetween:
          - 0
          - 30000000
        dimension: completeness
```

## Generated Checks

vowl builds these checks from the column details in your contract. You don't write them.

| Generated from                        | What vowl validates                                                                                                                               |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`                                | Column declared in the contract exists in the source table                                                                                        |
| `logicalType`                         | Values can be cast to the declared SQL type for `integer`, `number`, `boolean`, `date`, `timestamp`, and `time`                                   |
| `logicalTypeOptions.minLength`        | String length is at least the configured minimum                                                                                                  |
| `logicalTypeOptions.maxLength`        | String length does not exceed the configured maximum                                                                                              |
| `logicalTypeOptions.pattern`          | String values match the configured regex pattern                                                                                                  |
| `logicalTypeOptions.minimum`          | Value is greater than or equal to the configured minimum                                                                                          |
| `logicalTypeOptions.maximum`          | Value is less than or equal to the configured maximum                                                                                             |
| `logicalTypeOptions.exclusiveMinimum` | Value is strictly greater than the configured minimum                                                                                             |
| `logicalTypeOptions.exclusiveMaximum` | Value is strictly less than the configured maximum                                                                                                |
| `logicalTypeOptions.multipleOf`       | Value is a multiple of the configured number                                                                                                      |
| `logicalTypeOptions.format`           | Value satisfies the declared format (see [Format Checks](#format-checks) below)                                                                   |
| `logicalTypeOptions.minItems`         | Array (`logicalType: array`) contains at least the configured number of items (see [Array Checks](#array-checks) below)                           |
| `logicalTypeOptions.maxItems`         | Array contains at most the configured number of items                                                                                             |
| `logicalTypeOptions.uniqueItems`      | Array (`uniqueItems: true`) contains no duplicate items                                                                                           |
| `items.logicalType`                   | Every element of an array casts to the declared element type                                                                                      |
| `items.logicalTypeOptions.*`          | Every element satisfies the element option (`minLength`, `maxLength`, `pattern`, numeric bounds, `format`)                                        |
| `items.enum`                          | Every element is within the declared allowed set                                                                                                  |
| `enum`                                | Non-null values are within the declared allowed set (`enum` value list)                                                                           |
| `required: true`                      | Column contains no `NULL` values                                                                                                                  |
| `unique: true`                        | Non-null values are unique                                                                                                                        |
| `primaryKey: true`                    | Values are both unique and non-null                                                                                                               |
| `relationships` (`foreignKey`)        | Every non-null key value exists in the referenced target (referential integrity). See [Relationships (Foreign Keys)](#relationships-foreign-keys) |

In practice, a property like this:

```yaml
- name: block
  logicalType: string
  logicalTypeOptions:
    maxLength: 10
  required: true
```

produces three generated checks: the column exists, no value is longer than 10 characters, and no value is `NULL`.

!!! note
    `logicalType: string` makes no check of its own, because any value can be read as a string. It only tells vowl which `logicalTypeOptions` apply. For `integer`, `number`, `boolean`, `date`, `timestamp` and `time`, vowl also checks that every value can be converted to that type.

## Library Checks (`type: library`)

Instead of writing SQL by hand, you can declare common checks with `type: library` in your `quality` blocks. vowl writes the SQL for you when the check runs.

### Column-Level Checks

Under a property's `quality`:

| `metric`          | What it checks                                              | Arguments                                                                      |
| ----------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `nullValues`      | Count of `NULL` values in the column                        | -                                                                              |
| `missingValues`   | Count of values matching a configurable missing-values list | `arguments.missingValues`: values that mean "missing", such as `""` or `"N/A"` (use `null` for SQL NULL) |
| `invalidValues`   | Count of values that fail valid-value or pattern criteria   | `arguments.validValues`: allowed values list and/or `arguments.pattern`: regex |
| `duplicateValues` | Count of duplicate non-NULL values in the column            | -                                                                              |

### Table-Level Checks

Under a schema's `quality`:

| `metric`          | What it checks                                   | Arguments                                             |
| ----------------- | ------------------------------------------------ | ----------------------------------------------------- |
| `rowCount`        | Total number of rows in the table                | -                                                     |
| `duplicateValues` | Count of duplicate rows across specified columns | `arguments.properties`: list of column names to check |

All library checks support `unit: "percent"` to return the result as a percentage of total rows instead of an absolute count. They also accept any of the standard check operators (`mustBe`, `mustBeGreaterThan`, etc.).

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
## Relationships (Foreign Keys)

A `relationships` entry of type `foreignKey` requires every non-`NULL` key value to exist in the table it points to. vowl runs this as a real check (`dimension: consistency`, `mustBe: 0`).

### Property-level, single column

Declare the relationship on the property that holds the key. The `to` target uses **shorthand** notation (`<object>.<property>`), resolved by property `name`:

```yaml
schema:
  - name: customers
    properties:
      - name: customer_id
        logicalType: integer
        primaryKey: true
  - name: orders
    properties:
      - name: customer_id
        logicalType: integer
        required: false
        relationships:
          - type: foreignKey
            to: customers.customer_id
```

This generates a check named `orders_customer_id_foreign_key_check`. Rows with a `NULL` key are skipped, so an optional foreign key is allowed. A value fails only when it is present but has no match in the target.

### Schema-level, composite key

For multi-column keys, declare the relationship at the **schema** level with parallel `from`/`to` lists. Here the target columns use **fully-qualified** notation (`/schema/<schemaId>/properties/<propertyId>`), resolved by `id`:

```yaml
schema:
  - id: products_schema
    name: products
    properties:
      - id: products_category
        name: category
        primaryKey: true
        primaryKeyPosition: 1
      - id: products_sku
        name: sku
        primaryKey: true
        primaryKeyPosition: 2
  - name: order_items
    properties:
      - name: category
      - name: sku
    relationships:
      - type: foreignKey
        from:
          - order_items.category
          - order_items.sku
        to:
          - /schema/products_schema/properties/products_category
          - /schema/products_schema/properties/products_sku
```

A composite row is skipped if **any** of its key columns is `NULL`.

### Reference notations

| Notation        | Example                                                     | Resolves by                         |
| --------------- | ----------------------------------------------------------- | ----------------------------------- |
| Shorthand       | `customers.customer_id`                                     | property `name`                     |
| Fully-qualified | `/schema/customers_schema/properties/customer_id`           | property `id`                       |
| External file   | `customers.yaml#/schema/<schemaId>/properties/<propertyId>` | another contract file, then by `id` |

An external reference points at a property in **another contract file**, written as `customers.yaml#/schema/<id>/properties/<id>`. Two things to know:

- The part after `#` must use the full `/schema/.../properties/...` form. Shorthand like `customers.customer_id` is fine for same-file references, but ODCS doesn't allow it after a `file.yaml#` prefix, so `customers.yaml#customers.customer_id` won't pass contract validation.
- vowl looks for the external file **relative to the contract that references it**. When you pass a file path or URL to `validate_data` (for example `validate_data(contract="orders.yaml", …)`), vowl resolves the external file next to `orders.yaml` for you. The one case that breaks is building a `Contract` object yourself from an in-memory dict. That object has no location, so vowl can't find the external file, and reports the check as unsupported.

### How relationship checks run

- **Adapters are keyed by schema `name`.** vowl does not connect using the contract's `servers` on its own. You give one adapter per schema. When the two sides are in different databases, vowl runs the check across them for you.
- **External references still need an adapter for the target schema.** Loading `customers.yaml` only tells vowl the target's schema `name` and column. It never opens a connection. Give the adapter under that schema's **`name`** as declared in the external file (not its `id`, and not the file name). vowl accepts it without the "no schema with that name" warning, even though the schema is not in the contract you loaded. Load the referencing contract from a path or URL, so vowl knows where to look for the external file:

  ```python
  validate_data(
      contract="orders.yaml",                      # loaded from a path -> has an origin
      adapters={
          "orders": IbisAdapter(orders_con),       # the referencing schema
          "customers": IbisAdapter(customers_con),  # the external target, keyed by its schema `name`
      },
  )
  ```

  See [`examples/2_multiple_sources`](https://github.com/govtech-data-practice/vowl/blob/main/examples/2_multiple_sources/multiple_sources.ipynb) for a runnable version.

- **Self-referential** keys (a table referencing itself) run as a single-table check.
- Failed rows hold only the referencing table's columns, so they are marked on that table's annotated output instead of becoming a residue.
- If the **reference itself** can't be resolved (missing target property, or an external path with no known contract location to resolve against), vowl logs a warning and reports the check as unsupported. The rest of the run goes ahead.
- If the reference resolves to an external schema but you **don't register an adapter** for it, vowl reads the target table through the referencing schema's adapter, as it does for any [table outside the contract](known-issues.md#queries-accessing-tables-outside-the-contract). The check runs if that connection has the table. Otherwise it comes back `ERROR` with the database's "table not found" message, and the rest of the run still executes.

See [Reference resolution for relationships](design-considerations.md#reference-resolution-for-relationships) for the exact rules vowl uses to find `relationships` targets.

## Format Checks

The `logicalTypeOptions.format` key validates that column values conform to a declared format. The check generated depends on the column's `logicalType`:

### Integer formats

Validates that values fall within the range of a fixed-width integer type.

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

```yaml
- name: age
  logicalType: integer
  logicalTypeOptions:
    format: u8 # 0 – 255
```

### String formats

Validates values against a built-in regex pattern.

| `format`   | What it checks                                                 |
| ---------- | -------------------------------------------------------------- |
| `uuid`     | UUID v1–v5 hex format (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) |
| `email`    | Basic `local@domain.tld` structure                             |
| `ipv4`     | Dotted-decimal IPv4 address (`0.0.0.0` – `255.255.255.255`)    |
| `ipv6`     | Full-form colon-separated IPv6 address                         |
| `hostname` | RFC-952 hostname with TLD                                      |
| `uri`      | URI with a valid scheme prefix (e.g. `https:`, `s3:`)          |

`password`, `byte`, and `binary` are recognised but skipped because they cannot be validated against data.

```yaml
- name: request_id
  logicalType: string
  logicalTypeOptions:
    format: uuid
```

### Number formats

`f32` and `f64` are recognised but produce no check. SQL engines do not tell them apart when they read the data.

### Date, timestamp and time formats

For `date`, `timestamp` and `time` logical types, `format` takes a pattern such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss`. These are Java date patterns ([`DateTimeFormatter`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html)): `yyyy` is a four-digit year, `MM` a two-digit month, and so on. vowl turns the pattern into a regex and checks that each value, read as text, matches it.

Supported tokens include `yyyy`, `yy`, `MM`, `M`, `dd`, `d`, `HH`, `H`, `hh`, `h`, `mm`, `ss`, `SSS` (fractional seconds), and timezone offsets (`X`/`XX`/`XXX`/`Z`). Literal characters such as `-`, `:`, `T`, and quoted sections (`'T'`) are preserved. If a pattern contains tokens vowl cannot translate, the check is skipped with a warning.

```yaml
- name: created_at
  logicalType: timestamp
  logicalTypeOptions:
    format: "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
```

## Array Checks

When a property declares `logicalType: array`, vowl checks the array's size, and it uses the `items` sub-schema to check the elements inside it. These checks run only when `logicalType: array` is set. On any other property, the same options or an `items` block are reported as unsupported checks.

```yaml
- name: tags
  logicalType: array
  logicalTypeOptions:
    minItems: 1
    maxItems: 10
    uniqueItems: true
  items:
    logicalType: string
    logicalTypeOptions:
      minLength: 2
    enum:
      - value: red
      - value: green
      - value: blue
```

This produces a column-exists check plus one check per array constraint:

| Constraint                           | What vowl validates                                                       |
| ------------------------------------ | ------------------------------------------------------------------------- |
| `logicalTypeOptions.minItems`        | Array has at least `minItems` elements                                    |
| `logicalTypeOptions.maxItems`        | Array has at most `maxItems` elements                                     |
| `logicalTypeOptions.uniqueItems`     | Array has no duplicate elements (`uniqueItems: true`, `false` is a no-op) |
| `items.logicalType`                  | Every element casts to the element type                                   |
| `items.logicalTypeOptions.minLength` | Every element satisfies the element option                                |
| `items.enum`                         | Every element is one of the allowed values                                |

**NULL vs empty.** A `NULL` array is skipped by every array check (use `required` to forbid NULLs). An empty array `[]` only fails `minItems`. With no elements, `uniqueItems` and the `items` checks have nothing to flag, so they pass.

**Backend support.** These checks are tested on DuckDB. Size checks (`minItems`/`maxItems`) use `ARRAY_LENGTH`, which most engines with arrays support. `uniqueItems` and element checks use `ARRAY_DISTINCT` and `UNNEST`, which not every engine has. For other engines the behaviour is worked out from the SQL vowl writes, not tested, so see [Known Issues](known-issues.md#native-array-checks) for the per-engine expectations. Where a construct isn't supported, the check returns `ERROR` rather than silently passing.

## How vowl Finds Each Check

This section is for readers working with vowl's Python objects. When a contract loads, vowl creates one `CheckReference` for every check, both the ones you wrote and the generated ones. `Contract.get_check_references_by_schema()` returns them grouped by schema. Generated checks run before the ones in `quality`.

Each reference records where in the contract its check came from, as a JSONPath:

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
| Property foreign-key check   | `relationships` entry on a property            | `$.schema[N].properties[M].relationships[K]`               |
| Schema foreign-key check     | `relationships` entry on a schema              | `$.schema[N].relationships[K]`                             |
