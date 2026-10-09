---
description: Learn how to define data quality rules in declarative YAML using the Open Data Contract Standard (ODCS) with vowl.
---

# Data Quality with Data Contracts

## The Core Concept

Instead of writing validation logic in Python, you declare it in a YAML file following the [Open Data Contract Standard (ODCS)](https://github.com/bitol-io/open-data-contract-standard). This keeps your rules apart from your code, so they are easier to manage, version and share.

A contract has one entry under `schema` for each table. Each **schema** lists its columns under `properties`. Checks come from two places:

- **Checks you write**, in a [`quality` block](#data-quality), either as SQL (`type: sql`) or as [library checks](#library-checks) (`type: library`), where vowl writes the SQL for you.
- **[Generated checks](#auto-generated-checks)**, which vowl builds from the column details you declare, such as `logicalType`, `required` or `unique`.

**Example `hdb_resale_simple.yaml`**:

```yaml
kind: DataContract
apiVersion: v3.2.0
version: 1.0.0
id: c11443ee-542f-4442-b28d-2d224342be37
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
            description: Based on ISO 8601 | YYYY-MM
            mustBe: 0
            query: |-
              SELECT COUNT(*)
              FROM "hdb_resale_prices"
              WHERE CAST(month AS TEXT) !~ '^[0-9]{4}-(0[1-9]|1[0-2])$';
            dimension: conformity

      # Generated check: allowed values from enum
      - name: flat_type
        enum:
          - value: 1 ROOM
          - value: 2 ROOM
          - value: 3 ROOM
          - value: 4 ROOM
          - value: 5 ROOM
          - value: EXECUTIVE
          - value: MULTI-GENERATION

      # SQL check: business rule
      - name: floor_area_sqm
        quality:
          - name: floor_area_must_be_less_than_200
            type: sql
            query: SELECT COUNT(*) FROM "hdb_resale_prices" WHERE floor_area_sqm >= 200
            mustBe: 0
            dimension: consistency

      # SQL check: resale price cap
      - name: resale_price
        quality:
          - name: resale_price_must_not_exceed_2m
            type: sql
            query: SELECT COUNT(*) FROM "hdb_resale_prices" WHERE resale_price > 2000000
            mustBe: 0
            dimension: consistency
```

## Data Quality

A `quality` block is a list of checks. Where you put it decides what the check is about:

- Under a **property**, the check is about that column.
- Under the **schema**, the check is about the whole table.

```yaml
schema:
  - name: hdb_resale_prices
    properties:
      - name: resale_price
        quality: # column-level checks
          - name: resale_price_must_be_positive
            type: sql
            description: Resale price must be above zero
            dimension: conformity
            query: SELECT COUNT(*) FROM "hdb_resale_prices" WHERE resale_price <= 0
            mustBe: 0
    quality: # table-level checks
      - name: at_least_one_row
        type: library
        metric: rowCount
        mustBeGreaterThan: 0
        dimension: completeness
```

### Fields of a check

| Field         | What it does                                                                                                                                      |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`        | `sql` runs your `query`. `library` runs a built-in [library check](#library-checks) named by `metric`. See [Check types](#check-types).           |
| `name`        | The check's name in every result. If you leave it out, vowl names it `<column or schema>_<metric or dimension>`.                                  |
| `query`       | For `type: sql`. A query that returns one number, usually `SELECT COUNT(*) ... WHERE <rows that break the rule>`.                                 |
| `metric`      | For `type: library`. Which library check to run, such as `nullValues` or `rowCount`.                                                              |
| `dimension`   | The group the check belongs to in the results: `accuracy`, `completeness`, `conformity`, `consistency`, `coverage`, `timeliness` or `uniqueness`. |
| `description` | Free text, shown next to failed checks in the report.                                                                                             |
| `unit`        | `percent` turns a library check's count into a percentage of all rows. `rowCount` ignores it.                                                     |
| An operator   | What the number must be for the check to pass. See below.                                                                                         |

Check names must be unique within one `quality` list. Two checks with the same name on one column, or both on the schema, make `Contract.load` raise a `ValueError`. The same name on different columns is fine.

### Check types

| `type`    | What vowl does                                                                                                                                   |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `sql`     | Runs `query`.                                                                                                                                    |
| `library` | Runs the [library check](#library-checks) named by `metric`. A check with a `metric` and no `type` is a library check.                           |
| `custom`  | Sends the check, with its `engine` and `implementation`, to the executor registered for that `engine`. With no such executor it ends as `ERROR`. |
| `text`    | A description for people. vowl cannot run it, so it always ends as `ERROR`.                                                                      |

Under `apiVersion` v3.1.0 and v3.2.0 the ODCS schema needs `type: sql` on a `query` check, so leave `type` out only on library checks.

### Operators

Each check takes one operator. vowl compares the number the check returns with it.

| Operator                 | Passes when the number is       | Example                        |
| ------------------------ | ------------------------------- | ------------------------------ |
| `mustBe`                 | equal to the value              | `mustBe: 0`                    |
| `mustNotBe`              | not equal to the value          | `mustNotBe: 0`                 |
| `mustBeGreaterThan`      | greater than the value          | `mustBeGreaterThan: 0`         |
| `mustBeGreaterOrEqualTo` | greater than or equal to it     | `mustBeGreaterOrEqualTo: 100`  |
| `mustBeLessThan`         | less than the value             | `mustBeLessThan: 10`           |
| `mustBeLessOrEqualTo`    | less than or equal to it        | `mustBeLessOrEqualTo: 10`      |
| `mustBeBetween`          | inside the range, ends included | `mustBeBetween: [0, 30000000]` |
| `mustNotBeBetween`       | outside the range               | `mustNotBeBetween: [1, 5]`     |

### Writing the SQL

Write `query` in PostgreSQL syntax. vowl translates it with [SQLGlot](https://github.com/tobymao/sqlglot) to the SQL of whichever data source the check runs on, so one contract works on DuckDB, Spark, Snowflake and the rest. Refer to the table by its schema `name`.

A query that counts rows, such as `SELECT COUNT(*) ... WHERE ...`, also lets vowl fetch the rows that failed and show them in the report. A query that returns another kind of number, such as an average, still passes or fails but has no failed rows to show.

Under `apiVersion` v3.1.0 and v3.2.0, an unknown `type` or `metric` fails the ODCS schema, so `Contract.load` raises a `ValidationError` before any check runs. Some checks load but cannot run, and end as `ERROR` instead of being skipped: `type: text`, a metric at the wrong level (such as `rowCount` under a property), a v3.0.2 check that names a `rule:` in place of a `metric:`, with or without `type: library`, and a `custom` check with no executor for its `engine`. A SQL check whose `query` has a syntax error also ends as `ERROR`, and the other checks still run.

## Auto-generated Checks

vowl writes the SQL for these checks. Most come from the column details in your contract, and you don't write them at all. [Library checks](#library-checks) are ones you declare by name.

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
| `logicalTypeOptions.minItems`         | Array (`logicalType: array`) contains at least the configured number of items (see [Array Formats](#array-formats) below)                         |
| `logicalTypeOptions.maxItems`         | Array contains at most the configured number of items                                                                                             |
| `logicalTypeOptions.uniqueItems`      | Array (`uniqueItems: true`) contains no duplicate items                                                                                           |
| `items.logicalType`                   | Every element of an array casts to the declared element type                                                                                      |
| `items.logicalTypeOptions.*`          | Every element satisfies the element option (`minLength`, `maxLength`, `pattern`, numeric bounds, `format`)                                        |
| `items.enum`                          | Every element is within the declared allowed set                                                                                                  |
| `enum`                                | Non-null values are within the declared allowed set (`enum` value list)                                                                           |
| `required: true`                      | Column contains no `NULL` values                                                                                                                  |
| `unique: true`                        | Non-null values are unique                                                                                                                        |
| `primaryKey: true`                    | Values are unique and non-null. Two or more key columns form one key, see [Composite Primary Keys](#composite-primary-keys)                       |
| `relationships` (`foreignKey`)        | Every non-null key value exists in the referenced target (referential integrity). See [Relationships (Foreign Keys)](#relationships-foreign-keys) |

In practice, a property like this:

```yaml
- name: block
  logicalType: string
  logicalTypeOptions:
    maxLength: 10
  required: true
```

produces four generated checks. Each one counts the rows that break it and must return `0`:

```sql
-- block_column_exists_check: errors if the column is missing, reads no rows
SELECT COUNT(*) FROM (SELECT "block" FROM "hdb_resale_prices" LIMIT 0) AS _vowl_column_exists;

-- block_logical_type_check
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "block" IS NULL AND TRY_CAST("block" AS TEXT) IS NULL;

-- block_logical_type_options_maxLength_check
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "block" IS NULL AND LENGTH(TRY_CAST("block" AS TEXT)) > 10;

-- block_required_check
SELECT COUNT(*) FROM "hdb_resale_prices" WHERE "block" IS NULL;
```

The SQL on this page is what vowl writes for DuckDB. On other data sources vowl writes the same query in that source's SQL.

!!! note

    Any value can be read as a string, so the logical type check for `logicalType: string` always passes. It is still listed in the results. For `integer`, `number`, `boolean`, `date`, `timestamp` and `time`, the check fails every value that cannot be converted to that type. Other logical types, such as `object`, get no type check, and vowl emits a `UserWarning`.

### Column Details

Every check below skips `NULL` values, except `required`. Use `required: true` to forbid `NULL`.

=== "logicalType"

    ```yaml
    - name: price
      logicalType: number
    - name: age
      logicalType: integer
    ```

    ```sql
    -- price_logical_type_check
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "price" IS NULL AND TRY_CAST("price" AS DOUBLE) IS NULL;

    -- age_logical_type_check: a number with a fraction is not an integer
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "age" IS NULL
      AND (TRY_CAST("age" AS DOUBLE) IS NULL
           OR TRY_CAST("age" AS DOUBLE) <> TRY_CAST("age" AS BIGINT));
    ```

    `boolean`, `date`, `timestamp` and `time` cast to `BOOLEAN`, `DATE`, `TIMESTAMP` and `TIME` in the same way.

=== "Length and pattern"

    ```yaml
    - name: code
      logicalType: string
      logicalTypeOptions:
        minLength: 2
        maxLength: 10
        pattern: "^[A-Z]+$"
    ```

    ```sql
    -- code_logical_type_options_minLength_check
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "code" IS NULL AND LENGTH(TRY_CAST("code" AS TEXT)) < 2;

    -- code_logical_type_options_maxLength_check
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "code" IS NULL AND LENGTH(TRY_CAST("code" AS TEXT)) > 10;

    -- code_logical_type_options_pattern_check
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "code" IS NULL AND NOT REGEXP_MATCHES(TRY_CAST("code" AS TEXT), '^[A-Z]+$');
    ```

=== "Numeric bounds"

    ```yaml
    - name: price
      logicalType: number
      logicalTypeOptions:
        minimum: 0            # or exclusiveMinimum: 0
        maximum: 2000000      # or exclusiveMaximum: 2000000
        multipleOf: 0.01
    ```

    ```sql
    -- price_logical_type_options_minimum_check
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "price" IS NULL AND TRY_CAST("price" AS DOUBLE) < 0;

    -- price_logical_type_options_maximum_check
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "price" IS NULL AND TRY_CAST("price" AS DOUBLE) > 2000000;

    -- price_logical_type_options_exclusiveMinimum_check
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "price" IS NULL AND TRY_CAST("price" AS DOUBLE) <= 0;

    -- price_logical_type_options_exclusiveMaximum_check
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "price" IS NULL AND TRY_CAST("price" AS DOUBLE) >= 2000000;

    -- price_logical_type_options_multipleOf_check
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "price" IS NULL AND (TRY_CAST("price" AS DOUBLE) % 0.01) <> 0;
    ```

=== "enum"

    ```yaml
    - name: status
      logicalType: string
      enum:
        - value: open
        - value: closed
    ```

    ```sql
    -- status_enum_check
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "status" IS NULL AND NOT "status" IN ('open', 'closed');
    ```

=== "required and unique"

    ```yaml
    - name: code
      required: true
      unique: true
    ```

    ```sql
    -- code_required_check
    SELECT COUNT(*) FROM "hdb_resale_prices" WHERE "code" IS NULL;

    -- code_unique_check: counts every copy of a repeated value
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE "code" IN (
      SELECT "code" FROM "hdb_resale_prices"
      WHERE NOT "code" IS NULL
      GROUP BY "code" HAVING COUNT(*) > 1
    );
    ```

### Format Checks

The `logicalTypeOptions.format` key validates that column values conform to a declared format. The check generated depends on the column's `logicalType`:

#### Integer formats

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

`i128` and `u128` get no range check, because their ranges exceed what SQL engines can represent. They end as an `ERROR` result named `<col>_check` that says so. An unknown integer format does the same.

```yaml
- name: age
  logicalType: integer
  logicalTypeOptions:
    format: u8 # 0 – 255
```

```sql
-- age_logical_type_options_format_check
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "age" IS NULL
  AND (TRY_CAST("age" AS DOUBLE) < 0 OR TRY_CAST("age" AS DOUBLE) > 255);
```

#### String formats

Validates values against a built-in regex pattern.

| `format`   | What it checks                                                 |
| ---------- | -------------------------------------------------------------- |
| `uuid`     | UUID v1–v5 hex format (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) |
| `email`    | Basic `local@domain.tld` structure                             |
| `ipv4`     | Dotted-decimal IPv4 address (`0.0.0.0` – `255.255.255.255`)    |
| `ipv6`     | Full-form colon-separated IPv6 address                         |
| `hostname` | RFC-952 hostname with TLD                                      |
| `uri`      | URI with a valid scheme prefix (e.g. `https:`, `s3:`)          |

A `format` that is not in this table is read as a Java date pattern, as for [date formats](#date-timestamp-and-time-formats), and checked the same way. `password`, `byte` and `binary` cannot be validated against data. They, and a format vowl cannot read, end as an `ERROR` result named `<col>_check` that explains why.

```yaml
- name: request_id
  logicalType: string
  logicalTypeOptions:
    format: uuid
```

```sql
-- request_id_logical_type_options_format_check
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "request_id" IS NULL
  AND NOT REGEXP_MATCHES(
    TRY_CAST("request_id" AS TEXT),
    '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
  );
```

#### Number formats

`f32` and `f64` produce no check, because SQL engines do not tell them apart when they read the data. They end as an `ERROR` result named `<col>_check` that says so.

#### Date, timestamp and time formats

For `date`, `timestamp` and `time` logical types, `format` takes a pattern such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss`. These are Java date patterns ([`DateTimeFormatter`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html)): `yyyy` is a four-digit year, `MM` a two-digit month, and so on. vowl turns the pattern into a regex and checks that each value, read as text, matches it.

Supported tokens include `yyyy`, `yy`, `MM`, `M`, `dd`, `d`, `HH`, `H`, `hh`, `h`, `mm`, `ss`, `SSS` (fractional seconds), and timezone offsets (`X`/`XX`/`XXX`/`Z`). Literal characters such as `-`, `:`, `T`, and quoted sections (`'T'`) are preserved. If a pattern contains tokens vowl cannot translate, vowl emits a warning and the check ends as an `ERROR` result named `<col>_check`.

```yaml
- name: created_at
  logicalType: timestamp
  logicalTypeOptions:
    format: "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
```

```sql
-- created_at_logical_type_options_format_check
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "created_at" IS NULL
  AND NOT REGEXP_MATCHES(
    TRY_CAST("created_at" AS TEXT),
    '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\d\.\d{3}(Z|[+-]\d{2}:\d{2})$'
  );
```

#### Array Formats

When a property declares `logicalType: array`, vowl checks the array's size, and it uses the `items` sub-schema to check the elements inside it. These checks run only when `logicalType: array` is set. Under `apiVersion` v3.1.0 and v3.2.0, the ODCS schema rejects `minItems`, `maxItems` and `uniqueItems` on other logical types when the contract loads. An `items` block on a property with no `logicalType` loads, and ends as an `ERROR` result.

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
| `logicalTypeOptions.uniqueItems`     | Array has no duplicate elements (`uniqueItems: true`, `false` is `ERROR`) |
| `items.logicalType`                  | Every element casts to the element type                                   |
| `items.logicalTypeOptions.minLength` | Every element satisfies the element option                                |
| `items.enum`                         | Every element is one of the allowed values                                |

```sql
-- tags_logical_type_options_minItems_check
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "tags" IS NULL AND ARRAY_LENGTH("tags") < 1;

-- tags_logical_type_options_maxItems_check
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "tags" IS NULL AND ARRAY_LENGTH("tags") > 10;

-- tags_logical_type_options_uniqueItems_check
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "tags" IS NULL AND ARRAY_LENGTH(LIST_DISTINCT("tags")) <> ARRAY_LENGTH("tags");

-- tags_array_items_logical_type_check
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "tags" IS NULL AND EXISTS(
  SELECT 1 FROM UNNEST("tags") AS "_vowl_arr"("_vowl_elem")
  WHERE TRY_CAST("_vowl_elem" AS TEXT) IS NULL
);

-- tags_array_items_minLength_check
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "tags" IS NULL AND EXISTS(
  SELECT 1 FROM UNNEST("tags") AS "_vowl_arr"("_vowl_elem")
  WHERE LENGTH(TRY_CAST("_vowl_elem" AS TEXT)) < 2
);

-- tags_array_items_enum_check
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "tags" IS NULL AND EXISTS(
  SELECT 1 FROM UNNEST("tags") AS "_vowl_arr"("_vowl_elem")
  WHERE NOT "_vowl_elem" IN ('red', 'green', 'blue')
);
```

**NULL vs empty.** A `NULL` array is skipped by every array check (use `required` to forbid NULLs). An empty array `[]` only fails `minItems`. With no elements, `uniqueItems` and the `items` checks have nothing to flag, so they pass.

**Backend support.** These checks are tested on DuckDB. Size checks (`minItems`/`maxItems`) use `ARRAY_LENGTH`, which most engines with arrays support. `uniqueItems` and element checks use `ARRAY_DISTINCT` and `UNNEST`, which not every engine has. For other engines the behaviour is worked out from the SQL vowl writes, not tested, so see [Known Issues](known-issues.md#native-array-checks) for the per-engine expectations. Where a construct isn't supported, the check returns `ERROR` rather than silently passing.

### Primary Keys

`primaryKey: true` on one column generates `<col>_primary_key_check`, which fails every row whose key is `NULL` or appears more than once.

```yaml
- name: customers
  properties:
    - name: customer_id
      logicalType: integer
      primaryKey: true
```

```sql
-- customer_id_primary_key_check
SELECT COUNT(*) FROM "customers"
WHERE "customer_id" IS NULL
   OR "customer_id" IN (
     SELECT "customer_id" FROM "customers"
     WHERE NOT "customer_id" IS NULL
     GROUP BY "customer_id" HAVING COUNT(*) > 1
   );
```

#### Composite Primary Keys

When two or more properties of a schema set `primaryKey: true`, ODCS reads them together as one key, ordered by `primaryKeyPosition` (starting from 1). vowl then generates a single check for the whole key instead of one per column. It is named `<schema>_<col1>_<col2>_primary_key_check` and fails every row that has a `NULL` in any key column or whose key tuple appears more than once. One key column repeating on its own is fine.

```yaml
- name: products
  properties:
    - name: category
      primaryKey: true
      primaryKeyPosition: 1
    - name: sku
      primaryKey: true
      primaryKeyPosition: 2
```

This generates `products_category_sku_primary_key_check`:

```sql
SELECT COUNT(*) FROM "products"
WHERE "category" IS NULL
   OR "sku" IS NULL
   OR ("category", "sku") IN (
     SELECT "category", "sku" FROM "products"
     WHERE NOT "category" IS NULL AND NOT "sku" IS NULL
     GROUP BY "category", "sku" HAVING COUNT(*) > 1
   );
```

Columns without a `primaryKeyPosition` follow the positioned ones in the order they are declared. A schema with a single key column keeps the per-column `<col>_primary_key_check`. A column that also sets `unique: true` still gets its own unique check.

#### Relationships (Foreign Keys)

A `relationships` entry of type `foreignKey` requires every non-`NULL` key value to exist in the table it points to. vowl runs this as a real check (`dimension: consistency`, `mustBe: 0`).

##### Property-level, single column

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

```sql
-- orders_customer_id_foreign_key_check
SELECT COUNT(*) FROM "orders" AS "_vowl_fk_from"
WHERE NOT "_vowl_fk_from"."customer_id" IS NULL
  AND NOT EXISTS(
    SELECT 1 FROM "customers" AS "_vowl_fk_to"
    WHERE "_vowl_fk_to"."customer_id" = "_vowl_fk_from"."customer_id"
  );
```

When the two tables are on different data sources, vowl runs this query on copies of both tables. See [Using Relationship References](design-considerations/cross-table/relationship-references.md).

##### Schema-level, composite key

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

```sql
-- order_items_category_sku_foreign_key_check
SELECT COUNT(*) FROM "order_items" AS "_vowl_fk_from"
WHERE NOT "_vowl_fk_from"."category" IS NULL
  AND NOT "_vowl_fk_from"."sku" IS NULL
  AND NOT EXISTS(
    SELECT 1 FROM "products" AS "_vowl_fk_to"
    WHERE "_vowl_fk_to"."category" = "_vowl_fk_from"."category"
      AND "_vowl_fk_to"."sku" = "_vowl_fk_from"."sku"
  );
```

The two `primaryKey` columns of `products` form one composite key, so vowl checks that each `(category, sku)` pair is unique, not each column. See [Composite Primary Keys](#composite-primary-keys).

##### Reference notations

| Notation        | Example                                                     | Resolves by                         |
| --------------- | ----------------------------------------------------------- | ----------------------------------- |
| Shorthand       | `customers.customer_id`                                     | property `name`                     |
| Fully-qualified | `/schema/customers_schema/properties/customer_id`           | property `id`                       |
| External file   | `customers.yaml#/schema/<schemaId>/properties/<propertyId>` | another contract file, then by `id` |

Give one adapter per schema, keyed by schema `name`, including the target of
an external reference. [Using Relationship References](design-considerations/cross-table/relationship-references.md)
explains how vowl finds each target, how to point at another contract file,
and where the check runs when the two schemas are on different data sources.

### Library Checks

Instead of writing SQL by hand, you can declare common checks with `type: library` in your `quality` blocks. vowl writes the SQL for you when the check runs.

#### Column-Level Checks

Under a property's `quality`:

| `metric`          | What it checks                                              | Arguments                                                                                                |
| ----------------- | ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `nullValues`      | Count of `NULL` values in the column                        | -                                                                                                        |
| `missingValues`   | Count of values matching a configurable missing-values list | `arguments.missingValues`: values that mean "missing", such as `""` or `"N/A"` (use `null` for SQL NULL) |
| `invalidValues`   | Count of values that fail valid-value or pattern criteria   | `arguments.validValues`: allowed values list and/or `arguments.pattern`: regex                           |
| `duplicateValues` | Count of duplicate non-NULL values in the column            | -                                                                                                        |

#### Table-Level Checks

Under a schema's `quality`:

| `metric`          | What it checks                                   | Arguments                                             |
| ----------------- | ------------------------------------------------ | ----------------------------------------------------- |
| `rowCount`        | Total number of rows in the table                | -                                                     |
| `duplicateValues` | Count of duplicate rows across specified columns | `arguments.properties`: list of column names to check |

All library checks except `rowCount` support `unit: "percent"` to return the result as a percentage of total rows instead of an absolute count. `rowCount` ignores it. They also accept any of the standard check operators (`mustBe`, `mustBeGreaterThan`, etc.).

#### Example

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

#### Generated SQL

The table name is the schema `name`, here `hdb_resale_prices`.

=== "nullValues"

    ```sql
    -- town_nullValues
    SELECT COUNT(*) FROM "hdb_resale_prices" WHERE "town" IS NULL;
    ```

=== "missingValues"

    With `arguments.missingValues: ["", "N/A", null]`:

    ```sql
    -- town_missingValues
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE "town" IS NULL OR TRY_CAST("town" AS TEXT) IN ('', 'N/A');
    ```

=== "invalidValues"

    With `arguments.validValues`:

    ```sql
    -- flat_type_invalidValues
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "flat_type" IS NULL
      AND NOT TRY_CAST("flat_type" AS TEXT) IN ('3 ROOM', '4 ROOM', '5 ROOM', 'EXECUTIVE');
    ```

    With `arguments.pattern: "^[0-9] ROOM$"`:

    ```sql
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE NOT "flat_type" IS NULL
      AND NOT REGEXP_MATCHES(TRY_CAST("flat_type" AS TEXT), '^[0-9] ROOM$');
    ```

=== "duplicateValues (column)"

    ```sql
    -- town_duplicateValues: counts every copy of a repeated value
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE "town" IN (
      SELECT "town" FROM "hdb_resale_prices"
      WHERE NOT "town" IS NULL
      GROUP BY "town" HAVING COUNT(*) > 1
    );
    ```

=== "rowCount"

    ```sql
    -- hdb_resale_prices_rowCount
    SELECT COUNT(*) FROM "hdb_resale_prices";
    ```

=== "duplicateValues (table)"

    With `arguments.properties: [month, block, street_name]`:

    ```sql
    -- hdb_resale_prices_duplicateValues
    SELECT COUNT(*) FROM "hdb_resale_prices"
    WHERE EXISTS(
      SELECT 1 FROM "hdb_resale_prices" AS "dup_src"
      WHERE "dup_src"."month" = "hdb_resale_prices"."month"
        AND NOT "dup_src"."month" IS NULL
        AND "dup_src"."block" = "hdb_resale_prices"."block"
        AND NOT "dup_src"."block" IS NULL
        AND "dup_src"."street_name" = "hdb_resale_prices"."street_name"
        AND NOT "dup_src"."street_name" IS NULL
      GROUP BY "dup_src"."month", "dup_src"."block", "dup_src"."street_name"
      HAVING COUNT(*) > 1
    );
    ```

=== "unit: percent"

    `unit: percent` divides the count by the number of rows:

    ```sql
    SELECT
      (SELECT COUNT(*) FROM "hdb_resale_prices" WHERE "town" IS NULL) * 100.0
      / NULLIF((SELECT COUNT(*) FROM "hdb_resale_prices"), 0);
    ```

### How vowl Derives Auto Checks

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
| Composite primary key check  | `primaryKey: true` on two or more properties   | `$.schema[N].primaryKey`                                   |
| Property foreign-key check   | `relationships` entry on a property            | `$.schema[N].properties[M].relationships[K]`               |
| Schema foreign-key check     | `relationships` entry on a schema              | `$.schema[N].relationships[K]`                             |
