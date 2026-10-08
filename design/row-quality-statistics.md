---
title: "Design: Row-Quality Statistics"
description: >-
  Internal design record for computing exact failed-row counts, total-row counts
  and row pass rates per schema and per data-quality dimension, exporting the
  full table only where a check needs it, unless
  `row_counts="scalar"` is set.
status: Implemented
---

# Design: Row-Quality Statistics

!!! note "Superseded in part"

    `row_counts` and the `server_scalar` route were removed later. Attribution
    is now the only DQ-metrics mode, and it runs only when DQ metrics are
    asked for. See `design/two-tier-row-counts-plan.md`. The passages below
    that mention `row_counts="scalar"` or `"off"` are kept as history.

Internal design record for how vowl counts the rows with issues in each table,
in total and per data-quality dimension, and turns those counts into pass rates.
It replaces the earlier Python dedup over fetched failed rows. It also makes the
numbers agree with annotated output wherever both are exact.

The design is implemented in `src/vowl/validation/row_quality/`. See
[Implementation](#implementation) for the module layout and the few places where
the implementation differs from the text below.

## Terminology

This record uses the terms of the user docs
([Check Results](../docs/design-considerations/checks/check-results.md#failed-row-results)):

| Term                 | Meaning                                                                                                                                                                                                                                                                                                                                                          |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Failed check**     | A check with the status `FAILED`. Its rows are the rows that failed when it is a row-level check. The rows of passed checks are attributed only under `attribute_tolerated_rows=True`.                                                                                                                                                                           |
| **Query output**     | What the check's own queries return: the scalar count and the failed rows.                                                                                                                                                                                                                                                                                       |
| **Scalar count**     | The number the count (scalar) query returns. It decides pass or fail. `scalar_count` on the check selection.                                                                                                                                                                                                                                                     |
| **Failed rows**      | The rows the row query returns, as fetched for `show_failed_rows()`, `get_output_dfs()` and residues.                                                                                                                                                                                                                                                            |
| **Attributed rows**  | The source table rows the failed rows stand for, every copy counted. Every row count in this record.                                                                                                                                                                                                                                                             |
| **Attribute**        | To find those rows by the match key. The `server_predicate`, `server_lookup` and `client_lookup` routes attribute. `server_scalar` does not.                                                                                                                                                                                                                     |
| **Row-level check**  | A check about bad rows: not ERROR, row-level, with an operator that sets an upper bound. Step 2 picks them.                                                                                                                                                                                                                                                      |
| **Not attributable** | A row-level check whose attributed rows are not in the row counts. It is never attributable when its failed rows have no match key, and those rows become residues. It is not attributable this run when, for example, its rows were truncated or the table could not be exported. Under `row_counts="scalar"` every failed row-level check is not attributable. |

The `failed_rows` column of `get_row_quality_df()` at schema and dimension
level, and the "failed rows" of a schema or dimension below, are attributed
rows that failed. At check level the column is `attributed_rows`.

## Goal

For every schema in a run, produce:

- **Total rows.** The in-scope row count, after adapter filter conditions.
- **Failed rows.** Attributed rows that fail at least one row-level check.
- **Failed rows per dimension.** Attributed rows that fail at least one
  row-level check in that dimension.
- **Pass rates.** `(total rows - failed rows) / total rows`, in total and per
  dimension.
- **Trust metadata.** Whether each number is exact, which checks were row-level
  and which were not (with the reason).

These numbers must be:

- **Exact.** Correct regardless of `max_failed_rows` or duplicate rows.
- **Cheap.** Never export the full table to count a certified row filter.
  Export it for other checks only where the data source cannot attribute
  their rows, and never under `ValidationConfig(row_counts="scalar")` (see [decision 2](#2-the-exported-table-is-shared-by-counting-and-annotated-output)),
  and at most once per run.
- **Consistent.** The same numbers that `print_summary`, the OTEL gauges and a
  public API report. Equal to what annotated output flags where both are exact.
  Annotated output matches on the same key as the numbers: the declared primary
  key when it is proven unique, else the full columns.
- **Engine-neutral.** Work for SQL checks, the upcoming Python engine and
  cross-source checks.

## Problem with the previous implementation

`_get_row_quality_summary_by_schema` and `_failed_rows_by_dimension` in
`validation/result.py` deduped `CheckResult.failed_rows` in Python. Measured on
the HDB example with `tests/hdb_resale/hdb_resale.yaml` (201,879 rows, 13 failed
checks):

| Method                              | Schema | uniqueness | conformity | consistency |
| ----------------------------------- | ------ | ---------- | ---------- | ----------- |
| Annotated output (reference)        | 10,571 | 10,551     | 10         | 12          |
| Previous dedup                      | 10,273 | 10,253     | 10         | 12          |
| Previous dedup, `max_failed_rows=1` | 8      | 1          | 6          | 1           |

The defects were:

1. **Silent undercount under a fetch cap.** The dedup read the fetched rows, so
   `max_failed_rows` shrank the schema and dimension counts. Annotated output
   raises in this case. The metrics and `print_summary` did not.
2. **Duplicate rows collapsed.** The numerator counted distinct value tuples
   (10,273) while the denominator counted table rows (201,879).
3. **Double counting across column sets.** `iter_unique_failed_row_keys` put the
   column-name tuple in the dedup key. The same row failing a `SELECT *` check
   and a column-subset check counted twice.
4. **Mismatched cap on the denominator.** `max_rows_for_statistics` capped the
   total-row count but not the failed-row count.

Annotated output gets the right answer on HDB, but only by exporting the whole
table and matching every row in a Python loop (981 ms on HDB, versus 41 ms for
the pushdown below). That cost grows with the table, not with the failures.
Annotated output also has failure modes of its own (see
[Correctness](#correctness)), so it is not a reference in general.

## Key insight

`get_annotated_output` works in two steps:

1. **Merge.** Stack every check's failed rows and group identical rows,
   collecting their check names (`_group_check_ids_by_row`).
2. **Annotate.** Look up every full-table row in the merged set
   (`_annotate_full_table`).

Counting only needs step 1. Step 2 exists to produce the artifact (it needs the
passing rows too) and, incidentally, to recover duplicate copies that step 1
throws away with `.unique()`. If step 1 keeps copy counts instead, the full table
adds nothing to the numbers of certified row filters. Checks that are not row
filters return copies that do not match the table. For those, the full table
is the reference, used where the data source cannot match the rows, and
`row_counts="scalar"` skips it (see [Full-table match](#full-table-match)).

## The process

Run once per schema.

### Step 1: Total rows

`COUNT(*)` over the same in-scope rows the checks saw, with adapter filter
conditions applied (the adapter's `get_total_rows`). It must not be capped
independently of the numerator, so `max_rows_for_statistics` no longer applies
to row-quality totals. The capped total is used only as a last resort, when no
uncapped count is available, and is then marked `approximate = true` (see
[Implementation](#implementation)). When the pushdown fits in one chunk and the
schema has no fetched-route check, the total runs in the same statement so both
counts see the same snapshot of a live table. With several chunks each chunk has
its own snapshot (see [Chunking](#chunking)).

### Step 2: Select the row-level checks

A check contributes rows only if all of these hold:

| Rule                                                                                           | Reason                                                                                                                                                       |
| ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| It is anchored to this schema                                                                  | Rows from other tables are other rows.                                                                                                                       |
| Its status is PASSED or FAILED                                                                 | ERROR never qualifies (see step 6). A PASSED check is attributed only under `attribute_tolerated_rows=True` ([decision 4](#4-tolerated-rows-are-a-setting)). |
| Its result is a row count (`supports_row_level_output`)                                        | Averages and sums have no rows to point at.                                                                                                                  |
| Its operator marks the matched rows as bad (see [decision 3](#3-inverted-checks-are-excluded)) | Inverted checks return the good rows.                                                                                                                        |

Every PASSED or FAILED check that passes these rules is a row-level check. A PASSED
check with matches is a tolerated check. Checks that fail a rule are reported as
not row-level, with the rule they failed. By default a PASSED row-level check is
not attributed. It runs no query, takes no route, has the reason
`passed, not attributed`, and is not counted in `checks_not_attributable`.

A row-level check is also **attributable** only when its failed rows have the
table's full columns, or its declared primary key. That is needed to recognise
the same row across checks, and is the mergeability rule annotated output
already uses. A row-level check without such a key is never attributable. It
stays row-level, is reported as not attributable with its reason, and its failed
rows become a residue. vowl checks this after it picks the match key in step 4.

### Step 3: Collect each check's attributed rows, copies included

| Check kind                                            | Route                                                                                        |
| ----------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| SQL, on the schema's own connection, certified        | Pushdown (below)                                                                             |
| SQL, on the schema's own connection, not certified    | Full-table match, or table-anchored pushdown (see [Uncertified checks](#uncertified-checks)) |
| SQL, on an adapter without pushdown                   | Full-table match                                                                             |
| Python engine (not implemented)                       | Full-table match on the handler's returned failed rows                                       |
| Cross-source (materialised into the temporary DuckDB) | Full-table match                                                                             |
| A failed check, under `row_counts="scalar"`           | The check's scalar count (`server_scalar`)                                                   |

`ValidationConfig.row_counts` picks between three modes:

| `row_counts`             | Certified filter | Other checks                                                      | Exports the table                    |
| ------------------------ | ---------------- | ----------------------------------------------------------------- | ------------------------------------ |
| `"attributed"` (default) | pushdown         | table match on a key_is_exact dialect, full-table match elsewhere | Only for the full-table match checks |
| `"scalar"`               | scalar           | scalar                                                            | Never, and no query or fetch runs    |
| `"off"`                  | none             | none                                                              | Never, and no row counts             |

`row_counts="scalar"` adds up the scalar counts of the failed checks only. An
earlier version also added the scalars of passed checks, which counted
tolerated rows the default leaves out. The deprecated
`enable_additional_schema_statistics=False` sets `row_counts="off"`, and
combining it with `row_counts="scalar"` raises a `ValueError`.

Table match and full-table match count the same rows on a key_is_exact
dialect, so the default only exports where table match cannot run.

**History.** An earlier `row_count_accuracy` setting had three levels:
`"accurate"` (full-table match for every uncertified check), `"balanced"` (the
current default) and `"fast_estimate"` (the current `row_counts="scalar"`). `"accurate"` gave
the same numbers as `"balanced"` on key_is_exact dialects and exported more,
so it was dropped before release.

Every route except the scalar must return the attributed rows, every copy counted.
A check that no route can attribute on a default run is not attributable. It
takes no route and adds nothing to the row counts.
The scalar is the count the check already ran to decide pass or fail. It is
exact for a certified filter, but it carries no rows, so a sum of scalars
cannot see rows two checks share. See [The scalar route](#the-scalar-route).

### Step 4: Merge

Match rows across checks by value tuple over the table's full columns, or over
the declared primary key when pushdown is available, the key leaves out at least
one column, and a duplicate-key probe over the table returns 0 (see P6 in
[Preconditions](#preconditions)). A unique key groups the rows exactly as the
full columns do. It gives a narrower GROUP BY and smaller table-match
statements, avoids comparing float, NaN, nested and timestamp values outside
the key, and lets column-subset checks that hold the key merge. The probe runs
at most once per schema. When it finds a duplicate, or fails, a check that
holds the key but not every column is left out with its own reason. For each
distinct row keep:

- **copies**: the largest number of times any single check returned it
- **checks**: the set of checks that caught it

Taking the maximum per check, not the sum, is what keeps a duplicated row at its
true count of copies without counting it again for each check that caught it.

The same rule applies when pushdown chunks are merged. Each chunk returns its
copy count per key, and the merge takes the maximum. A merge that only collects
keys collapses duplicates. A prototype that did this on Spark lost 314 of 16,823
rows.

### Step 5: Roll up

- **Failed rows** = sum of copies over all merged rows.
- **Failed rows for dimension d** = sum of copies over merged rows caught by at
  least one check in d.
- **Not-attributable checks** add nothing (`roll_up_bucket` in
  `row_quality/rollup.py`). A bucket is not exact when one of them may have
  rows in scope: a failed check with a scalar count above 0, or under
  `attribute_tolerated_rows=True` any such check. A bucket whose row-level checks are all not
  attributable has no failed rows or pass rate (N/A), like a bucket with no
  row-level checks. Under `row_counts="scalar"` the scalar counts of the failed
  checks are added instead (see [The scalar route](#the-scalar-route)).
- **Checks not attributable** = the bucket's row-level checks in scope that are
  not attributable. A passed check out of scope is not counted. At run level it is the sum over the schemas.
- **Pass rate** = `(total rows - failed rows) / total rows`. The total and every
  dimension share the same denominator.

Dimensions overlap, so per-dimension counts do not add up to the total. On HDB
the dimensions sum to 10,573 against a total of 10,571 because 2 rows fail checks
in two dimensions.

Any other grouping (severity, tags, custom attributes) is a different rollup of
the same merged rows and needs no further queries.

### Step 6: Trust metadata

Every number carries:

- **`approximate`**: true when any check in the bucket is approximate, when the total is
  capped or lower than the rows one of the table's checks found, or when a value
  could not be turned into a merge key. The causes that make a check approximate are
  listed under `approximate` in [Public API](#public-api). One of them is a check that
  would have been row-level but ended in ERROR, since a broken check could be
  hiding bad rows.
- **Checks row-level**, **checks not row-level** and **checks not attributable**,
  as integer counts, so a high pass rate cannot hide the fact that half the
  dimension's checks were not row-level, or not in the numbers. The checks themselves, with the route each took and the reason each
  was left out, are listed one per row in
  [`get_row_quality_df(by="check")`](#public-api).
- **Empty cases**:
  - A dimension with no row-level checks has no row pass rate. Report it as
    missing, not 100%.
  - A table with 0 rows has no pass rate. The previous code reported 100%. It is
    now reported as missing.
  - A dimension whose row-level checks all passed correctly reports 0 failed and
    100%.

### Step 7: Run-level rollup (optional)

Across schemas, failed rows and total rows are summed. Rows in different tables
are different rows, so nothing is merged across schemas. This rate is weighted
by table size. Where an unweighted view is useful, also report the mean of the
per-schema rates and label which is which.

vowl does not compute these run-level rates yet.

## Routes

### Pushdown for SQL checks

The query stacks each row-level check's own row query, tags each branch
with a check index, and groups in the engine. The inner queries are used
unchanged, so filter conditions, `TRY_CAST`, FK aliases and cross-table checks
that project the anchor's columns all work without SQL rewriting. The wrapper is
built for the adapter's dialect (see [Builder](#builder)).

The basic shape is below. Its column names are illustrative. The
[hardened form](#hardened-form) that ships changes the key, the flags and the
names, and splits large schemas into chunks.

```sql
WITH tagged AS (
    SELECT c1, c2, 0 AS _vowl_check FROM (<row query of check 0>) AS _q0
    UNION ALL
    SELECT c1, c2, 1 AS _vowl_check FROM (<row query of check 1>) AS _q1
),
per_check AS (
    SELECT c1, c2, _vowl_check, COUNT(*) AS copies
    FROM tagged
    GROUP BY c1, c2, _vowl_check
),
per_row AS (
    SELECT c1, c2,
           MAX(CASE WHEN _vowl_check = 0 THEN 1 ELSE 0 END) AS _vowl_f0,
           MAX(CASE WHEN _vowl_check = 1 THEN 1 ELSE 0 END) AS _vowl_f1,
           MAX(copies) AS copies
    FROM per_check
    GROUP BY c1, c2
)
SELECT _vowl_f0, _vowl_f1, SUM(copies) AS row_count
FROM per_row
GROUP BY _vowl_f0, _vowl_f1
```

The result has one row per distinct failure pattern (10 rows for HDB). Python
rolls the patterns up into schema, dimension or any other grouping.

When a schema also has fetched-route checks (cross-source checks, for example),
the query stops after `per_row` and returns the failing rows with their flags,
copies and one original value tuple per key. Python then merges those with the
other routes. This moves only failing rows. An optional optimisation, not
implemented, would upload the smaller side (usually the fetched rows) to the
engine as a temporary table and finish the whole merge there. The security
validator allows only SELECT, so this would need an adapter-level upload such as
DuckDB's `con.register`.

### Hardened form

The basic shape is exact only under the preconditions in
[Correctness](#correctness), and it does not scale (see [Scale](#scale)). Each
rule below closes one of those gaps.

#### Certification

A check joins a pushdown chunk only if its row query is a pure row
filter of the anchor. sqlglot must find:

- a single `SELECT` with no `WITH`, whose select list is `*` or `anchor.*`
- a single `FROM`, which is the anchor table. The table must match the schema's
  table including any qualifiers the query writes, so `SELECT * FROM other.t`
  does not certify for schema `t`. An unqualified `t` in the query still matches
  a schema named `db.t`.
- subqueries only inside `WHERE`
- none of `DISTINCT`, `GROUP BY`, `HAVING`, `LIMIT`/`TOP`/`FETCH`, `OFFSET`
  (reported as `LIMIT`), `QUALIFY`, `TABLESAMPLE`, `LATERAL`, `PIVOT`, set
  operations, joins or nondeterministic functions (`random`, `uuid`, `now`)
- no window functions outside `WHERE`
- a scalar query with a single `COUNT`, optionally aliased. For `COUNT(expr)`
  the derived row query adds `expr IS NOT NULL`, so it skips the same
  NULL rows the count skips

Certification runs before filter conditions are applied. vowl wraps the anchor
in the same `(SELECT * FROM t WHERE ...)` subquery for every check, so the
wrapper does not change the row-filter shape.

Every built-in row-count check passes. `unique`, `primaryKey` and
`duplicateValues` derive
`SELECT * FROM t WHERE col IN (SELECT col FROM t WHERE col IS NOT NULL GROUP BY col HAVING COUNT(*) > 1)`,
where the `GROUP BY` sits inside a `WHERE` subquery. The library's generated
checks and the custom checks in the HDB contract are plain `WHERE` filters. Only
hand-written custom SQL can fail.

#### Key

Group on one binary, type-tagged expression per column, aliased by position as
`_vowl_c0`, `_vowl_c1` and so on.

- Binary identity removes collation effects in the keyed dialects: NOCASE and
  RTRIM (SQLite), Spark UTF8_LCASE, Databricks collations through the shared
  Spark entry, and Postgres nondeterministic collations. It also keeps `-0.0` apart from `0.0`. SQL Server CI_AS, MySQL PAD
  SPACE and `max_sort_length`, and Snowflake collations raise the same problem,
  but those dialects have no key entry yet and are reported approximate.
- The type tag separates SQLite's `1`, `1.0` and `'1'` in one column. The
  other keyed dialects hold one type per column, so they need no tag.
- Positional aliases cannot collide with data columns. With the basic shape's
  names, a table with a column named `copies` counted 30 rows against a truth
  of 3. They also avoid Spark's `AMBIGUOUS_REFERENCE` on case-variant or
  duplicated column names.

The key expression comes from a per-dialect table:

| Dialect                                          | Key expression                                                                                                                                                                                                                                  | Status                                                                                                                                                                                                                                          |       |     |                                                                                                       |                                                                                                                                                                                                 |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- | --- | ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| DuckDB                                           | `encode(CAST(c AS VARCHAR))`. Nested types use `encode(CAST(to_json(c) AS VARCHAR))`, so `['a, b']` and `['a', 'b']` stay apart. BLOB columns are used as they are.                                                                             | Tested. Doubles round-trip bit for bit, including ±0, ±inf and subnormals.                                                                                                                                                                      |       |     |                                                                                                       |                                                                                                                                                                                                 |
| SQLite                                           | `typeof(c) \                                                                                                                                                                                                                                    | \                                                                                                                                                                                                                                               | ':' \ | \   | CASE typeof(c) WHEN 'real' THEN printf('%!.17g', c) WHEN 'blob' THEN hex(c) ELSE CAST(c AS TEXT) END` | Tested. `CAST AS BLOB` alone is not enough, and `printf('%.17g')` prints 0.30000000000000004 as `0.3`. `%!.17g` round-trips every double tried. The concatenation drops the column's collation. |
| Spark and Databricks (shared)                    | `CAST(CAST(c AS STRING) AS BINARY)`. Nested types use `CAST(to_json(c) AS BINARY)`.                                                                                                                                                             | Tested on Spark 4.0.2, including `-0.0` against `0.0` and NaN. Databricks shares the entry and the single-scan form but was not tested separately.                                                                                              |       |     |                                                                                                       |                                                                                                                                                                                                 |
| Postgres                                         | `float8send(CAST(c AS DOUBLE PRECISION))` for floating types, so the key is the stored bits. `convert_to(CAST(c AS TEXT), 'UTF8')` for everything else, which drops the collation and also groups `json`. `bytea` columns are used as they are. | Tested on Postgres 16 with testcontainers: `-0.0` against `0.0`, NaN, 0.30000000000000004 against 0.3, `a` and `A` under a nondeterministic ICU collation, duplicates, table match, and boolean, `json` and `bytea` columns in the mixed route. |       |     |                                                                                                       |                                                                                                                                                                                                 |
| Other dialects (BigQuery, Snowflake, SQL Server) | Plain column. `MD5(CAST(c AS VARCHAR))` only for the types ibis reports as JSON, geospatial or nested. Types ibis reports as strings (SQL Server `text` and `xml`, Oracle CLOB) use the plain column.                                           | Untested. Not used when the [keyless mask histogram](#keyless-mask-histogram) can count the schema. Otherwise reported with `approximate = true`.                                                                                               |       |     |                                                                                                       |                                                                                                                                                                                                 |

A dialect with no entry counts with the keyless mask histogram where it can.
Otherwise it runs with plain column keys and reports `approximate = true`.

#### Keyless mask histogram

When every row-level check of a schema is a certified row filter, the union of
their failing rows needs no row identity. In a dialect without a key entry, the
certified queries are lifted into one scan of the table, as in the single-scan
form. Each row gets a mask of the checks it fails, and the histogram groups on
the masks alone:

```sql
WITH _vowl_scan AS (
  SELECT CAST(CASE WHEN (p0) THEN 1 ELSE 0 END + CASE WHEN (p1) THEN 2 ELSE 0 END AS BIGINT) AS _vowl_m0
  FROM t WHERE (p0) OR (p1)
), _vowl_hist AS (
  SELECT _vowl_m0, CAST(COUNT(*) AS BIGINT) AS _vowl_rows FROM _vowl_scan GROUP BY _vowl_m0
)
SELECT _vowl_t._vowl_total, _vowl_hist.*
FROM (SELECT COUNT(*) AS _vowl_total FROM (<anchor>) AS _vowl_a) AS _vowl_t
LEFT JOIN _vowl_hist ON 1 = 1
```

Each row of the scan is one table row, so every copy is counted once and no
collation or mixed type can merge two rows. The numbers are exact. The table's
total comes from the same statement.

It runs only when no row-level check is on the fetched route, every query lifts
into a scan of one FROM clause, and the branches fit in one chunk. If any of
these fails, or the statement raises, the schema falls back to plain column keys
and reports `approximate = true`. Keyed dialects do not use it, because table match
and fetched rows need the per-row keys anyway.

Some types cannot be grouped (Spark VARIANT, BigQuery JSON and GEOGRAPHY,
SQL Server `text` and `xml`, Oracle CLOB), and a key can be
wider than SQL Server's 8,060-byte limit. The design called for grouping these
on a 128-bit hash of an injective encoding: a NULL marker plus length-prefixed
text per column. A 64-bit hash is not enough. Its collision probability is about
2.7% at 10^9 rows. That hash is not implemented. What protects these cases today
is the per-column `MD5` fallback above in dialects without an entry, and the
grouped preflight (see [Probe](#probe)), which sends a schema whose keys the
engine cannot group to fetched rows before any chunk runs.

The whole-row width hash is deferred. It only matters for SQL Server, which has
no key entry yet and already reports `approximate = true`, so it belongs with a future
SQL Server entry.

#### Flags

Each branch emits a literal word index and bit value, so 63 checks share one
`BIGINT` word:

```sql
per_row AS (
    SELECT _vowl_c0, _vowl_c1,
           CAST(SUM(CASE WHEN _vowl_w = 0 THEN _vowl_bit ELSE 0 END) AS BIGINT) AS _vowl_m0,
           MAX(_vowl_copies) AS _vowl_copies
    FROM per_check
    GROUP BY _vowl_c0, _vowl_c1
)
```

`per_check` groups by key, word and bit, so each bit appears at most once per
key and `SUM` equals bitwise OR. The `CAST` is needed because DuckDB widens `SUM`
to HUGEINT. Using 63 bits keeps clear of the sign bit. Python decodes the words.

The snippet and the basic shape use short names. The shipped statements name
the CTEs `_vowl_tagged`, `_vowl_per_check`, `_vowl_per_row` and `_vowl_hist`
(and `_vowl_scan` for the single-scan form), with the columns `_vowl_w`,
`_vowl_bit`, `_vowl_m{i}`, `_vowl_copies`, `_vowl_v{i}`, `_vowl_rows` and
`_vowl_total`.

This cuts the output from |C| + N + 1 columns to |C| + ceil(N/63) + 1. With 32
columns and 2,000 checks that is 65 columns instead of 2,033, which is over the
limits of Postgres (1,664), Oracle (1,000) and SQLite (2,000). A `string_agg` of
check ids was also tested and rejected, because MySQL, Oracle and SQL Server
truncate it silently at 1,024 to 8,000 bytes.

#### Chunking

A schema's certified checks are split into chunks. Each statement stays within:

| Budget         | Value                                         | Limit it protects                                                                                                                                                                                                                               |     |                                                                                  |     |                                              |
| -------------- | --------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --- | -------------------------------------------------------------------------------- | --- | -------------------------------------------- |
| Branches       | 250                                           | SQLite rejects more than 500 compound terms. DuckDB memory grows with branches times threads. Spark fails to parse at about 1,500 branches.                                                                                                     |     |                                                                                  |     |                                              |
| Statement size | `MAX_SQL_BYTES` = 512,000 bytes               | Half of the 1 MB limits of BigQuery, Snowflake and Trino, leaving room for dialect expansion. Each branch is sized from its rendered SQL in UTF-8 bytes, so the budget holds per statement, give or take the small wrapper around the branches. |     |                                                                                  |     |                                              |
| Output columns | keys + values + ceil(K/63) + 2, at most 1,000 | Oracle's 1,000, Postgres 1,664, SQLite 2,000. The keys are \                                                                                                                                                                                    | C\  | columns. Values are returned only in the mixed route, so the data columns are 2\ | C\  | then (`KeySpec.max_words` in `pushdown.py`). |

Each chunk stops at `per_row` and returns keys, mask words and copies, plus
values in the mixed route. The cross-chunk merge always runs in Python on the
key bytes, where equality is identity. It ORs the words and takes the maximum of
the copies per key (see [step 4](#step-4-merge)). A schema that fits in one
chunk and has no fetched-route check runs the full pattern histogram, with the
total, in a single statement instead.

Chunking is exact because maximum and disjoint-bit OR are associative and
commutative, provided the merge uses the same equality as the chunks. It gives
up the single snapshot. Each chunk reads its own snapshot of a live table. The
per-check scalar counts and annotated output already read separately, so this is
no worse than those.

The chunk shape depends on the engine:

- **DuckDB.** Tagged `UNION ALL` chunks of 100 to 250 branches were the fastest
  form measured.
- **Spark and Databricks.** Every `UNION ALL` branch is its own table scan (232 s
  against 16 s for a single scan at 500 checks). Certified checks whose `WHERE`
  can be lifted go into single-scan chunks of up to 250 predicates, grouped by
  their `FROM` clause, with the mask words built as sums of `CASE` terms. The
  single-scan form groups by key with `MAX` of each mask word and `COUNT(*)` as
  the copies. Uncertified checks, and certified checks whose `WHERE` cannot be
  lifted, use `UNION ALL` chunks.
- **Other engines.** Tagged `UNION ALL` chunks until measured.

#### Probe

A check whose derived row query is invalid (for example a `GROUP BY`
check, which binds only in its scalar form) fails the whole statement. A failed
chunk is bisected until the invalid branches are found. They are dropped, and
their checks stay row-level but are not attributable, with the reason `the counting
query failed in the data source`. The common case, where every branch is valid, costs no
extra queries. A per-branch `SELECT * FROM (<q>) AS _p LIMIT 0` probe would be
the alternative on engines where a failed statement is expensive. It is not
implemented.

The implementation adds two cheap probes. Before any chunk runs, a
`SELECT <keys> FROM (<anchor>) AS _vowl_a WHERE 1 = 0 GROUP BY <keys>` checks
that the key expressions bind on the table and that the engine can group them.
A failure sends the whole schema to the full-table match up front, instead of failing
every chunk. Each check routed to table match gets a
`SELECT * FROM (<q>) AS _vowl_p WHERE 1 = 0` column probe, since its columns
decide whether it can be matched. A check whose column probe fails is not
attributable. `WHERE 1 = 0` is used instead of `LIMIT 0`
because not every dialect has `LIMIT`. A single-chunk histogram statement that
fails is retried as per-row chunks before bisecting.

#### Builder

A copying left fold of `exp.union` is quadratic. It took 46 s at 500 checks and
238 s at 1,000. The prototype used sqlglot with `copy=False` or the varargs
`exp.union(*branches)`.

The implementation assembles each statement as text instead, from identifiers
and key expressions that sqlglot rendered for the adapter's dialect. Identifier
rendering is cached, so building statements is fast. This is linear in the
branches, and it keeps sqlglot from rewriting the key functions (`encode`,
`typeof`, `printf`) when a whole statement is re-rendered. The top level is
always one `WITH ... SELECT`, because the security validator accepts only a
`SELECT` at the top, and every statement passes that validator before it runs.

### Uncertified checks

A valid check that fails certification is counted by one of three routes:

- **Table match (TBSEMI).** Count the rows of the anchor whose binary key is in
  the check's distinct failed keys. This is annotated output's own definition, a
  table row matching a failed row, computed with a sound equality and no cap. It
  gives the annotated answer on `DISTINCT`, join fan-out and `QUALIFY` checks,
  where the plain pushdown overcounts or undercounts. It costs one anchor scan
  per table-match check. A second statement counts, per check, the distinct
  failed keys that no anchor row holds, with the same keys and the same
  null-safe equality (an anti-join). A check with any such key is marked
  `approximate = true`, with the reason `some failed rows could not be attributed to a table row`, as
  the full-table match does. If that statement fails for a check, the check is
  marked `approximate = true` and keeps its reason.
- **Scalar.** The check's scalar count, with `approximate = true` because the check is
  not certified as a row filter. Used only for failed checks under
  `row_counts="scalar"`.

- **Full-table match.** Export the schema's table once and match the check's
  fetched failed rows onto its rows by value, on your machine. See
  [Full-table match](#full-table-match).

An uncertified check on the schema's own connection in a dialect with a key
entry uses table match, and every other uncertified check uses the full-table
match. That covers cross-source checks, adapters without pushdown
and dialects without a key entry. Table-match checks are listed in `by="check"`
with route `server_lookup` and the certification rule they failed as the
reason. Both matching routes also mark a check `approximate = true` when its SELECT
list holds anything other than plain table columns or `*`, or vowl cannot parse
it (`returns_table_values` in `row_quality/match_key.py`). Such a check may
return values no table row holds, or values equal to another row's.

### Full-table match

The table is exported with `_fetch_full_table`, the same export annotated
output uses. It is cached on the `ValidationResult`, so a run exports each
table at most once across counting and annotated output. The match then runs
in Python (`merge_onto_table` in `row_quality/merge.py`):

- Every exported row is keyed with the normalised Python keys of
  [Merge equality](#merge-equality), over the schema's match key.
- Each check's fetched failed rows are keyed the same way. Every table row that
  holds a returned key is caught by the check. So the copies come from the
  table, not from the check's query. A `DISTINCT` check returning one row for
  3 identical copies counts 3, and a join returning a row twice counts it once.
- Certified pushdown checks run in the engine as before, with values, and their
  rows are placed on the exported table by the same keys.
- The schema total is the exported table's row count, which is exact, unless a
  pushdown histogram already gave the total.

Once the table is exported, counting uses its column names, the same as
annotated output, so both flag the same rows. In a dialect without a key entry,
certified pushdown checks are matched onto the exported table too, since the
table is there anyway and the plain-column pushdown keys could merge distinct
values.

A check whose returned rows include keys that no table row holds is counted
from the keys that do match, marked `approximate = true`, with the reason
`some failed rows could not be attributed to a table row`. This happens with transformed values
such as `c * 1.5` or `upper(s)`. The test is per distinct key. A transformed
value that happens to equal another row's values (`lower('A')` = `'a'` when `a`
exists) matches that row.

A full-table match check that cannot be matched is not attributable. It adds
nothing to the row counts. Table match is not an option, since a check that
could use it already does. It keeps a reason:

| Cause                                                | Reason                                                |
| ---------------------------------------------------- | ----------------------------------------------------- |
| The export fails, or there is no adapter             | `the table could not be exported`                     |
| The table's values cannot be turned into row keys    | `the failed rows could not be turned into match keys` |
| The check's failed rows could not be fetched         | `the failed rows could not be fetched`                |
| The check's rows were cut short by `max_failed_rows` | `truncated by max_failed_rows`                        |

A table match or pushdown branch that fails in the engine also leaves its check
not attributable, with the reason `its row query failed in the data source`.
Table match counts in SQL, so `max_failed_rows` does not affect it.

The other checks of the schema still use the full-table match when one check
is not attributable because of truncation.

### The scalar route

Under `row_counts="scalar"` every failed row-level check takes `server_scalar`
and is not attributable, so `checks_not_attributable` equals the number of
failed row-level checks. Passed checks add nothing, unlike an earlier version
that added their scalars too. No pushdown,
table match, fetch or export runs. The schema's primary key probe is deferred
until annotated output asks for the match key, so annotated output still keys
on it. Each check's scalar count is its `scalar_count`, exact when the check
is certified.

The roll-up adds the scalars of a bucket (schema or dimension). It keeps the
bucket exact only when at most one of its checks has failing rows and that
check is exact. The sum is capped at `total_rows`. Tolerated rows come from the
scalars of passed checks in the same way. The run total is exact only when
every schema is exact.

The export holds the whole table in memory on the machine running vowl.
Measured at 6 columns: about 3 s and 1 to 1.5 GiB at 1M rows, and about 13 s
and 3.5 GiB at 5M rows. Schemas whose checks are all certified filters never
export.

### Cross-route merge

Pushdown keys are binary encodings, while Python and cross-source rows are Arrow
values, so they cannot be compared directly. In the mixed route the pushdown
also returns one original value tuple per key, and the cross-route merge uses
the normalised Python row keys described in [Merge equality](#merge-equality).
The merge itself marks the schema and dimension numbers `approximate = true` only when
a value cannot be turned into a key, for example a type that fails to export.

### Python engine

Not implemented. vowl has no Python engine yet, so this section records the
intended contract.

The Python engine follows the SQL two-query contract: a scalar failed-row count
and a failed-rows result per check, via a handler. The handler's failed rows feed
step 4 directly. For exact counts the handler must:

- return every failing row, including every copy of a duplicate
- return the table's full columns or its primary key
- carry values of the same types the adapter's Arrow export produces, so a row
  caught by both a SQL and a Python check is recognised as one row

On DuckDB and Spark a Python check could alternatively be registered as a UDF and
join the pushdown query as an ordinary predicate. UDF registration is not
implemented either. A DuckDB prototype combining a Python UDF check with a SQL
check counted correctly in 150 ms over HDB.

#### Merge equality

The Python merge (`_group_check_ids_by_row` and `_annotate_full_table` in
`result.py`) used to match rows on `as_py()` value tuples. A trace on DuckDB with
pyarrow 23 found five faults:

- NaN never equals itself, so NaN rows never matched (annotated 0, truth 3).
- `-0.0` equals `0.0` in Python, so a row flagged only for `-0.0` also flagged
  every `0.0` row.
- `.unique()` cannot hash LIST, STRUCT, MAP or fixed-size array columns, so a
  schema holding one raised before any key was built.
- TIMESTAMP_NS lost its nanoseconds when the grouped rows were rebuilt with
  `pa.table()` from Python values, which re-infers `timestamp[us]`. The fetch and
  the Arrow export both keep nanoseconds. The truncated row then matched only
  itself (annotated 1, truth 3).
- UBIGINT above 2^63 overflowed in the same rebuild. The export returns `uint64`
  without trouble.

The merge now builds one normalised key per row (`row_keys` in
`result_row_quality.py`):

- NaN maps to one placeholder, so NaN rows match each other.
- `-0.0` maps to its own placeholder. Other floats stay plain, so `1.0` still
  matches `1` where the two sides were promoted to different types.
- Timestamp, duration, time and date64 columns compare as int64 at their stored
  unit.
- Nested values are frozen into tuples, with the same float rule inside.
- Before matching, the failed side's key columns are cast to the full table's
  types where the cast succeeds.

Grouped tables are rebuilt with `take()` on the original Arrow table, so every
column keeps its source type. The `.unique()` calls are gone. The grouping
already collapses duplicate rows, and residues dedupe on the same keys.

When a unique declared primary key is the match key, only the key columns are
compared. Float, nested and temporal values in the other columns then never
reach the merge, so the rules above matter only for key columns.

Still out of scope:

- INTERVAL (ibis casts it to a duration), BIT (invalid UTF8) and UNION (no cast
  from `sparse_union` to string). All three fail in the adapter's Arrow export,
  before the merge runs.
- SQLite columns holding `1`, `1.0` and `'1'`, which also fail on export.
- The `max_failed_rows` cap.

With these fixed, the Python route reports `approximate = true` only when a check's
rows were truncated, the check is not a certified row filter, or a column type
failed to export.

### Cross-source checks

These run in a temporary DuckDB that `MultiSourceSQLExecutor.cleanup()` deletes
afterwards. Their fetched failed rows are matched onto the exported table
([Full-table match](#full-table-match)). Under
`row_counts="scalar"` they use their scalar. When they cannot be
matched they are not attributable.
Running the pushdown before cleanup is a possible later improvement.

## Correctness

An adversarial investigation compared the pushdown with annotated output's merge
(`_group_check_ids_by_row`) to answer one question: is there any case only the
merge handles? It found that neither path dominates the other, and that the
hardened pushdown is provably exact under stated conditions.

### Preconditions

| ID  | Precondition                                                     | Met by                                                                                                                                                               |
| --- | ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P0  | Every branch is a valid query that projects the anchor's columns | [Probe](#probe) and [Certification](#certification)                                                                                                                  |
| P1  | Every check is a pure row filter of the anchor, counted per copy | [Certification](#certification)                                                                                                                                      |
| P2  | Rows the engine groups together agree on every check's predicate | Binary, type-tagged [keys](#key) and deterministic predicates                                                                                                        |
| P3  | Every column type can be grouped                                 | Partly. The `MD5` fallback in dialects without a key entry, and the grouped preflight, which sends the schema to the full-table match ([Key](#key), [Probe](#probe)) |
| P4  | All branches read one snapshot                                   | A single statement. Lost under [chunking](#chunking).                                                                                                                |
| P5  | No row cap on branches                                           | By construction. The pushdown never goes through the `max_failed_rows` fetch.                                                                                        |
| P6  | A declared primary key used as the key is actually unique        | A duplicate-key probe, run once per schema. The primary key is used only when it returns 0 ([step 4](#step-4-merge)).                                                |

**Theorem.** Under P0 to P4 the pushdown's failed-row count equals the number of
attributed rows, every copy counted, that fail at least one row-level check. The pattern histogram is
exact too: for every non-empty combination of checks, its row count is the number
of attributed rows, every copy counted, failing exactly that combination.

**Proof sketch.** Take one group of rows the engine treats as equal, with m
copies. By P1 each check returns all m copies or none. By P2 that choice
is the same for every copy. So the largest per-check count in the group is m if
any check caught it and the group is absent otherwise. Summing m over the caught
groups gives the true count. P0 and P3 only ensure the statement runs.

Under the same preconditions the pushdown is always at least as good as the
merge, which additionally needs Python equality to match the stored values, a
lossless Arrow round trip and no cap.

### Where each path fails

All cases were run on DuckDB 1.5.1 through `validate_data`, unless marked.
Annotated values marked "was" are fixed by the [merge equality](#merge-equality)
change, and the bold value is the one measured before it.

| Case                                                                                       | Truth   | Annotated         | Basic pushdown              | Hardened pushdown                   |
| ------------------------------------------------------------------------------------------ | ------- | ----------------- | --------------------------- | ----------------------------------- |
| NaN floats                                                                                 | 3       | 3 (was **0**)     | 3                           | 3                                   |
| Only `-0.0` flagged                                                                        | 1       | 1 (was **2**)     | 1                           | 1                                   |
| `-0.0` and `0.0` caught by different checks                                                | 2       | 2                 | **1**                       | 2 (binary key)                      |
| NOCASE or RTRIM collation, `a` and `A` split across checks                                 | 2       | 2                 | **1**                       | 2 (binary key)                      |
| Spark UTF8_LCASE, same split (Spark 4.0.2)                                                 | 2       | n/a               | **1**                       | 2 (binary key)                      |
| SQLite `1`, `1.0` and `'1'` in one column                                                  | 4       | **ERROR**         | **3**                       | 4 (type tag)                        |
| `DISTINCT` inside the check, 3 identical copies                                            | 3       | 3                 | **1**                       | 3 (table match or full-table match) |
| Join fan-out, 2 matches per row                                                            | 3       | 3                 | **6**, rate can exceed 100% | 3 (table match or full-table match) |
| Data column named `copies`                                                                 | 3       | 3                 | **30**                      | 3 (positional aliases)              |
| `GROUP BY` check with an invalid derived query                                             | skipped | skipped           | whole statement fails       | skipped (probe)                     |
| LIST, STRUCT, MAP, fixed-size array, UBIGINT above 2^63                                    | 3       | 3 (was **ERROR**) | 3                           | 3                                   |
| INTERVAL, BIT (fail on export)                                                             | 3       | **ERROR**         | 3                           | 3                                   |
| TIMESTAMP_NS                                                                               | 3       | 3 (was **1**)     | 3                           | 3                                   |
| `max_failed_rows=1`                                                                        | 3       | warns             | 3                           | 3                                   |
| NULLs in key columns, NOACCENT, anti-join and `NOT EXISTS` checks, `ORDER BY` in the check | tie     | tie               | tie                         | tie                                 |

The full-table match gives the annotated answer in every row of the table
above where the table can be exported, since it is annotated output's own
match on the same export.

Table match used to make `-0.0` equal `0.0` on DuckDB 1.5.1. It also merged
NaN with `-NaN`, INTERVAL `1 month` with `30 days`, and structs that held
them. DuckDB runs a correlated `EXISTS` by deduplicating the outer columns the
subquery references and joining the answer back on them. Correlating on the
raw column therefore merged values that are equal under the column type but
have different keys, and the first row decided the answer for both. Turning
the optimizer off did not change this. The anchor's keys are now computed in
a derived table, and the `EXISTS` is correlated on those keys.

A check that returns transformed values (`c * 1.5`, `upper(s)`) is counted from
the keys that match a table row on both table match and the full-table match.
Both detect the keys that match no row and report `approximate = true`. Neither can
detect a transformed value that equals another real row's key (`c + 1` where
that row exists). That row is counted as caught, and the check stays exact.

Two cases used to be wrong in both paths. Both belonged to the check layer and
are now fixed in `get_row_query` (`contracts/check_reference_sql.py`):

- A scalar query written `COUNT(*) AS n` derived no row query, because
  the alias hid the `COUNT`. Such a check was dropped as a probe failure, with
  `approximate = true`. The alias is now unwrapped, so the check is certified and
  marked on annotated output.
- A `COUNT(col)` check skips NULL rows that its row query returned.
  The derived query now adds `col IS NOT NULL` to its `WHERE`, so the two
  queries agree and the check is certified. This applies to any `COUNT(expr)`
  other than `COUNT(*)` or a literal.
- A `COUNT(DISTINCT x, ...)` check counts values, not rows. Its failed rows
  are the rows holding those values, so the derived query keeps the `WHERE`
  and adds `x IS NOT NULL` for each argument. Its scalar is not a row count,
  so `server_scalar` marks it `approximate = true` with
  `the check counts distinct values, not rows`, and an `ERROR` check of this
  kind sets `approximate`. Row-value arguments, `(a, b)` and `ROW(a, b)`,
  are not row-level: such a value is not NULL when only some fields are, and
  databases disagree on how they count it. A check with no `WHERE` flags every
  row with a non-NULL `x`, which is a cardinality check rather than a row
  filter. This is kept as it is and noted in the user docs.

Truncation under `max_failed_rows` is detected by fetching `cap + 1` rows
(`CappedFetch` in `executors/base.py`). Comparing the fetched rows with the
check's count missed truncation whenever the count was lower than the rows
the row query returns (`DISTINCT`, joins, `COUNT(DISTINCT ...)`), and
flagged a check whose count was higher. Annotated output now warns instead of
raising, and tags the check's `check_info` items with `"truncated": true`.

## Scale

Measured on a 1,009,881-row, 32-column table with 9,881 duplicate rows, at
500, 1,000 and 2,000 checks and three failure rates.

The basic shape breaks well before 2,000 checks:

| Limit                 | Where it breaks                                                                                                                                                       |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SQLite compound terms | 501 branches (`too many terms in compound SELECT`)                                                                                                                    |
| Statement size        | 1.64 to 1.68 MB at 2,000 checks, over the 1 MB limits of BigQuery, Snowflake and Trino. Size grows by about 830 bytes per check at 32 columns.                        |
| Output columns        | 2,033 at 2,000 checks, over Postgres, Oracle and SQLite                                                                                                               |
| DuckDB memory         | Out of memory under a 20 GiB spill cap at 2,000 checks (low fail rate) and at 1,000 checks (high fail rate). Memory grows with branches times threads, not with rows. |
| Spark 4.0.2 parse     | `FAILED_TO_PARSE_TOO_COMPLEX` between 1,492 and 1,500 branches                                                                                                        |
| Spark 4.0.2 runtime   | 232 s at 500 checks against 16 s for a single scan                                                                                                                    |

The hardened form, chunked, was exact in every run and the fastest form measured
wherever rows actually failed:

| Engine and size                     | Unchunked      | Chunked                         |
| ----------------------------------- | -------------- | ------------------------------- |
| DuckDB, 1,000 checks, low fail rate | 39.5 s         | 7.2 s (K = 100)                 |
| DuckDB, 2,000 checks, low fail rate | out of memory  | 15.6 s at 6.8 GB peak (K = 100) |
| Spark, 1,000 checks                 | 1,090 s        | 53 s (single scan, K = 250)     |
| Spark, 2,000 checks                 | fails to parse | 102 s (single scan, K = 250)    |

K = 500 was consistently slower than K = 100 or 250. Planning is cheap on both
engines, a few seconds at 2,000 checks. For comparison, the Python path moved
1.96 million rows at 2,000 checks and peaked at 9.4 GB.

## Design decisions

### 1. Count attributed rows, every copy counted, not distinct values

A duplicated failing row counts once per copy. This matches the denominator
(`COUNT(*)` counts every copy) and matches annotated output, which flags every
copy. It raised the previous numbers on tables with duplicates (HDB: 10,273 to
10,571).

### 2. The exported table is shared by counting and annotated output

The exported table was first reserved for annotated output, and counting never
read it. Counting now reads it too (see [Full-table match](#full-table-match)).
It is exported only for checks the data source cannot table-match. Under
`row_counts="scalar"` counting never exports it. The export is cached on the `ValidationResult`, so
counting and annotated output share one export per table. When counting uses
the export it keys on the exported column names, as annotated output does.

The cost is memory on the machine running vowl (see
[Risks](#risks-and-open-questions)). The gain is exact numbers for `DISTINCT`,
join, transformed-value and cross-source checks, which the engine-side routes
either cannot count or count without knowing they are off.

Annotated output stays the row-level evidence artifact. It reads the same check
selection as the statistics (step 2), then does its own Python merge over the
fetched rows. Before the merge, each check's rows are cast to the exported
table's Arrow types, so two checks that return `int32` and `int64` for one
column no longer fail the concat with `ArrowTypeError`. It matches rows on the key the
statistics chose in step 4 (`RowQuality.merge_key`): the full columns, or the
declared primary key when it was found unique. Both paths decide whether a
check's rows can be merged with one function, `has_match_key` in
`row_quality/match_key.py`. Counting passes the columns it knows, and marking
the columns of the exported table. Its flagged-row counts equal the statistics where both paths are exact
(see [Where each path fails](#where-each-path-fails)).

A later step can let annotated output read the engine-side flags directly, which
would carry the pushdown's remaining advantages (no cap, and types that fail on
export) into the artifact.

#### Future work: marking from engine keys

Deferred plan. Annotated output would export the table together with the same
binary key expressions the pushdown groups on
(`SELECT *, <KeySpec key expressions> AS _vowl_cN FROM <anchor>`). It would
look up each exported row's key in `PushdownOutcome.rows`, which maps a key to
a check mask, and decode the mask bits into `check_info`. Marking would then no
longer need each check's fetched failed rows, so the `max_failed_rows` warning
goes away for `server_predicate` and `server_lookup` checks, and so does the per-check
fetch.

It needs:

- an adapter export API that takes extra projections and an explicit ibis
  schema,
- a pushdown run that returns per-row entries instead of the histogram,
- parity tests against the current marking.

It does not help with types that fail on export (INTERVAL, BIT, UNION), since
the table is still exported. An engine-side `LEFT JOIN` of the table to the
flags was rejected. It would hit the chunk limits, needs temporary tables that
vowl does not create, and its Arrow types drift from the plain export.

### 3. Inverted checks are excluded

A row-count check marks its matched rows as bad only when its operator bounds the
count from above. The rule, by operator:

| Operator                                                                                  | Matched rows are bad?                                        |
| ----------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| `mustBeLessThan`, `mustBeLessOrEqualTo`                                                   | Yes                                                          |
| `mustBe 0`                                                                                | Yes                                                          |
| `mustBeBetween [0, n]`                                                                    | Yes                                                          |
| `mustBe n` with n > 0, `mustNotBe`, `mustBeBetween [a, b]` with a > 0, `mustNotBeBetween` | No (exact or two-sided target, so no single row is at fault) |
| `mustBeGreaterThan`, `mustBeGreaterOrEqualTo`                                             | No (inverted, the matched rows are the good ones)            |

Excluded checks still count toward check-level pass and fail. They are listed as
not row-level with reason `operator does not set an upper limit`. Annotated output
applies the same rule. A FAILED inverted check's matched rows are neither
flagged nor kept as a residue.

### 4. Tolerated rows are a setting

A check can pass while some rows break it, for example `mustBeLessThan 100`
passing with 50 matching rows. Whether those 50 rows count as rows with issues
depends on what the user is reporting, so it is a setting:

```python
ValidationConfig(attribute_tolerated_rows=False)   # default
ValidationConfig(attribute_tolerated_rows=True)
```

| Value             | Rows counted                                      | Suits                                                                                     |
| ----------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `False` (default) | Only rows from checks that FAILED                 | Row counts that agree with the check verdicts. Tolerances mean "this much is acceptable". |
| `True`            | Rows from every row-level check, passed or failed | DQ reporting where every rule-breaking row counts as rejected, whatever the tolerance.    |

Inverted checks stay excluded either way (decision 3). The setting applies only
under `row_counts="attributed"`.

Under the default a passed check is not attributed on any route. It runs no
pushdown, probe, fetch or export, adds nothing to the row counts, is not counted
in `checks_not_attributable`, does not affect `approximate`, and is not flagged in
annotated output or kept as a residue. Its reason is `passed, not attributed`.

Under `True` a tolerated check is attributed like a failed one, so it joins the
pushdown or needs the table exported. An export then moves the failed checks of
that table that cannot be pushed down onto `client_lookup` too. Annotated output
flags its rows, and their `check_info` items carry `"tolerated": true`.

An earlier version reported a separate `tolerated_rows` figure and fetched the
rows of tolerated checks on most routes even under the default. It was dropped:
it cost queries for a number the default does not report, and `True` gives the
same rows in `failed_rows`.

Per-check overrides (a contract-level property that forces one check in or out)
are a possible extension. They are not part of this design.

### 5. One component, many consumers

A single row-quality component computes the statistics lazily and caches them on
the `ValidationResult`. `print_summary`, the OTEL gauges
(`vowl.check.row.count`, `vowl.schema.row.count`, `vowl.dimension.row.count`
and the pass-rate gauges),
annotated output and `get_row_quality_df` all read from it. Nothing else counts
merged rows. Two figures still use each check's scalar count, by design: the
Multi Table "Non-unique Failed Rows" line in `print_summary`, and the per-check
OTEL gauges `vowl.check.row.scalar_count` and `vowl.check.row.scalar_pass_rate`.

## Public API

As implemented:

```python
result.get_row_quality_df(by="schema")      # one row per schema
result.get_row_quality_df(by="dimension")   # one row per (schema, dimension)
result.get_row_quality_df(by="check")       # one row per (schema, check)
```

Columns for `by="schema"` and `by="dimension"`: `schema_name`, `dimension` (for
`by="dimension"`), `total_rows`, `failed_rows`, `passed_rows`,
`pass_rate`, `approximate`, `checks_row_level`, `checks_not_row_level`,
`checks_not_attributable`. The last three are integer counts. `failed_rows` here
is the attributed rows that failed at least one row-level check.

`by="check"` is the diagnostic view behind those counts. It reads the same cached
computation as the headline frames, so it also shows checks dropped at run time.
Columns:

- `schema_name`, `check_name`, `dimension`, `status`
- `row_level`: whether the check is about bad rows (step 2)
- `route`: `server_predicate`, `server_lookup` or `client_lookup`, `server_scalar`
  for a failed check under `row_counts="scalar"`, and empty when not row-level,
  not attributed or not attributable
- `reason`: why the check was not row-level or not attributable, or why it left
  pushdown
- `scalar_count`: the check's scalar count
- `attributed_rows`: the rows of the table the check caught, before the merge.
  None when the check is not attributable
- `approximate`: true when this check's rows are incomplete or approximate. The
  causes are:
  - it took `server_scalar` and is not a certified row filter
  - it took `client_lookup` or `server_lookup` and its SELECT list holds more
    than plain table columns, or some of its returned rows match no table row
  - it was pushed down in a dialect without a key entry and the keyless mask
    histogram could not count the schema
  - the table's columns are unknown
  - it ended in ERROR but would have been row-level

A value that cannot be turned into a merge key does not mark a check. It makes
only the schema and dimension numbers approximate.

`reason` uses a short fixed vocabulary:

| Reason                                                                          | Row-level                      | Attributable   | Route                              |
| ------------------------------------------------------------------------------- | ------------------------------ | -------------- | ---------------------------------- |
| `operator does not set an upper limit` (decision 3)                             | no                             | no             | empty                              |
| `not a row-level check`                                                         | no                             | no             | empty                              |
| `check ended in ERROR`                                                          | no                             | no             | empty                              |
| `failed rows do not have the table's columns or primary key`                    | yes                            | no, never      | empty                              |
| `primary key has duplicate values`                                              | yes                            | no, never      | empty                              |
| `primary key uniqueness could not be checked`                                   | yes                            | no, never      | empty                              |
| `not certified for pushdown: uses <rule>`, for example `uses DISTINCT`          | yes                            | yes            | `client_lookup` or `server_lookup` |
| `checks tables from more than one data source`                                  | yes                            | yes            | `client_lookup`                    |
| `data source does not support pushdown`                                         | yes                            | yes            | `client_lookup`                    |
| `its row query failed in the data source`                                       | yes                            | no, this run   | empty                              |
| `truncated by max_failed_rows`                                                  | yes                            | no, this run   | empty                              |
| `the table could not be exported`                                               | yes                            | no, this run   | empty                              |
| `the failed rows could not be turned into match keys`                           | yes                            | no, this run   | empty                              |
| `the failed rows could not be fetched`                                          | yes                            | no, this run   | empty                              |
| `passed, not attributed` (the default for a PASSED check)                       | yes                            | not attributed | empty                              |
| `the check counts distinct values, not rows` (only under `row_counts="scalar"`) | yes, with `approximate = true` | no             | `server_scalar`                    |
| `some failed rows could not be attributed to a table row`                       | yes, with `approximate = true` | yes            | `client_lookup` or `server_lookup` |

Under `row_counts="scalar"` every failed row-level check is not attributable
and takes `server_scalar`, with the reasons above where they apply.

Some reasons cover more than their name suggests:

- A check with `DISTINCT` in a subquery, such as
  `SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE ...)`, typically reports
  `uses a FROM that is not the table itself`, because the rule names the first
  construct that fails.
- `truncated by max_failed_rows` replaces an earlier reason, such as the
  cross-source one.

A row-level check that matched no rows (for example a passing check with a count
of 0) runs no query and shows an empty route.

This also closes two gaps found during the OTEL work. Dimension statistics become
reachable without OTEL or private methods, and `get_check_results_df` gains the
resolved `dimension` column.

## Validation so far

Prototypes (not in the repo) compared pushdown with annotated output:

| Case                                                                        | Result                                                                                               |
| --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| HDB, complex contract, 13 failed checks                                     | Exact match on schema and all three dimensions                                                       |
| Same, `max_failed_rows=1`                                                   | Still an exact match (pushdown ignores the fetch cap)                                                |
| Employee, multi-table contract incl. FK and a mergeable cross-table check   | Exact match on both tables. Residue checks excluded as annotated does.                               |
| Mixed route: 2 SQL checks via pushdown, 11 checks via failed rows in Python | Exact match, 10,264 rows moved from the engine                                                       |
| Spark 4.0.2 local                                                           | All query shapes run (subqueries under `OR` and inside `CASE`, tagged `UNION ALL` with `GROUP BY`)   |
| Adversarial cases on DuckDB, SQLite and Spark                               | See [Where each path fails](#where-each-path-fails)                                                  |
| 500 to 2,000 checks on DuckDB, SQLite and Spark                             | See [Scale](#scale). Every finished variant matched an independent `COUNT(*) WHERE p1 OR ... OR pN`. |

Four annotated-output bugs found by the investigation are already fixed:

- The `max_failed_rows` cap was skipped whenever a row query mentioned
  `LIMIT`, for example in a `credit_limit` column. Only an outer `LIMIT`, `TOP`
  or `FETCH` now counts.
- `max_failed_rows=0` annotated every row as passing instead of warning.
- A zero-row failed-rows fetch lost its column names, so the check was treated
  as not mergeable.
- The merge mishandled NaN, `-0.0`, nested types, TIMESTAMP_NS and UBIGINT above
  2^63 (see [Merge equality](#merge-equality)).

## Risks and open questions

- **Key expressions for untested dialects.** BigQuery, Snowflake and SQL Server
  need a binary key entry, and each needs a round-trip test for floats
  and a collation test. Until then they are exact only where the keyless mask
  histogram counts the schema, and report `approximate = true` otherwise.
- **Declared primary keys.** A duplicated key would merge distinct rows (P6).
  The implementation guards it. The primary key is used only when a
  duplicate-key query over the binary keys returns 0. A check that holds the
  key but not every column reports `primary key has duplicate values` or
  `primary key uniqueness could not be checked` when the key is not used. The
  fallback is logged at debug level only, because the generated primary key
  check already reports the duplicates.
- **Cross-route equality.** The mixed route merges on normalised Python keys (see
  [Cross-route merge](#cross-route-merge)). Types that fail on export cannot be
  matched and mark the schema `approximate = true`.
- **Snapshots under chunking.** A live table can change between chunks. Engines
  with time travel (Snowflake, BigQuery, Delta) could pin one snapshot. Not in
  scope.
- **Trino stage count.** `query.max-stage-count` defaults to 150. Whether a
  250-branch chunk plans more stages than that is untested.
- **Export memory.** For full-table match checks the whole table is held in
  memory on the machine running vowl. At 6 columns: about 3 s and 1 to 1.5 GiB
  at 1M rows, about 13 s and 3.5 GiB at 5M rows. Large tables on a source
  without table match should set `row_counts="scalar"`, which never exports.
- **Transformed values that land on another row.** Both matching routes test
  each failed key against the table. A transformed value that equals another
  real row's key matches that row. vowl marks every check whose SELECT list
  holds more than plain table columns as not exact for this reason.
- **Connection lifetime.** Pushdown needs the adapter's connection after the run,
  as annotated output already does.
- **Backends tested.** DuckDB, SQLite, Spark 4.0.2 and Postgres 16 only.
  Databricks shares the Spark entry but was not tested separately. Spark 3.x,
  Snowflake, BigQuery and others are untested. An Ibis backend that vowl does
  not map to a dialect is rendered as Postgres. If it lacks `float8send` or
  `convert_to`, the preflight fails and its schemas go to the full-table match.

## Testing

`tests/test_row_quality.py` covers:

- Truth: schema and dimension numbers equal an independent
  `COUNT(*) WHERE p1 OR ... OR pN` on DuckDB and SQLite, on a fixture with
  duplicates, NULLs, overlapping checks, a `DISTINCT` check, a tolerated check
  and an inverted check.
- Adversarial cases: `-0.0` and NaN, nested and BLOB columns, data columns
  named like vowl's aliases, SQLite NOCASE and RTRIM and mixed types in one
  column, DuckDB INTERVAL and BIT columns (counted, with no annotated table),
  `DISTINCT`, join fan-out by table match, duplicates caught by several checks,
  NULLs, the `max_failed_rows` cap, and `-0.0` and NaN on Spark.
- Parity: annotated flagged rows equal `failed_rows` on the mixed fixture, on
  the join fan-out fixture, and on HDB resale (`HDBResaleWithErrors.csv`, 10,571
  failed rows, uniqueness 10,551, conformity 10 and consistency 12, unchanged
  with `max_failed_rows=1`).
- Certification: accepted and rejected shapes, the scalar `COUNT` rule, and
  every generated check certified. `COUNT(*) AS n` and `COUNT(col)` checks are
  pushed down and marked on annotated output.
- Chunking: identical results for chunk sizes 1, 63, 64 and 250, on a table
  with every row duplicated so copies spread across chunks.
- Scale: a 510-check schema runs on SQLite, which rejects more than 500
  branches in one statement.
- Probe: a failing branch is found by bisection and moves alone to its
  scalar.
- Single scan: the single-scan form gives the same numbers as the union form.
- Cap independence: identical results for `max_failed_rows` of -1, 0 and 1.
- Route mix: pushdown, table match and a cross-source full-table match in one
  schema, and a unit test of the Arrow merge onto the table.
- Primary key: a column-subset check merges through a declared primary key, and
  a duplicated primary key is not trusted.

`tests/test_row_quality_merge_key.py` covers the primary key as the default
match key: the same numbers as a contract without a key, the two primary key
reasons, one probe per schema, a table-match check with changed values that
matches on the key, and `has_match_key`. `tests/test_marks_match_counts.py`
checks that annotated output marks as many rows as `failed_rows` on every exact
schema, over duplicates, `attribute_tolerated_rows=True`, a column-subset check, a join that
projects the anchor's columns and mixed routes. It also covers a contract that
lists fewer columns than the table when the column types are unknown, which
reports `approximate = true`.

- Filter conditions: they apply to the total and to the rows.
- Operator rule: each operator in decision 3 is counted or excluded as specified.
- Tolerance: both `attribute_tolerated_rows` values on a check that passes with
  non-zero matches, in the numbers and in annotated output, and a passed check
  that runs no query under the default.
- Trust metadata: `approximate` is true under truncation and under an ERROR check in
  the bucket, empty dimensions and empty tables report missing, and non-row-level
  checks are left out.
- Statistics off, capped statistics no longer capping the total, the report
  computed once, the OTEL approximate attribute, and statement shapes that pass the
  security validator.

`tests/test_table_attributed_counts.py` covers
`row_counts`: the config default, the routes a mixed
schema takes, the scalar mode running no query, fetch or export, its capped and
overlapping sums over failed checks only, certified-only schemas never exporting, one export shared by
counting and annotated output, `DISTINCT` and join fan-out, a cross-source
join, transformed values reported as unmatched, a truncated local check
counted in SQL and a truncated cross-source check left not attributable, the
checks not attributable when the export or keying fails, the total taken from the export, NaN, `-0.0`,
NULL and case, every column type, and annotated output over checks that
return different Arrow types.

`tests/test_row_quality_postgres.py` runs the key cases on Postgres 16 with
testcontainers (Docker only): `-0.0`, NaN and close floats, a nondeterministic
case-insensitive collation, duplicates, table match, and boolean, `json` and
`bytea` columns next to a cross-source full-table match. Postgres has no `MIN` for
boolean, `bytea` or `json`, so the value aggregate there is
`(ARRAY_AGG(x))[1]`.

`tests/test_row_quality.py` also covers Spark UTF8_LCASE, where 'a' and 'A'
caught by different checks stay two rows. The test asserts the per-dimension
counts, because the single-scan form counts `COUNT(*)` per group and gives the
same schema total with a plain column key. The Employee parity test checks that
annotated output flags as many rows as `failed_rows` reports on both tables, once
with both tables on one connection (the joins go by table match) and once on two
connections (the joins go by the full-table match).

## Implementation

| Module in `src/vowl/validation/row_quality/` | Holds                                                                                                             |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `__init__.py`                                | `RowQuality`, the component cached on `ValidationResult`. It runs steps 1 to 5 per schema.                        |
| `selection.py`                               | Step 2, the operator rule (decision 3), the scope (decision 4) and the reason vocabulary                          |
| `certify.py`                                 | Certification                                                                                                     |
| `keys.py`                                    | Key expressions, null-safe equality and the primary key order                                                     |
| `pushdown.py`                                | Branches, chunk budgets, the statement builder, the probes, table match, the single-scan form and the chunk merge |
| `merge.py`                                   | The cross-route merge and the full-table match (`merge_onto_table`)                                               |
| `rollup.py`                                  | The report dataclasses and the schema, dimension, check and run rollups                                           |

Details that the sections above leave open:

- **Totals.** In priority order: the single-chunk statement's own count, the
  exported table's row count when the full-table match ran, the run's recorded total when the deprecated `max_rows_for_statistics` is -1, an uncapped
  `get_total_rows`, and last the capped recorded total with `approximate = true`.
  `get_total_rows` returns 0 on errors, so a recorded or fetched total of 0 is
  counted again with a direct `SELECT COUNT(*)` over the filtered table when the
  adapter supports `run_arrow_query`. A table whose total is lower than the rows
  one of its checks found is reported with `approximate = true`.
- **Pushdown needs** a SQL check whose row source ran in the anchor adapter's
  dialect, not across sources, on an adapter that implements `run_arrow_query`
  and `get_column_types`. Otherwise the check's rows are fetched.
- **Scalars are certified too.** A check on `server_scalar` that is not a pure
  row filter reports `approximate = true`, since `DISTINCT` or a join changes the
  copies.
- **Passed checks** are left out before routing under the default
  `attribute_tolerated_rows=False`, so they never reach a probe, a fetch or the
  export.
- **Unknown columns.** When neither the adapter nor the contract lists the
  table's columns, counting takes them from the exported table. When only the
  contract lists them, a check left out as not mergeable reports
  `approximate = true`, because the contract can list fewer columns than the table
  and annotated output compares with the exported table.
- **Attribution and `row_counts`.** `_assign_route` in `row_quality/__init__.py`
  picks each check's route, and `_fetch` fetches the rows
  of the full-table match checks. `_export_table` exports once through
  `ValidationResult._fetch_full_table`, and `_run_onto_table` counts the schema
  on the export. `_leave_lookup_unattributed` leaves a check that cannot be attributed out of
  the row counts as not attributable, and `_run_scalars` counts every failed check from its scalar count under
  `row_counts="scalar"`. The export warnings read
  `No adapter for schema %r, so its table cannot be exported.` and
  `Could not export the table of %r: %s`.
- **Annotated output** reads the same step 2 selection, so inverted checks are
  not flagged and tolerated checks are flagged only under
  `attribute_tolerated_rows=True`. It
  then merges the fetched rows in Python on `RowQuality.merge_key`, the same
  key the statistics chose. That is the full columns, or the declared primary
  key when the key was found unique.
- **OTEL.** The row gauges carry no trust attributes. A changing attribute
  would start a new series each time it flips. The flag is a separate 0/1
  gauge instead, `vowl.<level>.row.approximate`, sent next to each level's
  row counts with the attributes of its `row.pass_rate`, and as `0` when
  exact. At check level it is sent for every check with a scalar count and
  means the check made its schema approximate, the same as the span
  attribute, so it names the check. The `vowl.validate` span carries
  `vowl.row_quality.approximate` and `vowl.row_quality.checks_not_attributable`,
  and each `vowl.check` span and log record that gets row counts carries the
  check's `vowl.row_quality.approximate`, `route`, `reason` and
  `attributed_rows`. A gauge is left out when its number is
  missing. The row counts need a total and a failed count, and the rate also
  needs a non-empty table.
- **`print_summary`** adds `(approx.)` to a Passed Rows figure that is not
  exact, and prints `N/A` where a number is missing.
- **Statistics off.** With `row_counts="off"` (or the deprecated
  `enable_additional_schema_statistics=False`) the report is disabled. Every number is missing, `approximate` is true and no
  row-quality query runs.

## Not included

- Per-check overrides of the tolerance setting.
- Row-quality statistics for aggregate checks (averages, sums).
- Mapping column-subset checks back to table rows without a declared primary
  key.
