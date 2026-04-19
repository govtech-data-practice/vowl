# Check Reference Variations — Test Plan

## Goal

Construct all meaningful variations of check references via YAML contracts and
verify that:

1. The correct `CheckReference` subclass is instantiated.
2. `get_expected_value()` returns the right `(operator, expected_value)`.
3. `evaluate()` produces the correct PASSED / FAILED for each operator.
4. Generated SQL is syntactically valid (can be transpiled to DuckDB and executed).
5. `build_result()` yields the expected status.

---

## Axis 1 — Check Reference Types (17 concrete classes)

### User-defined SQL checks
| # | Class | Level | Source |
|---|-------|-------|--------|
| 1 | `SQLTableCheckReference` | table | `schema[].quality[]` with `type: sql` |
| 2 | `SQLColumnCheckReference` | column | `schema[].properties[].quality[]` with `type: sql` |

### User-defined custom checks
| # | Class | Level | Source |
|---|-------|-------|--------|
| 3 | `CustomTableCheckReference` | table | `type: custom` |
| 4 | `CustomColumnCheckReference` | column | `type: custom` |

### Auto-generated from property attributes
| # | Class | Trigger |
|---|-------|---------|
| 5 | `DeclaredColumnExistsCheckReference` | every declared property |
| 6 | `LogicalTypeCheckReference` | `logicalType` present |
| 7 | `LogicalTypeOptionsCheckReference` | each key in `logicalTypeOptions` |
| 8 | `RequiredCheckReference` | `required: true` |
| 9 | `UniqueCheckReference` | `unique: true` |
| 10 | `PrimaryKeyCheckReference` | `primaryKey: true` |

### Library metrics — column-level
| # | Class | Metric name |
|---|-------|-------------|
| 11 | `NullValuesCheckReference` | `nullValues` |
| 12 | `MissingValuesCheckReference` | `missingValues` |
| 13 | `InvalidValuesCheckReference` | `invalidValues` |
| 14 | `DuplicateValuesColumnCheckReference` | `duplicateValues` |

### Library metrics — table-level
| # | Class | Metric name |
|---|-------|-------------|
| 15 | `RowCountCheckReference` | `rowCount` |
| 16 | `DuplicateValuesTableCheckReference` | `duplicateValues` |

### Unsupported / error
| # | Class | Trigger |
|---|-------|---------|
| 17 | `UnsupportedTableCheckReference` | unknown `type` at table level |
| 18 | `UnsupportedColumnCheckReference` | unknown `type` / unknown metric at column level |

---

## Axis 2 — Operators (8 total)

Each user-defined and library-metric check can carry exactly one of:

| Operator | Expected value type |
|----------|-------------------|
| `mustBe` | scalar |
| `mustNotBe` | scalar |
| `mustBeGreaterThan` | scalar |
| `mustBeGreaterOrEqualTo` | scalar |
| `mustBeLessThan` | scalar |
| `mustBeLessOrEqualTo` | scalar |
| `mustBeBetween` | `[low, high]` |
| `mustNotBeBetween` | `[low, high]` |

---

## Axis 3 — Library metric arguments / modes

| Metric | Argument variations |
|--------|-------------------|
| `nullValues` | plain / `unit: percent` |
| `missingValues` | no args (null only) / explicit `missingValues` list |
| `invalidValues` | `validValues` only / `pattern` only / both |
| `duplicateValues` (column) | plain / `unit: percent` |
| `duplicateValues` (table) | `arguments.properties: [cols]` |
| `rowCount` | no args (just operator) |

---

## Axis 4 — logicalTypeOptions keys (10)

`minLength`, `maxLength`, `pattern`, `minimum`, `maximum`,
`exclusiveMinimum`, `exclusiveMaximum`, `multipleOf`, `format`

Format sub-variations: integer formats (`i8`..`u64`), string formats
(`uuid`, `email`, `ipv4`, `ipv6`, `hostname`, `uri`), date/time JDK
patterns, non-validatable formats (`password`, `byte`, `binary`, `f32`, `f64`).

---

## Test Matrix

### Group A — Operator × evaluate() (parametrized, no DB needed)

Test `CheckReference.evaluate()` with all 8 operators for PASS and FAIL cases.
16 parametrized cases.

### Group B — Operator × SQL check reference (table + column)

For each of the 8 operators, construct a minimal YAML contract with a
`type: sql` check at both table and column level. Verify:
- Correct class instantiation
- `get_expected_value()` returns correct tuple
- `build_result()` returns PASSED/FAILED correctly

16 cases (8 operators × 2 levels).

### Group C — Operator × library metric (representative)

Use `nullValues` as the representative metric. Apply all 8 operators to it
and verify `get_expected_value()` + `build_result()`.

8 cases.

### Group D — All library metric variations

For each library metric, construct the YAML contract and verify:
- Correct class is created
- SQL is generated and valid
- Percentage mode works where applicable

Sub-cases:
- `nullValues` — plain + percent (2)
- `missingValues` — default + explicit list (2)
- `invalidValues` — validValues only + pattern only + both (3)
- `duplicateValues` column — plain + percent (2)
- `rowCount` — basic (1)
- `duplicateValues` table — with properties (1)

11 cases.

### Group E — Auto-generated attribute checks

Verify the full set of auto-generated checks from a single well-decorated
property. Construct a contract with one column that has: `logicalType`,
`required`, `unique`, `primaryKey`, and `logicalTypeOptions` with several
keys. Assert the expected set of `CheckReference` subclasses is produced.

1 integration case + parametrized per-attribute cases.

### Group F — Unsupported / unknown types

- Table-level unknown type → `UnsupportedTableCheckReference`
- Column-level unknown type → `UnsupportedColumnCheckReference`
- Column-level unknown metric → `UnsupportedColumnCheckReference`

3 cases.

### Group G — Custom engine checks

- Table-level custom check → `CustomTableCheckReference`
- Column-level custom check → `CustomColumnCheckReference`
- Verify `get_engine()` and `get_implementation()` accessors.

2 cases.

### Group H — Edge cases

- No operator specified → `("unknown", None)`, `evaluate()` returns `False`
- `mustBeBetween` / `mustNotBeBetween` at boundaries (inclusive edges)
- Multiple quality blocks on one table/column (ordering preserved)
- `severity` and `dimension` metadata round-trip through `get_result_metadata()`

~6 cases.

---

## Estimated total: ~63 test cases

All tests will be placed in `tests/test_check_reference_variations.py`.
