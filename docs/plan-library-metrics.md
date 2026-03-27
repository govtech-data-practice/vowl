# Plan: Implement Library Metrics as Generated SQL Checks

## Context

ODCS `type: "library"` checks define data quality rules using standardised **metric** names
(`nullValues`, `missingValues`, `invalidValues`, `duplicateValues`, `rowCount`) instead of
raw SQL.  Every one of these metrics maps directly to a SQL pattern, so we can implement
them as **generated SQL check references** — the same mechanism used today for `logicalType`,
`required`, `unique`, and `primaryKey` auto-checks.

The existing `LibraryTableCheckReference` / `LibraryColumnCheckReference` stub classes
(from the engine-agnostic refactor) currently inherit from `CheckReference` only and carry
no SQL capability.  This plan replaces them with SQL-backed generated classes that produce
the right query from the metric definition.

---

## Metric → SQL Mapping

### Property-level (column) metrics

| Metric | SQL Pattern | Supports `unit: percent` |
|--------|-------------|--------------------------|
| `nullValues` | `SELECT COUNT(*) FROM T WHERE col IS NULL` | Yes |
| `missingValues` | `SELECT COUNT(*) FROM T WHERE col IS NULL OR col IN (…missingValues)` | Yes |
| `invalidValues` (validValues) | `SELECT COUNT(*) FROM T WHERE col NOT IN (…validValues)` | Yes |
| `invalidValues` (pattern) | `SELECT COUNT(*) FROM T WHERE NOT REGEXP_LIKE(col, pattern)` | Yes |
| `duplicateValues` | `SELECT COUNT(*) FROM (SELECT col FROM T GROUP BY col HAVING COUNT(*) > 1)` | Yes |

### Schema-level (table) metrics

| Metric | SQL Pattern | Supports `unit: percent` |
|--------|-------------|--------------------------|
| `rowCount` | `SELECT COUNT(*) FROM T` | No (absolute only) |
| `duplicateValues` | `SELECT COUNT(*) FROM (SELECT col1, col2, … FROM T GROUP BY col1, col2, … HAVING COUNT(*) > 1)` | Yes |

### `unit: percent` handling

When `unit: "percent"`, the query must return a percentage instead of a raw count.
Wrap the core count in: `SELECT (core_count * 100.0) / NULLIF(total_count, 0)`

- `total_count` = `SELECT COUNT(*) FROM T`
- `core_count` = the metric-specific count query

The operators (`mustBe`, `mustBeLessThan`, etc.) then compare against this percentage
value directly — no special handling needed in `build_result()`, since `CheckReference.evaluate()`
already does numeric comparison.

`NULLIF(…, 0)` prevents division-by-zero on empty tables (returns NULL → comparison
fails → `FAILED` result, which is a reasonable outcome for an empty table with a
percentage constraint).

---

## Class Design

### New base: `GeneratedTableCheckReference`

Currently all generated checks are column-level (`GeneratedColumnCheckReference`).
The schema-level metrics (`rowCount`, `duplicateValues`) need an equivalent base
for table-level generated checks.

```
GeneratedTableCheckReference(SQLCheckReference, ABC)
    ├── path points to: $.schema[i].quality[j]
    ├── get_schema_name() → resolve 1 level up
    ├── get_schema_path() → resolve_parent(path, 1)
    ├── is_generated() → True
    ├── get_check() → returns the synthetic DataQuality from _generate_check()
    ├── get_query() → builds AST, renders, applies filters/try_cast
    └── abstract: _build_ast(), _generate_check()
```

### New metric check reference classes

All in a new file `check_reference_library_metrics.py`:

| Class | Base | Level | Metric |
|-------|------|-------|--------|
| `NullValuesCheckReference` | `GeneratedColumnCheckReference` | property | `nullValues` |
| `MissingValuesCheckReference` | `GeneratedColumnCheckReference` | property | `missingValues` |
| `InvalidValuesCheckReference` | `GeneratedColumnCheckReference` | property | `invalidValues` |
| `DuplicateValuesColumnCheckReference` | `GeneratedColumnCheckReference` | property | `duplicateValues` |
| `RowCountCheckReference` | `GeneratedTableCheckReference` | schema | `rowCount` |
| `DuplicateValuesTableCheckReference` | `GeneratedTableCheckReference` | schema | `duplicateValues` |

Each class:
1. Reads `metric`, `arguments`, `unit` from the quality entry at its JSONPath
2. Implements `_build_ast()` to construct the sqlglot AST for the metric
3. Implements `_generate_check()` to return a synthetic `DataQuality` dict
4. Handles `unit: "percent"` by wrapping the core AST in the percentage formula

### `__init__` signatures

**Column-level** metrics follow the existing `GeneratedColumnCheckReference` pattern:
```python
def __init__(self, contract, quality_path: str, property_path: str):
    # quality_path = "$.schema[i].properties[j].quality[k]"
    # property_path = "$.schema[i].properties[j]"
```

Note: unlike existing generated checks (which derive from a property attribute
like `logicalType`), library metrics are defined in `quality[]` entries.
The `path` for the reference should be the quality entry path itself (`quality_path`),
and `_property_path` should point to the parent property for column/schema resolution.

**Schema-level** metrics:
```python
def __init__(self, contract, quality_path: str, schema_path: str):
    # quality_path = "$.schema[i].quality[j]"
    # schema_path = "$.schema[i]"
```

---

## Implementation Steps

### Step 1 — Add `GeneratedTableCheckReference` base

Add to `check_reference_generated.py` (alongside the existing `GeneratedColumnCheckReference`):

```python
class GeneratedTableCheckReference(SQLCheckReference, ABC):
    """Base class for auto-generated table-level checks."""

    def __init__(self, contract, quality_path: str):
        super().__init__(contract, quality_path)
        self._generated_check = None
        self._cached_ast = None

    @abstractmethod
    def _build_ast(self) -> exp.Expression: ...

    @abstractmethod
    def _generate_check(self) -> DataQuality: ...

    def get_check(self):
        if self._generated_check is None:
            self._generated_check = self._generate_check()
        return self._generated_check

    def get_query(self, dialect, filter_conditions=None, use_try_cast=False):
        ast = self._build_ast()
        query = self._render_sql(ast, dialect)
        if filter_conditions:
            query = self.apply_filters(query, dialect, filter_conditions)
        if use_try_cast:
            query, _ = self.apply_try_cast(query, dialect)
        return query

    def get_schema_name(self):
        return self._contract.resolve(f"{self.get_schema_path()}.name")

    def get_schema_path(self):
        return self._contract.resolve_parent(self._path, levels=1)

    def is_generated(self) -> bool:
        return True
```

### Step 2 — Create `check_reference_library_metrics.py`

New file with six metric classes. Each follows this template:

```python
class NullValuesCheckReference(GeneratedColumnCheckReference):
    """Library metric: nullValues — counts NULL values in a column."""

    def __init__(self, contract, quality_path, property_path):
        # Path points to the quality entry itself
        super(GeneratedColumnCheckReference, self).__init__(contract, quality_path)
        self._property_path = property_path
        self._generated_check = None
        self._cached_ast = None

    def get_check(self):
        # Return the ORIGINAL quality entry from the contract (not synthetic)
        # because it already has the metric, operators, unit, etc.
        return self._contract.resolve(self._path)

    def _build_ast(self):
        if self._cached_ast is not None:
            return self._cached_ast
        col = ...  # build column/table references
        self._cached_ast = ...  # build the SQL AST
        return self._cached_ast

    def _generate_check(self):
        # Not needed — get_check() returns the real quality entry
        return self.get_check()

    def is_generated(self) -> bool:
        return True
```

Key design point: unlike the existing generated checks which create **synthetic**
`DataQuality` dicts (with generated `mustBe: 0`), library metrics use the
**original** quality entry from the contract. The operator and expected value
(`mustBe`, `mustBeLessThan`, etc.) come from the user's contract YAML, not from
code. The generated part is only the SQL query.

#### Percent wrapper utility

Add a shared helper for `unit: "percent"`:

```python
def _wrap_percent(core_count_ast, table):
    """Wrap a count AST to return a percentage of total rows."""
    total = sqlglot.select(exp.Count(this=exp.Star())).from_(table)
    return sqlglot.select(
        exp.Mul(
            this=exp.Paren(this=core_count_ast.subquery()),
            expression=exp.Div(
                this=exp.Literal.number(100.0),
                expression=exp.Anonymous(
                    this="NULLIF", expressions=[total.subquery(), exp.Literal.number(0)]
                ),
            ),
        )
    )
```

### Step 3 — Update factory dispatch in `contract.py`

For `type: "library"` checks, instead of creating `LibraryTableCheckReference` /
`LibraryColumnCheckReference`, dispatch on the `metric` field:

```python
LIBRARY_COLUMN_METRICS = {
    "nullValues": NullValuesCheckReference,
    "missingValues": MissingValuesCheckReference,
    "invalidValues": InvalidValuesCheckReference,
    "duplicateValues": DuplicateValuesColumnCheckReference,
}

LIBRARY_TABLE_METRICS = {
    "rowCount": RowCountCheckReference,
    "duplicateValues": DuplicateValuesTableCheckReference,
}
```

For table-level:
```python
if check_type == "library":
    metric = quality_entry.get("metric")
    cls = LIBRARY_TABLE_METRICS.get(metric)
    if cls is None:
        raise NotImplementedError(f"Unsupported library metric '{metric}' at table level")
    refs_by_schema[schema_name].append(cls(self, check_path))
```

For column-level:
```python
if check_type == "library":
    metric = quality_entry.get("metric")
    cls = LIBRARY_COLUMN_METRICS.get(metric)
    if cls is None:
        raise NotImplementedError(f"Unsupported library metric '{metric}' at column level")
    refs_by_schema[schema_name].append(cls(self, check_path, prop_path))
```

Since these are SQL-generated references (they inherit from `SQLCheckReference`),
they have `type: "library"` in their check data but produce SQL queries.
The executor dispatch groups by `check.get("type")` — so we need to ensure
the generated check's type is `"sql"` or register the SQL executor for `"library"` too.

**Resolution:** The simplest approach is to register `IbisSQLExecutor` for both
`"sql"` and `"library"` check types in the adapter:
```python
self._executors = {"sql": IbisSQLExecutor, "library": IbisSQLExecutor, "text": TextExecutor}
```

### Step 4 — Register `library` executor type in adapters

In `IbisAdapter.__init__()` and `MultiSourceAdapter.__init__()`:
```python
self._executors = {
    "sql": IbisSQLExecutor,
    "library": IbisSQLExecutor,
    "text": TextExecutor,
}
```

### Step 5 — Update barrel exports

Add the new classes to `check_reference.py` and `check_reference_generated.py` `__all__`.

### Step 6 — Clean up `check_reference_library.py`

The existing stub `LibraryCheckReference`, `LibraryTableCheckReference`,
`LibraryColumnCheckReference` are no longer needed since library metrics are now
SQL-generated. Either:
- **Remove** them entirely (since library metrics are fully handled by the generated classes), or
- **Keep** them as fallback for unknown metrics (but this is unlikely to be useful).

Recommendation: remove them. The factory in `contract.py` raises `NotImplementedError`
for unknown metrics, which is clearer than creating a non-executable reference.

### Step 7 — Tests

| Test | Description |
|------|-------------|
| `test_null_values_metric` | Contract with `metric: nullValues` → produces correct SQL, handles `unit: percent` |
| `test_missing_values_metric` | Verifies `missingValues` arguments are included in SQL |
| `test_invalid_values_valid_values` | `invalidValues` with `validValues` argument |
| `test_invalid_values_pattern` | `invalidValues` with `pattern` argument |
| `test_duplicate_values_column` | Property-level `duplicateValues` |
| `test_duplicate_values_schema` | Schema-level `duplicateValues` with `properties` argument |
| `test_row_count_metric` | Schema-level `rowCount` |
| `test_percent_unit` | Same metric with `unit: percent` returns percentage query |
| `test_unknown_metric_raises` | Unknown metric name raises `NotImplementedError` |
| Integration test | Full `validate()` pipeline with library metrics against DuckDB |

---

## Files Affected

| File | Action |
|------|--------|
| `src/vowl/contracts/check_reference_generated.py` | Add `GeneratedTableCheckReference` base class |
| `src/vowl/contracts/check_reference_library_metrics.py` | **New** — all 6 metric check reference classes |
| `src/vowl/contracts/check_reference_library.py` | **Remove** (stubs replaced by generated classes) |
| `src/vowl/contracts/check_reference.py` | Update barrel exports |
| `src/vowl/contracts/contract.py` | Metric-aware factory dispatch for `type: "library"` |
| `src/vowl/adapters/ibis_adapter.py` | Register `IbisSQLExecutor` for `"library"` type |
| `src/vowl/adapters/multi_source_adapter.py` | Register executor for `"library"` type |
| `test/` | New metric tests |

---

## Open Questions

1. **`invalidValues` with both `validValues` and `pattern`** — should both be applied
   (AND)? The ODCS docs show them as separate examples. Proposal: support each
   independently; if both are present, combine with OR (invalid if not in valid set
   OR doesn't match pattern). This matches the name "invalidValues" — counting values
   that are invalid by any criterion.

2. **`missingValues` default list** — if `arguments.missingValues` is not provided,
   should we default to `[NULL]` only (making it equivalent to `nullValues`)? The
   ODCS spec implies arguments are required for `missingValues`. Proposal: require
   `arguments.missingValues`, raise `ValueError` if absent.

3. **`duplicateValues` at schema level** — the `arguments.properties` field lists
   column names as strings. These need to be resolved to actual column references.
   Proposal: use the property names directly as SQL column identifiers (with quoting).

4. **`unit: percent` for `duplicateValues`** — percentage of what? Total rows or
   total distinct values? Proposal: percentage of total rows, consistent with other
   metrics.
