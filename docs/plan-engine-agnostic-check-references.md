# Plan: Engine-Agnostic Check References

## Problem

`TableCheckReference` and `ColumnCheckReference` currently inherit from `SQLCheckReference`, which hard-couples them to SQL query generation, dialect transpilation, and sqlglot AST manipulation. But the ODCS contract spec defines four check types — `sql`, `text`, `library`, and `custom` — and only `sql` is engine-specific. A contract's `quality[]` entry might specify `type: "library"` (with a `metric` field) or `type: "custom"` (with an `engine` + `implementation` field), yet the current code always wraps them in a SQL-aware reference that expects a `query` field.

### Current class hierarchy

```
CheckReference (ABC)                         ← check_reference_base.py
└── SQLCheckReference (ABC)                  ← check_reference_sql.py
    ├── TableCheckReference                  ← check_reference_sql.py
    ├── ColumnCheckReference                 ← check_reference_sql.py
    └── GeneratedColumnCheckReference (ABC)  ← check_reference_generated.py
        ├── DeclaredColumnExistsCheckReference
        ├── LogicalTypeCheckReference
        ├── LogicalTypeOptionsCheckReference
        ├── RequiredCheckReference
        ├── UniqueCheckReference
        └── PrimaryKeyCheckReference
```

Problems with this design:
1. **Tight coupling** — `TableCheckReference` and `ColumnCheckReference` inherit SQL utilities (`transpile`, `apply_filters`, `get_query`) they don't conceptually need.
2. **No dispatch on `type`** — `Contract.get_check_references_by_schema()` always creates `TableCheckReference` / `ColumnCheckReference` regardless of what `type` the quality entry declares.
3. **Unimplemented types silently pass** — `library` and `custom` checks get wrapped in a SQL reference, eventually failing at query-generation time (or worse, silently skipped).
4. **Can't extend** — Adding a new engine (e.g., Great Expectations, Soda, DQX) requires subclassing `SQLCheckReference`, which drags in irrelevant SQL machinery.

---

## Proposed Design

### Target class hierarchy

```
CheckReference (ABC)                              ← check_reference_base.py  (unchanged)
├── SQLCheckReference (ABC)                       ← check_reference_sql.py   (unchanged)
│   ├── SQLTableCheckReference                    ← check_reference_sql.py   (renamed)
│   ├── SQLColumnCheckReference                   ← check_reference_sql.py   (renamed)
│   └── GeneratedColumnCheckReference (ABC)       ← check_reference_generated.py (unchanged)
│       └── (DeclaredColumnExists, LogicalType, Required, Unique, PrimaryKey)
│
├── LibraryTableCheckReference                    ← check_reference_library.py (new)
├── LibraryColumnCheckReference                   ← check_reference_library.py (new)
│
├── CustomTableCheckReference                     ← check_reference_custom.py  (new)
├── CustomColumnCheckReference                    ← check_reference_custom.py  (new)
│
├── TextTableCheckReference                       ← check_reference_text.py    (new)
└── TextColumnCheckReference                      ← check_reference_text.py    (new)
```

### Core idea

- **`TableCheckReference` / `ColumnCheckReference` become abstract roles**, not concrete classes. The concrete class is chosen by the check's `type` field.
- **`SQLCheckReference` remains the SQL mixin** — dialect, transpiling, filter application, aggregation detection. Only SQL-typed checks inherit it.
- **New base mixins** for shared table-level / column-level navigation (schema name, column name resolution) are extracted if needed, or the patterns remain in the leaf classes since they're trivial.
- **`Contract.get_check_references_by_schema()` dispatches on `type`**, creating the right reference subclass.
- **Unsupported/unimplemented types** raise a clear `NotImplementedError` at reference creation time (not at execution time).

---

## Implementation Steps

### Phase 1 — Introduce scope-level mixins (non-breaking)

**Goal:** Extract the "I'm a table-level check" and "I'm a column-level check" navigation logic from the current `TableCheckReference` / `ColumnCheckReference` into reusable mixins. This avoids duplicating `get_schema_name()` / `get_column_name()` across SQL, library, and custom variants.

1. **Create `TableCheckMixin` and `ColumnCheckMixin`** in `check_reference_base.py`:
   - `TableCheckMixin`: implements `get_schema_name()` (resolve 1 level up), `get_schema_path()`.
   - `ColumnCheckMixin`: implements `get_schema_name()` (resolve 2 levels up), `get_schema_path()`, `get_column_name()`, `get_logical_type()`, `get_logical_type_options()`.
   - These are plain mixins (no ABC), providing only the JSONPath navigation methods that all flavours share.

2. **Refactor existing SQL references** to use the mixins:
   - `SQLTableCheckReference(TableCheckMixin, SQLCheckReference)` — replaces `TableCheckReference`.
   - `SQLColumnCheckReference(ColumnCheckMixin, SQLCheckReference)` — replaces `ColumnCheckReference`.
   - Rename directly — no deprecated aliases. Update all references across the codebase.

### Phase 2 — Type-dispatched factory in Contract

**Goal:** `Contract.get_check_references_by_schema()` creates the correct reference subclass based on the `type` field.

3. **Add a factory function** `_create_check_reference(contract, check_path, scope)` that:
   - Resolves the quality dict at `check_path`.
   - Reads `type` (`"sql"` | `"library"` | `"custom"` | `"text"`).
   - Returns the scope-appropriate reference:
     | `type` | table-level | column-level |
     |--------|-------------|--------------|
     | `sql` | `SQLTableCheckReference` | `SQLColumnCheckReference` |
     | `library` | `LibraryTableCheckReference` | `LibraryColumnCheckReference` |
     | `custom` | `CustomTableCheckReference` | `CustomColumnCheckReference` |
     | `text` | `TextTableCheckReference` | `TextColumnCheckReference` |
     | unknown | raise `NotImplementedError` | raise `NotImplementedError` |

4. **Update `get_check_references_by_schema()`** to call the factory instead of hard-coding `TableCheckReference` / `ColumnCheckReference`:
   ```python
   # Before
   refs_by_schema[schema_name].append(
       TableCheckReference(self, check_path)
   )
   
   # After
   refs_by_schema[schema_name].append(
       _create_check_reference(self, check_path, scope="table")
   )
   ```

5. **Auto-generated checks stay SQL-only.** `GeneratedColumnCheckReference` subclasses continue inheriting from `SQLCheckReference`, since they produce SQL queries by construction.

### Phase 3 — Stub implementations for non-SQL types

**Goal:** Define the new reference classes with enough structure to be dispatched and produce useful error results when no executor is registered.

6. **`check_reference_library.py`** — new file:
   - `LibraryCheckReference(CheckReference, ABC)`: base for library-metric checks. Exposes `get_metric()` → returns the `metric` dict from the quality entry.
   - `LibraryTableCheckReference(TableCheckMixin, LibraryCheckReference)`.
   - `LibraryColumnCheckReference(ColumnCheckMixin, LibraryCheckReference)`.

7. **`check_reference_custom.py`** — new file:
   - `CustomCheckReference(CheckReference, ABC)`: base for custom-engine checks. Exposes `get_engine()` → `str`, `get_implementation()` → `str | dict`.
   - `CustomTableCheckReference(TableCheckMixin, CustomCheckReference)`.
   - `CustomColumnCheckReference(ColumnCheckMixin, CustomCheckReference)`.

8. **`check_reference_text.py`** — new file:
   - `TextCheckReference(CheckReference)`: informational-only, no execution. `is_generated()` → `False`, `supports_row_level_output` → `False`.
   - `TextTableCheckReference(TableCheckMixin, TextCheckReference)`.
   - `TextColumnCheckReference(ColumnCheckMixin, TextCheckReference)`.
   - Text checks should be explicitly skipped (or returned as INFO) during execution.

### Phase 4 — Executor / Adapter integration for unimplemented engines

**Goal:** When a contract contains `library` or `custom` checks and no matching executor is registered, the system should return an informative `ERROR` result, not crash.

9. **`BaseAdapter.run_checks()` already handles `NotImplementedError`** from `_get_executor()` and returns `ERROR` results. Verify this path works for the new reference types (it should — it groups by `check.get("type")`).

10. **Add a `TextExecutor`** (or handle in adapter) that converts text checks to `INFO`-status results without execution:
    ```python
    class TextExecutor(BaseExecutor):
        def run_single_check(self, ref):
            return CheckResult(
                check_name=ref.get_check_name(),
                status="INFO",
                details=ref.get_check().get("description", "Text-only check, no execution"),
                execution_time_ms=0,
            )
    ```

11. **Register `TextExecutor` in adapters** alongside the SQL executor:
    ```python
    self._executors = {"sql": IbisSQLExecutor, "text": TextExecutor}
    ```

### Phase 5 — Barrel re-exports and rename cleanup

12. **Update `check_reference.py`** (barrel file) to re-export all new classes from the new modules.

13. **Rename all references** to `TableCheckReference` → `SQLTableCheckReference` and `ColumnCheckReference` → `SQLColumnCheckReference` across the entire codebase (tests, executors, adapters, contract.py).

### Phase 6 — Tests

14. **Unit tests for the factory dispatch** — contract with mixed `type` values produces the right reference classes.
15. **Unit tests for `LibraryCheckReference.get_metric()`** and **`CustomCheckReference.get_engine()`**.
16. **Integration test** — contract with `type: "custom"` checks returns `ERROR` results with a clear message when no custom executor is registered.


---

## Renames

| Symbol | Change |
|--------|--------|
| `TableCheckReference` | Renamed to `SQLTableCheckReference` — all references updated directly |
| `ColumnCheckReference` | Renamed to `SQLColumnCheckReference` — all references updated directly |
| `SQLCheckReference` | Unchanged |
| `GeneratedColumnCheckReference` | Unchanged |
| `CheckReference` | Unchanged |
| `Contract.get_check_references_by_schema()` | Returns mixed reference types — callers that assume all refs are SQL need updating |

---

## Files Affected

| File | Action |
|------|--------|
| `src/vowl/contracts/check_reference_base.py` | Add `TableCheckMixin`, `ColumnCheckMixin` |
| `src/vowl/contracts/check_reference_sql.py` | Rename classes, use mixins |
| `src/vowl/contracts/check_reference_library.py` | **New** — library check references |
| `src/vowl/contracts/check_reference_custom.py` | **New** — custom engine check references |
| `src/vowl/contracts/check_reference_text.py` | **New** — text (info-only) check references |
| `src/vowl/contracts/check_reference.py` | Update barrel exports, add aliases |
| `src/vowl/contracts/contract.py` | Factory dispatch in `get_check_references_by_schema()` |
| `src/vowl/executors/base.py` | Add `TextExecutor` |
| `src/vowl/adapters/ibis_adapter.py` | Register `TextExecutor` |
| `src/vowl/adapters/multi_source_adapter.py` | Register `TextExecutor` |
| `test/` | New and updated tests |

---

## Out of Scope (for now)

- **Implementing `LibraryExecutor`** — requires defining the ODCS metrics-to-execution mapping. The reference classes will be ready, but execution will return `ERROR` until an executor is registered.
- **Implementing `CustomExecutor`** — requires a plugin/dispatch mechanism for arbitrary engines. Same approach: reference classes ready, execution errors until registered.
- **Multi-engine contracts** — a single contract mixing SQL + custom checks already works through the `refs_by_type` grouping in `BaseAdapter.run_checks()`. No additional work needed.
