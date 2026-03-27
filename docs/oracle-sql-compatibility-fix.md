# Oracle SQL Compatibility — Root Cause Analysis & Fix Proposal

## Problem Summary

Multiple Oracle checks fail with `ORA-00907` (missing right parenthesis), `ORA-00911` (invalid character), and `ORA-22849` (CLOB not supported). The root cause is that sqlglot's Oracle dialect transpilation produces SQL constructs that Oracle does not support.

## Root Causes

### 1. `TRY_CAST` — Oracle has no `TRY_CAST` function (ORA-00907)

**Affected checks:** LogicalTypeCheck, LogicalTypeOptionsCheck (pattern, minLength, maxLength, minimum, maximum), InvalidValuesCheck, and any check where `apply_try_cast` runs.

sqlglot renders `exp.TryCast` as `TRY_CAST(... AS ...)` for Oracle, but Oracle does **not** have `TRY_CAST`. This is the primary source of `ORA-00907` errors.

**Two code paths produce TryCast:**

1. **`_build_ast()` in generated checks** — Directly constructs `exp.TryCast` nodes (e.g., `exp.TryCast(this=col, to=exp.DataType.build("VARCHAR"))`). These go through `_render_sql()` → `_apply_dialect_transforms()` → `.sql(dialect)`.

2. **`apply_try_cast()` in `check_reference_sql.py`** — Takes an already-rendered SQL string, parses it, and converts `exp.Cast` → `exp.TryCast`. This runs *after* `_render_sql()`, so the output bypasses `_apply_dialect_transforms()`.

**Oracle equivalent:** `CAST(expr AS type DEFAULT NULL ON CONVERSION ERROR)` (Oracle 12c R2+). sqlglot supports generating this syntax via `exp.Cast(..., default=exp.Null())`.

### 2. `TEXT` → `CLOB` type mapping (ORA-22849)

**Affected checks:** User-written SQL checks that use `CAST(col AS TEXT)` in postgres dialect.

sqlglot maps `TEXT` → `CLOB` for Oracle. CLOB cannot be used with:
- `REGEXP_LIKE()`
- `LENGTH()`
- Most comparison operators

This causes `ORA-22849: Type CLOB is not supported for this function or operator`.

**Example from contract YAML (user-written SQL):**
```sql
-- Internal (postgres):
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT REGEXP_LIKE(CAST(block AS TEXT), '^[A-Za-z0-9]{1,10}$')

-- sqlglot Oracle output:
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT REGEXP_LIKE(CAST(block AS CLOB), '^[A-Za-z0-9]{1,10}$')
-- ❌ ORA-22849: CLOB not supported for REGEXP_LIKE
```

Generated checks avoid this because they use `exp.DataType.build("VARCHAR")` which maps to `VARCHAR2` in Oracle. But any user SQL with `CAST(... AS TEXT)` hits this issue.

### 3. Underscore-prefixed subquery aliases (ORA-00911)

**Affected checks:** `DeclaredColumnExistsCheckReference`, `DuplicateValuesTableCheckReference`, `get_scalar_query()`.

Oracle requires unquoted identifiers to start with a letter (A-Z). Aliases like `_vowl_column_exists`, `_dup`, `_sub` are invalid in Oracle without quoting.

sqlglot renders: `(...) _vowl_column_exists` → `ORA-00911: _: invalid character`

## Proposed Fix

Add Oracle-specific AST transforms in `_DIALECT_AST_TRANSFORMS["oracle"]` (in `check_reference_sql.py`) and adjust `apply_try_cast()` for Oracle.

### Fix 1: Oracle AST transforms in `_DIALECT_AST_TRANSFORMS`

Add to `_DIALECT_AST_TRANSFORMS` in `check_reference_sql.py`:

```python
_DIALECT_AST_TRANSFORMS: Dict[str, list] = {
    "sqlite": [ ... ],  # existing
    "oracle": [
        # (a) TryCast → Cast with DEFAULT NULL ON CONVERSION ERROR
        lambda ast: ast.transform(
            lambda node: (
                exp.Cast(
                    this=node.this.copy(),
                    to=node.to.copy(),
                    default=exp.Null(),
                )
                if isinstance(node, exp.TryCast)
                else node
            )
        ),
        # (b) TEXT/CLOB → VARCHAR in Cast nodes to avoid ORA-22849
        lambda ast: ast.transform(
            lambda node: (
                node.copy().transform(
                    lambda n: n
                ).replace(node)  # simpler approach below
                if isinstance(node, (exp.Cast, exp.TryCast))
                and node.to
                and node.to.this == exp.DataType.Type.TEXT
                else node
            )
        ),
        # (c) Quote underscore-prefixed subquery aliases
        lambda ast: ast.transform(
            lambda node: (
                exp.Subquery(
                    this=node.this,
                    alias=exp.to_identifier(node.alias, quoted=True),
                )
                if isinstance(node, exp.Subquery)
                and node.alias
                and isinstance(node.alias, str)
                and node.alias.startswith("_")
                else node
            )
        ),
    ],
}
```

Cleaner implementation (recommended):

```python
def _oracle_trycast_to_safe_cast(ast: exp.Expression) -> exp.Expression:
    """Convert TryCast → CAST ... DEFAULT NULL ON CONVERSION ERROR."""
    return ast.transform(
        lambda node: (
            exp.Cast(this=node.this.copy(), to=node.to.copy(), default=exp.Null())
            if isinstance(node, exp.TryCast)
            else node
        )
    )


def _oracle_text_to_varchar(ast: exp.Expression) -> exp.Expression:
    """Replace TEXT (→ CLOB) with VARCHAR (→ VARCHAR2) in Cast nodes."""
    def _transform(node):
        if isinstance(node, exp.Cast) and node.to and node.to.this == exp.DataType.Type.TEXT:
            new = node.copy()
            new.set("to", exp.DataType.build("VARCHAR"))
            return new
        return node
    return ast.transform(_transform)


def _oracle_quote_underscore_aliases(ast: exp.Expression) -> exp.Expression:
    """Quote subquery aliases starting with underscore for Oracle."""
    def _transform(node):
        if isinstance(node, exp.Subquery):
            alias_node = node.args.get("alias")
            if alias_node and isinstance(alias_node, exp.TableAlias):
                ident = alias_node.this
                if isinstance(ident, exp.Identifier) and not ident.quoted and ident.name.startswith("_"):
                    new_alias = exp.TableAlias(this=exp.to_identifier(ident.name, quoted=True))
                    new_node = node.copy()
                    new_node.set("alias", new_alias)
                    return new_node
        return node
    return ast.transform(_transform)
```

Register them:
```python
_DIALECT_AST_TRANSFORMS: Dict[str, list] = {
    "sqlite": [...],
    "oracle": [
        _oracle_trycast_to_safe_cast,
        _oracle_text_to_varchar,
        _oracle_quote_underscore_aliases,
    ],
}
```

### Fix 2: Modify `apply_try_cast()` for Oracle

`apply_try_cast()` runs **after** `_render_sql()` and produces new `TryCast` nodes that bypass `_apply_dialect_transforms()`. For Oracle, it should produce `Cast(..., default=exp.Null())` instead of `TryCast(...)`.

In `apply_try_cast()` (around line 401 of `check_reference_sql.py`):

```python
@classmethod
def apply_try_cast(cls, query: str, dialect: str) -> Tuple[str, bool]:
    try:
        parsed = sqlglot.parse_one(query, dialect=dialect)
        modified = False
        is_oracle = dialect == "oracle"

        for cast_node in list(parsed.find_all(exp.Cast)):
            if is_oracle:
                if not cast_node.args.get("default"):
                    safe_cast = exp.Cast(
                        this=cast_node.this.copy(),
                        to=cast_node.to.copy(),
                        default=exp.Null(),
                    )
                    cast_node.replace(safe_cast)
                    modified = True
            else:
                try_cast = exp.TryCast(this=cast_node.this.copy(), to=cast_node.to.copy())
                cast_node.replace(try_cast)
                modified = True

        # ... rest of comparison wrapping logic, using Cast+default for Oracle
        # instead of TryCast ...
```

### Fix 3: Quote underscore aliases in generated checks

In `check_reference_generated.py`, quote the subquery aliases:

```python
# DeclaredColumnExistsCheckReference._build_ast():
inner_query.subquery(alias=exp.to_identifier("_vowl_column_exists", quoted=True))
```

Similarly for `_dup`, `_sub`, `_cnt`, `_tot` aliases in `check_reference_library_metrics.py`.

Alternatively, rename the aliases to not start with underscore (e.g., `vowl_column_exists` instead of `_vowl_column_exists`).

## Summary of Changes

| File | Change | Fixes |
|------|--------|-------|
| `check_reference_sql.py` | Add `"oracle"` to `_DIALECT_AST_TRANSFORMS` with TryCast→SafeCast and TEXT→VARCHAR transforms | ORA-00907 (TRY_CAST), ORA-22849 (CLOB) |
| `check_reference_sql.py` | Modify `apply_try_cast()` to use `Cast(..., default=Null())` for Oracle | ORA-00907 from apply_try_cast path |
| `check_reference_generated.py` | Quote `_vowl_column_exists` alias | ORA-00911 (invalid char) |
| `check_reference_library_metrics.py` | Quote `_dup`, `_cnt`, `_tot` aliases | ORA-00911 (invalid char) |
| `check_reference_sql.py` | Quote `_sub` alias in `get_scalar_query()` | ORA-00911 (invalid char) |

## Verification

After applying fixes, the Oracle-generated SQL should look like:

```sql
-- Pattern check (was TRY_CAST → now CAST DEFAULT NULL):
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "block" IS NULL
AND NOT REGEXP_LIKE(
    CAST("block" AS VARCHAR2 DEFAULT NULL ON CONVERSION ERROR),
    '^[A-Za-z0-9]{1,10}$'
)

-- User SQL (was CLOB → now VARCHAR2):
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT REGEXP_LIKE(CAST(block AS VARCHAR2), '^[A-Za-z0-9]{1,10}$')

-- Column exists (was _vowl... unquoted → now quoted):
SELECT COUNT(*) FROM (
    SELECT "block" FROM "hdb_resale_prices" FETCH FIRST 0 ROWS ONLY
) "_vowl_column_exists"

-- Integer type check (was TRY_CAST → now CAST DEFAULT NULL):
SELECT COUNT(*) FROM "hdb_resale_prices"
WHERE NOT "lease_commence_date" IS NULL
AND (
    CAST("lease_commence_date" AS DOUBLE PRECISION DEFAULT NULL ON CONVERSION ERROR) IS NULL
    OR CAST("lease_commence_date" AS DOUBLE PRECISION DEFAULT NULL ON CONVERSION ERROR)
       <> CAST("lease_commence_date" AS INT DEFAULT NULL ON CONVERSION ERROR)
)
```

## Notes

- The `CAST(... DEFAULT NULL ON CONVERSION ERROR)` syntax requires **Oracle 12c R2** (12.2) or later. Earlier Oracle versions would need a different approach (e.g., PL/SQL function wrappers).
- sqlglot natively supports generating this syntax: `exp.Cast(this=..., to=..., default=exp.Null())`.
- The TEXT→VARCHAR transform should run **after** the TryCast→Cast transform (since TryCast nodes also carry type info that maps TEXT→CLOB).
- The `build_error_result` exception handler in `ibis_sql_executor.py` (line ~189) doesn't pass `dialect` to `build_error_result`, so the `rule` in error results shows postgres-dialect SQL. This is a cosmetic issue, not a functional one.
