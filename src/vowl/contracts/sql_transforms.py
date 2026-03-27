"""SQL AST manipulation utilities for dialect transpilation and query rewriting.

This module centralises all dialect-aware SQL transformations (Oracle, SQLite,
etc.) as well as generic helpers such as filter application, TRY_CAST
injection, aggregation detection and table-name extraction.

The functions here are pure SQL utilities with no dependency on check-reference
semantics; ``SQLCheckReference`` delegates to them.
"""

from __future__ import annotations

import fnmatch
import re
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import sqlglot
from sqlglot import exp

if TYPE_CHECKING:
    from vowl.adapters.models import FilterCondition

    FilterConditionType = Union[FilterCondition, List[FilterCondition], Dict[str, Any]]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGICAL_TYPE_TO_SQL: Dict[str, str] = {
    "string": "VARCHAR",
    "integer": "BIGINT",
    "number": "DOUBLE PRECISION",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "time": "TIME",
}

_ORACLE_STRING_TYPES = frozenset({
    exp.DataType.Type.VARCHAR,
    exp.DataType.Type.TEXT,
    exp.DataType.Type.CHAR,
    exp.DataType.Type.NCHAR,
    exp.DataType.Type.NVARCHAR,
})

_ORACLE_SIZEABLE_TYPES = frozenset({
    exp.DataType.Type.VARCHAR,
    exp.DataType.Type.CHAR,
    exp.DataType.Type.NCHAR,
    exp.DataType.Type.NVARCHAR,
})


# ---------------------------------------------------------------------------
# Oracle AST transforms
# ---------------------------------------------------------------------------

def oracle_trycast_to_safe_cast(ast: exp.Expression) -> exp.Expression:
    """Convert TryCast to Oracle-compatible Cast.

    String targets use plain CAST (cast-to-string never fails and Oracle
    rejects ``DEFAULT … ON CONVERSION ERROR`` for character types).
    Non-string targets use ``CAST … DEFAULT NULL ON CONVERSION ERROR``.
    """

    def _transform(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.TryCast):
            target_type = node.to.this if node.to else None
            if target_type in _ORACLE_STRING_TYPES:
                return exp.Cast(this=node.this.copy(), to=node.to.copy())
            return exp.Cast(this=node.this.copy(), to=node.to.copy(), default=exp.Null())
        return node

    return ast.transform(_transform)


def oracle_fix_cast_types(ast: exp.Expression) -> exp.Expression:
    """Fix string type casts for Oracle.

    * TEXT/CLOB → VARCHAR(4000) (CLOB cannot be used in REGEXP_LIKE, etc.)
    * Bare VARCHAR / CHAR without a size → add (4000) (Oracle CAST
      requires an explicit size for character types).
    """

    def _transform(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Cast) or not node.to:
            return node
        dtype = node.to
        # TEXT → VARCHAR(4000)
        if dtype.this == exp.DataType.Type.TEXT:
            new = node.copy()
            new.set("to", exp.DataType.build("VARCHAR(4000)"))
            return new
        # Bare VARCHAR/CHAR without explicit size → add (4000)
        if dtype.this in _ORACLE_SIZEABLE_TYPES and not dtype.expressions:
            new = node.copy()
            new.set("to", exp.DataType(
                this=dtype.this,
                expressions=[exp.DataTypeParam(this=exp.Literal.number(4000))],
            ))
            return new
        return node

    return ast.transform(_transform)


def oracle_quote_underscore_aliases(ast: exp.Expression) -> exp.Expression:
    """Quote subquery aliases starting with underscore for Oracle."""

    def _transform(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Subquery):
            alias_node = node.args.get("alias")
            if alias_node and isinstance(alias_node, exp.TableAlias):
                ident = alias_node.this
                if isinstance(ident, exp.Identifier) and not ident.quoted and ident.name.startswith("_"):
                    new_node = node.copy()
                    new_node.set("alias", exp.TableAlias(this=exp.to_identifier(ident.name, quoted=True)))
                    return new_node
        return node

    return ast.transform(_transform)


def oracle_quote_column_identifiers(ast: exp.Expression) -> exp.Expression:
    """Quote unquoted column identifiers to preserve case on Oracle.

    Oracle folds unquoted identifiers to uppercase which breaks
    references to lowercase columns created with quoted names.
    """

    def _transform(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column):
            ident = node.this
            if isinstance(ident, exp.Identifier) and not ident.quoted:
                new = node.copy()
                new.set("this", exp.to_identifier(ident.name, quoted=True))
                return new
        return node

    return ast.transform(_transform)


# ---------------------------------------------------------------------------
# Dialect AST transform registry
# ---------------------------------------------------------------------------

_DIALECT_AST_TRANSFORMS: Dict[str, list] = {
    "sqlite": [
        lambda ast: ast.transform(
            lambda node: (
                exp.Anonymous(
                    this="_IBIS_REGEX_SEARCH",
                    expressions=[node.this, node.expression],
                )
                if isinstance(node, exp.RegexpLike)
                else node
            )
        ),
    ],
    "oracle": [
        oracle_trycast_to_safe_cast,
        oracle_fix_cast_types,
        oracle_quote_underscore_aliases,
        oracle_quote_column_identifiers,
    ],
}


def apply_dialect_transforms(ast: exp.Expression, dialect: str) -> exp.Expression:
    """Apply registered AST transforms for *dialect* and return the result."""
    transforms = _DIALECT_AST_TRANSFORMS.get(dialect)
    if not transforms:
        return ast
    for transform_fn in transforms:
        ast = transform_fn(ast)
    return ast


def render_sql(ast: exp.Expression, dialect: str) -> str:
    """Apply dialect transforms then render the AST as a SQL string."""
    transformed = apply_dialect_transforms(ast.copy(), dialect)
    return transformed.sql(dialect=dialect)


def transpile(query: str, source_dialect: str, target_dialect: str) -> str:
    """Transpile a SQL string between dialects via sqlglot."""
    parsed = sqlglot.parse_one(query, read=source_dialect)
    return render_sql(parsed, target_dialect)


# ---------------------------------------------------------------------------
# Safe / TRY_CAST helpers
# ---------------------------------------------------------------------------

def infer_type_from_literal(literal_value: Any) -> Optional[str]:
    """Infer the SQL type to cast to based on a literal value."""
    if isinstance(literal_value, bool):
        return "BOOLEAN"
    elif isinstance(literal_value, int):
        return "BIGINT"
    elif isinstance(literal_value, float):
        return "DOUBLE"
    elif isinstance(literal_value, str):
        if re.match(r"^\d{4}-\d{2}-\d{2}$", literal_value):
            return "DATE"
        elif re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", literal_value):
            return "TIMESTAMP"
        return None
    return None


def make_safe_cast(
    node: exp.Expression,
    to: exp.DataType,
    dialect: str,
) -> exp.Expression:
    """Build a safe-cast expression appropriate for *dialect*."""
    if dialect == "oracle":
        target_type = to.this if to else None
        if target_type in _ORACLE_STRING_TYPES:
            return exp.Cast(this=node, to=to)
        return exp.Cast(this=node, to=to, default=exp.Null())
    return exp.TryCast(this=node, to=to)


def apply_try_cast(query: str, dialect: str) -> Tuple[str, bool]:
    """Transform a query to use TRY_CAST instead of CAST where appropriate."""
    try:
        parsed = sqlglot.parse_one(query, dialect=dialect)
        modified = False

        for cast_node in list(parsed.find_all(exp.Cast)):
            if cast_node.args.get("default") is not None:
                continue
            safe = make_safe_cast(cast_node.this.copy(), cast_node.to.copy(), dialect)
            cast_node.replace(safe)
            modified = True

        comparisons = []
        for comp_type in (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE):
            comparisons.extend(parsed.find_all(comp_type))

        for comparison in comparisons:
            left = comparison.left
            right = comparison.right

            col_side = None
            literal_side = None
            is_left_col = False

            if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                col_side = left
                literal_side = right
                is_left_col = True
            elif isinstance(right, exp.Column) and isinstance(left, exp.Literal):
                col_side = right
                literal_side = left
                is_left_col = False

            if col_side and literal_side:
                literal_val = literal_side.this
                if literal_side.is_int:
                    target_type = "BIGINT"
                elif literal_side.is_number:
                    target_type = "DOUBLE"
                else:
                    target_type = infer_type_from_literal(literal_val)

                if target_type:
                    cast_expr = make_safe_cast(
                        col_side.copy(),
                        exp.DataType.build(target_type),
                        dialect,
                    )

                    if is_left_col:
                        comparison.set("this", cast_expr)
                    else:
                        comparison.set("expression", cast_expr)
                    modified = True

        if modified:
            return parsed.sql(dialect=dialect), True
        return query, False

    except Exception:
        return query, False


# ---------------------------------------------------------------------------
# Filter application
# ---------------------------------------------------------------------------

def apply_filters(
    query: str,
    dialect: str,
    filter_conditions: Dict[str, "FilterConditionType"],
) -> str:
    """Apply filter conditions to table references in a SQL string."""
    from vowl.adapters.models import build_filter_ast

    if not filter_conditions:
        return query

    try:
        parsed = sqlglot.parse_one(query, dialect=dialect)
        tables = list(parsed.find_all(exp.Table))
    except Exception as e:
        warnings.warn(
            f"Failed to parse SQL query for filter application: {e}. "
            "Filter conditions will not be applied.",
            UserWarning,
            stacklevel=2,
        )
        return query

    if not tables:
        return parsed.sql(dialect=dialect)

    table_filter_ast: Dict[str, Optional[exp.Expression]] = {}

    for table in tables:
        tbl_name = table.name
        if tbl_name in table_filter_ast:
            continue

        matching_conditions: List[Any] = []
        for pattern, conditions in filter_conditions.items():
            if pattern == tbl_name or fnmatch.fnmatch(tbl_name, pattern):
                if isinstance(conditions, list):
                    matching_conditions.extend(conditions)
                else:
                    matching_conditions.append(conditions)

        if not matching_conditions:
            table_filter_ast[tbl_name] = None
        else:
            table_filter_ast[tbl_name] = build_filter_ast(matching_conditions)

    for table in tables:
        tbl_name = table.name
        filter_ast = table_filter_ast[tbl_name]

        if filter_ast is None:
            continue

        subquery_alias = table.alias_or_name
        inner_table = table.copy()
        inner_table.set("alias", None)
        inner_select = exp.Select(expressions=[exp.Star()]).from_(inner_table).where(filter_ast)
        subquery = exp.Subquery(
            this=inner_select,
            alias=exp.TableAlias(this=exp.to_identifier(subquery_alias)),
        )
        table.replace(subquery)

    return parsed.sql(dialect=dialect)


# ---------------------------------------------------------------------------
# Query analysis
# ---------------------------------------------------------------------------

def detect_aggregation_type(query: str, dialect: str) -> str:
    """Detect the aggregate function used in a SQL check query."""
    try:
        parsed = sqlglot.parse_one(query, dialect=dialect)
    except Exception:
        return "custom"

    if not isinstance(parsed, exp.Select):
        return "custom"

    agg_nodes: list[exp.Expression] = []
    for select_expr in parsed.expressions:
        agg_nodes.extend(
            node
            for node in select_expr.walk()
            if isinstance(node, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max))
        )

    if len(agg_nodes) == 0:
        if parsed.find(exp.From):
            return "none"
        return "custom"

    if len(agg_nodes) != 1:
        return "custom"

    agg = agg_nodes[0]
    if isinstance(agg, exp.Count):
        if isinstance(agg.this, exp.Distinct):
            return "count_distinct"
        return "count"
    if isinstance(agg, exp.Sum):
        return "sum"
    if isinstance(agg, exp.Avg):
        return "avg"
    if isinstance(agg, exp.Min):
        return "min"
    if isinstance(agg, exp.Max):
        return "max"

    return "custom"


def extract_table_names(query: str, dialect: str) -> List[str]:
    """Extract sorted unique table names from a SQL query."""
    try:
        parsed = sqlglot.parse_one(query, dialect=dialect)
        tables = {t.name for t in parsed.find_all(exp.Table) if t.name}
        return sorted(tables)
    except Exception:
        return []


def wrap_count_subquery(inner_query: str, dialect: str) -> str:
    """Wrap *inner_query* in ``SELECT COUNT(*) FROM (...) AS _sub``.

    Handles dialect-specific alias quoting (e.g. Oracle requires
    underscore-prefixed aliases to be quoted).
    """
    alias = '"_sub"' if dialect == "oracle" else "_sub"
    return f"SELECT COUNT(*) FROM ({inner_query}) AS {alias}"


__all__ = [
    "LOGICAL_TYPE_TO_SQL",
    "apply_dialect_transforms",
    "apply_filters",
    "apply_try_cast",
    "detect_aggregation_type",
    "extract_table_names",
    "infer_type_from_literal",
    "wrap_count_subquery",
    "make_safe_cast",
    "oracle_fix_cast_types",
    "oracle_quote_column_identifiers",
    "oracle_quote_underscore_aliases",
    "oracle_trycast_to_safe_cast",
    "render_sql",
    "transpile",
]
