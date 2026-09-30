"""Key expressions: how the pushdown recognises the same row across checks.

Each key column is grouped on one binary, type-tagged expression, so that
collations (NOCASE, RTRIM, UTF8_LCASE and so on), ``-0.0`` against ``0.0`` and
SQLite's mixed types cannot merge rows the checks tell apart. See "Key" in
``design/row-quality-statistics.md``.
"""

from __future__ import annotations

from collections.abc import Sequence
from functools import lru_cache
from typing import Any

from sqlglot import exp

# Dialects with a tested binary key entry. Any other dialect groups on the
# plain column and reports ``exact = false``.
KEYED_DIALECTS = frozenset({"duckdb", "sqlite", "spark", "databricks", "postgres"})

_SPARK_DIALECTS = frozenset({"spark", "databricks"})


# Rendering through sqlglot costs about a millisecond a call, and a statement
# renders the same few identifiers and casts thousands of times, so the
# renderings are cached.


@lru_cache(maxsize=4096)
def quote(name: str, dialect: str) -> str:
    """Render *name* as a quoted identifier in *dialect*."""
    return exp.to_identifier(name, quoted=True).sql(dialect=dialect)


@lru_cache(maxsize=64)
def _cast_template(type_name: str, dialect: str) -> str:
    return exp.cast(exp.column("_vowl_x"), type_name).sql(dialect=dialect)


def cast(sql: str, type_name: str, dialect: str) -> str:
    """Render ``CAST(sql AS type_name)`` with the type spelled for *dialect*.

    MySQL, for example, spells BIGINT as SIGNED inside a CAST.
    """
    return _cast_template(type_name, dialect).replace("_vowl_x", f"({sql})", 1)


def column_ref(alias: str, name: str, dialect: str) -> str:
    """Render ``alias.name`` with both parts quoted."""
    return f"{quote(alias, dialect)}.{quote(name, dialect)}"


def _is(dtype: Any, predicate: str) -> bool:
    check = getattr(dtype, predicate, None)
    try:
        return bool(check()) if callable(check) else False
    except Exception:
        return False


def key_expression(column_sql: str, dtype: Any, dialect: str) -> str:
    """The key expression for one column, given its rendered reference.

    Args:
        column_sql: The rendered column reference, for example ``"q"."price"``.
        dtype: The column's Ibis data type, or None when unknown.
        dialect: The sqlglot dialect of the statement.
    """
    if dialect == "duckdb":
        if _is(dtype, "is_binary"):
            return column_sql
        if _is(dtype, "is_nested"):
            # to_json quotes strings, so ['a, b'] and ['a', 'b'] stay apart.
            return f"encode(CAST(to_json({column_sql}) AS VARCHAR))"
        return f"encode(CAST({column_sql} AS VARCHAR))"
    if dialect == "sqlite":
        # The type tag keeps 1, 1.0, '1' and x'31' apart in one column.
        # '%!.17g' prints every double so that it round-trips. The
        # concatenation drops the column's collation, so NOCASE and RTRIM
        # values stay apart too.
        return (
            f"typeof({column_sql}) || ':' || CASE typeof({column_sql}) "
            f"WHEN 'real' THEN printf('%!.17g', {column_sql}) "
            f"WHEN 'blob' THEN hex({column_sql}) "
            f"ELSE CAST({column_sql} AS TEXT) END"
        )
    if dialect in _SPARK_DIALECTS:
        if _is(dtype, "is_binary"):
            return column_sql
        if _is(dtype, "is_nested"):
            return f"CAST(to_json({column_sql}) AS BINARY)"
        return f"CAST(CAST({column_sql} AS STRING) AS BINARY)"
    if dialect == "postgres":
        # A Postgres column holds one type, so no type tag is needed.
        # float8send gives the stored bits, so -0.0 and 0.0 stay apart and the
        # key does not depend on extra_float_digits. convert_to gives bytea,
        # which has no collation, and it also covers json, which has no
        # equality operator.
        if _is(dtype, "is_binary"):
            return column_sql
        if _is(dtype, "is_floating"):
            return f"float8send(CAST({column_sql} AS DOUBLE PRECISION))"
        return f"convert_to(CAST({column_sql} AS TEXT), 'UTF8')"
    if _is(dtype, "is_json") or _is(dtype, "is_geospatial") or _is(dtype, "is_nested"):
        # Not groupable on most engines. A hash is not exact, but nor is any
        # key in a dialect without an entry.
        return f"MD5({cast(column_sql, 'VARCHAR', dialect)})"
    return column_sql


def key_is_exact(dialect: str) -> bool:
    """Whether *dialect* has a tested binary key entry."""
    return dialect in KEYED_DIALECTS


def null_safe_equal(left_sql: str, right_sql: str, dialect: str) -> str:
    """Render ``left IS NOT DISTINCT FROM right`` in *dialect*."""
    if dialect == "sqlite":
        return f"({left_sql}) IS ({right_sql})"
    return _null_safe_template(dialect).replace("_vowl_l", f"({left_sql})", 1).replace("_vowl_r", f"({right_sql})", 1)


@lru_cache(maxsize=64)
def _null_safe_template(dialect: str) -> str:
    return exp.NullSafeEQ(this=exp.column("_vowl_l"), expression=exp.column("_vowl_r")).sql(dialect=dialect)


def value_aggregate(value_sql: str, dialect: str) -> str:
    """Render an aggregate that returns one of a group's (equal) values."""
    if dialect in ("duckdb", "spark", "databricks", "bigquery", "snowflake"):
        return f"ANY_VALUE({value_sql})"
    if dialect == "sqlite":
        return f"MAX({value_sql})"
    if dialect == "postgres":
        # Postgres has no MIN for boolean, bytea or json, and ANY_VALUE needs 16.
        return f"(ARRAY_AGG({value_sql}))[1]"
    return f"MIN({value_sql})"


def primary_key_columns(properties: Sequence[dict[str, Any]]) -> list[str]:
    """The declared primary key columns of a schema, in key order."""
    keyed = [
        (index, prop)
        for index, prop in enumerate(properties)
        if isinstance(prop, dict) and prop.get("name") and prop.get("primaryKey") is True
    ]

    def position(item: tuple[int, dict[str, Any]]) -> tuple[int, int]:
        index, prop = item
        declared = prop.get("primaryKeyPosition")
        return (declared if isinstance(declared, int) and declared >= 0 else 1_000_000 + index, index)

    return [prop["name"] for _, prop in sorted(keyed, key=position)]
