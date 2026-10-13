"""Certification: is a check's row query a pure row filter of its table?

A certified check returns every copy of each failing row and nothing
else, so its rows can be counted in the data source by the server_predicate
attribution method. See "Certification" in ``design/row-quality-statistics.md``.
"""

from __future__ import annotations

from typing import Any

import sqlglot
from sqlglot import exp

from ...executors.base import row_query_of
from ...executors.security import to_table_expression

_NONDETERMINISTIC_NODES: tuple[type[exp.Expression], ...] = (
    exp.Rand,
    exp.Randn,
    exp.Uuid,
    exp.CurrentTimestamp,
    exp.CurrentDate,
    exp.CurrentTime,
    exp.CurrentDatetime,
    exp.CurrentTimestampLTZ,
    exp.Localtime,
    exp.Localtimestamp,
    exp.Systimestamp,
    exp.UtcDate,
    exp.UtcTime,
    exp.UtcTimestamp,
    exp.Shuffle,
)

_NONDETERMINISTIC_NAMES = frozenset(
    {
        "random",
        "rand",
        "randn",
        "uuid",
        "gen_random_uuid",
        "newid",
        "now",
        "sysdate",
        "systimestamp",
        "getdate",
        "sysdatetime",
        "current_timestamp",
        "localtimestamp",
        "sys_guid",
        "newsequentialid",
        "uuid_short",
        "getutcdate",
        "sysutcdatetime",
        "clock_timestamp",
        "statement_timestamp",
        "transaction_timestamp",
        "timeofday",
        "get_current_timestamp",
        "setseed",
        "monotonically_increasing_id",
        "spark_partition_id",
        "input_file_name",
    }
)

# Functions that read the clock only when called without arguments.
_NULLARY_CLOCK_NAMES = frozenset({"unix_timestamp", "unixepoch"})

# Pseudocolumns that number rows or change between statements. An Oracle
# ROWNUM filter keeps the first rows the engine returns, a LIMIT in disguise.
_ROW_LIMITING_COLUMNS = frozenset({"rownum"})

# SQLite date functions read the clock for a 'now' argument or none at all.
_SQLITE_CLOCK_FUNCTIONS = frozenset({"date", "time", "datetime", "julianday", "unixepoch", "strftime", "timediff"})

# String values that SQLite date functions and Postgres casts read as the clock.
_CLOCK_LITERALS = frozenset({"now", "today", "tomorrow", "yesterday"})

# The arguments sqlglot keeps on a Star for DuckDB, BigQuery and Snowflake
# select-list modifiers: * EXCLUDE/EXCEPT, * REPLACE, * RENAME, * ILIKE.
_STAR_MODIFIERS = ("except_", "except", "replace", "rename", "ilike")


def _from_clause(select: exp.Select) -> exp.From | None:
    # sqlglot renamed the "from" arg to "from_". Support both.
    return select.args.get("from_") or select.args.get("from")


def _reads_sqlite_clock(node: exp.Expression) -> bool:
    """``date('now')``, ``datetime()`` and the like, which sqlglot does not map to CURRENT_*."""
    if isinstance(node, exp.Date) and not node.args.get("this"):
        return True
    if not isinstance(node, exp.Anonymous) or node.name.lower() not in _SQLITE_CLOCK_FUNCTIONS:
        return False
    arguments = node.expressions
    if not arguments:
        return True
    first = arguments[0]
    return isinstance(first, exp.Literal) and first.is_string and first.name.lower() == "now"


def _is_nondeterministic(node: exp.Expression, dialect: str) -> bool:
    if isinstance(node, _NONDETERMINISTIC_NODES):
        return True
    if isinstance(node, exp.Anonymous) and node.name.lower() in _NULLARY_CLOCK_NAMES and not node.expressions:
        return True
    if (
        dialect == "oracle"
        and isinstance(node, exp.Column)
        and not node.table
        and isinstance(node.this, exp.Identifier)
    ):
        # A quoted "ROWNUM" is an ordinary column.
        return not node.this.quoted and node.name.lower() in _ROW_LIMITING_COLUMNS
    if isinstance(node, exp.Literal) and node.is_string and node.name.lower() in _CLOCK_LITERALS:
        parent = node.parent
        # SQLite date('now') and strftime(f, 'now') parse to Date and
        # TsOrDsToTimestamp. Postgres 'now'::timestamp is a Cast.
        if isinstance(parent, (exp.Date, exp.TsOrDsToTimestamp)):
            return True
        if isinstance(parent, (exp.Cast, exp.TryCast)) and parent.to.is_type(*exp.DataType.TEMPORAL_TYPES):
            return True
    if _reads_sqlite_clock(node):
        return True
    if isinstance(node, (exp.Anonymous, exp.Func)):
        name = node.name if isinstance(node, exp.Anonymous) else node.sql_name()
        return str(name).lower() in _NONDETERMINISTIC_NAMES
    return False


def _outer_scope_nodes(select: exp.Select) -> list[exp.Expression]:
    """Nodes of *select* outside its WHERE clause and outside nested queries."""
    nodes: list[exp.Expression] = []
    for key, value in select.args.items():
        if key == "where" or value is None:
            continue
        for child in value if isinstance(value, list) else [value]:
            if isinstance(child, exp.Expression):
                nodes.extend(child.walk())
    return nodes


def _same_table(table: exp.Table, schema_name: str) -> bool:
    """Whether *table* is the schema's table, qualifiers included.

    The query may leave out qualifiers the schema name has (``t`` matches
    ``db.t``), but every qualifier it writes must match (``other.t`` does not
    match ``t``).
    """
    try:
        anchor = [part.name.lower() for part in to_table_expression(schema_name).parts]
    except Exception:
        anchor = [part.lower() for part in schema_name.split(".")]
    written = [part.name.lower() for part in table.parts]
    return len(written) <= len(anchor) and anchor[len(anchor) - len(written) :] == written


def certify_row_query(query: str | None, schema_name: str, dialect: str) -> tuple[bool, str]:
    """Check that *query* is a pure row filter of the table *schema_name*.

    Returns:
        ``(True, "")`` when certified, else ``(False, rule)`` where *rule* names
        the construct that failed, for example ``"DISTINCT"``.
    """
    if not query:
        return False, "no row query"
    try:
        parsed = sqlglot.parse_one(query, dialect=dialect)
    except Exception:
        return False, "SQL that could not be parsed"

    if isinstance(parsed, exp.SetOperation):
        return False, "a set operation"
    if not isinstance(parsed, exp.Select):
        return False, "a statement that is not a SELECT"
    if parsed.args.get("with") or parsed.args.get("with_"):
        return False, "WITH"
    if parsed.args.get("distinct"):
        return False, "DISTINCT"
    if parsed.args.get("group"):
        return False, "GROUP BY"
    if parsed.args.get("having"):
        return False, "HAVING"
    if parsed.args.get("limit") or parsed.args.get("offset"):
        return False, "LIMIT"
    if parsed.args.get("qualify"):
        return False, "QUALIFY"
    if parsed.args.get("joins"):
        return False, "a join"
    if parsed.args.get("laterals"):
        return False, "LATERAL"
    if parsed.args.get("pivots"):
        return False, "PIVOT"

    from_clause = _from_clause(parsed)
    source = from_clause.this if from_clause is not None else None
    if not isinstance(source, exp.Table):
        return False, "a FROM that is not the table itself"
    if source.args.get("sample") or source.find(exp.TableSample):
        return False, "TABLESAMPLE"
    if source.args.get("joins"):
        return False, "a join"
    if source.args.get("pivots"):
        return False, "PIVOT"
    if not _same_table(source, schema_name):
        return False, "a FROM that is not the table itself"

    projections = parsed.expressions
    anchor_names = {source.name.lower(), source.alias_or_name.lower()}
    if len(projections) != 1:
        return False, "a select list that is not *"
    projection = projections[0]
    is_star = isinstance(projection, exp.Star)
    is_anchor_star = (
        isinstance(projection, exp.Column)
        and isinstance(projection.this, exp.Star)
        and projection.table.lower() in anchor_names
    )
    if not (is_star or is_anchor_star):
        return False, "a select list that is not *"
    star = projection if is_star else projection.this
    if any(star.args.get(modifier) for modifier in _STAR_MODIFIERS):
        # EXCLUDE, RENAME and ILIKE drop or rename table columns. REPLACE
        # returns changed values under the table's column names.
        return False, "a select list that is not *"

    outer = _outer_scope_nodes(parsed)
    if any(isinstance(node, exp.Window) for node in outer):
        return False, "a window function"
    if any(isinstance(node, (exp.Subquery, exp.Select)) for node in outer):
        return False, "a subquery outside WHERE"
    if any(isinstance(node, exp.TableSample) for node in parsed.walk()):
        # A sample inside a WHERE subquery changes the rows the filter keeps.
        return False, "TABLESAMPLE"
    for node in parsed.walk():
        if (
            node is not parsed
            and isinstance(node, exp.Select)
            and not node.args.get("order")
            and not isinstance(node.parent, exp.Exists)
        ):
            if node.args.get("limit") or node.args.get("offset"):
                # Which rows an unordered LIMIT keeps can change between runs.
                # EXISTS only asks whether a row exists, so it is exempt.
                return False, "LIMIT"
    if any(_is_nondeterministic(node, dialect) for node in parsed.walk()):
        return False, "a nondeterministic function"
    return True, ""


def certify_scalar_query(query: str | None, aggregation_type: str, dialect: str) -> tuple[bool, str]:
    """Check that a count check's scalar query counts with a single ``COUNT``.

    The derived row query keeps only the rows a ``COUNT(expr)`` counts,
    by adding ``expr IS NOT NULL``, so the two queries agree. ``"none"`` checks
    are wrapped in ``COUNT(*)`` by vowl and always pass.
    """
    if aggregation_type != "count":
        return True, ""
    if not query:
        return False, "no scalar query"
    try:
        parsed = sqlglot.parse_one(query, dialect=dialect)
    except Exception:
        return False, "SQL that could not be parsed"
    if not isinstance(parsed, exp.Select):
        return False, "a statement that is not a SELECT"
    counts = [node for projection in parsed.expressions for node in projection.walk() if isinstance(node, exp.Count)]
    if len(counts) != 1:
        return False, "more than one COUNT"
    return True, ""


def certify_check(
    check_ref: Any,
    schema_name: str,
    dialect: str,
    use_try_cast: bool,
    *,
    rendered: tuple[str | None, str | None] | None = None,
) -> tuple[bool, str]:
    """Certify a SQL check reference for the server_predicate attribution method.

    Certification runs on the unfiltered queries. Filter conditions wrap the
    table in the same ``(SELECT * FROM t WHERE ...)`` subquery for every check,
    which keeps the row-filter shape.

    Args:
        rendered: The ``(row query, scalar query)`` the check already
            ran, when it ran without filter conditions. Rendering them again
            resolves the contract, which is slow for many checks.
    """
    try:
        if rendered is not None:
            row_query, scalar_query = rendered
        else:
            row_query = row_query_of(check_ref, dialect, None, use_try_cast=use_try_cast)
            scalar_query = check_ref.get_query(dialect, None, use_try_cast=use_try_cast)
        aggregation_type = check_ref.aggregation_type
    except Exception:
        return False, "SQL that could not be rendered"
    ok, rule = certify_row_query(row_query, schema_name, dialect)
    if not ok:
        return ok, rule
    return certify_scalar_query(scalar_query, aggregation_type, dialect)
