"""Certification: is a check's failed-rows query a pure row filter of its table?

A certified check returns every physical copy of each failing row and nothing
else, so its rows can be counted in the data source by the pushdown route. See
"Certification" in ``design/row-quality-statistics.md``.
"""

from __future__ import annotations

from typing import Any

import sqlglot
from sqlglot import exp

from ...executors.security import to_table_expression

_NONDETERMINISTIC_NODES: tuple[type[exp.Expression], ...] = (
    exp.Rand,
    exp.Randn,
    exp.Uuid,
    exp.CurrentTimestamp,
    exp.CurrentDate,
    exp.CurrentTime,
    exp.CurrentDatetime,
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
    }
)


def _from_clause(select: exp.Select) -> exp.From | None:
    # sqlglot renamed the "from" arg to "from_". Support both.
    return select.args.get("from_") or select.args.get("from")


def _is_nondeterministic(node: exp.Expression) -> bool:
    if isinstance(node, _NONDETERMINISTIC_NODES):
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


def certify_failed_rows_query(query: str | None, schema_name: str, dialect: str) -> tuple[bool, str]:
    """Check that *query* is a pure row filter of the table *schema_name*.

    Returns:
        ``(True, "")`` when certified, else ``(False, rule)`` where *rule* names
        the construct that failed, for example ``"DISTINCT"``.
    """
    if not query:
        return False, "no failed-rows query"
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

    outer = _outer_scope_nodes(parsed)
    if any(isinstance(node, exp.Window) for node in outer):
        return False, "a window function"
    if any(isinstance(node, (exp.Subquery, exp.Select)) for node in outer):
        return False, "a subquery outside WHERE"
    if any(isinstance(node, exp.TableSample) for node in outer):
        return False, "TABLESAMPLE"
    if any(_is_nondeterministic(node) for node in parsed.walk()):
        return False, "a nondeterministic function"
    return True, ""


def certify_scalar_query(query: str | None, aggregation_type: str, dialect: str) -> tuple[bool, str]:
    """Check that a count check's scalar query counts with a single ``COUNT``.

    The derived failed-rows query keeps only the rows a ``COUNT(expr)`` counts,
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
    """Certify a SQL check reference for the pushdown route.

    Certification runs on the unfiltered queries. Filter conditions wrap the
    table in the same ``(SELECT * FROM t WHERE ...)`` subquery for every check,
    which keeps the row-filter shape.

    Args:
        rendered: The ``(failed-rows query, scalar query)`` the check already
            ran, when it ran without filter conditions. Rendering them again
            resolves the contract, which is slow for many checks.
    """
    try:
        if rendered is not None:
            failed_rows_query, scalar_query = rendered
        else:
            failed_rows_query = check_ref.get_failed_rows_query(dialect, None, use_try_cast=use_try_cast)
            scalar_query = check_ref.get_query(dialect, None, use_try_cast=use_try_cast)
        aggregation_type = check_ref.aggregation_type
    except Exception:
        return False, "SQL that could not be rendered"
    ok, rule = certify_failed_rows_query(failed_rows_query, schema_name, dialect)
    if not ok:
        return ok, rule
    return certify_scalar_query(scalar_query, aggregation_type, dialect)
