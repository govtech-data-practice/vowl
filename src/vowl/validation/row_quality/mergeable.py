"""Whether a check's failed rows can be merged with the other checks of a table.

Counting (``_SchemaComputation._check_mergeable``) and marking
(``ValidationResult._is_mergeable_for_full_table``) both use
:func:`rows_mergeable`, so they apply the same rule. Each passes its own
columns: counting the table columns it knows, marking the columns of the
exported table. See ``docs/design-considerations/failed-rows/how-rows-are-counted.md``.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Sequence

import sqlglot
from sqlglot import exp

#: Columns vowl adds to failed-rows output. They are not table columns.
#: ``check_id`` and ``check_ids`` tag the consolidated failed-rows output,
#: ``check_info`` and ``check_info_item`` the annotated output.
METADATA_COLUMNS = ("check_id", "check_ids", "check_info", "check_info_item", "tables_in_query")


def rows_mergeable(
    row_columns: Iterable[str],
    table_columns: Collection[str],
    key_columns: Sequence[str] | None,
) -> bool:
    """True when failed rows with *row_columns* can be matched to table rows.

    Args:
        row_columns: The columns of the check's failed rows. vowl's metadata
            columns are ignored.
        table_columns: The table's columns.
        key_columns: The primary key the table's rows are matched on, or
            ``None`` when they are matched on every column.

    Returns:
        With a key, whether the rows hold every key column. Without one,
        whether the rows have exactly the table's columns.
    """
    columns = set(row_columns) - set(METADATA_COLUMNS)
    if key_columns:
        return set(key_columns) <= columns
    return columns == set(table_columns)


def _sources(select: exp.Select, ctes: dict[str, exp.Expression]) -> dict[str, exp.Expression | None]:
    """The sources of *select* by alias: a query for a derived table or CTE, None for a table."""
    found: dict[str, exp.Expression | None] = {}
    from_ = select.args.get("from_") or select.args.get("from")
    nodes = [from_.this] if from_ is not None else []
    nodes += [join.this for join in select.args.get("joins") or []]
    for node in nodes:
        if isinstance(node, exp.Subquery):
            found[node.alias_or_name] = node.this
        elif isinstance(node, exp.Table):
            found[node.alias_or_name] = ctes.get(node.name) if not node.db else None
        else:
            # A function or VALUES source: its values are not the table's.
            found[node.alias_or_name] = node
    return found


def _plain_projection(node: exp.Expression, sources: dict[str, exp.Expression | None], ctes) -> bool:
    if isinstance(node, exp.Alias):
        if not isinstance(node.this, exp.Column) or node.alias_or_name != node.this.name:
            return False
        node = node.this
    if isinstance(node, exp.Star):
        return all(_plain_source(source, ctes) for source in sources.values())
    if not isinstance(node, exp.Column):
        return False
    qualifier = node.table
    if qualifier:
        return _plain_source(sources.get(qualifier), ctes)
    return all(_plain_source(source, ctes) for source in sources.values())


def _plain_source(source: exp.Expression | None, ctes: dict[str, exp.Expression]) -> bool:
    return source is None or _returns_plain_columns(source, ctes)


def _returns_plain_columns(query: exp.Expression, ctes: dict[str, exp.Expression]) -> bool:
    with_ = query.args.get("with_") or query.args.get("with")
    if with_ is not None:
        ctes = {**ctes, **{cte.alias_or_name: cte.this for cte in with_.expressions}}
    if isinstance(query, exp.Subquery):
        return _returns_plain_columns(query.this, ctes)
    if isinstance(query, exp.SetOperation):
        return _returns_plain_columns(query.left, ctes) and _returns_plain_columns(query.right, ctes)
    if not isinstance(query, exp.Select):
        return False
    sources = _sources(query, ctes)
    return all(_plain_projection(node, sources, ctes) for node in query.expressions)


def returns_table_values(query: str | None, dialect: str) -> bool:
    """True when a failed-rows query returns only plain columns or ``*``.

    Its rows then hold the table's own values, so they can be matched onto
    table rows. ``c * 1.5``, ``upper(s)`` or a column renamed to another's
    name return changed values, which can match another row or none. The
    columns are followed into the subqueries and CTEs they come from. A query
    that cannot be parsed counts as changed.
    """
    if not query:
        return False
    try:
        tree = sqlglot.parse_one(query, dialect=dialect or None)
    except Exception:
        return False
    return _returns_plain_columns(tree, {})
