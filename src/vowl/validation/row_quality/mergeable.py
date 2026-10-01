"""Whether a check's failed rows can be merged with the other checks of a table.

Counting (``_SchemaComputation._check_mergeable``) and marking
(``ValidationResult._is_mergeable_for_full_table``) both use
:func:`rows_mergeable`, so they apply the same rule. Each passes its own
columns: counting the table columns it knows, marking the columns of the
exported table. See "Shared step 2" in ``docs/merging-failed-rows.md``.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Sequence

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
