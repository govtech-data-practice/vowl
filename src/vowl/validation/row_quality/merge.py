"""Merge the routes of one schema into one entry per attributed row.

Each entry is ``(mask, copies)``: the checks that caught the row and the number
of copies of it in the table. Within the pushdown the entries are keyed by binary
key bytes. When a schema also has client_lookup checks, the pushdown returns
one value tuple per key, and both are matched onto the exported table on the
normalised Python row keys of ``result_row_quality.row_keys``. See
"Cross-route merge" in ``design/row-quality-statistics.md``.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from typing import Any

import pyarrow as pa

from ..result_row_quality import align_to_schema, match_onto_table, row_keys
from .pushdown import PushdownOutcome


def _pushdown_value_table(outcome: PushdownOutcome, key_columns: Sequence[str]) -> tuple[pa.Table, list[list[Any]]]:
    """The value columns of every pushdown row, in the order of ``outcome.rows``."""
    entries = list(outcome.rows.values())
    by_table: dict[int, list[int]] = {}
    order: list[tuple[int, int]] = []
    for entry in entries:
        table_index, row = entry[2]
        rows = by_table.setdefault(table_index, [])
        order.append((table_index, len(rows)))
        rows.append(row)
    parts = {
        table_index: outcome.value_tables[table_index].take(pa.array(rows, type=pa.int64()))
        for table_index, rows in by_table.items()
    }
    offsets: dict[int, int] = {}
    running = 0
    ordered_parts = []
    for table_index in sorted(parts):
        offsets[table_index] = running
        running += parts[table_index].num_rows
        ordered_parts.append(parts[table_index].rename_columns(list(key_columns)))
    table = pa.concat_tables(ordered_parts, promote_options="default") if ordered_parts else pa.table({})
    positions = [offsets[table_index] + index for table_index, index in order]
    return table.take(pa.array(positions, type=pa.int64())), entries


def pushdown_entries(outcome: PushdownOutcome | None) -> list[tuple[int, int]]:
    """The pushdown's ``(mask, copies)`` entries, from its histogram or its rows."""
    if outcome is None:
        return []
    if outcome.histogram is not None:
        return list(outcome.histogram)
    return [(entry[0], entry[1]) for entry in outcome.rows.values()]


def merge_onto_table(
    outcome: PushdownOutcome | None,
    fetched: Sequence[tuple[int, pa.Table]],
    key_columns: Sequence[str],
    table: pa.Table,
    index: dict[Any, list[int]],
) -> tuple[list[tuple[int, int]], bool, dict[int, tuple[int, int] | None]]:
    """Merge pushdown entries and fetched rows onto the rows of the exported table.

    Each fetched check marks the table rows that equal one of its rows, so a
    row's copies come from the table, not from the check's rows. A check that
    returns a row once (``DISTINCT``) still counts all its copies, and one that
    returns a row twice (a join) counts it once.

    Args:
        outcome: The pushdown outcome, run with values, or None when nothing
            ran there.
        fetched: ``(check_id, rows)`` of each client_lookup check.
        key_columns: The schema's key columns.
        table: The exported table.
        index: ``table_key_index(table, key_columns)``.

    Returns:
        ``(entries, exact, matched)``. ``entries`` holds one ``(mask, rows)``
        per set of checks. ``exact`` is false when a pushdown row matched no
        table row. ``matched`` maps each fetched check to ``(table rows
        matched, distinct keys not found)``, or to None when its rows could
        not be keyed.
    """
    exact = True
    masks = [0] * table.num_rows
    extra: list[tuple[int, int]] = []

    if outcome is not None and outcome.histogram is not None:
        # Not run with values, so its rows cannot be placed.
        extra.extend(outcome.histogram)
        exact = False
    elif outcome is not None and outcome.rows:
        values, entries = _pushdown_value_table(outcome, key_columns)
        values = align_to_schema(values, table.schema, key_columns)
        try:
            keys = row_keys(values, key_columns)
        except Exception:
            keys = None
        if keys is None:
            extra.extend((entry[0], entry[1]) for entry in entries)
            exact = False
        else:
            for key, entry in zip(keys, entries, strict=True):
                positions = index.get(key)
                if positions is None:
                    extra.append((entry[0], entry[1]))
                    exact = False
                    continue
                for position in positions:
                    masks[position] |= entry[0]

    matched: dict[int, tuple[int, int] | None] = {}
    for check_id, rows in fetched:
        try:
            positions, missing = match_onto_table(table, rows, key_columns, index=index)
        except Exception:
            # The caller counts it from its scalar count instead.
            matched[check_id] = None
            continue
        bit = 1 << check_id
        for position in positions:
            masks[position] |= bit
        matched[check_id] = (len(positions), missing)

    entries_out = list(Counter(mask for mask in masks if mask).items())
    return entries_out + extra, exact, matched
