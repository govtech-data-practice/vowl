"""Merge the routes of one schema into one entry per physical row.

Each entry is ``(mask, copies)``: the checks that caught the row and the number
of physical copies of it. Within the pushdown route the entries are keyed by
binary key bytes. When a schema also has fetched checks, the pushdown returns
one value tuple per key and both sides are matched on the normalised Python
row keys of ``result_row_quality.row_keys``. See "Cross-route merge" in
``design/row-quality-statistics.md``.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from ..result_row_quality import align_to_schema, match_onto_table, row_keys
from .pushdown import PushdownOutcome


@dataclass
class FetchedRows:
    """One fetched check's rows, restricted to the columns to match on.

    Attributes:
        check_id: Schema-local check id.
        table: The check's failed rows.
        key_columns: The columns to key on.
        prefix: Put the column names in the key, for schemas whose columns
            are unknown, so checks with different columns never merge.
    """

    check_id: int
    table: pa.Table
    key_columns: Sequence[str]
    prefix: bool = False


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


def merge_routes(
    outcome: PushdownOutcome | None,
    fetched: Sequence[FetchedRows],
    key_columns: Sequence[str],
    target_schema: pa.Schema | None,
) -> tuple[list[tuple[int, int]], bool]:
    """Merge pushdown entries with fetched rows.

    Args:
        outcome: The pushdown outcome, or None when nothing ran there.
        fetched: The fetched checks.
        key_columns: The schema's key columns.
        target_schema: Arrow types to align fetched rows to, when the pushdown
            returned no values to take them from.

    Returns:
        ``(entries, exact)``. ``entries`` holds one ``(mask, copies)`` per
        physical row group. ``exact`` is false when a value could not be
        turned into a key.
    """
    if outcome is not None and outcome.histogram is not None:
        return list(outcome.histogram), True
    if not fetched:
        rows = outcome.rows.values() if outcome is not None else ()
        return [(entry[0], entry[1]) for entry in rows], True

    exact = True
    merged: dict[tuple[Any, ...], list[int]] = {}
    schema = target_schema
    if outcome is not None and outcome.rows:
        values, entries = _pushdown_value_table(outcome, key_columns)
        schema = values.schema
        try:
            keys = row_keys(values, key_columns)
        except Exception:
            keys, exact = [(index,) for index in range(len(entries))], False
        for key, entry in zip(keys, entries, strict=True):
            existing = merged.get(key)
            if existing is None:
                merged[key] = [entry[0], entry[1]]
            else:
                # Two binary keys with one normalised key (for example 1 and
                # 1.0 in SQLite) are different physical rows.
                existing[0] |= entry[0]
                existing[1] += entry[1]

    for rows in fetched:
        table = rows.table
        if schema is not None and not rows.prefix:
            table = align_to_schema(table, schema, rows.key_columns)
        try:
            keys = row_keys(table, rows.key_columns)
        except Exception:
            exact = False
            continue
        if rows.prefix:
            prefix = (tuple(rows.key_columns),)
            keys = [prefix + key for key in keys]
        bit = 1 << rows.check_id
        for key, copies in Counter(keys).items():
            entry = merged.setdefault(key, [0, 0])
            entry[0] |= bit
            entry[1] = max(entry[1], copies)

    return [(mask, copies) for mask, copies in merged.values()], exact


def merge_onto_table(
    outcome: PushdownOutcome | None,
    fetched: Sequence[FetchedRows],
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
        fetched: The fetched checks, keyed on *key_columns*.
        key_columns: The schema's key columns.
        table: The exported table.
        index: ``table_key_index(table, key_columns)``.

    Returns:
        ``(entries, exact, matched)``. ``entries`` holds one ``(mask, rows)``
        per set of checks. ``exact`` is false when a pushdown row matched no
        table row or a check's rows could not be keyed. ``matched`` maps each
        fetched check to ``(table rows matched, distinct keys not found)``, or
        to None when its rows could not be keyed.
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
    for rows in fetched:
        try:
            positions, missing = match_onto_table(table, rows.table, key_columns, index=index)
        except Exception:
            # Counted as rows of their own, so the schema still has them.
            matched[rows.check_id] = None
            exact = False
            if rows.table.num_rows:
                extra.append((1 << rows.check_id, rows.table.num_rows))
            continue
        bit = 1 << rows.check_id
        for position in positions:
            masks[position] |= bit
        matched[rows.check_id] = (len(positions), missing)

    entries_out = list(Counter(mask for mask in masks if mask).items())
    return entries_out + extra, exact, matched
