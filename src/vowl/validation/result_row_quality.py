"""Row keys: Python value equality for matching failed rows to table rows."""

from __future__ import annotations

import math
from collections.abc import Hashable, Iterable, Sequence
from typing import Any

import pyarrow as pa


class _KeySentinel:
    """A named placeholder used inside row keys."""

    __slots__ = ("_name",)

    def __init__(self, name: str) -> None:
        self._name = name

    def __repr__(self) -> str:
        return self._name


# NaN never equals itself, so every NaN maps to one placeholder and NaN rows
# match each other. -0.0 equals 0.0 in Python but is a different value in SQL
# engines that compare bits, so it gets its own placeholder.
_NAN = _KeySentinel("NaN")
_NEG_ZERO = _KeySentinel("-0.0")

_ARROW_ERRORS = (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError)


def _float_key(value: Any) -> Any:
    if value is None:
        return None
    if value != value:
        return _NAN
    if value == 0.0 and math.copysign(1.0, value) < 0:
        return _NEG_ZERO
    return value


def _freeze(value: Any) -> Any:
    """Turn a nested Python value into a hashable one, normalising floats."""
    if isinstance(value, float):
        return _float_key(value)
    if isinstance(value, dict):
        return tuple((key, _freeze(item)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _column_key_values(column: pa.ChunkedArray | pa.Array) -> list[Any]:
    """Convert one column into hashable key values, one per row."""
    arrow_type = column.type
    if pa.types.is_floating(arrow_type):
        return [_float_key(value) for value in column.to_pylist()]
    if pa.types.is_date32(arrow_type):
        # Days as integers, without building a Python date per row.
        return column.cast(pa.int32()).to_pylist()
    if (
        pa.types.is_timestamp(arrow_type)
        or pa.types.is_duration(arrow_type)
        or pa.types.is_time(arrow_type)
        or pa.types.is_date64(arrow_type)
    ):
        # Integers keep every unit exact. Python values can lose nanoseconds.
        try:
            return column.cast(pa.int64()).to_pylist()
        except _ARROW_ERRORS:
            return column.to_pylist()
    if pa.types.is_nested(arrow_type):
        return [_freeze(value) for value in column.to_pylist()]
    return column.to_pylist()


def row_keys(table: pa.Table, columns: Sequence[str]) -> list[tuple[Any, ...]]:
    """Return one hashable key per row of *table*, built from *columns*.

    Two rows get the same key exactly when their values are equal, with NaN
    equal to NaN, ``-0.0`` distinct from ``0.0``, nested values compared
    element by element and temporal values compared at full precision.
    """
    if not columns:
        return [()] * table.num_rows
    return list(zip(*(_column_key_values(table.column(name)) for name in columns), strict=True))


def first_occurrence_indices(keys: Iterable[tuple[Any, ...]]) -> dict[tuple[Any, ...], int]:
    """Map each distinct key to the index of its first row, in first-seen order."""
    first: dict[tuple[Any, ...], int] = {}
    for index, key in enumerate(keys):
        first.setdefault(key, index)
    return first


def align_to_schema(table: pa.Table, target_schema: pa.Schema, columns: Sequence[str]) -> pa.Table:
    """Cast *columns* of *table* to their types in *target_schema* where possible.

    Row keys depend on the Arrow type, so both sides of a match must share it.
    A column whose cast fails is left unchanged.
    """
    for name in columns:
        if name not in target_schema.names:
            continue
        target_type = target_schema.field(name).type
        index = table.schema.get_field_index(name)
        if index < 0 or table.schema.field(index).type == target_type:
            continue
        try:
            column = table.column(index).cast(target_type)
        except _ARROW_ERRORS:
            continue
        table = table.set_column(index, pa.field(name, target_type), column)
    return table


def table_key_index(table: pa.Table, columns: Sequence[str]) -> dict[Hashable, list[int]]:
    """Map each row key of *table* to the indices of the rows that have it."""
    index: dict[Hashable, list[int]] = {}
    for position, key in enumerate(row_keys(table, columns)):
        index.setdefault(key, []).append(position)
    return index


def match_onto_table(
    table: pa.Table,
    rows: pa.Table,
    columns: Sequence[str],
    *,
    index: dict[Hashable, list[int]] | None = None,
) -> tuple[list[int], int]:
    """Find the rows of *table* that equal a row of *rows* on *columns*.

    *rows* is cast to the types of *table* first, so both sides build keys
    from the same Arrow types. Every copy of a matching table row is returned,
    however many times *rows* holds it.

    Args:
        table: The table to match onto.
        rows: The rows to look up.
        columns: The columns to compare.
        index: ``table_key_index(table, columns)``, when already built.

    Returns:
        ``(indices, missing)``: the sorted indices of the matching table rows,
        and the number of distinct keys of *rows* that match no table row.

    Raises:
        Whatever ``row_keys`` raises for a value it cannot turn into a key.
    """
    if index is None:
        index = table_key_index(table, columns)
    aligned = align_to_schema(rows, table.schema, columns)
    matched: list[int] = []
    missing = 0
    for key in set(row_keys(aligned, columns)):
        positions = index.get(key)
        if positions is None:
            missing += 1
        else:
            matched.extend(positions)
    matched.sort()
    return matched, missing
