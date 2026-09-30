"""Internal helpers for validation row-quality summaries."""

from __future__ import annotations

import math
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

import narwhals as nw
import pyarrow as pa

from ..executors.base import CheckResult
from .result_models import RowQualitySummary


def get_eligible_schema_names(
    eligible_checks: Iterable[CheckResult],
    total_rows_by_schema: dict[str, int],
) -> set[str]:
    return {
        schema_name
        for check_result in eligible_checks
        for schema_name in [check_result.metadata.get("schema_name")]
        if isinstance(schema_name, str) and schema_name in total_rows_by_schema
    }


def select_relevant_failed_row_columns(
    schema_name: str,
    failed_rows: nw.DataFrame,
    schema_columns: dict[str, list[str]],
    excluded_columns: Sequence[str],
) -> list[str]:
    relevant_columns = [
        column_name for column_name in failed_rows.columns if column_name in schema_columns.get(schema_name, [])
    ]
    if relevant_columns:
        return relevant_columns
    return [column_name for column_name in failed_rows.columns if column_name not in excluded_columns]


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


def iter_unique_failed_row_keys(
    failed_rows: nw.DataFrame,
    relevant_columns: Sequence[str],
) -> Iterator[tuple[Any, ...]]:
    failed_rows_table = failed_rows.to_arrow()
    prefix = (tuple(relevant_columns),)
    for key in row_keys(failed_rows_table, relevant_columns):
        yield prefix + key


def build_row_quality_summary(total_rows: int, records_with_issues: int) -> RowQualitySummary:
    clean_records = max(total_rows - records_with_issues, 0)
    data_quality = (clean_records / total_rows * 100) if total_rows else 100.0
    return RowQualitySummary(
        total_rows=total_rows,
        records_with_issues=records_with_issues,
        clean_records=clean_records,
        data_quality=data_quality,
    )
