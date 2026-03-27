"""Internal helpers for validation row-quality summaries."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from typing import Any

import narwhals as nw

from ..executors.base import CheckResult
from .result_models import RowQualitySummary


def get_eligible_schema_names(
    eligible_checks: Iterable[CheckResult],
    total_rows_by_schema: dict[str, int],
) -> set[str]:
    return {
        schema_name
        for check_result in eligible_checks
        for schema_name in [check_result.metadata.get('schema')]
        if isinstance(schema_name, str) and schema_name in total_rows_by_schema
    }


def select_relevant_failed_row_columns(
    schema_name: str,
    failed_rows: nw.DataFrame,
    schema_columns: dict[str, list[str]],
    excluded_columns: Sequence[str],
) -> list[str]:
    relevant_columns = [
        column_name
        for column_name in failed_rows.columns
        if column_name in schema_columns.get(schema_name, [])
    ]
    if relevant_columns:
        return relevant_columns
    return [
        column_name
        for column_name in failed_rows.columns
        if column_name not in excluded_columns
    ]


def iter_unique_failed_row_keys(
    failed_rows: nw.DataFrame,
    relevant_columns: Sequence[str],
) -> Iterator[tuple[Any, ...]]:
    failed_rows_table = failed_rows.to_arrow().select(list(relevant_columns))
    for row_index in range(failed_rows_table.num_rows):
        yield (tuple(relevant_columns),) + tuple(
            failed_rows_table[column_name][row_index].as_py()
            for column_name in relevant_columns
        )


def build_row_quality_summary(total_rows: int, records_with_issues: int) -> RowQualitySummary:
    clean_records = max(total_rows - records_with_issues, 0)
    data_quality = (clean_records / total_rows * 100) if total_rows else 100.0
    return RowQualitySummary(
        total_rows=total_rows,
        records_with_issues=records_with_issues,
        clean_records=clean_records,
        data_quality=data_quality,
    )
