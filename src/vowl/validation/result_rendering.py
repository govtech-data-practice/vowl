"""Internal rendering helpers for validation results."""

from __future__ import annotations

import math
from typing import List, Optional, Sequence

import pyarrow as pa

from ..executors.base import CheckResult
from .result_models import CheckStatusSummary, SchemaValidationBreakdown, SingleTableSummary

STATUS_ORDER = ('FAILED', 'ERROR', 'PASSED')
SUMMARY_LABELS = (
    "Checks Pass Rate:",
    "ERRORED Checks:",
    "Unique Passed Rows:",
    "Non-unique Failed Rows:",
)


def _truncate_pct(value: float) -> str:
    """Format a percentage truncated to 1 decimal place (no rounding up)."""
    truncated = math.floor(value * 10) / 10
    return f"{truncated:.1f}%"


def format_check_pass_rate(passed_checks: int, total_checks: int) -> str:
    if total_checks == 0:
        return f"{passed_checks} / {total_checks} (N/A)"
    pct = passed_checks / total_checks * 100
    return f"{passed_checks} / {total_checks} ({_truncate_pct(pct)})"


def format_summary_metric(indent: str, label: str, value: str, label_width: int) -> str:
    return f"{indent}{label.ljust(label_width)} {value}"


def get_summary_metric_width() -> int:
    return max(len(label) for label in SUMMARY_LABELS)


def get_tables_in_query(check_result: CheckResult) -> List[str]:
    tables_in_query = check_result.metadata.get('tables_in_query', [])
    if isinstance(tables_in_query, str):
        return [table_name.strip() for table_name in tables_in_query.split(',') if table_name.strip()]
    if isinstance(tables_in_query, (list, tuple, set)):
        return [str(table_name).strip() for table_name in tables_in_query if str(table_name).strip()]
    return []


def is_cross_table_check(check_result: CheckResult) -> bool:
    return len(get_tables_in_query(check_result)) > 1


def get_field_label(check_result: CheckResult) -> str:
    target = check_result.metadata.get('target')
    if target:
        return target
    schema_name = check_result.metadata.get('schema') or 'unknown'
    return str(schema_name)


def build_check_results_table(
    check_results: List[CheckResult],
) -> pa.Table:
    return pa.table({
        'check_id': [check_result.check_name for check_result in check_results],
        'Target': [get_field_label(check_result) for check_result in check_results],
        'tables_in_query': [', '.join(get_tables_in_query(check_result)) for check_result in check_results],
        'status': [check_result.status for check_result in check_results],
        'operator': [check_result.metadata.get('operator', '') or '' for check_result in check_results],
        'expected': ["" if check_result.expected_value is None else str(check_result.expected_value) for check_result in check_results],
        'actual': ["" if check_result.actual_value is None else str(check_result.actual_value) for check_result in check_results],
        'execution time': [f"{check_result.execution_time_ms:.2f} ms" for check_result in check_results],
    })


def format_ascii_table(table: pa.Table, divider_before_rows: Optional[Sequence[int]] = None) -> str:
    column_names = list(table.column_names)
    if not column_names:
        return "(no columns)"

    rows = table.to_pylist()
    string_rows = [
        ["" if row.get(column_name) is None else str(row.get(column_name)) for column_name in column_names]
        for row in rows
    ]
    widths = [len(column_name) for column_name in column_names]
    for row in string_rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    border = "+" + "+".join("-" * (width + 2) for width in widths) + "+"
    header = "| " + " | ".join(
        column_name.ljust(widths[index])
        for index, column_name in enumerate(column_names)
    ) + " |"
    body = [
        "| " + " | ".join(value.ljust(widths[index]) for index, value in enumerate(row)) + " |"
        for row in string_rows
    ]

    lines = [border, header, border]
    divider_rows = set(divider_before_rows or [])
    for row_index, row in enumerate(body):
        if row_index in divider_rows:
            lines.append(border)
        lines.append(row)
    lines.append(border)
    return "\n".join(lines)


def format_unique_passed_rows(single_table: SingleTableSummary) -> str:
    if single_table.total_rows is None:
        return f"{single_table.passed_unique_rows:,} / 0 (N/A)"
    return (
        f"{single_table.passed_unique_rows:,} / {single_table.total_rows:,} "
        f"({_truncate_pct(single_table.passed_row_percentage)})"
    )


def _check_status_lines(
    status_summary: CheckStatusSummary,
    label_width: int,
) -> List[str]:
    return [
        format_summary_metric(
            "       ", "Checks Pass Rate:",
            format_check_pass_rate(status_summary.passed_checks, status_summary.total_checks),
            label_width,
        ),
        format_summary_metric(
            "       ", "ERRORED Checks:",
            f"{status_summary.error_checks:,}",
            label_width,
        ),
    ]


def build_schema_summary_lines(
    schema_name: str,
    schema_breakdown: SchemaValidationBreakdown,
    summary_metric_width: int,
) -> str:
    overall = schema_breakdown.overall
    single_table = schema_breakdown.single_table
    multi_table = schema_breakdown.multi_table
    w = summary_metric_width

    lines = [f"   {schema_name}:"]
    lines += ["     Overall:"] + _check_status_lines(overall, w)
    lines += ["     Single Table:"] + _check_status_lines(single_table, w) + [
        format_summary_metric(
            "       ", "Unique Passed Rows:",
            format_unique_passed_rows(single_table), w,
        ),
    ]
    lines += ["     Multi Table:"] + _check_status_lines(multi_table, w) + [
        format_summary_metric(
            "       ", "Non-unique Failed Rows:",
            f"{multi_table.failed_non_unique_rows:,}", w,
        ),
    ]
    return "\n".join(lines)


def build_summary_section(
    *,
    total_checks: int,
    passed_checks: int,
    error_checks: int,
    check_pass_rate: float,
    schema_names: Sequence[str],
    schema_validation_breakdown: dict[str, SchemaValidationBreakdown],
) -> str:
    if not schema_validation_breakdown:
        return ""

    summary_metric_width = get_summary_metric_width()
    summary_sections = [
        "   Overall:",
        format_summary_metric(
            "     ",
            "Checks Pass Rate:",
            f"{passed_checks} / {total_checks} ({_truncate_pct(check_pass_rate)})",
            summary_metric_width,
        ),
    ]
    if error_checks > 0:
        summary_sections.append(
            format_summary_metric(
                "     ",
                "ERRORED Checks:",
                f"{error_checks:,}",
                summary_metric_width,
            )
        )

    summary_text = "\n".join(summary_sections)
    schema_text = "\n\n".join(
        build_schema_summary_lines(
            schema_name,
            schema_validation_breakdown[schema_name],
            summary_metric_width,
        )
        for schema_name in schema_names
        if schema_name in schema_validation_breakdown
    )
    return f"""
 OVERALL DATA QUALITY
{summary_text}

{schema_text}
"""


def build_check_results_section(
    sorted_check_results: Sequence[CheckResult],
) -> str:
    if not sorted_check_results:
        return ""

    divider_before_rows: List[int] = []
    previous_status = None
    for row_index, check_result in enumerate(sorted_check_results):
        if previous_status is not None and check_result.status != previous_status:
            divider_before_rows.append(row_index)
        previous_status = check_result.status

    table = build_check_results_table(sorted_check_results)
    return f"""
 CHECK RESULTS
{format_ascii_table(table, divider_before_rows=divider_before_rows)}
"""