from __future__ import annotations

import pytest

from vowl.validation.result import _safe_filename_component
from vowl.validation.result_models import SingleTableSummary
from vowl.validation.result_rendering import format_unique_passed_rows


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("public.users", "public.users"),
        ("orders_2024", "orders_2024"),
        ("../../etc/passwd", "etc_passwd"),
        ("../../../root/.ssh/authorized_keys", "root_.ssh_authorized_keys"),
        ("/absolute/path", "absolute_path"),
        ("schema/table", "schema_table"),
        ("..", "output"),
        ("", "output"),
        ("nul\x00byte", "nul_byte"),
        ("a b, c", "a_b_c"),
    ],
)
def test_safe_filename_component_strips_traversal(value: str, expected: str):
    result = _safe_filename_component(value)
    assert result == expected
    # Result must never contain path separators or parent-dir references.
    assert "/" not in result
    assert "\\" not in result
    assert not result.startswith(".")


def _summary(*, total_rows, passed_row_percentage, passed_unique_rows=0):
    return SingleTableSummary(
        passed_checks=0,
        error_checks=0,
        total_checks=0,
        failed_unique_rows=0,
        passed_unique_rows=passed_unique_rows,
        total_rows=total_rows,
        passed_row_percentage=passed_row_percentage,
    )


def test_format_unique_passed_rows_handles_none_total_rows():
    summary = _summary(total_rows=None, passed_row_percentage=None, passed_unique_rows=5)
    assert format_unique_passed_rows(summary) == "5 / 0 (N/A)"


def test_format_unique_passed_rows_handles_zero_total_rows():
    """A zero-row table has passed_row_percentage None; must not crash in
    _truncate_pct. Regression for 'unsupported operand *: NoneType and int'
    when every check errored on an empty/unstatted table."""
    summary = _summary(total_rows=0, passed_row_percentage=None)
    assert format_unique_passed_rows(summary) == "0 / 0 (N/A)"


def test_format_unique_passed_rows_formats_percentage():
    summary = _summary(total_rows=1000, passed_row_percentage=99.99, passed_unique_rows=999)
    # Truncated (not rounded) to one decimal place.
    assert format_unique_passed_rows(summary) == "999 / 1,000 (99.9%)"
