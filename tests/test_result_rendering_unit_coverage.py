from __future__ import annotations

from dataclasses import replace

import pytest

from vowl.validation.result import _safe_filename_component
from vowl.validation.result_models import OverallSummary
from vowl.validation.result_rendering import format_passed_rows


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


def _summary(*, total_rows, passed_row_percentage, passed_rows=0, approximate=False):
    return OverallSummary(
        passed_checks=0,
        error_checks=0,
        total_checks=0,
        failed_rows=0 if passed_rows is not None else None,
        passed_rows=passed_rows,
        total_rows=total_rows,
        passed_row_percentage=passed_row_percentage,
        approximate=approximate,
    )


def test_format_passed_rows_handles_none_total_rows():
    summary = _summary(total_rows=None, passed_row_percentage=None, passed_rows=5)
    assert format_passed_rows(summary) == "5 / 0 (N/A)"


def test_format_passed_rows_handles_zero_total_rows():
    """A zero-row table has passed_row_percentage None; must not crash in
    _truncate_pct. Regression for 'unsupported operand *: NoneType and int'
    when every check errored on an empty/unstatted table."""
    summary = _summary(total_rows=0, passed_row_percentage=None)
    assert format_passed_rows(summary) == "0 / 0 (N/A)"


def test_format_passed_rows_formats_percentage():
    summary = _summary(total_rows=1000, passed_row_percentage=99.99, passed_rows=999)
    # Truncated (not rounded) to one decimal place.
    assert format_passed_rows(summary) == "999 / 1,000 (99.9%)"


def test_format_passed_rows_marks_approximate_numbers():
    summary = _summary(total_rows=1000, passed_row_percentage=99.99, passed_rows=999, approximate=True)
    assert format_passed_rows(summary) == "999 / 1,000 (99.9%) (approx.)"


def test_format_passed_rows_without_row_level_checks_is_na():
    summary = _summary(total_rows=1000, passed_row_percentage=None, passed_rows=None)
    assert format_passed_rows(summary) == "N/A"


@pytest.mark.parametrize(
    ("count", "suffix"),
    [(0, " (approx.)"), (1, " (approx., 1 check not attributable)"), (2, " (approx., 2 checks not attributable)")],
)
def test_format_passed_rows_names_the_checks_not_attributable(count, suffix):
    summary = replace(
        _summary(total_rows=10, passed_row_percentage=80.0, passed_rows=8, approximate=True),
        checks_not_attributable=count,
    )
    assert format_passed_rows(summary) == f"8 / 10 (80.0%){suffix}"
