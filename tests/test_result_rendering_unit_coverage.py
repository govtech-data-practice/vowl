from __future__ import annotations

import pytest

from vowl.validation.result import _safe_filename_component
from vowl.validation.result_models import OverallSummary
from vowl.validation.result_rendering import format_failed_rows_approximate


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


def _summary(failed_rows_approximate):
    return OverallSummary(
        passed_checks=0,
        error_checks=0,
        total_checks=0,
        failed_rows_approximate=failed_rows_approximate,
    )


def test_format_failed_rows_approximate_groups_thousands():
    assert format_failed_rows_approximate(_summary(1234)) == "1,234"


def test_format_failed_rows_approximate_shows_zero():
    assert format_failed_rows_approximate(_summary(0)) == "0"


def test_format_failed_rows_approximate_without_row_level_checks_is_na():
    assert format_failed_rows_approximate(_summary(None)) == "N/A"
