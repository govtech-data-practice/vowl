from __future__ import annotations

import pytest

from vowl.validation.result import _safe_filename_component
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
        ("a\\b:c*d?e|f", "a_b_c_d_e_f"),
        # Letters and digits of any script are kept.
        ("客户", "客户"),
        ("café", "café"),
        ("客户_orders", "客户_orders"),
        # Combining accents (as macOS stores them) clean to the composed form.
        ("cafe\u0301", "café"),
        ("***", "output"),
    ],
)
def test_safe_filename_component_strips_traversal(value: str, expected: str):
    result = _safe_filename_component(value)
    assert result == expected
    # Result must never contain path separators or parent-dir references.
    assert "/" not in result
    assert "\\" not in result
    assert not result.startswith(".")


def test_format_failed_rows_approximate_groups_thousands():
    assert format_failed_rows_approximate(1234) == "1,234"


def test_format_failed_rows_approximate_shows_zero():
    assert format_failed_rows_approximate(0) == "0"


def test_format_failed_rows_approximate_without_row_level_checks_is_na():
    assert format_failed_rows_approximate(None) == "N/A"
