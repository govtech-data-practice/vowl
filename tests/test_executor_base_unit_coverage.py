from __future__ import annotations

from types import SimpleNamespace

import pyarrow as pa
import pytest

from vowl.contracts.check_reference import CheckReference, SQLCheckReference
from vowl.executors.base import BaseExecutor, CheckResult, GXExecutor, SQLExecutor


class StubExecutor(BaseExecutor):
    def run_single_check(self, check_ref):
        return CheckResult("stub", "PASSED", "ok")

    def run_batch_checks(self, check_refs):
        return []


class StubSQLExecutor(SQLExecutor):
    def run_single_check(self, check_ref):
        return CheckResult("stub", "PASSED", "ok")

    def run_batch_checks(self, check_refs):
        return []


class StubGXExecutor(GXExecutor):
    def run_single_check(self, check_ref):
        return CheckResult("stub", "PASSED", "ok")

    def run_batch_checks(self, check_refs):
        return []


def test_check_result_failed_rows_fetches_lazily_and_caches_once():
    fetch_calls: list[str] = []

    def fetch_rows():
        fetch_calls.append("called")
        return SimpleNamespace(to_pandas=lambda: pa.table({"id": [1]}).to_pandas())

    result = CheckResult(
        check_name="row_check",
        status="FAILED",
        details="details",
        failed_rows_fetcher=fetch_rows,
        failed_rows_count=1,
    )

    assert result.failed_rows_count == 1
    assert fetch_calls == []

    first = result.failed_rows
    second = result.failed_rows

    assert first.to_pandas().to_dict(orient="records") == [{"id": 1}]
    assert second.to_pandas().to_dict(orient="records") == [{"id": 1}]
    assert fetch_calls == ["called"]


def test_check_result_failed_rows_returns_empty_frame_when_fetcher_returns_none():
    fetch_calls: list[str] = []

    def fetch_rows():
        fetch_calls.append("called")
        return None

    result = CheckResult(
        check_name="row_check",
        status="FAILED",
        details="details",
        failed_rows_fetcher=fetch_rows,
    )

    empty_df = result.failed_rows.to_pandas()

    assert empty_df.empty
    assert fetch_calls == ["called"]
    assert result.failed_rows.to_pandas().empty
    assert fetch_calls == ["called"]


def test_check_result_repr_is_concise():
    result = CheckResult("my_check", "PASSED", "details")

    assert repr(result) == "CheckResult(name='my_check', status='PASSED')"


def test_base_executor_adapter_property_exposes_original_adapter():
    adapter = SimpleNamespace(name="adapter")
    executor = StubExecutor(adapter)

    assert executor.adapter is adapter


def test_base_executor_default_methods_are_directly_reachable():
    executor = StubExecutor(SimpleNamespace())

    assert BaseExecutor.run_single_check(executor, None) is None
    assert BaseExecutor.run_batch_checks(executor, []) is None
    assert BaseExecutor.cleanup(executor) is None


def test_sql_executor_deduplicate_arrow_columns_no_duplicates_is_noop():
    table = pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["id", "value"])

    result = StubSQLExecutor._deduplicate_arrow_columns(table)

    assert result is table
    assert result.column_names == ["id", "value"]


def test_sql_executor_deduplicate_arrow_columns_renames_duplicates_deterministically():
    table = pa.Table.from_arrays(
        [pa.array([1]), pa.array([2]), pa.array([3]), pa.array([4])],
        names=["id", "id", "value", "id"],
    )

    result = StubSQLExecutor._deduplicate_arrow_columns(table)

    assert result.column_names == ["id", "id.1", "value", "id.2"]


def test_sql_executor_extract_table_names_returns_sorted_unique_tables():
    tables = SQLCheckReference.extract_table_names(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
        "postgres",
    )

    assert tables == ["orders", "users"]


def test_sql_executor_extract_table_names_returns_empty_list_on_parse_failure(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr("vowl.contracts.check_reference_sql.sqlglot.parse_one", lambda query, dialect: (_ for _ in ()).throw(ValueError("bad sql")))

    assert SQLCheckReference.extract_table_names("SELECT * FROM", "postgres") == []


def test_sql_executor_output_dialect_defaults_to_input_dialect():
    executor = StubSQLExecutor(SimpleNamespace())

    assert executor.output_dialect == "postgres"


def test_sql_executor_validate_query_security_delegates_with_output_dialect(
    monkeypatch: pytest.MonkeyPatch,
):
    executor = StubSQLExecutor(SimpleNamespace())
    captured: list[tuple[str, str]] = []

    monkeypatch.setattr(
        "vowl.executors.base.validate_query_security",
        lambda query, dialect: captured.append((query, dialect)),
    )

    executor.validate_query_security("SELECT 1")

    assert captured == [("SELECT 1", "postgres")]


@pytest.mark.parametrize(
    ("check", "expected"),
    [
        ({"mustBe": 1}, ("mustBe", 1)),
        ({"mustNotBe": 1}, ("mustNotBe", 1)),
        ({"mustBeGreaterThan": 1}, ("mustBeGreaterThan", 1)),
        ({"mustBeGreaterOrEqualTo": 1}, ("mustBeGreaterOrEqualTo", 1)),
        ({"mustBeLessThan": 1}, ("mustBeLessThan", 1)),
        ({"mustBeLessOrEqualTo": 1}, ("mustBeLessOrEqualTo", 1)),
        ({"mustBeBetween": [1, 3]}, ("mustBeBetween", [1, 3])),
        ({"mustNotBeBetween": [1, 3]}, ("mustNotBeBetween", [1, 3])),
        ({}, ("unknown", None)),
    ],
)
def test_check_reference_get_expected_value_covers_all_supported_operators(check, expected):
    ref = SimpleNamespace(get_check=lambda: check)

    assert CheckReference.get_expected_value(ref) == expected


@pytest.mark.parametrize(
    ("actual_value", "operator", "expected_value", "expected_result"),
    [
        (3, "mustBe", 3, True),
        (3, "mustBe", 2, False),
        (3, "mustNotBe", 2, True),
        (3, "mustNotBe", 3, False),
        (3, "mustBeGreaterThan", 2, True),
        (3, "mustBeGreaterThan", 3, False),
        (3, "mustBeGreaterOrEqualTo", 3, True),
        (2, "mustBeGreaterOrEqualTo", 3, False),
        (2, "mustBeLessThan", 3, True),
        (3, "mustBeLessThan", 3, False),
        (3, "mustBeLessOrEqualTo", 3, True),
        (4, "mustBeLessOrEqualTo", 3, False),
        (2, "mustBeBetween", [1, 3], True),
        (4, "mustBeBetween", [1, 3], False),
        (4, "mustNotBeBetween", [1, 3], True),
        (2, "mustNotBeBetween", [1, 3], False),
        (2, "unknown", None, False),
    ],
)
def test_check_reference_evaluate_covers_supported_and_unknown_operators(
    actual_value,
    operator,
    expected_value,
    expected_result,
):
    assert CheckReference.evaluate(actual_value, operator, expected_value) is expected_result


def test_gx_executor_init_is_reachable_via_concrete_subclass():
    adapter = SimpleNamespace(name="adapter")
    executor = StubGXExecutor(adapter)

    assert executor.adapter is adapter
