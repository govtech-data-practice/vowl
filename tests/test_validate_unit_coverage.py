from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import conftest as test_conftest
import narwhals as nw
import pandas as pd
import pyarrow as pa
import pytest

from vowl.adapters.ibis_adapter import IbisAdapter
from vowl.adapters.multi_source_adapter import MultiSourceAdapter
from vowl.config import ValidationConfig
from vowl.contracts.contract import Contract
from vowl.executors.base import CheckResult
from vowl.validate import ValidationResult, ValidationRunner
from vowl.validation.result_rendering import format_ascii_table


def _nw_df(data: dict[str, list]) -> nw.DataFrame:
    return nw.from_native(pa.table(data), eager_only=True)


def _minimal_contract_data(schema_names: list[str] | None = None) -> dict:
    if schema_names is None:
        schema_names = ["users"]
    return {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "version": "1.0.0",
        "id": "contract-id",
        "status": "active",
        "schema": [{"name": name, "properties": []} for name in schema_names],
    }


def _contract_data_with_properties(schema_columns: dict[str, list[str]]) -> dict:
    return {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "version": "1.0.0",
        "id": "contract-id",
        "status": "active",
        "schema": [
            {
                "name": schema_name,
                "properties": [{"name": column_name} for column_name in column_names],
            }
            for schema_name, column_names in schema_columns.items()
        ],
    }


def _make_contract(monkeypatch: pytest.MonkeyPatch, schema_names: list[str] | None = None) -> Contract:
    monkeypatch.setattr("vowl.contracts.contract.validate_contract", lambda data, version: None)
    return Contract(_minimal_contract_data(schema_names))


class FakeMultiAdapter:
    def __init__(self, adapters: dict[str, object]):
        self.adapters = adapters
        self.max_failed_rows = None
        self.use_try_cast = None
        self.test_connections_called_with = None
        self.run_checks_called_with = None
        self.total_rows_called_with = None

    def test_connections(self, check_refs_by_schema):
        self.test_connections_called_with = check_refs_by_schema
        return {"users": {"status": "ok"}}

    def run_checks(self, check_refs_by_schema):
        self.run_checks_called_with = check_refs_by_schema
        return [CheckResult("check_1", "PASSED", "ok", failed_rows_count=0)]

    def get_total_rows_by_schema(self, max_rows_for_statistics):
        self.total_rows_called_with = max_rows_for_statistics
        return {"users": 10}


class FakeExportAdapter:
    def __init__(self, columns: list[str]):
        self._columns = columns

    def export_table_as_arrow(self, schema_name: str) -> pa.Table:
        return pa.table({column_name: [None] for column_name in self._columns})


class FakeFailingExportAdapter:
    def export_table_as_arrow(self, schema_name: str) -> pa.Table:
        raise RuntimeError(f"failed to export {schema_name}")


class CountingExportAdapter:
    def __init__(self, columns: list[str]):
        self._columns = columns
        self.calls: list[str] = []

    def export_table_as_arrow(self, schema_name: str) -> pa.Table:
        self.calls.append(schema_name)
        return pa.table({column_name: [None] for column_name in self._columns})


def _sample_validation_result() -> ValidationResult:
    failed_a = CheckResult(
        "rule_a",
        "FAILED",
        "failed details",
        actual_value=2,
        expected_value=0,
        failed_rows=_nw_df({"id": [1], "value": ["bad"]}),
        failed_rows_count=1,
        supports_row_level_output=True,
        metadata={"tables_in_query": ["users"], "dimension": "completeness", "schema_name": "users", "target": "users.value", "rendered_implementation": "value IS NULL"},
        execution_time_ms=1.5,
    )
    failed_b = CheckResult(
        "rule_b",
        "FAILED",
        "failed details",
        actual_value=1,
        expected_value=0,
        failed_rows=_nw_df({"id": [1], "value": ["bad"]}),
        failed_rows_count=1,
        supports_row_level_output=True,
        metadata={"tables_in_query": ["users"], "dimension": "validity", "schema_name": "users", "target": "users.value", "rendered_implementation": "value NOT IN ('bad')"},
        execution_time_ms=2.5,
    )
    passed = CheckResult(
        "rule_c",
        "PASSED",
        "passed details",
        actual_value=0,
        expected_value=0,
        failed_rows=_nw_df({}),
        failed_rows_count=0,
        supports_row_level_output=True,
        metadata={"tables_in_query": ["users"], "dimension": "conformity", "schema_name": "users", "tags": ["a", "b"]},
        execution_time_ms=0.5,
    )
    error = CheckResult(
        "rule_d",
        "ERROR",
        "x" * 400,
        failed_rows_count=0,
        metadata={"dimension": "accuracy", "schema_name": "users"},
        execution_time_ms=3.0,
    )
    summary = {
        "validation_summary": {
            "total_checks": 4,
            "passed": 1,
            "failed": 2,
            "errors": 1,
            "total_rows_by_schema": {"users": 10},
            "config": {"max_rows_for_statistics": 5},
            "failed_rows": 2,
            "total_execution_time_ms": 7.5,
            "success_rate": 25.0,
            "connection_results": {"users": {"status": "ok"}},
        },
        "check_results": [],
        "contract_metadata": {"id": "contract-id"},
    }
    contract = SimpleNamespace(
        get_api_version=lambda: "v3.1.0",
        get_metadata=lambda: {"id": "contract-id"},
        contract_data={"kind": "DataContract"},
    )
    multi_adapter = SimpleNamespace(adapters={"users": SimpleNamespace()})
    return ValidationResult(summary, [failed_a, failed_b, passed, error], contract, multi_adapter, ["users"])


def test_validation_result_repr_and_passed_property():
    result = _sample_validation_result()

    assert repr(result) == "ValidationResult(passed=False, checks=4, passed_checks=1, failed_checks=2)"
    assert result.passed is False


def test_validation_result_contract_id_falls_back_to_unknown():
    summary = {
        "validation_summary": {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "total_rows_by_schema": {},
            "config": {},
            "failed_rows": 0,
            "total_execution_time_ms": 0.0,
            "success_rate": 100.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(get_api_version=lambda: "v3.1.0", get_metadata=lambda: {}, contract_data={})
    result = ValidationResult(summary, [], contract, SimpleNamespace(adapters={}), [])

    assert result.contract_id == "unknown"


def test_validation_result_contract_data_property_returns_underlying_contract_data():
    summary = {
        "validation_summary": {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "total_rows_by_schema": {},
            "config": {},
            "failed_rows": 0,
            "total_execution_time_ms": 0.0,
            "success_rate": 100.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(get_api_version=lambda: "v3.1.0", get_metadata=lambda: {}, contract_data={"kind": "DataContract"})
    result = ValidationResult(summary, [], contract, SimpleNamespace(adapters={}), [])

    assert result.contract_data == {"kind": "DataContract"}


def test_validation_result_print_summary_show_methods_and_chaining(capsys: pytest.CaptureFixture[str]):
    result = _sample_validation_result()

    chained = result.print_summary().show_failed_checks().show_failed_rows(max_rows=1)

    assert chained is result
    output = capsys.readouterr().out
    assert "Data Quality Validation Results" in output
    assert "OVERALL DATA QUALITY" in output
    assert "Overall:" in output
    assert "Checks Pass Rate:       1 / 4 (25.0%)" in output
    assert "Single Table:" in output
    assert "Multi Table:" in output
    assert "ERRORED Checks:         1" in output
    assert "Unique Passed Rows:     9 / 10 (90.0%)" in output
    assert "Non-unique Failed Rows: 0" in output
    assert "VALIDATION CHECKS" not in output
    assert "CHECK RESULTS" in output
    assert "Total Execution:       7.50 ms" in output
    assert "FAILED" in output
    assert "ERROR" in output
    assert "PASSED" in output
    assert "users" in output
    assert "Target" in output
    assert "users.value" in output
    assert "rule_a" in output
    assert "check_id" in output
    assert "tables_in_query" in output
    assert "| users" in output
    assert "execution time" in output
    assert output.count("+--------+") >= 2
    assert "Failed Checks and Rows" in output
    assert "=== Failed Checks and Rows (up to 1 row(s) per failed check) ===" in output
    assert "  users" in output
    assert "    Single checks" in output
    assert "      [rule_a]" in output
    assert "        Rule:     value IS NULL" in output
    assert output.index("| rule_a") < output.index("| rule_d")
    assert output.index("| rule_d") < output.index("| rule_c")


def test_validation_result_show_methods_when_nothing_failed(capsys: pytest.CaptureFixture[str]):
    summary = {
        "validation_summary": {
            "total_checks": 1,
            "passed": 1,
            "failed": 0,
            "errors": 0,
            "total_rows_by_schema": {},
            "config": {},
            "failed_rows": 0,
            "total_execution_time_ms": 0.1,
            "success_rate": 100.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(get_api_version=lambda: "v3.1.0", get_metadata=lambda: {}, contract_data={})
    result = ValidationResult(
        summary,
        [CheckResult("rule", "PASSED", "ok", failed_rows=_nw_df({}))],
        contract,
        SimpleNamespace(adapters={"users": SimpleNamespace()}),
        ["users"],
    )

    result.show_failed_checks().show_failed_rows()

    output = capsys.readouterr().out
    assert result.passed is True
    assert "All checks passed!" in output
    assert "No failed rows found!" in output


def test_validation_result_show_failed_rows_supports_full_mode(capsys: pytest.CaptureFixture[str]):
    summary = {
        "validation_summary": {
            "total_checks": 1,
            "passed": 0,
            "failed": 1,
            "errors": 0,
            "total_rows_by_schema": {"users": 3},
            "config": {},
            "failed_rows": 3,
            "total_execution_time_ms": 0.1,
            "success_rate": 0.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(get_api_version=lambda: "v3.1.0", get_metadata=lambda: {}, contract_data={})
    result = ValidationResult(
        summary,
        [
            CheckResult(
                "rule",
                "FAILED",
                "bad rows",
                actual_value=3,
                expected_value=0,
                failed_rows=_nw_df({"id": [1, 2, 3]}),
                failed_rows_count=3,
                metadata={"tables_in_query": ["users"], "schema_name": "users"},
            )
        ],
        contract,
        SimpleNamespace(adapters={"users": SimpleNamespace()}),
        ["users"],
    )

    result.show_failed_rows(max_rows=1)
    truncated_output = capsys.readouterr().out
    assert "=== Failed Checks and Rows (up to 1 row(s) per failed check) ===" in truncated_output
    assert "  users" in truncated_output
    assert "    Single checks" in truncated_output
    assert "Rows shown: 1 of 3" in truncated_output

    result.show_failed_rows(max_rows=-1)
    full_output = capsys.readouterr().out
    assert "=== Failed Checks and Rows (all) ===" in full_output
    assert "  users" in full_output
    assert "    Single checks" in full_output
    assert "Rows shown: 3 of 3" in full_output


def test_validation_result_format_ascii_table_renders_pretty_grid():
    table = pa.table(
        {
            "City name": ["Adelaide", "Brisbane"],
            "Area": [1295, 5905],
            "Population": [1158259, 1857594],
        }
    )

    rendered = format_ascii_table(table)

    assert "+-----------+------+------------+" in rendered
    assert "| City name | Area | Population |" in rendered
    assert "| Adelaide  | 1295 | 1158259    |" in rendered
    assert "| Brisbane  | 5905 | 1857594    |" in rendered


def test_validation_result_output_and_consolidation_helpers():
    result = _sample_validation_result()

    output_dfs = result.get_output_dfs(checks=["rule_a", "rule_c"])
    assert list(output_dfs) == ["users::rule_a", "users::rule_c"]
    assert output_dfs["users::rule_a"].to_pandas()["check_id"].tolist() == ["rule_a"]
    assert output_dfs["users::rule_c"].to_pandas().empty

    consolidated = result.get_consolidated_output_dfs(checks=["rule_a", "rule_b"])
    assert list(consolidated) == ["users"]
    consolidated_df = consolidated["users"].to_pandas()
    assert consolidated_df["check_ids"].tolist() == ["rule_a, rule_b"]
    assert consolidated_df["tables_in_query"].tolist() == ["users"]


def test_validation_result_row_quality_summary_uses_deduplicated_failed_rows():
    result = _sample_validation_result()

    row_quality_by_schema = result._get_row_quality_summary_by_schema()

    assert row_quality_by_schema == {
        "users": {
            "total_rows": 10,
            "records_with_issues": 1,
            "clean_records": 9,
            "data_quality": 90.0,
        }
    }


def test_validation_result_row_quality_excludes_cross_table_failures():
    summary = {
        "validation_summary": {
            "total_checks": 2,
            "passed": 0,
            "failed": 2,
            "errors": 0,
            "total_rows_by_schema": {"users": 10, "orders": 5},
            "config": {},
            "failed_rows": 4,
            "total_execution_time_ms": 1.0,
            "success_rate": 0.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(get_api_version=lambda: "v3.1.0", get_metadata=lambda: {}, contract_data={})
    result = ValidationResult(
        summary,
        [
            CheckResult(
                "users_rule",
                "FAILED",
                "failed details",
                actual_value=1,
                expected_value=0,
                failed_rows=_nw_df({"id": [1]}),
                failed_rows_count=1,
                supports_row_level_output=True,
                metadata={"tables_in_query": ["users"], "schema_name": "users"},
            ),
            CheckResult(
                "cross_rule",
                "FAILED",
                "failed details",
                actual_value=3,
                expected_value=0,
                failed_rows=_nw_df({"id": [1, 1, 2], "order_id": [10, 11, 12]}),
                failed_rows_count=3,
                metadata={"tables_in_query": ["users", "orders"], "schema_name": "users"},
            ),
        ],
        contract,
        SimpleNamespace(adapters={"users": FakeExportAdapter(["id"]), "orders": FakeExportAdapter(["order_id"])}),
        ["users", "orders"],
    )

    row_quality_by_schema = result._get_row_quality_summary_by_schema()

    assert row_quality_by_schema == {
        "users": {
            "total_rows": 10,
            "records_with_issues": 1,
            "clean_records": 9,
            "data_quality": 90.0,
        }
    }


def test_validation_result_row_quality_uses_failed_row_columns_when_export_fails():
    summary = {
        "validation_summary": {
            "total_checks": 1,
            "passed": 0,
            "failed": 1,
            "errors": 0,
            "total_rows_by_schema": {"employees": 2, "payroll": 2},
            "config": {},
            "failed_rows": 2,
            "total_execution_time_ms": 1.0,
            "success_rate": 0.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(
        get_api_version=lambda: "v3.1.0",
        get_metadata=lambda: {},
        contract_data=_contract_data_with_properties(
            {
                "employees": ["employee_id"],
                "payroll": ["employee_id", "payroll_id"],
            }
        ),
    )
    result = ValidationResult(
        summary,
        [
            CheckResult(
                "payroll_rule",
                "FAILED",
                "failed details",
                actual_value=2,
                expected_value=0,
                failed_rows=_nw_df({"employee_id": [1001, 1001], "payroll_id": [5001, 5002]}),
                failed_rows_count=2,
                supports_row_level_output=True,
                metadata={"tables_in_query": ["payroll"], "schema_name": "payroll"},
            )
        ],
        contract,
        SimpleNamespace(
            adapters={
                "employees": SimpleNamespace(),
                "payroll": FakeFailingExportAdapter(),
            }
        ),
        ["employees", "payroll"],
    )

    row_quality_by_schema = result._get_row_quality_summary_by_schema()

    assert row_quality_by_schema == {
        "payroll": {
            "total_rows": 2,
            "records_with_issues": 2,
            "clean_records": 0,
            "data_quality": 0.0,
        }
    }


def test_validation_result_summary_does_not_use_adapter_export_for_schema_columns(capsys: pytest.CaptureFixture[str]):
    adapter = CountingExportAdapter(["id"])
    summary = {
        "validation_summary": {
            "total_checks": 1,
            "passed": 0,
            "failed": 1,
            "errors": 0,
            "total_rows_by_schema": {"users": 2},
            "config": {},
            "failed_rows": 1,
            "total_execution_time_ms": 1.0,
            "success_rate": 0.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(
        get_api_version=lambda: "v3.1.0",
        get_metadata=lambda: {},
        contract_data=_contract_data_with_properties({"users": ["id"]}),
    )
    result = ValidationResult(
        summary,
        [
            CheckResult(
                "users_rule",
                "FAILED",
                "failed details",
                actual_value=1,
                expected_value=0,
                failed_rows=_nw_df({"id": [1]}),
                failed_rows_count=1,
                supports_row_level_output=True,
                metadata={"tables_in_query": ["users"], "schema_name": "users"},
            )
        ],
        contract,
        SimpleNamespace(adapters={"users": adapter}),
        ["users"],
    )

    result.print_summary()
    capsys.readouterr()
    result.print_summary()

    assert adapter.calls == []


def test_validation_result_print_summary_shows_row_quality_per_schema(capsys: pytest.CaptureFixture[str]):
    payroll_failure = CheckResult(
        "payroll_rule",
        "FAILED",
        "failed details",
        actual_value=2,
        expected_value=0,
        failed_rows=_nw_df({"employee_id": ["e1", "e2"], "payroll_id": ["p1", "p2"]}),
        failed_rows_count=2,
        supports_row_level_output=True,
        metadata={"tables_in_query": ["payroll"], "schema_name": "payroll"},
    )
    cross_schema_failure = CheckResult(
        "cross_rule",
        "FAILED",
        "failed details",
        actual_value=2,
        expected_value=0,
        failed_rows=_nw_df(
            {
                "employee_id": ["e1", "e2"],
                "payroll_id": ["p1", "p2"],
                "employee_id.1": [None, None],
                "phone_number": [None, None],
            }
        ),
        failed_rows_count=2,
        supports_row_level_output=True,
        metadata={"tables_in_query": ["employee_list", "payroll"], "schema_name": "payroll"},
    )
    employee_list_failure = CheckResult(
        "employee_list_rule",
        "FAILED",
        "failed details",
        actual_value=2,
        expected_value=0,
        failed_rows=_nw_df({"employee_id": ["e10", "e11"], "phone_number": ["1", "2"]}),
        failed_rows_count=2,
        supports_row_level_output=True,
        metadata={"tables_in_query": ["employee_list"], "schema_name": "employee_list"},
    )
    cross_schema_pass = CheckResult(
        "cross_pass_rule",
        "PASSED",
        "passed details",
        actual_value=0,
        expected_value=0,
        failed_rows=_nw_df({}),
        failed_rows_count=0,
        supports_row_level_output=True,
        metadata={"tables_in_query": ["employee_list", "payroll"], "schema_name": "employee_list"},
    )
    payroll_error = CheckResult(
        "payroll_error_rule",
        "ERROR",
        "query failed",
        failed_rows=_nw_df({}),
        failed_rows_count=0,
        metadata={"tables_in_query": ["payroll"], "schema_name": "payroll"},
    )
    summary = {
        "validation_summary": {
            "total_checks": 5,
            "passed": 1,
            "failed": 3,
            "errors": 1,
            "total_rows_by_schema": {"payroll": 2, "employee_list": 2},
            "config": {},
            "failed_rows": 6,
            "total_execution_time_ms": 2.0,
            "success_rate": 20.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(get_api_version=lambda: "v3.1.0", get_metadata=lambda: {}, contract_data={})
    result = ValidationResult(
        summary,
        [payroll_failure, cross_schema_failure, employee_list_failure, cross_schema_pass, payroll_error],
        contract,
        SimpleNamespace(
            adapters={
                "payroll": FakeExportAdapter(["employee_id", "payroll_id"]),
                "employee_list": FakeExportAdapter(["employee_id", "phone_number"]),
            }
        ),
        ["payroll", "employee_list"],
    )

    result.print_summary()

    output = capsys.readouterr().out
    assert "OVERALL DATA QUALITY" in output
    assert "payroll:" in output
    assert "employee_list:" in output
    assert "Overall:" in output
    assert "Single Table:" in output
    assert "Multi Table:" in output
    assert "Checks Pass Rate:       0 / 2 (0.0%)" in output
    assert "Checks Pass Rate:       1 / 2 (50.0%)" in output
    assert "Checks Pass Rate:       0 / 1 (0.0%)" in output
    assert "Checks Pass Rate:       1 / 1 (100.0%)" in output
    assert "Checks Pass Rate:       0 / 3 (0.0%)" in output
    assert output.count("ERRORED Checks:         0") >= 4
    assert "ERRORED Checks:         1" in output
    assert "Unique Passed Rows:     0 / 2 (0.0%)" in output
    assert "Non-unique Failed Rows: 2" in output
    assert "Non-unique Failed Rows: 0" in output
    assert "CHECK RESULTS" in output
    assert "payroll" in output
    assert "employee_list" in output
    assert "Target" in output
    assert "employee_list, payroll" in output
    assert "employee_list" in output
    assert "cross_rule" in output


def test_validation_result_print_summary_omits_row_quality_when_only_cross_table_failures(capsys: pytest.CaptureFixture[str]):
    summary = {
        "validation_summary": {
            "total_checks": 1,
            "passed": 0,
            "failed": 1,
            "errors": 0,
            "total_rows_by_schema": {"users": 10, "orders": 5},
            "config": {},
            "failed_rows": 2,
            "total_execution_time_ms": 1.0,
            "success_rate": 0.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(get_api_version=lambda: "v3.1.0", get_metadata=lambda: {}, contract_data={})
    result = ValidationResult(
        summary,
        [
            CheckResult(
                "cross_rule",
                "FAILED",
                "failed details",
                actual_value=2,
                expected_value=0,
                failed_rows=_nw_df({"id": [1, 2], "order_id": [10, 20]}),
                failed_rows_count=2,
                supports_row_level_output=True,
                metadata={"tables_in_query": ["users", "orders"], "schema_name": "users"},
            )
        ],
        contract,
        SimpleNamespace(adapters={"users": FakeExportAdapter(["id"]), "orders": FakeExportAdapter(["order_id"])}),
        ["users", "orders"],
    )

    result.print_summary()

    output = capsys.readouterr().out
    assert "OVERALL DATA QUALITY" in output
    assert "users:" in output
    assert "orders:" in output
    assert "Overall:" in output
    assert "Single Table:" in output
    assert "Checks Pass Rate:       0 / 0 (N/A)" in output
    assert "ERRORED Checks:         0" in output
    assert "Unique Passed Rows:     10 / 10 (100.0%)" in output
    assert "Unique Passed Rows:     5 / 5 (100.0%)" in output
    assert "Multi Table:" in output
    assert output.count("Non-unique Failed Rows: 2") == 1
    assert "CHECK RESULTS" in output
    assert "users" in output
    assert "Target" in output
    assert "users, orders" in output
    assert "cross_rule" in output


def test_validation_result_consolidation_handles_no_failed_rows_and_no_data_columns():
    summary = {
        "validation_summary": {
            "total_checks": 1,
            "passed": 1,
            "failed": 0,
            "errors": 0,
            "total_rows_by_schema": {},
            "config": {},
            "failed_rows": 0,
            "total_execution_time_ms": 0.0,
            "success_rate": 100.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(get_api_version=lambda: "v3.1.0", get_metadata=lambda: {"id": "contract-id"}, contract_data={})
    empty_result = ValidationResult(
        summary,
        [CheckResult("rule", "PASSED", "ok", failed_rows=_nw_df({}))],
        contract,
        SimpleNamespace(adapters={"users": SimpleNamespace()}),
        ["users"],
    )
    assert empty_result.get_consolidated_output_dfs() == {}

    grouped = ValidationResult._consolidate_grouped_output(
        _nw_df({"check_id": ["rule_a", "rule_b"], "tables_in_query": ["users", "users"]})
    ).to_pandas()
    assert grouped["check_ids"].tolist() == ["rule_a, rule_b"]
    assert grouped["tables_in_query"].tolist() == ["users"]


def test_validation_result_consolidation_adds_suffix_for_same_table_different_column_sets():
    summary = {
        "validation_summary": {
            "total_checks": 2,
            "passed": 0,
            "failed": 2,
            "errors": 0,
            "total_rows_by_schema": {"users": 10},
            "config": {},
            "failed_rows": 2,
            "total_execution_time_ms": 0.0,
            "success_rate": 0.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(get_api_version=lambda: "v3.1.0", get_metadata=lambda: {"id": "contract-id"}, contract_data={})
    result = ValidationResult(
        summary,
        [
            CheckResult("rule_a", "FAILED", "bad", failed_rows=_nw_df({"id": [1]}), failed_rows_count=1, metadata={"tables_in_query": ["users"]}),
            CheckResult("rule_b", "FAILED", "bad", failed_rows=_nw_df({"other": [2]}), failed_rows_count=1, metadata={"tables_in_query": ["users"]}),
        ],
        contract,
        SimpleNamespace(adapters={"users": SimpleNamespace()}),
        ["users"],
    )

    consolidated = result.get_consolidated_output_dfs()
    assert list(consolidated) == ["users__1", "users__2"]


def test_validation_result_get_check_results_df():
    result = _sample_validation_result()

    checks_df = result.get_check_results_df().to_pandas()
    assert {"check_name", "status", "message", "execution_time_ms", "failed_rows_count"}.issubset(checks_df.columns)
    assert checks_df.columns.tolist()[:7] == [
        "check_name",
        "target",
        "schema_name",
        "dimension",
        "status",
        "actual_value",
        "expected_value",
    ]
    assert checks_df.loc[checks_df["check_name"] == "rule_c", "tags"].iloc[0] == "['a', 'b']"
    assert checks_df.loc[checks_df["check_name"] == "rule_a", "failed_rows_count"].iloc[0] == 1
    assert checks_df.loc[checks_df["check_name"] == "rule_d", "failed_rows_count"].iloc[0] == 0
    assert checks_df.loc[checks_df["check_name"] == "rule_a", "message"].iloc[0] == ""
    assert checks_df.loc[checks_df["check_name"] == "rule_c", "message"].iloc[0] == ""
    assert checks_df.loc[checks_df["check_name"] == "rule_d", "message"].iloc[0] == "x" * 400


def test_validation_result_get_output_dfs_normalizes_string_tables_in_query():
    summary = {
        "validation_summary": {
            "total_checks": 1,
            "passed": 0,
            "failed": 1,
            "errors": 0,
            "total_rows_by_schema": {"users": 10},
            "config": {},
            "failed_rows": 1,
            "total_execution_time_ms": 0.0,
            "success_rate": 0.0,
            "connection_results": {},
        },
        "check_results": [],
        "contract_metadata": {},
    }
    contract = SimpleNamespace(get_api_version=lambda: "v3.1.0", get_metadata=lambda: {"id": "contract-id"}, contract_data={})
    result = ValidationResult(
        summary,
        [
            CheckResult(
                "rule_a",
                "FAILED",
                "bad",
                failed_rows=_nw_df({"id": [1]}),
                failed_rows_count=1,
                metadata={"tables_in_query": "users, orders", "schema_name": "users"},
            )
        ],
        contract,
        SimpleNamespace(adapters={"users": SimpleNamespace()}),
        ["users"],
    )

    output_dfs = result.get_output_dfs()

    assert output_dfs["users::rule_a"].to_pandas()["tables_in_query"].tolist() == ["orders, users"]


def test_validation_result_save_and_save_dataframe(tmp_path: Path, capsys: pytest.CaptureFixture[str]):
    result = _sample_validation_result()

    result.save(output_dir=str(tmp_path), prefix="artifact")

    assert (tmp_path / "artifact_check_results.csv").exists()
    assert (tmp_path / "artifact_users.csv").exists()
    assert (tmp_path / "artifact_summary.json").exists()

    df = _nw_df({"id": [1], "value": ["x"]})
    ValidationResult.save_dataframe(df, str(tmp_path / "out.csv"), "csv")
    ValidationResult.save_dataframe(df, str(tmp_path / "out.parquet"), "parquet")
    ValidationResult.save_dataframe(df, str(tmp_path / "out.json"), "json")
    assert (tmp_path / "out.csv").exists()
    assert (tmp_path / "out.parquet").exists()
    assert json.loads((tmp_path / "out.json").read_text().splitlines()[0]) == {"id": 1, "value": "x"}

    with pytest.raises(ValueError, match="Unsupported format"):
        ValidationResult.save_dataframe(df, str(tmp_path / "out.txt"), "txt")

    assert "Saved to:" in capsys.readouterr().out


def test_validation_result_save_dataframe_supports_arrow_tables_and_native_to_arrow(tmp_path: Path):
    arrow_table = pa.table({"id": [1]})
    ValidationResult.save_dataframe(arrow_table, str(tmp_path / "arrow.csv"), "csv")

    class NativeWithArrow:
        def to_arrow(self):
            return pa.table({"id": [2]})

    ValidationResult.save_dataframe(NativeWithArrow(), str(tmp_path / "native.csv"), "csv")

    assert (tmp_path / "arrow.csv").exists()
    assert (tmp_path / "native.csv").exists()


def test_validation_result_save_dataframe_wraps_plain_native_dataframes(tmp_path: Path):
    plain_df = pd.DataFrame({"id": [3], "value": ["plain"]})

    ValidationResult.save_dataframe(plain_df, str(tmp_path / "plain.csv"), "csv")

    assert (tmp_path / "plain.csv").exists()


def test_validation_result_display_full_report_returns_self(capsys: pytest.CaptureFixture[str]):
    result = _sample_validation_result()

    assert result.display_full_report() is result
    assert "Data Quality Validation Results" in capsys.readouterr().out


def test_validation_runner_resolve_adapters_handles_multi_source_direct(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch, ["users"])
    adapter = IbisAdapter(SimpleNamespace(raw_sql=lambda query: None))
    multi = MultiSourceAdapter({"users": adapter})
    runner = ValidationRunner(contract=contract, adapters=multi)

    resolved = runner._resolve_adapters()

    assert resolved is multi
    assert runner._schema_names == ["users"]


def test_validation_runner_init_uses_existing_contract_instance(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch, ["users"])
    runner = ValidationRunner(contract=contract, adapters={"users": object()})

    assert runner._contract is contract


def test_validation_runner_init_loads_contract_from_path(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch, ["users"])

    monkeypatch.setattr("vowl.validate.Contract.load", lambda path: contract)

    runner = ValidationRunner(contract="contract.yaml", adapters={"users": object()})

    assert runner._contract is contract


def test_validation_runner_resolve_adapters_validates_input_and_missing_schemas(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch, ["users"])
    runner = ValidationRunner(contract=contract, adapters="bad-input")

    with pytest.raises(TypeError, match="Expected dict or MultiSourceAdapter"):
        runner._resolve_adapters()

    contract_without_schemas = _make_contract(monkeypatch, [])
    runner = ValidationRunner(contract=contract_without_schemas, adapters={})
    with pytest.raises(ValueError, match="Contract has no schemas with names defined"):
        runner._resolve_adapters()


def test_validation_runner_resolve_adapters_warns_on_extra_and_errors_on_missing(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch, ["users", "orders"])
    runner = ValidationRunner(contract=contract, adapters={"users": object(), "extra": object()})
    mapped = IbisAdapter(SimpleNamespace(raw_sql=lambda query: None))

    monkeypatch.setattr("vowl.validate.DataSourceMapper.get_adapter", lambda self, adapter_input, schema_name: mapped)

    with pytest.warns(UserWarning, match="Adapter provided for 'extra'"):
        with pytest.raises(ValueError, match=r"No adapter provided for schema\(s\)"):
            runner._resolve_adapters()


def test_validation_runner_resolve_adapters_builds_multi_source(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch, ["users"])
    runner = ValidationRunner(contract=contract, adapters={"users": object()})
    mapped = IbisAdapter(SimpleNamespace(raw_sql=lambda query: None))

    monkeypatch.setattr("vowl.validate.DataSourceMapper.get_adapter", lambda self, adapter_input, schema_name: mapped)

    resolved = runner._resolve_adapters()

    assert isinstance(resolved, MultiSourceAdapter)
    assert resolved.get_adapter("users") is mapped


def test_validation_runner_resolve_adapters_keeps_existing_ibis_adapter(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch, ["users"])
    adapter = IbisAdapter(SimpleNamespace(raw_sql=lambda query: None))
    runner = ValidationRunner(contract=contract, adapters={"users": adapter})

    resolved = runner._resolve_adapters()

    assert resolved.get_adapter("users") is adapter


def test_validation_runner_run_propagates_config_and_builds_result(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch, ["users"])
    fake_multi = FakeMultiAdapter({"users": SimpleNamespace(max_failed_rows=None, use_try_cast=None)})
    config = ValidationConfig(max_failed_rows=7, use_try_cast=False, enable_additional_schema_statistics=True, max_rows_for_statistics=12)
    runner = ValidationRunner(contract=contract, adapters={"users": object()}, config=config)

    monkeypatch.setattr(runner, "_resolve_adapters", lambda: fake_multi)
    monkeypatch.setattr(contract, "get_check_references_by_schema", lambda: {"users": ["check-ref"]})

    result = runner.run()

    assert isinstance(result, ValidationResult)
    assert fake_multi.max_failed_rows == 7
    assert fake_multi.use_try_cast is False
    assert fake_multi.adapters["users"].max_failed_rows == 7
    assert fake_multi.adapters["users"].use_try_cast is False
    assert fake_multi.test_connections_called_with == {"users": ["check-ref"]}
    assert fake_multi.run_checks_called_with == {"users": ["check-ref"]}
    assert fake_multi.total_rows_called_with == 12
    assert result.summary["validation_summary"]["total_rows_by_schema"] == {"users": 10}


def test_validation_runner_build_summary_aggregates_counts(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch, ["users"])
    runner = ValidationRunner(contract=contract, adapters={"users": object()})
    check_results = [
        CheckResult("pass", "PASSED", "ok", failed_rows_count=0, execution_time_ms=1.0),
        CheckResult("fail", "FAILED", "bad", failed_rows_count=3, supports_row_level_output=True, execution_time_ms=2.0),
        CheckResult("err", "ERROR", "boom", failed_rows_count=0, execution_time_ms=4.0),
    ]

    summary = runner._build_summary(check_results, {"users": 10}, {"users": {"status": "ok"}})

    assert summary["validation_summary"]["passed"] == 1
    assert summary["validation_summary"]["failed"] == 1
    assert summary["validation_summary"]["errors"] == 1
    assert summary["validation_summary"]["failed_rows"] == 3
    assert summary["validation_summary"]["total_execution_time_ms"] == 7.0


def test_validate_data_guards_and_dispatch(monkeypatch: pytest.MonkeyPatch):
    original_validate_data = test_conftest._ORIGINAL_VALIDATE_DATA
    contract = _make_contract(monkeypatch, ["users"])
    runner_calls: list[tuple[object, object, object]] = []

    class FakeRunner:
        def __init__(self, contract, adapters, config):
            runner_calls.append((contract, adapters, config))

        def run(self):
            return "runner-result"

    monkeypatch.setattr("vowl.validate.ValidationRunner", FakeRunner)
    monkeypatch.setattr("vowl.validate.Contract.load", lambda path: contract)

    with pytest.raises(ValueError, match="data source must be provided"):
        original_validate_data(contract="contract.yaml")

    with pytest.raises(ValueError, match="Only one data source"):
        original_validate_data(contract="contract.yaml", df=object(), connection_str="sqlite://")

    adapters_result = original_validate_data(contract="contract.yaml", adapters={"users": object()})
    assert adapters_result == "runner-result"
    assert runner_calls[-1][1] == {"users": object()} or isinstance(runner_calls[-1][1], dict)

    adapter = IbisAdapter(SimpleNamespace(raw_sql=lambda query: None))
    single_result = original_validate_data(contract="contract.yaml", adapter=adapter)
    assert single_result == "runner-result"
    assert runner_calls[-1][1].keys() == {"users"}
    assert runner_calls[-1][1]["users"] is adapter

    with pytest.raises(TypeError, match="Pass MultiSourceAdapter via 'adapters='"):
        original_validate_data(contract="contract.yaml", adapter=MultiSourceAdapter({"users": adapter}))


def test_validate_data_accepts_contract_object_and_non_adapter_single_sources(monkeypatch: pytest.MonkeyPatch):
    original_validate_data = test_conftest._ORIGINAL_VALIDATE_DATA
    contract = _make_contract(monkeypatch, ["users"])
    runner_calls: list[tuple[object, object, object]] = []

    class FakeRunner:
        def __init__(self, contract, adapters, config):
            runner_calls.append((contract, adapters, config))

        def run(self):
            return "runner-result"

    monkeypatch.setattr("vowl.validate.ValidationRunner", FakeRunner)

    assert original_validate_data(contract=contract, connection_str="sqlite://") == "runner-result"
    assert runner_calls[-1][0] is contract
    assert runner_calls[-1][1] == {"users": "sqlite://"}


def test_validate_data_errors_when_named_schemas_cannot_be_inferred(monkeypatch: pytest.MonkeyPatch):
    original_validate_data = test_conftest._ORIGINAL_VALIDATE_DATA
    contract = _make_contract(monkeypatch, [])

    monkeypatch.setattr("vowl.validate.Contract.load", lambda path: contract)

    with pytest.raises(ValueError, match="Contract has no schemas with names defined. Cannot infer table name"):
        original_validate_data(contract="contract.yaml", df=object())


