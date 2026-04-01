from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pytest

from vowl.adapters.base import BaseAdapter
from vowl.adapters.ibis_adapter import IbisAdapter
from vowl.adapters.multi_source_adapter import MultiSourceAdapter
from vowl.contracts.models import get_schema, validate_contract
from vowl.executors.base import CheckResult
from vowl.mapper import DataSourceMapper


class StubCheckReference:
    def __init__(self, query: str | None, name: str | None = "check", check_type: str = "sql") -> None:
        self._check = {"query": query, "name": name, "type": check_type}

    def get_check(self):
        return self._check

    def get_check_name(self):
        name = self._check.get("name") or self._check.get("id")
        if name:
            return name
        check_type = self._check.get("metric") or self._check.get("dimension") or "check"
        return check_type


class StubAdapter(BaseAdapter):
    def __init__(self, connection_results=None, run_results=None) -> None:
        super().__init__()
        self.connection_results = connection_results or {}
        self.run_results = run_results or []
        self.tested_tables: list[str] = []
        self.received_refs = []

    def test_connection(self, table_name: str):
        self.tested_tables.append(table_name)
        return self.connection_results.get(table_name)

    def run_checks(self, check_refs):
        self.received_refs.append(check_refs)
        return self.run_results


class StubExecutor:
    def __init__(self, results):
        self.results = results
        self.received_refs = None
        self.cleanup_called = False

    def run_batch_checks(self, check_refs):
        self.received_refs = check_refs
        return self.results

    def cleanup(self):
        self.cleanup_called = True


class StubFetchOneResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class StubConnection:
    def __init__(self, result, name: str = "duckdb") -> None:
        self.result = result
        self.name = name
        self.queries: list[str] = []

    def raw_sql(self, query: str):
        self.queries.append(query)
        return self.result


class DummyFrame:
    def __init__(self, dataframe: pd.DataFrame) -> None:
        self._dataframe = dataframe

    def to_pandas(self) -> pd.DataFrame:
        return self._dataframe.copy()


def test_base_adapter_default_row_count_and_connection_behaviors_warn():
    adapter = BaseAdapter()

    with pytest.warns(UserWarning, match="does not implement get_total_rows"):
        assert adapter.get_total_rows("users") == 0

    with pytest.warns(UserWarning, match="does not implement test_connection"):
        assert adapter.test_connection("users") == (
            "not supported: test_connection is not implemented for this adapter"
        )


def test_base_adapter_is_incompatible_by_default():
    adapter = BaseAdapter()

    assert adapter.is_compatible_with(StubAdapter()) is False


def test_is_multi_table_check_returns_false_without_query():
    adapter = MultiSourceAdapter({"users": StubAdapter()})

    assert adapter._is_multi_table_check(StubCheckReference(query=None)) is False


def test_test_connections_warns_for_inaccessible_and_accessible_unknown_tables():
    users_adapter = StubAdapter(
        connection_results={
            "users": "schema unavailable",
            "ghost": "missing table",
            "reachable": None,
        }
    )
    orders_adapter = StubAdapter()
    adapter = MultiSourceAdapter({"users": users_adapter, "orders": orders_adapter})
    refs = {
        "users": [
            StubCheckReference(
                query=(
                    "SELECT * FROM users "
                    "JOIN orders ON users.id = orders.id "
                    "JOIN ghost ON users.id = ghost.id "
                    "JOIN reachable ON users.id = reachable.id"
                )
            )
        ]
    }

    with pytest.warns(UserWarning) as caught:
        results = adapter.test_connections(refs)

    assert results["users"]["users"] == "schema unavailable"
    assert results["users"]["orders"] == "skipped: table defined in schema 'orders'"
    assert results["users"]["ghost"] == (
        "error: table 'ghost' not accessible via schema 'users' adapter: missing table"
    )
    assert "reachable" not in results["users"]
    assert users_adapter.tested_tables == ["users", "ghost", "reachable"]
    messages = [str(item.message) for item in caught]
    assert any("Connection test failed for schema 'users'" in message for message in messages)
    assert any("not accessible" in message and "ghost" in message for message in messages)
    assert any("not a defined schema but is accessible" in message for message in messages)


def test_run_checks_handles_empty_schema_missing_adapter_and_multi_table_cleanup():
    single_result = CheckResult("single", "PASSED", "ok", execution_time_ms=1)
    multi_result = CheckResult("multi", "PASSED", "ok", execution_time_ms=2)
    child_adapter = StubAdapter(run_results=[single_result])
    adapter = MultiSourceAdapter({"users": child_adapter})
    executor = StubExecutor([multi_result])
    adapter._get_executor = lambda check_type: executor  # type: ignore[method-assign]

    results = adapter.run_checks(
        {
            "empty": [],
            "missing": [StubCheckReference(query="SELECT * FROM missing", name=None)],
            "users": [
                StubCheckReference(query="SELECT * FROM users", name="single"),
                StubCheckReference(
                    query="SELECT * FROM users JOIN orders ON users.id = orders.id",
                    name="multi",
                ),
            ],
        }
    )

    assert [result.check_name for result in results] == ["check", "single", "multi"]
    assert results[0].status == "ERROR"
    assert results[0].details == "No adapter configured for schema 'missing'"
    assert len(child_adapter.received_refs) == 1
    assert child_adapter.received_refs[0][0].get_check()["name"] == "single"
    assert executor.received_refs[0].get_check()["name"] == "multi"
    assert executor.cleanup_called is True


def test_mapper_stringifies_all_columns_when_error_column_is_unknown():
    mapper = DataSourceMapper()
    frame = DummyFrame(pd.DataFrame({"a": [1, None], "b": [2, 3]}))

    table, coerced_columns = mapper._build_arrow_with_column_fallback(
        frame,
        TypeError("Arrow conversion failed without a column name"),
    )

    assert coerced_columns == ["a", "b"]
    assert pa.types.is_string(table.schema.field("a").type) or pa.types.is_large_string(
        table.schema.field("a").type
    )
    assert pa.types.is_string(table.schema.field("b").type) or pa.types.is_large_string(
        table.schema.field("b").type
    )


def test_mapper_reraises_retry_error_after_full_dataframe_stringification(monkeypatch: pytest.MonkeyPatch):
    from vowl import mapper as mapper_module

    mapper = DataSourceMapper()
    frame = DummyFrame(pd.DataFrame({"a": [1, 2]}))

    def raise_arrow_error(*args, **kwargs):
        raise TypeError("Arrow conversion still failed")

    monkeypatch.setattr(
        mapper_module,
        "pa",
        SimpleNamespace(Table=SimpleNamespace(from_pandas=raise_arrow_error)),
    )

    with pytest.raises(TypeError, match="still failed"):
        mapper._build_arrow_with_column_fallback(
            frame,
            TypeError("Arrow conversion failed without a column name"),
        )


def test_ibis_adapter_reports_filter_state_and_incompatible_adapters():
    shared_connection = StubConnection(StubFetchOneResult((1,)))
    adapter = IbisAdapter(shared_connection, filter_conditions={"users": {"field": "id", "operator": ">", "value": 0}})
    other_connection = StubConnection(StubFetchOneResult((1,)))

    assert adapter.has_filter_conditions is True
    assert adapter.is_compatible_with(StubAdapter()) is False
    assert adapter.is_compatible_with(IbisAdapter(other_connection)) is False
    assert adapter.is_compatible_with(IbisAdapter(shared_connection)) is False


def test_ibis_adapter_get_total_rows_uses_limited_subquery_when_capped():
    connection = StubConnection(StubFetchOneResult((5,)))
    adapter = IbisAdapter(connection)

    total_rows = adapter.get_total_rows("users", max_rows=10)

    assert total_rows == 5
    assert "LIMIT 10" in connection.queries[0]
    assert "COUNT(*)" in connection.queries[0]


def test_ibis_adapter_get_total_rows_returns_zero_for_unsupported_result_shape():
    adapter = IbisAdapter(StubConnection(object()))

    assert adapter.get_total_rows("users") == 0


def test_get_schema_raises_when_supported_schema_file_is_missing(monkeypatch: pytest.MonkeyPatch):
    from vowl.contracts import models as models_module

    models_module._schema_cache.clear()
    monkeypatch.setitem(models_module.SCHEMA_FILES, "v9.9.9", "definitely-missing.json")

    with pytest.raises(FileNotFoundError, match="Schema file not found"):
        get_schema("v9.9.9")


def test_validate_contract_requires_api_version_when_not_provided():
    with pytest.raises(ValueError, match="Contract does not specify an apiVersion"):
        validate_contract({})
