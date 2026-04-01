from __future__ import annotations

from types import SimpleNamespace

import pyarrow as pa
import pytest
from sqlglot import exp

from vowl.contracts.check_reference import CheckReference, SQLCheckReference
from vowl.executors.base import CheckResult
from vowl.executors.ibis_sql_executor import IbisSQLExecutor
from vowl.executors.multi_source_sql_executor import MultiSourceSQLExecutor
from vowl.executors.security import (
    SQLSecurityError,
    _check_for_write_subqueries,
    to_table_expression,
    validate_read_only_query,
)


class StubCheckReference:
    def __init__(
        self,
        *,
        check: dict | None = None,
        rendered_query: str | None = "SELECT COUNT(*) FROM users",
        failed_rows_query: str | None = "SELECT * FROM users",
        column_name: str | None = None,
        logical_type: str | None = None,
        schema_name: str = "users",
        path: str = "$.checks[0]",
    ):
        self._check = check or {
            "name": "check_name",
            "type": "sql",
            "query": "SELECT COUNT(*) FROM users",
            "mustBe": 0,
        }
        self._rendered_query = rendered_query
        self._failed_rows_query = failed_rows_query
        self._column_name = column_name
        self._logical_type = logical_type
        self._schema_name = schema_name
        self.path = path

    def get_check(self):
        return self._check

    def is_generated(self):
        return False

    def get_column_name(self):
        return self._column_name

    def get_logical_type(self):
        return self._logical_type

    def get_schema_name(self):
        return self._schema_name

    def get_result_metadata(self):
        metadata = {
            "check_path": self.path,
            "check_ref_type": type(self).__name__,
            "type": self._check.get("type"),
            "description": self._check.get("description"),
            "severity": self._check.get("severity"),
            "schema": self._schema_name,
            "is_generated": self.is_generated(),
            "engine": "sql",
        }

        dimension = self._check.get("dimension")
        if dimension is not None:
            metadata["dimension"] = getattr(dimension, "value", dimension)

        if self._column_name:
            metadata["target"] = f"{self._schema_name}.{self._column_name}" if self._schema_name else self._column_name

        if self._logical_type:
            metadata["logical_type"] = self._logical_type

        metadata["aggregation_type"] = self.aggregation_type
        if self.unit:
            metadata["unit"] = self.unit

        return metadata

    def get_query(self, output_dialect, query_filters, use_try_cast=True):
        return self._rendered_query

    @property
    def unit(self):
        return self._check.get("unit")

    @property
    def aggregation_type(self):
        query = self._rendered_query
        if not query:
            return "custom"
        return SQLCheckReference.detect_aggregation_type(query, "duckdb")

    @property
    def supports_row_level_output(self):
        if self.unit is not None and self.unit != "rows":
            return False
        return self.aggregation_type in ("count", "none")

    def get_scalar_query(self, output_dialect, query_filters, use_try_cast=True):
        query = self._rendered_query
        if not query:
            return None
        if self.aggregation_type == "none":
            return f"SELECT COUNT(*) FROM ({query}) AS _sub"
        return query

    def get_failed_rows_query(self, output_dialect, query_filters, use_try_cast=True):
        return self._failed_rows_query

    def compute_failed_rows_count(self, actual_value):
        unit_is_rows = self.unit is None or self.unit == "rows"
        if self.aggregation_type in ("count", "none") and unit_is_rows:
            try:
                return int(actual_value)
            except (TypeError, ValueError):
                return 0
        return 0

    def get_check_name(self):
        check = self._check
        name = check.get("name") or check.get("id")
        if name:
            return name
        parts = []
        if self._column_name:
            parts.append(self._column_name)
        elif self._schema_name:
            parts.append(self._schema_name)
        check_type = check.get("metric") or check.get("dimension") or "check"
        parts.append(check_type)
        return "_".join(parts)

    def get_expected_value(self):
        return CheckReference.get_expected_value(self)

    @staticmethod
    def evaluate(actual_value, operator, expected_value):
        return CheckReference.evaluate(actual_value, operator, expected_value)

    def _build_full_metadata(self, dialect="", filter_conditions=None, use_try_cast=False, **extra):
        metadata = self.get_result_metadata()
        query = self.get_query(dialect, filter_conditions, use_try_cast)
        if query:
            metadata["tables_in_query"] = SQLCheckReference.extract_table_names(query, dialect or "duckdb")
            metadata["rule"] = query
        metadata.update(extra)
        return metadata

    def build_result(self, *, actual_value, execution_time_ms, failed_rows_fetcher=None,
                     dialect="", filter_conditions=None, use_try_cast=False):
        check = self.get_check()
        operator, expected_value = self.get_expected_value()
        passed = self.evaluate(actual_value, operator, expected_value)
        metadata = self._build_full_metadata(dialect, filter_conditions, use_try_cast)
        if passed:
            return CheckResult(
                check_name=self.get_check_name(), status="PASSED",
                details=check.get("description") or f"Check passed: {operator} {expected_value}",
                actual_value=actual_value, expected_value=expected_value,
                supports_row_level_output=self.supports_row_level_output,
                metadata=metadata, execution_time_ms=execution_time_ms,
            )
        return CheckResult(
            check_name=self.get_check_name(), status="FAILED",
            details=check.get("description") or f"Check failed: expected {operator} {expected_value}, got {actual_value}",
            actual_value=actual_value, expected_value=expected_value,
            failed_rows_fetcher=failed_rows_fetcher,
            failed_rows_count=self.compute_failed_rows_count(actual_value),
            supports_row_level_output=self.supports_row_level_output,
            metadata=metadata, execution_time_ms=execution_time_ms,
        )

    def build_error_result(self, *, error_message, execution_time_ms, dialect="",
                           filter_conditions=None, use_try_cast=False, **extra_metadata):
        metadata = self._build_full_metadata(dialect, filter_conditions, use_try_cast, **extra_metadata)
        return CheckResult(
            check_name=self.get_check_name(), status="ERROR",
            details=error_message, metadata=metadata,
            execution_time_ms=execution_time_ms,
        )


class StubRawSQLConnection:
    def __init__(self, handler):
        self._handler = handler
        self.queries: list[str] = []

    def raw_sql(self, query: str):
        self.queries.append(query)
        return self._handler(query)


class StubFetchArrowResult:
    def __init__(self, table: pa.Table):
        self._table = table

    def to_arrow_table(self):
        return self._table

    def fetch_arrow_table(self):
        return self._table


class StubToArrowResult:
    def __init__(self, table: pa.Table):
        self._table = table

    def toArrow(self):
        return self._table


class StubCollectArrowResult:
    def __init__(self, table: pa.Table):
        self._table = table

    def _collect_as_arrow(self):
        return self._table.to_batches()


class StubFetchOneResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class StubCollectRowsResult:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class StubIbisAdapter:
    def __init__(self, connection, *, filter_conditions=None, max_failed_rows=1000, dialect="duckdb"):
        self._connection = connection
        self.filter_conditions = filter_conditions or {}
        self.max_failed_rows = max_failed_rows
        self._dialect = dialect

    def get_sql_dialect(self):
        return self._dialect

    def get_connection(self):
        return self._connection


class StubMode1Executor:
    def __init__(self, result):
        self.result = result
        self.calls: list[object] = []

    def run_single_check(self, check_ref):
        self.calls.append(check_ref)
        return self.result


class StubAdapter:
    def __init__(self, *, compatible=True, executor=None, export_table=None):
        self.compatible = compatible
        self._executor = executor
        self._export_table = export_table or pa.table({"id": [1]})

    def is_compatible_with(self, other):
        return self.compatible and getattr(other, "compatible", False)

    def _get_executor(self, executor_type):
        assert executor_type == "sql"
        return self._executor

    def export_table_as_arrow(self, schema_name: str):
        return self._export_table


class StubMultiAdapter:
    def __init__(self, adapters, *, max_failed_rows=1000, use_try_cast=True):
        self._adapters = adapters
        self.max_failed_rows = max_failed_rows
        self.use_try_cast = use_try_cast

    def get_adapter(self, schema_name: str):
        return self._adapters.get(schema_name)


def test_multisource_executor_adapter_property_is_not_available():
    executor = MultiSourceSQLExecutor(StubMultiAdapter({}))

    with pytest.raises(NotImplementedError, match="does not have a single adapter"):
        _ = executor.adapter


def test_multisource_detect_tables_warns_on_parse_failure(monkeypatch: pytest.MonkeyPatch):
    executor = MultiSourceSQLExecutor(StubMultiAdapter({}))

    monkeypatch.setattr(
        "vowl.executors.multi_source_sql_executor.sqlglot.parse_one",
        lambda query: (_ for _ in ()).throw(ValueError("bad sql")),
    )

    with pytest.warns(UserWarning, match="Failed to parse SQL query for table detection"):
        assert executor._detect_tables("SELECT") == set()


def test_multisource_are_backends_compatible_returns_false_when_adapter_missing():
    executor = MultiSourceSQLExecutor(StubMultiAdapter({"users": StubAdapter()}))

    assert executor._are_backends_compatible({"users", "orders"}) is False


def test_multisource_fetch_failed_rows_returns_none_for_empty_query():
    executor = MultiSourceSQLExecutor(StubMultiAdapter({}))

    assert executor._fetch_failed_rows(None, {"users"}) is None


def test_multisource_fetch_failed_rows_adds_limit_and_returns_dataframe(monkeypatch: pytest.MonkeyPatch):
    query_log: list[str] = []
    local_con = StubRawSQLConnection(lambda query: query_log.append(query) or StubFetchArrowResult(pa.table({"id": [1]})))
    executor = MultiSourceSQLExecutor(StubMultiAdapter({}, max_failed_rows=5))
    executor._local_duckdb_con = local_con

    monkeypatch.setattr(executor, "validate_query_security", lambda query: None)
    monkeypatch.setattr(executor, "_ensure_tables_available", lambda table_names: None)

    result = executor._fetch_failed_rows("SELECT * FROM users", {"users"})

    assert query_log == ["SELECT * FROM users LIMIT 5"]
    assert result is not None
    assert result.to_pandas().to_dict(orient="records") == [{"id": 1}]


def test_multisource_fetch_failed_rows_warns_and_returns_none_on_error(monkeypatch: pytest.MonkeyPatch):
    local_con = StubRawSQLConnection(lambda query: (_ for _ in ()).throw(RuntimeError("boom")))
    executor = MultiSourceSQLExecutor(StubMultiAdapter({}))
    executor._local_duckdb_con = local_con

    monkeypatch.setattr(executor, "validate_query_security", lambda query: None)
    monkeypatch.setattr(executor, "_ensure_tables_available", lambda table_names: None)

    with pytest.warns(UserWarning, match="Failed to fetch failed rows for cross-schema check: boom"):
        assert executor._fetch_failed_rows("SELECT * FROM users", {"users"}) is None


def test_multisource_run_single_check_errors_when_no_tables_detected(monkeypatch: pytest.MonkeyPatch):
    executor = MultiSourceSQLExecutor(StubMultiAdapter({}))
    check_ref = StubCheckReference()

    monkeypatch.setattr(executor, "_detect_tables", lambda query: set())

    result = executor.run_single_check(check_ref)

    assert result.status == "ERROR"
    assert result.details == "Could not detect tables in query"


def test_multisource_run_single_check_wraps_missing_mode1_adapter(monkeypatch: pytest.MonkeyPatch):
    executor = MultiSourceSQLExecutor(StubMultiAdapter({}))
    check_ref = StubCheckReference()

    monkeypatch.setattr(executor, "_detect_tables", lambda query: {"users"})
    monkeypatch.setattr(executor, "_are_backends_compatible", lambda table_names: True)

    result = executor.run_single_check(check_ref)

    assert result.status == "ERROR"
    assert "No adapter for table 'users'" in result.details


def test_multisource_run_single_check_errors_when_rendered_query_is_missing(monkeypatch: pytest.MonkeyPatch):
    executor = MultiSourceSQLExecutor(StubMultiAdapter({}))
    check_ref = StubCheckReference(rendered_query=None)

    monkeypatch.setattr(executor, "_detect_tables", lambda query: {"users"})
    monkeypatch.setattr(executor, "_are_backends_compatible", lambda table_names: False)

    result = executor.run_single_check(check_ref)

    assert result.status == "ERROR"
    assert result.details == "No query specified for SQL check"


def test_multisource_run_single_check_returns_security_error_with_metadata(monkeypatch: pytest.MonkeyPatch):
    executor = MultiSourceSQLExecutor(StubMultiAdapter({}))
    check_ref = StubCheckReference(column_name="employee_id", logical_type="integer")

    monkeypatch.setattr(executor, "_detect_tables", lambda query: {"users"})
    monkeypatch.setattr(executor, "_are_backends_compatible", lambda table_names: False)
    monkeypatch.setattr(
        executor,
        "_execute_query",
        lambda query, table_names: (_ for _ in ()).throw(
            SQLSecurityError("blocked", violation_type="write_operation", query=query)
        ),
    )

    result = executor.run_single_check(check_ref)

    assert result.status == "ERROR"
    assert result.metadata["schema"] == "users"
    assert result.metadata["target"] == "users.employee_id"
    assert result.metadata["logical_type"] == "integer"
    assert result.metadata["security_violation"] == "write_operation"


def test_multisource_run_single_check_failed_result_defaults_row_count_to_zero(monkeypatch: pytest.MonkeyPatch):
    executor = MultiSourceSQLExecutor(StubMultiAdapter({}))
    check_ref = StubCheckReference()

    monkeypatch.setattr(executor, "_detect_tables", lambda query: {"users"})
    monkeypatch.setattr(executor, "_are_backends_compatible", lambda table_names: False)
    monkeypatch.setattr(executor, "_execute_query", lambda query, table_names: ["not-an-int"])
    monkeypatch.setattr(
        executor,
        "_fetch_failed_rows",
        lambda query, table_names: SimpleNamespace(to_pandas=lambda: pa.table({"id": [1]}).to_pandas()),
    )

    result = executor.run_single_check(check_ref)

    assert result.status == "FAILED"
    assert result.metadata["schema"] == "users"
    assert result.failed_rows_count == 0
    assert result.failed_rows.to_pandas().to_dict(orient="records") == [{"id": 1}]


def test_multisource_cleanup_clears_state_when_disconnect_fails():
    local_con = SimpleNamespace(disconnect=lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    executor = MultiSourceSQLExecutor(StubMultiAdapter({}))
    executor._local_duckdb_con = local_con
    executor._attached_sources = {"users"}

    executor.cleanup()

    assert executor._local_duckdb_con is None
    assert executor._attached_sources == set()


def test_ibis_fetch_failed_rows_returns_none_for_empty_query():
    adapter = StubIbisAdapter(StubRawSQLConnection(lambda query: None))
    executor = IbisSQLExecutor(adapter)

    assert executor._fetch_failed_rows(None) is None


def test_ibis_fetch_failed_rows_adds_limit_and_supports_to_arrow(monkeypatch: pytest.MonkeyPatch):
    query_log: list[str] = []
    connection = StubRawSQLConnection(
        lambda query: query_log.append(query) or StubToArrowResult(pa.table({"id": [1]}))
    )
    executor = IbisSQLExecutor(StubIbisAdapter(connection, max_failed_rows=3))

    monkeypatch.setattr(executor, "validate_query_security", lambda query: None)

    result = executor._fetch_failed_rows("SELECT * FROM users")

    assert query_log == ["SELECT * FROM users LIMIT 3"]
    assert result is not None
    assert result.to_pandas().to_dict(orient="records") == [{"id": 1}]


def test_ibis_fetch_failed_rows_returns_none_for_unsupported_result_shape(monkeypatch: pytest.MonkeyPatch):
    connection = StubRawSQLConnection(lambda query: object())
    executor = IbisSQLExecutor(StubIbisAdapter(connection))

    monkeypatch.setattr(executor, "validate_query_security", lambda query: None)

    assert executor._fetch_failed_rows("SELECT * FROM users LIMIT 1") is None


def test_ibis_fetch_failed_rows_warns_and_returns_none_on_error(monkeypatch: pytest.MonkeyPatch):
    connection = StubRawSQLConnection(lambda query: (_ for _ in ()).throw(RuntimeError("boom")))
    executor = IbisSQLExecutor(StubIbisAdapter(connection))

    monkeypatch.setattr(executor, "validate_query_security", lambda query: None)

    with pytest.warns(UserWarning, match="Failed to fetch failed rows: boom"):
        assert executor._fetch_failed_rows("SELECT * FROM users") is None


def test_ibis_execute_query_returns_none_for_unknown_result_shape(monkeypatch: pytest.MonkeyPatch):
    executor = IbisSQLExecutor(StubIbisAdapter(StubRawSQLConnection(lambda query: object())))

    monkeypatch.setattr(executor, "validate_query_security", lambda query: None)

    assert executor._execute_query("SELECT COUNT(*) FROM users") is None


def test_ibis_run_single_check_errors_when_rendered_query_is_missing():
    executor = IbisSQLExecutor(StubIbisAdapter(StubRawSQLConnection(lambda query: None)))
    check_ref = StubCheckReference(rendered_query=None)

    result = executor.run_single_check(check_ref)

    assert result.status == "ERROR"
    assert result.details == "No query specified for SQL check"


def test_ibis_run_single_check_returns_security_error(monkeypatch: pytest.MonkeyPatch):
    executor = IbisSQLExecutor(StubIbisAdapter(StubRawSQLConnection(lambda query: None)))
    check_ref = StubCheckReference(column_name="employee_id", logical_type="integer")

    monkeypatch.setattr(
        executor,
        "_execute_query",
        lambda query: (_ for _ in ()).throw(
            SQLSecurityError("blocked", violation_type="write_operation", query=query)
        ),
    )

    result = executor.run_single_check(check_ref)

    assert result.status == "ERROR"
    assert result.metadata["target"] == "users.employee_id"
    assert result.metadata["logical_type"] == "integer"
    assert result.metadata["security_violation"] == "write_operation"


def test_ibis_run_single_check_failed_result_defaults_row_count_to_zero(monkeypatch: pytest.MonkeyPatch):
    executor = IbisSQLExecutor(StubIbisAdapter(StubRawSQLConnection(lambda query: None)))
    check_ref = StubCheckReference()

    monkeypatch.setattr(executor, "_execute_query", lambda query: "not-an-int")
    monkeypatch.setattr(
        executor,
        "_fetch_failed_rows",
        lambda query: SimpleNamespace(to_pandas=lambda: pa.table({"id": [1]}).to_pandas()),
    )

    result = executor.run_single_check(check_ref)

    assert result.status == "FAILED"
    assert result.failed_rows_count == 0
    assert result.failed_rows.to_pandas().to_dict(orient="records") == [{"id": 1}]


def test_validate_read_only_query_raises_parse_error(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "vowl.executors.security.sqlglot.parse",
        lambda query, dialect: (_ for _ in ()).throw(ValueError("bad sql")),
    )

    with pytest.raises(SQLSecurityError, match="Failed to parse SQL query") as exc_info:
        validate_read_only_query("SELECT", dialect="duckdb")

    assert exc_info.value.violation_type == "parse_error"


def test_validate_read_only_query_raises_when_no_statements(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("vowl.executors.security.sqlglot.parse", lambda query, dialect: [])

    with pytest.raises(SQLSecurityError, match="No valid SQL statements") as exc_info:
        validate_read_only_query("SELECT", dialect="duckdb")

    assert exc_info.value.violation_type == "no_statements"


def test_validate_read_only_query_skips_none_statements(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("vowl.executors.security.sqlglot.parse", lambda query, dialect: [None])

    validate_read_only_query("SELECT 1", dialect="duckdb")


def test_check_for_write_subqueries_raises_for_nested_write_operation():
    ast = SimpleNamespace(walk=lambda: [exp.Delete()])

    with pytest.raises(SQLSecurityError, match="Write operation 'Delete'") as exc_info:
        _check_for_write_subqueries(ast, "WITH x AS (...) SELECT 1")

    assert exc_info.value.violation_type == "write_in_subquery"


def test_to_table_expression_wraps_parse_errors(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "vowl.executors.security.sqlglot.parse_one",
        lambda identifier, into=None: (_ for _ in ()).throw(ValueError("bad identifier")),
    )

    with pytest.raises(SQLSecurityError, match="Failed to parse identifier 'users'") as exc_info:
        to_table_expression("users")

    assert exc_info.value.violation_type == "invalid_identifier"


def test_to_table_expression_rejects_non_table_results(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "vowl.executors.security.sqlglot.parse_one",
        lambda identifier, into=None: exp.Column(this=exp.Identifier(this="users")),
    )

    with pytest.raises(SQLSecurityError, match="did not produce a table expression") as exc_info:
        to_table_expression("users")

    assert exc_info.value.violation_type == "invalid_identifier"
