"""Tests for aggregation type detection and non-COUNT aggregation support."""

from __future__ import annotations

import pytest

from vowl.contracts.check_reference import CheckReference, SQLCheckReference
from vowl.executors.base import CheckResult
from vowl.executors.ibis_sql_executor import IbisSQLExecutor

# ─── detect_aggregation_type ─────────────────────────────────────────


class TestDetectAggregationType:
    """Unit tests for SQLCheckReference.detect_aggregation_type."""

    @pytest.mark.parametrize(
        "query,expected",
        [
            ("SELECT COUNT(*) FROM users", "count"),
            ("SELECT count(*) FROM users WHERE active = 1", "count"),
            ("SELECT COUNT(id) FROM users", "count"),
        ],
        ids=["count_star", "count_star_with_where", "count_column"],
    )
    def test_count_queries(self, query: str, expected: str):
        assert SQLCheckReference.detect_aggregation_type(query, "duckdb") == expected

    @pytest.mark.parametrize(
        "query,expected",
        [
            ("SELECT COUNT(DISTINCT id) FROM users", "count_distinct"),
            ("SELECT count(DISTINCT name) FROM users", "count_distinct"),
        ],
        ids=["count_distinct_id", "count_distinct_name"],
    )
    def test_count_distinct_queries(self, query: str, expected: str):
        assert SQLCheckReference.detect_aggregation_type(query, "duckdb") == expected

    @pytest.mark.parametrize(
        "query,expected",
        [
            ("SELECT SUM(amount) FROM orders", "sum"),
            ("SELECT AVG(price) FROM products", "avg"),
            ("SELECT MIN(created_at) FROM events", "min"),
            ("SELECT MAX(score) FROM results", "max"),
        ],
        ids=["sum", "avg", "min", "max"],
    )
    def test_other_aggregations(self, query: str, expected: str):
        assert SQLCheckReference.detect_aggregation_type(query, "duckdb") == expected

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT SUM(a), AVG(b) FROM t",
            "SELECT 42",
            "INSERT INTO t VALUES (1)",
        ],
        ids=["multiple_aggs", "literal", "non_select"],
    )
    def test_custom_fallback(self, query: str):
        assert SQLCheckReference.detect_aggregation_type(query, "duckdb") == "custom"

    def test_unparseable_query_returns_custom(self):
        assert SQLCheckReference.detect_aggregation_type("NOT VALID SQL !!!", "duckdb") == "custom"

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT * FROM t WHERE x > 5",
            "SELECT a FROM t",
            "SELECT col1, col2 FROM t WHERE col1 IS NOT NULL",
        ],
        ids=["select_star_where", "select_column", "select_columns_where"],
    )
    def test_none_for_plain_select(self, query: str):
        assert SQLCheckReference.detect_aggregation_type(query, "duckdb") == "none"


# ─── compute_failed_rows_count ───────────────────────────────────────


def _make_ref(query: str, unit: str | None = None) -> DummySQLCheckReference:
    """Helper to build a DummySQLCheckReference for compute_failed_rows_count tests."""
    check: dict = {"type": "sql", "query": query}
    if unit is not None:
        check["unit"] = unit
    return DummySQLCheckReference(check, query)


class TestComputeFailedRowsCount:
    """Unit tests for SQLCheckReference.compute_failed_rows_count."""

    def test_count_no_unit(self):
        ref = _make_ref("SELECT COUNT(*) FROM t")
        assert ref.compute_failed_rows_count(5) == 5

    def test_count_unit_rows(self):
        ref = _make_ref("SELECT COUNT(*) FROM t", unit="rows")
        assert ref.compute_failed_rows_count(5) == 5

    def test_count_unit_percent(self):
        ref = _make_ref("SELECT COUNT(*) FROM t", unit="percent")
        assert ref.compute_failed_rows_count(60.0) == 0

    def test_none_no_unit(self):
        ref = _make_ref("SELECT * FROM t WHERE bad = 1")
        assert ref.compute_failed_rows_count(3) == 3

    def test_avg_no_unit(self):
        ref = _make_ref("SELECT AVG(price) FROM t")
        assert ref.compute_failed_rows_count(42.5) == 0

    def test_sum_unit_rows(self):
        ref = _make_ref("SELECT SUM(amount) FROM t", unit="rows")
        assert ref.compute_failed_rows_count(100) == 0

    def test_custom_no_unit(self):
        ref = _make_ref("SELECT 42")
        assert ref.compute_failed_rows_count(99) == 0

    def test_none_value_returns_zero(self):
        ref = _make_ref("SELECT COUNT(*) FROM t")
        assert ref.compute_failed_rows_count(None) == 0

    def test_non_numeric_value_returns_zero(self):
        ref = _make_ref("SELECT COUNT(*) FROM t")
        assert ref.compute_failed_rows_count("abc") == 0


class DummySQLCheckReference(SQLCheckReference):
    def __init__(self, check: dict, rendered_query: str):
        super().__init__(contract=None, path="$.checks[0]")
        self._check = check
        self._rendered_query = rendered_query

    def get_check(self):
        return self._check

    def get_schema_name(self):
        return "users"

    def get_schema_path(self):
        return "$.schema[0]"

    def get_query(self, dialect: str, filter_conditions=None, use_try_cast: bool = False) -> str:
        return self._rendered_query


class TestSQLCheckReferenceProperties:
    def test_aggregation_type_property(self):
        ref = DummySQLCheckReference(
            {"type": "sql", "query": "SELECT AVG(price) FROM products"},
            "SELECT AVG(price) FROM products",
        )
        assert ref.aggregation_type == "avg"

    def test_unit_property(self):
        ref = DummySQLCheckReference(
            {"type": "sql", "unit": "percent", "query": "SELECT COUNT(*) FROM t"},
            "SELECT COUNT(*) FROM t",
        )
        assert ref.unit == "percent"

    def test_supports_row_level_output_true_for_none_rows(self):
        ref = DummySQLCheckReference(
            {"type": "sql", "query": "SELECT * FROM t WHERE bad = 1"},
            "SELECT * FROM t WHERE bad = 1",
        )
        assert ref.supports_row_level_output is True

    def test_supports_row_level_output_false_for_percent(self):
        ref = DummySQLCheckReference(
            {"type": "sql", "unit": "percent", "query": "SELECT COUNT(*) * 100.0 / 10 FROM t"},
            "SELECT COUNT(*) * 100.0 / 10 FROM t",
        )
        assert ref.supports_row_level_output is False

    def test_get_result_metadata_includes_aggregation_metadata(self):
        ref = DummySQLCheckReference(
            {"type": "sql", "unit": "rows", "query": "SELECT COUNT(*) FROM t"},
            "SELECT COUNT(*) FROM t",
        )
        metadata = ref.get_result_metadata()
        assert metadata["aggregation_type"] == "count"
        assert metadata["contract_definition"]["unit"] == "rows"


# ─── Stub infrastructure (reused from test_sql_executors_unit_coverage) ──


class StubFetchOneResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class StubRawSQLConnection:
    def __init__(self, handler):
        self._handler = handler
        self.queries: list[str] = []

    def raw_sql(self, query: str):
        self.queries.append(query)
        return self._handler(query)


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


class StubCheckReference:
    def __init__(
        self,
        *,
        check: dict | None = None,
        rendered_query: str = "SELECT COUNT(*) FROM users",
        failed_rows_query: str | None = "SELECT * FROM users",
        schema_name: str = "users",
        path: str = "$.checks[0]",
    ):
        self._check = check or {
            "name": "check_name",
            "type": "sql",
            "query": rendered_query,
            "mustBe": 0,
        }
        self._rendered_query = rendered_query
        self._failed_rows_query = failed_rows_query
        self._schema_name = schema_name
        self.path = path

    def get_check(self):
        return self._check

    def is_generated(self):
        return False

    def get_column_name(self):
        return None

    def get_logical_type(self):
        return None

    def get_schema_name(self):
        return self._schema_name

    def get_result_metadata(self):
        metadata = {
            "check_path": self.path,
            "check_ref_type": type(self).__name__,
            "schema_name": self._schema_name,
            "is_generated": False,
            "engine": "sql",
            "contract_definition": dict(self._check),
        }
        metadata["aggregation_type"] = self.aggregation_type
        return metadata

    def get_query(self, output_dialect, query_filters, use_try_cast=True):
        return self._rendered_query

    @property
    def unit(self):
        return self._check.get("unit")

    @property
    def aggregation_type(self):
        return SQLCheckReference.detect_aggregation_type(self._rendered_query, "duckdb")

    @property
    def supports_row_level_output(self):
        if self.unit is not None and self.unit != "rows":
            return False
        return self.aggregation_type in ("count", "none")

    def get_scalar_query(self, dialect, filter_conditions=None, use_try_cast=False):
        query = self._rendered_query
        if self.aggregation_type == "none":
            return f"SELECT COUNT(*) FROM ({query}) AS _sub"
        return query

    def compute_failed_rows_count(self, actual_value):
        unit_is_rows = self.unit is None or self.unit == "rows"
        if self.aggregation_type in ("count", "none") and unit_is_rows:
            try:
                return int(actual_value)
            except (TypeError, ValueError):
                return 0
        return 0

    def get_failed_rows_query(self, output_dialect, query_filters, use_try_cast=True):
        return self._failed_rows_query

    def get_check_name(self):
        check = self._check
        name = check.get("name") or check.get("id")
        if name:
            return name
        parts = []
        if self._schema_name:
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
            metadata["rendered_implementation"] = query
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


# ─── Executor: aggregation_type in metadata ─────────────────────────


class TestIbisExecutorAggregationType:
    """Verify IbisSQLExecutor records aggregation_type metadata."""

    def test_count_check_sets_aggregation_type_count(self, monkeypatch: pytest.MonkeyPatch):
        connection = StubRawSQLConnection(lambda q: StubFetchOneResult((0,)))
        executor = IbisSQLExecutor(StubIbisAdapter(connection))
        check_ref = StubCheckReference(rendered_query="SELECT COUNT(*) FROM users")

        result = executor.run_single_check(check_ref)

        assert result.status == "PASSED"
        assert result.metadata["aggregation_type"] == "count"

    def test_avg_check_sets_aggregation_type_avg(self, monkeypatch: pytest.MonkeyPatch):
        connection = StubRawSQLConnection(lambda q: StubFetchOneResult((500.0,)))
        executor = IbisSQLExecutor(StubIbisAdapter(connection))
        check_ref = StubCheckReference(
            check={
                "name": "avg_price",
                "type": "sql",
                "query": "SELECT AVG(price) FROM products",
                "mustBeLessThan": 1000,
            },
            rendered_query="SELECT AVG(price) FROM products",
            failed_rows_query=None,
        )

        result = executor.run_single_check(check_ref)

        assert result.status == "PASSED"
        assert result.metadata["aggregation_type"] == "avg"

    def test_failed_avg_check_has_zero_failed_rows_count(self, monkeypatch: pytest.MonkeyPatch):
        connection = StubRawSQLConnection(lambda q: StubFetchOneResult((1500.0,)))
        executor = IbisSQLExecutor(StubIbisAdapter(connection))
        check_ref = StubCheckReference(
            check={
                "name": "avg_price",
                "type": "sql",
                "query": "SELECT AVG(price) FROM products",
                "mustBeLessThan": 1000,
            },
            rendered_query="SELECT AVG(price) FROM products",
            failed_rows_query=None,
        )

        result = executor.run_single_check(check_ref)

        assert result.status == "FAILED"
        assert result.metadata["aggregation_type"] == "avg"
        assert result.failed_rows_count == 0
        assert result.actual_value == 1500.0

    def test_failed_count_check_has_nonzero_failed_rows_count(self, monkeypatch: pytest.MonkeyPatch):
        connection = StubRawSQLConnection(lambda q: StubFetchOneResult((5,)))
        executor = IbisSQLExecutor(StubIbisAdapter(connection))
        check_ref = StubCheckReference(rendered_query="SELECT COUNT(*) FROM users")

        result = executor.run_single_check(check_ref)

        assert result.status == "FAILED"
        assert result.metadata["aggregation_type"] == "count"
        assert result.failed_rows_count == 5


# ─── Row-quality summaries exclude non-count checks ─────────────────


class TestRowQualityExcludesNonCount:
    """Verify non-COUNT checks don't corrupt row-quality summaries."""

    def test_supports_row_level_output_true_for_count(self):
        from vowl.validation.result import ValidationResult

        cr = CheckResult("check1", "FAILED", "d", supports_row_level_output=True)
        assert ValidationResult._supports_row_level_output(cr) is True

    def test_supports_row_level_output_true_for_none(self):
        from vowl.validation.result import ValidationResult

        cr = CheckResult("check1", "FAILED", "d", supports_row_level_output=True)
        assert ValidationResult._supports_row_level_output(cr) is True

    def test_supports_row_level_output_defaults_false(self):
        from vowl.validation.result import ValidationResult

        cr = CheckResult("check1", "FAILED", "d", metadata={})
        assert ValidationResult._supports_row_level_output(cr) is False

    def test_supports_row_level_output_false_for_avg(self):
        from vowl.validation.result import ValidationResult

        cr = CheckResult("check1", "FAILED", "d", supports_row_level_output=False)
        assert ValidationResult._supports_row_level_output(cr) is False

    def test_supports_row_level_output_false_for_sum(self):
        from vowl.validation.result import ValidationResult

        cr = CheckResult("check1", "FAILED", "d", supports_row_level_output=False)
        assert ValidationResult._supports_row_level_output(cr) is False

    def test_supports_row_level_output_false_for_percent(self):
        from vowl.validation.result import ValidationResult

        cr = CheckResult("check1", "FAILED", "d", supports_row_level_output=False)
        assert ValidationResult._supports_row_level_output(cr) is False

    def test_supports_row_level_output_true_for_rows(self):
        from vowl.validation.result import ValidationResult

        cr = CheckResult("check1", "FAILED", "d", supports_row_level_output=True)
        assert ValidationResult._supports_row_level_output(cr) is True


# ─── Phase 2: plain SELECT handling & unit field ─────────────────────


class TestGetFailedRowsQueryPlainSelect:
    """get_failed_rows_query returns the query itself for plain SELECTs."""

    def test_plain_select_has_no_aggregates(self):
        """Sanity: confirm plain SELECT is detected as having no aggregates."""
        import sqlglot
        from sqlglot import exp as sqlexp
        query = "SELECT * FROM users WHERE active = 0"
        parsed = sqlglot.parse_one(query, dialect="duckdb")
        has_agg = any(
            isinstance(node, (sqlexp.Count, sqlexp.Sum, sqlexp.Avg, sqlexp.Min, sqlexp.Max))
            for sel in parsed.expressions
            for node in sel.walk()
        )
        assert not has_agg
        assert parsed.find(sqlexp.From) is not None


class TestGetFailedRowsQueryIntegration:
    """Integration test: SQLCheckReference.get_failed_rows_query with real parsing."""

    @pytest.fixture()
    def _make_ref(self):
        """Create a minimal SQLCheckReference-like object for testing get_failed_rows_query."""

        class FakeRef:
            def get_query(self, dialect, filter_conditions, use_try_cast=False):
                return self._query

        def _factory(query: str):
            ref = FakeRef()
            ref._query = query
            return ref

        return _factory

    def test_plain_select_returns_self(self, _make_ref):
        query = "SELECT * FROM users WHERE active = 0"
        ref = _make_ref(query)
        result = SQLCheckReference.get_failed_rows_query(ref, "duckdb", None, use_try_cast=False)
        assert result == query

    def test_select_columns_returns_self(self, _make_ref):
        query = "SELECT col1, col2 FROM t WHERE col1 > 10"
        ref = _make_ref(query)
        result = SQLCheckReference.get_failed_rows_query(ref, "duckdb", None, use_try_cast=False)
        assert result == query

    def test_count_returns_star(self, _make_ref):
        ref = _make_ref("SELECT COUNT(*) FROM users WHERE active = 0")
        result = SQLCheckReference.get_failed_rows_query(ref, "duckdb", None, use_try_cast=False)
        assert result == "SELECT * FROM users WHERE active = 0"

    def test_avg_returns_none(self, _make_ref):
        ref = _make_ref("SELECT AVG(price) FROM products")
        result = SQLCheckReference.get_failed_rows_query(ref, "duckdb", None, use_try_cast=False)
        assert result is None

    def test_sum_returns_none(self, _make_ref):
        ref = _make_ref("SELECT SUM(amount) FROM orders")
        result = SQLCheckReference.get_failed_rows_query(ref, "duckdb", None, use_try_cast=False)
        assert result is None


class TestPlainSelectExecutor:
    """Verify executor wraps plain SELECT in COUNT and stores unit."""

    def test_plain_select_wraps_in_count(self):
        queries_executed = []

        def handler(q):
            queries_executed.append(q)
            return StubFetchOneResult((3,))

        connection = StubRawSQLConnection(handler)
        executor = IbisSQLExecutor(StubIbisAdapter(connection))
        check_ref = StubCheckReference(
            check={
                "name": "expensive_flats",
                "type": "sql",
                "query": "SELECT * FROM t WHERE price > 2000000",
                "unit": "rows",
                "mustBe": 0,
            },
            rendered_query="SELECT * FROM t WHERE price > 2000000",
            failed_rows_query="SELECT * FROM t WHERE price > 2000000",
        )

        result = executor.run_single_check(check_ref)

        assert result.status == "FAILED"
        assert result.metadata["aggregation_type"] == "none"
        assert result.metadata["contract_definition"]["unit"] == "rows"
        assert result.actual_value == 3
        assert result.failed_rows_count == 3
        # Should have executed a COUNT wrapper, not the raw query
        assert any("COUNT(*)" in q for q in queries_executed)

    def test_plain_select_passed(self):
        connection = StubRawSQLConnection(lambda q: StubFetchOneResult((0,)))
        executor = IbisSQLExecutor(StubIbisAdapter(connection))
        check_ref = StubCheckReference(
            check={
                "name": "no_bad_rows",
                "type": "sql",
                "query": "SELECT * FROM t WHERE invalid = 1",
                "mustBe": 0,
            },
            rendered_query="SELECT * FROM t WHERE invalid = 1",
        )

        result = executor.run_single_check(check_ref)

        assert result.status == "PASSED"
        assert result.metadata["aggregation_type"] == "none"
        assert result.actual_value == 0

    def test_unit_stored_in_metadata(self):
        connection = StubRawSQLConnection(lambda q: StubFetchOneResult((0,)))
        executor = IbisSQLExecutor(StubIbisAdapter(connection))
        check_ref = StubCheckReference(
            check={
                "name": "check",
                "type": "sql",
                "query": "SELECT COUNT(*) FROM t",
                "unit": "percent",
                "mustBe": 0,
            },
            rendered_query="SELECT COUNT(*) FROM t",
        )

        result = executor.run_single_check(check_ref)
        assert result.metadata["contract_definition"]["unit"] == "percent"

    def test_unit_absent_when_not_specified(self):
        connection = StubRawSQLConnection(lambda q: StubFetchOneResult((0,)))
        executor = IbisSQLExecutor(StubIbisAdapter(connection))
        check_ref = StubCheckReference(rendered_query="SELECT COUNT(*) FROM t")

        result = executor.run_single_check(check_ref)
        assert "unit" not in result.metadata.get("contract_definition", {})

    def test_percent_count_query_has_zero_failed_rows_count(self):
        """COUNT query with unit=percent should not be treated as a row count."""
        connection = StubRawSQLConnection(lambda q: StubFetchOneResult((60.0,)))
        executor = IbisSQLExecutor(StubIbisAdapter(connection))
        check_ref = StubCheckReference(
            check={
                "name": "pct_check",
                "type": "sql",
                "query": "SELECT COUNT(*) * 100.0 / 500 FROM t WHERE x > 0",
                "unit": "percent",
                "mustBeLessThan": 50,
            },
            rendered_query="SELECT COUNT(*) * 100.0 / 500 FROM t WHERE x > 0",
            failed_rows_query=None,
        )

        result = executor.run_single_check(check_ref)

        assert result.status == "FAILED"
        assert result.metadata["contract_definition"]["unit"] == "percent"
        # Should NOT interpret 60.0 as 60 failed rows
        assert result.failed_rows_count == 0
        assert result.actual_value == 60.0

    def test_unit_rows_still_counts(self):
        """COUNT query with unit=rows should still set failed_rows_count."""
        connection = StubRawSQLConnection(lambda q: StubFetchOneResult((5,)))
        executor = IbisSQLExecutor(StubIbisAdapter(connection))
        check_ref = StubCheckReference(
            check={
                "name": "row_check",
                "type": "sql",
                "query": "SELECT COUNT(*) FROM t WHERE bad = 1",
                "unit": "rows",
                "mustBe": 0,
            },
            rendered_query="SELECT COUNT(*) FROM t WHERE bad = 1",
        )

        result = executor.run_single_check(check_ref)

        assert result.status == "FAILED"
        assert result.failed_rows_count == 5
