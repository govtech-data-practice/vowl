"""Tests for parallel Mode 2 execution in MultiSourceSQLExecutor."""

from __future__ import annotations

import time

import pyarrow as pa

from vowl.adapters.base import BaseAdapter
from vowl.adapters.pooled_adapter import PooledAdapter
from vowl.executors.multi_source_sql_executor import MultiSourceSQLExecutor


class StubSQLCheckRef:
    """Minimal SQL check reference for multi-source testing."""

    def __init__(self, name: str, query: str):
        self._name = name
        self._query = query

    def get_check_name(self) -> str:
        return self._name

    def get_check(self) -> dict:
        return {"name": self._name, "type": "sql", "query": self._query}

    def get_execution_engine(self) -> str:
        return "sql"

    def get_scalar_query(self, dialect, filters, **kwargs) -> str:
        return self._query

    def get_failed_rows_query(self, dialect, filters, **kwargs) -> str | None:
        return None

    def get_result_metadata(self):
        return {}

    def build_result(self, actual_value, execution_time_ms, **kwargs):
        from vowl.executors.base import CheckResult

        return CheckResult(
            check_name=self._name,
            status="PASSED",
            details=str(actual_value),
            execution_time_ms=execution_time_ms,
        )

    def build_error_result(self, error_message, execution_time_ms, **kwargs):
        from vowl.executors.base import CheckResult

        return CheckResult(
            check_name=self._name,
            status="ERROR",
            details=error_message,
            execution_time_ms=execution_time_ms,
        )


class SlowExportAdapter(BaseAdapter):
    """Adapter with configurable export delay for timing tests."""

    def __init__(self, data: dict[str, pa.Table] | None = None, delay: float = 0.0):
        super().__init__()
        self._data = data or {}
        self._delay = delay

    def export_table_as_arrow(self, schema_name: str) -> pa.Table:
        if self._delay > 0:
            time.sleep(self._delay)
        if schema_name in self._data:
            return self._data[schema_name]
        return pa.table({"id": [1, 2, 3]})

    def test_connection(self, table_name: str) -> str | None:
        return None

    def cleanup(self) -> None:
        pass


class StubMultiAdapter:
    """Minimal MultiSourceAdapter mock for executor testing."""

    def __init__(self, adapters: dict[str, BaseAdapter]):
        self._adapters = adapters
        self.max_failed_rows = -1
        self.use_try_cast = True

    def get_adapter(self, schema_name: str) -> BaseAdapter | None:
        return self._adapters.get(schema_name)


class TestDeriveConcurrency:
    def test_returns_1_when_non_pooled_adapter_present(self):
        adapters = {
            "table_a": PooledAdapter(factory=SlowExportAdapter, max_concurrency=4),
            "table_b": SlowExportAdapter(),
        }
        multi = StubMultiAdapter(adapters)
        executor = MultiSourceSQLExecutor(multi)

        ref = StubSQLCheckRef("check", "SELECT COUNT(*) FROM table_a JOIN table_b ON 1=1")
        result = executor._derive_concurrency([ref])

        assert result == 1

    def test_returns_min_of_pooled_adapters(self):
        adapters = {
            "table_a": PooledAdapter(factory=SlowExportAdapter, max_concurrency=4),
            "table_b": PooledAdapter(factory=SlowExportAdapter, max_concurrency=2),
        }
        multi = StubMultiAdapter(adapters)
        executor = MultiSourceSQLExecutor(multi)

        ref = StubSQLCheckRef("check", "SELECT COUNT(*) FROM table_a JOIN table_b ON 1=1")
        result = executor._derive_concurrency([ref])

        assert result == 2

    def test_returns_1_when_adapter_not_found(self):
        adapters = {
            "table_a": PooledAdapter(factory=SlowExportAdapter, max_concurrency=4),
        }
        multi = StubMultiAdapter(adapters)
        executor = MultiSourceSQLExecutor(multi)

        ref = StubSQLCheckRef("check", "SELECT COUNT(*) FROM table_a JOIN unknown ON 1=1")
        result = executor._derive_concurrency([ref])

        assert result == 1

    def test_returns_1_when_no_tables_detected(self):
        multi = StubMultiAdapter({})
        executor = MultiSourceSQLExecutor(multi)

        ref = StubSQLCheckRef("check", "")
        result = executor._derive_concurrency([ref])

        assert result == 1

    def test_returns_single_adapter_concurrency(self):
        adapters = {
            "table_a": PooledAdapter(factory=SlowExportAdapter, max_concurrency=6),
            "table_b": PooledAdapter(factory=SlowExportAdapter, max_concurrency=6),
        }
        multi = StubMultiAdapter(adapters)
        executor = MultiSourceSQLExecutor(multi)

        ref = StubSQLCheckRef("check", "SELECT COUNT(*) FROM table_a JOIN table_b ON 1=1")
        result = executor._derive_concurrency([ref])

        assert result == 6


class TestParallelMaterialization:
    def test_parallel_materialization_produces_correct_results(self):
        data_a = pa.table({"id": [1, 2], "val": [10, 20]})
        data_b = pa.table({"id": [1, 2], "ref": ["a", "b"]})

        adapters = {
            "table_a": PooledAdapter(
                factory=lambda: SlowExportAdapter({"table_a": data_a}),
                max_concurrency=2,
            ),
            "table_b": PooledAdapter(
                factory=lambda: SlowExportAdapter({"table_b": data_b}),
                max_concurrency=2,
            ),
        }
        multi = StubMultiAdapter(adapters)
        executor = MultiSourceSQLExecutor(multi)

        refs = [
            StubSQLCheckRef("check_1", "SELECT COUNT(*) FROM table_a JOIN table_b ON table_a.id = table_b.id"),
            StubSQLCheckRef("check_2", "SELECT COUNT(*) FROM table_a JOIN table_b ON table_a.id = table_b.id"),
        ]

        results = executor.run_batch_checks(refs)

        assert len(results) == 2
        assert all(r.status == "PASSED" for r in results)
        assert all(r.details == "2" for r in results)


class TestMaterializationErrors:
    def test_materialization_error_propagates_to_check(self):
        class FailingExportAdapter(BaseAdapter):
            def export_table_as_arrow(self, schema_name: str):
                raise RuntimeError("Connection refused")

            def test_connection(self, table_name):
                return None

            def cleanup(self):
                pass

        adapters = {
            "table_a": PooledAdapter(
                factory=lambda: SlowExportAdapter({"table_a": pa.table({"id": [1]})}),
                max_concurrency=2,
            ),
            "table_b": PooledAdapter(
                factory=FailingExportAdapter,
                max_concurrency=2,
            ),
        }
        multi = StubMultiAdapter(adapters)
        executor = MultiSourceSQLExecutor(multi)

        # Need 2+ checks to trigger parallel path
        refs = [
            StubSQLCheckRef("check_1", "SELECT COUNT(*) FROM table_a JOIN table_b ON 1=1"),
            StubSQLCheckRef("check_2", "SELECT COUNT(*) FROM table_a JOIN table_b ON 1=1"),
        ]
        results = executor.run_batch_checks(refs)

        assert len(results) == 2
        assert all(r.status == "ERROR" for r in results)
        assert "Connection refused" in results[0].details


class TestParallelQueryExecution:
    def test_multiple_checks_run_on_independent_duckdb_instances(self):
        data_a = pa.table({"id": [1, 2, 3], "val": [10, 20, 30]})
        data_b = pa.table({"id": [1, 2, 3], "ref": ["x", "y", "z"]})

        adapters = {
            "table_a": PooledAdapter(
                factory=lambda: SlowExportAdapter({"table_a": data_a}),
                max_concurrency=3,
            ),
            "table_b": PooledAdapter(
                factory=lambda: SlowExportAdapter({"table_b": data_b}),
                max_concurrency=3,
            ),
        }
        multi = StubMultiAdapter(adapters)
        executor = MultiSourceSQLExecutor(multi)

        refs = [
            StubSQLCheckRef(
                f"check_{i}",
                "SELECT COUNT(*) FROM table_a JOIN table_b ON table_a.id = table_b.id",
            )
            for i in range(4)
        ]

        results = executor.run_batch_checks(refs)

        assert len(results) == 4
        assert all(r.status == "PASSED" for r in results)
        # Each check runs on its own DuckDB instance with the same data
        assert all(r.details == "3" for r in results)

    def test_result_ordering_preserved(self):
        data = pa.table({"id": [1, 2, 3]})

        adapters = {
            "t1": PooledAdapter(
                factory=lambda: SlowExportAdapter({"t1": data, "t2": data}),
                max_concurrency=3,
            ),
            "t2": PooledAdapter(
                factory=lambda: SlowExportAdapter({"t1": data, "t2": data}),
                max_concurrency=3,
            ),
        }
        multi = StubMultiAdapter(adapters)
        executor = MultiSourceSQLExecutor(multi)

        refs = [StubSQLCheckRef(f"check_{i}", "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.id = t2.id") for i in range(5)]

        results = executor.run_batch_checks(refs)

        assert [r.check_name for r in results] == [f"check_{i}" for i in range(5)]


class TestSequentialFallback:
    def test_falls_back_when_single_check(self):
        data = pa.table({"id": [1]})
        adapters = {
            "t1": PooledAdapter(
                factory=lambda: SlowExportAdapter({"t1": data, "t2": data}),
                max_concurrency=4,
            ),
            "t2": PooledAdapter(
                factory=lambda: SlowExportAdapter({"t1": data, "t2": data}),
                max_concurrency=4,
            ),
        }
        multi = StubMultiAdapter(adapters)
        executor = MultiSourceSQLExecutor(multi)

        ref = StubSQLCheckRef("only", "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.id = t2.id")
        results = executor.run_batch_checks([ref])

        assert len(results) == 1
        assert results[0].check_name == "only"
