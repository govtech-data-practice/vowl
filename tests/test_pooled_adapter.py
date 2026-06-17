"""Tests for PooledAdapter."""

from __future__ import annotations

import threading
import time

import pyarrow as pa
import pytest

from vowl.adapters.base import BaseAdapter
from vowl.adapters.pooled_adapter import PooledAdapter
from vowl.executors.base import CheckResult


class StubCheckRef:
    """Minimal check reference for testing."""

    def __init__(self, name: str = "check_1", engine: str = "sql"):
        self._name = name
        self._engine = engine

    def get_check_name(self) -> str:
        return self._name

    def get_execution_engine(self) -> str:
        return self._engine

    def get_check(self) -> dict:
        return {"name": self._name, "type": "sql", "query": "SELECT 1"}

    def get_result_metadata(self):
        return {}


class CountingAdapter(BaseAdapter):
    """Adapter that tracks creation and execution for testing."""

    _instance_counter = 0
    _counter_lock = threading.Lock()

    def __init__(self, delay: float = 0.0):
        from vowl.executors.base import BaseExecutor

        super().__init__()
        with CountingAdapter._counter_lock:
            CountingAdapter._instance_counter += 1
            self.instance_id = CountingAdapter._instance_counter
        self._delay = delay
        self.checks_run: list[str] = []
        self.cleaned_up = False

    def run_checks(self, check_refs):
        if self._delay > 0:
            time.sleep(self._delay)
        results = []
        for ref in check_refs:
            self.checks_run.append(ref.get_check_name())
            results.append(
                CheckResult(
                    check_name=ref.get_check_name(),
                    status="PASSED",
                    details=f"adapter_{self.instance_id}",
                    execution_time_ms=10,
                )
            )
        return results

    def test_connection(self, table_name: str) -> str | None:
        return None

    def get_total_rows(self, schema_name: str, max_rows: int = -1) -> int:
        return 100

    def export_table_as_arrow(self, schema_name: str) -> pa.Table:
        return pa.table({"id": [1, 2, 3]})

    def cleanup(self) -> None:
        self.cleaned_up = True


class ErrorAdapter(BaseAdapter):
    """Adapter that raises on specific check names."""

    def __init__(self, fail_on: str = "fail_check"):
        super().__init__()
        self._fail_on = fail_on

    def run_checks(self, check_refs):
        results = []
        for ref in check_refs:
            if ref.get_check_name() == self._fail_on:
                raise RuntimeError(f"Simulated failure on {self._fail_on}")
            results.append(
                CheckResult(
                    check_name=ref.get_check_name(),
                    status="PASSED",
                    details="ok",
                    execution_time_ms=5,
                )
            )
        return results

    def cleanup(self) -> None:
        pass


@pytest.fixture(autouse=True)
def reset_counter():
    CountingAdapter._instance_counter = 0
    yield


class TestSequentialFallback:
    def test_max_concurrency_1_uses_single_adapter(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=1)
        refs = [StubCheckRef(f"check_{i}") for i in range(5)]

        results = pooled.run_checks(refs)

        assert len(results) == 5
        assert len(pooled._all_instances) == 1

    def test_single_check_skips_thread_pool(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=4)
        refs = [StubCheckRef("only_check")]

        results = pooled.run_checks(refs)

        assert len(results) == 1
        assert results[0].check_name == "only_check"
        assert len(pooled._all_instances) == 1

    def test_empty_check_list_returns_empty(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=4)

        results = pooled.run_checks([])

        assert results == []
        assert len(pooled._all_instances) == 0


class TestParallelExecution:
    def test_parallel_creates_up_to_max_concurrency(self):
        pooled = PooledAdapter(
            factory=lambda: CountingAdapter(delay=0.02),
            max_concurrency=3,
        )
        refs = [StubCheckRef(f"check_{i}") for i in range(10)]

        results = pooled.run_checks(refs)

        assert len(results) == 10
        assert len(pooled._all_instances) <= 3


class TestOrderPreservation:
    def test_results_match_input_order(self):
        pooled = PooledAdapter(
            factory=lambda: CountingAdapter(delay=0.01),
            max_concurrency=4,
        )
        refs = [StubCheckRef(f"check_{i}") for i in range(10)]

        results = pooled.run_checks(refs)

        assert [r.check_name for r in results] == [f"check_{i}" for i in range(10)]


class TestPoolExhaustion:
    def test_only_creates_max_concurrency_adapters(self):
        pooled = PooledAdapter(
            factory=lambda: CountingAdapter(delay=0.02),
            max_concurrency=2,
        )
        refs = [StubCheckRef(f"check_{i}") for i in range(5)]

        results = pooled.run_checks(refs)

        assert len(results) == 5
        assert len(pooled._all_instances) == 2


class TestErrorIsolation:
    def test_error_in_one_check_does_not_break_others(self):
        class SelectiveErrorAdapter(BaseAdapter):
            def __init__(self):
                super().__init__()

            def run_checks(self, check_refs):
                results = []
                for ref in check_refs:
                    if ref.get_check_name() == "check_3":
                        raise RuntimeError("boom")
                    results.append(
                        CheckResult(
                            check_name=ref.get_check_name(),
                            status="PASSED",
                            details="ok",
                            execution_time_ms=5,
                        )
                    )
                return results

            def cleanup(self):
                pass

        pooled = PooledAdapter(factory=SelectiveErrorAdapter, max_concurrency=3)
        refs = [StubCheckRef(f"check_{i}") for i in range(5)]

        with pytest.raises(RuntimeError, match="boom"):
            pooled.run_checks(refs)


class TestConfigPropagation:
    def test_propagates_to_existing_instances(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=2)
        # Force adapter creation
        adapter = pooled._checkout()
        pooled._return(adapter)

        pooled.max_failed_rows = 50
        pooled.use_try_cast = False

        assert pooled._all_instances[0].max_failed_rows == 50
        assert pooled._all_instances[0].use_try_cast is False

    def test_propagates_to_future_instances(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=2)
        pooled.max_failed_rows = 25
        pooled.use_try_cast = False

        adapter = pooled._checkout()
        pooled._return(adapter)

        assert adapter.max_failed_rows == 25
        assert adapter.use_try_cast is False


class TestCleanup:
    def test_cleanup_all_instances(self):
        pooled = PooledAdapter(
            factory=lambda: CountingAdapter(delay=0.01),
            max_concurrency=3,
        )
        refs = [StubCheckRef(f"check_{i}") for i in range(6)]
        pooled.run_checks(refs)

        instances = list(pooled._all_instances)
        assert len(instances) > 0

        pooled.cleanup()

        assert all(a.cleaned_up for a in instances)
        assert pooled._all_instances == []
        assert pooled._created_count == 0
        assert pooled._primary is None
        assert pooled._pool.empty()


class TestDelegation:
    def test_test_connection_delegates_to_primary(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=2)

        result = pooled.test_connection("my_table")

        assert result is None
        assert len(pooled._all_instances) == 1

    def test_get_total_rows_delegates_to_primary(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=2)

        result = pooled.get_total_rows("my_table")

        assert result == 100

    def test_export_table_as_arrow_delegates_to_primary(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=2)

        result = pooled.export_table_as_arrow("my_table")

        assert result.num_rows == 3

    def test_primary_adapter_lazily_created(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=4)

        assert pooled._primary is None
        assert len(pooled._all_instances) == 0

        _ = pooled._primary_adapter

        assert pooled._primary is not None
        assert len(pooled._all_instances) == 1

    def test_primary_reuses_existing_instance(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=4)
        adapter = pooled._checkout()
        pooled._return(adapter)

        primary = pooled._primary_adapter

        assert primary is adapter

    def test_get_sql_dialect_raises_if_not_available(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=2)

        with pytest.raises(AttributeError, match="has no get_sql_dialect"):
            pooled.get_sql_dialect()

    def test_is_compatible_with_delegates(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=2)
        other = CountingAdapter()

        assert pooled.is_compatible_with(other) is False


class TestMaxConcurrencyProperty:
    def test_exposes_max_concurrency(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=8)
        assert pooled.max_concurrency == 8

    def test_clamps_to_minimum_1(self):
        pooled = PooledAdapter(factory=CountingAdapter, max_concurrency=0)
        assert pooled.max_concurrency == 1

        pooled2 = PooledAdapter(factory=CountingAdapter, max_concurrency=-5)
        assert pooled2.max_concurrency == 1
