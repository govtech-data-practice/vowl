"""
Pooled Adapter for parallel data quality check execution.

Wraps any BaseAdapter factory with a thread-safe connection pool, dispatching
checks across multiple adapter instances using concurrent.futures.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from vowl.adapters.base import BaseAdapter

if TYPE_CHECKING:
    from vowl.contracts.check_reference import CheckReference
    from vowl.executors.base import BaseExecutor, CheckResult


class PooledAdapter(BaseAdapter):
    """
    Adapter that pools multiple adapter instances for parallel check execution.

    Wraps any BaseAdapter factory with a queue-based connection pool.
    Each pooled adapter instance is used by at most one thread at a time.

    Example:
        >>> from vowl.adapters import PooledAdapter, IbisAdapter
        >>> import ibis
        >>>
        >>> adapter = PooledAdapter(
        ...     factory=lambda: IbisAdapter(con=ibis.duckdb.connect("mydb.db")),
        ...     max_concurrency=4,
        ... )
        >>> results = adapter.run_checks(check_refs)
    """

    def __init__(
        self,
        factory: Callable[[], BaseAdapter],
        max_concurrency: int = 4,
        executors: dict[str, type[BaseExecutor]] | None = None,
    ) -> None:
        super().__init__(executors=executors)
        self._factory = factory
        self._max_concurrency = max(1, max_concurrency)
        self._pool: queue.Queue[BaseAdapter] = queue.Queue()
        self._all_instances: list[BaseAdapter] = []
        self._lock = threading.Lock()
        self._created_count = 0
        self._primary: BaseAdapter | None = None

    @property
    def max_concurrency(self) -> int:
        return self._max_concurrency

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if name in ("max_failed_rows", "use_try_cast") and hasattr(self, "_all_instances"):
            for adapter in self._all_instances:
                setattr(adapter, name, value)

    def _create_adapter(self) -> BaseAdapter:
        adapter = self._factory()
        adapter.max_failed_rows = self.max_failed_rows
        adapter.use_try_cast = self.use_try_cast
        self._all_instances.append(adapter)
        self._created_count += 1
        return adapter

    def _checkout(self) -> BaseAdapter:
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            with self._lock:
                if self._created_count < self._max_concurrency:
                    return self._create_adapter()
            return self._pool.get()

    def _return(self, adapter: BaseAdapter) -> None:
        self._pool.put(adapter)

    @property
    def _primary_adapter(self) -> BaseAdapter:
        if self._primary is None:
            with self._lock:
                if self._primary is None:
                    if self._all_instances:
                        self._primary = self._all_instances[0]
                    else:
                        adapter = self._create_adapter()
                        self._pool.put(adapter)
                        self._primary = adapter
        return self._primary

    def run_checks(
        self,
        check_refs: list[CheckReference],
    ) -> list[CheckResult]:
        if not check_refs:
            return []

        if self._max_concurrency <= 1 or len(check_refs) <= 1:
            adapter = self._checkout()
            try:
                return adapter.run_checks(check_refs)
            finally:
                self._return(adapter)

        results: list[list[CheckResult]] = [[] for _ in check_refs]

        def run_single(index: int, ref: CheckReference) -> None:
            adapter = self._checkout()
            try:
                results[index] = adapter.run_checks([ref])
            finally:
                self._return(adapter)

        with ThreadPoolExecutor(max_workers=self._max_concurrency) as pool:
            futures = [
                pool.submit(run_single, i, ref)
                for i, ref in enumerate(check_refs)
            ]
            for future in futures:
                future.result()

        return [r for batch in results for r in batch]

    def test_connection(self, table_name: str) -> str | None:
        return self._primary_adapter.test_connection(table_name)

    def get_total_rows(self, schema_name: str, max_rows: int = -1) -> int:
        return self._primary_adapter.get_total_rows(schema_name, max_rows)

    def export_table_as_arrow(self, schema_name: str) -> pa.Table:
        adapter = self._checkout()
        try:
            return adapter.export_table_as_arrow(schema_name)
        finally:
            self._return(adapter)

    def is_compatible_with(self, other: BaseAdapter) -> bool:
        return self._primary_adapter.is_compatible_with(other)

    def get_sql_dialect(self) -> str:
        primary = self._primary_adapter
        if hasattr(primary, "get_sql_dialect"):
            return primary.get_sql_dialect()
        raise AttributeError(
            f"{type(primary).__name__} has no get_sql_dialect method"
        )

    def get_connection(self):
        primary = self._primary_adapter
        if hasattr(primary, "get_connection"):
            return primary.get_connection()
        raise AttributeError(
            f"{type(primary).__name__} has no get_connection method"
        )

    def cleanup(self) -> None:
        for adapter in self._all_instances:
            if hasattr(adapter, "cleanup"):
                adapter.cleanup()
        self._all_instances.clear()
        self._created_count = 0
        self._primary = None
        while not self._pool.empty():
            try:
                self._pool.get_nowait()
            except queue.Empty:
                break
