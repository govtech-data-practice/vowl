"""
Pooled Adapter for parallel data quality check execution.

Wraps any BaseAdapter factory with a thread-safe connection pool, dispatching
checks across multiple adapter instances using concurrent.futures.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
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

    Shallow copies (``copy.copy``) share the pool, its instances and its
    lock, so ``max_concurrency`` caps the connections of every copy
    together. ``MultiSourceAdapter`` makes such copies when one pool serves
    several schemas.

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

    @property
    def max_concurrency(self) -> int:
        return self._max_concurrency

    @property
    def _created_count(self) -> int:
        # Derived from the shared instance list so every copy sees one count
        return len(self._all_instances)

    @property
    def _primary(self) -> BaseAdapter | None:
        return self._all_instances[0] if self._all_instances else None

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

    @contextmanager
    def _lease(self) -> Iterator[BaseAdapter]:
        """Check out one pooled instance for the duration of a block."""
        adapter = self._checkout()
        try:
            yield adapter
        finally:
            self._return(adapter)

    @property
    def _primary_adapter(self) -> BaseAdapter:
        primary = self._primary
        if primary is None:
            with self._lock:
                primary = self._primary
                if primary is None:
                    primary = self._create_adapter()
                    self._pool.put(primary)
        return primary

    def run_checks(
        self,
        check_refs: list[CheckReference],
    ) -> list[CheckResult]:
        return self._run_check_batches([check_refs])[0]

    def _run_check_batches(
        self,
        batches: list[list[CheckReference]],
    ) -> list[list[CheckResult]]:
        """Run several lists of checks through the pool at once.

        ``MultiSourceAdapter`` uses this when one pool serves several
        schemas, so their checks share the pool's workers instead of running
        one schema at a time.

        Args:
            batches: One list of checks per caller, for example per schema.

        Returns:
            One list of results per batch, the same list ``run_checks``
            returns for that batch on its own.
        """
        jobs = [(b, i) for b, batch in enumerate(batches) for i in range(len(batch))]
        if self._max_concurrency <= 1 or len(jobs) <= 1:
            results: list[list[CheckResult]] = []
            for batch in batches:
                if not batch:
                    results.append([])
                    continue
                with self._lease() as adapter:
                    results.append(adapter.run_checks(batch))
            return results

        per_ref: list[list[list[CheckResult]]] = [[[] for _ in batch] for batch in batches]

        def run_single(batch_index: int, ref_index: int) -> None:
            with self._lease() as adapter:
                per_ref[batch_index][ref_index] = adapter.run_checks([batches[batch_index][ref_index]])

        with ThreadPoolExecutor(max_workers=self._max_concurrency) as pool:
            futures = [pool.submit(run_single, b, i) for b, i in jobs]
            for future in futures:
                future.result()

        return [[r for ref_results in batch for r in ref_results] for batch in per_ref]

    def test_connection(self, table_name: str) -> str | None:
        return self._primary_adapter.test_connection(table_name)

    def get_total_rows(self, schema_name: str, max_rows: int = -1) -> int:
        return self._primary_adapter.get_total_rows(schema_name, max_rows)

    def export_table_as_arrow(self, schema_name: str) -> pa.Table:
        with self._lease() as adapter:
            return adapter.export_table_as_arrow(schema_name)

    def is_compatible_with(self, other: BaseAdapter) -> bool:
        """Copies of one pool are compatible with each other.

        Their instances all come from the same factory, so a join between
        tables they serve can run on any one leased instance. Separate pools
        and other adapters are not compatible, because the instances they
        hold are separate connections.
        """
        return isinstance(other, PooledAdapter) and other._pool is self._pool

    def get_sql_dialect(self) -> str:
        primary = self._primary_adapter
        if hasattr(primary, "get_sql_dialect"):
            return primary.get_sql_dialect()
        raise AttributeError(f"{type(primary).__name__} has no get_sql_dialect method")

    def get_connection(self):
        primary = self._primary_adapter
        if hasattr(primary, "get_connection"):
            return primary.get_connection()
        raise AttributeError(f"{type(primary).__name__} has no get_connection method")

    def cleanup(self) -> None:
        for adapter in self._all_instances:
            if hasattr(adapter, "cleanup"):
                adapter.cleanup()
        self._all_instances.clear()
        while not self._pool.empty():
            try:
                self._pool.get_nowait()
            except queue.Empty:
                break
