from __future__ import annotations

from abc import ABC
from collections import defaultdict
import warnings
from typing import TYPE_CHECKING, Dict, List, Optional, Type

import pyarrow as pa

if TYPE_CHECKING:
    from vowl.contracts.check_reference import CheckReference
    from vowl.executors.base import BaseExecutor, CheckResult


class BaseAdapter(ABC):
    """
    Abstract base class for Adapters.
    
    Adapters provide connectivity and context for accessing data sources.
    They encapsulate connection details and can be shared across multiple
    Executors (1:N relationship from Adapter to Executors).
    
    Subclasses should implement adapter-specific connection logic and
    expose methods for executors to interact with the data source.
    """

    def __init__(self, executors: Optional[Dict[str, Type["BaseExecutor"]]] = None) -> None:
        """
        Initialize the adapter.
        
        Args:
            executors: Optional mapping of engine names to executor classes.
                       If None, an empty registry is created.
        """
        self._executors: Dict[str, Type["BaseExecutor"]] = executors.copy() if executors else {}
        self.max_failed_rows: int = -1
        self.use_try_cast: bool = True

    def get_executors(self) -> Dict[str, Type["BaseExecutor"]]:
        """
        Get the mapping of engine names to executor classes.
        
        Returns:
            Dict mapping engine name strings (e.g., "sql", "dbt") 
            to executor classes that can handle those engines.
        """
        return self._executors.copy()

    def set_executors(self, executors: Dict[str, Type["BaseExecutor"]]) -> None:
        """
        Replace the entire executor configuration.
        
        Args:
            executors: Dict mapping engine name strings to executor classes.
        """
        self._executors = executors.copy()

    def _get_executor(self, engine: str) -> "BaseExecutor":
        """
        Create an executor instance for an engine.
        
        Args:
            engine: The execution engine name (e.g., "sql", "dbt").
            
        Returns:
            A new executor instance.
            
        Raises:
            NotImplementedError: If no executor is registered for the engine.
        """
        executor_class: Optional[Type["BaseExecutor"]] = self._executors.get(engine)
        if executor_class is None:
            available = ', '.join(sorted(self._executors.keys())) or 'none'
            raise NotImplementedError(
                f"{type(self).__name__} has no executor registered for engine '{engine}'. "
                f"Available engines: {available}"
            )
        return executor_class(self)

    def get_total_rows(self, schema_name: str, max_rows: int = -1) -> int:
        """Get total row count for a table. Subclasses should override."""
        warnings.warn(
            f"{type(self).__name__} does not implement get_total_rows. Returning 0.",
            UserWarning,
            stacklevel=2,
        )
        return 0

    def test_connection(self, table_name: str) -> Optional[str]:
        """
        Test whether the adapter can connect and access a table.

        Subclasses should override this for real connectivity checks.

        Args:
            table_name: Logical or physical table name to probe.

        Returns:
            None on success, or an error/status message on failure.
        """
        warnings.warn(
            f"{type(self).__name__} does not implement test_connection.",
            UserWarning,
            stacklevel=2,
        )
        return "not supported: test_connection is not implemented for this adapter"

    def is_compatible_with(self, other: "BaseAdapter") -> bool:
        """Whether this adapter can execute queries jointly with *other*.

        Two adapters are compatible when a SQL query referencing tables
        from both can be executed directly on one of them without
        materializing data.  The default implementation returns ``False``;
        subclasses should override with backend-specific logic.
        """
        return False

    def export_table_as_arrow(self, schema_name: str) -> pa.Table:
        """
        Export a logical table as a PyArrow table for local materialization.

        Required for adapters that participate in multi-source mode 2
        (materialization into local DuckDB for cross-schema queries).

        Implementations should apply any adapter-owned retrieval logic
        needed for the exported table, including filter conditions if the
        adapter supports them.

        .. note::
            Mode 2 materialization currently assumes whole-table export
            into memory before registration in local DuckDB.  Adapter-side
            filtering is therefore important not just for query semantics
            but also for limiting exported data volume.  Chunked or
            streaming materialization is a potential future enhancement.

        Args:
            schema_name: The logical table name to export.

        Returns:
            A PyArrow Table containing the exported data.

        Raises:
            NotImplementedError: If the adapter does not support
                multi-source materialization.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement export_table_as_arrow. "
            "Multi-source materialization (mode 2) is not supported for this adapter."
        )

    def run_checks(
        self,
        check_refs: List["CheckReference"],
    ) -> List["CheckResult"]:
        """
        Execute data quality checks by dispatching to appropriate executors.
        
        Groups checks by their type, initializes the corresponding executor
        for each type, runs the checks, and aggregates all results.
        
        Args:
            check_refs: List of CheckReference objects to execute.
            
        Returns:
            List of CheckResult objects from all executed checks.
            
        Raises:
            ValueError: If a check type has no registered executor.
        """
        # Group check references by type
        all_results: List["CheckResult"] = []
        refs_by_type: Dict[str, List["CheckReference"]] = defaultdict(list)
        for check_ref in check_refs:
            # Unsupported refs produce ERROR results immediately
            from vowl.contracts.check_reference_unsupported import UnsupportedCheckReference
            if isinstance(check_ref, UnsupportedCheckReference):
                from vowl.executors.base import CheckResult
                all_results.append(
                    CheckResult(
                        check_name=check_ref.get_check_name(),
                        status="ERROR",
                        details=check_ref.error_message,
                        metadata=dict(check_ref.get_result_metadata()),
                        execution_time_ms=0,
                    )
                )
                continue
            engine = check_ref.get_execution_engine()
            refs_by_type[engine].append(check_ref)
        
        # Process each engine
        for engine, type_refs in refs_by_type.items():
            try:
                executor = self._get_executor(engine)
                results = executor.run_batch_checks(type_refs)
                all_results.extend(results)
            except NotImplementedError as e:
                # Return error results for unsupported check types
                from vowl.executors.base import CheckResult
                all_results.extend([
                    CheckResult(
                        check_name=ref.get_check_name(),
                        status="ERROR",
                        details=str(e),
                        execution_time_ms=0,
                    )
                    for ref in type_refs
                ])
        
        return all_results

