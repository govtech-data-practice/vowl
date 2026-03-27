from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import narwhals as nw
import pyarrow as pa

from vowl.executors.security import (
    SQLSecurityError,
    validate_query_security,
)

if TYPE_CHECKING:
    from vowl.adapters.base import BaseAdapter
    from vowl.contracts.check_reference import CheckReference


class CheckResult:
    """
    Represents the result of a single data quality validation check.

    Failed rows are fetched **lazily**: the actual SELECT query is only
    executed when ``.failed_rows`` is first accessed.  This means that
    callers who only inspect ``.passed`` or ``.print_summary()`` never
    pay the cost of fetching row-level data from the backend.
    """

    def __init__(
        self,
        check_name: str,
        status: str,
        details: str,
        actual_value: Any = None,
        expected_value: Any = None,
        failed_rows: Optional[nw.DataFrame] = None,
        failed_rows_fetcher: Optional[Callable[[], Optional[nw.DataFrame]]] = None,
        failed_rows_count: int = 0,
        supports_row_level_output: bool = False,
        metadata: Dict[str, Any] | None = None,
        execution_time_ms: float = 0.0,
    ):
        """
        Initialize a check result.

        Args:
            check_name: Name of the validation check that was executed.
            status: Result status ('PASSED', 'FAILED', or 'ERROR').
            details: Description of the validation outcome.
            actual_value: The actual value returned by the validation query.
            expected_value: The expected value that the check should have returned.
            failed_rows: Eagerly-provided DataFrame of failed rows.  Mutually
                exclusive with *failed_rows_fetcher* — if both are given the
                eager value wins.
            failed_rows_fetcher: A zero-argument callable that returns the
                failed-rows DataFrame.  Called at most once, on first access
                of the ``failed_rows`` property.
            failed_rows_count: Number of rows that failed the check, taken
                directly from the aggregate SQL result so that the summary
                can be built without triggering a row fetch.
            supports_row_level_output: Whether the check can be represented as
                row-level failures in summaries and output DataFrames.
            metadata: Additional metadata about the validation check.
            execution_time_ms: Time taken to execute the check in milliseconds.
        """
        self.check_name = check_name
        self.status = status
        self.details = details
        self.actual_value = actual_value
        self.expected_value = expected_value
        self._failed_rows: Optional[nw.DataFrame] = failed_rows
        self._failed_rows_fetcher = failed_rows_fetcher
        self._failed_rows_count = failed_rows_count
        self._supports_row_level_output = supports_row_level_output
        self.metadata = metadata or {}
        self.execution_time_ms = execution_time_ms

    @property
    def failed_rows(self) -> nw.DataFrame:
        """Rows that failed this check (lazily fetched on first access)."""
        _empty = nw.from_native(pa.table({}), eager_only=True)
        if self._failed_rows is None and self._failed_rows_fetcher is not None:
            self._failed_rows = self._failed_rows_fetcher() or _empty
            self._failed_rows_fetcher = None  # release closure references
        return self._failed_rows if self._failed_rows is not None else _empty

    @property
    def failed_rows_count(self) -> int:
        """Number of failed rows (from SQL count — no row fetch required)."""
        return self._failed_rows_count

    @property
    def supports_row_level_output(self) -> bool:
        """Whether this result can participate in row-level summaries/output."""
        return self._supports_row_level_output

    def __repr__(self) -> str:
        return f"CheckResult(name={self.check_name!r}, status={self.status!r})"


class BaseExecutor(ABC):
    """
    Abstract base class for Executors.

    Executors are responsible for running data quality checks against
    a data source via an Adapter. Each executor holds a reference to
    its adapter (1:1 relationship), while adapters can be shared across
    multiple executors (1:N relationship).
    """

    def __init__(self, adapter: BaseAdapter) -> None:
        """
        Initialize the executor with an adapter.

        Args:
            adapter: The adapter providing data source connectivity.
        """
        self._adapter = adapter

    @property
    def adapter(self) -> BaseAdapter:
        """Get the adapter associated with this executor."""
        return self._adapter

    @abstractmethod
    def run_single_check(self, check_ref: "CheckReference") -> CheckResult:
        """
        Execute a single data quality check.

        Args:
            check_ref: A CheckReference containing the check and its context.

        Returns:
            A CheckResult containing the check outcome.
        """
        pass

    @abstractmethod
    def run_batch_checks(self, check_refs: list["CheckReference"]) -> list[CheckResult]:
        """
        Execute multiple data quality checks.

        Args:
            check_refs: A list of CheckReference objects.

        Returns:
            A list of CheckResult objects.
        """
        pass

    def cleanup(self) -> None:
        """Release any resources held by this executor. No-op by default."""
        pass


class SQLExecutor(BaseExecutor):
    """
    Base class for SQL-based executors.
    
    Provides common functionality for executors that run SQL queries
    against data sources.
    
    Attributes:
        dialect: SQL dialect for sqlglot parsing/generation. Defaults to "postgres"
            which is widely compatible with most SQL databases (PostgreSQL, DuckDB,
            Snowflake, etc.). Override in subclasses if a specific dialect is needed.
    """
    
    # SQL dialect for sqlglot parsing/generation of **input** queries.
    # "postgres" is used as the default because all queries are generated
    # in postgres dialect by the check reference layer (via sqlglot).
    # This is the "read" dialect — specifying how to parse incoming SQL.
    dialect: str = "postgres"

    @property
    def output_dialect(self) -> str:
        """SQL dialect for output/execution.

        By default, same as the input dialect. Subclasses can override
        to transpile queries to a different backend dialect (e.g., Spark,
        MySQL) before execution.
        """
        return self.dialect

    @staticmethod
    def _deduplicate_arrow_columns(table: pa.Table) -> pa.Table:
        """Return an Arrow table with deterministic unique column names."""
        column_names = table.column_names
        counts: Dict[str, int] = {}
        duplicated_names = {name for name in column_names if column_names.count(name) > 1}

        if not duplicated_names:
            return table

        renamed_columns: List[str] = []
        for name in column_names:
            if name in duplicated_names:
                index = counts.get(name, 0)
                renamed_columns.append(name if index == 0 else f"{name}.{index}")
                counts[name] = index + 1
            else:
                renamed_columns.append(name)

        return table.rename_columns(renamed_columns)

    def __init__(
        self,
        adapter: "BaseAdapter",
        use_try_cast: bool = True,
    ) -> None:
        """
        Initialize the SQL executor with an adapter.

        Args:
            adapter: The adapter providing database connectivity.
            use_try_cast: If True, proactively wrap CAST expressions and
                column-vs-literal comparisons in TRY_CAST before execution.
                Default True.
        """
        super().__init__(adapter)
        self._use_try_cast = getattr(adapter, 'use_try_cast', use_try_cast)

    def validate_query_security(self, query: str) -> None:
        """
        Validate a SQL query for security issues.

        Checks that the query:
        1. Is a read-only SELECT query (no INSERT, UPDATE, DELETE, etc.)
        2. Does not contain SQL injection patterns

        Args:
            query: The SQL query to validate.

        Raises:
            SQLSecurityError: If the query fails security validation.
        """
        validate_query_security(query, dialect=self.output_dialect)


# TODO: Remove dead class — GXExecutor has no implementation and nothing uses it.
class GXExecutor(BaseExecutor):
    """
    Base class for Great Expectations (GX) executors.
    
    Provides common functionality for executors that use Great Expectations
    for data validation.
    """

    def __init__(self, adapter: BaseAdapter) -> None:
        """
        Initialize the GX executor with an adapter.

        Args:
            adapter: The adapter providing data source connectivity.
        """
        super().__init__(adapter)
