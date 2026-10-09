from __future__ import annotations

import time
import warnings
from typing import TYPE_CHECKING, Any

import narwhals as nw
import pyarrow as pa

from vowl.executors.base import CappedFetch, CheckResult, RowSource, SQLExecutor
from vowl.executors.security import SQLSecurityError

if TYPE_CHECKING:
    from vowl.adapters.ibis_adapter import IbisAdapter
    from vowl.contracts.check_reference import SQLCheckReference


def raw_result_to_arrow(result: Any) -> pa.Table | None:
    """Resolve an Arrow table from whatever an Ibis backend's ``raw_sql`` returns.

    - DuckDB/Postgres/SQLite: cursor-like with .fetch_arrow_table()
      (DuckDB >= 1.3 renamed it to .to_arrow_table())
    - PySpark 4.0+: DataFrame with .toArrow()
    - PySpark 3.x: internal Arrow export via ._collect_as_arrow()
    - MySQL/MSSQL/Oracle/SQLite: standard DB-API cursor with fetchall()

    Spark Connect DataFrames resolve any attribute to a Column via
    __getattr__, so hasattr() is misleadingly True for methods that don't
    exist. Guard on callable() so a Column (not callable) is skipped while a
    real method is used.

    Returns:
        The rows as an Arrow table, or None when the result has no known shape.
    """
    if callable(getattr(result, "to_arrow_table", None)):
        return result.to_arrow_table()
    if callable(getattr(result, "fetch_arrow_table", None)):
        return result.fetch_arrow_table()
    if callable(getattr(result, "toArrow", None)):
        return result.toArrow()
    if callable(getattr(result, "_collect_as_arrow", None)):
        return pa.Table.from_batches(result._collect_as_arrow())
    if callable(getattr(result, "fetchall", None)) and getattr(result, "description", None) is not None:
        rows = result.fetchall()
        columns = [desc[0] for desc in result.description]
        return pa.table({col: [row[i] for row in rows] for i, col in enumerate(columns)})
    return None


class IbisSQLExecutor(SQLExecutor):
    """
    SQL Executor implementation using Ibis framework.

    Uses an IbisAdapter to execute SQL-based data quality checks
    against various database backends supported by Ibis.
    """

    def __init__(
        self,
        adapter: IbisAdapter,
        use_try_cast: bool = True,
    ) -> None:
        """
        Initialize the Ibis SQL executor.

        Args:
            adapter: An IbisAdapter providing database connectivity.
            use_try_cast: If True, proactively wrap CAST expressions and
                column-vs-literal comparisons in TRY_CAST before execution.
                Default True.
        """
        super().__init__(adapter, use_try_cast)
        self._adapter: IbisAdapter = adapter
        # Detect target SQL dialect from the Ibis backend
        self._target_dialect = adapter.get_sql_dialect()

    @property
    def output_dialect(self) -> str:
        """SQL dialect matching the Ibis backend (e.g. duckdb, sqlite, postgres)."""
        return self._target_dialect

    def _max_failed_rows(self) -> int:
        return getattr(self._adapter, "max_failed_rows", 1000)

    def _fetch_failed_rows(
        self,
        select_query: str | None,
        max_rows: int | None = None,
    ) -> nw.DataFrame | None:
        """
        Fetch the actual rows that failed a check.

        Args:
            select_query: A SELECT query for the failing rows (from
                CheckReference.get_row_query). None if the
                transformation was not possible.
            max_rows: The row limit. Defaults to the adapter's ``max_failed_rows``.

        Returns:
            DataFrame of failed rows, or None if query is None or execution fails.
        """
        if not select_query:
            return None

        # Add LIMIT to avoid fetching too many rows (controlled by config.max_failed_rows)
        if max_rows is None:
            max_rows = self._max_failed_rows()
        select_query = self._with_row_cap(select_query, max_rows, self._target_dialect)

        try:
            # Validate query security before execution
            self.validate_query_security(select_query)

            con = self._adapter.get_connection()
            arrow_table = raw_result_to_arrow(con.raw_sql(select_query))
            if arrow_table is None:
                return None
            arrow_table = self._deduplicate_arrow_columns(arrow_table)
            return nw.from_native(arrow_table, eager_only=True)
        except Exception as e:
            warnings.warn(
                f"Failed to fetch failed rows: {e}",
                UserWarning,
                stacklevel=2,
            )
            return None

    def _execute_query(self, query: str) -> Any:
        """
        Execute a SQL query and return the first result value.

        Args:
            query: The SQL query to execute.

        Returns:
            The first value from the result, or None.

        Raises:
            SQLSecurityError: If the query fails security validation.
        """
        # Validate query security before execution
        self.validate_query_security(query)

        con = self._adapter.get_connection()
        result = con.raw_sql(query)
        # Ibis backends return different types from raw_sql:
        #   - DuckDB/Postgres/SQLite: cursor-like with .fetchone()
        #   - PySpark: a PySpark DataFrame with .collect()
        # Spark Connect DataFrames resolve *any* attribute access to a Column
        # via __getattr__, so hasattr(result, "fetchone") is misleadingly True
        # and calling it raises "'Column' object is not callable". Guard on
        # callable() instead: a real method is callable, a Column is not.
        if callable(getattr(result, "fetchone", None)):
            row = result.fetchone()
            return row[0] if row else None
        elif callable(getattr(result, "collect", None)):
            rows = result.collect()
            return rows[0][0] if rows else None
        return None

    def run_single_check(self, check_ref: SQLCheckReference) -> CheckResult:
        """
        Execute a single data quality check.

        Args:
            check_ref: A SQLCheckReference containing the check and its context.

        Returns:
            A CheckResult containing the check outcome.
        """
        dialect = self.output_dialect
        filters = self._adapter.filter_conditions
        use_try_cast = self._use_try_cast
        start_time = time.perf_counter()

        try:
            scalar_query = check_ref.get_scalar_query(
                dialect,
                filters,
                use_try_cast=use_try_cast,
            )
            if not scalar_query:
                return check_ref.build_error_result(
                    error_message="No query specified for SQL check",
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                    dialect=dialect,
                )

            try:
                actual_value = self._execute_query(scalar_query)
            except SQLSecurityError as sec_error:
                return check_ref.build_error_result(
                    error_message=f"Security validation failed: {sec_error}",
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                    dialect=dialect,
                    filter_conditions=filters,
                    use_try_cast=use_try_cast,
                    security_violation=sec_error.violation_type,
                )

            failed_query = check_ref.get_row_query(
                dialect,
                filters,
                use_try_cast=use_try_cast,
            )

            fetcher = CappedFetch(
                lambda limit, q=failed_query: self._fetch_failed_rows(q, limit), self._max_failed_rows
            )

            result = check_ref.build_result(
                actual_value=actual_value,
                execution_time_ms=(time.perf_counter() - start_time) * 1000,
                failed_rows_fetcher=fetcher,
                dialect=dialect,
                filter_conditions=filters,
                use_try_cast=use_try_cast,
            )
            if result.status != "ERROR":
                result.row_source = RowSource(
                    check_ref=check_ref,
                    dialect=dialect,
                    filter_conditions=filters,
                    use_try_cast=use_try_cast,
                    adapter=self._adapter,
                    row_query=failed_query,
                )
            return result

        except Exception as e:
            return check_ref.build_error_result(
                error_message=f"Error executing check: {e}",
                execution_time_ms=(time.perf_counter() - start_time) * 1000,
                dialect=dialect,
                filter_conditions=filters,
                use_try_cast=use_try_cast,
            )

    def run_batch_checks(self, check_refs: list[SQLCheckReference]) -> list[CheckResult]:
        """
        Execute multiple data quality checks.

        Args:
            check_refs: A list of CheckReference objects.

        Returns:
            A list of CheckResult objects.
        """
        return [self.run_single_check(check_ref) for check_ref in check_refs]
