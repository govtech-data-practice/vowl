"""
Multi-Source SQL Executor for cross-schema queries.

Handles SQL queries that reference multiple schemas/tables, routing
execution appropriately based on the underlying adapter backends.
"""

from __future__ import annotations

import time
import warnings
from typing import TYPE_CHECKING, Any

import narwhals as nw
import sqlglot
from sqlglot import exp

from vowl.executors.base import CheckResult, SQLExecutor
from vowl.executors.security import SQLSecurityError, sanitize_identifier

if TYPE_CHECKING:
    from vowl.adapters.base import BaseAdapter
    from vowl.adapters.multi_source_adapter import MultiSourceAdapter
    from vowl.contracts.check_reference import SQLCheckReference

class MultiSourceSQLExecutor(SQLExecutor):
    """
    SQL Executor for handling cross-schema queries in multi-source scenarios.
    
    This executor handles queries that reference multiple tables/schemas:
    - For DuckDB-compatible backends (postgres, mysql, sqlite), uses ATTACH
      to connect directly without copying data
    - For other backends, materializes required data into a local DuckDB instance
    
    Filter conditions from each adapter are applied to their respective tables
    using the same subquery pattern as IbisSQLExecutor.
    
    Example:
        >>> # Query joining orders and products from different sources
        >>> query = "SELECT COUNT(*) FROM orders o JOIN products p ON o.product_id = p.id"
        >>> # Executor detects multiple tables and routes appropriately
    """

    # Mode 2 always executes on local DuckDB.
    dialect: str = "duckdb"

    def __init__(
        self,
        multi_adapter: MultiSourceAdapter,
        use_try_cast: bool = True,
    ) -> None:
        """
        Initialize the multi-source SQL executor.

        Args:
            multi_adapter: A MultiSourceAdapter containing adapters for each schema.
            use_try_cast: If True, proactively wrap CAST expressions and
                column-vs-literal comparisons in TRY_CAST before execution.
                Default True.
        """
        # Skip super().__init__() — MultiSourceSQLExecutor has no single adapter;
        # it delegates to per-schema adapters via self._multi_adapter instead.
        self._multi_adapter = multi_adapter
        # Mirror SQLExecutor behavior: prefer adapter-level configuration so
        # ValidationConfig.use_try_cast propagates consistently.
        self._use_try_cast = getattr(multi_adapter, "use_try_cast", use_try_cast)
        self._local_duckdb_con = None  # Lazily created by _get_local_duckdb()
        self._attached_sources: set[str] = set()  # Table names already materialized into local DuckDB

    @property
    def adapter(self):
        raise NotImplementedError(
            "MultiSourceSQLExecutor does not have a single adapter. "
            "Use self._multi_adapter to access individual adapters per schema."
        )

    def _detect_tables(self, query: str) -> set[str]:
        """
        Detect all table names referenced in a SQL query.
        
        Args:
            query: The SQL query to analyze
            
        Returns:
            Set of table names found in the query
        """
        try:
            parsed = sqlglot.parse_one(query)
            tables = parsed.find_all(exp.Table)
            return {t.name for t in tables if t.name}
        except Exception as e:
            warnings.warn(
                f"Failed to parse SQL query for table detection: {e}",
                UserWarning,
                stacklevel=2,
            )
            return set()

    def _are_backends_compatible(self, table_names: set[str]) -> bool:
        """
        Check if all required adapters can execute queries together directly.

        Delegates the compatibility decision to the adapters themselves
        via ``BaseAdapter.is_compatible_with``.
        
        Args:
            table_names: Set of table names that need to be queried
            
        Returns:
            True if every pair of adapters reports mutual compatibility.
        """
        adapters = []
        for table_name in table_names:
            adapter = self._multi_adapter.get_adapter(table_name)
            if adapter is None:
                return False
            adapters.append(adapter)

        return all(
            a.is_compatible_with(b)
            for i, a in enumerate(adapters)
            for b in adapters[i + 1:]
        )

    def _get_local_duckdb(self):
        """
        Get or create a local DuckDB connection for cross-backend queries.
        
        Returns:
            An Ibis DuckDB connection
        """
        if self._local_duckdb_con is None:
            import ibis
            self._local_duckdb_con = ibis.duckdb.connect()
        return self._local_duckdb_con

    def _fetch_failed_rows(
        self,
        select_query: str | None,
        table_names: set[str],
    ) -> nw.DataFrame | None:
        """
        Fetch the actual rows that failed a check.
        
        Args:
            select_query: A SELECT query for the failing rows (from
                CheckReference.get_failed_rows_query). None if the
                transformation was not possible.
            table_names: Tables referenced in the query
            
        Returns:
            DataFrame of failed rows, or None if query is None or execution fails.
        """
        if not select_query:
            return None

        max_rows = getattr(self._multi_adapter, 'max_failed_rows', 1000)
        if max_rows >= 0 and "LIMIT" not in select_query.upper():
            select_query = f"{select_query} LIMIT {max_rows}"

        try:
            self.validate_query_security(select_query)
            self._ensure_tables_available(table_names)
            local_con = self._get_local_duckdb()
            arrow_table = local_con.raw_sql(select_query).fetch_arrow_table()
            arrow_table = self._deduplicate_arrow_columns(arrow_table)
            return nw.from_native(arrow_table, eager_only=True)

        except Exception as e:
            warnings.warn(
                f"Failed to fetch failed rows for cross-schema check: {e}",
                UserWarning,
                stacklevel=2,
            )
            return None

    def _attach_or_materialize(
        self,
        adapter: BaseAdapter,
        schema_name: str,
        local_con,
    ) -> None:
        """
        Attach a database or materialize a table into local DuckDB.
        
        Prefers ATTACH for supported backends, falls back to materialization.
        
        Args:
            adapter: The source adapter.
            schema_name: The schema/table name to make available.
            local_con: The local DuckDB connection.
        """
        if schema_name in self._attached_sources:
            return

        # TODO: Use DuckDB ATTACH for supported backends (postgres, mysql, sqlite)
        # instead of materializing, to avoid copying data.
        self._materialize_table_to_duckdb(adapter, schema_name, local_con)
        self._attached_sources.add(schema_name)

    def _ensure_tables_available(self, table_names: set[str]) -> None:
        """
        Ensure all required tables are available in local DuckDB.

        Raises:
            NotImplementedError: If an adapter's ``export_table_as_arrow``
                is not implemented (raised by ``BaseAdapter``).
            ValueError: If no adapter is configured for a table.
        """
        local_con = self._get_local_duckdb()

        for table_name in table_names:
            if table_name in self._attached_sources:
                continue

            adapter = self._multi_adapter.get_adapter(table_name)
            if adapter is None:
                raise ValueError(f"No adapter configured for table '{table_name}'")

            self._attach_or_materialize(adapter, table_name, local_con)

    def _materialize_table_to_duckdb(
        self,
        adapter: BaseAdapter,
        schema_name: str,
        local_con,
    ) -> None:
        """
        Pull a table's data from its source adapter into local DuckDB.

        Delegates table retrieval (including filter application) entirely
        to the adapter via ``export_table_as_arrow``.  The executor is
        only responsible for registering the resulting Arrow table in
        local DuckDB.
        
        Args:
            adapter: The source adapter for the table.
            schema_name: Name to register the table as in DuckDB.
            local_con: The local DuckDB connection.
        """
        sanitize_identifier(schema_name)

        arrow_table = adapter.export_table_as_arrow(schema_name)
        local_con.raw_sql(f"DROP TABLE IF EXISTS {schema_name}")
        local_con.create_table(schema_name, arrow_table, overwrite=True)

    def _execute_query(
        self,
        query: str,
        table_names: set[str],
    ) -> Any:
        """
        Validate and execute a SQL query on local DuckDB.

        Args:
            query: The SQL query to execute.
            table_names: Tables referenced in the query.

        Returns:
            Query result (first row).

        Raises:
            SQLSecurityError: If the query fails security validation.
        """
        self.validate_query_security(query)
        self._ensure_tables_available(table_names)
        local_con = self._get_local_duckdb()
        return local_con.raw_sql(query).fetchone()

    def run_single_check(self, check_ref: SQLCheckReference) -> CheckResult:
        """
        Execute a single data quality check that may span multiple schemas.

        For compatible adapters (mode 1) the check is delegated entirely to
        the first adapter's own SQL executor.

        For incompatible adapters (mode 2) the executor materializes each
        table into local DuckDB via ``export_table_as_arrow`` and runs the
        query there.

        Args:
            check_ref: A SQLCheckReference containing the check and its context.

        Returns:
            A CheckResult containing the check outcome.
        """
        start_time = time.perf_counter()

        try:
            raw_query = check_ref.get_check().get("query") or ""
            table_names = self._detect_tables(raw_query) if raw_query else set()

            if not table_names:
                return check_ref.build_error_result(
                    error_message="Could not detect tables in query",
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                )

            # --- Mode 1: delegate to the first adapter's executor -----------
            if self._are_backends_compatible(table_names):
                first_table = next(iter(table_names))
                adapter = self._multi_adapter.get_adapter(first_table)
                if adapter is None:
                    raise ValueError(f"No adapter for table '{first_table}'")
                executor = adapter._get_executor("sql")
                return executor.run_single_check(check_ref)

            # --- Mode 2: materialize into local DuckDB ----------------------
            output_dialect = "duckdb"
            query_filters = None  # filters are baked into exported tables
            use_try_cast = self._use_try_cast

            scalar_query = check_ref.get_scalar_query(
                output_dialect, query_filters,
                use_try_cast=use_try_cast,
            )
            if not scalar_query:
                return check_ref.build_error_result(
                    error_message="No query specified for SQL check",
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                    dialect=output_dialect,
                    multi_source=True,
                )

            try:
                result = self._execute_query(scalar_query, table_names)
                actual_value = result[0] if result else None
            except SQLSecurityError as sec_error:
                return check_ref.build_error_result(
                    error_message=f"Security validation failed: {sec_error}",
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                    dialect=output_dialect,
                    filter_conditions=query_filters,
                    use_try_cast=use_try_cast,
                    multi_source=True,
                    security_violation=sec_error.violation_type,
                )

            failed_query = check_ref.get_failed_rows_query(
                output_dialect, query_filters,
                use_try_cast=use_try_cast,
            )
            fetcher = lambda q=failed_query, t=table_names: self._fetch_failed_rows(q, t)

            return check_ref.build_result(
                actual_value=actual_value,
                execution_time_ms=(time.perf_counter() - start_time) * 1000,
                failed_rows_fetcher=fetcher,
                dialect=output_dialect,
                filter_conditions=query_filters,
                use_try_cast=use_try_cast,
            )

        except Exception as e:
            return check_ref.build_error_result(
                error_message=f"Error executing cross-schema check: {e}",
                execution_time_ms=(time.perf_counter() - start_time) * 1000,
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

    def cleanup(self) -> None:
        """
        Clean up resources (close local DuckDB connection if created).
        """
        if self._local_duckdb_con is not None:
            try:
                self._local_duckdb_con.disconnect()
            except Exception:
                self._local_duckdb_con = None
                self._attached_sources.clear()
                return
            self._local_duckdb_con = None
        self._attached_sources.clear()
