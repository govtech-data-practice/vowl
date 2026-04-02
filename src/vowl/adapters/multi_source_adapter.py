"""
Multi-Source Adapter for routing checks across multiple data sources.

Handles contracts with multiple schemas where each schema may be backed
by a different data source (database, DataFrame, etc.).
"""

from __future__ import annotations

import copy
import warnings
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp

from .base import BaseAdapter

if TYPE_CHECKING:
    from ..contracts.check_reference import CheckReference
    from ..executors.base import CheckResult


class MultiSourceAdapter(BaseAdapter):
    """
    Adapter for validating contracts with multiple schemas/tables.

    Routes checks to the appropriate single-source adapter based on which
    schema they belong to. Each schema in the contract can be backed by
    a different data source.

    For cross-table queries (joins), uses a MultiSourceExecutor that can
    coordinate queries across multiple sources.

    Example:
        >>> from vowl.adapters import IbisAdapter, MultiSourceAdapter
        >>> import ibis
        >>>
        >>> # Create adapters for each schema
        >>> orders_adapter = IbisAdapter(
        ...     con=ibis.postgres.connect(...),
        ...     filter_conditions={"raw_orders": {"field": "created_at", "operator": ">=", "value": "2024-01-01"}},
        ... )
        >>> products_adapter = IbisAdapter(ibis.duckdb.connect())
        >>>
        >>> # Create multi-source adapter
        >>> adapter = MultiSourceAdapter({
        ...     "orders": orders_adapter,
        ...     "products": products_adapter,
        ... })
        >>>
        >>> # Run checks - automatically routes to correct adapter
        >>> refs_by_schema = contract.get_check_references_by_schema()
        >>> results = adapter.run_checks_by_schema(refs_by_schema)
    """

    def __init__(
        self,
        adapters: dict[str, BaseAdapter],
    ) -> None:
        """
        Initialize the multi-source adapter.

        Args:
            adapters: Dict mapping schema names to their adapter instances.
                Keys should match schema names defined in the contract.
                Adapters that implement ``export_table_as_arrow`` can
                participate in mode 2 (DuckDB materialization) cross-schema
                queries.
        """
        from ..executors.multi_source_sql_executor import MultiSourceSQLExecutor

        # Register multi-source executor for SQL checks
        super().__init__(executors={"sql": MultiSourceSQLExecutor})
        self._adapters = dict(adapters)

        # Detect when a single adapter instance is shared across multiple schemas.
        # This happens when the user provides one adapter for a multi-schema contract.
        # Shallow copy shared instances so each schema gets its own state
        # while still sharing the underlying connection.
        seen_ids: dict[int, str] = {}  # id(adapter) -> first schema_name
        shared_schemas: list[str] = []
        for schema_name, adapter in list(self._adapters.items()):
            adapter_id = id(adapter)
            if adapter_id in seen_ids:
                shared_schemas.append(schema_name)
                self._adapters[schema_name] = copy.copy(adapter)
            else:
                seen_ids[adapter_id] = schema_name

        if shared_schemas:
            all_schemas = list(self._adapters.keys())
            warnings.warn(
                f"Multiple schemas detected ({all_schemas}) but only 1 input adapter provided. "
                f"Assuming adapter configuration is used for all schemas.",
                UserWarning,
                stacklevel=2,
            )

    def _detect_tables_in_query(self, query: str) -> set[str]:
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
        except Exception:
            return set()

    def _is_multi_table_check(self, check_ref: CheckReference) -> bool:
        """
        Determine if a check involves multiple tables.

        Args:
            check_ref: A CheckReference to analyze

        Returns:
            True if the check's query references multiple tables
        """
        check = check_ref.get_check()
        query = check.get("query")
        if not query:
            return False

        tables = self._detect_tables_in_query(query)
        return len(tables) > 1

    @property
    def adapters(self) -> dict[str, BaseAdapter]:
        """Get the mapping of schema names to adapters."""
        return self._adapters

    @property
    def schema_names(self) -> list[str]:
        """Get the list of schema names this adapter handles."""
        return list(self._adapters.keys())

    def get_adapter(self, schema_name: str) -> BaseAdapter | None:
        """
        Get the adapter for a specific schema.

        Args:
            schema_name: The schema name to look up.

        Returns:
            The adapter for that schema, or None if not found.
        """
        return self._adapters.get(schema_name)

    def test_connections(
        self,
        check_refs_by_schema: dict[str, list[CheckReference]] | None = None,
    ) -> dict[str, dict[str, str]]:
        """
        Test all adapter connections, including tables referenced in checks.

        For each schema:
        1. Tests the schema's own table via its adapter.
        2. If check_refs_by_schema is provided, detects all tables referenced
           in that schema's check queries and tests each:
           - Own table: already tested in step 1.
           - Another registered schema: skipped (tested under its own adapter).
           - Unknown table: tested with this schema's adapter; warns if it
             succeeds (table exists but is not a defined schema) or errors
             if inaccessible.

        Args:
            check_refs_by_schema: Optional dict mapping schema names to their
                CheckReference objects. When provided, referenced tables in
                queries are also tested.

        Returns:
            Dict mapping schema names to a dict of {table_name: status_string}.
            Status is 'success', 'skipped', or an error message.
        """
        all_registered = set(self._adapters.keys())
        results: dict[str, dict[str, str]] = {}

        for schema_name, adapter in self._adapters.items():
            schema_results: dict[str, str] = {}

            # 1. Test the schema's own table
            error = adapter.test_connection(schema_name)
            schema_results[schema_name] = error or "success"
            if error:
                warnings.warn(
                    f"Connection test failed for schema '{schema_name}': {error}",
                    UserWarning,
                    stacklevel=2,
                )

            # 2. Test tables referenced in this schema's checks
            if check_refs_by_schema and schema_name in check_refs_by_schema:
                referenced_tables: set[str] = set()
                for check_ref in check_refs_by_schema[schema_name]:
                    check = check_ref.get_check()
                    query = check.get("query")
                    if query:
                        referenced_tables |= self._detect_tables_in_query(query)

                for table in sorted(referenced_tables):
                    if table == schema_name:
                        # Already tested above
                        continue

                    if table in all_registered:
                        # Belongs to another schema; will be tested there
                        schema_results[table] = f"skipped: table defined in schema '{table}'"
                        continue

                    # Unknown table; test with this schema's adapter
                    table_error = adapter.test_connection(table)
                    if table_error:
                        schema_results[table] = (
                            f"error: table '{table}' not accessible via "
                            f"schema '{schema_name}' adapter: {table_error}"
                        )
                        warnings.warn(
                            f"Schema '{schema_name}' references table '{table}' "
                            f"which is not a defined schema and is not accessible: {table_error}. "
                            f"Note: cross-source queries only work for tables defined as "
                            f"schemas in the contract.",
                            UserWarning,
                            stacklevel=2,
                        )
                    else:
                        warnings.warn(
                            f"Schema '{schema_name}' references table '{table}' "
                            f"which is not a defined schema but is accessible via "
                            f"this adapter's connection.",
                            UserWarning,
                            stacklevel=2,
                        )

            results[schema_name] = schema_results

        return results

    def run_checks(  # type: ignore[override]
        self,
        check_refs_by_schema: dict[str, list[CheckReference]],
    ) -> list[CheckResult]:
        """
        Run checks, routing each to the appropriate adapter based on schema.

        Single-table checks are routed to the corresponding adapter.
        Multi-table checks (joins) are handled by MultiSourceSQLExecutor.

        Args:
            check_refs_by_schema: Dict mapping schema names to their CheckReference objects.

        Returns:
            Combined list of CheckResult objects from all schemas.
        """
        from ..executors.base import CheckResult

        all_results: list[CheckResult] = []
        multi_table_refs: list[CheckReference] = []

        for schema_name, check_refs in check_refs_by_schema.items():
            if not check_refs:
                continue

            # Separate single-table and multi-table checks
            single_table_refs: list[CheckReference] = []

            for check_ref in check_refs:
                if self._is_multi_table_check(check_ref):
                    multi_table_refs.append(check_ref)
                else:
                    single_table_refs.append(check_ref)

            # Process single-table checks with the schema's adapter
            if single_table_refs:
                adapter = self._adapters.get(schema_name)

                if adapter is None:
                    # Return error results for this schema's checks
                    all_results.extend([
                        CheckResult(
                            check_name=check_ref.get_check_name(),
                            status="ERROR",
                            details=f"No adapter configured for schema '{schema_name}'",
                            execution_time_ms=0,
                        )
                        for check_ref in single_table_refs
                    ])
                else:
                    results = adapter.run_checks(single_table_refs)
                    all_results.extend(results)

        # Process multi-table checks with MultiSourceSQLExecutor
        if multi_table_refs:
            executor = self._get_executor("sql")
            multi_results = executor.run_batch_checks(multi_table_refs)
            all_results.extend(multi_results)
            # Clean up local DuckDB resources after cross-schema execution
            executor.cleanup()

        return all_results

    def get_total_rows_by_schema(self, max_rows: int = -1) -> dict[str, int]:
        """
        Get total row counts for each schema via its adapter.

        Args:
            max_rows: If >= 0, cap the count at this value per schema.

        Returns:
            Dict mapping schema name to row count.
        """
        return {
            schema_name: adapter.get_total_rows(schema_name, max_rows)
            for schema_name, adapter in self._adapters.items()
        }


