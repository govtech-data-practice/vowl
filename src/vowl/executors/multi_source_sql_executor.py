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

from vowl.contracts.sql_transforms import matching_filter_conditions
from vowl.executors.base import CheckResult, RowSource, SQLExecutor
from vowl.executors.security import SQLSecurityError, sanitize_identifier

if TYPE_CHECKING:
    from vowl.adapters.base import BaseAdapter
    from vowl.adapters.multi_source_adapter import MultiSourceAdapter
    from vowl.contracts.check_reference import SQLCheckReference


class MultiSourceSQLExecutor(SQLExecutor):
    """
    SQL Executor for handling cross-schema queries in multi-source scenarios.

    This executor handles queries that reference multiple tables/schemas via
    two modes (see ``run_single_check``):
    - Mode 1 (compatible adapters): when every referenced table is served by
      the same backend *and the same connection* (``BaseAdapter.is_compatible_with``),
      the check is delegated to that adapter's own SQL executor and runs
      natively there — no data is copied. Copies of one ``PooledAdapter``
      count as one connection. The check runs on an instance leased from
      the pool.
    - Mode 2 (incompatible adapters, e.g. tables spread across different
      connections or backends): each required table is materialized into a
      fresh local DuckDB instance via ``export_table_as_arrow`` and the query
      runs against those copies.

    Filter conditions from each adapter are applied to their respective tables
    using the same subquery pattern as IbisSQLExecutor. In mode 1 the tables'
    filters are merged into one dict for the adapter that runs the check (see
    ``_native_route``).

    A table the contract does not declare as a schema (for example a lookup
    table) is served by the adapter of the schema the check sits under. See
    ``_resolve_adapter``.

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
        # MultiSourceSQLExecutor has no single adapter; it delegates to
        # per-schema adapters via self._multi_adapter instead.  We pass
        # adapter=None here; the overridden `adapter` property will raise
        # if anyone tries to access it directly.
        super().__init__(adapter=None, use_try_cast=use_try_cast)
        self._multi_adapter = multi_adapter
        # Override: prefer adapter-level configuration so
        # ValidationConfig.use_try_cast propagates consistently.
        self._use_try_cast = getattr(multi_adapter, "use_try_cast", use_try_cast)
        self._local_duckdb_con = None  # Lazily created by _get_local_duckdb()
        # Table name -> adapter it was materialized from in local DuckDB
        self._attached_sources: dict[str, BaseAdapter] = {}

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

    @staticmethod
    def _owner_schema(check_ref: SQLCheckReference) -> str | None:
        """Return the name of the schema a check sits under, if known."""
        get_schema_name = getattr(check_ref, "get_schema_name", None)
        return get_schema_name() if callable(get_schema_name) else None

    def _resolve_adapter(self, table_name: str, owner_schema: str | None = None) -> BaseAdapter | None:
        """
        Return the adapter that serves ``table_name`` for one check.

        A declared schema is served by its own adapter. A table the contract
        does not declare is served by the adapter of ``owner_schema``, the
        schema the check sits under. That is the same connection a
        single-table check on the table runs on, and the one the pre-flight
        ``MultiSourceAdapter.test_connections`` tests it with.

        Args:
            table_name: Table referenced in the check's query.
            owner_schema: Schema the check sits under.

        Returns:
            The adapter, or None if neither lookup finds one.
        """
        adapter = self._multi_adapter.get_adapter(table_name)
        if adapter is None and owner_schema is not None:
            adapter = self._multi_adapter.get_adapter(owner_schema)
        return adapter

    def _are_backends_compatible(self, table_names: set[str], owner_schema: str | None = None) -> bool:
        """
        Check if all required adapters can execute queries together directly.

        Delegates the compatibility decision to the adapters themselves
        via ``BaseAdapter.is_compatible_with``.

        Args:
            table_names: Set of table names that need to be queried
            owner_schema: Schema the check sits under, used to resolve
                tables the contract does not declare.

        Returns:
            True if every pair of adapters reports mutual compatibility.
        """
        adapters = []
        for table_name in table_names:
            adapter = self._resolve_adapter(table_name, owner_schema)
            if adapter is None:
                return False
            adapters.append(adapter)

        # Ask both sides, since an adapter only knows its own kind of peer
        return all(
            a.is_compatible_with(b) and b.is_compatible_with(a)
            for i, a in enumerate(adapters)
            for b in adapters[i + 1 :]
        )

    def _native_route(
        self, table_names: set[str], owner_schema: str | None = None
    ) -> tuple[BaseAdapter, dict[str, list[Any]] | None] | None:
        """
        Return how a check runs natively, or None for mode 2.

        The adapters must be compatible (see ``_are_backends_compatible``).
        The native executor applies one filter dict to every table in the
        query, so each table's conditions are first resolved against the
        adapter that serves it and merged under the exact table name. That is
        the same set of conditions a mode 2 export of the table applies. When
        the merged dict differs from what the running adapter applies on its
        own, the check runs on a copy of it from ``with_filter_conditions``.
        An adapter without that method falls back to mode 2. For a
        ``PooledAdapter`` the method is looked up on its pooled instances.

        Args:
            table_names: Tables referenced in the check's query.
            owner_schema: Schema the check sits under, used to resolve
                tables the contract does not declare.

        Returns:
            ``(adapter, filters)`` to run the check on ``adapter``.
            ``filters`` is the merged dict to run it with, or None when the
            adapter's own filters already match. None in place of the tuple
            means copy the tables into local DuckDB.

        Raises:
            ValueError: If no adapter serves the table picked to run the check.
        """
        if not self._are_backends_compatible(table_names, owner_schema):
            return None

        # Prefer the owner schema's adapter, then go by name, so the choice
        # does not depend on set order.
        ordered = sorted(table_names, key=lambda name: (name != owner_schema, name))
        adapter = self._resolve_adapter(ordered[0], owner_schema)
        if adapter is None:
            raise ValueError(f"No adapter for table '{ordered[0]}'")

        own_filters = getattr(adapter, "filter_conditions", {}) or {}
        merged: dict[str, list[Any]] = {}
        own: dict[str, list[Any]] = {}
        for table_name in ordered:
            source = self._resolve_adapter(table_name, owner_schema)
            conditions = matching_filter_conditions(table_name, getattr(source, "filter_conditions", {}) or {})
            if conditions:
                merged[table_name] = conditions
            conditions = matching_filter_conditions(table_name, own_filters)
            if conditions:
                own[table_name] = conditions

        if merged == own:
            return adapter, None
        from vowl.adapters.pooled_adapter import PooledAdapter

        runner = adapter._primary_adapter if isinstance(adapter, PooledAdapter) else adapter
        if not callable(getattr(runner, "with_filter_conditions", None)):
            return None
        return adapter, merged

    def _run_native(
        self,
        adapter: BaseAdapter,
        filters: dict[str, list[Any]] | None,
        check_ref: SQLCheckReference,
    ) -> CheckResult:
        """Run a check on one adapter's own SQL executor (mode 1).

        A ``PooledAdapter`` lends one of its instances for the check, so
        the check never shares a connection with another thread.
        """
        from vowl.adapters.pooled_adapter import PooledAdapter

        if isinstance(adapter, PooledAdapter):
            with adapter._lease() as leased:
                return self._run_native(leased, filters, check_ref)
        if filters is not None:
            adapter = adapter.with_filter_conditions(filters)
        return adapter._get_executor("sql").run_single_check(check_ref)

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
        owner_schema: str | None = None,
    ) -> nw.DataFrame | None:
        """
        Fetch the actual rows that failed a check.

        Args:
            select_query: A SELECT query for the failing rows (from
                CheckReference.get_failed_rows_query). None if the
                transformation was not possible.
            table_names: Tables referenced in the query
            owner_schema: Schema the check sits under.

        Returns:
            DataFrame of failed rows, or None if query is None or execution fails.
        """
        if not select_query:
            return None

        max_rows = getattr(self._multi_adapter, "max_failed_rows", 1000)
        select_query = self._with_row_cap(select_query, max_rows, "duckdb")

        try:
            self.validate_query_security(select_query)
            self._ensure_tables_available(table_names, owner_schema)
            local_con = self._get_local_duckdb()
            result = local_con.raw_sql(select_query)
            if hasattr(result, "to_arrow_table"):
                arrow_table = result.to_arrow_table()
            else:
                arrow_table = result.fetch_arrow_table()
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
        Make a source table available in local DuckDB.

        Currently this always materializes the table (pulls its rows via the
        source adapter's ``export_table_as_arrow`` and registers the resulting
        Arrow table in local DuckDB). DuckDB ATTACH is not yet implemented — see
        the TODO below.

        Args:
            adapter: The source adapter.
            schema_name: The schema/table name to make available.
            local_con: The local DuckDB connection.
        """
        # An undeclared table can resolve to a different adapter for checks
        # under different schemas, so reuse the copy only if it came from
        # the same adapter.
        if self._attached_sources.get(schema_name) is adapter:
            return

        # TODO: For DuckDB-compatible backends (postgres, mysql, sqlite), use
        # DuckDB ATTACH to stream directly from the source instead of
        # materializing, to avoid copying data. Until then we always
        # materialize regardless of backend.
        self._materialize_table_to_duckdb(adapter, schema_name, local_con)
        self._attached_sources[schema_name] = adapter

    def _ensure_tables_available(self, table_names: set[str], owner_schema: str | None = None) -> None:
        """
        Ensure all required tables are available in local DuckDB.

        Args:
            table_names: Tables referenced in the query.
            owner_schema: Schema the check sits under, used to resolve
                tables the contract does not declare.

        Raises:
            NotImplementedError: If an adapter's ``export_table_as_arrow``
                is not implemented (raised by ``BaseAdapter``).
            ValueError: If no adapter is configured for a table.
        """
        local_con = self._get_local_duckdb()

        for table_name in table_names:
            adapter = self._resolve_adapter(table_name, owner_schema)
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
        owner_schema: str | None = None,
    ) -> Any:
        """
        Validate and execute a SQL query on local DuckDB.

        Args:
            query: The SQL query to execute.
            table_names: Tables referenced in the query.
            owner_schema: Schema the check sits under.

        Returns:
            Query result (first row).

        Raises:
            SQLSecurityError: If the query fails security validation.
        """
        self.validate_query_security(query)
        self._ensure_tables_available(table_names, owner_schema)
        local_con = self._get_local_duckdb()
        return local_con.raw_sql(query).fetchone()

    def run_single_check(self, check_ref: SQLCheckReference) -> CheckResult:
        """
        Execute a single data quality check that may span multiple schemas.

        For compatible adapters (mode 1) the check is delegated entirely to
        one adapter's own SQL executor, with every table's own filters (see
        ``_native_route``).

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
            owner_schema = self._owner_schema(check_ref)

            if not table_names:
                return check_ref.build_error_result(
                    error_message="Could not detect tables in query",
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                    dialect="duckdb",
                    multi_source=True,
                )

            # --- Mode 1: delegate to one adapter's executor ------------------
            route = self._native_route(table_names, owner_schema)
            if route is not None:
                return self._run_native(*route, check_ref)

            # --- Mode 2: materialize into local DuckDB ----------------------
            output_dialect = "duckdb"
            query_filters = None  # filters are baked into exported tables
            use_try_cast = self._use_try_cast

            scalar_query = check_ref.get_scalar_query(
                output_dialect,
                query_filters,
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
                result = self._execute_query(scalar_query, table_names, owner_schema)
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
                output_dialect,
                query_filters,
                use_try_cast=use_try_cast,
            )

            def fetcher(q=failed_query, t=table_names, o=owner_schema):
                return self._fetch_failed_rows(q, t, o)

            result = check_ref.build_result(
                actual_value=actual_value,
                execution_time_ms=(time.perf_counter() - start_time) * 1000,
                failed_rows_fetcher=fetcher,
                dialect=output_dialect,
                filter_conditions=query_filters,
                use_try_cast=use_try_cast,
            )
            if result.status != "ERROR":
                # The tables live in a local copy, so the row-quality
                # component can only use this check's fetched rows.
                result.row_source = RowSource(
                    check_ref=check_ref,
                    dialect=output_dialect,
                    filter_conditions=query_filters,
                    use_try_cast=use_try_cast,
                    failed_rows_query=failed_query,
                    fetch=fetcher,
                    cross_source=True,
                )
            return result

        except Exception as e:
            return check_ref.build_error_result(
                error_message=f"Error executing cross-schema check: {e}",
                execution_time_ms=(time.perf_counter() - start_time) * 1000,
                dialect="duckdb",
                multi_source=True,
            )

    def run_batch_checks(self, check_refs: list[SQLCheckReference]) -> list[CheckResult]:
        """
        Execute multiple data quality checks.

        Parallelizes checks when all involved adapters are PooledAdapters.
        Falls back to sequential execution otherwise.

        Args:
            check_refs: A list of CheckReference objects.

        Returns:
            A list of CheckResult objects.
        """
        concurrency = self._derive_concurrency(check_refs)
        if concurrency <= 1 or len(check_refs) <= 1:
            return [self.run_single_check(check_ref) for check_ref in check_refs]
        return self._run_parallel_mode2(check_refs, concurrency)

    def _derive_concurrency(self, check_refs: list[SQLCheckReference]) -> int:
        """Derive max concurrency from involved PooledAdapters.

        Returns 1 if any involved adapter is not a PooledAdapter.
        """
        from vowl.adapters.pooled_adapter import PooledAdapter

        all_tables: set[tuple[str, str | None]] = set()
        for ref in check_refs:
            raw_query = ref.get_check().get("query") or ""
            owner_schema = self._owner_schema(ref)
            all_tables |= {(table, owner_schema) for table in self._detect_tables(raw_query)}

        if not all_tables:
            return 1

        concurrencies: list[int] = []
        for table_name, owner_schema in all_tables:
            adapter = self._resolve_adapter(table_name, owner_schema)
            if adapter is None:
                return 1
            if not isinstance(adapter, PooledAdapter):
                return 1
            concurrencies.append(adapter.max_concurrency)

        return min(concurrencies) if concurrencies else 1

    def _run_parallel_mode2(
        self,
        check_refs: list[SQLCheckReference],
        concurrency: int,
    ) -> list[CheckResult]:
        """Execute checks in parallel. Every adapter involved is a PooledAdapter.

        Phase 1: Materialize all needed tables to Arrow in parallel.
        Phase 2: Run checks in parallel. Mode 2 checks run on independent
        local DuckDB instances. Mode 1 checks run on instances leased from
        their pool.
        """
        from concurrent.futures import ThreadPoolExecutor

        import ibis

        from vowl.executors.security import SQLSecurityError, validate_query_security

        # Resolve each check's tables to the adapters that serve them. An
        # undeclared table resolves through the check's own schema, so the
        # same name can come from different adapters for different checks.
        check_table_map: list[tuple[SQLCheckReference, set[str], dict[str, BaseAdapter | None], bool]] = []
        needed: dict[tuple[str, int], tuple[str, BaseAdapter | None]] = {}
        for ref in check_refs:
            raw_query = ref.get_check().get("query") or ""
            tables = self._detect_tables(raw_query)
            owner_schema = self._owner_schema(ref)
            sources = {tbl: self._resolve_adapter(tbl, owner_schema) for tbl in tables}
            native = self._native_route(tables, owner_schema) is not None
            check_table_map.append((ref, tables, sources, native))
            if native:
                # Mode 1 runs natively, so nothing to download
                continue
            for tbl, adapter in sources.items():
                needed[(tbl, id(adapter))] = (tbl, adapter)

        # Phase 1: Materialize tables in parallel
        arrow_tables: dict[tuple[str, int], Any] = {}
        materialization_errors: dict[tuple[str, int], str] = {}

        def materialize_table(key: tuple[str, int]) -> tuple[tuple[str, int], Any, str | None]:
            table_name, adapter = needed[key]
            if adapter is None:
                return (key, None, f"No adapter configured for table '{table_name}'")
            try:
                arrow_table = adapter.export_table_as_arrow(table_name)
                return (key, arrow_table, None)
            except Exception as e:
                return (key, None, str(e))

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(materialize_table, key) for key in needed]
            for future in futures:
                key, arrow_table, error = future.result()
                if error:
                    materialization_errors[key] = error
                else:
                    arrow_tables[key] = arrow_table

        # Phase 2: Run checks in parallel on isolated DuckDB instances
        results: list[CheckResult | None] = [None] * len(check_refs)

        def run_check_isolated(
            index: int,
            ref: SQLCheckReference,
            tables: set[str],
            sources: dict[str, BaseAdapter | None],
        ) -> None:
            start_time = time.perf_counter()

            # Check for materialization failures
            for tbl in tables:
                key = (tbl, id(sources[tbl]))
                if key in materialization_errors:
                    results[index] = ref.build_error_result(
                        error_message=(f"Materialization failed for table '{tbl}': {materialization_errors[key]}"),
                        execution_time_ms=(time.perf_counter() - start_time) * 1000,
                        dialect="duckdb",
                        multi_source=True,
                    )
                    return

            try:
                local_con = ibis.duckdb.connect()
                try:
                    from vowl.executors.security import sanitize_identifier

                    for tbl in tables:
                        sanitize_identifier(tbl)
                        local_con.create_table(tbl, arrow_tables[(tbl, id(sources[tbl]))], overwrite=True)

                    output_dialect = "duckdb"
                    use_try_cast = self._use_try_cast
                    scalar_query = ref.get_scalar_query(
                        output_dialect,
                        None,
                        use_try_cast=use_try_cast,
                    )

                    if not scalar_query:
                        results[index] = ref.build_error_result(
                            error_message="No query specified for SQL check",
                            execution_time_ms=(time.perf_counter() - start_time) * 1000,
                            dialect=output_dialect,
                            multi_source=True,
                        )
                        return

                    try:
                        validate_query_security(scalar_query, dialect=output_dialect)
                        result = local_con.raw_sql(scalar_query).fetchone()
                        actual_value = result[0] if result else None
                    except SQLSecurityError as sec_error:
                        results[index] = ref.build_error_result(
                            error_message=f"Security validation failed: {sec_error}",
                            execution_time_ms=(time.perf_counter() - start_time) * 1000,
                            dialect=output_dialect,
                            use_try_cast=use_try_cast,
                            multi_source=True,
                            security_violation=sec_error.violation_type,
                        )
                        return

                    failed_query = ref.get_failed_rows_query(
                        output_dialect,
                        None,
                        use_try_cast=use_try_cast,
                    )
                    max_rows = getattr(self._multi_adapter, "max_failed_rows", 1000)

                    def fetcher(q=failed_query, con=local_con, max_r=max_rows):
                        if not q:
                            return None
                        q = self._with_row_cap(q, max_r, "duckdb")
                        try:
                            validate_query_security(q, dialect="duckdb")
                            r = con.raw_sql(q)
                            if hasattr(r, "to_arrow_table"):
                                at = r.to_arrow_table()
                            else:
                                at = r.fetch_arrow_table()
                            at = self._deduplicate_arrow_columns(at)
                            return nw.from_native(at, eager_only=True)
                        except Exception:
                            return None

                    built = ref.build_result(
                        actual_value=actual_value,
                        execution_time_ms=(time.perf_counter() - start_time) * 1000,
                        failed_rows_fetcher=fetcher,
                        dialect=output_dialect,
                        filter_conditions=None,
                        use_try_cast=use_try_cast,
                    )
                    if built.status != "ERROR":
                        built.row_source = RowSource(
                            check_ref=ref,
                            dialect=output_dialect,
                            use_try_cast=use_try_cast,
                            failed_rows_query=failed_query,
                            fetch=fetcher,
                            cross_source=True,
                        )
                    results[index] = built
                except Exception as e:
                    results[index] = ref.build_error_result(
                        error_message=f"Error executing cross-schema check: {e}",
                        execution_time_ms=(time.perf_counter() - start_time) * 1000,
                        dialect="duckdb",
                        multi_source=True,
                    )
            except Exception as e:
                results[index] = ref.build_error_result(
                    error_message=f"Error executing cross-schema check: {e}",
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                    dialect="duckdb",
                    multi_source=True,
                )

        def run_check_native(index: int, ref: SQLCheckReference) -> None:
            # run_single_check makes the same native decision and leases a
            # pooled instance for the check
            results[index] = self.run_single_check(ref)

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = []
            for i, (ref, tables, sources, native) in enumerate(check_table_map):
                if native:
                    futures.append(pool.submit(run_check_native, i, ref))
                else:
                    futures.append(pool.submit(run_check_isolated, i, ref, tables, sources))
            for future in futures:
                future.result()

        return [r for r in results if r is not None]

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
