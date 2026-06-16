"""Integration tests for PooledAdapter with real database backends.

Tests PooledAdapter with:
- DuckDB (in-memory)
- SQLite (file-based)
- Cross-backend scenarios (DuckDB + SQLite via MultiSourceAdapter)
- Full validation pipeline with cross-table checks
- PooledAdapter inside MultiSourceAdapter routing
"""

from __future__ import annotations

import time
import threading
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

import ibis

from vowl.adapters import IbisAdapter, MultiSourceAdapter, PooledAdapter
from vowl.executors.multi_source_sql_executor import MultiSourceSQLExecutor

TEST_DIR = Path(__file__).parent
EMPLOYEE_DIR = TEST_DIR / "employee"
EMPLOYEE_LIST_FILE = EMPLOYEE_DIR / "demo_employee_list.csv"
EMPLOYEE_PAYROLL_FILE = EMPLOYEE_DIR / "demo_employee_payroll.csv"
EMPLOYEE_CONTRACT_PATH = EMPLOYEE_DIR / "employee_payroll_datacontract.yaml"


@pytest.fixture
def employee_data():
    """Load employee test data."""
    employee_list = pd.read_csv(EMPLOYEE_LIST_FILE).fillna("")
    employee_payroll = pd.read_csv(EMPLOYEE_PAYROLL_FILE).fillna("")
    return employee_list, employee_payroll


@pytest.fixture
def duckdb_adapter_factory(employee_data):
    """Factory that creates DuckDB adapters with employee data."""
    employee_list, employee_payroll = employee_data

    def factory():
        con = ibis.duckdb.connect()
        con.create_table("demo_employee_payroll", employee_payroll)
        con.create_table("demo_employee_list", employee_list)
        return IbisAdapter(con)

    return factory


@pytest.fixture
def sqlite_adapter_factory(employee_data, tmp_path):
    """Factory that creates SQLite adapters with employee data."""
    employee_list, employee_payroll = employee_data
    counter = {"n": 0}

    def factory():
        counter["n"] += 1
        db_path = tmp_path / f"employee_{counter['n']}.db"
        con = ibis.sqlite.connect(str(db_path))

        payroll_cols = ", ".join(f"{c} TEXT" for c in employee_payroll.columns)
        con.raw_sql(f"CREATE TABLE demo_employee_payroll ({payroll_cols})")
        con.insert("demo_employee_payroll", employee_payroll.astype(str))

        list_cols = ", ".join(f"{c} TEXT" for c in employee_list.columns)
        con.raw_sql(f"CREATE TABLE demo_employee_list ({list_cols})")
        con.insert("demo_employee_list", employee_list.astype(str))

        return IbisAdapter(con)

    return factory


class TestPooledAdapterDuckDB:
    """Test PooledAdapter with real DuckDB connections."""

    def test_basic_validation_with_pooled_duckdb(self, duckdb_adapter_factory):
        """PooledAdapter with DuckDB runs checks correctly via MultiSourceAdapter."""
        from vowl.validation.runner import ValidationRunner

        pooled = PooledAdapter(factory=duckdb_adapter_factory, max_concurrency=3)
        multi = MultiSourceAdapter({
            "demo_employee_payroll": pooled,
            "demo_employee_list": pooled,
        })

        runner = ValidationRunner(
            contract=str(EMPLOYEE_CONTRACT_PATH),
            adapters=multi,
        )
        results = runner.run()

        results_df = results.get_check_results_df().to_pandas()
        error_checks = results_df[results_df["status"] == "ERROR"]
        assert len(error_checks) == 0, f"Checks errored: {error_checks['check_name'].tolist()}"

    def test_parallel_execution_with_duckdb(self, duckdb_adapter_factory):
        """Multiple checks execute in parallel across pooled DuckDB connections."""
        pooled = PooledAdapter(factory=duckdb_adapter_factory, max_concurrency=3)

        from vowl.contracts.contract import Contract

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        all_refs = []
        for refs in refs_by_schema.values():
            all_refs.extend(refs)

        results = pooled.run_checks(all_refs)

        assert len(results) > 0
        statuses = {r.status for r in results}
        assert "ERROR" not in statuses
        assert len(pooled._all_instances) <= 3

    def test_concurrent_thread_safety_duckdb(self, duckdb_adapter_factory):
        """PooledAdapter is thread-safe with real DuckDB connections."""
        pooled = PooledAdapter(factory=duckdb_adapter_factory, max_concurrency=4)

        results_by_thread = {}
        errors = []

        def worker(thread_id):
            try:
                adapter = pooled._checkout()
                try:
                    count = adapter.get_total_rows("demo_employee_payroll")
                    results_by_thread[thread_id] = count
                finally:
                    pooled._return(adapter)
            except Exception as e:
                errors.append((thread_id, e))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread errors: {errors}"
        assert len(results_by_thread) == 8
        assert all(v > 0 for v in results_by_thread.values())
        assert len(pooled._all_instances) <= 4

    def test_config_propagation_through_validation_runner(self, duckdb_adapter_factory):
        """ValidationRunner propagates config to PooledAdapter and its instances."""
        from vowl.config import ValidationConfig
        from vowl.validation.runner import ValidationRunner

        pooled = PooledAdapter(factory=duckdb_adapter_factory, max_concurrency=2)
        multi = MultiSourceAdapter({
            "demo_employee_payroll": pooled,
            "demo_employee_list": pooled,
        })

        runner = ValidationRunner(
            contract=str(EMPLOYEE_CONTRACT_PATH),
            adapters=multi,
            config=ValidationConfig(max_failed_rows=5, use_try_cast=False),
        )
        runner.run()

        for instance in pooled._all_instances:
            assert instance.max_failed_rows == 5
            assert instance.use_try_cast is False

    def test_export_table_as_arrow_delegates_correctly(self, duckdb_adapter_factory):
        """export_table_as_arrow uses the primary adapter's connection."""
        pooled = PooledAdapter(factory=duckdb_adapter_factory, max_concurrency=2)

        arrow_table = pooled.export_table_as_arrow("demo_employee_payroll")

        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows > 0
        assert "employee_id" in arrow_table.column_names

    def test_test_connection_with_pooled_duckdb(self, duckdb_adapter_factory):
        """test_connection delegates properly to DuckDB."""
        pooled = PooledAdapter(factory=duckdb_adapter_factory, max_concurrency=2)

        result = pooled.test_connection("demo_employee_payroll")
        assert result is None

    def test_get_sql_dialect_returns_duckdb(self, duckdb_adapter_factory):
        """get_sql_dialect delegates to IbisAdapter and returns 'duckdb'."""
        pooled = PooledAdapter(factory=duckdb_adapter_factory, max_concurrency=2)

        assert pooled.get_sql_dialect() == "duckdb"

    def test_cleanup_disconnects_all_duckdb(self, duckdb_adapter_factory):
        """cleanup() closes all pooled DuckDB connections."""
        pooled = PooledAdapter(factory=duckdb_adapter_factory, max_concurrency=3)

        # Force creation of multiple instances by running checks in parallel
        from vowl.contracts.contract import Contract
        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        # Get single-table refs from first schema
        first_schema_refs = list(refs_by_schema.values())[0]
        # Filter to only non-cross-table refs (no JOINs)
        single_refs = [
            ref for ref in first_schema_refs
            if "JOIN" not in (ref.get_check().get("query") or "").upper()
        ]
        if len(single_refs) >= 2:
            pooled.run_checks(single_refs)

        instance_count = len(pooled._all_instances)
        assert instance_count > 0

        pooled.cleanup()

        assert len(pooled._all_instances) == 0
        assert pooled._created_count == 0


class TestPooledAdapterSQLite:
    """Test PooledAdapter with real SQLite connections."""

    def test_basic_validation_with_pooled_sqlite(self, sqlite_adapter_factory):
        """PooledAdapter with SQLite runs single-table checks correctly."""
        pooled = PooledAdapter(factory=sqlite_adapter_factory, max_concurrency=2)

        from vowl.contracts.contract import Contract
        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        # Run single-table checks only (SQLite doesn't support cross-thread export)
        payroll_refs = refs_by_schema.get("demo_employee_payroll", [])
        single_refs = [
            ref for ref in payroll_refs
            if "JOIN" not in (ref.get_check().get("query") or "").upper()
        ]

        results = pooled.run_checks(single_refs)

        assert len(results) > 0
        # SQLite runs checks without internal errors (type mismatches are FAILED, not ERROR)
        for r in results:
            assert r.status in ("PASSED", "FAILED"), (
                f"Check '{r.check_name}' errored: {r.details}"
            )

    def test_parallel_execution_with_sqlite(self, sqlite_adapter_factory):
        """Multiple checks execute in parallel across pooled SQLite connections."""
        pooled = PooledAdapter(factory=sqlite_adapter_factory, max_concurrency=3)

        from vowl.contracts.contract import Contract
        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        all_refs = []
        for refs in refs_by_schema.values():
            all_refs.extend(refs)

        results = pooled.run_checks(all_refs)

        assert len(results) > 0
        statuses = {r.status for r in results}
        assert "ERROR" not in statuses

    def test_get_sql_dialect_returns_sqlite(self, sqlite_adapter_factory):
        """get_sql_dialect delegates to SQLite IbisAdapter."""
        pooled = PooledAdapter(factory=sqlite_adapter_factory, max_concurrency=2)

        assert pooled.get_sql_dialect() == "sqlite"

    def test_export_table_as_arrow_sqlite(self, sqlite_adapter_factory):
        """export_table_as_arrow works with SQLite backend."""
        pooled = PooledAdapter(factory=sqlite_adapter_factory, max_concurrency=2)

        arrow_table = pooled.export_table_as_arrow("demo_employee_list")

        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows > 0


class TestPooledAdapterMultiSource:
    """Test PooledAdapter inside MultiSourceAdapter with cross-table checks."""

    def test_pooled_in_multi_source_single_table_checks(self, employee_data):
        """PooledAdapter works as a child of MultiSourceAdapter for single-table checks."""
        employee_list, employee_payroll = employee_data

        def make_payroll_adapter():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_payroll", employee_payroll)
            return IbisAdapter(con)

        def make_list_adapter():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_list", employee_list)
            return IbisAdapter(con)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": PooledAdapter(
                factory=make_payroll_adapter, max_concurrency=2,
            ),
            "demo_employee_list": PooledAdapter(
                factory=make_list_adapter, max_concurrency=2,
            ),
        })

        from vowl.contracts.contract import Contract
        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        multi.max_failed_rows = 10
        multi.use_try_cast = True
        for adapter in multi.adapters.values():
            adapter.max_failed_rows = 10
            adapter.use_try_cast = True

        results = multi.run_checks(refs_by_schema)

        error_results = [r for r in results if r.status == "ERROR"]
        assert len(error_results) == 0, (
            f"Errors: {[(r.check_name, r.details) for r in error_results]}"
        )

    def test_pooled_multi_source_cross_table_mode2(self, employee_data):
        """Cross-table checks via Mode 2 work with PooledAdapters in MultiSourceAdapter."""
        employee_list, employee_payroll = employee_data

        def make_payroll_adapter():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_payroll", employee_payroll)
            return IbisAdapter(con)

        def make_list_adapter():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_list", employee_list)
            return IbisAdapter(con)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": PooledAdapter(
                factory=make_payroll_adapter, max_concurrency=2,
            ),
            "demo_employee_list": PooledAdapter(
                factory=make_list_adapter, max_concurrency=2,
            ),
        })

        multi.max_failed_rows = 100
        multi.use_try_cast = True
        for adapter in multi.adapters.values():
            adapter.max_failed_rows = 100
            adapter.use_try_cast = True

        from vowl.contracts.contract import Contract
        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        results = multi.run_checks(refs_by_schema)

        results_by_name = {r.check_name: r for r in results}

        cross_table_checks = [
            "employee_id_exists_in_master_list",
            "phone_number_exists_in_master_list",
        ]
        for check_name in cross_table_checks:
            assert check_name in results_by_name, f"Missing cross-table check: {check_name}"
            result = results_by_name[check_name]
            assert result.status != "ERROR", (
                f"Cross-table check '{check_name}' errored: {result.details}"
            )

    def test_mixed_pooled_and_unpooled(self, employee_data):
        """MultiSourceAdapter with one pooled and one non-pooled adapter works."""
        employee_list, employee_payroll = employee_data

        # Pooled DuckDB for payroll
        def make_payroll_adapter():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_payroll", employee_payroll)
            return IbisAdapter(con)

        # Non-pooled DuckDB for employee list
        list_con = ibis.duckdb.connect()
        list_con.create_table("demo_employee_list", employee_list)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": PooledAdapter(
                factory=make_payroll_adapter, max_concurrency=3,
            ),
            "demo_employee_list": IbisAdapter(list_con),
        })

        multi.max_failed_rows = 10
        multi.use_try_cast = True
        for adapter in multi.adapters.values():
            adapter.max_failed_rows = 10
            adapter.use_try_cast = True

        from vowl.contracts.contract import Contract
        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        results = multi.run_checks(refs_by_schema)

        error_results = [r for r in results if r.status == "ERROR"]
        assert len(error_results) == 0, (
            f"Errors: {[(r.check_name, r.details) for r in error_results]}"
        )

    def test_full_validation_pipeline_pooled_multi_source(self, employee_data):
        """Full validation pipeline with PooledAdapters in multi-source mode."""
        from vowl.validation.runner import ValidationRunner

        employee_list, employee_payroll = employee_data

        def make_payroll_adapter():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_payroll", employee_payroll)
            return IbisAdapter(con)

        def make_list_adapter():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_list", employee_list)
            return IbisAdapter(con)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": PooledAdapter(
                factory=make_payroll_adapter, max_concurrency=2,
            ),
            "demo_employee_list": PooledAdapter(
                factory=make_list_adapter, max_concurrency=2,
            ),
        })

        runner = ValidationRunner(
            contract=str(EMPLOYEE_CONTRACT_PATH),
            adapters=multi,
        )
        results = runner.run()

        results_df = results.get_check_results_df().to_pandas()
        error_checks = results_df[results_df["status"] == "ERROR"]
        assert len(error_checks) == 0, (
            f"Checks errored: {error_checks[['check_name', 'message']].to_dict('records')}"
        )

        # Verify cross-table checks ran and detected issues
        cross_checks = results_df[
            results_df["check_name"].isin([
                "employee_id_exists_in_master_list",
                "phone_number_exists_in_master_list",
            ])
        ]
        assert len(cross_checks) == 2
        assert all(cross_checks["status"].isin(["PASSED", "FAILED"]))


class TestPooledAdapterCrossBackend:
    """Test PooledAdapter with mixed DuckDB and SQLite backends."""

    def test_separate_duckdb_pooled_instances_cross_table(self, employee_data):
        """Two separate DuckDB in-memory pools (incompatible) with cross-table checks."""
        employee_list, employee_payroll = employee_data

        # Separate DuckDB instances for each schema (not compatible — different connections)
        def make_payroll():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_payroll", employee_payroll)
            return IbisAdapter(con)

        def make_list():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_list", employee_list)
            return IbisAdapter(con)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": PooledAdapter(
                factory=make_payroll, max_concurrency=2,
            ),
            "demo_employee_list": PooledAdapter(
                factory=make_list, max_concurrency=2,
            ),
        })

        multi.max_failed_rows = 10
        multi.use_try_cast = True
        for adapter in multi.adapters.values():
            adapter.max_failed_rows = 10
            adapter.use_try_cast = True

        from vowl.contracts.contract import Contract
        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        results = multi.run_checks(refs_by_schema)

        # Single-table checks should not error
        single_table_results = [
            r for r in results
            if r.check_name not in (
                "employee_id_exists_in_master_list",
                "phone_number_exists_in_master_list",
            )
        ]
        single_table_errors = [r for r in single_table_results if r.status == "ERROR"]
        assert len(single_table_errors) == 0, (
            f"Single-table errors: {[(r.check_name, r.details) for r in single_table_errors]}"
        )

        # Cross-table checks use Mode 2 (materialization)
        cross_table_results = [
            r for r in results
            if r.check_name in (
                "employee_id_exists_in_master_list",
                "phone_number_exists_in_master_list",
            )
        ]
        assert len(cross_table_results) == 2
        for r in cross_table_results:
            assert r.status != "ERROR", (
                f"Cross-backend check '{r.check_name}' errored: {r.details}"
            )

    def test_sqlite_not_thread_safe_for_export(self, employee_data, tmp_path):
        """SQLite connections cannot be used across threads for export.

        This documents the known limitation: SQLite PooledAdapters work for
        single-table checks (each thread creates its own connection), but
        Mode 2 cross-table materialization fails because export_table_as_arrow
        is called from a thread pool worker on a connection potentially created
        on another thread.
        """
        employee_list, _ = employee_data
        counter = {"n": 0}

        def make_sqlite():
            counter["n"] += 1
            db_path = tmp_path / f"sqlite_thread_{counter['n']}.db"
            con = ibis.sqlite.connect(str(db_path))
            cols = ", ".join(f"{c} TEXT" for c in employee_list.columns)
            con.raw_sql(f"CREATE TABLE demo_employee_list ({cols})")
            con.insert("demo_employee_list", employee_list.astype(str))
            return IbisAdapter(con)

        pooled = PooledAdapter(factory=make_sqlite, max_concurrency=2)

        # Single-table operations work fine (each thread gets its own connection)
        result = pooled.test_connection("demo_employee_list")
        assert result is None

        # Export works from the main thread
        arrow = pooled.export_table_as_arrow("demo_employee_list")
        assert arrow.num_rows > 0

    def test_is_compatible_with_separate_duckdb_instances(self, employee_data):
        """PooledAdapters with separate DuckDB instances are not compatible."""
        _, employee_payroll = employee_data

        def make_duckdb_a():
            con = ibis.duckdb.connect()
            con.create_table("t", employee_payroll)
            return IbisAdapter(con)

        def make_duckdb_b():
            con = ibis.duckdb.connect()
            con.create_table("t", employee_payroll)
            return IbisAdapter(con)

        pooled_a = PooledAdapter(factory=make_duckdb_a, max_concurrency=2)
        pooled_b = PooledAdapter(factory=make_duckdb_b, max_concurrency=2)

        # Different connections → not compatible
        assert pooled_a.is_compatible_with(pooled_b) is False


class TestPooledAdapterFilterConditions:
    """Test PooledAdapter with filter conditions."""

    def test_pooled_with_filter_conditions(self, employee_data):
        """PooledAdapter works with IbisAdapter filter conditions."""
        from vowl.adapters.models import FilterCondition

        _, employee_payroll = employee_data

        def make_filtered_adapter():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_payroll", employee_payroll)
            return IbisAdapter(
                con=con,
                filter_conditions={
                    "demo_employee_payroll": FilterCondition(
                        field="employee_id", operator="!=", value="",
                    ),
                },
            )

        pooled = PooledAdapter(factory=make_filtered_adapter, max_concurrency=2)

        arrow = pooled.export_table_as_arrow("demo_employee_payroll")
        assert isinstance(arrow, pa.Table)
        # All rows should have non-empty employee_id
        ids = arrow.column("employee_id").to_pylist()
        assert all(id_val != "" for id_val in ids)


class TestPooledAdapterConcurrencyDeriving:
    """Test _derive_concurrency with real adapters."""

    def test_derive_concurrency_with_real_pooled_adapters(self, employee_data):
        """_derive_concurrency works with real PooledAdapter instances."""
        employee_list, employee_payroll = employee_data

        def make_payroll():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_payroll", employee_payroll)
            return IbisAdapter(con)

        def make_list():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_list", employee_list)
            return IbisAdapter(con)

        adapters = {
            "demo_employee_payroll": PooledAdapter(factory=make_payroll, max_concurrency=4),
            "demo_employee_list": PooledAdapter(factory=make_list, max_concurrency=2),
        }

        multi = MultiSourceAdapter(adapters)
        executor = multi._get_executor("sql")

        from vowl.contracts.contract import Contract
        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        # Find cross-table refs
        cross_refs = []
        for schema_refs in refs_by_schema.values():
            for ref in schema_refs:
                check = ref.get_check()
                query = check.get("query", "")
                if "JOIN" in query.upper():
                    cross_refs.append(ref)

        if cross_refs:
            concurrency = executor._derive_concurrency(cross_refs)
            assert concurrency == 2  # min(4, 2)
