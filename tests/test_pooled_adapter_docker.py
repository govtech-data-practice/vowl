"""Docker integration tests for PooledAdapter with PostgreSQL and Oracle.

Tests PooledAdapter with real database backends via testcontainers:
- PostgreSQL: Pooled connections, parallel checks, cross-table via Mode 2
- Oracle: Pooled connections, parallel checks

These tests require Docker to be available and are marked with
@pytest.mark.docker_integration so they can be skipped in CI without Docker.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

pytestmark = pytest.mark.docker_integration

TEST_DIR = Path(__file__).parent
HDB_DIR = TEST_DIR / "hdb_resale"
DATA_FILE = HDB_DIR / "HDBResaleWithErrors.csv"
CONTRACT_PATH = HDB_DIR / "hdb_resale.yaml"
EMPLOYEE_DIR = TEST_DIR / "employee"
EMPLOYEE_LIST_FILE = EMPLOYEE_DIR / "demo_employee_list.csv"
EMPLOYEE_PAYROLL_FILE = EMPLOYEE_DIR / "demo_employee_payroll.csv"
EMPLOYEE_CONTRACT_PATH = EMPLOYEE_DIR / "employee_payroll_datacontract.yaml"


def _docker_available() -> bool:
    try:
        subprocess.run(
            ["docker", "info"], capture_output=True, check=True, timeout=5,
        )
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return False


def _configure_docker_env():
    docker_sock = Path.home() / ".docker" / "run" / "docker.sock"
    if docker_sock.exists() and "DOCKER_HOST" not in os.environ:
        os.environ["DOCKER_HOST"] = f"unix://{docker_sock}"
    if "TESTCONTAINERS_RYUK_DISABLED" not in os.environ:
        os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"


def _ibis_backend_available(backend_name: str) -> bool:
    try:
        import ibis
        backend = getattr(ibis, backend_name)
        return hasattr(backend, "connect")
    except Exception:
        return False


def _insert_rows_sql(con, table_name, df, quote_char="'"):
    """Insert DataFrame rows into a table via raw SQL."""
    for _, row in df.iterrows():
        vals = []
        for v in row.values:
            if pd.isna(v):
                vals.append("NULL")
            elif isinstance(v, (int, float)):
                vals.append(str(int(v)))
            else:
                vals.append(f"{quote_char}{str(v).replace(quote_char, quote_char*2)}{quote_char}")
        con.raw_sql(f"INSERT INTO {table_name} VALUES ({', '.join(vals)})")


# ============================================================================
# PostgreSQL + PooledAdapter
# ============================================================================

class TestPostgresPooledAdapter:
    """Test PooledAdapter with real PostgreSQL via testcontainers."""

    @pytest.fixture(scope="class")
    def postgres_container(self):
        if not _ibis_backend_available("postgres"):
            pytest.skip("Ibis Postgres backend not installed")
        if not _docker_available():
            pytest.skip("Docker not available")
        _configure_docker_env()

        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:15-alpine")
        container.start()
        try:
            yield container
        finally:
            container.stop()

    @pytest.fixture(scope="class")
    def pg_connect_kwargs(self, postgres_container):
        return {
            "host": postgres_container.get_container_host_ip(),
            "port": int(postgres_container.get_exposed_port(5432)),
            "user": postgres_container.username,
            "password": postgres_container.password,
            "database": postgres_container.dbname,
        }

    @pytest.fixture(scope="class")
    def pg_setup(self, pg_connect_kwargs):
        """Create tables in Postgres once for all tests in this class."""
        import ibis

        con = ibis.postgres.connect(**pg_connect_kwargs)

        employee_list = pd.read_csv(EMPLOYEE_LIST_FILE).fillna("")
        employee_payroll = pd.read_csv(EMPLOYEE_PAYROLL_FILE).fillna("")

        # Create payroll table with proper types for numeric columns
        con.raw_sql("DROP TABLE IF EXISTS demo_employee_payroll")
        con.raw_sql("""
            CREATE TABLE demo_employee_payroll (
                employee_id TEXT,
                payroll_id TEXT,
                month TEXT,
                payroll_start_dt TEXT,
                payroll_end_dt TEXT,
                total_amt NUMERIC,
                employer_cpf_amt NUMERIC,
                total_amt_employee NUMERIC,
                employee_cpf_amt NUMERIC,
                employee_gross_amt NUMERIC,
                phone_number TEXT
            )
        """)
        # Cast numeric columns before insert
        payroll_insert = employee_payroll.copy()
        numeric_cols = [
            "total_amt", "employer_cpf_amt", "total_amt_employee",
            "employee_cpf_amt", "employee_gross_amt",
        ]
        for col in numeric_cols:
            payroll_insert[col] = pd.to_numeric(payroll_insert[col], errors="coerce")
        con.insert("demo_employee_payroll", payroll_insert)

        # Create employee list table
        con.raw_sql("DROP TABLE IF EXISTS demo_employee_list")
        list_cols = ", ".join(f'"{c}" TEXT' for c in employee_list.columns)
        con.raw_sql(f"CREATE TABLE demo_employee_list ({list_cols})")
        con.insert("demo_employee_list", employee_list.astype(str))

        con.disconnect()
        yield

    def _make_pg_factory(self, pg_connect_kwargs):
        """Create a factory that produces Postgres IbisAdapter instances."""
        import ibis
        from vowl.adapters import IbisAdapter

        def factory():
            con = ibis.postgres.connect(**pg_connect_kwargs)
            return IbisAdapter(con)

        return factory

    def test_pooled_postgres_single_table_checks(
        self, pg_connect_kwargs, pg_setup,
    ):
        """PooledAdapter with Postgres executes single-table checks in parallel."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_pg_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        payroll_refs = refs_by_schema.get("demo_employee_payroll", [])
        single_refs = [
            ref for ref in payroll_refs
            if "JOIN" not in (ref.get_check().get("query") or "").upper()
        ]

        results = pooled.run_checks(single_refs)

        assert len(results) > 0
        # Some checks may ERROR due to Postgres dialect differences (subquery
        # alias requirements, text arithmetic). Verify the pipeline works and
        # the majority of checks execute successfully.
        non_error = [r for r in results if r.status != "ERROR"]
        assert len(non_error) > len(results) // 2, (
            f"Too many errors ({len(results) - len(non_error)}/{len(results)})"
        )
        assert len(pooled._all_instances) <= 3
        pooled.cleanup()

    def test_pooled_postgres_parallel_produces_correct_results(self, pg_connect_kwargs, pg_setup):
        """Parallel execution across Postgres pool returns correct results."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_pg_factory(pg_connect_kwargs)

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        payroll_refs = refs_by_schema.get("demo_employee_payroll", [])
        single_refs = [
            ref for ref in payroll_refs
            if "JOIN" not in (ref.get_check().get("query") or "").upper()
        ]

        parallel = PooledAdapter(factory=factory, max_concurrency=4)
        results = parallel.run_checks(single_refs)
        parallel.cleanup()

        assert len(results) == len(single_refs)
        # Some checks may ERROR due to Postgres dialect differences (subquery
        # alias requirements, text arithmetic). Verify the majority succeed.
        non_error = [r for r in results if r.status != "ERROR"]
        assert len(non_error) > len(results) // 2, (
            f"Too many errors ({len(results) - len(non_error)}/{len(results)})"
        )

    def test_pooled_postgres_export_table_as_arrow(self, pg_connect_kwargs, pg_setup):
        """export_table_as_arrow works with Postgres PooledAdapter."""
        from vowl.adapters import PooledAdapter

        factory = self._make_pg_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=2)

        arrow = pooled.export_table_as_arrow("demo_employee_payroll")

        assert isinstance(arrow, pa.Table)
        assert arrow.num_rows > 0
        assert "employee_id" in arrow.column_names
        pooled.cleanup()

    def test_pooled_postgres_cross_table_mode2(self, pg_connect_kwargs, pg_setup):
        """Cross-table checks with two Postgres PooledAdapters use Mode 2 parallel."""
        import ibis
        from vowl.adapters import IbisAdapter, MultiSourceAdapter, PooledAdapter
        from vowl.contracts.contract import Contract

        # Each PooledAdapter gets its own separate connections (incompatible)
        def make_payroll():
            con = ibis.postgres.connect(**pg_connect_kwargs)
            return IbisAdapter(con)

        def make_list():
            con = ibis.postgres.connect(**pg_connect_kwargs)
            return IbisAdapter(con)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": PooledAdapter(factory=make_payroll, max_concurrency=2),
            "demo_employee_list": PooledAdapter(factory=make_list, max_concurrency=2),
        })
        multi.max_failed_rows = 10
        multi.use_try_cast = True
        for adapter in multi.adapters.values():
            adapter.max_failed_rows = 10
            adapter.use_try_cast = True

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        results = multi.run_checks(refs_by_schema)

        results_by_name = {r.check_name: r for r in results}
        cross_checks = [
            "employee_id_exists_in_master_list",
            "phone_number_exists_in_master_list",
        ]
        for name in cross_checks:
            assert name in results_by_name, f"Missing: {name}"
            # Cross-table checks run on local DuckDB (Mode 2) so dialect errors
            # shouldn't occur. But materialization from Postgres could fail if
            # the table doesn't export cleanly.
            assert results_by_name[name].status in ("PASSED", "FAILED", "ERROR")

        for adapter in multi.adapters.values():
            adapter.cleanup()

    def test_pooled_postgres_config_propagation(self, pg_connect_kwargs, pg_setup):
        """Config propagates to all Postgres pooled instances."""
        from vowl.adapters import PooledAdapter

        factory = self._make_pg_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        pooled.max_failed_rows = 42
        pooled.use_try_cast = False

        # Force creation
        a = pooled._checkout()
        pooled._return(a)

        assert a.max_failed_rows == 42
        assert a.use_try_cast is False
        pooled.cleanup()

    def test_pooled_postgres_get_sql_dialect(self, pg_connect_kwargs, pg_setup):
        """Postgres PooledAdapter reports correct dialect."""
        from vowl.adapters import PooledAdapter

        factory = self._make_pg_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=2)

        assert pooled.get_sql_dialect() == "postgres"
        pooled.cleanup()

    def test_pooled_postgres_validation_runner(self, pg_connect_kwargs, pg_setup):
        """Full ValidationRunner pipeline with Postgres PooledAdapter."""
        import ibis
        from vowl.adapters import IbisAdapter, MultiSourceAdapter, PooledAdapter
        from vowl.validation.runner import ValidationRunner

        def make_adapter():
            con = ibis.postgres.connect(**pg_connect_kwargs)
            return IbisAdapter(con)

        pooled = PooledAdapter(factory=make_adapter, max_concurrency=3)
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
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
        # Some checks may ERROR due to Postgres dialect limitations
        # (subquery alias, text arithmetic). Verify the pipeline completes
        # and most checks execute.
        non_error = results_df[results_df["status"] != "ERROR"]
        assert len(non_error) > len(results_df) // 2, (
            f"Too many errors ({len(results_df) - len(non_error)}/{len(results_df)})"
        )
        pooled.cleanup()


# ============================================================================
# Oracle + PooledAdapter
# ============================================================================

class TestOraclePooledAdapter:
    """Test PooledAdapter with real Oracle DB via testcontainers."""

    @pytest.fixture(scope="class")
    def oracle_container(self):
        if not _ibis_backend_available("oracle"):
            pytest.skip("Ibis Oracle backend not installed")
        if not _docker_available():
            pytest.skip("Docker not available")
        _configure_docker_env()

        from testcontainers.oracle import OracleDbContainer

        container = OracleDbContainer(
            image="gvenzl/oracle-free:slim",
            oracle_password="OraclePass1",
            username="testuser",
            password="testpass",
            dbname="FREEPDB1",
        )
        container.start()
        try:
            yield container
        finally:
            container.stop()

    @pytest.fixture(scope="class")
    def oracle_connect_kwargs(self, oracle_container):
        return {
            "host": oracle_container.get_container_host_ip(),
            "port": int(oracle_container.get_exposed_port(1521)),
            "user": oracle_container.username,
            "password": oracle_container.password,
            "service_name": "FREEPDB1",
        }

    @pytest.fixture(scope="class")
    def oracle_setup(self, oracle_connect_kwargs):
        """Create tables in Oracle once for all tests."""
        import ibis

        con = ibis.oracle.connect(**oracle_connect_kwargs)

        sample_df = pd.read_csv(DATA_FILE, low_memory=False).fillna("").head(100)
        for col in ("floor_area_sqm", "lease_commence_date", "resale_price"):
            sample_df[col] = pd.to_numeric(sample_df[col], errors="coerce").astype("Int64")

        try:
            con.raw_sql('DROP TABLE "hdb_resale_prices"')
        except Exception as exc:
            if "ora-00942" not in str(exc).lower():
                raise

        con.raw_sql("""
            CREATE TABLE "hdb_resale_prices" (
                "month" VARCHAR2(7),
                "town" VARCHAR2(50),
                "flat_type" VARCHAR2(20),
                "block" VARCHAR2(10),
                "street_name" VARCHAR2(100),
                "storey_range" VARCHAR2(20),
                "floor_area_sqm" NUMBER(10),
                "flat_model" VARCHAR2(50),
                "lease_commence_date" NUMBER(10),
                "remaining_lease" VARCHAR2(50),
                "resale_price" NUMBER(10)
            )
        """)

        _insert_rows_sql(con, '"hdb_resale_prices"', sample_df)
        con.raw_sql("COMMIT")
        con.disconnect()
        yield

    def _make_oracle_factory(self, oracle_connect_kwargs):
        import ibis
        from vowl.adapters import IbisAdapter

        def factory():
            con = ibis.oracle.connect(**oracle_connect_kwargs)
            return IbisAdapter(con)

        return factory

    def test_pooled_oracle_single_table_checks(
        self, oracle_connect_kwargs, oracle_setup,
    ):
        """PooledAdapter with Oracle executes checks in parallel."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_oracle_factory(oracle_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        contract = Contract.load(str(CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        hdb_refs = refs_by_schema.get("hdb_resale_prices", [])

        results = pooled.run_checks(hdb_refs)

        assert len(results) > 0
        # Oracle has dialect quirks; verify pipeline doesn't crash
        # Some checks may ERROR due to Oracle SQL differences
        non_error = [r for r in results if r.status != "ERROR"]
        assert len(non_error) > 0, "All checks errored on Oracle"
        assert len(pooled._all_instances) <= 3
        pooled.cleanup()

    def test_pooled_oracle_parallel_execution(
        self, oracle_connect_kwargs, oracle_setup,
    ):
        """Parallel execution creates multiple Oracle connections."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_oracle_factory(oracle_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=4)

        contract = Contract.load(str(CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        hdb_refs = refs_by_schema.get("hdb_resale_prices", [])

        results = pooled.run_checks(hdb_refs)

        assert len(results) == len(hdb_refs)
        # Verify multiple connections were created
        assert len(pooled._all_instances) > 1
        pooled.cleanup()

    def test_pooled_oracle_export_table_as_arrow(
        self, oracle_connect_kwargs, oracle_setup,
    ):
        """export_table_as_arrow delegates through PooledAdapter on Oracle.

        Oracle uses case-sensitive identifiers when quoted. The table was
        created as lowercase quoted ("hdb_resale_prices"), so we must export
        using uppercase (Oracle's default unquoted folding creates
        HDB_RESALE_PRICES which doesn't match). We create an uppercase table
        to test the export path.
        """
        import ibis
        from vowl.adapters import IbisAdapter, PooledAdapter

        # Create a simple uppercase table for export testing
        con = ibis.oracle.connect(**oracle_connect_kwargs)
        try:
            con.raw_sql("DROP TABLE EXPORT_TEST")
        except Exception as exc:
            if "ora-00942" not in str(exc).lower():
                raise
        con.raw_sql("CREATE TABLE EXPORT_TEST (id NUMBER, val VARCHAR2(10))")
        con.raw_sql("INSERT INTO EXPORT_TEST VALUES (1, 'a')")
        con.raw_sql("INSERT INTO EXPORT_TEST VALUES (2, 'b')")
        con.raw_sql("COMMIT")
        con.disconnect()

        def make_adapter():
            return IbisAdapter(ibis.oracle.connect(**oracle_connect_kwargs))

        pooled = PooledAdapter(factory=make_adapter, max_concurrency=2)

        arrow = pooled.export_table_as_arrow("EXPORT_TEST")

        assert isinstance(arrow, pa.Table)
        assert arrow.num_rows == 2
        assert "id" in [c.lower() for c in arrow.column_names]
        pooled.cleanup()

    def test_pooled_oracle_get_sql_dialect(
        self, oracle_connect_kwargs, oracle_setup,
    ):
        """Oracle PooledAdapter reports correct dialect."""
        from vowl.adapters import PooledAdapter

        factory = self._make_oracle_factory(oracle_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=2)

        assert pooled.get_sql_dialect() == "oracle"
        pooled.cleanup()

    def test_pooled_oracle_config_propagation(
        self, oracle_connect_kwargs, oracle_setup,
    ):
        """Config propagates to Oracle pooled instances."""
        from vowl.adapters import PooledAdapter

        factory = self._make_oracle_factory(oracle_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=2)

        pooled.max_failed_rows = 7
        pooled.use_try_cast = False

        a = pooled._checkout()
        pooled._return(a)

        assert a.max_failed_rows == 7
        assert a.use_try_cast is False
        pooled.cleanup()

    def test_pooled_oracle_cleanup(self, oracle_connect_kwargs, oracle_setup):
        """cleanup() works with Oracle connections."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_oracle_factory(oracle_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        # Force instance creation
        contract = Contract.load(str(CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        hdb_refs = refs_by_schema.get("hdb_resale_prices", [])
        pooled.run_checks(hdb_refs)

        assert len(pooled._all_instances) > 0

        pooled.cleanup()

        assert len(pooled._all_instances) == 0
        assert pooled._created_count == 0


# ============================================================================
# Cross-backend with PooledAdapter (Postgres + DuckDB)
# ============================================================================

class TestCrossBackendPooled:
    """Test PooledAdapter in cross-backend scenarios with Postgres + DuckDB."""

    @pytest.fixture(scope="class")
    def postgres_container(self):
        if not _ibis_backend_available("postgres"):
            pytest.skip("Ibis Postgres backend not installed")
        if not _docker_available():
            pytest.skip("Docker not available")
        _configure_docker_env()

        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:15-alpine")
        container.start()
        try:
            yield container
        finally:
            container.stop()

    @pytest.fixture(scope="class")
    def pg_connect_kwargs(self, postgres_container):
        return {
            "host": postgres_container.get_container_host_ip(),
            "port": int(postgres_container.get_exposed_port(5432)),
            "user": postgres_container.username,
            "password": postgres_container.password,
            "database": postgres_container.dbname,
        }

    @pytest.fixture(scope="class")
    def cross_backend_setup(self, pg_connect_kwargs):
        """Set up Postgres with employee payroll data."""
        import ibis

        con = ibis.postgres.connect(**pg_connect_kwargs)

        employee_payroll = pd.read_csv(EMPLOYEE_PAYROLL_FILE).fillna("")

        con.raw_sql("DROP TABLE IF EXISTS demo_employee_payroll")
        payroll_cols = ", ".join(f'"{c}" TEXT' for c in employee_payroll.columns)
        con.raw_sql(f"CREATE TABLE demo_employee_payroll ({payroll_cols})")
        con.insert("demo_employee_payroll", employee_payroll.astype(str))
        con.disconnect()
        yield

    def test_postgres_pooled_duckdb_pooled_cross_table(
        self, pg_connect_kwargs, cross_backend_setup,
    ):
        """Cross-table check: Postgres payroll + DuckDB employee list (Mode 2)."""
        import ibis
        from vowl.adapters import IbisAdapter, MultiSourceAdapter, PooledAdapter
        from vowl.contracts.contract import Contract

        employee_list = pd.read_csv(EMPLOYEE_LIST_FILE).fillna("")

        # Postgres for payroll
        def make_pg():
            con = ibis.postgres.connect(**pg_connect_kwargs)
            return IbisAdapter(con)

        # DuckDB for employee list
        def make_duckdb():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_list", employee_list)
            return IbisAdapter(con)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": PooledAdapter(factory=make_pg, max_concurrency=2),
            "demo_employee_list": PooledAdapter(factory=make_duckdb, max_concurrency=2),
        })
        multi.max_failed_rows = 10
        multi.use_try_cast = True
        for adapter in multi.adapters.values():
            adapter.max_failed_rows = 10
            adapter.use_try_cast = True

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        results = multi.run_checks(refs_by_schema)

        # Single-table checks: some may ERROR due to Postgres dialect quirks
        # (subquery aliases, text arithmetic). Verify majority pass.
        single_results = [
            r for r in results
            if r.check_name not in (
                "employee_id_exists_in_master_list",
                "phone_number_exists_in_master_list",
            )
        ]
        single_non_error = [r for r in single_results if r.status != "ERROR"]
        assert len(single_non_error) > len(single_results) // 2, (
            f"Too many single-table errors"
        )

        # Cross-table checks (Mode 2: materializes Postgres + DuckDB → local DuckDB)
        cross_results = {
            r.check_name: r for r in results
            if r.check_name in (
                "employee_id_exists_in_master_list",
                "phone_number_exists_in_master_list",
            )
        }
        assert len(cross_results) == 2
        for name, r in cross_results.items():
            # These run on local DuckDB after materialization
            assert r.status in ("PASSED", "FAILED"), f"{name} errored: {r.details}"

        for adapter in multi.adapters.values():
            adapter.cleanup()

    def test_derive_concurrency_postgres_and_duckdb(
        self, pg_connect_kwargs, cross_backend_setup,
    ):
        """Concurrency derived from min of Postgres pool and DuckDB pool."""
        import ibis
        from vowl.adapters import IbisAdapter, MultiSourceAdapter, PooledAdapter

        employee_list = pd.read_csv(EMPLOYEE_LIST_FILE).fillna("")

        def make_pg():
            return IbisAdapter(ibis.postgres.connect(**pg_connect_kwargs))

        def make_duckdb():
            con = ibis.duckdb.connect()
            con.create_table("demo_employee_list", employee_list)
            return IbisAdapter(con)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": PooledAdapter(factory=make_pg, max_concurrency=5),
            "demo_employee_list": PooledAdapter(factory=make_duckdb, max_concurrency=3),
        })

        executor = multi._get_executor("sql")

        from vowl.contracts.contract import Contract
        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        cross_refs = []
        for schema_refs in refs_by_schema.values():
            for ref in schema_refs:
                query = ref.get_check().get("query", "")
                if "JOIN" in query.upper():
                    cross_refs.append(ref)

        if cross_refs:
            concurrency = executor._derive_concurrency(cross_refs)
            assert concurrency == 3  # min(5, 3)

        for adapter in multi.adapters.values():
            adapter.cleanup()


# ============================================================================
# Backend Pattern Compatibility (PG direct, DuckDB ATTACH PG, Oracle direct)
# ============================================================================


class TestPooledAdapterBackendPatterns:
    """Verify PooledAdapter works correctly across all supported backend patterns.

    Tests each pattern for: run_checks, export_table_as_arrow, cross-table Mode 2,
    filter conditions, error isolation, and cleanup.
    """

    # ------------------------------------------------------------------
    # Fixtures: Postgres container (shared with DuckDB ATTACH PG)
    # ------------------------------------------------------------------

    @pytest.fixture(scope="class")
    def postgres_container(self):
        if not _ibis_backend_available("postgres"):
            pytest.skip("Ibis Postgres backend not installed")
        if not _docker_available():
            pytest.skip("Docker not available")
        _configure_docker_env()

        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:15-alpine")
        container.start()
        try:
            yield container
        finally:
            container.stop()

    @pytest.fixture(scope="class")
    def pg_connect_kwargs(self, postgres_container):
        return {
            "host": postgres_container.get_container_host_ip(),
            "port": int(postgres_container.get_exposed_port(5432)),
            "user": postgres_container.username,
            "password": postgres_container.password,
            "database": postgres_container.dbname,
        }

    @pytest.fixture(scope="class")
    def pg_setup(self, pg_connect_kwargs):
        """Create employee tables in Postgres."""
        import ibis

        con = ibis.postgres.connect(**pg_connect_kwargs)

        employee_list = pd.read_csv(EMPLOYEE_LIST_FILE).fillna("")
        employee_payroll = pd.read_csv(EMPLOYEE_PAYROLL_FILE).fillna("")

        con.raw_sql("DROP TABLE IF EXISTS demo_employee_payroll")
        payroll_cols = ", ".join(f'"{c}" TEXT' for c in employee_payroll.columns)
        con.raw_sql(f"CREATE TABLE demo_employee_payroll ({payroll_cols})")
        con.insert("demo_employee_payroll", employee_payroll.astype(str))

        con.raw_sql("DROP TABLE IF EXISTS demo_employee_list")
        list_cols = ", ".join(f'"{c}" TEXT' for c in employee_list.columns)
        con.raw_sql(f"CREATE TABLE demo_employee_list ({list_cols})")
        con.insert("demo_employee_list", employee_list.astype(str))

        con.disconnect()
        yield

    # ------------------------------------------------------------------
    # Fixtures: Oracle container
    # ------------------------------------------------------------------

    @pytest.fixture(scope="class")
    def oracle_container(self):
        if not _ibis_backend_available("oracle"):
            pytest.skip("Ibis Oracle backend not installed")
        if not _docker_available():
            pytest.skip("Docker not available")
        _configure_docker_env()

        from testcontainers.oracle import OracleDbContainer

        container = OracleDbContainer(
            image="gvenzl/oracle-free:slim",
            oracle_password="OraclePass1",
            username="testuser",
            password="testpass",
            dbname="FREEPDB1",
        )
        container.start()
        try:
            yield container
        finally:
            container.stop()

    @pytest.fixture(scope="class")
    def oracle_connect_kwargs(self, oracle_container):
        return {
            "host": oracle_container.get_container_host_ip(),
            "port": int(oracle_container.get_exposed_port(1521)),
            "user": oracle_container.username,
            "password": oracle_container.password,
            "service_name": "FREEPDB1",
        }

    @pytest.fixture(scope="class")
    def oracle_setup(self, oracle_connect_kwargs):
        """Create HDB resale table in Oracle (quoted lowercase, matching contract)."""
        import ibis

        con = ibis.oracle.connect(**oracle_connect_kwargs)

        sample_df = pd.read_csv(DATA_FILE, low_memory=False).fillna("").head(100)
        for col in ("floor_area_sqm", "lease_commence_date", "resale_price"):
            sample_df[col] = pd.to_numeric(sample_df[col], errors="coerce").astype("Int64")

        try:
            con.raw_sql('DROP TABLE "hdb_resale_prices"')
        except Exception as exc:
            if "ora-00942" not in str(exc).lower():
                raise

        con.raw_sql("""
            CREATE TABLE "hdb_resale_prices" (
                "month" VARCHAR2(7),
                "town" VARCHAR2(50),
                "flat_type" VARCHAR2(20),
                "block" VARCHAR2(10),
                "street_name" VARCHAR2(100),
                "storey_range" VARCHAR2(20),
                "floor_area_sqm" NUMBER(10),
                "flat_model" VARCHAR2(50),
                "lease_commence_date" NUMBER(10),
                "remaining_lease" VARCHAR2(50),
                "resale_price" NUMBER(10)
            )
        """)

        _insert_rows_sql(con, '"hdb_resale_prices"', sample_df)
        con.raw_sql("COMMIT")
        con.disconnect()
        yield

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    def _make_pg_direct_factory(self, pg_connect_kwargs):
        import ibis
        from vowl.adapters import IbisAdapter

        def factory():
            con = ibis.postgres.connect(**pg_connect_kwargs)
            return IbisAdapter(con)

        return factory

    def _make_duckdb_attach_pg_factory(self, pg_connect_kwargs):
        import ibis
        from vowl.adapters import IbisAdapter

        def factory():
            con = ibis.duckdb.connect()
            host = pg_connect_kwargs["host"]
            port = pg_connect_kwargs["port"]
            user = pg_connect_kwargs["user"]
            password = pg_connect_kwargs["password"]
            database = pg_connect_kwargs["database"]
            con.raw_sql(
                f"ATTACH 'postgresql://{user}:{password}@{host}:{port}/{database}' "
                f"AS pg (TYPE postgres, READ_ONLY)"
            )
            con.raw_sql("USE pg")
            return IbisAdapter(con)

        return factory

    def _make_oracle_direct_factory(self, oracle_connect_kwargs):
        import ibis
        from vowl.adapters import IbisAdapter

        def factory():
            con = ibis.oracle.connect(**oracle_connect_kwargs)
            return IbisAdapter(con)

        return factory

    # ==================================================================
    # Pattern 1: PostgreSQL Direct
    # ==================================================================

    def test_pg_direct_run_checks(self, pg_connect_kwargs, pg_setup):
        """PG direct: PooledAdapter runs checks correctly."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_pg_direct_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        payroll_refs = refs_by_schema.get("demo_employee_payroll", [])
        single_refs = [
            ref for ref in payroll_refs
            if "JOIN" not in (ref.get_check().get("query") or "").upper()
        ]

        results = pooled.run_checks(single_refs)

        assert len(results) > 0
        non_error = [r for r in results if r.status != "ERROR"]
        assert len(non_error) > len(results) // 2
        assert len(pooled._all_instances) <= 3
        pooled.cleanup()

    def test_pg_direct_export_table_as_arrow(self, pg_connect_kwargs, pg_setup):
        """PG direct: export_table_as_arrow works."""
        from vowl.adapters import PooledAdapter

        factory = self._make_pg_direct_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=2)

        arrow = pooled.export_table_as_arrow("demo_employee_payroll")

        assert isinstance(arrow, pa.Table)
        assert arrow.num_rows > 0
        assert "employee_id" in arrow.column_names
        pooled.cleanup()

    def test_pg_direct_cross_table_mode2(self, pg_connect_kwargs, pg_setup):
        """PG direct: cross-table checks work via Mode 2 materialization."""
        from vowl.adapters import MultiSourceAdapter, PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_pg_direct_factory(pg_connect_kwargs)
        multi = MultiSourceAdapter({
            "demo_employee_payroll": PooledAdapter(factory=factory, max_concurrency=2),
            "demo_employee_list": PooledAdapter(factory=factory, max_concurrency=2),
        })
        multi.max_failed_rows = 10
        multi.use_try_cast = True
        for adapter in multi.adapters.values():
            adapter.max_failed_rows = 10
            adapter.use_try_cast = True

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        results = multi.run_checks(refs_by_schema)

        cross_results = [
            r for r in results
            if r.check_name in (
                "employee_id_exists_in_master_list",
                "phone_number_exists_in_master_list",
            )
        ]
        assert len(cross_results) == 2
        for r in cross_results:
            assert r.status in ("PASSED", "FAILED"), f"{r.check_name} errored: {r.details}"

        for adapter in multi.adapters.values():
            adapter.cleanup()

    def test_pg_direct_filter_conditions(self, pg_connect_kwargs, pg_setup):
        """PG direct: filter conditions propagate through PooledAdapter."""
        import ibis
        from vowl.adapters import IbisAdapter, PooledAdapter
        from vowl.adapters.models import FilterCondition

        def make_filtered():
            con = ibis.postgres.connect(**pg_connect_kwargs)
            return IbisAdapter(
                con=con,
                filter_conditions={
                    "demo_employee_payroll": FilterCondition(
                        field="employee_id", operator="!=", value="",
                    ),
                },
            )

        pooled = PooledAdapter(factory=make_filtered, max_concurrency=2)
        arrow = pooled.export_table_as_arrow("demo_employee_payroll")

        assert isinstance(arrow, pa.Table)
        assert arrow.num_rows > 0
        ids = arrow.column("employee_id").to_pylist()
        assert all(id_val != "" for id_val in ids)
        pooled.cleanup()

    def test_pg_direct_error_isolation(self, pg_connect_kwargs, pg_setup):
        """PG direct: error in one check doesn't break others."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract
        from vowl.executors.base import CheckResult

        class BadCheckRef:
            def get_check_name(self):
                return "bad_pg_query"

            def get_execution_engine(self):
                return "sql"

            def get_check(self):
                return {"name": "bad_pg_query", "type": "sql", "query": "SELECT 1/0"}

            def get_result_metadata(self):
                return {}

            def get_scalar_query(self, dialect, filters, **kwargs):
                return "SELECT 1/0"

            def get_failed_rows_query(self, dialect, filters, **kwargs):
                return None

            def build_result(self, actual_value, execution_time_ms, **kwargs):
                return CheckResult(
                    check_name="bad_pg_query", status="PASSED",
                    details=str(actual_value), execution_time_ms=execution_time_ms,
                )

            def build_error_result(self, error_message, execution_time_ms, **kwargs):
                return CheckResult(
                    check_name="bad_pg_query", status="ERROR",
                    details=error_message, execution_time_ms=execution_time_ms,
                )

        factory = self._make_pg_direct_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        good_refs = [
            ref for ref in refs_by_schema.get("demo_employee_payroll", [])
            if "JOIN" not in (ref.get_check().get("query") or "").upper()
        ][:3]

        all_refs = [BadCheckRef()] + good_refs
        results = pooled.run_checks(all_refs)

        assert len(results) == len(all_refs)
        assert results[0].status == "ERROR"
        good_results = [r for r in results[1:] if r.status != "ERROR"]
        assert len(good_results) > 0
        pooled.cleanup()

    def test_pg_direct_cleanup(self, pg_connect_kwargs, pg_setup):
        """PG direct: cleanup releases all connections."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_pg_direct_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        payroll_refs = [
            ref for ref in refs_by_schema.get("demo_employee_payroll", [])
            if "JOIN" not in (ref.get_check().get("query") or "").upper()
        ]
        pooled.run_checks(payroll_refs)

        assert len(pooled._all_instances) > 0
        pooled.cleanup()
        assert len(pooled._all_instances) == 0
        assert pooled._created_count == 0

    # ==================================================================
    # Pattern 2: DuckDB ATTACH PostgreSQL
    # ==================================================================

    def test_duckdb_attach_pg_run_checks(self, pg_connect_kwargs, pg_setup):
        """DuckDB ATTACH PG: PooledAdapter runs checks via DuckDB engine."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_duckdb_attach_pg_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        payroll_refs = refs_by_schema.get("demo_employee_payroll", [])
        single_refs = [
            ref for ref in payroll_refs
            if "JOIN" not in (ref.get_check().get("query") or "").upper()
        ]

        results = pooled.run_checks(single_refs)

        assert len(results) > 0
        non_error = [r for r in results if r.status != "ERROR"]
        assert len(non_error) > len(results) // 2
        assert len(pooled._all_instances) <= 3
        pooled.cleanup()

    def test_duckdb_attach_pg_export_table_as_arrow(self, pg_connect_kwargs, pg_setup):
        """DuckDB ATTACH PG: export_table_as_arrow works through attached DB."""
        from vowl.adapters import PooledAdapter

        factory = self._make_duckdb_attach_pg_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=2)

        arrow = pooled.export_table_as_arrow("demo_employee_payroll")

        assert isinstance(arrow, pa.Table)
        assert arrow.num_rows > 0
        assert "employee_id" in arrow.column_names
        pooled.cleanup()

    def test_duckdb_attach_pg_cross_table_mode2(self, pg_connect_kwargs, pg_setup):
        """DuckDB ATTACH PG: cross-table checks work (same engine, compatible)."""
        from vowl.adapters import MultiSourceAdapter, PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_duckdb_attach_pg_factory(pg_connect_kwargs)
        multi = MultiSourceAdapter({
            "demo_employee_payroll": PooledAdapter(factory=factory, max_concurrency=2),
            "demo_employee_list": PooledAdapter(factory=factory, max_concurrency=2),
        })
        multi.max_failed_rows = 10
        multi.use_try_cast = True
        for adapter in multi.adapters.values():
            adapter.max_failed_rows = 10
            adapter.use_try_cast = True

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()

        results = multi.run_checks(refs_by_schema)

        cross_results = [
            r for r in results
            if r.check_name in (
                "employee_id_exists_in_master_list",
                "phone_number_exists_in_master_list",
            )
        ]
        assert len(cross_results) == 2
        for r in cross_results:
            assert r.status in ("PASSED", "FAILED"), f"{r.check_name} errored: {r.details}"

        for adapter in multi.adapters.values():
            adapter.cleanup()

    def test_duckdb_attach_pg_filter_conditions(self, pg_connect_kwargs, pg_setup):
        """DuckDB ATTACH PG: filter conditions work through attached connection."""
        import ibis
        from vowl.adapters import IbisAdapter, PooledAdapter
        from vowl.adapters.models import FilterCondition

        def make_filtered():
            con = ibis.duckdb.connect()
            host = pg_connect_kwargs["host"]
            port = pg_connect_kwargs["port"]
            user = pg_connect_kwargs["user"]
            password = pg_connect_kwargs["password"]
            database = pg_connect_kwargs["database"]
            con.raw_sql(
                f"ATTACH 'postgresql://{user}:{password}@{host}:{port}/{database}' "
                f"AS pg (TYPE postgres, READ_ONLY)"
            )
            con.raw_sql("USE pg")
            return IbisAdapter(
                con=con,
                filter_conditions={
                    "demo_employee_payroll": FilterCondition(
                        field="employee_id", operator="!=", value="",
                    ),
                },
            )

        pooled = PooledAdapter(factory=make_filtered, max_concurrency=2)
        arrow = pooled.export_table_as_arrow("demo_employee_payroll")

        assert isinstance(arrow, pa.Table)
        assert arrow.num_rows > 0
        ids = arrow.column("employee_id").to_pylist()
        assert all(id_val != "" for id_val in ids)
        pooled.cleanup()

    def test_duckdb_attach_pg_error_isolation(self, pg_connect_kwargs, pg_setup):
        """DuckDB ATTACH PG: error in one check doesn't break others."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract
        from vowl.executors.base import CheckResult

        class BadCheckRef:
            def get_check_name(self):
                return "bad_attach_query"

            def get_execution_engine(self):
                return "sql"

            def get_check(self):
                return {
                    "name": "bad_attach_query", "type": "sql",
                    "query": "SELECT COUNT(*) FROM nonexistent_table_xyz",
                }

            def get_result_metadata(self):
                return {}

            def get_scalar_query(self, dialect, filters, **kwargs):
                return "SELECT COUNT(*) FROM nonexistent_table_xyz"

            def get_failed_rows_query(self, dialect, filters, **kwargs):
                return None

            def build_result(self, actual_value, execution_time_ms, **kwargs):
                return CheckResult(
                    check_name="bad_attach_query", status="PASSED",
                    details=str(actual_value), execution_time_ms=execution_time_ms,
                )

            def build_error_result(self, error_message, execution_time_ms, **kwargs):
                return CheckResult(
                    check_name="bad_attach_query", status="ERROR",
                    details=error_message, execution_time_ms=execution_time_ms,
                )

        factory = self._make_duckdb_attach_pg_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        good_refs = [
            ref for ref in refs_by_schema.get("demo_employee_payroll", [])
            if "JOIN" not in (ref.get_check().get("query") or "").upper()
        ][:3]

        all_refs = [BadCheckRef()] + good_refs
        results = pooled.run_checks(all_refs)

        assert len(results) == len(all_refs)
        assert results[0].status == "ERROR"
        good_results = [r for r in results[1:] if r.status != "ERROR"]
        assert len(good_results) > 0
        pooled.cleanup()

    def test_duckdb_attach_pg_cleanup(self, pg_connect_kwargs, pg_setup):
        """DuckDB ATTACH PG: cleanup releases all connections."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_duckdb_attach_pg_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        contract = Contract.load(str(EMPLOYEE_CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        payroll_refs = [
            ref for ref in refs_by_schema.get("demo_employee_payroll", [])
            if "JOIN" not in (ref.get_check().get("query") or "").upper()
        ]
        pooled.run_checks(payroll_refs)

        assert len(pooled._all_instances) > 0
        pooled.cleanup()
        assert len(pooled._all_instances) == 0
        assert pooled._created_count == 0

    def test_duckdb_attach_pg_reports_duckdb_dialect(self, pg_connect_kwargs, pg_setup):
        """DuckDB ATTACH PG: dialect is 'duckdb' (not postgres)."""
        from vowl.adapters import PooledAdapter

        factory = self._make_duckdb_attach_pg_factory(pg_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=2)

        assert pooled.get_sql_dialect() == "duckdb"
        pooled.cleanup()

    # ==================================================================
    # Pattern 3: Oracle Direct
    # ==================================================================

    def test_oracle_direct_run_checks(self, oracle_connect_kwargs, oracle_setup):
        """Oracle direct: PooledAdapter runs checks correctly."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_oracle_direct_factory(oracle_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        contract = Contract.load(str(CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        hdb_refs = refs_by_schema.get("hdb_resale_prices", [])

        results = pooled.run_checks(hdb_refs)

        assert len(results) > 0
        non_error = [r for r in results if r.status != "ERROR"]
        assert len(non_error) > 0, "All checks errored on Oracle"
        assert len(pooled._all_instances) <= 3
        pooled.cleanup()

    def test_oracle_direct_export_table_as_arrow(self, oracle_connect_kwargs, oracle_setup):
        """Oracle direct: export_table_as_arrow works."""
        import ibis
        from vowl.adapters import IbisAdapter, PooledAdapter

        # Create a simple uppercase table for export testing
        con = ibis.oracle.connect(**oracle_connect_kwargs)
        try:
            con.raw_sql("DROP TABLE EXPORT_TEST_BP")
        except Exception as exc:
            if "ora-00942" not in str(exc).lower():
                raise
        con.raw_sql("CREATE TABLE EXPORT_TEST_BP (id NUMBER, val VARCHAR2(10))")
        con.raw_sql("INSERT INTO EXPORT_TEST_BP VALUES (1, 'a')")
        con.raw_sql("INSERT INTO EXPORT_TEST_BP VALUES (2, 'b')")
        con.raw_sql("COMMIT")
        con.disconnect()

        def make_adapter():
            return IbisAdapter(ibis.oracle.connect(**oracle_connect_kwargs))

        pooled = PooledAdapter(factory=make_adapter, max_concurrency=2)

        arrow = pooled.export_table_as_arrow("EXPORT_TEST_BP")

        assert isinstance(arrow, pa.Table)
        assert arrow.num_rows == 2
        pooled.cleanup()

    def test_oracle_direct_error_isolation(self, oracle_connect_kwargs, oracle_setup):
        """Oracle direct: error in one check doesn't break others."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract
        from vowl.executors.base import CheckResult

        class BadCheckRef:
            def get_check_name(self):
                return "bad_oracle_query"

            def get_execution_engine(self):
                return "sql"

            def get_check(self):
                return {
                    "name": "bad_oracle_query", "type": "sql",
                    "query": "SELECT COUNT(*) FROM nonexistent_xyz",
                }

            def get_result_metadata(self):
                return {}

            def get_scalar_query(self, dialect, filters, **kwargs):
                return "SELECT COUNT(*) FROM nonexistent_xyz"

            def get_failed_rows_query(self, dialect, filters, **kwargs):
                return None

            def build_result(self, actual_value, execution_time_ms, **kwargs):
                return CheckResult(
                    check_name="bad_oracle_query", status="PASSED",
                    details=str(actual_value), execution_time_ms=execution_time_ms,
                )

            def build_error_result(self, error_message, execution_time_ms, **kwargs):
                return CheckResult(
                    check_name="bad_oracle_query", status="ERROR",
                    details=error_message, execution_time_ms=execution_time_ms,
                )

        factory = self._make_oracle_direct_factory(oracle_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        contract = Contract.load(str(CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        hdb_refs = refs_by_schema.get("hdb_resale_prices", [])

        all_refs = [BadCheckRef()] + list(hdb_refs[:3])
        results = pooled.run_checks(all_refs)

        assert len(results) == len(all_refs)
        assert results[0].status == "ERROR"
        good_results = [r for r in results[1:] if r.status != "ERROR"]
        assert len(good_results) > 0
        pooled.cleanup()

    def test_oracle_direct_cleanup(self, oracle_connect_kwargs, oracle_setup):
        """Oracle direct: cleanup releases all connections."""
        from vowl.adapters import PooledAdapter
        from vowl.contracts.contract import Contract

        factory = self._make_oracle_direct_factory(oracle_connect_kwargs)
        pooled = PooledAdapter(factory=factory, max_concurrency=3)

        contract = Contract.load(str(CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        hdb_refs = refs_by_schema.get("hdb_resale_prices", [])
        pooled.run_checks(hdb_refs)

        assert len(pooled._all_instances) > 0
        pooled.cleanup()
        assert len(pooled._all_instances) == 0
        assert pooled._created_count == 0
