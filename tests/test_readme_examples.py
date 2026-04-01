"""End-to-end tests for every code example in README.md and examples/.

Covers:
  - examples/basic_usage.py (Validate in 3 lines)
  - Local DataFrame (Pandas / Polars)
  - PySpark integration
  - Ibis DuckDB connection
  - DuckDB ATTACH compatibility mode (PostgreSQL via testcontainers)
  - Explicit adapter with filter conditions
  - Multi-source validation (DuckDB ATTACH & multi-adapter)
  - Custom adapters and executors
  - ValidationResult API (chaining, save, get_output_dfs, etc.)
  - Contract API (load, get_server, resolve, check references)
  - Server definitions from contracts
  - Single adapter expanding to all schemas (notebook pattern)

Database-backed tests use testcontainers (Docker required) and are
marked with ``pytest.mark.docker_integration``.

Run all (including Docker tests):
    pytest tests/test_readme_examples.py -v

Run only non-Docker tests:
    pytest tests/test_readme_examples.py -v -m "not docker_integration"
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import narwhals as nw
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_DIR = Path(__file__).parent
HDB_DIR = TEST_DIR / "hdb_resale"
DATA_FILE = HDB_DIR / "HDBResaleWithErrors.csv"
CLEAN_DATA_FILE = HDB_DIR / "HDBResale.csv"
CONTRACT_PATH = HDB_DIR / "hdb_resale.yaml"
SIMPLE_CONTRACT_PATH = HDB_DIR / "hdb_resale_simple.yaml"

PSD_DIR = TEST_DIR / "psd_employee"
PSD_EMPLOYEE_LIST_FILE = PSD_DIR / "demo_employee_list.csv"
PSD_EMPLOYEE_PAYROLL_FILE = PSD_DIR / "demo_employee_payroll.csv"
PSD_CONTRACT_PATH = PSD_DIR / "employee_payroll_datacontract.yaml"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True, timeout=5)
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return False


def _configure_docker_env():
    docker_sock = Path.home() / ".docker" / "run" / "docker.sock"
    if docker_sock.exists() and "DOCKER_HOST" not in os.environ:
        os.environ["DOCKER_HOST"] = f"unix://{docker_sock}"
    if "TESTCONTAINERS_RYUK_DISABLED" not in os.environ:
        os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"


def _assert_no_error_checks(result) -> None:
    """Fail if any check returned ERROR status."""
    results_df = result.get_check_results_df().to_pandas()
    errors = results_df[results_df["status"].str.upper() == "ERROR"]
    if not errors.empty:
        details = errors[["check_name", "message"]].to_string(index=False)
        raise AssertionError(f"Validation returned ERROR checks:\n{details}")


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.read_csv(DATA_FILE, low_memory=False)


@pytest.fixture
def small_df(sample_df) -> pd.DataFrame:
    return sample_df.head(100)


@pytest.fixture
def contract_path() -> str:
    return str(CONTRACT_PATH)


# ============================================================================
# 1. examples/basic_usage.py  — "Validate in 3 lines"
# ============================================================================

class TestBasicUsageExample:
    """Mirrors ``examples/basic_usage.py`` exactly."""

    def test_basic_usage_script(self, capsys):
        """Run the same code as examples/basic_usage.py."""
        from vowl import validate_data

        df = pd.read_csv(DATA_FILE)
        result = validate_data(contract=str(CONTRACT_PATH), df=df)
        result.display_full_report()

        captured = capsys.readouterr()
        assert "Data Quality Validation Results" in captured.out
        assert result is not None
        _assert_no_error_checks(result)


# ============================================================================
# 2. Local DataFrame — Pandas & Polars
# ============================================================================

class TestLocalDataFramePandas:
    """README: Local DataFrame (Pandas/Polars) — pandas path."""

    def test_pandas_validate_data(self, sample_df, contract_path):
        from vowl import validate_data

        result = validate_data(contract=contract_path, df=sample_df)

        assert result is not None
        assert isinstance(result.passed, bool)
        _assert_no_error_checks(result)

    def test_pandas_display_full_report(self, sample_df, contract_path, capsys):
        from vowl import validate_data

        result = validate_data(contract=contract_path, df=sample_df)
        chained = result.display_full_report()

        assert chained is result  # chaining
        captured = capsys.readouterr()
        assert len(captured.out) > 0


class TestLocalDataFramePolars:
    """README: Local DataFrame (Pandas/Polars) — Polars path."""

    def test_polars_validate_data(self, sample_df, contract_path):
        import polars as pl

        from vowl import validate_data

        polars_df = pl.from_pandas(sample_df.astype(str))
        result = validate_data(contract=contract_path, df=polars_df)

        assert result is not None
        assert isinstance(result.passed, bool)
        _assert_no_error_checks(result)


# ============================================================================
# 3. PySpark
# ============================================================================

class TestPySparkExample:
    """README: PySpark pattern."""

    @pytest.fixture
    def spark_session(self):
        if "JAVA_HOME" not in os.environ:
            homebrew_java = Path("/opt/homebrew/opt/openjdk@17")
            if homebrew_java.exists():
                os.environ["JAVA_HOME"] = str(homebrew_java)

        pytest.importorskip("pyspark", reason="PySpark not installed")
        from pyspark.sql import SparkSession

        try:
            spark = (
                SparkSession.builder
                .master("local[1]")
                .appName("test_readme_examples")
                .config("spark.driver.memory", "512m")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
        except Exception as e:
            pytest.skip(f"PySpark could not start: {e}")

        yield spark
        spark.stop()

    def test_pyspark_validate(self, spark_session, small_df, contract_path):
        from vowl import validate_data

        spark_df = spark_session.createDataFrame(small_df.astype(str))
        result = validate_data(contract=contract_path, df=spark_df)

        assert result is not None
        assert isinstance(result.passed, bool)
        _assert_no_error_checks(result)

    def test_pyspark_ibis_adapter(self, spark_session, small_df, contract_path):
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        spark_df = spark_session.createDataFrame(small_df.astype(str))
        spark_df.createOrReplaceTempView("hdb_resale_prices")

        con = ibis.pyspark.connect(session=spark_session)
        adapter = IbisAdapter(con)
        result = validate_data(contract=contract_path, adapter=adapter)

        assert result is not None
        assert len(result.get_check_results_df()) > 0
        _assert_no_error_checks(result)


# ============================================================================
# 4. Ibis Connections — DuckDB
# ============================================================================

class TestIbisDuckDB:
    """README: Ibis Connections (20+ Backends) — DuckDB in-memory."""

    def test_ibis_duckdb_validate(self, small_df, contract_path):
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table("hdb_resale_prices", small_df.astype(str))

        adapter = IbisAdapter(con)
        result = validate_data(contract=contract_path, adapter=adapter)

        assert result is not None
        assert len(result.get_check_results_df()) > 0
        _assert_no_error_checks(result)


# ============================================================================
# 5. Ibis Connections — PostgreSQL via testcontainers
# ============================================================================

@pytest.mark.docker_integration
class TestIbisPostgres:
    """README: ``ibis.postgres.connect(...)`` with a real Postgres container."""

    @pytest.fixture(scope="class")
    def postgres_container(self):
        if not _docker_available():
            pytest.skip("Docker not available")
        _configure_docker_env()

        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:15-alpine")
        container.start()
        yield container
        container.stop()

    @pytest.fixture
    def pg_connection(self, postgres_container):
        import ibis

        host = postgres_container.get_container_host_ip()
        port = int(postgres_container.get_exposed_port(5432))

        con = ibis.postgres.connect(
            host=host,
            port=port,
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
        )

        df = pd.read_csv(DATA_FILE, low_memory=False).head(100)

        con.raw_sql("""
            CREATE TABLE IF NOT EXISTS hdb_resale_prices (
                month TEXT, town TEXT, flat_type TEXT, block TEXT,
                street_name TEXT, storey_range TEXT, floor_area_sqm INTEGER,
                flat_model TEXT, lease_commence_date INTEGER, remaining_lease TEXT,
                resale_price INTEGER
            )
        """)
        con.raw_sql("TRUNCATE TABLE hdb_resale_prices")

        prep = df.copy()
        for col in ("floor_area_sqm", "lease_commence_date", "resale_price"):
            prep[col] = pd.to_numeric(prep[col], errors="coerce").astype("Int64")
        con.insert("hdb_resale_prices", prep)

        yield con

    def test_postgres_ibis_adapter(self, pg_connection, contract_path):
        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        result = validate_data(contract=contract_path, adapter=IbisAdapter(pg_connection))

        assert result is not None
        assert len(result.get_check_results_df()) > 0
        _assert_no_error_checks(result)

    def test_postgres_filter_conditions(self, pg_connection, contract_path):
        from vowl import validate_data
        from vowl.adapters import FilterCondition, IbisAdapter

        adapter = IbisAdapter(
            pg_connection,
            filter_conditions={
                "hdb_resale_prices": FilterCondition(
                    field="month", operator=">=", value="2017-01",
                ),
            },
        )
        result = validate_data(contract=contract_path, adapter=adapter)

        assert result is not None
        _assert_no_error_checks(result)


# ============================================================================
# 6. DuckDB ATTACH — Compatibility Mode (PostgreSQL via testcontainers)
# ============================================================================

@pytest.mark.docker_integration
class TestDuckDBAttach:
    """README: Compatibility Mode (DuckDB ATTACH)."""

    @pytest.fixture(scope="class")
    def postgres_container(self):
        if not _docker_available():
            pytest.skip("Docker not available")
        _configure_docker_env()

        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:15-alpine")
        container.start()
        yield container
        container.stop()

    @pytest.fixture
    def pg_url(self, postgres_container):
        host = postgres_container.get_container_host_ip()
        port = postgres_container.get_exposed_port(5432)
        user = postgres_container.username
        password = postgres_container.password
        db = postgres_container.dbname
        return f"postgresql://{user}:{password}@{host}:{port}/{db}"

    @pytest.fixture
    def seeded_pg_url(self, postgres_container, pg_url):
        """Seed Postgres with test data and return the connection URI."""
        import ibis

        host = postgres_container.get_container_host_ip()
        port = int(postgres_container.get_exposed_port(5432))

        con = ibis.postgres.connect(
            host=host,
            port=port,
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
        )

        con.raw_sql("""
            CREATE TABLE IF NOT EXISTS hdb_resale_prices (
                month TEXT, town TEXT, flat_type TEXT, block TEXT,
                street_name TEXT, storey_range TEXT, floor_area_sqm INTEGER,
                flat_model TEXT, lease_commence_date INTEGER, remaining_lease TEXT,
                resale_price INTEGER
            )
        """)
        con.raw_sql("TRUNCATE TABLE hdb_resale_prices")

        df = pd.read_csv(DATA_FILE, low_memory=False).head(100)
        for col in ("floor_area_sqm", "lease_commence_date", "resale_price"):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        con.insert("hdb_resale_prices", df)

        yield pg_url

    def test_duckdb_attach_postgres(self, seeded_pg_url, contract_path):
        """README: DuckDB ATTACH lets DuckDB query your remote database."""
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.raw_sql(f"ATTACH '{seeded_pg_url}' AS pg (TYPE postgres, READ_ONLY)")
        con.raw_sql("USE pg")

        result = validate_data(contract=contract_path, adapter=IbisAdapter(con))

        assert result is not None
        assert len(result.get_check_results_df()) > 0
        _assert_no_error_checks(result)


# ============================================================================
# 7. Explicit Adapter with Filter Conditions
# ============================================================================

class TestFilterConditions:
    """README: Explicit Adapter with Filter Conditions."""

    @pytest.fixture
    def duckdb_with_data(self, small_df):
        import ibis

        con = ibis.duckdb.connect()
        con.create_table("hdb_resale_prices", small_df.astype(str))
        return con

    def test_dict_style_filter(self, duckdb_with_data, contract_path):
        """Filter conditions using plain dicts."""
        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        adapter = IbisAdapter(
            duckdb_with_data,
            filter_conditions={
                "hdb_resale_prices": {
                    "field": "month",
                    "operator": ">=",
                    "value": "2017-01",
                }
            },
        )
        result = validate_data(contract=contract_path, adapter=adapter)

        assert result is not None
        _assert_no_error_checks(result)

    def test_filter_condition_class(self, duckdb_with_data, contract_path):
        """Filter conditions using FilterCondition dataclass."""
        from vowl import validate_data
        from vowl.adapters import FilterCondition, IbisAdapter

        adapter = IbisAdapter(
            duckdb_with_data,
            filter_conditions={
                "hdb_resale_prices": FilterCondition(
                    field="month", operator=">=", value="2017-01",
                )
            },
        )
        result = validate_data(contract=contract_path, adapter=adapter)

        assert result is not None
        _assert_no_error_checks(result)

    def test_multiple_conditions_list(self, duckdb_with_data, contract_path):
        """Multiple filter conditions combined with AND."""
        from vowl import validate_data
        from vowl.adapters import FilterCondition, IbisAdapter

        adapter = IbisAdapter(
            duckdb_with_data,
            filter_conditions={
                "hdb_resale_prices": [
                    FilterCondition(field="month", operator=">=", value="2017-01"),
                    FilterCondition(field="town", operator="=", value="ANG MO KIO"),
                ]
            },
        )
        result = validate_data(contract=contract_path, adapter=adapter)

        assert result is not None
        _assert_no_error_checks(result)

    def test_wildcard_patterns(self, duckdb_with_data):
        """Wildcard pattern matching in filter conditions."""
        from vowl.adapters import FilterCondition, IbisAdapter

        adapter = IbisAdapter(
            duckdb_with_data,
            filter_conditions={
                "hdb_*": FilterCondition(field="month", operator=">=", value="2017-01"),
                "*_prices": FilterCondition(field="town", operator="!=", value=""),
                "*": FilterCondition(field="resale_price", operator=">", value=0),
            },
        )

        assert "hdb_*" in adapter.filter_conditions
        assert "*_prices" in adapter.filter_conditions
        assert "*" in adapter.filter_conditions


# ============================================================================
# 8. Multi-Source Validation
# ============================================================================

class TestMultiSourceValidation:
    """README: Multi-Source Validation (Option B — multi-adapter)."""

    def test_multi_adapter_dict(self, small_df, contract_path):
        """Pass ``adapters={...}`` to validate_data."""
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table("hdb_resale_prices", small_df.astype(str))

        adapters = {"hdb_resale_prices": IbisAdapter(con)}
        result = validate_data(contract=contract_path, adapters=adapters)

        assert result is not None
        _assert_no_error_checks(result)

    def test_single_adapter_expands_to_all_schemas(self):
        """Notebook pattern: single adapter auto-expands to multiple schemas."""
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table("PSD_demo_employee_payroll", pd.read_csv(PSD_EMPLOYEE_PAYROLL_FILE))
        con.create_table("PSD_demo_employee_list", pd.read_csv(PSD_EMPLOYEE_LIST_FILE))

        adapter = IbisAdapter(con)

        with pytest.warns(UserWarning, match="only 1 input adapter provided"):
            result = validate_data(contract=str(PSD_CONTRACT_PATH), adapter=adapter)

        results_df = result.get_check_results_df().to_pandas()
        schemas = set(results_df["schema"].dropna().unique())
        assert "PSD_demo_employee_payroll" in schemas
        assert "PSD_demo_employee_list" in schemas
        _assert_no_error_checks(result)


@pytest.mark.docker_integration
class TestMultiSourceDuckDBAttach:
    """README: Multi-Source Validation — Option A (DuckDB ATTACH)."""

    @pytest.fixture(scope="class")
    def postgres_container(self):
        if not _docker_available():
            pytest.skip("Docker not available")
        _configure_docker_env()

        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:15-alpine")
        container.start()
        yield container
        container.stop()

    def test_duckdb_attach_multi_source_with_views(self, postgres_container):
        """DuckDB ATTACH + CREATE VIEW for prefix-free table names."""
        import ibis

        from vowl.adapters import IbisAdapter

        # Seed Postgres
        host = postgres_container.get_container_host_ip()
        port = int(postgres_container.get_exposed_port(5432))
        user = postgres_container.username
        pw = postgres_container.password
        db = postgres_container.dbname

        pg_con = ibis.postgres.connect(
            host=host, port=port, user=user, password=pw, database=db,
        )
        pg_con.raw_sql("""
            CREATE TABLE IF NOT EXISTS hdb_resale_prices (
                month TEXT, town TEXT, flat_type TEXT, block TEXT,
                street_name TEXT, storey_range TEXT, floor_area_sqm INTEGER,
                flat_model TEXT, lease_commence_date INTEGER,
                remaining_lease TEXT, resale_price INTEGER
            )
        """)
        pg_con.raw_sql("TRUNCATE TABLE hdb_resale_prices")
        df = pd.read_csv(DATA_FILE, low_memory=False).head(50)
        for col in ("floor_area_sqm", "lease_commence_date", "resale_price"):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        pg_con.insert("hdb_resale_prices", df)

        pg_url = f"postgresql://{user}:{pw}@{host}:{port}/{db}"

        # DuckDB ATTACH + views
        con = ibis.duckdb.connect()
        con.raw_sql(f"ATTACH '{pg_url}' AS pg_sales (TYPE postgres, READ_ONLY)")
        con.raw_sql("USE memory")
        con.raw_sql("CREATE VIEW hdb_resale_prices AS SELECT * FROM pg_sales.public.hdb_resale_prices")

        adapter = IbisAdapter(con)

        # Basic sanity: query through the view
        count = con.raw_sql("SELECT COUNT(*) FROM hdb_resale_prices").fetchone()[0]
        assert count == 50

        # Verify adapter connects
        assert adapter.test_connection("hdb_resale_prices") is None


# ============================================================================
# 9. Custom Adapters and Executors
# ============================================================================

class TestCustomAdaptersExecutors:
    """README: Custom Adapters and Executors boilerplate."""

    def test_custom_adapter_subclass(self, small_df):
        import ibis

        from vowl.adapters import BaseAdapter, IbisAdapter
        from vowl.executors import IbisSQLExecutor

        class CustomAdapter(BaseAdapter):
            def __init__(self, con, **kwargs):
                super().__init__(executors={"sql": IbisSQLExecutor})
                self._wrapped = IbisAdapter(con, **kwargs)

            def get_connection(self):
                return self._wrapped.get_connection()

            @property
            def filter_conditions(self):
                return self._wrapped.filter_conditions

            def test_connection(self, table_name: str):
                return self._wrapped.test_connection(table_name)

        con = ibis.duckdb.connect()
        con.create_table("hdb_resale_prices", small_df.astype(str))

        adapter = CustomAdapter(con)
        executors = adapter.get_executors()

        assert "sql" in executors
        assert executors["sql"] == IbisSQLExecutor


# ============================================================================
# 10. ValidationResult API
# ============================================================================

class TestValidationResultAPI:
    """README: The ValidationResult Object."""

    @pytest.fixture
    def result(self, sample_df, contract_path):
        from vowl import validate_data

        return validate_data(contract=contract_path, df=sample_df)

    def test_passed_property(self, result):
        assert isinstance(result.passed, bool)

    def test_print_summary_chaining(self, result, capsys):
        chained = result.print_summary()
        assert chained is result
        captured = capsys.readouterr()
        assert len(captured.out) > 0

    def test_show_failed_rows_chaining(self, result):
        chained = result.show_failed_rows(max_rows=2)
        assert chained is result

    def test_display_full_report_chaining(self, result, capsys):
        chained = result.display_full_report(max_rows=3)
        assert chained is result
        captured = capsys.readouterr()
        assert "Data Quality Validation Results" in captured.out

    def test_get_output_dfs(self, result):
        output_dfs = result.get_output_dfs()

        assert isinstance(output_dfs, dict)
        assert len(output_dfs) > 0
        for _check_id, df in output_dfs.items():
            assert isinstance(df, nw.DataFrame)
            assert "check_id" in df.columns
            assert "tables_in_query" in df.columns

    def test_get_consolidated_output_dfs(self, result):
        consolidated = result.get_consolidated_output_dfs()

        assert isinstance(consolidated, dict)
        for _table_name, df in consolidated.items():
            assert isinstance(df, nw.DataFrame)

    def test_get_check_results_df(self, result):
        results_df = result.get_check_results_df()

        assert isinstance(results_df, nw.DataFrame)
        assert len(results_df) > 0
        assert "check_name" in results_df.columns
        assert "status" in results_df.columns

    def test_save_results(self, result, tmp_path):
        original_dir = os.getcwd()
        os.chdir(tmp_path)
        try:
            chained = result.save(prefix="test_readme_results")
            assert chained is result

            files = list(tmp_path.iterdir())
            assert len(files) >= 1
        finally:
            os.chdir(original_dir)

    def test_summary_dict(self, result):
        assert "validation_summary" in result.summary
        vs = result.summary["validation_summary"]
        assert "total_checks" in vs
        assert "passed" in vs
        assert "failed" in vs


# ============================================================================
# 11. Contract API
# ============================================================================

class TestContractAPI:
    """README: Contract loading, schema introspection, check references."""

    def test_load_contract(self, contract_path):
        from vowl import Contract

        contract = Contract.load(contract_path)

        assert contract is not None
        assert contract.contract_data is not None

    def test_get_api_version(self, contract_path):
        from vowl import Contract

        contract = Contract.load(contract_path)
        version = contract.get_api_version()

        assert version is not None
        assert version.startswith("v")

    def test_get_schema_names(self, contract_path):
        from vowl import Contract

        contract = Contract.load(contract_path)
        schema_names = contract.get_schema_names()

        assert isinstance(schema_names, list)
        assert "hdb_resale_prices" in schema_names

    def test_get_check_references_by_schema(self, contract_path):
        from vowl import Contract

        contract = Contract.load(contract_path)
        check_refs = contract.get_check_references_by_schema()

        assert isinstance(check_refs, dict)
        assert "hdb_resale_prices" in check_refs
        refs = check_refs["hdb_resale_prices"]
        assert isinstance(refs, list)
        assert len(refs) > 0


# ============================================================================
# 12. DataSourceMapper
# ============================================================================

class TestDataSourceMapper:
    """README implicitly: DataSourceMapper auto-detection."""

    def test_mapper_from_dataframe(self, small_df):
        from vowl import DataSourceMapper

        mapper = DataSourceMapper()
        adapter = mapper.get_adapter(small_df, "test_table")

        assert adapter is not None

    def test_create_adapter_convenience(self, small_df):
        from vowl import create_adapter

        adapter = create_adapter(small_df, "test_table")

        assert adapter is not None


# ============================================================================
# 13. Error Handling (documented behaviour)
# ============================================================================

class TestErrorHandling:
    """Verify error paths mentioned / implied in README."""

    def test_missing_data_source(self, contract_path):
        from vowl import validate_data

        with pytest.raises(ValueError, match="data source must be provided"):
            validate_data(contract=contract_path)

    def test_multiple_data_sources(self, sample_df, contract_path):
        from vowl import validate_data

        with pytest.raises(ValueError, match="Only one data source"):
            validate_data(
                contract=contract_path,
                df=sample_df,
                connection_str="postgresql://localhost/test",
            )

    def test_invalid_contract_path(self, sample_df):
        from vowl import validate_data

        with pytest.raises(FileNotFoundError):
            validate_data(contract="/nonexistent/contract.yaml", df=sample_df)


# ============================================================================
# 14. MySQL via testcontainers
# ============================================================================

@pytest.mark.docker_integration
class TestMySQL:
    """Notebook pattern: MySQL via testcontainers."""

    @pytest.fixture(scope="class")
    def mysql_container(self):
        if not _docker_available():
            pytest.skip("Docker not available")
        _configure_docker_env()

        try:
            from testcontainers.mysql import MySqlContainer
        except ImportError:
            pytest.skip("testcontainers[mysql] not installed")

        try:
            import ibis
            ibis.mysql.connect  # noqa: B018 — just check it exists
        except (ImportError, AttributeError):
            pytest.skip("Ibis MySQL backend not installed")

        container = MySqlContainer(
            image="mysql:8.0",
            username="testuser",
            password="testpass",
            dbname="testdb",
        )
        container.start()
        yield container
        container.stop()

    @pytest.fixture
    def mysql_connection(self, mysql_container):
        import ibis

        host = mysql_container.get_container_host_ip()
        port = int(mysql_container.get_exposed_port(3306))

        con = ibis.mysql.connect(
            host=host,
            port=port,
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
        )

        con.raw_sql("""
            CREATE TABLE IF NOT EXISTS hdb_resale_prices (
                month VARCHAR(7), town VARCHAR(50), flat_type VARCHAR(20),
                block VARCHAR(10), street_name VARCHAR(100),
                storey_range VARCHAR(20), floor_area_sqm INT,
                flat_model VARCHAR(50), lease_commence_date INT,
                remaining_lease VARCHAR(50), resale_price INT
            )
        """)
        con.raw_sql("TRUNCATE TABLE hdb_resale_prices")

        df = pd.read_csv(DATA_FILE, low_memory=False).head(100)
        for col in ("floor_area_sqm", "lease_commence_date", "resale_price"):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        for _, row in df.iterrows():
            vals = []
            for v in row.values:
                if pd.isna(v):
                    vals.append("NULL")
                elif isinstance(v, (int, float)):
                    vals.append(str(int(v)))
                else:
                    vals.append("'" + str(v).replace("'", "''") + "'")
            con.raw_sql(
                f"INSERT INTO hdb_resale_prices VALUES ({', '.join(vals)})"
            )

        yield con
        con.disconnect()

    def test_mysql_validate(self, mysql_connection, contract_path):
        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        adapter = IbisAdapter(mysql_connection)
        result = validate_data(contract=contract_path, adapter=adapter)

        assert result is not None
        assert len(result.get_check_results_df()) > 0
        _assert_no_error_checks(result)

    def test_mysql_filter_conditions(self, mysql_connection, contract_path):
        from vowl import validate_data
        from vowl.adapters import FilterCondition, IbisAdapter

        adapter = IbisAdapter(
            mysql_connection,
            filter_conditions={
                "hdb_resale_prices": FilterCondition(
                    field="month", operator=">=", value="2017-01",
                ),
            },
        )
        result = validate_data(contract=contract_path, adapter=adapter)

        assert result is not None
        _assert_no_error_checks(result)


# ============================================================================
# 15. DuckDB ATTACH with PostgreSQL — multi-source views
# ============================================================================

@pytest.mark.docker_integration
class TestDuckDBAttachMultiSourceValidation:
    """README Option A: DuckDB ATTACH for multi-source with views + validate."""

    @pytest.fixture(scope="class")
    def postgres_container(self):
        if not _docker_available():
            pytest.skip("Docker not available")
        _configure_docker_env()

        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:15-alpine")
        container.start()
        yield container
        container.stop()

    def test_attach_validate_full_pipeline(self, postgres_container, contract_path):
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        # Seed Postgres
        host = postgres_container.get_container_host_ip()
        port = int(postgres_container.get_exposed_port(5432))
        user = postgres_container.username
        pw = postgres_container.password
        db = postgres_container.dbname

        pg_con = ibis.postgres.connect(
            host=host, port=port, user=user, password=pw, database=db,
        )
        pg_con.raw_sql("""
            CREATE TABLE IF NOT EXISTS hdb_resale_prices (
                month TEXT, town TEXT, flat_type TEXT, block TEXT,
                street_name TEXT, storey_range TEXT, floor_area_sqm INTEGER,
                flat_model TEXT, lease_commence_date INTEGER,
                remaining_lease TEXT, resale_price INTEGER
            )
        """)
        pg_con.raw_sql("TRUNCATE TABLE hdb_resale_prices")

        df = pd.read_csv(DATA_FILE, low_memory=False).head(100)
        for col in ("floor_area_sqm", "lease_commence_date", "resale_price"):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        pg_con.insert("hdb_resale_prices", df)

        pg_url = f"postgresql://{user}:{pw}@{host}:{port}/{db}"

        # DuckDB ATTACH + USE
        con = ibis.duckdb.connect()
        con.raw_sql(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")
        con.raw_sql("USE pg")

        result = validate_data(contract=contract_path, adapter=IbisAdapter(con))

        assert result is not None
        assert len(result.get_check_results_df()) > 0
        _assert_no_error_checks(result)
