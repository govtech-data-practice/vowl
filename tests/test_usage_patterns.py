"""Tests for Usage Patterns documented in docs/usage_patterns.md.

This module provides test coverage for all usage patterns including:
1. Local DataFrame (Pandas and Polars)
2. PySpark DataFrames
3. Ibis Connections (DuckDB, SQLite, PostgreSQL)
4. Explicit Adapter with Filter Conditions
5. Multi-Adapters
6. Custom Adapters and Executors

Uses real database instances:
- DuckDB: In-memory
- SQLite: File-based via tmp_path
- PostgreSQL: Via testcontainers (requires Docker)
"""
import os
from pathlib import Path

import narwhals as nw
import pandas as pd
import pytest

# Path constants
TEST_DIR = Path(__file__).parent
HDB_DIR = TEST_DIR / "hdb_resale"
DATA_FILE = HDB_DIR / "HDBResaleWithErrors.csv"
CONTRACT_PATH = HDB_DIR / "hdb_resale.yaml"
PSD_DIR = TEST_DIR / "psd_employee"
PSD_EMPLOYEE_LIST_FILE = PSD_DIR / "demo_employee_list.csv"
PSD_EMPLOYEE_PAYROLL_FILE = PSD_DIR / "demo_employee_payroll.csv"
PSD_CONTRACT_PATH = PSD_DIR / "employee_payroll_datacontract.yaml"


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.read_csv(DATA_FILE)


@pytest.fixture
def contract_path() -> str:
    """Get the path to the HDB resale contract."""
    return str(CONTRACT_PATH)


@pytest.fixture
def clean_dataframe(sample_dataframe) -> pd.DataFrame:
    """Create a DataFrame with all columns as strings for Arrow compatibility."""
    return sample_dataframe.astype(str)


@pytest.fixture
def small_clean_dataframe(sample_dataframe) -> pd.DataFrame:
    """Create a small DataFrame for faster tests."""
    return sample_dataframe.head(100).astype(str)


def assert_no_check_errors(results):
    """Assert that no checks have 'ERROR' status in validation results.

    This helper ensures tests fail if any data quality check encountered
    an error during execution (as opposed to PASSED/FAILED status).
    """
    results_df = results.get_check_results_df().to_pandas()
    error_checks = results_df[results_df['status'].str.upper() == 'ERROR']
    if len(error_checks) > 0:
        error_info = error_checks[['check_name', 'status', 'message']].to_dict('records')
        pytest.fail(f"Checks returned ERROR: {error_info}")


# ============================================================================
# 1. Local DataFrame Tests (Pandas and Polars)
# ============================================================================

class TestLocalDataFramePandas:
    """Test validate_data with pandas DataFrame as documented in usage patterns."""

    def test_basic_pandas_validation(self, sample_dataframe, contract_path):
        """
        Test basic validation pattern:

        >>> import pandas as pd
        >>> from vowl import validate_data
        >>> df = pd.read_csv(data_file_path)
        >>> results = validate_data(df, data_contracts_path)
        """
        from vowl import validate_data

        results = validate_data(
            contract=contract_path,
            df=sample_dataframe,
        )

        assert results is not None
        assert hasattr(results, 'display_full_report')
        assert hasattr(results, 'passed')
        assert_no_check_errors(results)

    def test_pandas_validation_display_full_report(self, sample_dataframe, contract_path, capsys):
        """Test that display_full_report works as documented."""
        from vowl import validate_data

        results = validate_data(
            contract=contract_path,
            df=sample_dataframe,
        )

        # Method should be callable and return self for chaining
        result = results.display_full_report()
        assert result is results  # Chaining support

        # Check output was printed
        captured = capsys.readouterr()
        assert "Data Quality Validation Results" in captured.out or len(captured.out) > 0

    def test_pandas_validation_returns_valid_results_df(self, sample_dataframe, contract_path):
        """Test that validation returns check results as DataFrame."""
        from vowl import validate_data

        results = validate_data(
            contract=contract_path,
            df=sample_dataframe,
        )

        results_df = results.get_check_results_df()

        assert isinstance(results_df, nw.DataFrame)
        assert len(results_df) > 0
        assert 'check_name' in results_df.columns
        assert 'status' in results_df.columns
        assert_no_check_errors(results)

    def test_pandas_validation_summary(self, sample_dataframe, contract_path):
        """Test that validation provides summary information."""
        from vowl import validate_data

        results = validate_data(
            contract=contract_path,
            df=sample_dataframe,
        )

        assert 'validation_summary' in results.summary
        vs = results.summary['validation_summary']
        assert 'total_checks' in vs
        assert 'passed' in vs
        assert 'failed' in vs


class TestLocalDataFramePolars:
    """Test validate_data with Polars DataFrame."""

    @pytest.fixture
    def polars_dataframe(self, sample_dataframe):
        """Create a Polars DataFrame for testing."""
        import polars as pl
        # Convert to strings first to handle mixed types in CSV
        return pl.from_pandas(sample_dataframe.astype(str))

    def test_polars_validation(self, polars_dataframe, contract_path):
        """
        Test Polars DataFrame validation (works via IbisAdapter):

        >>> import polars as pl
        >>> from vowl import validate_data
        >>> df = pl.read_csv(data_file_path)
        >>> results = validate_data(df, data_contracts_path)
        """
        from vowl import validate_data

        results = validate_data(
            contract=contract_path,
            df=polars_dataframe.to_pandas(),  # Convert for compatibility
        )

        assert results is not None
        assert hasattr(results, 'passed')
        assert_no_check_errors(results)

    def test_polars_direct_ibis_validation(self, polars_dataframe, contract_path):
        """
        Test Polars DataFrame validation using Ibis directly.

        Ibis natively supports Polars DataFrames.
        """
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        # Ibis can work directly with Polars
        con = ibis.duckdb.connect()
        con.create_table('hdb_resale_prices', polars_dataframe.to_pandas())

        adapter = IbisAdapter(con)
        results = validate_data(
            contract=contract_path,
            adapter=adapter,
        )

        assert results is not None
        assert hasattr(results, 'passed')
        assert_no_check_errors(results)


# ============================================================================
# 2. PySpark Tests (requires PySpark installation)
# ============================================================================

class TestPySparkValidation:
    """Test validate_data with PySpark DataFrame and SparkSession.

    These tests require PySpark to be installed. They are skipped if PySpark
    is not available, but will run in CI environments where PySpark is configured.
    """

    @pytest.fixture
    def spark_session(self):
        """Create a real SparkSession for testing."""
        import os
        from pathlib import Path

        # Auto-detect Java on macOS (Homebrew)
        if "JAVA_HOME" not in os.environ:
            homebrew_java = Path("/opt/homebrew/opt/openjdk@17")
            if homebrew_java.exists():
                os.environ["JAVA_HOME"] = str(homebrew_java)

        pytest.importorskip("pyspark", reason="PySpark not installed")
        from pyspark.sql import SparkSession

        try:
            spark = SparkSession.builder \
                .master("local[1]") \
                .appName("test_vowl") \
                .config("spark.driver.memory", "512m") \
                .config("spark.sql.shuffle.partitions", "1") \
                .getOrCreate()
        except Exception as e:
            pytest.skip(f"PySpark could not start (Java not available?): {e}")

        yield spark
        spark.stop()

    @pytest.fixture
    def spark_dataframe(self, spark_session, sample_dataframe):
        """Create a real PySpark DataFrame from pandas."""
        # Convert to string types for consistency
        pdf = sample_dataframe.astype(str)
        return spark_session.createDataFrame(pdf)

    def test_pyspark_dataframe_to_pandas(self, spark_dataframe, contract_path):
        """
        Test PySpark DataFrame validation by converting to pandas.

        >>> from vowl import validate_data
        >>> spark_df = spark.read.csv(...)
        >>> results = validate_data(contract_path, df=spark_df.toPandas())
        """
        from vowl import validate_data

        # Convert PySpark DataFrame to pandas for validation
        pandas_df = spark_dataframe.toPandas()

        results = validate_data(
            contract=contract_path,
            df=pandas_df,
        )

        assert results is not None
        assert hasattr(results, 'passed')
        assert_no_check_errors(results)

    def test_pyspark_with_ibis_adapter(self, spark_session, spark_dataframe, contract_path):
        """
        Test PySpark integration via IbisAdapter using ibis.pyspark connector.

        >>> con = ibis.pyspark.connect(session=spark)
        >>> adapter = IbisAdapter(con)
        """
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        # Register Spark DataFrame as a temp view
        spark_dataframe.createOrReplaceTempView("hdb_resale_prices")

        # Connect ibis to PySpark session
        con = ibis.pyspark.connect(session=spark_session)

        adapter = IbisAdapter(con)
        results = validate_data(
            contract=contract_path,
            adapter=adapter,
        )

        assert results is not None
        results_df = results.get_check_results_df()
        assert len(results_df) > 0
        assert_no_check_errors(results)

    def test_spark_sql_query(self, spark_session, spark_dataframe):
        """Test running SQL queries on Spark DataFrames."""
        # Register as temp view
        spark_dataframe.createOrReplaceTempView("hdb_data")

        # Run SQL query
        result = spark_session.sql("SELECT COUNT(*) as cnt FROM hdb_data")
        count = result.collect()[0]['cnt']

        assert count > 0  # Verify data exists


# ============================================================================
# 3. Ibis Connections Tests
# ============================================================================

class TestIbisConnections:
    """Test validate_data with Ibis connection backends."""

    def test_ibis_duckdb_connection(self, clean_dataframe, contract_path):
        """
        Test Ibis DuckDB connection pattern:

        >>> import ibis
        >>> from vowl import validate_data
        >>> con = ibis.duckdb.connect()
        >>> results = validate_data(con, data_contracts_path)
        """
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table('hdb_resale_prices', clean_dataframe)

        adapter = IbisAdapter(con)
        results = validate_data(
            contract=contract_path,
            adapter=adapter,
        )

        assert results is not None
        assert hasattr(results, 'passed')
        assert_no_check_errors(results)

    def test_ibis_connection_with_table(self, clean_dataframe, contract_path):
        """Test Ibis connection with explicit table registration."""
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table('hdb_resale_prices', clean_dataframe)

        adapter = IbisAdapter(con)
        results = validate_data(
            contract=contract_path,
            adapter=adapter,
        )

        assert results is not None
        results_df = results.get_check_results_df()
        assert len(results_df) > 0
        assert_no_check_errors(results)


class TestSQLiteConnection:
    """Test validate_data with real SQLite connections."""

    @pytest.fixture
    def sqlite_connection(self, small_clean_dataframe, tmp_path):
        """Create a real SQLite database with test data."""
        import ibis

        db_path = tmp_path / "test.db"
        con = ibis.sqlite.connect(str(db_path))

        # Create table with explicit schema
        con.raw_sql("""
            CREATE TABLE hdb_resale_prices (
                month TEXT, town TEXT, flat_type TEXT, block TEXT,
                street_name TEXT, storey_range TEXT, floor_area_sqm TEXT,
                flat_model TEXT, lease_commence_date TEXT, remaining_lease TEXT,
                resale_price TEXT
            )
        """)

        # Insert sample data
        con.insert('hdb_resale_prices', small_clean_dataframe.astype(str))

        yield con
        # Connection auto-closes when test ends

    def test_sqlite_connection_setup(self, sqlite_connection):
        """Test that SQLite connection is properly established."""
        # Verify table exists and has data
        result = sqlite_connection.raw_sql("SELECT COUNT(*) FROM hdb_resale_prices")
        count = result.fetchone()[0]
        assert count == 100  # small_clean_dataframe has 100 rows

    def test_sqlite_with_ibis_adapter(self, sqlite_connection, contract_path):
        """
        Test SQLite connection using Ibis:

        >>> import ibis
        >>> con = ibis.sqlite.connect("database.db")
        >>> adapter = IbisAdapter(con)
        >>> results = validate_data(contract_path, adapter=adapter)
        """
        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        adapter = IbisAdapter(sqlite_connection)
        results = validate_data(
            contract=contract_path,
            adapter=adapter,
        )

        assert results is not None
        assert hasattr(results, 'passed')

        # Verify checks were executed
        results_df = results.get_check_results_df()
        assert len(results_df) > 0
        assert_no_check_errors(results)

    def test_sqlite_raw_queries(self, sqlite_connection):
        """Test running raw SQL queries on SQLite."""
        # Test SELECT with WHERE
        result = sqlite_connection.raw_sql(
            "SELECT COUNT(*) FROM hdb_resale_prices WHERE town = 'ANG MO KIO'"
        )
        count = result.fetchone()[0]
        assert isinstance(count, int)

        # Test aggregation
        result = sqlite_connection.raw_sql(
            "SELECT town, COUNT(*) as cnt FROM hdb_resale_prices GROUP BY town"
        )
        rows = result.fetchall()
        assert len(rows) > 0

    def test_sqlite_with_filter_conditions(self, sqlite_connection, contract_path):
        """Test SQLite adapter with filter conditions."""
        from vowl import validate_data
        from vowl.adapters import FilterCondition, IbisAdapter

        adapter = IbisAdapter(
            sqlite_connection,
            filter_conditions={
                'hdb_resale_prices': FilterCondition(
                    field='month',
                    operator='>=',
                    value='2017-01'
                )
            }
        )

        results = validate_data(
            contract=contract_path,
            adapter=adapter,
        )

        assert results is not None
        assert_no_check_errors(results)


@pytest.mark.docker_integration
class TestPostgresConnection:
    """Test validate_data with real PostgreSQL connections via testcontainers.

    These tests require Docker to be running. They spin up a real PostgreSQL
    container for integration testing.
    """

    @pytest.fixture(scope="class")
    def postgres_container(self):
        """Start a PostgreSQL container for testing."""
        import os
        from pathlib import Path

        try:
            from testcontainers.postgres import PostgresContainer
        except ImportError:
            pytest.skip("testcontainers not installed")

        # Check if Docker is available
        import subprocess
        try:
            subprocess.run(["docker", "info"], capture_output=True, check=True, timeout=5)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            pytest.skip("Docker not available")

        # Configure Docker Desktop socket path (macOS)
        docker_sock_path = Path.home() / ".docker" / "run" / "docker.sock"
        if docker_sock_path.exists() and "DOCKER_HOST" not in os.environ:
            os.environ["DOCKER_HOST"] = f"unix://{docker_sock_path}"

        # Disable Ryuk (testcontainers reaper) for Docker Desktop compatibility
        if "TESTCONTAINERS_RYUK_DISABLED" not in os.environ:
            os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"

        postgres = PostgresContainer("postgres:15-alpine")
        postgres.start()

        yield postgres

        postgres.stop()

    @pytest.fixture
    def postgres_connection(self, postgres_container, sample_dataframe):
        """Create an Ibis connection to the PostgreSQL container."""
        import ibis

        # Get connection URL from container
        host = postgres_container.get_container_host_ip()
        port = postgres_container.get_exposed_port(5432)
        user = postgres_container.username
        password = postgres_container.password
        database = postgres_container.dbname

        con = ibis.postgres.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )

        # Use proper column types matching the contract's logicalType definitions.
        # PostgreSQL is strictly typed, so the table should reflect actual types
        # rather than storing everything as TEXT.
        con.raw_sql("""
            CREATE TABLE IF NOT EXISTS hdb_resale_prices (
                month TEXT, town TEXT, flat_type TEXT, block TEXT,
                street_name TEXT, storey_range TEXT, floor_area_sqm INTEGER,
                flat_model TEXT, lease_commence_date INTEGER, remaining_lease TEXT,
                resale_price INTEGER
            )
        """)

        # Clear any existing data
        con.raw_sql("TRUNCATE TABLE hdb_resale_prices")

        # Prepare a small slice with proper types for integer columns
        df = sample_dataframe.head(100).copy()
        for col in ("floor_area_sqm", "lease_commence_date", "resale_price"):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # Insert sample data
        con.insert('hdb_resale_prices', df)

        yield con

    def test_postgres_connection_setup(self, postgres_connection):
        """Test that PostgreSQL connection is properly established."""
        result = postgres_connection.raw_sql("SELECT COUNT(*) FROM hdb_resale_prices")
        count = result.fetchone()[0]
        assert count == 100

    def test_postgres_with_ibis_adapter(self, postgres_connection, contract_path):
        """
        Test PostgreSQL connection using Ibis:

        >>> import ibis
        >>> con = ibis.postgres.connect(host=..., port=..., user=..., password=..., database=...)
        >>> adapter = IbisAdapter(con)
        >>> results = validate_data(contract_path, adapter=adapter)
        """
        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        adapter = IbisAdapter(postgres_connection)
        results = validate_data(
            contract=contract_path,
            adapter=adapter,
        )

        assert results is not None
        assert hasattr(results, 'passed')

        results_df = results.get_check_results_df()
        assert len(results_df) > 0
        assert_no_check_errors(results)

    def test_postgres_with_filter_conditions(self, postgres_connection, contract_path):
        """Test PostgreSQL adapter with filter conditions."""
        from vowl import validate_data
        from vowl.adapters import FilterCondition, IbisAdapter

        adapter = IbisAdapter(
            postgres_connection,
            filter_conditions={
                'hdb_resale_prices': FilterCondition(
                    field='month',
                    operator='>=',
                    value='2017-01'
                )
            }
        )

        results = validate_data(
            contract=contract_path,
            adapter=adapter,
        )

        assert results is not None
        assert_no_check_errors(results)

    def test_postgres_test_connection(self, postgres_connection):
        """Test the adapter's test_connection method with real PostgreSQL."""
        from vowl.adapters import IbisAdapter

        adapter = IbisAdapter(postgres_connection)

        # test_connection returns None on success, error message on failure
        result = adapter.test_connection('hdb_resale_prices')
        assert result is None  # Connection successful

    def test_postgres_raw_queries(self, postgres_connection):
        """Test running raw SQL queries on PostgreSQL."""
        # Test SELECT with WHERE
        result = postgres_connection.raw_sql(
            "SELECT COUNT(*) FROM hdb_resale_prices WHERE town = 'ANG MO KIO'"
        )
        count = result.fetchone()[0]
        assert isinstance(count, int)

        # Test PostgreSQL-specific features
        result = postgres_connection.raw_sql(
            "SELECT version()"
        )
        version = result.fetchone()[0]
        assert 'PostgreSQL' in version


# ============================================================================
# 4. Explicit Adapter with Filter Conditions Tests
# ============================================================================

class TestExplicitAdapterFilterConditions:
    """Test IbisAdapter with explicit filter conditions as documented."""

    def test_filter_condition_dict_style(self, clean_dataframe, contract_path):
        """
        Test filter conditions using dict format:

        >>> adapter = IbisAdapter(
        ...     con,
        ...     filter_conditions={
        ...         "TableA": {"field": "date_dt", "operator": ">=", "value": date_limit}
        ...     }
        ... )
        """
        import ibis

        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table('hdb_resale_prices', clean_dataframe)

        adapter = IbisAdapter(
            con,
            filter_conditions={
                "hdb_resale_prices": {
                    "field": "month",
                    "operator": ">=",
                    "value": "2017-01"
                }
            }
        )

        assert adapter.filter_conditions is not None
        assert "hdb_resale_prices" in adapter.filter_conditions

    def test_filter_condition_class_style(self, clean_dataframe, contract_path):
        """
        Test filter conditions using FilterCondition class:

        >>> from vowl.adapters import IbisAdapter, FilterCondition
        >>> adapter = IbisAdapter(
        ...     con,
        ...     filter_conditions={
        ...         "TableA": FilterCondition(field="date_dt", operator=">=", value=date_limit)
        ...     }
        ... )
        """
        import ibis

        from vowl.adapters import FilterCondition, IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table('hdb_resale_prices', clean_dataframe)

        adapter = IbisAdapter(
            con,
            filter_conditions={
                "hdb_resale_prices": FilterCondition(
                    field="month",
                    operator=">=",
                    value="2017-01"
                )
            }
        )

        assert adapter.filter_conditions is not None

    def test_multiple_filter_conditions_list(self, clean_dataframe, contract_path):
        """
        Test multiple filter conditions combined with AND:

        >>> adapter = IbisAdapter(
        ...     con,
        ...     filter_conditions={
        ...         "TableA": [
        ...             FilterCondition(field="date_dt", operator=">=", value=date_limit),
        ...             FilterCondition(field="status", operator="=", value="active"),
        ...         ]
        ...     }
        ... )
        """
        import ibis

        from vowl.adapters import FilterCondition, IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table('hdb_resale_prices', clean_dataframe)

        adapter = IbisAdapter(
            con,
            filter_conditions={
                "hdb_resale_prices": [
                    FilterCondition(field="month", operator=">=", value="2017-01"),
                    FilterCondition(field="town", operator="=", value="ANG MO KIO"),
                ]
            }
        )

        conditions = adapter.filter_conditions["hdb_resale_prices"]
        assert isinstance(conditions, list)
        assert len(conditions) == 2

    def test_wildcard_filter_conditions(self, clean_dataframe, contract_path):
        """
        Test wildcard pattern matching in filter conditions:

        >>> adapter = IbisAdapter(
        ...     con,
        ...     filter_conditions={
        ...         "emp*": {...},           # Matches employees, emp_history, etc.
        ...         "*_archive": {...},      # Matches orders_archive, etc.
        ...         "*": {...},              # Apply to ALL tables
        ...     }
        ... )
        """
        import ibis

        from vowl.adapters import FilterCondition, IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table('hdb_resale_prices', clean_dataframe)

        adapter = IbisAdapter(
            con,
            filter_conditions={
                "hdb_*": FilterCondition(field="month", operator=">=", value="2017-01"),
                "*_prices": FilterCondition(field="town", operator="!=", value=""),
                "*": FilterCondition(field="resale_price", operator=">", value=0),
            }
        )

        # Verify wildcard patterns are stored
        assert "hdb_*" in adapter.filter_conditions
        assert "*_prices" in adapter.filter_conditions
        assert "*" in adapter.filter_conditions


# ============================================================================
# 5. Multi-Adapters Tests
# ============================================================================

class TestMultiAdapters:
    """Test multi-adapter validation for cross-table contracts."""

    @pytest.fixture
    def multi_table_dataframes(self, sample_dataframe):
        """Create multiple DataFrames for multi-adapter testing."""
        # Split data into two "tables"
        df1 = sample_dataframe.head(100).astype(str)
        df2 = sample_dataframe.tail(100).astype(str)
        return df1, df2

    def test_multi_adapter_creation(self, multi_table_dataframes):
        """
        Test multi-adapter pattern:

        >>> adapters = {
        ...     "table_a": IbisAdapter(con_a),
        ...     "table_b": IbisAdapter(con_b)
        ... }
        """
        import ibis

        from vowl.adapters import IbisAdapter, MultiSourceAdapter

        df1, df2 = multi_table_dataframes

        con_a = ibis.duckdb.connect()
        con_a.create_table('hdb_resale_prices', df1)

        con_b = ibis.duckdb.connect()
        con_b.create_table('hdb_resale_prices', df2)

        adapters = {
            "hdb_resale_prices": IbisAdapter(con_a),
        }

        multi_adapter = MultiSourceAdapter(adapters)

        assert multi_adapter is not None
        assert "hdb_resale_prices" in multi_adapter.schema_names

    def test_multi_adapter_get_adapter(self, multi_table_dataframes):
        """Test retrieving individual adapter from MultiSourceAdapter."""
        import ibis

        from vowl.adapters import IbisAdapter, MultiSourceAdapter

        df1, _ = multi_table_dataframes

        con = ibis.duckdb.connect()
        con.create_table('hdb_resale_prices', df1)

        adapter = IbisAdapter(con)
        multi_adapter = MultiSourceAdapter({"hdb_resale_prices": adapter})

        retrieved = multi_adapter.get_adapter("hdb_resale_prices")
        assert retrieved is adapter

        missing = multi_adapter.get_adapter("nonexistent")
        assert missing is None

    def test_multi_adapter_validation(self, clean_dataframe, contract_path):
        """Test validation using multi-adapter."""
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table('hdb_resale_prices', clean_dataframe)

        adapters = {
            "hdb_resale_prices": IbisAdapter(con),
        }

        results = validate_data(
            contract=contract_path,
            adapters=adapters,
        )

        assert results is not None
        assert hasattr(results, 'passed')
        assert_no_check_errors(results)

    def test_single_adapter_expands_to_all_schemas(self):
        """
        Test convenience loading: a single adapter or df auto-expands to all
        schemas defined in the contract.

        Uses the PSD contract, which has two schemas backed by one DuckDB
        connection. validate_data should reuse the adapter across both schema
        names and run the cross-table checks successfully.
        """
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        employee_list_df = pd.read_csv(PSD_EMPLOYEE_LIST_FILE)
        employee_payroll_df = pd.read_csv(PSD_EMPLOYEE_PAYROLL_FILE)

        con = ibis.duckdb.connect()
        con.create_table("PSD_demo_employee_payroll", employee_payroll_df)
        con.create_table("PSD_demo_employee_list", employee_list_df)

        adapter = IbisAdapter(con)

        with pytest.warns(UserWarning, match="only 1 input adapter provided"):
            results = validate_data(
                contract=str(PSD_CONTRACT_PATH),
                adapter=adapter,
            )

        results_df = results.get_check_results_df().to_pandas()
        cross_check_names = {
            "employee_id_exists_in_master_list",
            "phone_number_exists_in_master_list",
        }
        cross_checks_df = results_df[results_df["check_name"].isin(cross_check_names)]

        assert results is not None
        assert_no_check_errors(results)
        assert set(results_df["schema"].dropna().unique()) == {
            "PSD_demo_employee_payroll",
            "PSD_demo_employee_list",
        }
        assert set(cross_checks_df["check_name"]) == cross_check_names
        assert set(cross_checks_df["status"]) == {"FAILED"}
        assert set(cross_checks_df["schema"]) == {"PSD_demo_employee_payroll"}
        assert cross_checks_df["tables_in_query"].astype(str).str.contains("PSD_demo_employee_payroll").all()
        assert cross_checks_df["tables_in_query"].astype(str).str.contains("PSD_demo_employee_list").all()

        output_dfs = results.get_output_dfs()
        phone_output = output_dfs["PSD_demo_employee_payroll::phone_number_exists_in_master_list"].to_pandas()

        assert "employee_id.1" in phone_output.columns
        assert "phone_number.1" in phone_output.columns


class TestMultiDatabaseIntegration:
    """Test multi-database scenarios with real database connections.

    These tests demonstrate using different database backends together,
    which is a common pattern when data is spread across systems.
    """

    def test_duckdb_and_sqlite_together(self, small_clean_dataframe, tmp_path):
        """Test using DuckDB and SQLite adapters together."""
        import ibis

        from vowl.adapters import IbisAdapter, MultiSourceAdapter

        # Setup DuckDB (in-memory)
        duckdb_con = ibis.duckdb.connect()
        duckdb_con.create_table('orders', small_clean_dataframe)

        # Setup SQLite (file-based)
        sqlite_path = tmp_path / "products.db"
        sqlite_con = ibis.sqlite.connect(str(sqlite_path))
        sqlite_con.raw_sql("""
            CREATE TABLE products (
                month TEXT, town TEXT, flat_type TEXT, block TEXT,
                street_name TEXT, storey_range TEXT, floor_area_sqm TEXT,
                flat_model TEXT, lease_commence_date TEXT, remaining_lease TEXT,
                resale_price TEXT
            )
        """)
        sqlite_con.insert('products', small_clean_dataframe.head(50).astype(str))

        # Create adapters
        duckdb_adapter = IbisAdapter(duckdb_con)
        sqlite_adapter = IbisAdapter(sqlite_con)

        # Create multi-source adapter
        multi_adapter = MultiSourceAdapter({
            "orders": duckdb_adapter,
            "products": sqlite_adapter,
        })

        # Verify both adapters are accessible
        assert multi_adapter.get_adapter("orders") is duckdb_adapter
        assert multi_adapter.get_adapter("products") is sqlite_adapter

        # Verify we can query both
        duckdb_count = duckdb_con.raw_sql("SELECT COUNT(*) FROM orders").fetchone()[0]
        sqlite_count = sqlite_con.raw_sql("SELECT COUNT(*) FROM products").fetchone()[0]

        assert duckdb_count == 100
        assert sqlite_count == 50

    def test_multiple_duckdb_connections(self, small_clean_dataframe):
        """Test using multiple DuckDB connections (different in-memory databases)."""
        import ibis

        from vowl.adapters import IbisAdapter, MultiSourceAdapter

        # Create two separate DuckDB connections
        con1 = ibis.duckdb.connect()
        con1.create_table('table_a', small_clean_dataframe.head(50))

        con2 = ibis.duckdb.connect()
        con2.create_table('table_b', small_clean_dataframe.tail(50))

        # Create adapters
        adapter1 = IbisAdapter(con1)
        adapter2 = IbisAdapter(con2)

        # Create multi-source adapter
        multi_adapter = MultiSourceAdapter({
            "schema_a": adapter1,
            "schema_b": adapter2,
        })

        assert len(multi_adapter.schema_names) == 2
        assert "schema_a" in multi_adapter.schema_names
        assert "schema_b" in multi_adapter.schema_names


# ============================================================================
# 6. Custom Adapters and Executors Tests
# ============================================================================

class TestCustomAdaptersExecutors:
    """Test custom adapter and executor patterns."""

    def test_custom_executor_registration(self, clean_dataframe):
        """
        Test setting custom executors on adapter:

        >>> adapter = CustomAdapter(xxx, executors={"sql": CustomSQLExecutor})
        """
        import ibis

        from vowl.adapters import IbisAdapter
        from vowl.executors import IbisSQLExecutor

        con = ibis.duckdb.connect()
        con.create_table('test_table', clean_dataframe)

        adapter = IbisAdapter(con)

        # Set custom executors
        adapter.set_executors({
            'sql': IbisSQLExecutor,
        })

        executors = adapter.get_executors()
        assert 'sql' in executors
        assert executors['sql'] == IbisSQLExecutor

    def test_base_adapter_interface(self):
        """Test that BaseAdapter provides expected interface."""
        from vowl.adapters import BaseAdapter

        # BaseAdapter should have get_executors, set_executors
        assert hasattr(BaseAdapter, 'get_executors')
        assert hasattr(BaseAdapter, 'set_executors')
        assert hasattr(BaseAdapter, 'run_checks')

    def test_custom_adapter_subclass(self, clean_dataframe, contract_path):
        """Test creating a custom adapter subclass."""
        import ibis

        from vowl.adapters import BaseAdapter, IbisAdapter
        from vowl.executors import IbisSQLExecutor

        class CustomAdapter(BaseAdapter):
            """Custom adapter with logging."""

            def __init__(self, con, **kwargs):
                super().__init__(executors={"sql": IbisSQLExecutor})
                self._wrapped = IbisAdapter(con, **kwargs)
                self._log: list = []

            def get_connection(self):
                self._log.append("get_connection called")
                return self._wrapped.get_connection()

            @property
            def filter_conditions(self):
                return self._wrapped.filter_conditions

            def test_connection(self, table_name: str) -> str | None:
                return self._wrapped.test_connection(table_name)

        con = ibis.duckdb.connect()
        con.create_table('hdb_resale_prices', clean_dataframe)

        adapter = CustomAdapter(con)

        # Verify custom adapter works
        assert adapter.get_executors() is not None


# ============================================================================
# 7. ValidationResult API Tests
# ============================================================================

class TestValidationResultAPI:
    """Test the ValidationResult API methods."""

    def test_passed_property(self, sample_dataframe, contract_path):
        """Test the passed property."""
        from vowl import validate_data

        results = validate_data(
            contract=contract_path,
            df=sample_dataframe,
        )

        # Should be boolean
        assert isinstance(results.passed, bool)

    def test_api_version_property(self, sample_dataframe, contract_path):
        """Test the api_version property."""
        from vowl import validate_data

        results = validate_data(
            contract=contract_path,
            df=sample_dataframe,
        )

        assert results.api_version is not None
        assert results.api_version.startswith("v")

    def test_contract_data_property(self, sample_dataframe, contract_path):
        """Test the contract_data property."""
        from vowl import validate_data

        results = validate_data(
            contract=contract_path,
            df=sample_dataframe,
        )

        assert results.contract_data is not None

    def test_get_output_dfs(self, sample_dataframe, contract_path):
        """Test get_output_dfs method."""
        from vowl import validate_data

        results = validate_data(
            contract=contract_path,
            df=sample_dataframe,
        )

        output_dfs = results.get_output_dfs()

        assert isinstance(output_dfs, dict)
        # All non-ERROR checks should appear (PASSED and FAILED)
        non_error = [cr for cr in results.check_results if cr.status != 'ERROR']
        assert len(output_dfs) == len(non_error)
        for _check_id, df in output_dfs.items():
            assert isinstance(df, nw.DataFrame)
            assert 'check_id' in df.columns
            assert 'tables_in_query' in df.columns

    def test_method_chaining(self, sample_dataframe, contract_path, capsys):
        """Test that ValidationResult methods support chaining."""
        from vowl import validate_data

        results = validate_data(
            contract=contract_path,
            df=sample_dataframe,
        )

        # All display methods should return self for chaining
        chained = results.print_summary().show_failed_checks().show_failed_rows()
        assert chained is results

    def test_save_results(self, sample_dataframe, contract_path, tmp_path):
        """Test saving validation results."""
        from vowl import validate_data

        results = validate_data(
            contract=contract_path,
            df=sample_dataframe,
        )

        original_dir = os.getcwd()
        os.chdir(tmp_path)
        try:
            results.save(prefix="test_results")

            # Check files were created
            files = list(tmp_path.iterdir())
            assert len(files) >= 1
        finally:
            os.chdir(original_dir)


# ============================================================================
# 8. Error Handling Tests
# ============================================================================

class TestErrorHandling:
    """Test error handling scenarios."""

    def test_missing_data_source_error(self, contract_path):
        """Test that missing data source raises appropriate error."""
        from vowl import validate_data

        with pytest.raises(ValueError, match="data source must be provided"):
            validate_data(contract=contract_path)

    def test_multiple_data_sources_error(self, sample_dataframe, contract_path):
        """Test that multiple data sources raises appropriate error."""
        from vowl import validate_data

        with pytest.raises(ValueError, match="Only one data source"):
            validate_data(
                contract=contract_path,
                df=sample_dataframe,
                connection_str="postgresql://localhost/test",
            )

    def test_invalid_contract_path(self, sample_dataframe):
        """Test that invalid contract path raises appropriate error."""
        from vowl import validate_data

        with pytest.raises(FileNotFoundError):
            validate_data(
                contract="/nonexistent/contract.yaml",
                df=sample_dataframe,
            )


# ============================================================================
# 9. Contract API Tests
# ============================================================================

class TestContractAPI:
    """Test the Contract class API."""

    def test_load_contract(self, contract_path):
        """Test loading a contract via Contract.load."""
        from vowl import Contract

        contract = Contract.load(contract_path)

        assert contract is not None
        assert contract.contract_data is not None

    def test_get_api_version(self, contract_path):
        """Test getting API version from contract."""
        from vowl import Contract

        contract = Contract.load(contract_path)
        version = contract.get_api_version()

        assert version is not None
        assert version.startswith("v")

    def test_get_schema_names(self, contract_path):
        """Test getting schema names from contract."""
        from vowl import Contract

        contract = Contract.load(contract_path)
        schema_names = contract.get_schema_names()

        assert isinstance(schema_names, list)
        assert len(schema_names) > 0

    def test_get_check_references_by_schema(self, contract_path):
        """Test getting check references organized by schema."""
        from vowl import Contract

        contract = Contract.load(contract_path)
        check_refs = contract.get_check_references_by_schema()

        assert isinstance(check_refs, dict)
        assert len(check_refs) > 0

        # Each schema should have a list of check references
        for _schema_name, refs in check_refs.items():
            assert isinstance(refs, list)


# ============================================================================
# 10. DataSourceMapper Tests
# ============================================================================

class TestDataSourceMapper:
    """Test the DataSourceMapper utility."""

    def test_mapper_creates_adapter_from_dataframe(self, sample_dataframe):
        """Test that mapper creates adapter from pandas DataFrame."""
        from vowl import DataSourceMapper

        mapper = DataSourceMapper()
        adapter = mapper.get_adapter(sample_dataframe, "test_table")

        assert adapter is not None

    def test_create_adapter_function(self, sample_dataframe):
        """Test the create_adapter convenience function."""
        from vowl import create_adapter

        adapter = create_adapter(sample_dataframe, "test_table")

        assert adapter is not None
