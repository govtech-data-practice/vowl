"""
Mapper module for inferring the correct adapter based on user input.

This module provides functionality to automatically detect the data source type
and create the appropriate adapter for data quality validation.
"""

from __future__ import annotations

import functools
import re
from typing import TYPE_CHECKING, Any, cast

import ibis
import narwhals as nw
import pyarrow as pa
from ibis.backends.sql import SQLBackend

from vowl.adapters.base import BaseAdapter
from vowl.adapters.ibis_adapter import IbisAdapter

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession


@functools.cache
def _spark_types() -> tuple[type, type] | None:
    """Return (SparkSession, SparkDataFrame) if pyspark is installed, else None.

    Covers the *classic* pyspark classes only. Cached so the import is attempted
    at most once per process.
    """
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
        from pyspark.sql import SparkSession

        return (SparkSession, SparkDataFrame)
    except ImportError:
        return None


@functools.cache
def _spark_connect_types() -> tuple[type, type] | None:
    """Return (ConnectSparkSession, ConnectDataFrame) if Spark Connect is importable, else None.

    Spark Connect DataFrames/Sessions live in ``pyspark.sql.connect.*`` and are
    *separate* classes from the classic ones:

    - On pyspark 3.5 the Connect ``DataFrame`` is **not** a subclass of the classic
      ``pyspark.sql.DataFrame``, so ``isinstance`` against the classic type misses it.
    - The Connect ``SparkSession`` is **not** a subclass of the classic session on
      any version.

    Importing ``pyspark.sql.connect.*`` requires ``grpcio``/``grpcio-status`` and
    fails with ImportError when they are absent, so this is guarded separately from
    :func:`_spark_types` — the classic path must never depend on grpc being present.
    Cached so the import is attempted at most once per process.
    """
    try:
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

        return (ConnectSparkSession, ConnectDataFrame)
    except ImportError:
        return None


@functools.cache
def _spark_dataframe_types() -> tuple[type, ...]:
    """Return the tuple of recognised Spark DataFrame classes (classic + connect).

    Empty when pyspark is not installed. Testing membership with ``isinstance``
    against this tuple covers classic, Spark Connect, and any subclasses across
    pyspark 3.5 and 4.x.
    """
    types: tuple[type, ...] = ()
    if (classic := _spark_types()) is not None:
        types += (classic[1],)
    if (connect := _spark_connect_types()) is not None:
        types += (connect[1],)
    return types


@functools.cache
def _spark_session_types() -> tuple[type, ...]:
    """Return the tuple of recognised SparkSession classes (classic + connect).

    Empty when pyspark is not installed.
    """
    types: tuple[type, ...] = ()
    if (classic := _spark_types()) is not None:
        types += (classic[0],)
    if (connect := _spark_connect_types()) is not None:
        types += (connect[0],)
    return types


def _is_spark_dataframe(obj: Any) -> bool:
    """Check if obj is a PySpark DataFrame (classic or Connect) without importing pyspark eagerly."""
    types = _spark_dataframe_types()
    return bool(types) and isinstance(obj, types)


def _is_spark_session(obj: Any) -> bool:
    """Check if obj is a PySpark SparkSession (classic or Connect) without importing pyspark eagerly."""
    types = _spark_session_types()
    return bool(types) and isinstance(obj, types)


def _is_narwhals_dataframe(obj: Any) -> bool:
    """Check if *obj* is any DataFrame type supported by narwhals.

    This covers pandas, polars, PyArrow, cuDF, Modin, and any other
    eager DataFrame library that narwhals recognises.
    """
    try:
        nw.from_native(obj, eager_only=True)
        return True
    except TypeError:
        return False


class DataSourceMapper:
    """
    Maps user input to the appropriate adapter for data quality validation.

    Supports the following input types:
    - Any DataFrame supported by narwhals (pandas, polars, PyArrow, cuDF,
      Modin, etc.): Creates an Ibis DuckDB in-memory connection
    - pyspark.sql.DataFrame: Creates an Ibis PySpark connection
    - pyspark.sql.SparkSession: Creates an Ibis PySpark connection
    - str (connection string): Creates an Ibis connection from the URI
    - BaseAdapter: Uses the adapter directly
    - ibis.BaseBackend: Wraps in an IbisAdapter

    Example:
        >>> mapper = DataSourceMapper()
        >>> adapter = mapper.get_adapter(pandas_df)
        >>> # Returns IbisAdapter with DuckDB backend

        >>> adapter = mapper.get_adapter(polars_df)
        >>> # Returns IbisAdapter with DuckDB backend

        >>> adapter = mapper.get_adapter("postgresql://user:pass@host/db")  # trufflehog:ignore
        >>> # Returns IbisAdapter with PostgreSQL backend
    """

    def __init__(self) -> None:
        """Initialize the DataSourceMapper."""
        pass

    def get_adapter(
        self,
        data_source: Any,
        table_name: str = "source_data",
    ) -> IbisAdapter:
        """
        Create an appropriate adapter for the given data source.

        Args:
            data_source: The data source to create an adapter for. Can be:
                - Any DataFrame supported by narwhals (pandas, polars,
                  PyArrow, cuDF, Modin, etc.)
                - pyspark.sql.DataFrame
                - pyspark.sql.SparkSession
                - str (connection URI)
                - BaseAdapter (returned as-is if IbisAdapter)
                - ibis.BaseBackend
            table_name: Name to register the table as (for DataFrame inputs).
                Defaults to "source_data".

        Returns:
            IbisAdapter configured for the data source

        Raises:
            TypeError: If the data source type is not supported
        """
        # Already an adapter - return if IbisAdapter, otherwise wrap
        if isinstance(data_source, IbisAdapter):
            return data_source

        if isinstance(data_source, BaseAdapter):
            raise TypeError(f"Unsupported adapter type: {type(data_source).__name__}. Only IbisAdapter is supported.")

        # PySpark DataFrame - use ibis pyspark backend (check before narwhals
        # since PySpark DataFrames need special handling via the Spark backend)
        if _is_spark_dataframe(data_source):
            return self._create_adapter_from_spark_df(data_source, table_name)

        # SparkSession - use ibis pyspark backend
        if _is_spark_session(data_source):
            return self._create_adapter_from_spark_session(data_source)

        # Any DataFrame supported by narwhals (pandas, polars, PyArrow, etc.) - use DuckDB
        if _is_narwhals_dataframe(data_source):
            return self._create_adapter_from_dataframe(data_source, table_name)

        # Connection string - parse and create appropriate backend
        if isinstance(data_source, str):
            return self._create_adapter_from_connection_string(data_source)

        # Ibis backend - duck typing fallback
        if hasattr(data_source, "raw_sql"):
            return IbisAdapter(data_source)

        # Unknown type
        raise TypeError(
            f"Unsupported data source type: {type(data_source).__name__}. "
            "Supported types: any DataFrame (pandas, polars, PyArrow, etc.), "
            "pyspark.sql.DataFrame, pyspark.sql.SparkSession, "
            "connection string (str), ibis.BaseBackend, or IbisAdapter"
        )

    def _create_adapter_from_dataframe(
        self,
        df: Any,
        table_name: str,
    ) -> IbisAdapter:
        """
        Create an IbisAdapter from any narwhals-supported DataFrame using DuckDB.

        Converts the input to a PyArrow Table via narwhals and loads it into an
        in-memory DuckDB connection.  If Arrow conversion fails (e.g. due to
        mixed types), falls back to converting to pandas, coercing all columns
        to strings, and retrying.
        """
        import warnings

        con = ibis.duckdb.connect()
        nw_frame = nw.from_native(df, eager_only=True)

        try:
            # Convert to Arrow (zero-copy for polars/arrow, cheap for pandas)
            arrow_table = nw_frame.to_arrow()
            con.create_table(table_name, arrow_table)
        except Exception as e:
            error_msg = str(e).lower()
            is_type_error = any(keyword in error_msg for keyword in ["arrow", "type", "conversion", "expected"])

            if is_type_error:
                arrow_table, coerced_columns = self._build_arrow_with_column_fallback(
                    nw_frame,
                    initial_error=e,
                )
                warnings.warn(
                    "Arrow type conversion failed, loading problematic columns as strings: "
                    f"{', '.join(coerced_columns) if coerced_columns else 'all columns'}. "
                    f"Type validation will occur in DQ checks. Original error: {e}",
                    UserWarning,
                    stacklevel=3,
                )
                con.create_table(table_name, arrow_table)
            else:
                raise

        return IbisAdapter(con)

    @staticmethod
    def _extract_arrow_error_column(exc: Exception) -> str | None:
        """Return the column name mentioned in a PyArrow conversion error, if any."""
        match = re.search(r"Conversion failed for column (.+?) with type", str(exc))
        return match.group(1) if match else None

    def _build_arrow_with_column_fallback(
        self,
        nw_frame: Any,
        initial_error: Exception,
    ) -> tuple[pa.Table, list[str]]:
        """Retry Arrow conversion by coercing only columns that fail conversion."""
        import pandas as pd

        pandas_df = nw_frame.to_pandas()
        coerced_columns: list[str] = []
        current_error: Exception = initial_error

        while True:
            column_name = self._extract_arrow_error_column(current_error)

            if column_name and column_name in pandas_df.columns and column_name not in coerced_columns:
                pandas_df[column_name] = pandas_df[column_name].map(
                    lambda value: None if pd.isna(value) else str(value)
                )
                coerced_columns.append(column_name)
            elif not coerced_columns:
                for column_name in pandas_df.columns:
                    pandas_df[column_name] = pandas_df[column_name].map(
                        lambda value: None if pd.isna(value) else str(value)
                    )
                coerced_columns = list(pandas_df.columns)
            else:
                raise current_error

            try:
                return pa.Table.from_pandas(pandas_df, preserve_index=False), coerced_columns
            except Exception as retry_error:
                current_error = retry_error

    def _create_adapter_from_spark_df(
        self,
        df: SparkDataFrame,
        table_name: str,
    ) -> IbisAdapter:
        """Create an IbisAdapter from a PySpark DataFrame.

        Works for classic Spark and *driveable* Spark Connect sessions (local, and
        remote sessions ibis can drive). A remote Connect client that ibis cannot
        drive (e.g. Databricks Connect, with no local JVM/SparkContext) raises a
        clear error pointing at the pandas workaround rather than a cryptic failure.
        """
        try:
            # Register as temp view and connect via pyspark backend
            df.createOrReplaceTempView(table_name)
            con = ibis.pyspark.connect(df.sparkSession)
        except Exception as e:
            raise self._undriveable_spark_connect_error(e) from e
        return IbisAdapter(con)

    def _create_adapter_from_spark_session(
        self,
        session: SparkSession,
    ) -> IbisAdapter:
        """Create an IbisAdapter from a SparkSession.

        See :meth:`_create_adapter_from_spark_df` for the remote Connect behaviour.
        """
        try:
            con = ibis.pyspark.connect(session)
        except Exception as e:
            raise self._undriveable_spark_connect_error(e) from e
        return IbisAdapter(con)

    @staticmethod
    def _undriveable_spark_connect_error(original: Exception) -> TypeError:
        """Build a clear error for a Spark session ibis could not drive.

        A remote Spark Connect client (e.g. Databricks Connect) has no local
        JVM/SparkContext, so ``ibis.pyspark.connect(...)`` may be unable to drive
        it. Rather than surface a cryptic failure, point the user at the supported
        workaround: convert to pandas and validate via the DuckDB backend.
        """
        return TypeError(
            "Failed to connect to the provided Spark session via the ibis PySpark "
            "backend. This can happen with a remote Spark Connect client (e.g. "
            "Databricks Connect) that has no local Spark engine for ibis to drive. "
            "Convert the DataFrame to pandas and validate via the DuckDB backend "
            "instead, e.g. validate_data(contract, df=df.toPandas()). "
            f"Original error: {original}"
        )

    def _create_adapter_from_connection_string(
        self,
        connection_string: str,
    ) -> IbisAdapter:
        """Create an IbisAdapter from a database connection string.

        Delegates URI parsing and backend resolution to ibis.connect.
        Missing backend dependencies are surfaced by Ibis.
        """
        # Use ibis.connect which auto-detects the backend from URI
        con = cast(SQLBackend, ibis.connect(connection_string))
        return IbisAdapter(con)


# Convenience function for quick adapter creation
def create_adapter(
    data_source: Any,
    table_name: str = "source_data",
) -> IbisAdapter:
    """
    Create an IbisAdapter for the given data source.

    This is a convenience function that creates a DataSourceMapper
    and returns the appropriate adapter.

    Args:
        data_source: The data source (DataFrame, connection string, etc.)
        table_name: Name to register the table as (for DataFrame inputs)

    Returns:
        IbisAdapter configured for the data source

    Example:
        >>> adapter = create_adapter(pandas_df, table_name="my_table")
        >>> adapter = create_adapter("postgresql://localhost/mydb")
    """
    mapper = DataSourceMapper()
    return mapper.get_adapter(data_source, table_name)
