"""Regression tests for Spark Connect DataFrame/Session detection (issue #39).

Spark Connect DataFrames and Sessions live in ``pyspark.sql.connect.*`` and are
separate classes from the classic ones. On pyspark 3.5 a Connect DataFrame is not
a subclass of the classic ``pyspark.sql.DataFrame``, so the original
``isinstance``-against-classic detection missed it and vowl raised
``TypeError: Unsupported data source type``. The Connect ``SparkSession`` is not a
subclass of the classic session on any version.

These tests are guarded with ``pytest.importorskip`` so the base suite still runs
without the ``spark`` extra (pyspark) or ``grpcio`` (Spark Connect).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

# Path to the shared HDB contract + data reused by the behavioural test.
TEST_DIR = Path(__file__).parent
HDB_DIR = TEST_DIR / "hdb_resale"
DATA_FILE = HDB_DIR / "HDBResaleWithErrors.csv"
CONTRACT_PATH = str(HDB_DIR / "hdb_resale.yaml")
# Schema name in the contract == temp view name the mapper registers the df under.
HDB_SCHEMA_NAME = "hdb_resale_prices"


def _ensure_java_home() -> None:
    """Auto-detect Homebrew Java on macOS, mirroring TestPySparkValidation."""
    if "JAVA_HOME" not in os.environ:
        homebrew_java = Path("/opt/homebrew/opt/openjdk@17")
        if homebrew_java.exists():
            os.environ["JAVA_HOME"] = str(homebrew_java)


# ---------------------------------------------------------------------------
# Regression guard: explicit type-set membership.
#
# A plain ``isinstance(connect_df, pyspark.sql.DataFrame)`` passes on pyspark 4
# even without the fix (Connect DF subclasses classic DF there), so it is NOT a
# sufficient guard. We assert the Connect classes are explicitly present in the
# recognised type tuples instead — this fails if Connect handling is removed.
# ---------------------------------------------------------------------------


def test_connect_dataframe_class_is_recognised() -> None:
    pytest.importorskip("pyspark", reason="PySpark not installed")
    pytest.importorskip("grpc", reason="grpcio not installed (Spark Connect unavailable)")

    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

    from vowl.mapper import _spark_dataframe_types

    assert ConnectDataFrame in _spark_dataframe_types()


def test_connect_session_class_is_recognised() -> None:
    pytest.importorskip("pyspark", reason="PySpark not installed")
    pytest.importorskip("grpc", reason="grpcio not installed (Spark Connect unavailable)")

    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

    from vowl.mapper import _spark_session_types

    assert ConnectSparkSession in _spark_session_types()


def test_classic_dataframe_class_still_recognised() -> None:
    """The classic path must keep working alongside the Connect additions."""
    pytest.importorskip("pyspark", reason="PySpark not installed")

    from pyspark.sql import DataFrame as ClassicDataFrame
    from pyspark.sql import SparkSession as ClassicSparkSession

    from vowl.mapper import _spark_dataframe_types, _spark_session_types

    assert ClassicDataFrame in _spark_dataframe_types()
    assert ClassicSparkSession in _spark_session_types()


# ---------------------------------------------------------------------------
# Behavioural: a local Spark Connect DataFrame routes to the Spark adapter and
# validates without ERROR (rather than raising TypeError: Unsupported...).
# ---------------------------------------------------------------------------


@pytest.fixture
def spark_connect_session():
    """A local Spark Connect session.

    Uses ``.remote("local[1]")`` which spins up a local Connect server. On the
    project's pyspark 4 env this needs no extra jars; on pyspark 3.5 the connect
    server jar must be provided (out of scope here — skipped if startup fails).
    """
    pytest.importorskip("pyspark", reason="PySpark not installed")
    pytest.importorskip("grpc", reason="grpcio not installed (Spark Connect unavailable)")
    _ensure_java_home()

    from pyspark.sql import SparkSession

    try:
        spark = SparkSession.builder.remote("local[1]").appName("test_vowl_connect").getOrCreate()
    except Exception as e:  # pragma: no cover - environment dependent
        pytest.skip(f"Local Spark Connect could not start: {e}")

    yield spark
    spark.stop()


@pytest.fixture
def spark_connect_dataframe(spark_connect_session):
    """A small Spark Connect DataFrame built from the HDB sample data."""
    import pandas as pd
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

    pdf = pd.read_csv(DATA_FILE).fillna("").astype(str).head(50)
    df = spark_connect_session.createDataFrame(pdf)
    # Sanity: this is genuinely a Connect DataFrame, not a classic one.
    assert isinstance(df, ConnectDataFrame)
    return df


def test_get_adapter_accepts_connect_dataframe(spark_connect_dataframe) -> None:
    """get_adapter must return an IbisAdapter, not raise TypeError (issue #39)."""
    from vowl.adapters import IbisAdapter
    from vowl.mapper import DataSourceMapper

    adapter = DataSourceMapper().get_adapter(spark_connect_dataframe, HDB_SCHEMA_NAME)

    assert isinstance(adapter, IbisAdapter)


def test_connect_dataframe_connection_test_succeeds(spark_connect_dataframe) -> None:
    """The adapter's connection test must pass over a Connect DataFrame.

    Issue #39's symptom was the connection probe raising
    ``'Column' object is not callable`` (runner.py test_connections), turning every
    check into ERROR. ``test_connection`` returns None on success or the error
    string on failure, so this asserts that exact path works end-to-end without
    the golden-output machinery.
    """
    from vowl.mapper import DataSourceMapper

    adapter = DataSourceMapper().get_adapter(spark_connect_dataframe, HDB_SCHEMA_NAME)

    assert adapter.test_connection(HDB_SCHEMA_NAME) is None


# ---------------------------------------------------------------------------
# Step 3: an undriveable remote Connect session raises a clear, actionable error
# pointing at the pandas workaround (simulated via monkeypatch, since a real
# remote cluster e.g. Databricks Connect is not available locally).
# ---------------------------------------------------------------------------


def test_undriveable_connect_session_raises_clear_error(monkeypatch) -> None:
    """When ibis cannot drive the session, surface the toPandas() workaround."""
    import ibis

    from vowl.mapper import DataSourceMapper

    class _RemoteConnectDataFrame:
        """Stub recognised as a Connect DataFrame by monkeypatching detection."""

        class _Session:
            pass

        sparkSession = _Session()

        def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802 - Spark API name
            pass

    df = _RemoteConnectDataFrame()

    # Route the stub through the Spark-df branch.
    monkeypatch.setattr("vowl.mapper._is_spark_dataframe", lambda obj: obj is df)
    monkeypatch.setattr("vowl.mapper._is_spark_session", lambda obj: False)

    def _boom(*args, **kwargs):
        raise RuntimeError("'Column' object is not callable")

    monkeypatch.setattr(ibis.pyspark, "connect", _boom)

    with pytest.raises(TypeError) as excinfo:
        DataSourceMapper().get_adapter(df, "source_data")

    message = str(excinfo.value)
    assert "toPandas()" in message
    assert "Spark Connect" in message
