from __future__ import annotations

import builtins
import importlib
import importlib.metadata
import sys
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pytest
from sqlglot import exp

from vowl.adapters.base import BaseAdapter
from vowl.adapters.ibis_adapter import IbisAdapter
from vowl.adapters.models import FilterCondition, build_filter_ast
from vowl.mapper import DataSourceMapper

vowl = sys.modules["vowl"]


class DummyAdapter(BaseAdapter):
    pass


class FakeDuckDBConnection:
    def __init__(self) -> None:
        self.created_tables: list[tuple[str, pa.Table]] = []

    def create_table(self, table_name: str, table: pa.Table) -> None:
        self.created_tables.append((table_name, table))


class FakeNarwhalsFrame:
    def __init__(self, stage: str = "initial") -> None:
        self.stage = stage
        self.columns = ["value"]

    def to_arrow(self) -> pa.Table:
        if self.stage == "initial":
            raise TypeError("Conversion failed for column value with type object")
        return pa.table({"value": ["1", None]})

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame({"value": [1, None]})

    def with_columns(self, *args, **kwargs) -> FakeNarwhalsFrame:
        for arg in args:
            if hasattr(arg, "__iter__") and not isinstance(arg, (str, bytes)):
                list(arg)
        if self.stage == "initial":
            return FakeNarwhalsFrame(stage="fallback")
        return self


class FakeNarwhalsFrameFatal(FakeNarwhalsFrame):
    def to_arrow(self) -> pa.Table:
        raise RuntimeError("boom")


class FakeSparkSession:
    pass


class FakeSparkDataFrame:
    def __init__(self, spark_session: FakeSparkSession) -> None:
        self.sparkSession = spark_session
        self.registered_name: str | None = None

    def createOrReplaceTempView(self, table_name: str) -> None:
        self.registered_name = table_name


@pytest.mark.parametrize(
    ("operator", "expected_type", "expected_fragment"),
    [
        ("=", exp.EQ, '"age" = 5'),
        ("!=", exp.NEQ, '"age" <> 5'),
        (">", exp.GT, '"age" > 5'),
        (">=", exp.GTE, '"age" >= 5'),
        ("<", exp.LT, '"age" < 5'),
        ("<=", exp.LTE, '"age" <= 5'),
    ],
)
def test_filter_condition_to_ast_for_comparison_operators(operator, expected_type, expected_fragment):
    ast = FilterCondition(field="age", operator=operator, value=5).to_ast()

    assert isinstance(ast, expected_type)
    assert ast.sql() == expected_fragment


def test_filter_condition_to_ast_for_in_and_not_in():
    in_ast = FilterCondition(field="status", operator="IN", value=["active", "pending"]).to_ast()
    not_in_ast = FilterCondition(field="status", operator="NOT IN", value="archived").to_ast()

    assert isinstance(in_ast, exp.In)
    assert in_ast.sql() == '"status" IN (\'active\', \'pending\')'
    assert isinstance(not_in_ast, exp.Not)
    assert not_in_ast.sql() == 'NOT "status" IN (\'archived\')'


def test_filter_condition_to_ast_for_like_and_not_like():
    like_ast = FilterCondition(field="name", operator="LIKE", value="A%").to_ast()
    not_like_ast = FilterCondition(field="name", operator="NOT LIKE", value="B%").to_ast()

    assert isinstance(like_ast, exp.Like)
    assert like_ast.sql() == '"name" LIKE \'A%\''
    assert isinstance(not_like_ast, exp.Not)
    assert not_like_ast.sql() == 'NOT "name" LIKE \'B%\''


def test_filter_condition_to_ast_for_null_operators():
    is_null_ast = FilterCondition(field="deleted_at", operator="IS NULL").to_ast()
    is_not_null_ast = FilterCondition(field="deleted_at", operator="IS NOT NULL").to_ast()

    assert is_null_ast.sql() == '"deleted_at" IS NULL'
    assert is_not_null_ast.sql() == 'NOT "deleted_at" IS NULL'


@pytest.mark.parametrize(
    ("value", "expected_sql"),
    [
        (None, "NULL"),
        (True, "TRUE"),
        (5, "5"),
        (3.5, "3.5"),
        ("abc", "'abc'"),
    ],
)
def test_filter_condition_to_literal(value, expected_sql):
    literal = FilterCondition._to_literal(value)

    assert literal.sql() == expected_sql


def test_filter_condition_invalid_operator_raises_value_error():
    with pytest.raises(ValueError, match="Unsupported operator"):
        FilterCondition(field="age", operator="BETWEEN", value=5).to_ast()


def test_build_filter_ast_accepts_single_filter_condition_and_dict_input():
    dataclass_ast = build_filter_ast(FilterCondition(field="age", operator=">=", value=18))
    dict_ast = build_filter_ast({"field": "status", "operator": "=", "value": "active"})

    assert dataclass_ast.sql() == '"age" >= 18'
    assert dict_ast.sql() == '"status" = \'active\''


def test_build_filter_ast_combines_multiple_conditions_with_and():
    ast = build_filter_ast(
        [
            FilterCondition(field="age", operator=">=", value=18),
            {"field": "status", "operator": "=", "value": "active"},
        ]
    )

    assert isinstance(ast, exp.And)
    assert ast.sql() == '"age" >= 18 AND "status" = \'active\''


def test_mapper_returns_existing_ibis_adapter_unchanged():
    mapper = DataSourceMapper()
    backend = SimpleNamespace(raw_sql=lambda query: None)
    adapter = IbisAdapter(backend)

    assert mapper.get_adapter(adapter) is adapter


def test_is_spark_dataframe_returns_false_when_pyspark_types_are_unavailable(monkeypatch: pytest.MonkeyPatch):
    from vowl import mapper as mapper_module

    monkeypatch.setattr(mapper_module, "_spark_types", lambda: None)

    assert mapper_module._is_spark_dataframe(object()) is False


def test_is_spark_session_returns_false_when_pyspark_types_are_unavailable(monkeypatch: pytest.MonkeyPatch):
    from vowl import mapper as mapper_module

    monkeypatch.setattr(mapper_module, "_spark_types", lambda: None)

    assert mapper_module._is_spark_session(object()) is False


def test_spark_types_returns_none_when_pyspark_import_fails(monkeypatch: pytest.MonkeyPatch):
    from vowl import mapper as mapper_module

    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "pyspark.sql":
            raise ImportError("pyspark is unavailable")
        return original_import(name, globals, locals, fromlist, level)

    mapper_module._spark_types.cache_clear()
    monkeypatch.setattr(builtins, "__import__", fake_import)

    assert mapper_module._spark_types() is None

    mapper_module._spark_types.cache_clear()


def test_mapper_rejects_non_ibis_base_adapter():
    mapper = DataSourceMapper()

    with pytest.raises(TypeError, match="Only IbisAdapter is supported"):
        mapper.get_adapter(DummyAdapter())


def test_mapper_rejects_unknown_objects():
    mapper = DataSourceMapper()

    with pytest.raises(TypeError, match="Unsupported data source type"):
        mapper.get_adapter(object())


def test_mapper_wraps_raw_sql_duck_typed_backend():
    mapper = DataSourceMapper()
    backend = SimpleNamespace(raw_sql=lambda query: None)

    adapter = mapper.get_adapter(backend)

    assert isinstance(adapter, IbisAdapter)
    assert adapter.get_connection() is backend


def test_mapper_creates_adapter_from_local_dataframe(monkeypatch: pytest.MonkeyPatch):
    mapper = DataSourceMapper()
    fake_connection = FakeDuckDBConnection()
    df = pd.DataFrame({"value": [1, 2, 3]})

    monkeypatch.setattr("vowl.mapper.ibis.duckdb.connect", lambda: fake_connection)

    adapter = mapper.get_adapter(df, "numbers")

    assert isinstance(adapter, IbisAdapter)
    assert len(fake_connection.created_tables) == 1
    assert fake_connection.created_tables[0][0] == "numbers"


def test_mapper_uses_ibis_connect_for_connection_strings(monkeypatch: pytest.MonkeyPatch):
    mapper = DataSourceMapper()
    backend = SimpleNamespace(raw_sql=lambda query: None)

    monkeypatch.setattr("vowl.mapper.ibis.connect", lambda connection_string: backend)

    adapter = mapper.get_adapter("duckdb://")

    assert isinstance(adapter, IbisAdapter)
    assert adapter.get_connection() is backend


def test_mapper_falls_back_to_string_casts_when_arrow_conversion_fails(monkeypatch: pytest.MonkeyPatch):
    mapper = DataSourceMapper()
    fake_connection = FakeDuckDBConnection()

    monkeypatch.setattr("vowl.mapper.ibis.duckdb.connect", lambda: fake_connection)
    monkeypatch.setattr("vowl.mapper.nw.from_native", lambda df, eager_only=True: FakeNarwhalsFrame())

    with pytest.warns(UserWarning, match="Arrow type conversion failed"):
        adapter = mapper.get_adapter({"placeholder": "value"}, "test_table")

    assert isinstance(adapter, IbisAdapter)
    assert len(fake_connection.created_tables) == 1
    assert fake_connection.created_tables[0][0] == "test_table"


def test_mapper_only_stringifies_problematic_columns_on_fallback(monkeypatch: pytest.MonkeyPatch):
    mapper = DataSourceMapper()
    fake_connection = FakeDuckDBConnection()
    df = pd.DataFrame(
        {
            "good_int": [1, 2, 3],
            "bad_mixed": [1, "oops", 3],
        }
    )

    monkeypatch.setattr("vowl.mapper.ibis.duckdb.connect", lambda: fake_connection)

    with pytest.warns(UserWarning, match="loading problematic columns as strings: bad_mixed"):
        adapter = mapper.get_adapter(df, "test_table")

    assert isinstance(adapter, IbisAdapter)
    assert len(fake_connection.created_tables) == 1

    arrow_table = fake_connection.created_tables[0][1]
    assert arrow_table.schema.field("good_int").type == pa.int64()
    assert pa.types.is_string(arrow_table.schema.field("bad_mixed").type) or pa.types.is_large_string(arrow_table.schema.field("bad_mixed").type)


def test_mapper_reraises_non_type_related_dataframe_conversion_errors(monkeypatch: pytest.MonkeyPatch):
    mapper = DataSourceMapper()

    monkeypatch.setattr("vowl.mapper.ibis.duckdb.connect", FakeDuckDBConnection)
    monkeypatch.setattr("vowl.mapper.nw.from_native", lambda df, eager_only=True: FakeNarwhalsFrameFatal())

    with pytest.raises(RuntimeError, match="boom"):
        mapper.get_adapter({"placeholder": "value"}, "test_table")


def test_mapper_creates_adapter_from_spark_dataframe(monkeypatch: pytest.MonkeyPatch):
    mapper = DataSourceMapper()
    backend = SimpleNamespace(raw_sql=lambda query: None)
    spark_session = FakeSparkSession()
    spark_df = FakeSparkDataFrame(spark_session)

    monkeypatch.setattr("vowl.mapper._spark_types", lambda: (FakeSparkSession, FakeSparkDataFrame))
    monkeypatch.setattr("vowl.mapper.ibis.pyspark.connect", lambda session: backend)

    adapter = mapper.get_adapter(spark_df, "employees")

    assert isinstance(adapter, IbisAdapter)
    assert adapter.get_connection() is backend
    assert spark_df.registered_name == "employees"


def test_mapper_creates_adapter_from_spark_session(monkeypatch: pytest.MonkeyPatch):
    mapper = DataSourceMapper()
    backend = SimpleNamespace(raw_sql=lambda query: None)
    spark_session = FakeSparkSession()

    monkeypatch.setattr("vowl.mapper._spark_types", lambda: (FakeSparkSession, FakeSparkDataFrame))
    monkeypatch.setattr("vowl.mapper.ibis.pyspark.connect", lambda session: backend)

    adapter = mapper.get_adapter(spark_session)

    assert isinstance(adapter, IbisAdapter)
    assert adapter.get_connection() is backend


def test_create_adapter_convenience_function_uses_mapper(monkeypatch: pytest.MonkeyPatch):
    backend = SimpleNamespace(raw_sql=lambda query: None)
    expected_adapter = IbisAdapter(backend)

    monkeypatch.setattr(DataSourceMapper, "get_adapter", lambda self, data_source, table_name: expected_adapter)

    adapter = vowl.create_adapter({"placeholder": "value"}, "test_table")

    assert adapter is expected_adapter


def test_vowl_dunder_dir_exposes_public_api():
    members = vowl.__dir__()

    assert "Contract" in members
    assert "DataSourceMapper" in members
    assert "validate_data" in members


def test_vowl_version_falls_back_when_package_metadata_is_missing(monkeypatch: pytest.MonkeyPatch):
    with monkeypatch.context() as context:
        def raise_not_found(_: str) -> str:
            raise importlib.metadata.PackageNotFoundError

        context.setattr(importlib.metadata, "version", raise_not_found)
        reloaded = importlib.reload(vowl)
        assert reloaded.__version__ == "0.0.0"

    importlib.reload(vowl)
