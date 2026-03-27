from __future__ import annotations

from types import SimpleNamespace

import pytest

from vowl.contracts.check_reference import (
    SQLColumnCheckReference,
    DeclaredColumnExistsCheckReference,
    LogicalTypeCheckReference,
    LogicalTypeOptionsCheckReference,
    PrimaryKeyCheckReference,
    RequiredCheckReference,
    SQLCheckReference,
    SQLTableCheckReference,
    UniqueCheckReference,
)
from vowl.contracts.contract import Contract
from vowl.contracts.models import get_latest_version


def _make_contract(
    monkeypatch: pytest.MonkeyPatch,
    *,
    schema_name: str | None = "users",
    properties: list[dict] | None = None,
    table_quality: list[dict] | None = None,
) -> Contract:
    monkeypatch.setattr("vowl.contracts.contract.validate_contract", lambda data, version: None)
    return Contract(
        {
            "apiVersion": get_latest_version(),
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "test-contract",
            "status": "active",
            "schema": [
                {
                    "name": schema_name,
                    "properties": properties
                    or [
                        {
                            "name": "id",
                            "logicalType": "integer",
                            "logicalTypeOptions": {
                                "minimum": 1,
                                "maximum": 10,
                                "exclusiveMinimum": 0,
                                "exclusiveMaximum": 11,
                                "multipleOf": 2,
                            },
                            "required": True,
                            "unique": True,
                            "primaryKey": True,
                        },
                        {
                            "name": "name",
                            "logicalType": "string",
                            "logicalTypeOptions": {
                                "minLength": 2,
                                "maxLength": 5,
                                "pattern": "^A",
                            },
                            "quality": [
                                {
                                    "name": "name_check",
                                    "type": "sql",
                                    "query": "SELECT COUNT(*) FROM users WHERE name IS NULL",
                                    "mustBe": 0,
                                }
                            ],
                        },
                    ],
                    "quality": table_quality
                    or [
                        {
                            "name": "table_check",
                            "type": "sql",
                            "query": "SELECT COUNT(*) FROM users",
                            "mustBe": 0,
                        }
                    ],
                }
            ],
        }
    )


class _StubSQLCheckReference(SQLCheckReference):
    def __init__(self, query: str):
        super().__init__(SimpleNamespace(resolve=lambda path: None, resolve_parent=lambda path, levels: "$"), "$.stub")
        self._query = query

    def get_schema_name(self) -> str | None:
        return None

    def get_schema_path(self) -> str:
        return "$"

    def get_query(self, dialect: str, filter_conditions=None, use_try_cast: bool = False) -> str:
        return self._query


def test_check_reference_navigation_and_defaults(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch)
    table_ref = SQLTableCheckReference(contract, "$.schema[0].quality[0]")
    column_ref = SQLColumnCheckReference(contract, "$.schema[0].properties[1].quality[0]")

    assert table_ref.contract is contract
    assert table_ref.get_logical_type() is None
    assert table_ref.get_logical_type_options() is None
    assert table_ref.get_column_name() is None
    assert column_ref.get_schema_name() == "users"
    assert column_ref.get_schema_path() == "$.schema[0]"
    assert column_ref.get_column_path() == "$.schema[0].properties[1]"
    assert column_ref.get_column_name() == "name"
    assert column_ref.get_logical_type() == "string"
    assert column_ref.get_logical_type_options() == {"minLength": 2, "maxLength": 5, "pattern": "^A"}


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("", None),
        ("SELECT * FROM", None),
        ("DELETE FROM users", None),
        ("SELECT id FROM users", "SELECT id FROM users"),
        ("SELECT COUNT(*) FROM users WHERE id > 1", "SELECT * FROM users WHERE id > 1"),
        ("SELECT AVG(price) FROM products", None),
    ],
)
def test_get_failed_rows_query_handles_empty_invalid_and_count_queries(query: str, expected: str | None):
    ref = _StubSQLCheckReference(query)

    assert ref.get_failed_rows_query("postgres") == expected


def test_apply_filters_warns_and_returns_original_query_on_parse_failure():
    query = "SELECT * FROM"

    with pytest.warns(UserWarning, match="Failed to parse SQL query for filter application"):
        result = SQLCheckReference.apply_filters(
            query,
            "postgres",
            {"users": {"field": "id", "operator": ">", "value": 1}},
        )

    assert result == query


def test_apply_filters_handles_queries_without_tables():
    assert SQLCheckReference.apply_filters(
        "SELECT 1",
        "postgres",
        {"users": {"field": "id", "operator": ">", "value": 1}},
    ) == "SELECT 1"


def test_apply_filters_keeps_unmatched_tables_unchanged():
    query = "SELECT * FROM users"

    result = SQLCheckReference.apply_filters(
        query,
        "postgres",
        {"orders": {"field": "status", "operator": "=", "value": "open"}},
    )

    assert result == query


def test_apply_filters_supports_globs_lists_and_aliases():
    query = "SELECT * FROM users AS u JOIN orders AS o ON u.id = o.user_id"

    result = SQLCheckReference.apply_filters(
        query,
        "postgres",
        {
            "us*": [
                {"field": "id", "operator": ">", "value": 1},
                {"field": "active", "operator": "=", "value": True},
            ],
            "orders": {"field": "status", "operator": "=", "value": "open"},
        },
    )

    assert 'FROM (SELECT * FROM users WHERE "id" > 1 AND "active" = TRUE) AS u' in result
    assert "JOIN (SELECT * FROM orders WHERE \"status\" = 'open') AS o" in result


@pytest.mark.parametrize(
    ("literal", "expected"),
    [
        (True, "BOOLEAN"),
        (7, "BIGINT"),
        (3.5, "DOUBLE"),
        ("2024-01-02", "DATE"),
        ("2024-01-02 03:04", "TIMESTAMP"),
        ("plain-text", None),
        (object(), None),
    ],
)
def test_infer_type_from_literal_covers_supported_variants(literal: object, expected: str | None):
    assert SQLCheckReference._infer_type_from_literal(literal) == expected


def test_apply_try_cast_rewrites_casts_and_numeric_literal_comparisons():
    query = (
        "SELECT * FROM users "
        "WHERE CAST(raw_id AS INT) = 1 AND score = 2 AND 3 < attempts AND note = 'plain-text'"
    )

    result, modified = SQLCheckReference.apply_try_cast(query, "duckdb")

    assert modified is True
    assert "TRY_CAST(raw_id AS INT)" in result
    assert "TRY_CAST(score AS BIGINT) = 2" in result
    assert "3 < TRY_CAST(attempts AS BIGINT)" in result
    assert "note = 'plain-text'" in result


def test_apply_try_cast_returns_original_for_unhandled_or_invalid_queries():
    untouched_query = "SELECT * FROM users WHERE note = 'plain-text'"
    invalid_query = "SELECT * FROM"

    untouched_result, untouched_modified = SQLCheckReference.apply_try_cast(untouched_query, "postgres")
    invalid_result, invalid_modified = SQLCheckReference.apply_try_cast(invalid_query, "postgres")

    assert untouched_result == untouched_query
    assert untouched_modified is False
    assert invalid_result == invalid_query
    assert invalid_modified is False


def test_generated_column_reference_accessors_return_property_context(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch)
    ref = DeclaredColumnExistsCheckReference(contract, "$.schema[0].properties[1]")

    assert ref.get_column_path() == "$.schema[0].properties[1]"
    assert ref.get_column_name() == "name"
    assert ref.get_logical_type() == "string"
    assert ref.get_logical_type_options() == {"minLength": 2, "maxLength": 5, "pattern": "^A"}
    assert ref.is_generated() is True


def test_logical_type_options_reference_rejects_unsupported_option(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch)

    with pytest.warns(UserWarning, match="Unsupported logicalTypeOptions key 'format'"):
        with pytest.raises(ValueError, match="Unsupported logicalTypeOptions key: format"):
            LogicalTypeOptionsCheckReference(contract, "$.schema[0].properties[0]", "format", "uuid")


@pytest.mark.parametrize(
    ("option_key", "option_value", "description_fragment"),
    [
        ("minLength", 2, "minimum length of 2"),
        ("maxLength", 5, "maximum length of 5"),
        ("pattern", "^A", "match pattern '^A'"),
        ("minimum", 1, ">= 1"),
        ("maximum", 10, "<= 10"),
        ("exclusiveMinimum", 0, "> 0"),
        ("exclusiveMaximum", 11, "< 11"),
        ("multipleOf", 2, "multiple of 2"),
    ],
)
def test_logical_type_options_reference_builds_queries_for_supported_options(
    monkeypatch: pytest.MonkeyPatch,
    option_key: str,
    option_value: int | str,
    description_fragment: str,
):
    contract = _make_contract(monkeypatch)
    property_index = 1 if option_key in {"minLength", "maxLength", "pattern"} else 0
    ref = LogicalTypeOptionsCheckReference(
        contract,
        f"$.schema[0].properties[{property_index}]",
        option_key,
        option_value,
    )

    check = ref.get_check()

    assert check["type"] == "sql"
    assert description_fragment in check["description"]
    assert "COUNT(*)" in check["query"]


def test_logical_type_options_reference_raises_for_missing_query_implementation(
    monkeypatch: pytest.MonkeyPatch,
):
    contract = _make_contract(monkeypatch)
    ref = LogicalTypeOptionsCheckReference(contract, "$.schema[0].properties[0]", "minimum", 1)
    ref._cached_ast = None
    ref._option_key = "unexpectedOption"

    with pytest.raises(ValueError, match="No query implementation for logicalTypeOptions key 'unexpectedOption'"):
        ref._build_ast()


def test_logical_type_check_reference_uses_numeric_integrality(monkeypatch: pytest.MonkeyPatch):
    contract = _make_contract(monkeypatch)
    ref = LogicalTypeCheckReference(contract, "$.schema[0].properties[0]")

    query = ref.get_query("duckdb")

    assert "TRY_CAST" in query.upper()
    assert "DOUBLE" in query.upper()
    assert "BIGINT" in query.upper()


def test_logical_type_check_reference_flags_only_non_integer_rows(monkeypatch: pytest.MonkeyPatch):
    duckdb = pytest.importorskip("duckdb")

    contract = _make_contract(monkeypatch)
    ref = LogicalTypeCheckReference(contract, "$.schema[0].properties[0]")

    con = duckdb.connect()
    con.execute("CREATE TABLE users(id VARCHAR)")
    con.execute("INSERT INTO users VALUES ('44'), ('44.0'), ('44.5'), ('abc'), (NULL)")

    result = con.execute(ref.get_query("duckdb")).fetchone()[0]
    failed_rows = con.execute(ref.get_failed_rows_query("duckdb")).fetchall()

    assert result == 2
    assert failed_rows == [("44.5",), ("abc",)]


@pytest.mark.parametrize(
    "ref_cls",
    [
        DeclaredColumnExistsCheckReference,
        LogicalTypeCheckReference,
        RequiredCheckReference,
        UniqueCheckReference,
        PrimaryKeyCheckReference,
    ],
)
def test_generated_references_warn_and_raise_when_schema_context_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    ref_cls: type,
):
    contract = _make_contract(
        monkeypatch,
        schema_name=None,
        properties=[
            {
                "name": "id",
                "logicalType": "integer",
                "required": True,
                "unique": True,
                "primaryKey": True,
            }
        ],
    )
    ref = ref_cls(contract, "$.schema[0].properties[0]")

    with pytest.warns(UserWarning, match="Could not generate"):
        with pytest.raises(ValueError, match="Cannot generate"):
            ref._build_ast()