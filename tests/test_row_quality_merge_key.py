"""The declared primary key as the default match key, and the shared mergeability rule.

A unique declared primary key groups the rows as the full columns do, so it is
the match key whenever the data source can run the pushdown and no key value
appears twice. See docs/design-considerations/failed-rows/how-rows-are-counted.md.
"""

from __future__ import annotations

import json

import pytest
import test_row_quality as rq

import vowl.validation.row_quality as row_quality_module
from vowl.validation.result import ValidationResult
from vowl.validation.row_quality.mergeable import METADATA_COLUMNS, rows_mergeable
from vowl.validation.row_quality.selection import (
    REASON_NOT_MERGEABLE,
    REASON_PK_NOT_UNIQUE,
    REASON_PK_UNCHECKED,
    REASON_UNMATCHED,
)

_skip_contract_validation = rq._skip_contract_validation

_PK = [{"name": "id", "primaryKey": True, "primaryKeyPosition": 1}, {"name": "c"}]
_SUBSET = {"name": "ids_negative", "query": "SELECT COUNT(*) FROM (SELECT id FROM t WHERE c < 0) AS s", "mustBe": 0}


def _table(rows: str, backend: str = "duckdb"):
    con = rq._connect(backend)
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql(f"INSERT INTO t VALUES {rows}")
    return con


def _marked(result) -> dict[int, set[str]]:
    rows = result.get_annotated_output()["annotated"]["t"].to_arrow().to_pylist()
    return {
        row["id"]: {item["check_name"] for item in json.loads(row["check_info"])} for row in rows if row["check_info"]
    }


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_a_unique_primary_key_is_the_match_key_for_full_column_checks(backend: str):
    con = _table("(1, -1), (2, 6), (3, -3), (4, 2), (5, NULL)", backend)
    checks = [rq._check("negative", "c < 0"), rq._check("big", "c > 5"), rq._check("null_c", "c IS NULL")]

    keyed = rq._validate(con, [rq._schema("t", checks, _PK)])
    unkeyed = rq._validate(con, [rq._schema("t", checks)])

    assert keyed._row_quality().merge_key("t") == ["id"]
    assert unkeyed._row_quality().merge_key("t") is None
    assert rq._schema_row(keyed)["failed_rows"] == rq._schema_row(unkeyed)["failed_rows"] == 4
    assert rq._schema_row(keyed)["exact"] is True
    # The keyed contract also generates a primary key check, which passes.
    assert rq._dimension_rows(keyed)["validity"] == rq._dimension_rows(unkeyed)["validity"]
    assert _marked(keyed) == _marked(unkeyed)


def test_a_primary_key_that_is_every_column_is_not_a_merge_key():
    con = rq._connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1), (-2)")

    result = rq._validate(con, [rq._schema("t", [rq._check("negative", "id < 0")], [_PK[0]])])

    assert result._row_quality().merge_key("t") is None
    assert rq._schema_row(result)["failed_rows"] == 1


def test_a_duplicated_primary_key_is_not_attributed_with_its_own_reason():
    con = _table("(1, -1), (1, 3), (2, 5)")

    result = rq._validate(con, [rq._schema("t", [_SUBSET, rq._check("five", "c = 5")], _PK)])

    assert result._row_quality().merge_key("t") is None
    checks = rq._check_rows(result)
    assert checks["ids_negative"]["reason"] == REASON_PK_NOT_UNIQUE
    # Still about bad rows, so counted, but its rows become residues.
    assert (checks["ids_negative"]["counted"], checks["ids_negative"]["attributed"]) == (True, False)
    assert checks["ids_negative"]["attributed_rows"] is None
    # A full-column check still merges on the full columns.
    assert checks["five"]["counted"] is True
    # The two id = 1 rows fail the generated primary key check, and (2, 5) fails five.
    schema = rq._schema_row(result)
    assert (schema["failed_rows"], schema["checks_not_attributed"], schema["exact"]) == (3, 1, False)
    assert "t::ids_negative" in result.get_annotated_output()["residues"]


def test_a_failed_probe_falls_back_with_its_own_reason(monkeypatch: pytest.MonkeyPatch):
    def broken(spec):
        raise RuntimeError("probe failed")

    monkeypatch.setattr(row_quality_module, "duplicate_key_statement", broken)
    con = _table("(1, -1), (2, 5)")

    result = rq._validate(con, [rq._schema("t", [_SUBSET, rq._check("five", "c = 5")], _PK)])

    assert result._row_quality().merge_key("t") is None
    assert rq._check_rows(result)["ids_negative"]["reason"] == REASON_PK_UNCHECKED
    assert rq._schema_row(result)["failed_rows"] == 1


def test_a_check_without_the_primary_key_keeps_the_general_reason():
    con = _table("(1, -1), (1, 3)")
    only_c = {"name": "c_negative", "query": "SELECT COUNT(*) FROM (SELECT c FROM t WHERE c < 0) AS s", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [only_c], _PK)])

    assert rq._check_rows(result)["c_negative"]["reason"] == REASON_NOT_MERGEABLE


def test_the_primary_key_is_probed_once_per_schema(monkeypatch: pytest.MonkeyPatch):
    calls: list[object] = []
    original = row_quality_module.duplicate_key_statement

    def counting(spec):
        calls.append(spec)
        return original(spec)

    monkeypatch.setattr(row_quality_module, "duplicate_key_statement", counting)
    con = _table("(1, -1), (2, -2), (3, 3)")
    checks = [_SUBSET, rq._check("negative", "c < 0"), rq._check("three", "c = 3")]

    result = rq._validate(con, [rq._schema("t", checks, _PK)])
    result.get_row_quality_df()
    result.get_annotated_output()

    assert len(calls) == 1


def test_a_table_match_check_with_transformed_values_matches_on_the_primary_key():
    # The check returns the table's column names, but c holds a changed value,
    # so no table row has the same values in every column.
    con = _table("(1, -1), (2, -2), (3, 3)")
    shifted = {
        "name": "shifted",
        "query": "SELECT COUNT(*) FROM (SELECT id, c * 10 AS c FROM t WHERE c < 0) AS s",
        "mustBe": 0,
    }

    keyed = rq._validate(con, [rq._schema("t", [shifted], _PK)])
    unkeyed = rq._validate(con, [rq._schema("t", [shifted])])

    assert rq._check_rows(keyed)["shifted"]["route"] == "server_lookup"
    # c * 10 is not a plain column, so the check is marked approximate.
    assert rq._check_rows(keyed)["shifted"]["exact"] is False
    assert rq._schema_row(keyed)["failed_rows"] == 2
    assert _marked(keyed) == {1: {"shifted"}, 2: {"shifted"}}
    assert rq._schema_row(unkeyed)["failed_rows"] == 0
    # No table row has the changed values, which the count reports.
    shifted_row = rq._check_rows(unkeyed)["shifted"]
    assert (shifted_row["reason"], shifted_row["exact"]) == (REASON_UNMATCHED, False)


# ---------------------------------------------------------------------------
# rows_mergeable
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("row_columns", "key", "expected"),
    [
        (["id", "c"], None, True),
        (["c", "id"], None, True),
        (["id"], None, False),
        (["id", "c", "extra"], None, False),
        ([*METADATA_COLUMNS, "id", "c"], None, True),
        (["id"], ["id"], True),
        (["id", "extra"], ["id"], True),
        (["c"], ["id"], False),
        (["id", "check_info"], ["id", "c"], False),
        ([], [], False),
    ],
)
def test_rows_mergeable(row_columns: list[str], key: list[str] | None, expected: bool):
    assert rows_mergeable(row_columns, ["id", "c"], key) is expected


def test_annotated_output_uses_the_shared_rule():
    import narwhals as nw
    import pyarrow as pa

    rows = nw.from_native(pa.table({"id": [1], "check_info": ["[]"]}), eager_only=True)

    assert ValidationResult._is_mergeable_for_full_table(rows, {"id", "c"}, ["id"]) is True
    assert ValidationResult._is_mergeable_for_full_table(rows, {"id", "c"}) is False
    assert ValidationResult._is_mergeable_for_full_table(rows, {"id"}) is True
