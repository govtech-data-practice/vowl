"""Annotated output marks as many rows as the row numbers count, wherever both are exact.

Counting and marking merge failed rows separately (see
docs/design-considerations/checks/how-attributed-rows-work.md). They pick the same checks and the same match
key, so on an exact schema the rows with a ``check_info`` in the annotated
table must equal the schema's ``failed_rows``.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
import test_row_quality as rq

from vowl.adapters.ibis_adapter import IbisAdapter
from vowl.config import ValidationConfig

_skip_contract_validation = rq._skip_contract_validation

_PK = [{"name": "id", "primaryKey": True, "primaryKeyPosition": 1}, {"name": "c"}]


def _assert_marks_match_counts(result) -> int:
    """Assert the invariant on every exact schema, and return how many were checked."""
    report = result._row_quality().report()
    annotated = result.get_annotated_output()["annotated"]
    checked = 0
    for schema in report.schemas:
        if schema.approximate or schema.failed_rows is None or schema.schema_name not in annotated:
            continue
        rows = annotated[schema.schema_name].to_arrow().column("check_info").to_pylist()
        marked = sum(1 for cell in rows if cell is not None)
        assert marked == schema.failed_rows, schema.schema_name
        checked += 1
    return checked


def _duplicates():
    return rq._mixed("duckdb")[1]


def _tolerated_rows_attributed():
    return rq._mixed("sqlite", ValidationConfig(fetch_tolerated_rows=True))[1]


def _pk_subset_check():
    con = rq._connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (2, -2), (3, 3), (4, 9)")
    subset = {"name": "ids_negative", "query": "SELECT COUNT(*) FROM (SELECT id FROM t WHERE c < 0) AS s", "mustBe": 0}
    return rq._validate(con, [rq._schema("t", [subset, rq._check("nine", "c = 9")], _PK)])


def _anchor_projection_join():
    con = rq._connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (1, -1), (2, -2), (3, -3), (4, 4)")
    con.raw_sql("CREATE TABLE l (id INTEGER, k INTEGER)")
    con.raw_sql("INSERT INTO l VALUES (1, 1), (1, 2), (2, 1), (3, 1)")
    joined = {"name": "fan", "query": "SELECT t.* FROM t JOIN l ON t.id = l.id WHERE t.c < -1", "mustBe": 0}
    return rq._validate(con, [rq._schema("t", [joined, rq._check("four", "c = 4")])])


def _mixed_routes():
    return rq._two_sources()[1]


def _sqlite_one_and_one_point_zero():
    con = rq._connect("sqlite")
    con.raw_sql("CREATE TABLE t (id INTEGER, c)")
    con.raw_sql("INSERT INTO t VALUES (1, 1), (1, 1.0), (1, 1), (2, 5)")
    checks = [rq._check("integer", "typeof(c) = 'integer'"), rq._check("real", "typeof(c) = 'real'")]
    return rq._validate(con, [rq._schema("t", checks)])


_SCENARIOS: dict[str, tuple[Callable, bool]] = {
    # name: (build the result, whether at least one schema must be exact)
    "duplicates": (_duplicates, True),
    "tolerated_rows_attributed": (_tolerated_rows_attributed, True),
    "pk_subset_check": (_pk_subset_check, True),
    "anchor_projection_join": (_anchor_projection_join, True),
    "mixed_routes": (_mixed_routes, True),
    # An untyped SQLite column with mixed values cannot be exported, so there
    # is no annotated table to compare. The helper must still run.
    "sqlite_one_and_one_point_zero": (_sqlite_one_and_one_point_zero, False),
}


@pytest.mark.parametrize("scenario", list(_SCENARIOS))
def test_marks_match_counts(scenario: str):
    build, must_check = _SCENARIOS[scenario]

    checked = _assert_marks_match_counts(build())

    if must_check:
        assert checked >= 1


def _partial_contract(monkeypatch: pytest.MonkeyPatch):
    def no_types(self, schema_name):
        raise NotImplementedError

    monkeypatch.setattr(IbisAdapter, "get_column_types", no_types)
    con = rq._connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER, extra INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1, 0), (2, 2, 0), (3, -3, 0)")
    return rq._validate(con, [rq._schema("t", [rq._check("negative", "c < 0")])])


def test_unknown_column_types_with_a_partial_contract_count_on_the_exported_table(monkeypatch: pytest.MonkeyPatch):
    # The contract lists two of the table's three columns, and the adapter
    # cannot give the column types. Counting takes the columns from the
    # exported table, as annotated output does.
    result = _partial_contract(monkeypatch)

    check = rq._check_rows(result)["negative"]
    assert (check["row_level"], check["route"], check["attributed_rows"], check["approximate"]) == (
        True,
        "client_lookup",
        2,
        False,
    )
    assert rq._schema_row(result)["failed_rows"] == 2
    assert _assert_marks_match_counts(result) == 1
