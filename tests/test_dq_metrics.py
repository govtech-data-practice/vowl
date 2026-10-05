"""Tests for the DQ metrics computation and ``dq_metrics.json``.

These need no OpenTelemetry. The OTel metrics replay the same points, which
``tests/test_otel_export.py`` checks point for point.
"""

from __future__ import annotations

import json
import sys

import pandas as pd
import pytest

from vowl import validate_data as _validate_data
from vowl.contracts.contract import Contract

# See tests/test_otel_export.py: bypass the golden-file wrapper from conftest.
_run_validation = _validate_data
del _validate_data


def _contract_data() -> dict:
    return {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "version": "2.0.0",
        "id": "orders-contract",
        "status": "active",
        "schema": [
            {
                "name": "orders",
                "properties": [
                    {"name": "order_id", "required": True},
                    {
                        "name": "amount",
                        "quality": [
                            {
                                "name": "amount_non_negative",
                                "type": "sql",
                                "dimension": "consistency",
                                "query": 'SELECT COUNT(*) FROM "orders" WHERE amount < 0',
                                "mustBe": 0,
                            }
                        ],
                    },
                ],
            }
        ],
    }


@pytest.fixture
def result():
    df = pd.DataFrame({"order_id": [1, 2, 3, 4], "amount": [10.0, -5.0, 20.0, -1.0]})
    return _run_validation(contract=Contract(_contract_data()), df=df)


def _points(document: dict, name: str) -> list[dict]:
    return [point for point in document["points"] if point["name"] == name]


def test_run_id_is_made_at_run_time(result):
    assert result.run_id
    assert result.get_dq_metrics()["run"]["vowl.run.id"] == result.run_id


def test_each_run_gets_its_own_run_id():
    df = pd.DataFrame({"order_id": [1], "amount": [1.0]})
    first = _run_validation(contract=Contract(_contract_data()), df=df)
    second = _run_validation(contract=Contract(_contract_data()), df=df)
    assert first.run_id != second.run_id


def test_a_caller_set_run_id_is_used(result):
    result.run_id = "nightly-2026-09-30"
    assert result.get_dq_metrics()["run"]["vowl.run.id"] == "nightly-2026-09-30"


def test_every_level_has_check_and_row_counts(result):
    document = result.get_dq_metrics()
    names = {point["name"] for point in document["points"]}
    for level in ("check", "dimension", "schema", "run"):
        assert f"vowl.{level}.check.count" in names, level
        assert f"vowl.{level}.row.count" in names, level
        assert f"vowl.{level}.row.pass_rate" in names, level
    assert "vowl.run.schema.count" in names


def test_point_types_follow_additivity(result):
    """Counts that add up are counters. Row counts and rates are gauges."""
    for point in result.get_dq_metrics()["points"]:
        name = point["name"]
        if name.endswith((".check.count", ".schema.count")):
            assert point["type"] == "counter", name
        elif name.endswith((".row.count", ".pass_rate")):
            assert point["type"] == "gauge", name
        else:
            assert name.endswith(".duration") and point["type"] == "histogram", name


def test_run_numbers(result):
    document = result.get_dq_metrics()
    checks = {p["attributes"]["status"]: p["value"] for p in _points(document, "vowl.run.check.count")}
    assert checks == {"PASSED": 3, "FAILED": 1, "ERROR": 0}
    (check_rate,) = _points(document, "vowl.run.check.pass_rate")
    assert check_rate["value"] == 0.75
    rows = {p["attributes"]["status"]: p["value"] for p in _points(document, "vowl.run.row.count")}
    assert rows == {"PASSED": 2, "FAILED": 2}
    (row_rate,) = _points(document, "vowl.run.row.pass_rate")
    assert row_rate["value"] == 0.5
    assert row_rate["attributes"]["vowl.row_quality.exact"] is True


def test_errored_checks_count_against_the_check_pass_rate():
    data = _contract_data()
    data["schema"][0]["properties"][1]["quality"][0]["query"] = 'SELECT COUNT(*) FROM "orders" WHERE nope < 0'
    df = pd.DataFrame({"order_id": [1, 2], "amount": [1.0, 2.0]})
    errored = _run_validation(contract=Contract(data), df=df)

    document = errored.get_dq_metrics()
    checks = {p["attributes"]["status"]: p["value"] for p in _points(document, "vowl.run.check.count")}
    assert checks == {"PASSED": 3, "FAILED": 0, "ERROR": 1}
    (check_rate,) = _points(document, "vowl.run.check.pass_rate")
    assert check_rate["value"] == 0.75  # ERROR stays in the denominator
    schemas = {p["attributes"]["status"]: p["value"] for p in _points(document, "vowl.run.schema.count")}
    assert schemas == {"PASSED": 0, "FAILED": 0, "ERROR": 1}


def test_points_with_the_same_name_and_attributes_are_merged(result):
    """Two checks sharing a name in one schema merge as OpenTelemetry would:
    counters add up, the last gauge wins, and every timing is kept."""
    failing = next(cr for cr in result.check_results if cr.status == "FAILED")
    result.check_results.append(failing)

    document = result.get_dq_metrics()
    counts = [
        p
        for p in _points(document, "vowl.check.check.count")
        if p["attributes"]["check_name"] == failing.check_name and p["attributes"]["status"] == "FAILED"
    ]
    assert [p["value"] for p in counts] == [2]
    rows = [
        p
        for p in _points(document, "vowl.check.row.count")
        if p["attributes"]["check_name"] == failing.check_name and p["attributes"]["status"] == "FAILED"
    ]
    assert len(rows) == 1
    timings = [
        p for p in _points(document, "vowl.check.duration") if p["attributes"]["check_name"] == failing.check_name
    ]
    assert len(timings) == 2


def test_save_writes_the_dq_metrics_document(result, tmp_path):
    result.save(str(tmp_path), prefix="dq", output_mode="annotated")
    document = json.loads((tmp_path / "dq_dq_metrics.json").read_text())

    assert document == json.loads(json.dumps(result.get_dq_metrics(), default=str))
    assert document["schema_version"] == 1
    assert document["run"]["vowl.contract.id"] == "orders-contract"
    assert document["run_started_at"] and document["run_finished_at"]


def test_dq_metrics_needs_no_opentelemetry(monkeypatch, result):
    monkeypatch.setitem(sys.modules, "opentelemetry", None)
    assert result.get_dq_metrics()["points"]


# ---------------------------------------------------------------------------
# The check level reports the check's own scalar count, the higher levels the
# attributed rows. See "How failed rows are counted" in the docs.


def _row_points(document: dict, level: str, **attributes) -> dict:
    points = _points(document, f"vowl.{level}.row.count")
    values = {
        p["attributes"]["status"]: p["value"]
        for p in points
        if all(p["attributes"].get(key) == value for key, value in attributes.items())
    }
    (rate,) = [
        p["value"]
        for p in _points(document, f"vowl.{level}.row.pass_rate")
        if all(p["attributes"].get(key) == value for key, value in attributes.items())
    ]
    return {**values, "pass_rate": rate}


@pytest.fixture
def rq(monkeypatch):
    import test_row_quality

    monkeypatch.setattr(test_row_quality.contract_module, "validate_contract", lambda data, version: None)
    return test_row_quality


def _duckdb_table(rq, rows: str):
    con = rq._connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql(f"INSERT INTO t VALUES {rows}")
    return con


def test_a_tolerated_check_reports_the_rows_it_caught(rq):
    con = _duckdb_table(rq, "(1, -1), (2, -2), (3, 3), (4, 4)")
    result = rq._validate(con, [rq._schema("t", [rq._check("tolerated", "c < 0", mustBeLessThan=10)])])

    document = result.get_dq_metrics()
    assert result.check_results[0].status == "PASSED"
    assert _row_points(document, "check", check_name="tolerated") == {"PASSED": 2, "FAILED": 2, "pass_rate": 0.5}
    # Under the default scope a check that passed adds no failed rows to the schema.
    assert _row_points(document, "schema", schema_name="t") == {"PASSED": 4, "FAILED": 0, "pass_rate": 1.0}


def test_a_join_that_fans_out_goes_negative_at_check_level_only(rq):
    con = _duckdb_table(rq, "(1, -1), (1, -1), (2, -2), (3, 3)")
    con.raw_sql("CREATE TABLE l (id INTEGER)")
    con.raw_sql("INSERT INTO l VALUES (1), (1), (1), (2)")
    fan = {"name": "fan", "query": "SELECT t.* FROM t JOIN l ON t.id = l.id WHERE t.c < 0", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [fan])])

    document = result.get_dq_metrics()
    # The join returns 7 rows from a table of 4. Neither number is clamped.
    assert _row_points(document, "check", check_name="fan") == {"PASSED": -3, "FAILED": 7, "pass_rate": -0.75}
    assert rq._check_rows(result)["fan"]["attributed_rows"] == 3
    assert _row_points(document, "schema", schema_name="t") == {"PASSED": 1, "FAILED": 3, "pass_rate": 0.25}

    pytest.importorskip("opentelemetry")
    from vowl.otel._common import check_row_attributes

    (check,) = [cr for cr in result.check_results if cr.check_name == "fan"]
    assert check_row_attributes(result)[id(check)] == {
        "row.count.passed": -3,
        "row.count.failed": 7,
        "row.pass_rate": -0.75,
    }


def test_distinct_reports_the_scalar_at_check_level(rq):
    con = _duckdb_table(rq, "(1, 2), (1, 2), (1, 2), (2, 3)")
    check = {"name": "d", "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c = 2) AS s", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check])])

    document = result.get_dq_metrics()
    assert _row_points(document, "check", check_name="d")["FAILED"] == 1
    assert rq._check_rows(result)["d"]["attributed_rows"] == 3
    assert _row_points(document, "schema", schema_name="t")["FAILED"] == 3


def test_count_distinct_reports_the_values_it_counted(rq):
    con = _duckdb_table(rq, "(1, -1), (2, -1), (3, -2), (4, 3)")
    check = {"name": "values", "query": "SELECT COUNT(DISTINCT c) FROM t WHERE c < 0", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check])])

    assert _row_points(result.get_dq_metrics(), "check", check_name="values")["FAILED"] == 2


def test_row_points_flag_the_checks_not_attributed(rq):
    result = rq._not_attributed_pair()

    document = result.get_dq_metrics()
    key = "vowl.row_quality.checks_not_attributed"
    for level, expected in (("run", {2}), ("schema", {0, 2}), ("dimension", {0, 1})):
        points = _points(document, f"vowl.{level}.row.count") + _points(document, f"vowl.{level}.row.pass_rate")
        assert {p["attributes"][key] for p in points} == expected
    (schema,) = {
        p["attributes"][key]
        for p in _points(document, "vowl.schema.row.count")
        if p["attributes"]["schema_name"] == "t"
    }
    assert schema == 2
    # The check level always reports the scalar count and carries no flag.
    assert all(key not in p["attributes"] for p in _points(document, "vowl.check.row.count"))
    assert _row_points(document, "check", check_name="id_in_u")["FAILED"] == 2

    pytest.importorskip("opentelemetry")
    from test_otel_export import _tracer_provider

    from vowl.otel._traces import TraceEmitter

    provider, exporter = _tracer_provider()
    TraceEmitter(provider, namespace="vowl", sample_rows_by_check={}).emit(result)
    (root,) = [s for s in exporter.get_finished_spans() if s.name == "vowl.validate"]
    assert root.attributes[key] == 2
    assert root.attributes["vowl.row_quality.exact"] is False


def test_the_summary_names_the_checks_not_attributed(rq, capsys):
    rq._not_attributed_pair().print_summary()

    assert "(approx., 2 checks not attributed)" in capsys.readouterr().out
