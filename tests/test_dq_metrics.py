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
        elif name.endswith((".row.count", ".row.scalar_count", ".pass_rate", ".scalar_pass_rate")):
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
    # Whether a row number is approximate is on the spans, not the metrics.
    assert row_rate["attributes"] == {}


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


def test_save_dq_metrics_writes_the_document(result, tmp_path):
    result.save_dq_metrics(str(tmp_path), prefix="dq")
    document = json.loads((tmp_path / "dq_dq_metrics.json").read_text())

    assert document == json.loads(json.dumps(result.get_dq_metrics(), default=str))
    assert document["schema_version"] == 1
    assert document["run"]["vowl.contract.id"] == "orders-contract"
    assert document["run_started_at"] and document["run_finished_at"]


def test_save_writes_no_dq_metrics(result, tmp_path):
    result.save(str(tmp_path), prefix="dq", output_mode="annotated")
    assert not (tmp_path / "dq_dq_metrics.json").exists()


def test_dq_metrics_needs_no_opentelemetry(monkeypatch, result):
    monkeypatch.setitem(sys.modules, "opentelemetry", None)
    assert result.get_dq_metrics()["points"]


# ---------------------------------------------------------------------------
# Every level's row.count holds the attributed rows. The check level also
# reports the check's scalar count as row.scalar_count. See "How failed rows
# are counted" in the docs.


def _row_points(document: dict, level: str, measure: str = "count", **attributes) -> dict:
    """``{status: value, "pass_rate": rate}`` of ``vowl.<level>.row.<measure>``, ``{}`` when absent."""
    rate_name = "pass_rate" if measure == "count" else measure.replace("count", "pass_rate")

    def matches(p):
        return all(p["attributes"].get(key) == value for key, value in attributes.items())

    values = {
        p["attributes"]["status"]: p["value"] for p in _points(document, f"vowl.{level}.row.{measure}") if matches(p)
    }
    rates = [p["value"] for p in _points(document, f"vowl.{level}.row.{rate_name}") if matches(p)]
    if not values and not rates:
        return {}
    (rate,) = rates
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


def test_a_tolerated_check_follows_attribute_tolerated_rows(rq):
    con = _duckdb_table(rq, "(1, -1), (2, -2), (3, 3), (4, 4)")
    checks = [rq._schema("t", [rq._check("tolerated", "c < 0", mustBeLessThan=10)])]
    result = rq._validate(con, checks)

    document = result.get_dq_metrics()
    assert result.check_results[0].status == "PASSED"
    # Under the default scope a check that passed adds no failed rows, at the
    # check level as at the schema level. Its scalar count still shows them.
    assert _row_points(document, "check", check_name="tolerated") == {"PASSED": 4, "FAILED": 0, "pass_rate": 1.0}
    assert _row_points(document, "check", "scalar_count", check_name="tolerated") == {
        "PASSED": 2,
        "FAILED": 2,
        "pass_rate": 0.5,
    }
    assert _row_points(document, "schema", schema_name="t") == {"PASSED": 4, "FAILED": 0, "pass_rate": 1.0}

    tolerated = rq._validate(con, checks, config=rq.ValidationConfig(attribute_tolerated_rows=True))
    document = tolerated.get_dq_metrics()
    assert _row_points(document, "check", check_name="tolerated") == {"PASSED": 2, "FAILED": 2, "pass_rate": 0.5}
    assert _row_points(document, "schema", schema_name="t") == {"PASSED": 2, "FAILED": 2, "pass_rate": 0.5}


def test_a_join_that_fans_out_goes_negative_at_check_level_only(rq):
    con = _duckdb_table(rq, "(1, -1), (1, -1), (2, -2), (3, 3)")
    con.raw_sql("CREATE TABLE l (id INTEGER)")
    con.raw_sql("INSERT INTO l VALUES (1), (1), (1), (2)")
    fan = {"name": "fan", "query": "SELECT t.* FROM t JOIN l ON t.id = l.id WHERE t.c < 0", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [fan])])

    document = result.get_dq_metrics()
    # The join returns 7 rows from a table of 4. The scalar count is not clamped.
    assert _row_points(document, "check", "scalar_count", check_name="fan") == {
        "PASSED": -3,
        "FAILED": 7,
        "pass_rate": -0.75,
    }
    assert rq._check_rows(result)["fan"]["attributed_rows"] == 3
    assert _row_points(document, "check", check_name="fan") == {"PASSED": 1, "FAILED": 3, "pass_rate": 0.25}
    assert _row_points(document, "schema", schema_name="t") == {"PASSED": 1, "FAILED": 3, "pass_rate": 0.25}

    pytest.importorskip("opentelemetry")
    from vowl.otel._common import check_row_attributes

    (check,) = [cr for cr in result.check_results if cr.check_name == "fan"]
    attrs = check_row_attributes(result)[id(check)]
    assert {key: attrs[key] for key in ("row.count.passed", "row.count.failed", "row.pass_rate")} == {
        "row.count.passed": 1,
        "row.count.failed": 3,
        "row.pass_rate": 0.25,
    }
    scalar = ("row.scalar_count.passed", "row.scalar_count.failed", "row.scalar_pass_rate")
    assert {key: attrs[key] for key in scalar} == {
        "row.scalar_count.passed": -3,
        "row.scalar_count.failed": 7,
        "row.scalar_pass_rate": -0.75,
    }
    assert attrs["vowl.row_quality.attributed_rows"] == 3


def test_distinct_lowers_only_the_scalar_count(rq):
    con = _duckdb_table(rq, "(1, 2), (1, 2), (1, 2), (2, 3)")
    check = {"name": "d", "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c = 2) AS s", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check])])

    document = result.get_dq_metrics()
    assert _row_points(document, "check", "scalar_count", check_name="d")["FAILED"] == 1
    assert rq._check_rows(result)["d"]["attributed_rows"] == 3
    assert _row_points(document, "check", check_name="d")["FAILED"] == 3
    assert _row_points(document, "schema", schema_name="t")["FAILED"] == 3


def test_count_distinct_reports_the_values_it_counted(rq):
    con = _duckdb_table(rq, "(1, -1), (2, -1), (3, -2), (4, 3)")
    check = {"name": "values", "query": "SELECT COUNT(DISTINCT c) FROM t WHERE c < 0", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check])])

    document = result.get_dq_metrics()
    assert _row_points(document, "check", "scalar_count", check_name="values")["FAILED"] == 2
    # The attributed rows are the rows that hold those values.
    assert _row_points(document, "check", check_name="values")["FAILED"] == 3


def test_spans_name_the_checks_not_attributable(rq):
    result = rq._not_attributable_pair()

    document = result.get_dq_metrics()
    # The metrics carry no trust attributes at any level.
    for point in document["points"]:
        assert not any(key.startswith("vowl.row_quality.") for key in point["attributes"])
    assert _row_points(document, "check", "scalar_count", check_name="id_in_u")["FAILED"] == 2
    # A check that is not attributable gets no row.count, not a 0.
    assert _row_points(document, "check", check_name="id_in_u") == {}

    pytest.importorskip("opentelemetry")
    from test_otel_export import _tracer_provider

    from vowl.otel._traces import TraceEmitter

    provider, exporter = _tracer_provider()
    TraceEmitter(provider, namespace="vowl", sample_rows_by_check={}).emit(result)
    spans = exporter.get_finished_spans()
    (root,) = [s for s in spans if s.name == "vowl.validate"]
    assert root.attributes["vowl.row_quality.checks_not_attributable"] == 2
    assert root.attributes["vowl.row_quality.approximate"] is True

    # The check spans say which checks made it approximate, and why.
    checks = {s.attributes["check_name"]: s.attributes for s in spans if s.name == "vowl.check"}
    flagged = {name for name, attrs in checks.items() if attrs.get("vowl.row_quality.approximate")}
    rows = rq._check_rows(result)
    assert flagged == {name for name, row in rows.items() if row["approximate"] and name in checks}
    assert flagged
    for name in flagged:
        assert checks[name]["vowl.row_quality.reason"] == rows[name]["reason"]
        assert "vowl.row_quality.attributed_rows" not in checks[name]


def test_the_schema_rows_name_the_checks_not_attributable(rq):
    result = rq._not_attributable_pair()

    schemas = result.get_dq_metrics_df(by="schema").to_pandas()
    assert schemas["checks_not_attributable"].max() == 2
    assert schemas["approximate"].any()
