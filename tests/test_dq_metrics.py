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


def test_every_level_has_check_and_row_numbers(result):
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
