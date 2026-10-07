"""The grouped failed-rows view (``output_mode="failed_rows"``).

A FAILED check whose operator sets no upper limit (``mustBeGreaterThan``,
``mustBe: 5``, a table-level ``rowCount``) matched the good rows. Those rows
must not be listed as failures, and leaving them out must not attribute rows.
"""

from __future__ import annotations

import pandas as pd
import pytest

from vowl import validate_data as _validate_data
from vowl.contracts.contract import Contract
from vowl.validation.row_quality import RowQuality

# See tests/test_otel_export.py: bypass the golden-file wrapper from conftest.
_run_validation = _validate_data
del _validate_data


def _contract_data() -> dict:
    return {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "version": "1.0.0",
        "id": "orders-contract",
        "status": "active",
        "schema": [
            {
                "name": "orders",
                "properties": [
                    {"name": "order_id"},
                    {
                        "name": "amount",
                        "quality": [
                            {
                                "name": "negative_amount",
                                "type": "sql",
                                "query": 'SELECT COUNT(*) FROM "orders" WHERE amount < 0',
                                "mustBe": 0,
                            },
                            {
                                "name": "enough_positive",
                                "type": "sql",
                                "query": 'SELECT COUNT(*) FROM "orders" WHERE amount > 0',
                                "mustBeGreaterThan": 10,
                            },
                            {
                                "name": "exact_positive",
                                "type": "sql",
                                "query": 'SELECT COUNT(*) FROM "orders" WHERE amount > 0',
                                "mustBe": 5,
                            },
                        ],
                    },
                ],
                "quality": [
                    {"name": "enough_rows", "type": "library", "metric": "rowCount", "mustBeGreaterThan": 100},
                ],
            }
        ],
    }


@pytest.fixture
def result():
    df = pd.DataFrame({"order_id": [1, 2, 3, 4], "amount": [10.0, -5.0, 20.0, 30.0]})
    return _run_validation(contract=Contract(_contract_data()), df=df)


@pytest.fixture
def compute_calls(result, monkeypatch) -> list[int]:
    calls: list[int] = []
    compute = RowQuality._compute

    def counted(self):
        calls.append(1)
        return compute(self)

    monkeypatch.setattr(RowQuality, "_compute", counted)
    return calls


def _statuses(result) -> dict[str, str]:
    return {cr.check_name: cr.status for cr in result.check_results}


def test_checks_without_an_upper_limit_failed(result):
    statuses = _statuses(result)
    for name in ("negative_amount", "enough_positive", "exact_positive", "enough_rows"):
        assert statuses[name] == "FAILED"


def test_consolidated_output_lists_only_bad_rows(result, compute_calls):
    out = result.get_consolidated_output_dfs()
    assert list(out) == ["orders"]
    orders = out["orders"]
    assert orders["order_id"].to_list() == [2]
    assert orders["check_ids"].to_list() == ["negative_amount"]
    assert compute_calls == []


def test_failed_rows_save_lists_only_bad_rows(result, compute_calls, tmp_path):
    result.save(str(tmp_path), prefix="fr", output_mode="failed_rows")
    orders = pd.read_csv(tmp_path / "fr_orders.csv")
    assert orders["order_id"].tolist() == [2]
    assert orders["check_ids"].tolist() == ["negative_amount"]
    assert compute_calls == []


def test_get_output_dfs_still_returns_every_row_query(result):
    # The raw per-check view is unchanged: it returns what each row query matched.
    out = result.get_output_dfs()
    assert len(out["orders::enough_positive"]) == 3


# ---------------------------------------------------------------------------
# Tolerated rows under attribute_tolerated_rows
# ---------------------------------------------------------------------------


def _tolerated_contract_data() -> dict:
    """``fails`` FAILED, ``tolerates`` PASSED within its threshold, ``clean`` matched nothing."""
    return {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "version": "1.0.0",
        "id": "orders-contract",
        "status": "active",
        "schema": [
            {
                "name": "orders",
                "properties": [
                    {"name": "order_id"},
                    {
                        "name": "amount",
                        "quality": [
                            {
                                "name": "fails",
                                "type": "sql",
                                "query": 'SELECT COUNT(*) FROM "orders" WHERE amount < 0',
                                "mustBe": 0,
                            },
                            {
                                "name": "tolerates",
                                "type": "sql",
                                "query": 'SELECT COUNT(*) FROM "orders" WHERE amount < 15',
                                "mustBeLessThan": 5,
                            },
                            {
                                "name": "clean",
                                "type": "sql",
                                "query": 'SELECT COUNT(*) FROM "orders" WHERE amount > 1000',
                                "mustBe": 0,
                            },
                        ],
                    },
                ],
            }
        ],
    }


def _tolerated_result(attribute_tolerated_rows: bool):
    from vowl.config import ValidationConfig

    df = pd.DataFrame({"order_id": [1, 2, 3, 4], "amount": [10.0, -5.0, 20.0, 30.0]})
    return _run_validation(
        contract=Contract(_tolerated_contract_data()),
        df=df,
        config=ValidationConfig(attribute_tolerated_rows=attribute_tolerated_rows),
    )


def _count_passed_fetches(result) -> list[str]:
    """Wrap each PASSED check's row fetcher so a call is recorded."""
    calls: list[str] = []
    for cr in result.check_results:
        fetcher = cr._failed_rows_fetcher
        if cr.status == "PASSED" and fetcher is not None:

            def counted(fetcher=fetcher, name=cr.check_name):
                calls.append(name)
                return fetcher()

            cr._failed_rows_fetcher = counted
    return calls


def test_tolerated_checks_are_set_up():
    statuses = _statuses(_tolerated_result(False))
    assert {name: statuses[name] for name in ("fails", "tolerates", "clean")} == {
        "fails": "FAILED",
        "tolerates": "PASSED",
        "clean": "PASSED",
    }


def test_flag_off_reports_failed_checks_only_without_fetching_passed_rows(tmp_path, capsys):
    result = _tolerated_result(False)
    calls = _count_passed_fetches(result)

    out = result.get_output_dfs()
    assert list(out) == ["orders::fails"]
    assert "tolerated" not in out["orders::fails"].columns

    consolidated = result.get_consolidated_output_dfs()["orders"]
    assert "tolerated_check_ids" not in consolidated.columns
    assert consolidated["order_id"].to_list() == [2]

    result.save(str(tmp_path), prefix="fr", output_mode="failed_rows")
    assert "tolerated_check_ids" not in pd.read_csv(tmp_path / "fr_orders.csv").columns

    result.show_failed_rows()
    assert "(tolerated)" not in capsys.readouterr().out
    assert calls == []


def test_get_output_dfs_reports_tolerated_rows_under_the_flag():
    result = _tolerated_result(True)
    out = result.get_output_dfs()
    # clean matched nothing, so it is not tolerated and is left out.
    assert list(out) == ["orders::fails", "orders::tolerates"]
    assert out["orders::fails"]["tolerated"].to_list() == [False]
    tolerated = out["orders::tolerates"]
    assert sorted(tolerated["order_id"].to_list()) == [1, 2]
    assert tolerated["tolerated"].to_list() == [True, True]


def test_consolidated_output_marks_tolerated_check_ids():
    # Order 2 is picked out by the failed check and the tolerated one.
    orders = _tolerated_result(True).get_consolidated_output_dfs()["orders"].to_pandas()
    rows = orders.set_index("order_id")[["check_ids", "tolerated_check_ids"]].to_dict("index")
    assert rows == {
        1: {"check_ids": "tolerates", "tolerated_check_ids": "tolerates"},
        2: {"check_ids": "fails, tolerates", "tolerated_check_ids": "tolerates"},
    }


def test_failed_rows_save_holds_tolerated_rows_under_the_flag(tmp_path):
    _tolerated_result(True).save(str(tmp_path), prefix="fr", output_mode="failed_rows")
    orders = pd.read_csv(tmp_path / "fr_orders.csv").fillna("")
    assert sorted(orders["order_id"].tolist()) == [1, 2]
    assert set(orders["tolerated_check_ids"]) == {"tolerates"}


def test_show_failed_rows_labels_tolerated_checks(capsys):
    _tolerated_result(True).show_failed_rows()
    out = capsys.readouterr().out
    assert "[fails]\n" in out
    assert "[tolerates] (tolerated)" in out
    assert "[clean]" not in out


def test_every_output_reads_one_fetch_of_a_tolerated_check(tmp_path):
    result = _tolerated_result(True)
    calls = _count_passed_fetches(result)
    result.get_output_dfs()
    result.save(str(tmp_path), prefix="b", output_mode="both")
    result.get_dq_metrics()
    assert calls.count("tolerates") == 1
