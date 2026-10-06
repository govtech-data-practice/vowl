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
