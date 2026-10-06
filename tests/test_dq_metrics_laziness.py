"""Guard test for the two tiers of failed-row counts.

The basic tier (``print_summary``, ``get_check_results_df``, ``summary``,
``save``) runs only the check SQL and never attributes rows. The DQ-metrics
tier (``get_dq_metrics``, ``get_row_quality_df``, ``save_dq_metrics``,
``export_otel``) attributes rows once and reuses the cached report.

Attribution always goes through ``RowQuality._compute``, so counting its calls
is enough. Table totals are counted separately through
``IbisAdapter.get_total_rows``. Both counters are installed after
``validate_data`` returns, so only the method under test is measured.
"""

from __future__ import annotations

import pandas as pd
import pytest

from vowl import validate_data as _validate_data
from vowl.adapters.ibis_adapter import IbisAdapter
from vowl.contracts.contract import Contract
from vowl.validation.row_quality import RowQuality

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


class _Calls:
    def __init__(self) -> None:
        self.compute = 0
        self.total_rows = 0


@pytest.fixture
def result():
    df = pd.DataFrame({"order_id": [1, 2, None, 4], "amount": [10.0, -5.0, 20.0, -1.0]})
    return _run_validation(contract=Contract(_contract_data()), df=df)


@pytest.fixture
def calls(result, monkeypatch) -> _Calls:
    """Count attribution and table-total calls made after validation."""
    counter = _Calls()
    compute = RowQuality._compute
    get_total_rows = IbisAdapter.get_total_rows

    def counted_compute(self):
        counter.compute += 1
        return compute(self)

    def counted_total_rows(self, *args, **kwargs):
        counter.total_rows += 1
        return get_total_rows(self, *args, **kwargs)

    monkeypatch.setattr(RowQuality, "_compute", counted_compute)
    monkeypatch.setattr(IbisAdapter, "get_total_rows", counted_total_rows)
    return counter


# --------------------------------------------------------------------------- #
# Basic tier: never attributes, never counts table rows
# --------------------------------------------------------------------------- #


def _assert_basic(calls: _Calls) -> None:
    assert (calls.compute, calls.total_rows) == (0, 0)


def test_print_summary_does_not_attribute(result, calls, capsys):
    result.print_summary()
    _assert_basic(calls)


def test_check_results_df_does_not_attribute(result, calls):
    result.get_check_results_df()
    _assert_basic(calls)


def test_summary_does_not_attribute(result, calls):
    assert result.summary
    _assert_basic(calls)


def test_save_failed_rows_does_not_attribute(result, calls, tmp_path):
    with pytest.warns(DeprecationWarning):
        result.save(str(tmp_path), output_mode="failed_rows")
    _assert_basic(calls)


@pytest.mark.xfail(strict=True, reason="annotated output still attributes through merge_key() (decision 11)")
def test_save_annotated_does_not_attribute(result, calls, tmp_path):
    result.save(str(tmp_path), output_mode="annotated")
    _assert_basic(calls)


# --------------------------------------------------------------------------- #
# DQ-metrics tier: attributes exactly once across every method
# --------------------------------------------------------------------------- #


def _meter_provider():
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    return MeterProvider(metric_readers=[InMemoryMetricReader()])


def test_dq_metrics_methods_share_one_attribution(result, calls):
    result.get_dq_metrics()
    result.get_row_quality_df(by="schema")
    result.get_row_quality_df(by="check")
    result.get_dq_metrics()
    assert calls.compute == 1


def test_save_dq_metrics_reuses_the_attribution(result, calls, tmp_path):
    result.get_dq_metrics()
    result.save_dq_metrics(str(tmp_path))
    assert (tmp_path / "vowl_results_dq_metrics.json").exists()
    assert calls.compute == 1


def test_export_otel_reuses_the_attribution(result, calls):
    pytest.importorskip("opentelemetry.sdk", reason="requires the [otel] extra")
    result.get_dq_metrics()
    result.export_otel(signals=("metrics",), metric_provider=_meter_provider())
    assert calls.compute == 1
