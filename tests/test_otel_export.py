"""Tests for the optional OpenTelemetry exporter (:mod:`vowl.otel`).

These exercise each single-signal emitter against in-memory OTEL SDK readers or
exporters, the shared context/attribute builders as pure functions, and the
facade's lifecycle and row-sampling behaviour.  They also guard the two
packaging invariants: ``import vowl`` must not import ``opentelemetry``, and
:meth:`ValidationResult.export_otel` must raise a friendly ImportError when the
extra is absent.
"""

from __future__ import annotations

import subprocess
import sys

import pandas as pd
import pytest

from vowl import validate_data as _validate_data
from vowl.contracts.contract import Contract

pytest.importorskip("opentelemetry", reason="requires the [otel] extra")

# Capture the real function object at import time. conftest's autouse golden
# fixture swaps the ``validate_data`` attribute on the vowl modules, but this
# bound reference still points at the original, so these OTEL tests build a
# ValidationResult without triggering golden-file comparison (validation output
# correctness is already covered by the golden suites).  The reference is kept
# under a private name so the fixture does not rebind it either.
_run_validation = _validate_data
del _validate_data


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def _contract_data() -> dict:
    """A v3 contract with an authored, dimensioned, failing quality rule."""
    return {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "version": "2.0.0",
        "id": "orders-contract",
        "name": "Orders Data Contract",
        "status": "active",
        "domain": "sales",
        "dataProduct": "orders_dp",
        "tenant": "acme",
        "contractCreatedTs": "2026-01-15T10:30:00Z",
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
                                "severity": "error",
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
    """A finished ``ValidationResult`` with one FAILED consistency check."""
    df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "amount": [10.0, -5.0, 20.0, -1.0],
        }
    )
    return _run_validation(contract=Contract(_contract_data()), df=df)


def _failed_check(result):
    """The single FAILED check (``amount_non_negative``)."""
    (check,) = [cr for cr in result.check_results if cr.status == "FAILED"]
    return check


# --------------------------------------------------------------------------- #
# In-memory provider helpers
# --------------------------------------------------------------------------- #


def _meter_provider():
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    reader = InMemoryMetricReader()
    return MeterProvider(metric_readers=[reader]), reader


def _metric_points(reader):
    """Flatten collected metrics into ``{name: [data_points]}``."""
    data = reader.get_metrics_data()
    points: dict[str, list] = {}
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                points[metric.name] = list(metric.data.data_points)
    return points


def _tracer_provider():
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def _logger_provider():
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter, SimpleLogRecordProcessor

    exporter = InMemoryLogRecordExporter()
    provider = LoggerProvider()
    provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    return provider, exporter


# --------------------------------------------------------------------------- #
# Packaging invariants
# --------------------------------------------------------------------------- #


def test_import_vowl_does_not_import_opentelemetry():
    """``import vowl`` must stay free of the optional opentelemetry dependency."""
    code = "import vowl, sys; print('opentelemetry' in sys.modules)"
    completed = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    assert completed.stdout.strip() == "False"


def test_export_otel_without_extra_raises_friendly_error(monkeypatch, result):
    """A missing ``[otel]`` extra surfaces an actionable ImportError."""
    # Poisoning the module entry makes ``from ..otel import ...`` raise ImportError,
    # standing in for the extra never having been installed.
    monkeypatch.setitem(sys.modules, "vowl.otel", None)
    with pytest.raises(ImportError, match=r"pip install vowl\[otel\]"):
        result.export_otel()


# --------------------------------------------------------------------------- #
# Shared resource / attribute builders
# --------------------------------------------------------------------------- #


def test_build_resource_carries_contract_and_custom_attributes(result):
    from vowl.otel._common import build_resource

    resource = build_resource(
        result,
        service_name="dq-service",
        run_id="run-123",
        version="9.9.9",
        custom_attributes={"deployment.environment": "staging", "team": "data"},
    )
    attrs = resource.attributes
    assert attrs["service.name"] == "dq-service"
    assert attrs["vowl.version"] == "9.9.9"
    assert attrs["vowl.run.id"] == "run-123"
    assert attrs["vowl.contract.id"] == "orders-contract"
    assert attrs["vowl.contract.name"] == "Orders Data Contract"
    assert attrs["vowl.contract.version"] == "2.0.0"
    assert attrs["vowl.contract.api_version"] == "v3.1.0"
    assert attrs["vowl.contract.status"] == "active"
    assert attrs["vowl.contract.created_ts"] == "2026-01-15T10:30:00Z"
    assert attrs["vowl.domain"] == "sales"
    assert attrs["vowl.data_product"] == "orders_dp"
    assert attrs["vowl.tenant"] == "acme"
    # Pure pass-through: keys attached verbatim, never rerouted.
    assert attrs["deployment.environment"] == "staging"
    assert attrs["team"] == "data"


def test_contract_attributes_never_backfill_version_from_api_version():
    """A ``None`` author version is omitted, never replaced by the ODCS apiVersion.

    Every ODCS schema requires ``version``, so a validated contract always has
    one and ``get_version()`` never returns ``None`` in practice.  This guards
    the intent directly with a stub: were the old ``or result.api_version``
    fallback reintroduced, ``vowl.contract.version`` would wrongly carry the spec
    version (a different field with a different meaning) instead of being absent.
    """
    from types import SimpleNamespace

    from vowl.otel._common import contract_attributes

    contract = SimpleNamespace(
        contract_data={},
        get_metadata=lambda: {"id": "orders-contract", "status": "active"},
        get_version=lambda: None,
    )
    result = SimpleNamespace(contract=contract, api_version="v3.1.0")

    attrs = contract_attributes(result)  # type: ignore[arg-type]
    assert "vowl.contract.version" not in attrs
    assert attrs["vowl.contract.id"] == "orders-contract"


def test_check_attributes_resolve_dimension_and_severity_from_definition(result):
    from vowl.otel._common import check_attributes

    attrs = check_attributes(_failed_check(result))
    assert attrs["dimension"] == "consistency"
    assert attrs["severity"] == "error"
    assert attrs["schema_name"] == "orders"
    assert attrs["status"] == "FAILED"


# --------------------------------------------------------------------------- #
# Metric emitter
# --------------------------------------------------------------------------- #


def test_metric_emitter_check_level_metrics(result):
    from vowl.otel._metrics import MetricEmitter

    provider, reader = _meter_provider()
    MetricEmitter(provider, namespace="vowl").emit(result)
    points = _metric_points(reader)

    assert "vowl.check.count" in points
    assert "vowl.check.duration" in points
    assert "vowl.check.failed_rows" in points

    # SQL text is high-cardinality: it must never ride on a metric label.
    for point in points["vowl.check.count"]:
        assert "query" not in point.attributes

    # Per-check failed rows: only the failing check emits a data point.
    (check_failed,) = points["vowl.check.failed_rows"]
    assert check_failed.value == 2
    assert check_failed.attributes["check_name"] == "amount_non_negative"
    assert check_failed.attributes["dimension"] == "consistency"

    # Per-check row pass rate: every check with a schema gets one.
    check_row_rates = {
        p.attributes["check_name"]: p.value for p in points["vowl.check.row_pass_rate"]
    }
    assert check_row_rates["amount_non_negative"] == 0.5  # 2 of 4 rows failed
    assert all(v == 1.0 for k, v in check_row_rates.items() if k != "amount_non_negative")


def test_metric_emitter_dimension_level_metrics(result):
    from vowl.otel._metrics import MetricEmitter

    provider, reader = _meter_provider()
    MetricEmitter(provider, namespace="vowl").emit(result)
    points = _metric_points(reader)

    # Dimension-level failed rows (deduplicated within each dimension).
    (dim_failed,) = points["vowl.dimension.failed_rows"]
    assert dim_failed.value == 2
    assert dim_failed.attributes["schema_name"] == "orders"
    assert dim_failed.attributes["dimension"] == "consistency"

    # Check pass rate per dimension.
    check_rates = {p.attributes["dimension"]: p.value for p in points["vowl.dimension.check_pass_rate"]}
    assert check_rates["consistency"] == 0.0
    assert check_rates["completeness"] == 1.0
    assert check_rates["conformity"] == 1.0
    assert "unknown" not in check_rates

    # Row pass rate per dimension.
    row_rates = {p.attributes["dimension"]: p.value for p in points["vowl.dimension.row_pass_rate"]}
    assert row_rates["consistency"] == 0.5  # 2 of 4 rows failed
    assert row_rates["completeness"] == 1.0
    assert row_rates["conformity"] == 1.0


def test_metric_emitter_schema_level_metrics(result):
    from vowl.otel._metrics import MetricEmitter

    provider, reader = _meter_provider()
    MetricEmitter(provider, namespace="vowl").emit(result)
    points = _metric_points(reader)

    # Schema-level failed rows (deduplicated across all dimensions).
    (schema_failed,) = points["vowl.schema.failed_rows"]
    assert schema_failed.value == 2
    assert schema_failed.attributes["schema_name"] == "orders"
    assert "dimension" not in schema_failed.attributes

    # Total rows.
    (total,) = points["vowl.schema.rows_total"]
    assert total.value == 4
    assert total.attributes["schema_name"] == "orders"

    # Schema-level check pass rate.
    (schema_check_rate,) = points["vowl.schema.check_pass_rate"]
    assert schema_check_rate.attributes["schema_name"] == "orders"
    assert 0.0 < schema_check_rate.value < 1.0  # some pass, some fail

    # Schema-level row pass rate.
    (schema_row_rate,) = points["vowl.schema.row_pass_rate"]
    assert schema_row_rate.attributes["schema_name"] == "orders"
    assert schema_row_rate.value == 0.5  # 2 of 4 rows affected


def test_metric_names_use_vowl_prefix(result):
    from vowl.otel._metrics import MetricEmitter

    provider, reader = _meter_provider()
    MetricEmitter(provider, namespace="vowl").emit(result)
    names = set(_metric_points(reader))

    assert all(name.startswith("vowl.") for name in names)


def test_context_attributes_appear_on_every_metric_point(result):
    from vowl.otel._metrics import MetricEmitter

    ctx = {"vowl.contract.id": "orders-contract", "vowl.run.id": "run-42"}
    provider, reader = _meter_provider()
    MetricEmitter(provider, namespace="vowl", context_attributes=ctx).emit(result)
    for name, data_points in _metric_points(reader).items():
        for point in data_points:
            assert point.attributes["vowl.contract.id"] == "orders-contract", f"{name} missing contract id"
            assert point.attributes["vowl.run.id"] == "run-42", f"{name} missing run id"


def test_signal_specific_attrs_override_context_attrs(result):
    """Signal-level keys like ``status`` must not be overwritten by context."""
    from vowl.otel._metrics import MetricEmitter

    ctx = {"status": "SHOULD_BE_OVERRIDDEN", "vowl.run.id": "run-42"}
    provider, reader = _meter_provider()
    MetricEmitter(provider, namespace="vowl", context_attributes=ctx).emit(result)
    for point in _metric_points(reader)["vowl.check.count"]:
        assert point.attributes["status"] in ("PASSED", "FAILED", "ERROR")
        assert point.attributes["vowl.run.id"] == "run-42"


# --------------------------------------------------------------------------- #
# Trace emitter
# --------------------------------------------------------------------------- #


def test_trace_emitter_lays_out_root_and_child_spans(result):
    from opentelemetry.trace import StatusCode

    from vowl.otel._traces import TraceEmitter

    provider, exporter = _tracer_provider()
    contexts = TraceEmitter(provider, namespace="vowl", sample_rows_by_check={}).emit(result)
    spans = exporter.get_finished_spans()

    roots = [s for s in spans if s.name == "vowl.validate"]
    children = [s for s in spans if s.name == "vowl.check"]
    assert len(roots) == 1
    assert len(children) == len(result.check_results)

    # The one FAILED check maps to an ERROR span carrying the resolved dimension.
    failed_spans = [s for s in children if s.status.status_code == StatusCode.ERROR]
    assert len(failed_spans) == 1
    assert failed_spans[0].attributes["dimension"] == "consistency"
    assert failed_spans[0].attributes["severity"] == "error"

    # Returned contexts backlink each check by identity for the log emitter.
    assert set(contexts) == {id(cr) for cr in result.check_results}


def _contract_data_with_custom_properties() -> dict:
    """A contract whose failing rule carries scalar and nested custom properties."""
    data = _contract_data()
    quality = data["schema"][0]["properties"][1]["quality"][0]
    quality["customProperties"] = [
        {"property": "owner", "value": "risk-team"},
        {"property": "sla", "value": {"minutes": 30, "tier": "gold"}},
        {"property": "regions", "value": ["sg", "us", "eu"]},
    ]
    return data


@pytest.fixture
def result_with_custom_properties():
    """A finished result whose FAILED check declares custom properties."""
    df = pd.DataFrame({"order_id": [1, 2], "amount": [10.0, -5.0]})
    return _run_validation(contract=Contract(_contract_data_with_custom_properties()), df=df)


def test_custom_properties_flatten_onto_span_and_log_not_metrics(result_with_custom_properties):
    """customProperties surface as queryable check.definition.custom.* keys on
    spans and logs, nested values recurse into dotted subkeys, and none of it
    reaches a metric label."""
    from vowl.otel._logs import LogEmitter
    from vowl.otel._metrics import MetricEmitter
    from vowl.otel._traces import TraceEmitter

    res = result_with_custom_properties
    failed = _failed_check(res)

    tracer, span_exporter = _tracer_provider()
    contexts = TraceEmitter(tracer, namespace="vowl", sample_rows_by_check={}).emit(res)
    (failed_span,) = [s for s in span_exporter.get_finished_spans() if s.attributes.get("check_name") == failed.check_name]

    # Scalar property keyed by author name, verbatim; nested object recursed into
    # dotted subkeys; scalar list preserved as a native homogeneous array.
    assert failed_span.attributes["check.definition.custom.owner"] == "risk-team"
    assert failed_span.attributes["check.definition.custom.sla.minutes"] == 30
    assert failed_span.attributes["check.definition.custom.sla.tier"] == "gold"
    assert tuple(failed_span.attributes["check.definition.custom.regions"]) == ("sg", "us", "eu")

    # Same keys ride the WARN record.
    logger, log_exporter = _logger_provider()
    LogEmitter(logger, namespace="vowl", span_contexts=contexts, sample_rows_by_check={}).emit(res)
    (record,) = [entry.log_record for entry in log_exporter.get_finished_logs()]
    assert record.attributes["check.definition.custom.owner"] == "risk-team"
    assert record.attributes["check.definition.custom.sla.tier"] == "gold"

    # No check.definition.* key ever lands on a metric point.
    provider, reader = _meter_provider()
    MetricEmitter(provider, namespace="vowl").emit(res)
    for name, data_points in _metric_points(reader).items():
        for point in data_points:
            assert not any(key.startswith("check.definition.") for key in point.attributes), name


def test_trace_and_log_carry_executed_sql_query(result):
    """The check's executed SQL rides on its span and its failure log record."""
    from vowl.otel._logs import LogEmitter
    from vowl.otel._traces import TraceEmitter

    tracer, span_exporter = _tracer_provider()
    contexts = TraceEmitter(tracer, namespace="vowl", sample_rows_by_check={}).emit(result)

    logger, log_exporter = _logger_provider()
    LogEmitter(logger, namespace="vowl", span_contexts=contexts, sample_rows_by_check={}).emit(result)

    # The failed check's span carries the engine-rendered statement that ran.
    failed = _failed_check(result)
    (failed_span,) = [s for s in span_exporter.get_finished_spans() if s.attributes.get("check_name") == failed.check_name]
    query = failed_span.attributes["query"]
    assert "amount" in query and "<" in query

    # The single WARN record carries the same query for alert triage.
    (record,) = [entry.log_record for entry in log_exporter.get_finished_logs()]
    assert record.attributes["query"] == query


def test_context_attributes_appear_on_every_span(result):
    from vowl.otel._traces import TraceEmitter

    ctx = {"vowl.contract.id": "orders-contract", "vowl.run.id": "run-42"}
    provider, exporter = _tracer_provider()
    TraceEmitter(provider, namespace="vowl", sample_rows_by_check={}, context_attributes=ctx).emit(result)
    for span in exporter.get_finished_spans():
        assert span.attributes["vowl.contract.id"] == "orders-contract", f"{span.name} missing contract id"
        assert span.attributes["vowl.run.id"] == "run-42", f"{span.name} missing run id"


def test_trace_emitter_attaches_sample_row_events(result):
    from vowl.otel._traces import TraceEmitter

    failed = _failed_check(result)
    samples = {id(failed): [{"order_id": 2, "amount": -5.0}]}

    provider, exporter = _tracer_provider()
    TraceEmitter(provider, namespace="vowl", sample_rows_by_check=samples).emit(result)
    spans = exporter.get_finished_spans()

    events = [e for s in spans for e in s.events if e.name == "vowl.failed_row"]
    assert len(events) == 1
    assert events[0].attributes["order_id"] == 2


# --------------------------------------------------------------------------- #
# Log emitter
# --------------------------------------------------------------------------- #


def test_log_emitter_records_one_warn_per_failed_check_with_backlink(result):
    from opentelemetry._logs import SeverityNumber

    from vowl.otel._logs import LogEmitter
    from vowl.otel._traces import TraceEmitter

    tracer, _span_exporter = _tracer_provider()
    contexts = TraceEmitter(tracer, namespace="vowl", sample_rows_by_check={}).emit(result)

    provider, exporter = _logger_provider()
    LogEmitter(
        provider,
        namespace="vowl",
        span_contexts=contexts,
        sample_rows_by_check={},
    ).emit(result)
    records = [entry.log_record for entry in exporter.get_finished_logs()]

    # Passing checks are silent, so only the single FAILED check is logged.
    assert len(records) == 1
    record = records[0]
    assert record.severity_number == SeverityNumber.WARN
    assert record.severity_text == "WARN"
    assert record.attributes["dimension"] == "consistency"

    failed = _failed_check(result)
    assert record.trace_id == contexts[id(failed)].trace_id
    assert record.span_id == contexts[id(failed)].span_id


def test_context_attributes_appear_on_log_records(result):
    from vowl.otel._logs import LogEmitter
    from vowl.otel._traces import TraceEmitter

    ctx = {"vowl.contract.id": "orders-contract", "vowl.run.id": "run-42"}
    tracer, _span_exporter = _tracer_provider()
    contexts = TraceEmitter(tracer, namespace="vowl", sample_rows_by_check={}).emit(result)

    provider, exporter = _logger_provider()
    LogEmitter(provider, namespace="vowl", span_contexts=contexts, sample_rows_by_check={}, context_attributes=ctx).emit(result)
    records = [entry.log_record for entry in exporter.get_finished_logs()]
    assert len(records) >= 1
    for record in records:
        assert record.attributes["vowl.contract.id"] == "orders-contract"
        assert record.attributes["vowl.run.id"] == "run-42"


def test_log_emitter_is_silent_when_all_checks_pass():
    from vowl.otel._logs import LogEmitter

    df = pd.DataFrame({"order_id": [1, 2], "amount": [10.0, 20.0]})
    clean = _run_validation(contract=Contract(_contract_data()), df=df)
    assert clean.passed

    provider, exporter = _logger_provider()
    LogEmitter(provider, namespace="vowl", span_contexts={}, sample_rows_by_check={}).emit(clean)
    assert len(exporter.get_finished_logs()) == 0


# --------------------------------------------------------------------------- #
# Facade: validation, row sampling, lifecycle
# --------------------------------------------------------------------------- #


def test_exporter_rejects_invalid_arguments():
    from vowl.otel import OtelExporter

    with pytest.raises(ValueError, match="Unknown signal"):
        OtelExporter(endpoint="http://localhost:4317", signals=("metrics", "bogus"))
    with pytest.raises(ValueError, match="Unknown protocol"):
        OtelExporter(endpoint="http://localhost:4317", protocol="carrier-pigeon")
    with pytest.raises(ValueError, match="max_failed_rows_sample"):
        OtelExporter(endpoint="http://localhost:4317", max_failed_rows_sample=-1)
    with pytest.raises(ValueError, match="No endpoint configured"):
        OtelExporter()


def test_collect_row_samples_off_by_default(result):
    from vowl.otel import OtelExporter

    exporter = OtelExporter(endpoint="http://localhost:4317", max_failed_rows_sample=0)
    assert exporter._collect_row_samples(result) == {}


def test_collect_row_samples_is_capped_by_head(result):
    from vowl.otel import OtelExporter

    # Two rows fail, but the head cap keeps only one, the second of the two caps.
    exporter = OtelExporter(endpoint="http://localhost:4317", max_failed_rows_sample=1)
    samples = exporter._collect_row_samples(result)
    assert len(samples) == 1
    ((_check_id, rows),) = samples.items()
    assert len(rows) == 1


def test_export_end_to_end_with_explicit_providers(result):
    """The facade fans out to every enabled signal against caller providers."""
    from opentelemetry.trace import StatusCode

    meter, reader = _meter_provider()
    tracer, span_exporter = _tracer_provider()
    logger, log_exporter = _logger_provider()

    run_id = result.export_otel(
        signals=("metrics", "traces", "logs"),
        metric_provider=meter,
        tracer_provider=tracer,
        logger_provider=logger,
        custom_attributes={"deployment.environment": "test"},
    )
    assert isinstance(run_id, str) and run_id

    # Metrics: context attributes survive onto data points with explicit providers.
    (failed,) = _metric_points(reader)["vowl.dimension.failed_rows"]
    assert failed.attributes["dimension"] == "consistency"
    assert failed.attributes["vowl.contract.id"] == "orders-contract"
    assert "deployment.environment" in failed.attributes
    # Traces.
    spans = span_exporter.get_finished_spans()
    assert any(s.name == "vowl.validate" for s in spans)
    assert any(s.status.status_code == StatusCode.ERROR for s in spans if s.name == "vowl.check")
    for span in spans:
        assert "vowl.contract.id" in span.attributes
    # Logs.
    assert len(log_exporter.get_finished_logs()) == 1
    record = log_exporter.get_finished_logs()[0].log_record
    assert record.attributes["vowl.contract.id"] == "orders-contract"

    # Explicit providers are not owned, so the facade must not have shut them down.
    tracer.get_tracer("probe").start_span("still-alive").end()


def test_providers_flush_and_shutdown_only_owned():
    from vowl.otel._providers import Providers

    class Recorder:
        def __init__(self):
            self.calls: list[str] = []

        def force_flush(self):
            self.calls.append("flush")

        def shutdown(self):
            self.calls.append("shutdown")

    owned = Recorder()
    borrowed = Recorder()
    providers = Providers(meter=owned, tracer=borrowed, _owned=[owned])
    providers.flush_and_shutdown()

    assert owned.calls == ["flush", "shutdown"]
    assert borrowed.calls == []
