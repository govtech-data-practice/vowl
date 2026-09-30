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


def _row_counts(points, key):
    """Group ``*.row.count`` points into ``{attributes[key]: {status: value}}``."""
    grouped: dict[str, dict[str, int]] = {}
    for point in points:
        grouped.setdefault(point.attributes[key], {})[point.attributes["status"]] = point.value
    return grouped


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
    assert "vowl.check.row.count" in points

    # SQL text is high-cardinality: it must never ride on a metric label.
    for point in points["vowl.check.count"]:
        assert "query" not in point.attributes

    # Per-check row counts: a PASSED and a FAILED point for every row-level
    # check, zeros included.
    check_rows = _row_counts(points["vowl.check.row.count"], "check_name")
    assert check_rows["amount_non_negative"] == {"PASSED": 2, "FAILED": 2}
    assert all(
        counts == {"PASSED": 4, "FAILED": 0} for name, counts in check_rows.items() if name != "amount_non_negative"
    )
    (failing,) = [
        p
        for p in points["vowl.check.row.count"]
        if p.attributes["check_name"] == "amount_non_negative" and p.attributes["status"] == "FAILED"
    ]
    assert failing.attributes["dimension"] == "consistency"
    assert failing.attributes["schema_name"] == "orders"

    # Per-check row pass rate: every row-level check with a schema gets one.
    check_row_rates = {p.attributes["check_name"]: p.value for p in points["vowl.check.row_pass_rate"]}
    assert check_row_rates["amount_non_negative"] == 0.5  # 2 of 4 rows failed
    assert all(v == 1.0 for k, v in check_row_rates.items() if k != "amount_non_negative")


def test_metric_emitter_dimension_level_metrics(result):
    from vowl.otel._metrics import MetricEmitter

    provider, reader = _meter_provider()
    MetricEmitter(provider, namespace="vowl").emit(result)
    points = _metric_points(reader)

    # Dimension-level row counts (deduplicated within each dimension).
    dim_rows = _row_counts(points["vowl.dimension.row.count"], "dimension")
    assert dim_rows["consistency"] == {"PASSED": 2, "FAILED": 2}
    assert dim_rows["completeness"] == {"PASSED": 4, "FAILED": 0}
    assert dim_rows["conformity"] == {"PASSED": 4, "FAILED": 0}
    assert all(p.attributes["schema_name"] == "orders" for p in points["vowl.dimension.row.count"])

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

    # Schema-level row counts (each row counted once). PASSED + FAILED is the
    # table's row count.
    assert _row_counts(points["vowl.schema.row.count"], "schema_name") == {"orders": {"PASSED": 2, "FAILED": 2}}
    assert all("dimension" not in p.attributes for p in points["vowl.schema.row.count"])

    # Schema-level check pass rate.
    (schema_check_rate,) = points["vowl.schema.check_pass_rate"]
    assert schema_check_rate.attributes["schema_name"] == "orders"
    assert 0.0 < schema_check_rate.value < 1.0  # some pass, some fail

    # Schema-level row pass rate.
    (schema_row_rate,) = points["vowl.schema.row_pass_rate"]
    assert schema_row_rate.attributes["schema_name"] == "orders"
    assert schema_row_rate.value == 0.5  # 2 of 4 rows affected


def test_row_counts_are_gauges_and_the_check_count_is_a_counter(result):
    """Row counts are not additive across checks or runs, so they must be gauges."""
    from opentelemetry.sdk.metrics.export import Gauge, Sum

    from vowl.otel._metrics import MetricEmitter

    provider, reader = _meter_provider()
    MetricEmitter(provider, namespace="vowl").emit(result)
    types = {
        metric.name: type(metric.data)
        for resource_metrics in reader.get_metrics_data().resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
    }
    for name in ("vowl.check.row.count", "vowl.dimension.row.count", "vowl.schema.row.count"):
        assert types[name] is Gauge, name
    assert types["vowl.check.count"] is Sum


def test_clean_run_sends_zero_failed_rows():
    """A run with no failures still sends ``FAILED = 0``, replacing an earlier count."""
    from vowl.otel._metrics import MetricEmitter

    df = pd.DataFrame({"order_id": [1, 2], "amount": [10.0, 20.0]})
    clean = _run_validation(contract=Contract(_contract_data()), df=df)

    provider, reader = _meter_provider()
    MetricEmitter(provider, namespace="vowl").emit(clean)
    points = _metric_points(reader)

    assert _row_counts(points["vowl.schema.row.count"], "schema_name") == {"orders": {"PASSED": 2, "FAILED": 0}}
    for counts in _row_counts(points["vowl.check.row.count"], "check_name").values():
        assert counts == {"PASSED": 2, "FAILED": 0}


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

    # The one FAILED check is found through its ``status`` attribute. The check
    # itself ran fine, so its span and the root span stay OK.
    failed_spans = [s for s in children if s.attributes["status"] == "FAILED"]
    assert len(failed_spans) == 1
    assert failed_spans[0].status.status_code == StatusCode.OK
    assert failed_spans[0].attributes["dimension"] == "consistency"
    assert failed_spans[0].attributes["severity"] == "error"
    assert all(s.status.status_code == StatusCode.OK for s in spans)

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
    (failed_span,) = [
        s for s in span_exporter.get_finished_spans() if s.attributes.get("check_name") == failed.check_name
    ]

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
    (failed_span,) = [
        s for s in span_exporter.get_finished_spans() if s.attributes.get("check_name") == failed.check_name
    ]
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
    LogEmitter(
        provider, namespace="vowl", span_contexts=contexts, sample_rows_by_check={}, context_attributes=ctx
    ).emit(result)
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
    (failed,) = [
        p
        for p in _metric_points(reader)["vowl.dimension.row.count"]
        if p.attributes["dimension"] == "consistency" and p.attributes["status"] == "FAILED"
    ]
    assert failed.value == 2
    assert failed.attributes["vowl.contract.id"] == "orders-contract"
    assert "deployment.environment" in failed.attributes
    # Traces.
    spans = span_exporter.get_finished_spans()
    assert any(s.name == "vowl.validate" for s in spans)
    assert any(s.attributes["status"] == "FAILED" for s in spans if s.name == "vowl.check")
    assert all(s.status.status_code == StatusCode.OK for s in spans)
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


def test_providers_shut_down_every_owned_provider_even_if_one_raises():
    from vowl.otel._providers import Providers

    class Recorder:
        def __init__(self, fail: bool = False):
            self.fail = fail
            self.calls: list[str] = []

        def force_flush(self):
            self.calls.append("flush")

        def shutdown(self):
            self.calls.append("shutdown")
            if self.fail:
                raise RuntimeError("boom")

    first = Recorder(fail=True)
    second = Recorder()
    providers = Providers(_owned=[first, second])
    with pytest.raises(RuntimeError, match="boom"):
        providers.flush_and_shutdown()

    assert second.calls == ["flush", "shutdown"]


# --------------------------------------------------------------------------- #
# Error checks, run status and timing
# --------------------------------------------------------------------------- #


@pytest.fixture
def errored_result():
    """A finished result with one ERROR check and no FAILED checks."""
    data = _contract_data()
    quality = data["schema"][0]["properties"][1]["quality"][0]
    quality["name"] = "broken_query"
    quality["query"] = 'SELECT COUNT(*) FROM "orders" WHERE no_such_column < 0'
    df = pd.DataFrame({"order_id": [1, 2], "amount": [10.0, 20.0]})
    res = _run_validation(contract=Contract(data), df=df)
    statuses = [cr.status for cr in res.check_results]
    assert "ERROR" in statuses and "FAILED" not in statuses
    return res


def test_error_check_logs_at_error_severity(errored_result):
    from opentelemetry._logs import SeverityNumber

    from vowl.otel._logs import LogEmitter

    provider, exporter = _logger_provider()
    LogEmitter(provider, namespace="vowl", span_contexts={}, sample_rows_by_check={}).emit(errored_result)
    (record,) = [entry.log_record for entry in exporter.get_finished_logs()]
    assert record.severity_number == SeverityNumber.ERROR
    assert record.severity_text == "ERROR"
    assert record.attributes["check_name"] == "broken_query"


def test_root_span_is_error_when_a_check_errors(errored_result):
    """``result.passed`` ignores ERROR checks, but the root span must not."""
    from opentelemetry.trace import StatusCode

    from vowl.otel._traces import TraceEmitter

    assert errored_result.passed  # no FAILED checks, so the public flag stays True
    provider, exporter = _tracer_provider()
    TraceEmitter(provider, namespace="vowl", sample_rows_by_check={}).emit(errored_result)
    spans = exporter.get_finished_spans()
    (root,) = [s for s in spans if s.name == "vowl.validate"]
    assert root.status.status_code == StatusCode.ERROR
    (broken,) = [s for s in spans if s.name == "vowl.check" and s.attributes["status"] == "ERROR"]
    assert broken.status.status_code == StatusCode.ERROR


def test_root_span_is_ok_when_every_check_passes():
    from opentelemetry.trace import StatusCode

    from vowl.otel._traces import TraceEmitter

    df = pd.DataFrame({"order_id": [1, 2], "amount": [10.0, 20.0]})
    clean = _run_validation(contract=Contract(_contract_data()), df=df)
    provider, exporter = _tracer_provider()
    TraceEmitter(provider, namespace="vowl", sample_rows_by_check={}).emit(clean)
    (root,) = [s for s in exporter.get_finished_spans() if s.name == "vowl.validate"]
    assert root.status.status_code == StatusCode.OK


def test_runner_records_the_run_window(result):
    assert result._run_started_ns is not None
    assert result._run_finished_ns is not None
    assert result._run_finished_ns >= result._run_started_ns


def test_root_span_uses_the_recorded_run_window(result):
    from vowl.otel._traces import TraceEmitter

    result._run_started_ns = 1_700_000_000_000_000_000
    result._run_finished_ns = result._run_started_ns + 5_000_000_000  # 5 seconds

    provider, exporter = _tracer_provider()
    TraceEmitter(provider, namespace="vowl", sample_rows_by_check={}).emit(result)
    spans = exporter.get_finished_spans()
    (root,) = [s for s in spans if s.name == "vowl.validate"]
    assert root.start_time == result._run_started_ns
    assert root.end_time == result._run_finished_ns

    # Child spans stay inside the run window.
    for child in (s for s in spans if s.name == "vowl.check"):
        assert root.start_time <= child.start_time <= child.end_time <= root.end_time


# --------------------------------------------------------------------------- #
# Facade: endpoints, protocol and environment variables
# --------------------------------------------------------------------------- #

_OTEL_ENV_VARS = (
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
)


@pytest.fixture
def clean_otel_env(monkeypatch):
    """Clear the OTLP env vars so the developer's shell cannot leak in."""
    for name in _OTEL_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


@pytest.mark.parametrize(
    ("endpoint", "protocol", "signal", "expected"),
    [
        ("http://collector:4318", "http/protobuf", "traces", "http://collector:4318/v1/traces"),
        ("http://collector:4318/", "http/protobuf", "metrics", "http://collector:4318/v1/metrics"),
        ("http://collector:4318", "http/protobuf", "logs", "http://collector:4318/v1/logs"),
        # Already there: not doubled.
        ("http://collector:4318/v1/traces", "http/protobuf", "traces", "http://collector:4318/v1/traces"),
        # gRPC takes the address as-is.
        ("http://collector:4317", "grpc", "traces", "http://collector:4317"),
        (None, "http/protobuf", "traces", None),
    ],
)
def test_signal_endpoint_adds_the_http_path(endpoint, protocol, signal, expected):
    from vowl.otel._providers import _signal_endpoint

    assert _signal_endpoint(endpoint, protocol, signal) == expected


def _patch_http_exporters(monkeypatch, outcome: str = "SUCCESS"):
    """Swap the OTLP/HTTP exporters for ones that record their endpoint and
    return *outcome* without touching the network."""
    import opentelemetry.exporter.otlp.proto.http._log_exporter as log_mod
    import opentelemetry.exporter.otlp.proto.http.metric_exporter as metric_mod
    import opentelemetry.exporter.otlp.proto.http.trace_exporter as trace_mod
    from opentelemetry.sdk.metrics.export import MetricExportResult
    from opentelemetry.sdk.trace.export import SpanExportResult

    try:
        from opentelemetry.sdk._logs.export import LogRecordExportResult as LogResult
    except ImportError:
        from opentelemetry.sdk._logs.export import LogExportResult as LogResult

    endpoints: dict[str, str] = {}

    def fake(module, class_name, signal, result_enum):
        base = getattr(module, class_name)

        class Fake(base):
            def export(self, *args, **kwargs):
                endpoints[signal] = self._endpoint
                return getattr(result_enum, outcome)

        monkeypatch.setattr(module, class_name, Fake)

    fake(trace_mod, "OTLPSpanExporter", "traces", SpanExportResult)
    fake(metric_mod, "OTLPMetricExporter", "metrics", MetricExportResult)
    fake(log_mod, "OTLPLogExporter", "logs", LogResult)
    return endpoints


def test_http_export_sends_each_signal_to_its_own_path(clean_otel_env, result):
    import warnings

    endpoints = _patch_http_exporters(clean_otel_env)
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        result.export_otel(endpoint="http://collector:4318", protocol="http/protobuf")

    assert endpoints == {
        "metrics": "http://collector:4318/v1/metrics",
        "traces": "http://collector:4318/v1/traces",
        "logs": "http://collector:4318/v1/logs",
    }


def test_failed_delivery_raises_a_runtime_warning(clean_otel_env, result):
    _patch_http_exporters(clean_otel_env, outcome="FAILURE")
    with pytest.warns(RuntimeWarning, match=r"could not deliver logs, metrics, traces to http://collector:4318"):
        result.export_otel(endpoint="http://collector:4318", protocol="http/protobuf")


def test_protocol_falls_back_to_env_var_then_grpc(clean_otel_env):
    from vowl.otel import OtelExporter

    assert OtelExporter(endpoint="http://localhost:4317")._protocol == "grpc"
    clean_otel_env.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
    assert OtelExporter(endpoint="http://localhost:4318")._protocol == "http/protobuf"
    # An explicit argument still wins over the env var.
    assert OtelExporter(endpoint="http://localhost:4317", protocol="grpc")._protocol == "grpc"


def test_per_signal_endpoint_env_var_is_enough(clean_otel_env):
    from vowl.otel import OtelExporter

    clean_otel_env.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://collector:4317")
    OtelExporter(signals=("traces",))
    with pytest.raises(ValueError, match="No endpoint configured for metrics"):
        OtelExporter(signals=("traces", "metrics"))


def test_endpoint_check_skips_signals_with_an_explicit_provider(clean_otel_env):
    from vowl.otel import OtelExporter

    tracer, _exporter = _tracer_provider()
    OtelExporter(signals=("traces",), tracer_provider=tracer)
    with pytest.raises(ValueError, match="No endpoint configured for metrics"):
        OtelExporter(signals=("traces", "metrics"), tracer_provider=tracer)


# --------------------------------------------------------------------------- #
# Facade: run id and run duration
# --------------------------------------------------------------------------- #


def _explicit_export(result, **kwargs):
    meter, reader = _meter_provider()
    tracer, span_exporter = _tracer_provider()
    logger, log_exporter = _logger_provider()
    run_id = result.export_otel(
        metric_provider=meter,
        tracer_provider=tracer,
        logger_provider=logger,
        **kwargs,
    )
    return run_id, reader, span_exporter, log_exporter


def test_run_id_is_on_spans_and_logs_but_not_metrics(result):
    run_id, reader, span_exporter, log_exporter = _explicit_export(result)

    for name, data_points in _metric_points(reader).items():
        for point in data_points:
            assert "vowl.run.id" not in point.attributes, name
            assert point.attributes["vowl.contract.id"] == "orders-contract", name
    for span in span_exporter.get_finished_spans():
        assert span.attributes["vowl.run.id"] == run_id
    for entry in log_exporter.get_finished_logs():
        assert entry.log_record.attributes["vowl.run.id"] == run_id


def test_caller_supplied_run_id_is_used_and_returned(result):
    run_id, _reader, span_exporter, _logs = _explicit_export(result, run_id="nightly-2026-09-29")

    assert run_id == "nightly-2026-09-29"
    assert all(s.attributes["vowl.run.id"] == run_id for s in span_exporter.get_finished_spans())


def test_run_duration_is_a_histogram_of_the_run_window(result):
    from opentelemetry.sdk.metrics.export import Histogram

    from vowl.otel._metrics import MetricEmitter

    result._run_started_ns = 1_700_000_000_000_000_000
    result._run_finished_ns = result._run_started_ns + 250_000_000  # 250 ms

    provider, reader = _meter_provider()
    MetricEmitter(provider, namespace="vowl").emit(result)
    metrics = {
        metric.name: metric
        for rm in reader.get_metrics_data().resource_metrics
        for sm in rm.scope_metrics
        for metric in sm.metrics
    }
    duration = metrics["vowl.run.duration"]
    assert isinstance(duration.data, Histogram)
    assert duration.unit == "ms"
    (point,) = duration.data.data_points
    assert point.count == 1
    assert point.sum == pytest.approx(250.0)


# --------------------------------------------------------------------------- #
# Facade: row samples
# --------------------------------------------------------------------------- #


def test_row_sample_is_also_capped_by_max_failed_rows():
    """The sample never exceeds the run's own ``max_failed_rows`` fetch cap."""
    from vowl.config import ValidationConfig
    from vowl.otel import OtelExporter

    df = pd.DataFrame({"order_id": [1, 2, 3, 4], "amount": [10.0, -5.0, 20.0, -1.0]})
    capped = _run_validation(
        contract=Contract(_contract_data()),
        df=df,
        config=ValidationConfig(max_failed_rows=1),
    )
    exporter = OtelExporter(endpoint="http://localhost:4317", max_failed_rows_sample=10)
    samples = exporter._collect_row_samples(capped)
    ((_check_id, rows),) = samples.items()
    assert len(rows) == 1


def test_row_sample_off_by_default_exports_no_row_contents(result):
    _run_id, _reader, span_exporter, log_exporter = _explicit_export(result)

    for span in span_exporter.get_finished_spans():
        assert not [e for e in span.events if e.name == "vowl.failed_row"]
    for entry in log_exporter.get_finished_logs():
        assert "vowl.failed_rows_sample" not in entry.log_record.attributes
