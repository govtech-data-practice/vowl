"""Per-signal provider resolution for the OTEL exporter.

Three modes, in precedence order (see the design doc's Export path):

1. An explicitly passed provider is used as-is; vowl owns no lifecycle.
2. ``use_global_providers=True`` records into the process globals; vowl neither
   configures nor shuts down anything.
3. Otherwise vowl builds its own provider with an OTLP exporter, and the caller
   is expected to ``force_flush``/``shutdown`` it (the facade does this) so a
   short-lived batch job actually delivers before the process exits.

Additive metric instruments use **delta** temporality so each run contributes
its own counts and a warehouse rollup is a plain ``SUM``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

_GRPC = "grpc"
_HTTP = "http/protobuf"


@dataclass
class ResolvedProvider:
    """A provider plus whether vowl built (and therefore must tear down) it."""

    provider: Any
    owned: bool


@dataclass
class Providers:
    """Resolved providers for the enabled signals, and vowl-owned lifecycle."""

    meter: Any | None = None
    tracer: Any | None = None
    logger: Any | None = None
    _owned: list[Any] = field(default_factory=list)

    def flush_and_shutdown(self) -> None:
        """Force-flush then shut down only the providers vowl built itself."""
        for provider in self._owned:
            try:
                provider.force_flush()
            finally:
                provider.shutdown()


def _delta_temporality() -> dict[Any, Any]:
    from opentelemetry.sdk.metrics import Counter, Histogram, ObservableCounter, UpDownCounter
    from opentelemetry.sdk.metrics.export import AggregationTemporality

    delta = AggregationTemporality.DELTA
    return {
        Counter: delta,
        Histogram: delta,
        ObservableCounter: delta,
        UpDownCounter: delta,
    }


def _otlp_kwargs(endpoint: str | None, headers: dict[str, str] | None) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if endpoint:
        kwargs["endpoint"] = endpoint
    if headers:
        kwargs["headers"] = headers
    return kwargs


def _build_meter_provider(resource: Any, protocol: str, endpoint: str | None, headers: dict[str, str] | None) -> Any:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

    if protocol == _HTTP:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    else:
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

    exporter = OTLPMetricExporter(preferred_temporality=_delta_temporality(), **_otlp_kwargs(endpoint, headers))
    reader = PeriodicExportingMetricReader(exporter)
    return MeterProvider(resource=resource, metric_readers=[reader])


def _build_tracer_provider(resource: Any, protocol: str, endpoint: str | None, headers: dict[str, str] | None) -> Any:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    if protocol == _HTTP:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    else:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(**_otlp_kwargs(endpoint, headers))))
    return provider


def _build_logger_provider(resource: Any, protocol: str, endpoint: str | None, headers: dict[str, str] | None) -> Any:
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

    if protocol == _HTTP:
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    else:
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

    provider = LoggerProvider(resource=resource)
    provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter(**_otlp_kwargs(endpoint, headers))))
    return provider


def resolve_providers(
    signals: tuple[str, ...],
    *,
    resource: Any,
    protocol: str,
    endpoint: str | None,
    headers: dict[str, str] | None,
    use_global_providers: bool,
    metric_provider: Any | None,
    tracer_provider: Any | None,
    logger_provider: Any | None,
) -> Providers:
    """Resolve one provider per enabled signal following the precedence above."""
    from opentelemetry import metrics as _metrics
    from opentelemetry import trace as _trace
    from opentelemetry._logs import get_logger_provider

    resolved = Providers()

    def _resolve(explicit: Any | None, get_global: Any, build: Any) -> ResolvedProvider:
        if explicit is not None:
            return ResolvedProvider(explicit, owned=False)
        if use_global_providers:
            return ResolvedProvider(get_global(), owned=False)
        return ResolvedProvider(build(), owned=True)

    if "metrics" in signals:
        rp = _resolve(
            metric_provider,
            _metrics.get_meter_provider,
            lambda: _build_meter_provider(resource, protocol, endpoint, headers),
        )
        resolved.meter = rp.provider
        if rp.owned:
            resolved._owned.append(rp.provider)

    if "traces" in signals:
        rp = _resolve(
            tracer_provider,
            _trace.get_tracer_provider,
            lambda: _build_tracer_provider(resource, protocol, endpoint, headers),
        )
        resolved.tracer = rp.provider
        if rp.owned:
            resolved._owned.append(rp.provider)

    if "logs" in signals:
        rp = _resolve(
            logger_provider,
            get_logger_provider,
            lambda: _build_logger_provider(resource, protocol, endpoint, headers),
        )
        resolved.logger = rp.provider
        if rp.owned:
            resolved._owned.append(rp.provider)

    return resolved
