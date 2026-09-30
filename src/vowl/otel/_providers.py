"""Per-signal provider resolution for the OTEL exporter.

Three modes, in precedence order (see the design doc's Provider resolution):

1. An explicitly passed provider is used as-is. vowl owns no lifecycle.
2. ``use_global_providers=True`` records into the process globals. vowl neither
   configures nor shuts down anything.
3. Otherwise vowl builds its own provider with an OTLP exporter, and the caller
   is expected to ``force_flush``/``shutdown`` it (the facade does this) so a
   short-lived batch job actually delivers before the process exits.

Additive metric instruments use **delta** temporality so each run reports only
its own counts, not a running total since the process started.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Any

_GRPC = "grpc"
_HTTP = "http/protobuf"

#: OTLP/HTTP path per signal. The HTTP exporters only add it for the generic
#: ``OTEL_EXPORTER_OTLP_ENDPOINT`` env var, never for an explicit ``endpoint=``.
_HTTP_SIGNAL_PATHS = {
    "metrics": "/v1/metrics",
    "traces": "/v1/traces",
    "logs": "/v1/logs",
}


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
    endpoint: str | None = None
    _owned: list[Any] = field(default_factory=list)
    _failed_signals: set[str] = field(default_factory=set)

    def flush_and_shutdown(self) -> None:
        """Force-flush then shut down only the providers vowl built itself.

        Every owned provider is shut down even if an earlier one raises. The
        first exception is re-raised once they are all done. If any exporter
        vowl built failed to deliver, a ``RuntimeWarning`` names the signals.
        """
        first_error: BaseException | None = None
        for provider in self._owned:
            for step in (provider.force_flush, provider.shutdown):
                try:
                    step()
                except Exception as exc:
                    if first_error is None:
                        first_error = exc

        if self._failed_signals:
            target = self.endpoint or "the OTEL_EXPORTER_OTLP_* endpoint"
            warnings.warn(
                f"vowl OTel export: could not deliver {', '.join(sorted(self._failed_signals))} to {target}. "
                "Check that the endpoint is right and the collector is running.",
                RuntimeWarning,
                stacklevel=3,
            )

        if first_error is not None:
            raise first_error


def _signal_endpoint(endpoint: str | None, protocol: str, signal: str) -> str | None:
    """Return the endpoint to hand the exporter for *signal*.

    gRPC takes the collector address as-is. OTLP/HTTP needs the per-signal path
    (``/v1/traces`` and friends), which is added unless it is already there.
    """
    if not endpoint or protocol != _HTTP:
        return endpoint
    path = _HTTP_SIGNAL_PATHS[signal]
    base = endpoint.rstrip("/")
    return base if base.endswith(path) else base + path


def _watch_delivery(exporter: Any, signal: str, failed_signals: set[str]) -> Any:
    """Record *signal* in *failed_signals* when an export does not succeed.

    ``force_flush`` only says the flush finished, not that the backend accepted
    the data, and the SDK only logs export failures. Wrapping ``export`` on the
    exporters vowl builds lets the facade warn the caller instead.
    """
    original = exporter.export

    def export(*args: Any, **kwargs: Any) -> Any:
        try:
            outcome = original(*args, **kwargs)
        except Exception:
            failed_signals.add(signal)
            raise
        if getattr(outcome, "name", None) != "SUCCESS":
            failed_signals.add(signal)
        return outcome

    exporter.export = export
    return exporter


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


def _build_meter_provider(
    resource: Any,
    protocol: str,
    endpoint: str | None,
    headers: dict[str, str] | None,
    failed_signals: set[str],
) -> Any:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

    if protocol == _HTTP:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    else:
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

    exporter = OTLPMetricExporter(
        preferred_temporality=_delta_temporality(),
        **_otlp_kwargs(_signal_endpoint(endpoint, protocol, "metrics"), headers),
    )
    reader = PeriodicExportingMetricReader(_watch_delivery(exporter, "metrics", failed_signals))
    return MeterProvider(resource=resource, metric_readers=[reader])


def _build_tracer_provider(
    resource: Any,
    protocol: str,
    endpoint: str | None,
    headers: dict[str, str] | None,
    failed_signals: set[str],
) -> Any:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    if protocol == _HTTP:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    else:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    exporter = OTLPSpanExporter(**_otlp_kwargs(_signal_endpoint(endpoint, protocol, "traces"), headers))
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(_watch_delivery(exporter, "traces", failed_signals)))
    return provider


def _build_logger_provider(
    resource: Any,
    protocol: str,
    endpoint: str | None,
    headers: dict[str, str] | None,
    failed_signals: set[str],
) -> Any:
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

    if protocol == _HTTP:
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    else:
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

    exporter = OTLPLogExporter(**_otlp_kwargs(_signal_endpoint(endpoint, protocol, "logs"), headers))
    provider = LoggerProvider(resource=resource)
    provider.add_log_record_processor(BatchLogRecordProcessor(_watch_delivery(exporter, "logs", failed_signals)))
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

    resolved = Providers(endpoint=endpoint)
    failed = resolved._failed_signals

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
            lambda: _build_meter_provider(resource, protocol, endpoint, headers, failed),
        )
        resolved.meter = rp.provider
        if rp.owned:
            resolved._owned.append(rp.provider)

    if "traces" in signals:
        rp = _resolve(
            tracer_provider,
            _trace.get_tracer_provider,
            lambda: _build_tracer_provider(resource, protocol, endpoint, headers, failed),
        )
        resolved.tracer = rp.provider
        if rp.owned:
            resolved._owned.append(rp.provider)

    if "logs" in signals:
        rp = _resolve(
            logger_provider,
            get_logger_provider,
            lambda: _build_logger_provider(resource, protocol, endpoint, headers, failed),
        )
        resolved.logger = rp.provider
        if rp.owned:
            resolved._owned.append(rp.provider)

    return resolved
