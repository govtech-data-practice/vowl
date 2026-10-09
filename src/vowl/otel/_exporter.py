"""The OTEL export facade: builds the shared resource, resolves providers, and
fans out to the enabled single-signal emitters.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from .. import __version__
from ._common import build_context_attributes, build_resource, new_run_id
from ._logs import LogEmitter
from ._metrics import MetricEmitter
from ._providers import resolve_providers
from ._traces import TraceEmitter

if TYPE_CHECKING:
    from ..validation.result import ValidationResult

DEFAULT_SIGNALS: tuple[str, ...] = ("metrics", "traces", "logs")

_VALID_SIGNALS = ("metrics", "traces", "logs")
_VALID_PROTOCOLS = ("grpc", "http/protobuf")
_DEFAULT_PROTOCOL = "grpc"
_DEFAULT_PREFIX = "vowl"


def _without_attribute(resource: Any, key: str) -> Any:
    """A copy of *resource* without the attribute *key*."""
    from opentelemetry.sdk.resources import Resource

    attrs = {k: v for k, v in resource.attributes.items() if k != key}
    return Resource(attrs, resource.schema_url)


def _resolve_protocol(protocol: str | None) -> str:
    """The argument if given, else ``OTEL_EXPORTER_OTLP_PROTOCOL``, else gRPC."""
    resolved = protocol or os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL") or _DEFAULT_PROTOCOL
    if resolved not in _VALID_PROTOCOLS:
        raise ValueError(f"Unknown protocol {resolved!r}. Expected one of {_VALID_PROTOCOLS}.")
    return resolved


def _has_env_endpoint(signal: str) -> bool:
    """Whether the standard OTLP env vars give *signal* somewhere to send to."""
    return bool(
        os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or os.environ.get(f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT")
    )


class OtelExporter:
    """Orchestrates a single read-only export of a ``ValidationResult``."""

    def __init__(
        self,
        *,
        signals: tuple[str, ...] = DEFAULT_SIGNALS,
        endpoint: str | None = None,
        protocol: str | None = None,
        service_name: str = _DEFAULT_PREFIX,
        prefix: str = _DEFAULT_PREFIX,
        headers: dict[str, str] | None = None,
        custom_attributes: dict[str, Any] | None = None,
        run_id: str | None = None,
        max_failed_rows_sample: int = 0,
        use_global_providers: bool = False,
        metric_provider: Any | None = None,
        tracer_provider: Any | None = None,
        logger_provider: Any | None = None,
    ) -> None:
        signals = tuple(signals)
        unknown = [s for s in signals if s not in _VALID_SIGNALS]
        if unknown:
            raise ValueError(f"Unknown signal(s) {unknown}. Expected any of {_VALID_SIGNALS}.")
        protocol = _resolve_protocol(protocol)
        if max_failed_rows_sample < 0:
            raise ValueError("max_failed_rows_sample must be >= 0.")

        # Only signals vowl builds a provider for need an endpoint. An explicit
        # provider (or the globals) already knows where its data goes.
        if not use_global_providers and endpoint is None:
            explicit = {"metrics": metric_provider, "traces": tracer_provider, "logs": logger_provider}
            missing = [s for s in signals if explicit[s] is None and not _has_env_endpoint(s)]
            if missing:
                raise ValueError(
                    f"No endpoint configured for {', '.join(missing)}. Pass endpoint=, set the "
                    "OTEL_EXPORTER_OTLP_ENDPOINT environment variable, or pass your own provider."
                )

        self._signals = signals
        self._endpoint = endpoint
        self._protocol = protocol
        self._run_id = run_id
        self._service_name = service_name
        self._prefix = prefix
        self._headers = headers
        self._custom_attributes = custom_attributes
        self._max_failed_rows_sample = max_failed_rows_sample
        self._use_global_providers = use_global_providers
        self._metric_provider = metric_provider
        self._tracer_provider = tracer_provider
        self._logger_provider = logger_provider

    def export(self, result: ValidationResult) -> str:
        """Emit the enabled signals for *result*. Returns the run id."""
        # The result's own id by default, so dq_metrics.json and the telemetry
        # of one run share it. A result built without the runner may lack one.
        run_id = self._run_id or getattr(result, "run_id", None) or new_run_id()
        identity = {
            "service_name": self._service_name,
            "run_id": run_id,
            "version": __version__,
            "prefix": self._prefix,
            "custom_attributes": self._custom_attributes,
        }
        # Everything that can raise runs before any provider exists, so a
        # provider vowl builds is always reached by the shutdown below.
        sample_rows = self._collect_row_samples(result)
        context_attrs = build_context_attributes(result, **identity)
        # The run id stays off metric points and off the Resource of the meter
        # provider vowl builds: a fresh value per run would start a new metric
        # series every run. Spans and logs keep it, Resource included.
        run_id_key = f"{self._prefix}.run.id"
        metric_attrs = {k: v for k, v in context_attrs.items() if k != run_id_key}
        resource = build_resource(result, **identity)
        metrics_resource = _without_attribute(resource, run_id_key)

        providers = resolve_providers(
            self._signals,
            resource=resource,
            metrics_resource=metrics_resource,
            protocol=self._protocol,
            endpoint=self._endpoint,
            headers=self._headers,
            use_global_providers=self._use_global_providers,
            metric_provider=self._metric_provider,
            tracer_provider=self._tracer_provider,
            logger_provider=self._logger_provider,
        )

        try:
            if providers.meter is not None:
                MetricEmitter(
                    providers.meter,
                    namespace=self._prefix,
                    context_attributes=metric_attrs,
                ).emit(result)

            span_contexts: dict[int, Any] = {}
            if providers.tracer is not None:
                span_contexts = TraceEmitter(
                    providers.tracer,
                    namespace=self._prefix,
                    sample_rows_by_check=sample_rows,
                    context_attributes=context_attrs,
                ).emit(result)

            if providers.logger is not None:
                LogEmitter(
                    providers.logger,
                    namespace=self._prefix,
                    span_contexts=span_contexts,
                    sample_rows_by_check=sample_rows,
                    context_attributes=context_attrs,
                ).emit(result)
        finally:
            providers.flush_and_shutdown()

        return run_id

    def _collect_row_samples(self, result: ValidationResult) -> dict[int, list[dict]]:
        """Materialise up to ``max_failed_rows_sample`` rows per failing check.

        Off by default (``0``): no ``failed_rows`` fetch happens, so no cell
        values leave the process. The per-run ``max_failed_rows`` config already
        caps the fetch, so ``head`` here is the second of the two caps.
        """
        if self._max_failed_rows_sample <= 0:
            return {}
        samples: dict[int, list[dict]] = {}
        for check_result in result.check_results:
            if check_result.status not in ("FAILED", "ERROR"):
                continue
            failed_rows = check_result.failed_rows
            if len(failed_rows) == 0:
                continue
            rows = failed_rows.head(self._max_failed_rows_sample).to_arrow().to_pylist()
            if rows:
                samples[id(check_result)] = rows
        return samples


def export(result: ValidationResult, **kwargs: Any) -> str:
    """Functional form of :meth:`ValidationResult.export_otel`."""
    return OtelExporter(**kwargs).export(result)
