"""Metric emitter: the primary signal (dashboards and alerting).

Records the points of :func:`vowl.validation.dq_metrics.compute_points` into
OTEL instruments, so the metrics carry exactly the numbers ``dq_metrics.json``
does. Counters (check and schema counts) and histograms (durations) rely on the
provider being configured for delta temporality (see :mod:`._providers`). Row
counts and pass rates are gauges.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..validation.dq_metrics import COUNTER, GAUGE, MetricPoint, compute_points

if TYPE_CHECKING:
    from ..validation.result import ValidationResult


class MetricEmitter:
    """Records the DQ metric points of a validation result."""

    def __init__(
        self,
        meter_provider: Any,
        *,
        namespace: str,
        context_attributes: dict[str, Any] | None = None,
    ) -> None:
        self._meter = meter_provider.get_meter("vowl")
        self._ns = namespace
        self._ctx_attrs: dict[str, Any] = context_attributes or {}

    def _with_ctx(self, attrs: dict[str, Any]) -> dict[str, Any]:
        if not self._ctx_attrs:
            return attrs
        merged = dict(self._ctx_attrs)
        merged.update(attrs)
        return merged

    def emit(self, result: ValidationResult) -> None:
        instruments: dict[str, Any] = {}
        for point in compute_points(result, prefix=self._ns):
            instrument = instruments.get(point.name)
            if instrument is None:
                instrument = instruments[point.name] = self._create(point)
            attrs = self._with_ctx(point.attributes)
            if point.type == COUNTER:
                instrument.add(point.value, attrs)
            elif point.type == GAUGE:
                instrument.set(point.value, attrs)
            else:
                instrument.record(point.value, attrs)

    def _create(self, point: MetricPoint) -> Any:
        if point.type == COUNTER:
            return self._meter.create_counter(point.name, unit=point.unit)
        if point.type == GAUGE:
            return self._meter.create_gauge(point.name, unit=point.unit)
        return self._meter.create_histogram(point.name, unit=point.unit)
