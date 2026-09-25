"""Metric emitter: the primary signal (dashboards and alerting).

Walks a finished ``ValidationResult`` read-only and records into OTEL
instruments per the semantic convention (v1). Additive instruments rely on the
provider being configured for delta temporality (see :mod:`._providers`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ._common import check_attributes

if TYPE_CHECKING:
    from ..validation.result import ValidationResult


class MetricEmitter:
    """Records the v1 metric instruments from a validation result."""

    def __init__(self, meter_provider: Any, *, namespace: str) -> None:
        self._meter = meter_provider.get_meter("vowl")
        self._ns = namespace

    def emit(self, result: ValidationResult) -> None:
        self._emit_check_counts(result)
        self._emit_check_durations(result)
        self._emit_rows(result)
        self._emit_run_duration(result)
        self._emit_gauges(result)

    def _name(self, suffix: str) -> str:
        return f"{self._ns}.{suffix}"

    def _emit_check_counts(self, result: ValidationResult) -> None:
        checks = self._meter.create_counter(self._name("checks"), unit="{check}")
        for check_result in result.check_results:
            checks.add(1, check_attributes(check_result))

    def _emit_check_durations(self, result: ValidationResult) -> None:
        duration = self._meter.create_histogram(self._name("check.duration"), unit="ms")
        for check_result in result.check_results:
            metadata = check_result.metadata
            attrs = {
                "check_name": check_result.check_name,
                "schema_name": metadata.get("schema_name"),
                "engine": metadata.get("engine"),
            }
            duration.record(
                float(check_result.execution_time_ms or 0.0),
                {k: v for k, v in attrs.items() if v is not None},
            )

    def _emit_rows(self, result: ValidationResult) -> None:
        # Numerator: unique failing rows per (schema, dimension), deduplicated so
        # a row failing two checks in one dimension counts once.
        failed = self._meter.create_counter(self._name("failed_rows"), unit="{row}")
        for (schema, dimension), count in result._failed_rows_by_dimension().items():
            if count:
                failed.add(count, {"schema_name": schema, "dimension": dimension})

        # Denominator: one contribution per schema per run.
        total = self._meter.create_counter(self._name("rows.total"), unit="{row}")
        for schema, row_count in result._vs.get("total_rows_by_schema", {}).items():
            if row_count:
                total.add(int(row_count), {"schema_name": schema})

    def _emit_run_duration(self, result: ValidationResult) -> None:
        run_duration = self._meter.create_counter(self._name("run.duration"), unit="ms")
        run_duration.add(float(result._vs.get("total_execution_time_ms", 0.0) or 0.0))

    def _emit_gauges(self, result: ValidationResult) -> None:
        # Per-run convenience gauges. Never the aggregation primitive: windowed
        # rates are recomputed downstream from failed_rows / rows.total.
        data_quality = self._meter.create_gauge(self._name("dq.data_quality"), unit="1")
        for schema, summary in result._get_row_quality_summary_by_schema().items():
            data_quality.set(float(summary["data_quality"]) / 100.0, {"schema_name": schema})

        pass_rate = self._meter.create_gauge(self._name("dq.pass_rate"), unit="1")
        for (schema, dimension), rate in result._check_pass_rate_by_dimension().items():
            pass_rate.set(rate, {"schema_name": schema, "dimension": dimension})
