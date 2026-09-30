"""Metric emitter: the primary signal (dashboards and alerting).

Walks a finished ``ValidationResult`` read-only and records into OTEL
instruments per the semantic convention (v1). Additive instruments (the check
counter and the duration histograms) rely on the provider being configured for
delta temporality (see :mod:`._providers`). Row counts and rates are gauges.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ._common import check_attributes, check_dimension

if TYPE_CHECKING:
    from ..validation.result import ValidationResult


class MetricEmitter:
    """Records the v1 metric instruments from a validation result."""

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
        self._emit_check_metrics(result)
        self._emit_dimension_metrics(result)
        self._emit_schema_metrics(result)
        self._emit_run_metrics(result)

    def _name(self, suffix: str) -> str:
        return f"{self._ns}.{suffix}"

    def _set_row_counts(self, gauge: Any, total: int, failed: int, attrs: dict[str, Any]) -> None:
        """Record one ``PASSED`` and one ``FAILED`` row-count point, zeros included.

        Row counts are gauges, not counters: a row failing two checks, or
        re-checked on every run, would be counted twice by any sum. Both points
        are always sent so a dashboard showing the latest value never keeps a
        failure count from an earlier run.
        """
        gauge.set(max(total - failed, 0), self._with_ctx({**attrs, "status": "PASSED"}))
        gauge.set(failed, self._with_ctx({**attrs, "status": "FAILED"}))

    def _emit_check_metrics(self, result: ValidationResult) -> None:
        count = self._meter.create_counter(self._name("check.count"), unit="{check}")
        duration = self._meter.create_histogram(self._name("check.duration"), unit="ms")
        row_count = self._meter.create_gauge(self._name("check.row.count"), unit="{row}")
        row_pass_rate = self._meter.create_gauge(self._name("check.row_pass_rate"), unit="1")

        total_by_schema = result._vs.get("total_rows_by_schema", {})

        for cr in result.check_results:
            count.add(1, self._with_ctx(check_attributes(cr)))

            metadata = cr.metadata
            duration_attrs = {
                "check_name": cr.check_name,
                "schema_name": metadata.get("schema_name"),
                "engine": metadata.get("engine"),
            }
            duration.record(
                float(cr.execution_time_ms or 0.0),
                self._with_ctx({k: v for k, v in duration_attrs.items() if v is not None}),
            )

            schema = metadata.get("schema_name")
            failed = cr.failed_rows_count or 0
            rate_attrs: dict[str, str] = {"check_name": cr.check_name, "dimension": check_dimension(cr)}
            if schema:
                rate_attrs["schema_name"] = schema

            # Only checks that report failing rows get row counts and a row pass
            # rate. An aggregate check (or one that errored) would always show
            # every row as passing.
            total = total_by_schema.get(schema) if schema else None
            if total and cr.status != "ERROR" and cr.supports_row_level_output:
                self._set_row_counts(row_count, total, failed, rate_attrs)
                row_pass_rate.set((total - failed) / total, self._with_ctx(rate_attrs))

    def _emit_dimension_metrics(self, result: ValidationResult) -> None:
        check_pass_rate = self._meter.create_gauge(self._name("dimension.check_pass_rate"), unit="1")
        for (schema, dimension), rate in result._check_pass_rate_by_dimension().items():
            check_pass_rate.set(rate, self._with_ctx({"schema_name": schema, "dimension": dimension}))

        row_count = self._meter.create_gauge(self._name("dimension.row.count"), unit="{row}")
        row_pass_rate = self._meter.create_gauge(self._name("dimension.row_pass_rate"), unit="1")
        failed_by_dim = result._failed_rows_by_dimension()
        total_by_schema = result._vs.get("total_rows_by_schema", {})
        for schema, dimension in sorted(result._row_eligible_dimensions()):
            attrs = {"schema_name": schema, "dimension": dimension}
            total = total_by_schema[schema]
            failed = failed_by_dim.get((schema, dimension), 0)
            self._set_row_counts(row_count, total, failed, attrs)
            row_pass_rate.set((total - failed) / total, self._with_ctx(attrs))

    def _emit_schema_metrics(self, result: ValidationResult) -> None:
        # Same eligibility as the schema row pass rate below: schemas with at
        # least one check that reports failing rows.
        row_count = self._meter.create_gauge(self._name("schema.row.count"), unit="{row}")
        for schema, summary in result._get_row_quality_summary_by_schema().items():
            self._set_row_counts(
                row_count,
                int(summary["total_rows"]),
                int(summary["records_with_issues"]),
                {"schema_name": schema},
            )

        check_pass_rate = self._meter.create_gauge(self._name("schema.check_pass_rate"), unit="1")
        schema_totals: dict[str, int] = {}
        schema_passed: dict[str, int] = {}
        for cr in result.check_results:
            schema = cr.metadata.get("schema_name")
            if not isinstance(schema, str):
                continue
            schema_totals[schema] = schema_totals.get(schema, 0) + 1
            if cr.status == "PASSED":
                schema_passed[schema] = schema_passed.get(schema, 0) + 1
        for schema, total in schema_totals.items():
            check_pass_rate.set(schema_passed.get(schema, 0) / total, self._with_ctx({"schema_name": schema}))

        row_pass_rate = self._meter.create_gauge(self._name("schema.row_pass_rate"), unit="1")
        for schema, summary in result._get_row_quality_summary_by_schema().items():
            row_pass_rate.set(float(summary["data_quality"]) / 100.0, self._with_ctx({"schema_name": schema}))

    def _emit_run_metrics(self, result: ValidationResult) -> None:
        # A Histogram, not a Counter: a duration is a timing to average or take
        # percentiles of, not a quantity to add up across runs.
        run_duration = self._meter.create_histogram(self._name("run.duration"), unit="ms")
        run_duration.record(self._run_duration_ms(result), self._with_ctx({}))

    @staticmethod
    def _run_duration_ms(result: ValidationResult) -> float:
        """Wall-clock run time when the runner recorded it, else summed check time."""
        started = getattr(result, "_run_started_ns", None)
        finished = getattr(result, "_run_finished_ns", None)
        if started is not None and finished is not None and finished >= started:
            return (finished - started) / 1_000_000
        return float(result._vs.get("total_execution_time_ms", 0.0) or 0.0)
