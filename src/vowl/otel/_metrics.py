"""Metric emitter: the primary signal (dashboards and alerting).

Walks a finished ``ValidationResult`` read-only and records into OTEL
instruments per the semantic convention (v1). Additive instruments rely on the
provider being configured for delta temporality (see :mod:`._providers`).
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

    def _emit_check_metrics(self, result: ValidationResult) -> None:
        count = self._meter.create_counter(self._name("check.count"), unit="{check}")
        duration = self._meter.create_histogram(self._name("check.duration"), unit="ms")
        failed_rows = self._meter.create_counter(self._name("check.failed_rows"), unit="{row}")
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
            row_count = cr.failed_rows_count or 0
            rate_attrs: dict[str, str] = {"check_name": cr.check_name, "dimension": check_dimension(cr)}
            if schema:
                rate_attrs["schema_name"] = schema

            if row_count:
                failed_rows.add(row_count, self._with_ctx(rate_attrs))

            total = total_by_schema.get(schema) if schema else None
            if total:
                row_pass_rate.set((total - row_count) / total, self._with_ctx(rate_attrs))

    def _emit_dimension_metrics(self, result: ValidationResult) -> None:
        failed_rows = self._meter.create_counter(self._name("dimension.failed_rows"), unit="{row}")
        for (schema, dimension), dim_count in result._failed_rows_by_dimension().items():
            if dim_count:
                failed_rows.add(dim_count, self._with_ctx({"schema_name": schema, "dimension": dimension}))

        check_pass_rate = self._meter.create_gauge(self._name("dimension.check_pass_rate"), unit="1")
        for (schema, dimension), rate in result._check_pass_rate_by_dimension().items():
            check_pass_rate.set(rate, self._with_ctx({"schema_name": schema, "dimension": dimension}))

        row_pass_rate = self._meter.create_gauge(self._name("dimension.row_pass_rate"), unit="1")
        failed_by_dim = result._failed_rows_by_dimension()
        total_by_schema = result._vs.get("total_rows_by_schema", {})
        for (schema, dimension) in result._check_pass_rate_by_dimension():
            total = total_by_schema.get(schema)
            if not total:
                continue
            failed = failed_by_dim.get((schema, dimension), 0)
            row_pass_rate.set((total - failed) / total, self._with_ctx({"schema_name": schema, "dimension": dimension}))

    def _emit_schema_metrics(self, result: ValidationResult) -> None:
        failed_rows = self._meter.create_counter(self._name("schema.failed_rows"), unit="{row}")
        for schema, summary in result._get_row_quality_summary_by_schema().items():
            schema_count = int(summary["records_with_issues"])
            if schema_count:
                failed_rows.add(schema_count, self._with_ctx({"schema_name": schema}))

        rows_total = self._meter.create_counter(self._name("schema.rows_total"), unit="{row}")
        for schema, row_count in result._vs.get("total_rows_by_schema", {}).items():
            if row_count:
                rows_total.add(int(row_count), self._with_ctx({"schema_name": schema}))

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
        run_duration = self._meter.create_counter(self._name("run.duration"), unit="ms")
        run_duration.add(float(result._vs.get("total_execution_time_ms", 0.0) or 0.0), self._with_ctx({}))
