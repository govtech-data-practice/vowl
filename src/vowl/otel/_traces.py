"""Trace emitter: pipeline observability and correlation.

Emits one root ``{namespace}.validate`` span per run and one
``{namespace}.check`` child span per check, laid out as a timed waterfall using
explicit start/end timestamps derived from the recorded execution times. Spans
join any active context, so a run nested inside an instrumented orchestrator
attaches to the parent trace.

``emit`` returns the span context of each check span keyed by ``id(check)`` so
the log emitter can backlink a failure record to its span.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from ._common import check_attributes, check_query, coerce_attr, flatten_check_definition

if TYPE_CHECKING:
    from ..validation.result import ValidationResult

_MS_TO_NS = 1_000_000


class TraceEmitter:
    """Records the run/check span tree and returns per-check span contexts."""

    def __init__(
        self,
        tracer_provider: Any,
        *,
        namespace: str,
        sample_rows_by_check: dict[int, list[dict]],
        context_attributes: dict[str, Any] | None = None,
    ) -> None:
        self._tracer = tracer_provider.get_tracer("vowl")
        self._ns = namespace
        self._samples = sample_rows_by_check
        self._ctx_attrs: dict[str, Any] = context_attributes or {}

    def emit(self, result: ValidationResult) -> dict[int, Any]:
        from opentelemetry.trace import SpanContext, Status, StatusCode, set_span_in_context

        vs = result._vs
        total_ms = float(vs.get("total_execution_time_ms", 0.0) or 0.0)
        end_ns = time.time_ns()
        start_ns = end_ns - int(total_ms * _MS_TO_NS)

        root_attrs = dict(self._ctx_attrs)
        root_attrs.update({
            "total_checks": int(vs.get("total_checks", 0)),
            "passed": int(vs.get("passed", 0)),
            "failed": int(vs.get("failed", 0)),
            "errors": int(vs.get("errors", 0)),
            "success_rate": float(vs.get("success_rate", 0.0) or 0.0),
        })
        root = self._tracer.start_span(
            f"{self._ns}.validate",
            start_time=start_ns,
            attributes=root_attrs,
        )
        root.set_status(Status(StatusCode.OK if result.passed else StatusCode.ERROR))
        root_ctx = set_span_in_context(root)

        contexts: dict[int, SpanContext] = {}
        cursor_ns = start_ns
        for check_result in result.check_results:
            duration_ns = int(float(check_result.execution_time_ms or 0.0) * _MS_TO_NS)
            child = self._tracer.start_span(
                f"{self._ns}.check",
                context=root_ctx,
                start_time=cursor_ns,
                attributes=self._check_span_attributes(check_result),
            )
            if check_result.status in ("FAILED", "ERROR"):
                child.set_status(Status(StatusCode.ERROR, check_result.details or None))
            else:
                child.set_status(Status(StatusCode.OK))

            self._attach_sample_events(child, check_result)
            contexts[id(check_result)] = child.get_span_context()
            child.end(end_time=cursor_ns + duration_ns)
            cursor_ns += duration_ns

        root.end(end_time=end_ns)
        return contexts

    def _check_span_attributes(self, check_result: Any) -> dict[str, Any]:
        attrs = dict(self._ctx_attrs)
        attrs.update(check_attributes(check_result))
        metadata = check_result.metadata
        extra = {
            "operator": metadata.get("operator"),
            "query": check_query(check_result),
            "expected_value": coerce_attr(check_result.expected_value),
            "actual_value": coerce_attr(check_result.actual_value),
            "failed_rows_count": int(check_result.failed_rows_count or 0),
        }
        attrs.update({k: v for k, v in extra.items() if v is not None})
        # Full authored definition, flattened, as queryable check.definition.* keys
        # (spans/logs only, never metrics). Namespaced, so no collision with the
        # curated bare keys above.
        attrs.update(flatten_check_definition(check_result))
        return attrs

    def _attach_sample_events(self, span: Any, check_result: Any) -> None:
        sample = self._samples.get(id(check_result))
        if not sample:
            return
        for row in sample:
            span.add_event(
                f"{self._ns}.failed_row",
                attributes={key: coerce_attr(value) for key, value in row.items() if coerce_attr(value) is not None},
            )
