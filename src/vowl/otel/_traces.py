"""Trace emitter: pipeline observability and correlation.

Emits one root ``{namespace}.validate`` span per run and one
``{namespace}.check`` child span per check. The root span uses the real run
start and end recorded by the runner when they exist. Each child keeps its real
duration, but its start is approximate because vowl does not record when each
check began (checks can run in parallel). Spans join any active context, so a
run nested inside an instrumented orchestrator attaches to the parent trace.

``emit`` returns the span context of each check span keyed by ``id(check)`` so
the log emitter can backlink a failure record to its span.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from ..validation.dq_metrics import check_pass_rate, check_status_counts
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

        start_ns, end_ns = self._run_window(result)

        # The run-level check numbers, named like vowl.run.check.count and
        # vowl.run.check.pass_rate without the level (the span is the run).
        counts = check_status_counts(result.check_results)
        root_attrs = dict(self._ctx_attrs)
        root_attrs.update({f"check.count.{status.lower()}": count for status, count in counts.items()})
        pass_rate = check_pass_rate(counts)
        if pass_rate is not None:
            root_attrs["check.pass_rate"] = pass_rate
        root = self._tracer.start_span(
            f"{self._ns}.validate",
            start_time=start_ns,
            attributes=root_attrs,
        )
        # ERROR only if a check could not run. A FAILED check did its job, so
        # it stays OK and shows up through ``check.count.failed`` instead. This
        # keeps span error rates about broken checks, not bad data.
        run_ok = counts["ERROR"] == 0
        root.set_status(Status(StatusCode.OK if run_ok else StatusCode.ERROR))
        root_ctx = set_span_in_context(root)

        durations = [int(float(cr.execution_time_ms or 0.0) * _MS_TO_NS) for cr in result.check_results]
        # Lay checks out one after another. If that overruns the root span the
        # checks ran in parallel, so start them all at the root start instead.
        sequential = start_ns + sum(durations) <= end_ns

        contexts: dict[int, SpanContext] = {}
        cursor_ns = start_ns
        for check_result, duration_ns in zip(result.check_results, durations, strict=True):
            child_start = cursor_ns if sequential else start_ns
            child = self._tracer.start_span(
                f"{self._ns}.check",
                context=root_ctx,
                start_time=child_start,
                attributes=self._check_span_attributes(check_result),
            )
            # FAILED checks stay OK. The ``status`` attribute carries FAILED.
            if check_result.status == "ERROR":
                child.set_status(Status(StatusCode.ERROR, check_result.details or None))
            else:
                child.set_status(Status(StatusCode.OK))

            self._attach_sample_events(child, check_result)
            contexts[id(check_result)] = child.get_span_context()
            child.end(end_time=min(child_start + duration_ns, end_ns))
            cursor_ns += duration_ns

        root.end(end_time=end_ns)
        return contexts

    @staticmethod
    def _run_window(result: ValidationResult) -> tuple[int, int]:
        """The run's start and end in epoch nanoseconds.

        Uses the wall-clock times the runner recorded. A result built another
        way has none, so the window ends now and spans the summed check time.
        """
        started = getattr(result, "_run_started_ns", None)
        finished = getattr(result, "_run_finished_ns", None)
        if started is not None and finished is not None and finished >= started:
            return started, finished
        total_ms = float(result._vs.get("total_execution_time_ms", 0.0) or 0.0)
        end_ns = time.time_ns()
        return end_ns - int(total_ms * _MS_TO_NS), end_ns

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
