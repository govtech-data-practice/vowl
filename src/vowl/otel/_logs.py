"""Log emitter: routing failures into a log pipeline.

Emits one structured record per FAILED (WARN) or ERROR (ERROR) check. Passing
checks are silent. Each record carries the check span's ``trace_id``/``span_id``
when traces were emitted, so an alert links back to the trace, plus the opt-in
bounded row sample when the caller enabled it.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from ._common import check_attributes, check_query, check_row_attributes, flatten_check_definition, severity_for

if TYPE_CHECKING:
    from ..validation.result import ValidationResult


def _log_record_class() -> type:
    """The ``LogRecord`` class this SDK's ``Logger.emit`` expects.

    Older SDKs (such as 1.27, the minimum vowl supports) export their own
    ``LogRecord``. Newer ones dropped it and accept the API ``LogRecord``.
    """
    try:
        from opentelemetry.sdk._logs import LogRecord
    except ImportError:
        from opentelemetry._logs import LogRecord
    return LogRecord


class LogEmitter:
    """Records one log per failing/erroring check with an optional backlink."""

    def __init__(
        self,
        logger_provider: Any,
        *,
        namespace: str,
        span_contexts: dict[int, Any],
        sample_rows_by_check: dict[int, list[dict]],
        context_attributes: dict[str, Any] | None = None,
    ) -> None:
        self._logger = logger_provider.get_logger("vowl")
        self._ns = namespace
        self._span_contexts = span_contexts
        self._samples = sample_rows_by_check
        self._ctx_attrs: dict[str, Any] = context_attributes or {}

    def emit(self, result: ValidationResult) -> None:
        from opentelemetry.trace import TraceFlags

        LogRecord = _log_record_class()

        row_attrs = check_row_attributes(result)
        for check_result in result.check_results:
            severity = severity_for(check_result.status)
            if severity is None:
                continue
            severity_number, severity_text = severity

            attrs = dict(self._ctx_attrs)
            attrs.update(check_attributes(check_result))
            attrs.update(row_attrs.get(id(check_result), {}))
            query = check_query(check_result)
            if query is not None:
                attrs["query"] = query
            # Full authored definition flattened for alert triage (check.definition.*).
            attrs.update(flatten_check_definition(check_result))
            sample = self._samples.get(id(check_result))
            if sample:
                attrs[f"{self._ns}.failed_rows_sample"] = json.dumps(sample, default=str)

            trace_id, span_id, trace_flags = 0, 0, TraceFlags(TraceFlags.DEFAULT)
            span_context = self._span_contexts.get(id(check_result))
            if span_context is not None:
                trace_id, span_id = span_context.trace_id, span_context.span_id
                trace_flags = span_context.trace_flags

            verb = "errored" if check_result.status == "ERROR" else "failed"
            body = check_result.details or f"{self._ns}.check {verb}: {check_result.check_name}"

            record = self._build_record(
                LogRecord,
                trace_id=trace_id,
                span_id=span_id,
                trace_flags=trace_flags,
                severity_number=severity_number,
                severity_text=severity_text,
                body=body,
                attributes=attrs,
            )
            self._logger.emit(record)

    @staticmethod
    def _build_record(log_record_cls: Any, **kwargs: Any) -> Any:
        """Construct a ``LogRecord``, tolerating field churn across SDK versions.

        The logs signal is not yet stable, so the constructor's accepted fields
        differ between releases (e.g. ``resource`` was dropped). Pass only the
        keyword arguments this version's ``__init__`` actually declares.
        """
        import inspect

        accepted = set(inspect.signature(log_record_cls.__init__).parameters)
        return log_record_cls(**{key: value for key, value in kwargs.items() if key in accepted})
