"""Internal typed models for validation result summaries."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CheckStatusSummary:
    """Counts of passed, errored, and total checks."""

    passed_checks: int
    error_checks: int
    total_checks: int


@dataclass(frozen=True)
class OverallSummary(CheckStatusSummary):
    """A schema's check counts plus its approximate failed rows.

    ``failed_rows_approximate`` sums ``failed_rows_count`` over the schema's
    row-level checks. A row caught by two checks counts twice, so the sum is
    approximate. It is None when the schema has no row-level check.
    """

    failed_rows_approximate: int | None


@dataclass(frozen=True)
class SingleTableSummary(CheckStatusSummary):
    """Check counts of the checks that read one table."""


@dataclass(frozen=True)
class MultiTableSummary(CheckStatusSummary):
    """Check counts of the checks that read more than one table, plus their approximate failed rows.

    ``failed_rows_approximate`` is computed like the one in ``OverallSummary``,
    over the multi-table row-level checks only.
    """

    failed_rows_approximate: int | None


@dataclass(frozen=True)
class SchemaValidationBreakdown:
    """Typed breakdown of validation metrics for one schema."""

    overall: OverallSummary
    single_table: SingleTableSummary
    multi_table: MultiTableSummary


@dataclass(frozen=True)
class SaveRecord:
    """One call to ``ValidationResult.save()``, kept for the OpenTelemetry trace.

    ``started_ns`` and ``finished_ns`` are epoch nanoseconds, like the run
    window. ``outputs`` holds the outputs the call wrote, sorted.
    ``file_count`` counts the files it wrote. ``error`` is
    ``"<ExceptionType>: <message>"`` when the call raised, else None.
    """

    started_ns: int
    finished_ns: int
    outputs: tuple[str, ...]
    file_count: int
    error: str | None = None
