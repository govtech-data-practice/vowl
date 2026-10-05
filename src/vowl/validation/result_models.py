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
    """A schema's check counts plus its row-quality numbers.

    ``passed_rows`` and ``passed_row_percentage`` are None when no check was
    counted or the numbers are unavailable. ``exact`` is False when the
    numbers are approximate. ``checks_not_attributed`` counts the counted
    checks whose rows are not in the numbers.
    """

    failed_rows: int | None
    passed_rows: int | None
    total_rows: int | None
    passed_row_percentage: float | None
    exact: bool
    checks_not_attributed: int = 0


@dataclass(frozen=True)
class SingleTableSummary(CheckStatusSummary):
    """Check counts of the checks that read one table."""


@dataclass(frozen=True)
class MultiTableSummary(CheckStatusSummary):
    """Multi-table validation summary including failing row counts."""

    failed_non_unique_rows: int


@dataclass(frozen=True)
class SchemaValidationBreakdown:
    """Typed breakdown of validation metrics for one schema."""

    overall: OverallSummary
    single_table: SingleTableSummary
    multi_table: MultiTableSummary
