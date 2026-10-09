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
    """Multi-table validation summary including failing row counts."""

    failed_non_unique_rows: int


@dataclass(frozen=True)
class SchemaValidationBreakdown:
    """Typed breakdown of validation metrics for one schema."""

    overall: OverallSummary
    single_table: SingleTableSummary
    multi_table: MultiTableSummary
