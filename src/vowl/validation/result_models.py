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
class SingleTableSummary(CheckStatusSummary):
    """Single-table validation summary including row-quality counts."""

    failed_unique_rows: int
    passed_unique_rows: int
    total_rows: int | None
    passed_row_percentage: float | None


@dataclass(frozen=True)
class MultiTableSummary(CheckStatusSummary):
    """Multi-table validation summary including failing row counts."""

    failed_non_unique_rows: int


@dataclass(frozen=True)
class SchemaValidationBreakdown:
    """Typed breakdown of validation metrics for one schema."""

    overall: CheckStatusSummary
    single_table: SingleTableSummary
    multi_table: MultiTableSummary


@dataclass(frozen=True)
class RowQualitySummary:
    """Typed row-quality counters for a schema or aggregate result."""

    total_rows: int
    records_with_issues: int
    clean_records: int
    data_quality: float
