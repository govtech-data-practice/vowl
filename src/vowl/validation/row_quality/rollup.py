"""The report: schema, dimension, check and run rollups of the merged rows.

See "Step 5: Roll up" and "Step 6: Trust metadata" in
``design/row-quality-statistics.md``.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from ...config import RowIssueScope


@dataclass(frozen=True)
class SchemaRowQuality:
    """Row-quality numbers for one schema.

    ``failed_rows``, ``tolerated_rows``, ``passed_rows`` and ``pass_rate`` are
    None when no check was counted. ``pass_rate`` is also None when the table
    is empty or its row count is unknown. ``tolerated_rows`` is None when
    tolerated rows were not collected.
    """

    schema_name: str
    total_rows: int | None
    failed_rows: int | None
    tolerated_rows: int | None
    passed_rows: int | None
    pass_rate: float | None
    exact: bool
    checks_counted: int
    checks_not_counted: int


@dataclass(frozen=True)
class DimensionRowQuality:
    """Row-quality numbers for the checks of one dimension of one schema."""

    schema_name: str
    dimension: str
    total_rows: int | None
    failed_rows: int | None
    tolerated_rows: int | None
    passed_rows: int | None
    pass_rate: float | None
    exact: bool
    checks_counted: int
    checks_not_counted: int


@dataclass(frozen=True)
class CheckRowQuality:
    """How one check took part in the row-quality numbers.

    Attributes:
        route: ``"server_predicate"``, ``"server_lookup"``, ``"client_lookup"`` or
            ``"client_returned_rows"``, or empty when the check's rows were not
            collected.
        reason: Why the check was not counted, or why it left pushdown.
        failed_rows: The check's own physical failing rows, before the merge.
        exact: False when this check's rows are incomplete or approximate.
    """

    schema_name: str
    check_name: str
    dimension: str
    status: str
    counted: bool
    tolerated: bool
    route: str
    reason: str
    failed_rows: int | None
    exact: bool


@dataclass(frozen=True)
class RowQualityReport:
    """Every row-quality number of one validation run.

    Attributes:
        schemas: One entry per schema.
        dimensions: One entry per (schema, dimension) with at least one check.
        checks: One entry per check anchored to a schema.
        enabled: False when ``enable_additional_schema_statistics`` is off.
        weighted_pass_rate: Passed rows over total rows, summed across the
            schemas that have a pass rate. Weighted by table size.
        mean_pass_rate: The mean of the per-schema pass rates.
    """

    schemas: list[SchemaRowQuality] = field(default_factory=list)
    dimensions: list[DimensionRowQuality] = field(default_factory=list)
    checks: list[CheckRowQuality] = field(default_factory=list)
    enabled: bool = True
    weighted_pass_rate: float | None = None
    mean_pass_rate: float | None = None

    def schema(self, schema_name: str) -> SchemaRowQuality | None:
        return next((item for item in self.schemas if item.schema_name == schema_name), None)


@dataclass
class CheckState:
    """The rollup's view of one check of a schema.

    Attributes:
        check_id: Schema-local id, the bit the check owns in the masks.
        dimension: The check's dimension.
        counted: The check is counted (after run-time drops).
        failed: The check FAILED. False for tolerated and zero-count checks.
        collected: The check's rows are in the merged entries.
        skipped: A tolerated check whose rows were not collected.
        inexact: The check makes its buckets inexact.
        own_rows: The check's own failing rows, when known.
    """

    check_id: int
    dimension: str
    counted: bool
    failed: bool
    collected: bool
    skipped: bool = False
    inexact: bool = False
    own_rows: int | None = None


def own_rows_from_entries(entries: Iterable[tuple[int, int]], check_ids: Sequence[int]) -> dict[int, int]:
    """Sum the copies of the entries each check caught."""
    totals = dict.fromkeys(check_ids, 0)
    for mask, copies in entries:
        for check_id in check_ids:
            if mask >> check_id & 1:
                totals[check_id] += copies
    return totals


def _mask(states: Iterable[CheckState]) -> int:
    mask = 0
    for state in states:
        mask |= 1 << state.check_id
    return mask


def roll_up_bucket(
    states: Sequence[CheckState],
    entries: Sequence[tuple[int, int]],
    total_rows: int | None,
    *,
    scope: RowIssueScope,
    exact: bool,
) -> tuple[int | None, int | None, int | None, float | None, bool, int]:
    """Roll up one bucket of checks.

    Returns:
        ``(failed_rows, tolerated_rows, passed_rows, pass_rate, exact,
        checks_counted)``.
    """
    counted = [state for state in states if state.counted]
    exact = exact and not any(state.inexact for state in states)
    if not counted:
        return None, None, None, None, exact, 0

    collected = [state for state in counted if state.collected]
    strict_mask = _mask(state for state in collected if state.failed)
    all_mask = _mask(collected)
    strict = sum(copies for mask, copies in entries if mask & strict_mask)
    everything = sum(copies for mask, copies in entries if mask & all_mask)

    failed_rows = strict if scope == "failed_checks" else everything
    tolerated_rows = None if any(state.skipped for state in counted) else everything - strict
    passed_rows = max(total_rows - failed_rows, 0) if total_rows is not None else None
    pass_rate = passed_rows / total_rows if total_rows and passed_rows is not None else None
    return failed_rows, tolerated_rows, passed_rows, pass_rate, exact, len(counted)


def run_level_rates(schemas: Sequence[SchemaRowQuality]) -> tuple[float | None, float | None]:
    """The weighted and the mean pass rate across schemas that have one."""
    rated = [item for item in schemas if item.pass_rate is not None and item.total_rows]
    if not rated:
        return None, None
    total = sum(item.total_rows or 0 for item in rated)
    failed = sum(item.failed_rows or 0 for item in rated)
    weighted = max(total - failed, 0) / total if total else None
    mean = sum(item.pass_rate or 0.0 for item in rated) / len(rated)
    return weighted, mean
