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
    None when no counted check was attributed. ``pass_rate`` is also None when
    the table is empty or its row count is unknown. ``tolerated_rows`` is None
    when tolerated rows were not collected. ``checks_not_attributed`` counts the
    counted checks whose rows are not in these numbers.
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
    checks_not_attributed: int


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
    checks_not_attributed: int


@dataclass(frozen=True)
class CheckRowQuality:
    """How one check took part in the row-quality numbers.

    Attributes:
        route: ``"server_predicate"``, ``"server_lookup"``, ``"client_lookup"`` or
            ``"server_scalar"``, or empty when the check is not counted or not
            attributed. ``"server_scalar"`` is used only under
            ``disable_table_attributed_counts``.
        reason: Why the check was not counted or not attributed, or why it
            left pushdown.
        scalar_count: The number the check's own query returned, as a count.
            It decides pass or fail, and is what the summary shows as ``actual``.
        attributed_rows: The rows of the table the check caught, before the
            merge. It differs from ``scalar_count`` when the check's query does
            not return each failing row of the table once, for example under
            ``DISTINCT``. None when the check is not attributed.
        attributed: The check's attributed rows are in the row counts.
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
    scalar_count: int | None
    attributed_rows: int | None
    attributed: bool
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
        counted: The check is about bad rows.
        failed: The check FAILED. False for tolerated and zero-count checks.
        collected: The check's rows are in the merged entries.
        attributed: The check's attributed rows are in the row counts.
        scalar: The check is counted from its scalar count, outside the
            merged entries. Only under ``disable_table_attributed_counts``.
        inexact: The check makes its buckets inexact.
        attributed_rows: The check's attributed rows, when known.
        scalar_count: The count the check's own query returned.
    """

    check_id: int
    dimension: str
    counted: bool
    failed: bool
    collected: bool
    attributed: bool = True
    scalar: bool = False
    inexact: bool = False
    attributed_rows: int | None = None
    scalar_count: int | None = None


def attributed_rows_from_entries(entries: Iterable[tuple[int, int]], check_ids: Sequence[int]) -> dict[int, int]:
    """Sum the copies of the entries each check caught."""
    totals = dict.fromkeys(check_ids, 0)
    for mask, copies in entries:
        for check_id in check_ids:
            if mask >> check_id & 1:
                totals[check_id] += copies
    return totals


def bucket_rows(state: CheckState) -> int | None:
    """The rows *state* adds to its buckets: its attributed rows, or its scalar count when counted from it."""
    if state.scalar:
        return state.scalar_count
    return state.attributed_rows if state.attributed else None


def _may_have_rows(state: CheckState) -> bool:
    return state.scalar_count is None or state.scalar_count > 0


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
) -> tuple[int | None, int | None, int | None, float | None, bool, int, int]:
    """Roll up one bucket of checks.

    A counted check that is not attributed adds nothing, unless it is counted
    from its scalar count. When it caught rows in scope the bucket is not exact.

    Returns:
        ``(failed_rows, tolerated_rows, passed_rows, pass_rate, exact,
        checks_counted, checks_not_attributed)``.
    """
    counted = [state for state in states if state.counted]
    exact = exact and not any(state.inexact for state in states)
    if not counted:
        return None, None, None, None, exact, 0, 0
    not_attributed = [state for state in counted if not state.attributed]
    if any(
        not state.scalar and (state.failed or scope == "all_violations") and _may_have_rows(state)
        for state in not_attributed
    ):
        # Rows in scope are missing from the numbers.
        exact = False
    if not any(state.attributed or state.scalar for state in counted):
        return None, None, None, None, exact, len(counted), len(not_attributed)

    collected = [state for state in counted if state.collected]
    strict_mask = _mask(state for state in collected if state.failed)
    all_mask = _mask(collected)
    strict = sum(copies for mask, copies in entries if mask & strict_mask)
    everything = sum(copies for mask, copies in entries if mask & all_mask)

    scalars = [state for state in counted if state.scalar]
    if scalars:
        # Scalars cannot tell which rows overlap, so their sum is exact only
        # when no other check of the bucket has failing rows.
        with_rows = [state for state in counted if (bucket_rows(state) or 0) > 0]
        if len(with_rows) > 1 and any(state.scalar for state in with_rows):
            exact = False
        strict += sum(state.scalar_count or 0 for state in scalars if state.failed)
        everything += sum(state.scalar_count or 0 for state in scalars)
        if total_rows is not None:
            strict, everything = min(strict, total_rows), min(everything, total_rows)

    failed_rows = strict if scope == "failed_checks" else everything
    tolerated_rows: int | None = max(everything - strict, 0)
    if any(not state.scalar and not state.failed and _may_have_rows(state) for state in not_attributed):
        # A tolerated check's rows are missing, so the tolerated rows are unknown.
        tolerated_rows = None
    passed_rows = max(total_rows - failed_rows, 0) if total_rows is not None else None
    pass_rate = passed_rows / total_rows if total_rows and passed_rows is not None else None
    return failed_rows, tolerated_rows, passed_rows, pass_rate, exact, len(counted), len(not_attributed)


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
