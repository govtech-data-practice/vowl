"""The report: schema, dimension, check and run rollups of the merged rows.

See "Step 5: Roll up" and "Step 6: Trust metadata" in
``design/row-quality-statistics.md``.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SchemaRowQuality:
    """Row-quality numbers for one schema.

    ``failed_rows``, ``passed_rows`` and ``pass_rate`` are None when no check
    in scope could be attributed. ``pass_rate`` is also None when the table is
    empty or its row count is unknown. ``checks_not_attributable`` counts the
    checks in scope whose rows are not in these numbers. A passed check that is
    not attributed is not one of them.
    """

    schema_name: str
    total_rows: int | None
    failed_rows: int | None
    passed_rows: int | None
    pass_rate: float | None
    approximate: bool
    checks_row_level: int
    checks_not_row_level: int
    checks_not_attributable: int


@dataclass(frozen=True)
class DimensionRowQuality:
    """Row-quality numbers for the checks of one dimension of one schema."""

    schema_name: str
    dimension: str
    total_rows: int | None
    failed_rows: int | None
    passed_rows: int | None
    pass_rate: float | None
    approximate: bool
    checks_row_level: int
    checks_not_row_level: int
    checks_not_attributable: int


@dataclass(frozen=True)
class CheckRowQuality:
    """How one check took part in the row-quality numbers.

    Attributes:
        route: ``"server_predicate"``, ``"server_lookup"``, ``"client_lookup"`` or
            ``"server_scalar"``, or empty when the check is not row-level or not
            attributed. ``"server_scalar"`` is used only under
            ``row_counts="scalar"``.
        reason: Why the check was not row-level or not attributed, or why it
            left pushdown.
        scalar_count: The number the check's own query returned, as a count.
            It decides pass or fail, and is what the summary shows as ``actual``.
        attributed_rows: The rows of the table the check caught, before the
            merge. It differs from ``scalar_count`` when the check's query does
            not return each failing row of the table once, for example under
            ``DISTINCT``. None when the check is not attributed, and
            ``reason`` then says why.
        approximate: True when this check's rows are incomplete or approximate.
    """

    schema_name: str
    check_name: str
    dimension: str
    status: str
    row_level: bool
    route: str
    reason: str
    scalar_count: int | None
    attributed_rows: int | None
    approximate: bool


@dataclass(frozen=True)
class RowQualityReport:
    """Every row-quality number of one validation run.

    Attributes:
        schemas: One entry per schema.
        dimensions: One entry per (schema, dimension) with at least one check.
        checks: One entry per check anchored to a schema.
        enabled: False under ``row_counts="off"``.
    """

    schemas: list[SchemaRowQuality] = field(default_factory=list)
    dimensions: list[DimensionRowQuality] = field(default_factory=list)
    checks: list[CheckRowQuality] = field(default_factory=list)
    enabled: bool = True

    def schema(self, schema_name: str) -> SchemaRowQuality | None:
        return next((item for item in self.schemas if item.schema_name == schema_name), None)


@dataclass
class CheckState:
    """The rollup's view of one check of a schema.

    Attributes:
        check_id: Schema-local id, the bit the check owns in the masks.
        dimension: The check's dimension.
        row_level: The check is about bad rows.
        in_scope: The check's rows belong in the row counts: it FAILED, or it
            PASSED under ``attribute_tolerated_rows``.
        collected: The check's rows are in the merged entries.
        attributable: The check's attributed rows are in the row counts.
        from_scalar: The check is counted from its scalar count, outside the
            merged entries. Only under ``row_counts="scalar"``.
        approximate: The check makes its buckets approximate.
        attributed_rows: The check's attributed rows, when known.
        scalar_count: The count the check's own query returned.
    """

    check_id: int
    dimension: str
    row_level: bool
    in_scope: bool
    collected: bool
    attributable: bool = True
    from_scalar: bool = False
    approximate: bool = False
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
    if state.from_scalar:
        return state.scalar_count
    return state.attributed_rows if state.attributable else None


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
    approximate: bool,
) -> tuple[int | None, int | None, float | None, bool, int, int]:
    """Roll up one bucket of checks.

    Only checks in scope count. A check out of scope adds nothing and never
    makes the bucket approximate or N/A. A check in scope that is not attributable
    adds nothing, unless it is counted from its scalar count. When it may have
    rows the bucket is approximate.

    Returns:
        ``(failed_rows, passed_rows, pass_rate, approximate, checks_row_level,
        checks_not_attributable)``.
    """
    row_level = [state for state in states if state.row_level]
    scoped = [state for state in row_level if state.in_scope]
    approximate = approximate or any(state.approximate for state in states)
    if not row_level:
        return None, None, None, approximate, 0, 0
    not_attributable = [state for state in scoped if not state.attributable]
    if any(not state.from_scalar and _may_have_rows(state) for state in not_attributable):
        # Rows in scope are missing from the numbers.
        approximate = True
    if scoped and not any(state.attributable or state.from_scalar for state in scoped):
        return None, None, None, approximate, len(row_level), len(not_attributable)

    scoped_mask = _mask(state for state in scoped if state.collected)
    failed_rows = sum(copies for mask, copies in entries if mask & scoped_mask)

    scalars = [state for state in scoped if state.from_scalar]
    if scalars:
        # Scalars cannot tell which rows overlap, so their sum is approximate
        # when another check of the bucket also has failing rows.
        with_rows = [state for state in scoped if (bucket_rows(state) or 0) > 0]
        if len(with_rows) > 1 and any(state.from_scalar for state in with_rows):
            approximate = True
        failed_rows += sum(state.scalar_count or 0 for state in scalars)
        if total_rows is not None:
            failed_rows = min(failed_rows, total_rows)

    passed_rows = max(total_rows - failed_rows, 0) if total_rows is not None else None
    pass_rate = passed_rows / total_rows if total_rows and passed_rows is not None else None
    return failed_rows, passed_rows, pass_rate, approximate, len(row_level), len(not_attributable)
