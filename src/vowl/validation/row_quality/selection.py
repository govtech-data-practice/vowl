"""Which checks count toward the row-quality statistics, and why the others don't.

This is step 2 of the process in ``design/row-quality-statistics.md``. It only
reads the check results, so annotated output can use the same rules without
running the row-quality queries.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from ...config import RowIssueScope
from ...executors.base import CheckResult

# The short, fixed vocabulary of the ``reason`` column of
# ``get_row_quality_df(by="check")``.
REASON_OPERATOR = "operator does not identify bad rows"
REASON_NOT_ROW_LEVEL = "table-level or not a row filter"
REASON_ERROR = "check ended in ERROR"
REASON_PROBE_FAILURE = "the counting query failed in the data source"
REASON_NOT_MERGEABLE = "failed rows do not have the table's columns or primary key"
REASON_PK_NOT_UNIQUE = "primary key has duplicate values"
REASON_PK_UNCHECKED = "primary key uniqueness could not be checked"
REASON_TRUNCATED = "truncated by max_failed_rows"
REASON_CROSS_SOURCE = "checks tables from more than one data source"
REASON_NO_PUSHDOWN = "data source does not support pushdown"
REASON_TOLERATED_NOT_FETCHED = "tolerated rows not fetched under failed_checks"
REASON_NO_EXPORT = "the table could not be exported"
REASON_NO_FETCH = "the failed rows could not be fetched"
REASON_UNKEYABLE = "the failed rows could not be keyed"
REASON_UNMATCHED = "some failed rows match no table row"
REASON_COUNTS_VALUES = "the check counts distinct values, not rows"


def uncertified_reason(rule: str) -> str:
    """Reason for a counted check that left pushdown because of *rule*."""
    return f"not certified for pushdown: uses {rule}"


def resolve_check_dimension(check_result: CheckResult) -> str:
    """Resolve a check's DQ dimension, defaulting to ``"unknown"``.

    The dimension is read exactly as the check reports it: authored ``quality``
    rules carry the author-asserted dimension, and auto-generated checks
    (``required`` / ``unique`` / type / column-existence) carry the dimension
    vowl's generator assigns them (e.g. ``completeness`` for ``required``).
    ``"unknown"`` is only used when a check genuinely reports no dimension.
    """
    metadata = check_result.metadata
    definition = metadata.get("check_definition") or {}
    return metadata.get("dimension") or definition.get("dimension") or "unknown"


def _is_zero(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return float(value) == 0.0
    except (TypeError, ValueError):
        return False


def identifies_bad_rows(operator: str | None, expected_value: Any) -> bool:
    """True when a row-count check's matched rows are the bad rows.

    Only an upper bound on the count says that each matched row is at fault:
    ``mustBeLessThan``, ``mustBeLessOrEqualTo``, ``mustBe 0`` and
    ``mustBeBetween [0, n]``. Every other operator either targets an exact or
    two-sided count, or is inverted so the matched rows are the good ones.
    A result with no recorded operator (built before operators were recorded)
    is treated as a plain upper bound.
    """
    if operator is None:
        return True
    if operator in ("mustBeLessThan", "mustBeLessOrEqualTo"):
        return True
    if operator == "mustBe":
        return _is_zero(expected_value)
    if operator == "mustBeBetween":
        return isinstance(expected_value, (list, tuple)) and len(expected_value) == 2 and _is_zero(expected_value[0])
    return False


def _expected_value(check_result: CheckResult, operator: str | None) -> Any:
    if check_result.expected_value is not None or operator is None:
        return check_result.expected_value
    definition = check_result.metadata.get("check_definition") or {}
    return definition.get(operator)


def _would_be_row_level(check_result: CheckResult) -> bool:
    """Guess from its metadata whether an ERROR check would have been row-level."""
    metadata = check_result.metadata
    aggregation_type = metadata.get("aggregation_type")
    if aggregation_type is not None and aggregation_type not in ("count", "count_distinct", "none"):
        return False
    unit = (metadata.get("check_definition") or {}).get("unit")
    return unit is None or unit == "rows"


def scalar_row_count(check_result: CheckResult) -> int | None:
    """The number of rows a row-level check matched, from its scalar result.

    FAILED results carry it as ``failed_rows_count``. PASSED results do not, so
    it is derived from the scalar the same way the check reference does.
    """
    if check_result.status == "FAILED":
        return check_result.failed_rows_count
    row_source = getattr(check_result, "row_source", None)
    check_ref = getattr(row_source, "check_ref", None)
    if check_ref is not None and hasattr(check_ref, "compute_failed_rows_count"):
        return check_ref.compute_failed_rows_count(check_result.actual_value)
    try:
        return int(check_result.actual_value)
    except (TypeError, ValueError):
        return None


@dataclass
class CheckSelection:
    """The step 2 verdict for one check.

    Attributes:
        result: The check result.
        schema_name: The schema the check is anchored to.
        dimension: The check's resolved dimension.
        counted: Whether the check can contribute rows. It stays counted, and
            may be not attributed, either on this run or because it is never
            attributable, in which case its failed rows become residues.
        tolerated: The check PASSED but matched rows (a tolerance). Such a
            check feeds ``tolerated_rows``, and under ``all_violations`` it also
            feeds ``failed_rows``.
        in_scope: Whether the check's rows count toward the headline
            ``failed_rows`` under the configured ``row_issue_scope``.
        reason: Why the check is not counted, or empty.
        row_count: The number of rows the check's scalar query matched.
        blocks_exact: The check ended in ERROR but would have been counted, so
            the numbers of its schema and dimension are not exact.
    """

    result: CheckResult
    schema_name: str
    dimension: str
    counted: bool
    tolerated: bool
    in_scope: bool
    reason: str
    row_count: int | None
    blocks_exact: bool = False


def select_check(check_result: CheckResult, scope: RowIssueScope) -> CheckSelection | None:
    """Apply the step 2 rules to one check, or return None when it has no schema."""
    schema_name = check_result.metadata.get("schema_name")
    if not isinstance(schema_name, str):
        return None

    dimension = resolve_check_dimension(check_result)
    operator = check_result.metadata.get("operator")
    bounds_rows = identifies_bad_rows(operator, _expected_value(check_result, operator))

    def not_counted(reason: str, *, blocks_exact: bool = False) -> CheckSelection:
        return CheckSelection(
            result=check_result,
            schema_name=schema_name,
            dimension=dimension,
            counted=False,
            tolerated=False,
            in_scope=False,
            reason=reason,
            row_count=None,
            blocks_exact=blocks_exact,
        )

    if check_result.status == "ERROR":
        return not_counted(REASON_ERROR, blocks_exact=bounds_rows and _would_be_row_level(check_result))
    if check_result.status not in ("PASSED", "FAILED"):
        return not_counted(REASON_ERROR)
    if not check_result.supports_row_level_output:
        return not_counted(REASON_NOT_ROW_LEVEL)
    if not bounds_rows:
        return not_counted(REASON_OPERATOR)

    row_count = scalar_row_count(check_result)
    tolerated = check_result.status == "PASSED" and bool(row_count)
    return CheckSelection(
        result=check_result,
        schema_name=schema_name,
        dimension=dimension,
        counted=True,
        tolerated=tolerated,
        in_scope=check_result.status == "FAILED" or (tolerated and scope == "all_violations"),
        reason="",
        row_count=row_count,
    )


def select(check_results: Iterable[CheckResult], scope: RowIssueScope) -> list[CheckSelection]:
    """Apply the step 2 rules to every check that is anchored to a schema."""
    selections = (select_check(check_result, scope) for check_result in check_results)
    return [selection for selection in selections if selection is not None]


def flagged_checks(selections: Sequence[CheckSelection]) -> list[CheckSelection]:
    """The checks whose rows annotated output flags under the configured scope."""
    return [selection for selection in selections if selection.counted and selection.in_scope]
