"""Which checks count toward the row-quality statistics, and why the others don't.

This is step 2 of the process in ``design/row-quality-statistics.md``. It only
reads the check results, so annotated output can use the same rules without
running the row-quality queries.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from ...executors.base import CheckResult

# The short, fixed vocabulary of the ``reason`` column of
# ``get_dq_metrics_df(by="check")``.
REASON_OPERATOR = "operator does not set an upper limit"
REASON_NOT_ROW_LEVEL = "not a row-level check"
REASON_ERROR = "check ended in ERROR"
REASON_QUERY_FAILED = "its row query failed in the data source"
REASON_NO_MATCH_KEY = "failed rows do not have the table's columns or primary key"
REASON_PK_NOT_UNIQUE = "primary key has duplicate values"
REASON_PK_UNCHECKED = "primary key uniqueness could not be checked"
REASON_TRUNCATED = "truncated by max_failed_rows"
REASON_CROSS_SOURCE = "reads tables from more than one data source"
REASON_NO_PUSHDOWN = "the data source can't count rows for this check"
REASON_NO_EXPORT = "the table could not be downloaded"
REASON_NO_FETCH = "the failed rows could not be fetched"
REASON_MATCH_KEYS_FAILED = "the table's rows could not be turned into match keys"
REASON_UNATTRIBUTED = "some failed rows could not be attributed to a table row"
REASON_COUNTS_VALUES = "the check counts distinct values, not rows"
REASON_PASSED_NOT_ATTRIBUTED = "passed, not attributed"


def uncertified_reason(rule: str) -> str:
    """Reason for a row-level check that is not a plain filter because of *rule*."""
    return f"not a plain filter: uses {rule}"


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
        row_level: Whether the check can contribute rows. It stays row-level, and
            may be not attributable, either on this run or because it is never
            attributable, in which case its failed rows become residues.
        tolerated: The check PASSED but may have matched rows (a tolerance).
            Annotated output marks its ``check_info`` items with it.
        in_scope: Whether the check's rows are attributed: it FAILED, or it
            PASSED under ``fetch_tolerated_rows``.
        reason: Why the check is not row-level or not attributed, or empty.
        scalar_count: The number of rows the check's scalar query matched.
        approximate: The check ended in ERROR but would have been row-level, so
            the numbers of its schema and dimension are approximate.
    """

    result: CheckResult
    schema_name: str
    dimension: str
    row_level: bool
    tolerated: bool
    in_scope: bool
    reason: str
    scalar_count: int | None
    approximate: bool = False


def select_check(check_result: CheckResult, fetch_tolerated_rows: bool = False) -> CheckSelection | None:
    """Apply the step 2 rules to one check, or return None when it has no schema."""
    schema_name = check_result.metadata.get("schema_name")
    if not isinstance(schema_name, str):
        return None

    dimension = resolve_check_dimension(check_result)
    operator = check_result.metadata.get("operator")
    bounds_rows = identifies_bad_rows(operator, _expected_value(check_result, operator))

    def not_row_level(reason: str, *, approximate: bool = False) -> CheckSelection:
        return CheckSelection(
            result=check_result,
            schema_name=schema_name,
            dimension=dimension,
            row_level=False,
            tolerated=False,
            in_scope=False,
            reason=reason,
            scalar_count=None,
            approximate=approximate,
        )

    if check_result.status == "ERROR":
        return not_row_level(REASON_ERROR, approximate=bounds_rows and _would_be_row_level(check_result))
    if check_result.status not in ("PASSED", "FAILED"):
        return not_row_level(REASON_ERROR)
    if not check_result.supports_row_level_output:
        return not_row_level(REASON_NOT_ROW_LEVEL)
    if not bounds_rows:
        return not_row_level(REASON_OPERATOR)

    scalar_count = scalar_row_count(check_result)
    passed = check_result.status == "PASSED"
    in_scope = not passed or fetch_tolerated_rows
    return CheckSelection(
        result=check_result,
        schema_name=schema_name,
        dimension=dimension,
        row_level=True,
        tolerated=passed and scalar_count != 0,
        in_scope=in_scope,
        reason="" if in_scope else REASON_PASSED_NOT_ATTRIBUTED,
        scalar_count=scalar_count,
    )


def select(check_results: Iterable[CheckResult], fetch_tolerated_rows: bool = False) -> list[CheckSelection]:
    """Apply the step 2 rules to every check that is anchored to a schema."""
    selections = (select_check(check_result, fetch_tolerated_rows) for check_result in check_results)
    return [selection for selection in selections if selection is not None]


def attributed_checks(selections: Sequence[CheckSelection]) -> list[CheckSelection]:
    """The checks whose rows are attributed, and so flagged in annotated output."""
    return [selection for selection in selections if selection.row_level and selection.in_scope]
