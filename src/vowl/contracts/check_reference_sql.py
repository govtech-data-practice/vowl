"""SQL-backed check reference implementations.

SQL AST manipulation utilities (dialect transforms, filter application,
TRY_CAST rewriting, etc.) live in :mod:`vowl.contracts.sql_transforms`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from functools import cached_property
from typing import TYPE_CHECKING, Any

import sqlglot
from sqlglot import exp

from . import sql_transforms as _sql
from .check_reference_base import CheckReference, CheckResultMetadata, ColumnCheckMixin, TableCheckMixin

if TYPE_CHECKING:
    import narwhals as nw

    from vowl.adapters.models import FilterCondition
    from vowl.executors.base import CheckResult

    FilterConditionType = FilterCondition | list[FilterCondition] | dict[str, Any]

# Re-export so existing ``from .check_reference_sql import LOGICAL_TYPE_TO_SQL``
# continues to work.
LOGICAL_TYPE_TO_SQL = _sql.LOGICAL_TYPE_TO_SQL


def _count_skips_nulls(argument: exp.Expression | None) -> bool:
    """Whether ``COUNT(argument)`` skips the rows where the argument is NULL."""
    return argument is not None and not isinstance(argument, (exp.Star, exp.Literal, exp.Distinct))


def _null_skipping_arguments(argument: exp.Expression | None) -> list[exp.Expression]:
    """The expressions whose NULL rows ``COUNT(argument)`` skips.

    ``COUNT(DISTINCT a, b)`` skips a row when any of its arguments is NULL.
    """
    if isinstance(argument, exp.Distinct):
        return [e for e in argument.expressions if _count_skips_nulls(e)]
    return [argument] if argument is not None and _count_skips_nulls(argument) else []


def _counts_whole_values(argument: exp.Expression) -> bool:
    """Whether ``COUNT(DISTINCT ...)`` over *argument* skips exactly the rows where it is NULL.

    A row value such as ``(a, b)`` or ``ROW(a, b)`` is not NULL when only some
    of its fields are, and databases treat it differently, so it is left out.
    """
    if isinstance(argument, (exp.Tuple, exp.Struct, exp.Star)):
        return False
    return not (isinstance(argument, exp.Anonymous) and argument.name.upper() == "ROW")


class SQLCheckReference(CheckReference, ABC):
    """
    Abstract base for all SQL-based check references.

    Encapsulates SQL query generation, dialect transpilation, and filter
    application. All SQL check references inherit from this class.

    Future non-SQL engines (GX, DQX) should branch from CheckReference directly.
    """

    _INTERNAL_DIALECT: str = "postgres"

    def get_execution_engine(self) -> str:
        return "sql"

    @abstractmethod
    def get_query(
        self,
        dialect: str,
        filter_conditions: dict[str, FilterConditionType] | None = None,
        use_try_cast: bool = False,
    ) -> str:
        """
        Return the SQL query string rendered in ``dialect``, with optional
        filter conditions applied.
        """
        ...

    @cached_property
    def aggregation_type(self) -> str:
        """Normalized aggregation type detected from the canonical query."""
        query = self.get_query(self._INTERNAL_DIALECT, None, use_try_cast=False)
        if not query:
            return "custom"
        return self.detect_aggregation_type(query, self._INTERNAL_DIALECT)

    @cached_property
    def _counts_rows(self) -> bool:
        """Whether the check's query counts rows, so its failed rows are the counted rows.

        ``COUNT(DISTINCT x)`` counts values, but its failed rows query returns
        the rows holding them, with ``x IS NOT NULL``, so it qualifies too.
        """
        if self.aggregation_type in ("count", "none"):
            return True
        if self.aggregation_type != "count_distinct":
            return False
        try:
            parsed = sqlglot.parse_one(
                self.get_query(self._INTERNAL_DIALECT, None, use_try_cast=False), dialect=self._INTERNAL_DIALECT
            )
            count = parsed.find(exp.Count)
        except Exception:
            return False
        distinct = count.this if count is not None else None
        return isinstance(distinct, exp.Distinct) and all(_counts_whole_values(e) for e in distinct.expressions)

    @property
    def supports_row_level_output(self) -> bool:
        """Whether the check's failed rows are rows of the table it counted."""
        if self.unit is not None and self.unit != "rows":
            return False
        return self._counts_rows

    def get_result_metadata(self) -> CheckResultMetadata:
        """Extend base metadata with SQL-specific aggregation metadata."""
        metadata = super().get_result_metadata()
        metadata["aggregation_type"] = self.aggregation_type
        return metadata

    def get_failed_rows_query(
        self,
        dialect: str,
        filter_conditions: dict[str, FilterConditionType] | None = None,
        use_try_cast: bool = False,
    ) -> str | None:
        """Return a SELECT query for fetching the rows that failed a check."""
        query = self.get_query(dialect, filter_conditions, use_try_cast=use_try_cast)
        if not query:
            return None
        try:
            parsed = sqlglot.parse_one(query, dialect=dialect)
            if not isinstance(parsed, exp.Select):
                return None
            counts = [e.unalias() for e in parsed.expressions if isinstance(e.unalias(), exp.Count)]
            has_any_agg = any(
                isinstance(node, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max))
                for sel_expr in parsed.expressions
                for node in sel_expr.walk()
            )
            if counts:
                result = parsed.copy()
                result.set("expressions", [exp.Star()])
                # COUNT(expr) and COUNT(DISTINCT expr) skip rows where expr is
                # NULL, so the failed rows skip them too.
                if len(counts) == 1:
                    for argument in _null_skipping_arguments(counts[0].this):
                        result = result.where(exp.Not(this=exp.Is(this=argument.copy(), expression=exp.Null())))
                return result.sql(dialect=dialect)
            if not has_any_agg and parsed.find(exp.From):
                return query
            return None
        except Exception:
            return None

    def get_scalar_query(
        self,
        dialect: str,
        filter_conditions: dict[str, FilterConditionType] | None = None,
        use_try_cast: bool = False,
    ) -> str | None:
        """Return the query that produces the scalar value for comparison."""
        query = self.get_query(dialect, filter_conditions, use_try_cast=use_try_cast)
        if not query:
            return None
        if self.aggregation_type == "none":
            return _sql.wrap_count_subquery(query, dialect)
        return query

    def compute_failed_rows_count(self, actual_value: Any) -> int:
        """Derive failed_rows_count from a check's scalar result.

        For ``COUNT(DISTINCT ...)`` this is the number of distinct values, a
        lower bound on the rows that hold them.
        """
        if self.supports_row_level_output:
            try:
                return int(actual_value)
            except (TypeError, ValueError):
                return 0
        return 0

    def _build_full_metadata(
        self,
        dialect: str,
        filter_conditions: dict[str, FilterConditionType] | None = None,
        use_try_cast: bool = False,
        **extra: Any,
    ) -> dict[str, Any]:
        """Assemble complete metadata including SQL runtime fields."""
        metadata = dict(self.get_result_metadata())
        logical_query = self.get_query(dialect, filter_conditions, use_try_cast=use_try_cast)
        if logical_query:
            metadata["tables_in_query"] = self.extract_table_names(logical_query, dialect)
            metadata["rendered_implementation"] = logical_query
        metadata.update(extra)
        return metadata

    def build_result(
        self,
        *,
        actual_value: Any,
        execution_time_ms: float,
        failed_rows_fetcher: Callable[[], nw.DataFrame | None] | None = None,
        dialect: str = "",
        filter_conditions: dict[str, FilterConditionType] | None = None,
        use_try_cast: bool = False,
    ) -> CheckResult:
        """Build a PASSED or FAILED result with SQL-specific fields."""
        from vowl.executors.base import CheckResult

        check = self.get_check()
        check_name = self.get_check_name()
        operator, expected_value = self.get_expected_value()
        passed = self.evaluate(actual_value, operator, expected_value)
        metadata = self._build_full_metadata(dialect, filter_conditions, use_try_cast)

        if passed:
            return CheckResult(
                check_name=check_name,
                status="PASSED",
                details=check.get("description") or f"Check passed: {operator} {expected_value}",
                actual_value=actual_value,
                expected_value=expected_value,
                supports_row_level_output=self.supports_row_level_output,
                metadata=metadata,
                execution_time_ms=execution_time_ms,
            )
        return CheckResult(
            check_name=check_name,
            status="FAILED",
            details=check.get("description")
            or f"Check failed: expected {operator} {expected_value}, got {actual_value}",
            actual_value=actual_value,
            expected_value=expected_value,
            failed_rows_fetcher=failed_rows_fetcher,
            failed_rows_count=self.compute_failed_rows_count(actual_value),
            supports_row_level_output=self.supports_row_level_output,
            metadata=metadata,
            execution_time_ms=execution_time_ms,
        )

    def build_error_result(
        self,
        *,
        error_message: str,
        execution_time_ms: float,
        dialect: str = "",
        filter_conditions: dict[str, FilterConditionType] | None = None,
        use_try_cast: bool = False,
        **extra_metadata: Any,
    ) -> CheckResult:
        """Build an ERROR result with SQL metadata."""
        from vowl.executors.base import CheckResult

        metadata = self._build_full_metadata(
            dialect,
            filter_conditions,
            use_try_cast,
            **extra_metadata,
        )
        return CheckResult(
            check_name=self.get_check_name(),
            status="ERROR",
            details=error_message,
            metadata=metadata,
            execution_time_ms=execution_time_ms,
        )

    # ------------------------------------------------------------------
    # Delegate SQL utility methods to sql_transforms module.
    # Kept as static/class methods so existing callers
    # (tests, generated checks, adapters) work unchanged.
    # ------------------------------------------------------------------

    detect_aggregation_type = staticmethod(_sql.detect_aggregation_type)
    extract_table_names = staticmethod(_sql.extract_table_names)
    apply_filters = staticmethod(_sql.apply_filters)
    apply_try_cast = staticmethod(_sql.apply_try_cast)
    transpile = staticmethod(_sql.transpile)
    _render_sql = staticmethod(_sql.render_sql)
    _apply_dialect_transforms = staticmethod(_sql.apply_dialect_transforms)
    _make_safe_cast = staticmethod(_sql.make_safe_cast)
    _infer_type_from_literal = staticmethod(_sql.infer_type_from_literal)

    # Expose Oracle constants for any code that referenced them on the class.
    _ORACLE_STRING_TYPES = _sql._ORACLE_STRING_TYPES
    _ORACLE_SIZEABLE_TYPES = _sql._ORACLE_SIZEABLE_TYPES


class SQLTableCheckReference(TableCheckMixin, SQLCheckReference):
    """Reference to a table-level SQL quality check."""

    def get_query(
        self,
        dialect: str,
        filter_conditions: dict[str, FilterConditionType] | None = None,
        use_try_cast: bool = False,
    ) -> str:
        check = self.get_check()
        query = check.get("query") or ""
        query = self.transpile(query, self._INTERNAL_DIALECT, dialect)
        if filter_conditions:
            query = self.apply_filters(query, dialect, filter_conditions)
        if use_try_cast:
            query, _ = self.apply_try_cast(query, dialect)
        return query


class SQLColumnCheckReference(ColumnCheckMixin, SQLCheckReference):
    """Reference to a column-level SQL quality check."""

    def get_query(
        self,
        dialect: str,
        filter_conditions: dict[str, FilterConditionType] | None = None,
        use_try_cast: bool = False,
    ) -> str:
        check = self.get_check()
        query = check.get("query") or ""
        query = self.transpile(query, self._INTERNAL_DIALECT, dialect)
        if filter_conditions:
            query = self.apply_filters(query, dialect, filter_conditions)
        if use_try_cast:
            query, _ = self.apply_try_cast(query, dialect)
        return query


__all__ = [
    "LOGICAL_TYPE_TO_SQL",
    "SQLCheckReference",
    "SQLColumnCheckReference",
    "SQLTableCheckReference",
]
