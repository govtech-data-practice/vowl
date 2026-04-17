"""Library-metric check references backed by generated SQL.

Each ODCS ``metric`` (``nullValues``, ``missingValues``, ``invalidValues``,
``duplicateValues``, ``rowCount``) is translated into a sqlglot AST and
executed through the standard SQL executor pipeline.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp

from .check_reference_generated import (
    GeneratedColumnCheckReference,
    GeneratedTableCheckReference,
)

if TYPE_CHECKING:
    from .contract import Contract
    from .models.ODCS_types import DataQuality


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _table_ref(schema_name: str) -> exp.Table:
    return exp.Table(this=exp.to_identifier(schema_name, quoted=True))


def _col_ref(col_name: str) -> exp.Column:
    return exp.Column(this=exp.to_identifier(col_name, quoted=True))


def _count_star() -> exp.Count:
    return exp.Count(this=exp.Star())


def _wrap_percent(core_ast: exp.Expression, table: exp.Table) -> exp.Expression:
    """Wrap a count query to return ``(count * 100.0) / NULLIF(total, 0)``."""
    total = sqlglot.select(_count_star()).from_(table)
    return sqlglot.select(
        exp.Div(
            this=exp.Mul(
                this=exp.Paren(this=core_ast.subquery("_cnt")),
                expression=exp.Literal.number(100.0),
            ),
            expression=exp.Anonymous(
                this="NULLIF",
                expressions=[total.subquery("_tot"), exp.Literal.number(0)],
            ),
        )
    )


# ---------------------------------------------------------------------------
# Base for library-metric column checks
# ---------------------------------------------------------------------------

class _LibraryColumnMetricBase(GeneratedColumnCheckReference):
    """Intermediate base for library metrics at property level.

    Unlike other generated checks that derive from a property attribute
    (e.g. ``logicalType``), library metrics live in ``quality[]`` entries
    so the path points at the quality entry itself.
    """

    def __init__(self, contract: Contract, quality_path: str, property_path: str):
        # Derive the path suffix so we can use the standard init chain.
        # quality_path is always property_path + ".quality[N]".
        path_suffix = quality_path[len(property_path) + 1:]
        super().__init__(contract, property_path, path_suffix)

    # The check dict is the *original* quality entry from the contract,
    # which already carries metric, operators, unit, etc.
    def get_check(self) -> DataQuality:
        return self._contract.resolve(self._path)

    def _generate_check(self) -> DataQuality:
        return self.get_check()

    def _is_percent(self) -> bool:
        check = self.get_check()
        return (check.get("unit") or "").lower() == "percent"

    def get_check_name(self) -> str:
        check = self.get_check()
        if check.get("name"):
            return check["name"]
        metric = check.get("metric", "library")
        col = self.get_column_name() or "unknown"
        return f"{col}_{metric}"

    def _auto_description(self) -> str:
        """Generate a description for library column metrics when not provided."""
        check = self.get_check()
        metric = check.get("metric", "library")
        col = self.get_column_name() or "unknown"
        schema = self.get_schema_name() or "unknown"
        descriptions = {
            "nullValues": f"Count of NULL values in '{col}' of '{schema}'",
            "missingValues": f"Count of missing values in '{col}' of '{schema}'",
            "invalidValues": f"Count of invalid values in '{col}' of '{schema}'",
            "duplicateValues": f"Count of duplicate values in '{col}' of '{schema}'",
        }
        return descriptions.get(metric, f"Library metric '{metric}' on column '{col}' of '{schema}'")

    def get_result_metadata(self):
        metadata = super().get_result_metadata()
        cd = metadata.get("contract_definition", {})
        if not cd.get("description"):
            cd["description"] = self._auto_description()
            metadata["contract_definition"] = cd
        return metadata


# ---------------------------------------------------------------------------
# Property-level metrics
# ---------------------------------------------------------------------------

class NullValuesCheckReference(_LibraryColumnMetricBase):
    """``nullValues``: count of NULL values in a column."""

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        schema_name = self.get_schema_name()
        if not col_name or not schema_name:
            raise ValueError(f"Cannot generate nullValues check for {self._path}")

        col = _col_ref(col_name)
        table = _table_ref(schema_name)
        core = sqlglot.select(_count_star()).from_(table).where(col.is_(exp.Null()))

        self._cached_ast = _wrap_percent(core, table) if self._is_percent() else core
        return self._cached_ast


class MissingValuesCheckReference(_LibraryColumnMetricBase):
    """``missingValues``: count of values considered missing.

    Expects ``arguments.missingValues`` as a list of values to treat as
    missing.  ``null`` entries in that list match SQL NULL.  If the
    argument is absent, defaults to counting NULLs only.
    """

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        schema_name = self.get_schema_name()
        if not col_name or not schema_name:
            raise ValueError(f"Cannot generate missingValues check for {self._path}")

        check = self.get_check()
        args = check.get("arguments") or {}
        missing_list: list = args.get("missingValues", [None])

        col = _col_ref(col_name)
        table = _table_ref(schema_name)

        # Build OR conditions for each missing value sentinel
        conditions: list[exp.Expression] = []
        non_null_values: list = []
        for val in missing_list:
            if val is None:
                conditions.append(col.is_(exp.Null()))
            else:
                non_null_values.append(val)

        if non_null_values:
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("VARCHAR"), safe=True)
            in_vals = [exp.Literal.string(str(v)) for v in non_null_values]
            conditions.append(exp.In(this=cast_col, expressions=in_vals))

        if not conditions:
            conditions.append(col.is_(exp.Null()))

        combined = conditions[0]
        for cond in conditions[1:]:
            combined = exp.Or(this=combined, expression=cond)

        core = sqlglot.select(_count_star()).from_(table).where(combined)

        self._cached_ast = _wrap_percent(core, table) if self._is_percent() else core
        return self._cached_ast


class InvalidValuesCheckReference(_LibraryColumnMetricBase):
    """``invalidValues``: count of values that don't match valid criteria.

    Supports two argument modes (can be combined with OR):
    - ``arguments.validValues``: list of acceptable values
    - ``arguments.pattern``: regex pattern values must match
    """

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        schema_name = self.get_schema_name()
        if not col_name or not schema_name:
            raise ValueError(f"Cannot generate invalidValues check for {self._path}")

        check = self.get_check()
        args = check.get("arguments") or {}
        valid_values: list | None = args.get("validValues")
        pattern: str | None = args.get("pattern")

        if not valid_values and not pattern:
            raise ValueError(
                f"invalidValues metric at {self._path} requires "
                "'arguments.validValues' and/or 'arguments.pattern'"
            )

        col = _col_ref(col_name)
        table = _table_ref(schema_name)
        not_null = col.is_(exp.Null()).not_()

        # Build invalid conditions. A value is invalid if it fails ALL criteria
        invalid_conditions: list[exp.Expression] = []

        if valid_values is not None:
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("VARCHAR"), safe=True)
            in_vals = [exp.Literal.string(str(v)) for v in valid_values]
            not_in_valid = exp.Not(this=exp.In(this=cast_col, expressions=in_vals))
            invalid_conditions.append(not_in_valid)

        if pattern is not None:
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("VARCHAR"), safe=True)
            not_matching = exp.Not(
                this=exp.RegexpLike(this=cast_col, expression=exp.Literal.string(pattern))
            )
            invalid_conditions.append(not_matching)

        # If both are present, a value is invalid if it fails BOTH criteria
        # (i.e. not in validValues AND doesn't match pattern)
        combined = invalid_conditions[0]
        for cond in invalid_conditions[1:]:
            combined = exp.And(this=combined, expression=cond)

        core = sqlglot.select(_count_star()).from_(table).where(not_null).where(combined)

        self._cached_ast = _wrap_percent(core, table) if self._is_percent() else core
        return self._cached_ast


class DuplicateValuesColumnCheckReference(_LibraryColumnMetricBase):
    """``duplicateValues`` at property level: count of duplicate values."""

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        schema_name = self.get_schema_name()
        if not col_name or not schema_name:
            raise ValueError(f"Cannot generate duplicateValues check for {self._path}")

        col = _col_ref(col_name)
        table = _table_ref(schema_name)

        dup_subquery = (
            sqlglot.select(col)
            .from_(table)
            .where(col.is_(exp.Null()).not_())
            .group_by(col)
            .having(_count_star() > exp.Literal.number(1))
        )

        core = sqlglot.select(_count_star()).from_(dup_subquery.subquery("_dup"))

        self._cached_ast = _wrap_percent(core, table) if self._is_percent() else core
        return self._cached_ast


# ---------------------------------------------------------------------------
# Schema-level metrics
# ---------------------------------------------------------------------------

class _LibraryTableMetricBase(GeneratedTableCheckReference):
    """Intermediate base for library metrics at schema level."""

    def get_check_name(self) -> str:
        check = self.get_check()
        if check.get("name"):
            return check["name"]
        metric = check.get("metric", "library")
        schema = self.get_schema_name() or "unknown"
        return f"{schema}_{metric}"

    def _auto_description(self) -> str:
        """Generate a description for library table metrics when not provided."""
        check = self.get_check()
        metric = check.get("metric", "library")
        schema = self.get_schema_name() or "unknown"
        descriptions = {
            "rowCount": f"Total row count for '{schema}'",
            "duplicateValues": f"Count of duplicate rows in '{schema}'",
        }
        return descriptions.get(metric, f"Library metric '{metric}' on table '{schema}'")

    def get_result_metadata(self):
        metadata = super().get_result_metadata()
        cd = metadata.get("contract_definition", {})
        if not cd.get("description"):
            cd["description"] = self._auto_description()
            metadata["contract_definition"] = cd
        return metadata


class RowCountCheckReference(_LibraryTableMetricBase):
    """``rowCount``: total number of rows in a table."""

    def get_check(self) -> DataQuality:
        return self._contract.resolve(self._path)

    def _generate_check(self) -> DataQuality:
        return self.get_check()

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        schema_name = self.get_schema_name()
        if not schema_name:
            raise ValueError(f"Cannot generate rowCount check for {self._path}")

        table = _table_ref(schema_name)
        self._cached_ast = sqlglot.select(_count_star()).from_(table)
        return self._cached_ast


class DuplicateValuesTableCheckReference(_LibraryTableMetricBase):
    """``duplicateValues`` at schema level: duplicates across multiple columns.

    Expects ``arguments.properties`` listing the column names to check.
    """

    def get_check(self) -> DataQuality:
        return self._contract.resolve(self._path)

    def _generate_check(self) -> DataQuality:
        return self.get_check()

    def _is_percent(self) -> bool:
        check = self.get_check()
        return (check.get("unit") or "").lower() == "percent"

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        schema_name = self.get_schema_name()
        if not schema_name:
            raise ValueError(f"Cannot generate duplicateValues check for {self._path}")

        check = self.get_check()
        args = check.get("arguments") or {}
        prop_names: list[str] | None = args.get("properties")
        if not prop_names:
            raise ValueError(
                f"Schema-level duplicateValues metric at {self._path} requires "
                "'arguments.properties' listing the columns to check for duplicates"
            )

        table = _table_ref(schema_name)
        cols = [_col_ref(name) for name in prop_names]

        dup_subquery = sqlglot.select(*cols).from_(table)
        for col in cols:
            dup_subquery = dup_subquery.where(col.is_(exp.Null()).not_())
        dup_subquery = dup_subquery.group_by(*cols).having(
            _count_star() > exp.Literal.number(1)
        )

        core = sqlglot.select(_count_star()).from_(dup_subquery.subquery("_dup"))

        self._cached_ast = _wrap_percent(core, table) if self._is_percent() else core
        return self._cached_ast


# ---------------------------------------------------------------------------
# Dispatch maps (used by contract.py factory)
# ---------------------------------------------------------------------------

LIBRARY_COLUMN_METRICS = {
    "nullValues": NullValuesCheckReference,
    "missingValues": MissingValuesCheckReference,
    "invalidValues": InvalidValuesCheckReference,
    "duplicateValues": DuplicateValuesColumnCheckReference,
}

LIBRARY_TABLE_METRICS = {
    "rowCount": RowCountCheckReference,
    "duplicateValues": DuplicateValuesTableCheckReference,
}


__all__ = [
    "DuplicateValuesColumnCheckReference",
    "DuplicateValuesTableCheckReference",
    "InvalidValuesCheckReference",
    "LIBRARY_COLUMN_METRICS",
    "LIBRARY_TABLE_METRICS",
    "MissingValuesCheckReference",
    "NullValuesCheckReference",
    "RowCountCheckReference",
]
