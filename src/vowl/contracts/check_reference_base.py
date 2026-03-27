"""Base types for contract check references."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    import narwhals as nw

    from vowl.executors.base import CheckResult

    from .contract import Contract
    from .models.ODCS_types import DataQuality


class CheckResultMetadata(TypedDict, total=False):
    """Stable metadata derived from a check reference and its contract."""

    check_path: str
    check_ref_type: str
    dimension: str | None
    type: str | None
    description: str | None
    severity: str | None
    schema: str | None
    target: str
    logical_type: str
    is_generated: bool
    tables_in_query: list[str]
    rule: str
    security_violation: str
    operator: str
    multi_source: bool
    aggregation_type: str
    unit: str
    engine: str


class CheckReference(ABC):
    """
    Abstract base for all check references.

    A CheckReference maintains a JSONPath to a quality check (or property for
    type checks) and provides navigation methods to access related contract
    elements like schema name, column info, etc.
    """

    def __init__(self, contract: Contract, path: str):
        """
        Initialize a check reference.

        Args:
            contract: The Contract instance containing the check.
            path: JSONPath string pointing to the check or property.
        """
        self._contract = contract
        self._path = path

    @property
    def supports_row_level_output(self) -> bool:
        """Whether this check reference can provide meaningful row-level output."""
        return False

    def get_execution_engine(self) -> str:
        """Return the execution engine name for this check.

        Used by adapters to dispatch to the correct executor.
        Subclasses override to return the appropriate engine
        (e.g. ``"sql"``, ``"dbt"``).
        """
        return "unknown"

    @property
    def path(self) -> str:
        """Get the JSONPath to this check."""
        return self._path

    @property
    def contract(self) -> Contract:
        """Get the parent Contract."""
        return self._contract

    def get_check(self) -> DataQuality:
        """
        Get the check data dict.

        Returns:
            The DataQuality dict for this check.
        """
        return self._contract.resolve(self._path)

    @property
    def unit(self) -> str | None:
        """Optional ODCS unit declared for the check."""
        unit = self.get_check().get("unit")
        return unit if isinstance(unit, str) else None

    def get_result_metadata(self) -> CheckResultMetadata:
        """Build stable CheckResult metadata from the contract context."""
        check = self.get_check()
        dimension = check.get("dimension")
        schema_name = self.get_schema_name()
        operator, _expected = self.get_expected_value()
        metadata: CheckResultMetadata = {
            "check_path": self.path,
            "check_ref_type": type(self).__name__,
            "dimension": dimension.value if hasattr(dimension, "value") else dimension,
            "type": check.get("type"),
            "description": check.get("description"),
            "severity": check.get("severity"),
            "schema": schema_name,
            "operator": operator,
            "is_generated": self.is_generated(),
            "engine": self.get_execution_engine(),
        }

        column_name = self.get_column_name()
        if column_name:
            metadata["target"] = f"{schema_name}.{column_name}" if schema_name else column_name
        elif schema_name:
            metadata["target"] = schema_name

        logical_type = self.get_logical_type()
        if logical_type:
            metadata["logical_type"] = logical_type

        if self.unit:
            metadata["unit"] = self.unit

        return metadata

    @abstractmethod
    def get_schema_name(self) -> str | None:
        """Get the schema name this check belongs to."""
        ...

    @abstractmethod
    def get_schema_path(self) -> str:
        """Get the JSONPath to the parent schema."""
        ...

    def get_logical_type(self) -> str | None:
        """
        Get the logical type for the column this check applies to.

        Returns:
            The logicalType string, or None if not applicable (table-level checks).
        """
        return None

    def get_logical_type_options(self) -> dict[str, Any] | None:
        """
        Get the logical type options for the column this check applies to.

        Returns:
            The logicalTypeOptions dict, or None if not applicable.
        """
        return None

    def get_column_name(self) -> str | None:
        """
        Get the column name this check applies to.

        Returns:
            The column name, or None if this is a table-level check.
        """
        return None

    def is_generated(self) -> bool:
        """
        Check if this is an auto-generated check (e.g., type check).

        Returns:
                True if generated, False if defined in contract.
        """
        return False

    def get_check_name(self) -> str:
        """Return the check name from the contract, auto-generating if absent."""
        check = self.get_check()
        name = check.get("name") or check.get("id")
        if name:
            return name
        parts = []
        col = self.get_column_name()
        if col:
            parts.append(col)
        elif schema := self.get_schema_name():
            parts.append(schema)
        check_type = check.get("metric") or check.get("dimension") or "check"
        parts.append(check_type)
        return "_".join(parts)

    def get_expected_value(self) -> tuple[str, Any]:
        """Extract (operator, expected_value) from the check dict."""
        check = self.get_check()
        for key in (
            "mustBe",
            "mustNotBe",
            "mustBeGreaterThan",
            "mustBeGreaterOrEqualTo",
            "mustBeLessThan",
            "mustBeLessOrEqualTo",
            "mustBeBetween",
            "mustNotBeBetween",
        ):
            value = check.get(key)
            if value is not None:
                return (key, value)
        return ("unknown", None)

    @staticmethod
    def evaluate(actual_value: Any, operator: str, expected_value: Any) -> bool:
        """Evaluate whether actual_value satisfies the check condition."""
        if operator == "mustBe":
            return actual_value == expected_value
        elif operator == "mustNotBe":
            return actual_value != expected_value
        elif operator == "mustBeGreaterThan":
            return actual_value > expected_value
        elif operator == "mustBeGreaterOrEqualTo":
            return actual_value >= expected_value
        elif operator == "mustBeLessThan":
            return actual_value < expected_value
        elif operator == "mustBeLessOrEqualTo":
            return actual_value <= expected_value
        elif operator == "mustBeBetween":
            return expected_value[0] <= actual_value <= expected_value[1]
        elif operator == "mustNotBeBetween":
            return not (expected_value[0] <= actual_value <= expected_value[1])
        return False

    def build_result(
        self,
        *,
        actual_value: Any,
        execution_time_ms: float,
        failed_rows_fetcher: Callable[[], nw.DataFrame | None] | None = None,
    ) -> CheckResult:
        """Build a PASSED or FAILED CheckResult from the actual value."""
        from vowl.executors.base import CheckResult

        check = self.get_check()
        check_name = self.get_check_name()
        operator, expected_value = self.get_expected_value()
        passed = self.evaluate(actual_value, operator, expected_value)
        metadata = dict(self.get_result_metadata())

        if passed:
            return CheckResult(
                check_name=check_name,
                status="PASSED",
                details=check.get("description") or f"Check passed: {operator} {expected_value}",
                actual_value=actual_value,
                expected_value=expected_value,
                metadata=metadata,
                execution_time_ms=execution_time_ms,
            )
        return CheckResult(
            check_name=check_name,
            status="FAILED",
            details=check.get("description") or f"Check failed: expected {operator} {expected_value}, got {actual_value}",
            actual_value=actual_value,
            expected_value=expected_value,
            failed_rows_fetcher=failed_rows_fetcher,
            metadata=metadata,
            execution_time_ms=execution_time_ms,
        )

    def build_error_result(
        self,
        *,
        error_message: str,
        execution_time_ms: float,
        **extra_metadata: Any,
    ) -> CheckResult:
        """Build an ERROR CheckResult."""
        from vowl.executors.base import CheckResult

        metadata = dict(self.get_result_metadata())
        metadata.update(extra_metadata)
        return CheckResult(
            check_name=self.get_check_name(),
            status="ERROR",
            details=error_message,
            metadata=metadata,
            execution_time_ms=execution_time_ms,
        )


class TableCheckMixin:
    """Mixin for table-level check references (navigate 1 level up for schema)."""

    def get_schema_name(self) -> str | None:
        schema_path = self.get_schema_path()
        return self._contract.resolve(f"{schema_path}.name")

    def get_schema_path(self) -> str:
        return self._contract.resolve_parent(self._path, levels=1)


class ColumnCheckMixin:
    """Mixin for column-level check references (navigate 2 levels up for schema)."""

    def get_schema_name(self) -> str | None:
        schema_path = self.get_schema_path()
        return self._contract.resolve(f"{schema_path}.name")

    def get_schema_path(self) -> str:
        return self._contract.resolve_parent(self._path, levels=2)

    def get_column_path(self) -> str:
        return self._contract.resolve_parent(self._path, levels=1)

    def get_column_name(self) -> str | None:
        col_path = self.get_column_path()
        return self._contract.resolve(f"{col_path}.name")

    def get_logical_type(self) -> str | None:
        col_path = self.get_column_path()
        return self._contract.resolve(f"{col_path}.logicalType")

    def get_logical_type_options(self) -> dict[str, Any] | None:
        col_path = self.get_column_path()
        return self._contract.resolve(f"{col_path}.logicalTypeOptions")


__all__ = ["CheckReference", "CheckResultMetadata", "TableCheckMixin", "ColumnCheckMixin"]
