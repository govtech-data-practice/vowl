"""Custom-engine check reference implementations."""

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any

from .check_reference_base import CheckReference, ColumnCheckMixin, TableCheckMixin

if TYPE_CHECKING:
    pass


class CustomCheckReference(CheckReference, ABC):
    """Base for custom-engine quality checks (ODCS ``type: "custom"``)."""

    def get_execution_engine(self) -> str:
        return self.get_engine() or "unknown"

    def get_engine(self) -> str | None:
        """Return the ``engine`` name from the quality entry."""
        check = self.get_check()
        return check.get("engine")

    def get_implementation(self) -> str | dict[str, Any] | None:
        """Return the ``implementation`` from the quality entry."""
        check = self.get_check()
        return check.get("implementation")


class CustomTableCheckReference(TableCheckMixin, CustomCheckReference):
    """Table-level custom-engine quality check."""


class CustomColumnCheckReference(ColumnCheckMixin, CustomCheckReference):
    """Column-level custom-engine quality check."""


__all__ = [
    "CustomCheckReference",
    "CustomColumnCheckReference",
    "CustomTableCheckReference",
]
