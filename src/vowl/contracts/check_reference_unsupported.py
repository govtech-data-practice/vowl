"""Unsupported check reference — placeholder for unimplemented check types/metrics."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .check_reference_base import CheckReference, ColumnCheckMixin, TableCheckMixin

if TYPE_CHECKING:
    from .contract import Contract


class UnsupportedCheckReference(CheckReference):
    """A check reference for unrecognised or unimplemented check types.

    Instead of raising at contract parse time, this reference is created
    so the check still appears in consolidated output with ERROR status
    and a descriptive message.
    """

    def __init__(self, contract: Contract, path: str, error_message: str):
        super().__init__(contract, path)
        self._error_message = error_message

    @property
    def error_message(self) -> str:
        return self._error_message

    def get_schema_name(self) -> str | None:
        return None

    def get_schema_path(self) -> str:
        return "$"


class UnsupportedTableCheckReference(TableCheckMixin, UnsupportedCheckReference):
    """Table-level unsupported check reference."""


class UnsupportedColumnCheckReference(ColumnCheckMixin, UnsupportedCheckReference):
    """Column-level unsupported check reference."""


__all__ = [
    "UnsupportedCheckReference",
    "UnsupportedColumnCheckReference",
    "UnsupportedTableCheckReference",
]
