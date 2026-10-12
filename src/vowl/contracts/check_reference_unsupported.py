"""Unsupported check reference: placeholder for unimplemented check types/metrics."""

from __future__ import annotations

import re
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

    def get_check(self) -> dict:
        """Return a synthetic check dict so base-class methods work."""
        raw = self._contract.resolve(self._path)
        if isinstance(raw, dict):
            return raw
        return {"name": None, "type": "unsupported", "description": self._error_message}

    def get_schema_name(self) -> str | None:
        return None

    def get_schema_path(self) -> str:
        return "$"


# The schema and property a path sits under, however deep it is: an
# unsupported check can be a quality rule, ``logicalTypeOptions.format`` or
# ``items.logicalTypeOptions.x``, so a fixed number of levels up is wrong.
_SCHEMA_PATH_RE = re.compile(r"^\$\.schema\[\d+\]")
_PROPERTY_PATH_RE = re.compile(r"^\$\.schema\[\d+\]\.properties\[\d+\]")


class UnsupportedTableCheckReference(TableCheckMixin, UnsupportedCheckReference):
    """Table-level unsupported check reference."""

    def get_schema_path(self) -> str:
        match = _SCHEMA_PATH_RE.match(self._path)
        return match.group(0) if match else super().get_schema_path()


class UnsupportedColumnCheckReference(ColumnCheckMixin, UnsupportedCheckReference):
    """Column-level unsupported check reference."""

    def get_schema_path(self) -> str:
        match = _SCHEMA_PATH_RE.match(self._path)
        return match.group(0) if match else super().get_schema_path()

    def get_column_path(self) -> str:
        match = _PROPERTY_PATH_RE.match(self._path)
        return match.group(0) if match else super().get_column_path()


__all__ = [
    "UnsupportedCheckReference",
    "UnsupportedColumnCheckReference",
    "UnsupportedTableCheckReference",
]
