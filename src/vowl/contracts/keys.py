"""Primary key helpers shared by the contract checks and the row-quality pushdown."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def primary_key_columns(properties: Sequence[Mapping[str, Any]]) -> list[str]:
    """The declared primary key columns of a schema, in key order.

    ODCS orders a composite key by ``primaryKeyPosition`` (starting from 1).
    Columns without a position (or with the default -1) follow in declaration
    order.
    """
    keyed = [
        (index, prop)
        for index, prop in enumerate(properties)
        if isinstance(prop, Mapping) and prop.get("name") and prop.get("primaryKey") is True
    ]

    def position(item: tuple[int, Mapping[str, Any]]) -> tuple[int, int]:
        index, prop = item
        declared = prop.get("primaryKeyPosition")
        return (declared if isinstance(declared, int) and declared >= 0 else 1_000_000 + index, index)

    return [prop["name"] for _, prop in sorted(keyed, key=position)]


__all__ = ["primary_key_columns"]
