"""Data Quality Validation Module.

Compatibility facade that preserves the public `vowl.validate` import surface
while the implementation lives under `vowl.validation`.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from .adapters.base import BaseAdapter
from .adapters.ibis_adapter import IbisAdapter
from .adapters.multi_source_adapter import MultiSourceAdapter
from .config import ValidationConfig
from .contracts.contract import Contract
from .mapper import DataSourceMapper
from .validation.api import validate_data as _validate_data
from .validation.result import ValidationResult as _ValidationResult
from .validation.runner import ValidationRunner as _ValidationRunner

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class ValidationResult(_ValidationResult):
    """Public validation result facade."""


class ValidationRunner(_ValidationRunner):
    """Public validation runner facade with patch-friendly dependencies."""

    contract_cls = Contract
    mapper_cls = DataSourceMapper
    adapter_cls = IbisAdapter
    multi_adapter_cls = MultiSourceAdapter
    result_cls = ValidationResult
    config_cls = ValidationConfig


def validate_data(
    contract: Contract | str | Path,
    *,
    adapter: BaseAdapter | None = None,
    df: Any | None = None,
    connection_str: str | None = None,
    spark_session: SparkSession | None = None,
    adapters: dict[str, BaseAdapter] | None = None,
    config: ValidationConfig | None = None,
) -> ValidationResult:
    """Validate data against an ODCS data quality contract."""
    return cast(
        ValidationResult,
        _validate_data(
            contract,
            adapter=adapter,
            df=df,
            connection_str=connection_str,
            spark_session=spark_session,
            adapters=adapters,
            config=config,
            runner_cls=ValidationRunner,
            contract_cls=Contract,
            multi_adapter_cls=MultiSourceAdapter,
        ),
    )


__all__ = [
    "validate_data",
    "ValidationResult",
    "ValidationRunner",
    "Contract",
    "DataSourceMapper",
    "IbisAdapter",
    "MultiSourceAdapter",
    "ValidationConfig",
]
