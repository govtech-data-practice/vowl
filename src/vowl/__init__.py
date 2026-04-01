"""
vowl

A data quality validation framework that validates data against
ODCS (Open Data Contract Standard) data contracts.

Supports:
- Any DataFrame (pandas, polars, PyArrow, cuDF, Modin, etc. via narwhals + DuckDB/Ibis)
- PySpark DataFrames (via PySpark/Ibis)
- Database connections (PostgreSQL, MySQL, Snowflake, etc. via Ibis)
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("vowl")
except PackageNotFoundError:
    __version__ = "0.0.0"

from .adapters import BaseAdapter, IbisAdapter, MultiSourceAdapter
from .config import ValidationConfig
from .contracts.contract import Contract
from .executors import BaseExecutor, CheckResult, IbisSQLExecutor, SQLExecutor
from .mapper import DataSourceMapper, create_adapter
from .validate import ValidationResult, ValidationRunner, validate_data


__all__ = [
    # Main API
    "validate_data",
    "ValidationResult",
    "ValidationRunner",
    "ValidationConfig",
    # Contract
    "Contract",
    # Mapper
    "DataSourceMapper",
    "create_adapter",
    # Adapters
    "BaseAdapter",
    "IbisAdapter",
    "MultiSourceAdapter",
    # Executors
    "BaseExecutor",
    "SQLExecutor",
    "CheckResult",
    "IbisSQLExecutor",
]
