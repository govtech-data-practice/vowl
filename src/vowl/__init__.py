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

import importlib
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("vowl")
except PackageNotFoundError:
    __version__ = "0.0.0"

# Eager imports — always needed on every call
from .contracts.contract import Contract
from .config import ValidationConfig
from .validate import validate_data, ValidationResult, ValidationRunner

# Lazy imports — loaded on first access via __getattr__ (PEP 562)
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    # Mapper
    "DataSourceMapper": (".mapper", "DataSourceMapper"),
    "create_adapter": (".mapper", "create_adapter"),
    # Adapters
    "BaseAdapter": (".adapters", "BaseAdapter"),
    "IbisAdapter": (".adapters", "IbisAdapter"),
    "MultiSourceAdapter": (".adapters", "MultiSourceAdapter"),
    # Executors
    "BaseExecutor": (".executors", "BaseExecutor"),
    "SQLExecutor": (".executors", "SQLExecutor"),
    "CheckResult": (".executors", "CheckResult"),
    "IbisSQLExecutor": (".executors", "IbisSQLExecutor"),
}


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        module_path, attr = _LAZY_IMPORTS[name]
        mod = importlib.import_module(module_path, __name__)
        val = getattr(mod, attr)
        globals()[name] = val  # cache so __getattr__ isn't called again
        return val
    raise AttributeError(f"module 'vowl' has no attribute {name!r}")


def __dir__():
    return list(__all__)


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