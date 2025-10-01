__version__ = "1.0.0"

# from .executors.pandas_executor import PandasExecutor
from .contracts.contract import Contract

from .validate import validate_data, ValidationResult

__all__ = [
    "validate_data",
    "ValidationResult",
]