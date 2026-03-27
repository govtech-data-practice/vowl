"""
ODCS Models - TypedDict types and jsonschema validation.

This module provides:
1. TypedDict type definitions for IDE autocomplete and static type checking
2. jsonschema validation against official ODCS JSON schemas

Type hints are provided via ODCS_types.py which is iteratively updated
as new ODCS schema versions are released.

Validation is done using jsonschema with the official schema files in
src/vowl/contracts/models/schemas/.

Usage:
    from vowl.contracts.models import validate_contract, ValidationError
    
    # Validate contract data against schema
    contract_data = yaml.safe_load(open("contract.yaml"))
    validate_contract(contract_data)  # Raises ValidationError if invalid
    
    # Or use type hints for IDE support
    from vowl.contracts.models.ODCS_types import DataContract, DataQuality

Supported Versions:
    v3.1.0, v3.0.2, v3.0.1, v3.0.0, v2.2.2, v2.2.1
"""

import json
from pathlib import Path
from typing import Any, Dict

import jsonschema
from jsonschema import ValidationError  # Re-export for convenience

from .ODCS_types import (
    DataContract,
    DataQuality,
    DataQualityBase,
    DataQualityCustom,
    DataQualityLibrary,
    DataQualityOperatorsMixin,
    DataQualitySql,
    DataQualityText,
    Dimension,
    Metric,
    OpenDataContractStandardODCS,
    SchemaObject,
    SchemaProperty,
)

# Backwards compatibility alias
DataQualityOperators = DataQualityOperatorsMixin

# Path to schema files
SCHEMAS_DIR = Path(__file__).parent / "schemas"

# Version to schema file mapping
SCHEMA_FILES = {
    "v3.1.0": "odcs-json-schema-v3.1.0.json",
    "v3.0.2": "odcs-json-schema-v3.0.2.json",
    "v3.0.1": "odcs-json-schema-v3.0.1.json",
    "v3.0.0": "odcs-json-schema-v3.0.0.json",
    "v2.2.2": "odcs-json-schema-v2.2.2.json",
    "v2.2.1": "odcs-json-schema-v2.2.1.json",
}

# Supported versions (ordered newest to oldest)
SUPPORTED_VERSIONS = list(SCHEMA_FILES.keys())

# Cache for loaded schemas
_schema_cache: Dict[str, Any] = {}


def _load_schema(api_version: str) -> Dict[str, Any]:
    """Load and cache a JSON schema for the given API version."""
    if api_version not in _schema_cache:
        if api_version not in SCHEMA_FILES:
            supported = ", ".join(SUPPORTED_VERSIONS)
            raise ValueError(
                f"Unsupported API version: {api_version}. "
                f"Supported versions: {supported}"
            )
        
        schema_path = SCHEMAS_DIR / SCHEMA_FILES[api_version]
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_path, "r", encoding="utf-8") as f:
            _schema_cache[api_version] = json.load(f)
    
    return _schema_cache[api_version]


def validate_contract(contract_data: Dict[str, Any], api_version: str | None = None) -> None:
    """
    Validate contract data against the ODCS JSON schema.
    
    Args:
        contract_data: The contract data dictionary to validate
        api_version: The ODCS API version to validate against.
                    If None, uses the apiVersion from contract_data.
        
    Raises:
        ValidationError: If the contract data doesn't match the schema
        ValueError: If the API version is not supported or not specified
        
    Example:
        >>> contract_data = yaml.safe_load(open("contract.yaml"))
        >>> validate_contract(contract_data)  # Uses apiVersion from data
        >>> validate_contract(contract_data, "v3.1.0")  # Force specific version
    """
    if api_version is None:
        api_version = contract_data.get("apiVersion")
        if not api_version:
            raise ValueError(
                "Contract does not specify an apiVersion. "
                f"Supported versions: {', '.join(SUPPORTED_VERSIONS)}"
            )
    
    schema = _load_schema(api_version)
    jsonschema.validate(instance=contract_data, schema=schema)


def get_schema(api_version: str) -> Dict[str, Any]:
    """
    Get the JSON schema for the specified API version.
    
    Args:
        api_version: The ODCS API version (e.g., "v3.1.0")
        
    Returns:
        The JSON schema dictionary
        
    Raises:
        ValueError: If the API version is not supported
    """
    return _load_schema(api_version)


def get_latest_version() -> str:
    """Get the latest supported API version."""
    return SUPPORTED_VERSIONS[0]


__all__ = [
    # Validation functions
    "validate_contract",
    "get_schema",
    "ValidationError",
    # Version info
    "get_latest_version", 
    "SUPPORTED_VERSIONS",
    # Core types (for type hints)
    "DataContract",
    "OpenDataContractStandardODCS",
    "DataQuality",
    "DataQualityBase",
    "SchemaObject",
    "SchemaProperty",
    # DataQuality specialized types
    "DataQualityCustom",
    "DataQualityLibrary",
    "DataQualityOperatorsMixin",
    "DataQualityOperators",  # Backwards compat alias
    "DataQualitySql",
    "DataQualityText",
    # Enums/Literals
    "Dimension",
    "Metric",
]
