"""
ODCS TypedDict models for type hints only.

NOTE: This file is iteratively updated as new versions of ODCS schemas are released.
The type definitions here aim to be a superset compatible with supported ODCS versions.
For strict validation, use jsonschema with the official schema files in
src/vowl/contracts/models/schemas/.

This module provides static type definitions for the Open Data Contract Standard (ODCS).
These are NOT used for validation - they exist purely for IDE autocomplete and type checking.

Key design decisions:
- All fields use camelCase (matching JSON keys) - no aliases needed
- All fields are optional (total=False) to avoid blocking on schema updates
- Enums replaced with Literal unions for simpler typing
- Discriminated unions use plain Union (manual narrowing via type field check)
- No runtime validation - use jsonschema with official ODCS schema if needed

Usage:
    from vowl.contracts.models.ODCS_types import DataContract, DataQuality
    
    def process_contract(contract: DataContract) -> None:
        for table in contract.get("schema") or []:
            for quality in table.get("quality") or []:
                if quality.get("type") == "sql":
                    print(quality.get("query"))
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict, Union

# =============================================================================
# Type Aliases (replacing Enums with Literal unions for flexibility)
# =============================================================================

ApiVersion = Literal[
    "v3.1.0", "v3.0.2", "v3.0.1", "v3.0.0",
    "v2.2.2", "v2.2.1", "v2.2.0",
]

ServerType = Literal[
    "api", "athena", "azure", "bigquery", "clickhouse", "databricks",
    "denodo", "dremio", "duckdb", "glue", "cloudsql", "db2", "hive",
    "impala", "informix", "kafka", "kinesis", "local", "mysql", "oracle",
    "postgresql", "postgres", "presto", "pubsub", "redshift", "s3",
    "sftp", "snowflake", "sqlserver", "synapse", "trino", "vertica",
    "zen", "custom",
]

LogicalType = Literal[
    "string", "date", "timestamp", "time", "number",
    "integer", "object", "array", "boolean",
]

Dimension = Literal[
    "accuracy", "completeness", "conformity", "consistency",
    "coverage", "timeliness", "uniqueness",
]

Metric = Literal[
    "nullValues", "missingValues", "invalidValues",
    "duplicateValues", "rowCount",
]

# Reference types - just strings with specific patterns (patterns not enforced at type level)
ShorthandReference = str  # e.g., "table_name.column_name"
FullyQualifiedReference = str  # e.g., "schema/id/properties/column_id"


# =============================================================================
# Simple TypedDict definitions
# =============================================================================

class AuthoritativeDefinition(TypedDict, total=False):
    """Reference to external authoritative definition.
    
    Required fields in schema: url, type
    """
    id: str
    url: str  # Required in schema
    type: str  # Required in schema
    description: str


class CustomProperty(TypedDict, total=False):
    """User-defined key-value property.
    
    Required fields in schema: property, value
    """
    id: str
    property: str  # Required in schema
    value: str | float | int | bool | list[Any] | dict[str, Any] | None  # Required in schema
    description: str


class Pricing(TypedDict, total=False):
    """Pricing information for the data contract."""
    id: str
    priceAmount: float
    priceCurrency: str
    priceUnit: str


class ServiceLevelAgreementProperty(TypedDict, total=False):
    """SLA property definition.
    
    Required fields in schema: property, value
    """
    id: str
    property: str  # Required in schema
    value: str | float | int | bool | None  # Required in schema
    valueExt: str | float | int | bool | None
    unit: str
    element: str
    driver: str
    description: str
    scheduler: str
    schedule: str


class TeamMember(TypedDict, total=False):
    """Team member information.
    
    Required fields in schema: username
    """
    id: str
    username: str  # Required in schema
    name: str
    description: str
    role: str
    dateIn: str  # ISO date string
    dateOut: str  # ISO date string
    replacedByUsername: str
    tags: list[str]
    customProperties: list[CustomProperty]
    authoritativeDefinitions: list[AuthoritativeDefinition]


class Team(TypedDict, total=False):
    """Team information."""
    id: str
    name: str
    description: str
    members: list[TeamMember]
    tags: list[str]
    customProperties: list[CustomProperty]
    authoritativeDefinitions: list[AuthoritativeDefinition]


class Role(TypedDict, total=False):
    """IAM role definition.
    
    Required fields in schema: role
    """
    id: str
    role: str  # Required in schema
    description: str
    access: str
    firstLevelApprovers: str
    secondLevelApprovers: str
    customProperties: list[CustomProperty]


class Server(TypedDict, total=False):
    """Server/connection information.
    
    Required fields in schema: server, type
    """
    id: str
    server: str  # Required in schema
    type: ServerType  # Required in schema
    description: str
    environment: str
    roles: list[Role]
    customProperties: list[CustomProperty]


class SupportItem(TypedDict, total=False):
    """Support channel information.
    
    Required fields in schema: channel
    """
    id: str
    channel: str  # Required in schema
    url: str
    description: str
    tool: str
    scope: str
    invitationUrl: str
    customProperties: list[CustomProperty]


class Description(TypedDict, total=False):
    """Contract description section."""
    usage: str
    purpose: str
    limitations: str
    authoritativeDefinitions: list[AuthoritativeDefinition]
    customProperties: list[CustomProperty]


class RelationshipPropertyLevel(TypedDict, total=False):
    """Relationship at property/column level.
    
    Note: 'from' field must NOT be specified at property level.
    Required fields in schema: to
    """
    type: Literal["foreignKey"]
    to: str | list[str]  # Required in schema
    customProperties: list[CustomProperty]


# Note: 'from' is a Python reserved keyword. Access via subscript notation:
#   rel["from"] instead of rel.from_
class RelationshipSchemaLevel(TypedDict, total=False):
    """Relationship at schema/table level.
    
    Required fields in schema: from, to
    Note: Access 'from' field via subscript: rel["from"]
    """
    type: Literal["foreignKey"]
    to: str | list[str]  # Required in schema
    customProperties: list[CustomProperty]


# Use this to create RelationshipSchemaLevel with 'from' field:
# rel: RelationshipSchemaLevel = {"from": "col1", "to": "col2"}  # Works fine
RelationshipSchemaLevel.__annotations__["from"] = str | list[str]  # Add 'from' key dynamically


# =============================================================================
# DataQuality discriminated union types
# =============================================================================

class _DataQualityBase(TypedDict, total=False):
    """Common fields for all DataQuality types."""
    id: str
    name: str
    description: str
    dimension: Dimension
    severity: str
    businessImpact: str
    schedule: str
    scheduler: str
    method: str
    unit: str
    tags: list[str]
    authoritativeDefinitions: list[AuthoritativeDefinition]
    customProperties: list[CustomProperty]


class _DataQualityOperators(TypedDict, total=False):
    """Comparison operator fields."""
    mustBe: Any
    mustNotBe: Any
    mustBeGreaterThan: float
    mustBeGreaterOrEqualTo: float
    mustBeLessThan: float
    mustBeLessOrEqualTo: float
    mustBeBetween: list[float]
    mustNotBeBetween: list[float]


class DataQualityText(_DataQualityBase, total=False):
    """Text-only data quality description."""
    type: Literal["text"]


class DataQualityLibrary(_DataQualityBase, _DataQualityOperators, total=False):
    """Library-based data quality check using ODCS metrics.
    
    Required fields in schema: metric
    """
    type: Literal["library"]
    metric: Metric  # Required in schema
    rule: str  # Deprecated - use metric instead
    arguments: dict[str, Any]


class DataQualitySql(_DataQualityBase, _DataQualityOperators, total=False):
    """SQL-based data quality check.
    
    Required fields in schema: query
    """
    type: Literal["sql"]
    query: str  # Required in schema


class DataQualityCustom(_DataQualityBase, total=False):
    """Custom engine data quality check.
    
    Required fields in schema: engine, implementation
    """
    type: Literal["custom"]
    engine: str  # Required in schema
    implementation: str | dict[str, Any]  # Required in schema


# Union type - type narrowing works with manual checks on "type" field
DataQuality = Union[DataQualityText, DataQualityLibrary, DataQualitySql, DataQualityCustom]
"""
DataQuality union type.

Type narrowing example:
    def process_quality(dq: DataQuality) -> None:
        if dq.get("type") == "sql":
            # Type checkers understand dq has 'query' field here
            print(dq.get("query"))
        elif dq.get("type") == "library":
            print(dq.get("metric"))
"""


# =============================================================================
# Schema definitions
# =============================================================================

# Forward reference for recursive types
class SchemaItemProperty(TypedDict, total=False):
    """Item definition for array logicalType (recursive schema structure).
    
    Inherits all fields from SchemaBaseProperty in the JSON schema.
    """
    id: str
    name: str
    description: str
    businessName: str
    logicalType: LogicalType
    logicalTypeOptions: dict[str, Any]
    physicalType: str
    physicalName: str
    primaryKey: bool
    primaryKeyPosition: int
    required: bool
    unique: bool
    partitioned: bool
    partitionKeyPosition: int
    classification: str
    encryptedName: str
    transformSourceObjects: list[str]
    transformLogic: str
    transformDescription: str
    examples: list[Any]
    criticalDataElement: bool
    tags: list[str]
    authoritativeDefinitions: list[AuthoritativeDefinition]
    customProperties: list[CustomProperty]
    relationships: list[RelationshipPropertyLevel]
    quality: list[DataQuality]  # Forward reference
    # For nested objects within arrays
    properties: list[SchemaProperty]  # Forward reference


class SchemaProperty(TypedDict, total=False):
    """Column/property definition within a table.
    
    Required fields in schema: name
    """
    id: str
    name: str  # Required in schema
    description: str
    businessName: str
    logicalType: LogicalType
    logicalTypeOptions: dict[str, Any]
    physicalType: str
    physicalName: str
    primaryKey: bool
    primaryKeyPosition: int
    required: bool
    unique: bool
    partitioned: bool
    partitionKeyPosition: int
    classification: str
    encryptedName: str
    transformSourceObjects: list[str]
    transformLogic: str
    transformDescription: str
    examples: list[Any]
    criticalDataElement: bool
    tags: list[str]
    authoritativeDefinitions: list[AuthoritativeDefinition]
    customProperties: list[CustomProperty]
    relationships: list[RelationshipPropertyLevel]
    quality: list[DataQuality]
    # For nested objects (when logicalType is "object")
    properties: list[SchemaProperty]  # Self-reference
    # For array items (when logicalType is "array")
    items: SchemaItemProperty


class SchemaObject(TypedDict, total=False):
    """Table/object definition in the schema.
    
    Required fields in schema: name
    """
    id: str
    name: str  # Required in schema
    description: str
    businessName: str
    logicalType: Literal["object"]
    physicalType: str
    physicalName: str
    dataGranularityDescription: str
    tags: list[str]
    authoritativeDefinitions: list[AuthoritativeDefinition]
    customProperties: list[CustomProperty]
    properties: list[SchemaProperty]
    relationships: list[RelationshipSchemaLevel]
    quality: list[DataQuality]


# =============================================================================
# Root contract type
# =============================================================================

class DataContract(TypedDict, total=False):
    """
    Root type for Open Data Contract Standard (ODCS).
    
    This covers v3.x schemas. All fields are optional here to handle
    schema evolution gracefully - validation should be done separately
    with the official JSON schema if strictness is required.
    
    Required fields in schema: version, apiVersion, kind, id, status
    """
    # Required fields in schema (kept optional here for flexibility)
    version: str  # Required in schema
    apiVersion: ApiVersion  # Required in schema
    kind: Literal["DataContract"]  # Required in schema
    id: str  # Required in schema
    status: str  # Required in schema

    # Optional fields
    name: str
    tenant: str
    tags: list[str]
    servers: list[Server]
    dataProduct: str
    description: Description
    domain: str
    schema: list[SchemaObject]
    support: list[SupportItem]
    price: Pricing
    team: Team | list[TeamMember]
    roles: list[Role]
    slaDefaultElement: str  # Deprecated
    slaProperties: list[ServiceLevelAgreementProperty]
    authoritativeDefinitions: list[AuthoritativeDefinition]
    customProperties: list[CustomProperty]
    contractCreatedTs: str  # ISO datetime string


# Aliases for backward compatibility
OpenDataContractStandardODCS = DataContract

# Public aliases for internal base classes (used by __init__.py imports)
DataQualityBase = _DataQualityBase
DataQualityOperatorsMixin = _DataQualityOperators


# =============================================================================
# Helper functions for working with contracts
# =============================================================================

def get_quality_type(dq: DataQuality) -> str | None:
    """Get the type discriminator from a DataQuality dict."""
    return dq.get("type")


def is_sql_quality(dq: DataQuality) -> bool:
    """Check if a DataQuality is SQL-based."""
    return dq.get("type") == "sql"


def is_library_quality(dq: DataQuality) -> bool:
    """Check if a DataQuality is library-based."""
    return dq.get("type") == "library"
