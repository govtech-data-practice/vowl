"""Tests for ODCS models and validation."""

import inspect
import json
import re
from pathlib import Path

import pytest

# Test the jsonschema validation
from vowl.contracts.models import (
    SUPPORTED_VERSIONS,
    DataContract,
    DataQuality,
    ValidationError,
    get_latest_version,
    get_schema,
    validate_contract,
)

# Path to schema files
SCHEMAS_DIR = Path(__file__).parent.parent / "src" / "vowl" / "contracts" / "models" / "schemas"


def find_latest_schema_file() -> Path:
    """Find the latest schema file by parsing version numbers."""
    schema_files = list(SCHEMAS_DIR.glob("odcs-json-schema-v*.json"))
    # Filter out strict variants
    schema_files = [f for f in schema_files if "-strict" not in f.name]

    def parse_version(path: Path) -> tuple:
        """Extract version tuple from filename for sorting."""
        match = re.search(r"v(\d+)\.(\d+)\.(\d+)", path.name)
        if match:
            return tuple(int(x) for x in match.groups())
        return (0, 0, 0)

    # Sort by version and return the latest
    schema_files.sort(key=parse_version, reverse=True)
    return schema_files[0] if schema_files else None


LATEST_SCHEMA_FILE = find_latest_schema_file()
_match = re.search(r"v(\d+)\.(\d+)\.(\d+)", LATEST_SCHEMA_FILE.name)
LATEST_VERSION = "v" + ".".join(str(x) for x in _match.groups())


@pytest.fixture
def latest_schema():
    """Load the latest ODCS JSON schema."""
    with open(LATEST_SCHEMA_FILE, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def required_root_fields(latest_schema):
    """Get required fields from the root of the schema."""
    return latest_schema.get("required", [])


@pytest.fixture
def schema_properties(latest_schema):
    """Get all property definitions from the schema."""
    return latest_schema.get("properties", {})


class TestJsonSchemaValidation:
    """Test jsonschema validation against ODCS schemas."""

    def test_validate_minimal_contract_from_schema(self, required_root_fields):
        """Test validation of minimal valid contract using schema-defined required fields."""
        # Build minimal contract from schema requirements
        contract_data = {
            "apiVersion": LATEST_VERSION,
            "kind": "DataContract",
        }
        # Add all required fields with minimal valid values
        for field in required_root_fields:
            if field not in contract_data:
                contract_data[field] = f"test-{field}"

        # Should not raise
        validate_contract(contract_data)

    def test_validate_contract_with_schema_section(self):
        """Test validation of contract with schema section."""
        contract_data = {
            "apiVersion": LATEST_VERSION,
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "test-contract",
            "status": "active",
            "schema": [
                {
                    "name": "users",
                    "properties": [
                        {"name": "id", "logicalType": "integer", "primaryKey": True},
                        {"name": "email", "logicalType": "string", "required": True},
                    ],
                }
            ],
        }
        # Should not raise
        validate_contract(contract_data)

    def test_validate_fails_when_missing_required_fields(self, required_root_fields):
        """Test validation fails when required fields from schema are missing."""
        # Contract with only apiVersion and kind, missing other required fields
        contract_data = {
            "apiVersion": LATEST_VERSION,
            "kind": "DataContract",
        }
        # Should raise ValidationError because required fields are missing
        with pytest.raises(ValidationError):
            validate_contract(contract_data)

    def test_validate_invalid_api_version(self):
        """Test validation fails for unsupported API version."""
        contract_data = {
            "apiVersion": "v99.0.0",
            "version": "1.0.0",
            "id": "test",
            "status": "draft",
        }
        with pytest.raises(ValueError, match="Unsupported API version"):
            validate_contract(contract_data)

    def test_supported_versions_match_schema_files(self):
        """Test that SUPPORTED_VERSIONS matches available schema files."""
        for version in SUPPORTED_VERSIONS:
            schema_file = SCHEMAS_DIR / f"odcs-json-schema-{version}.json"
            assert schema_file.exists(), f"Schema file missing for version {version}"

    def test_get_schema_returns_valid_schema(self, latest_schema):
        """Test that get_schema returns the same schema as direct file load."""
        loaded_schema = get_schema(LATEST_VERSION)
        assert loaded_schema == latest_schema

    def test_schema_defines_data_quality_types(self, latest_schema):
        """Test that schema defines expected DataQuality types."""
        defs = latest_schema.get("$defs", {})
        data_quality = defs.get("DataQuality", {})

        # Check that type field has expected enum values
        type_prop = data_quality.get("properties", {}).get("type", {})
        expected_types = {"text", "library", "sql", "custom"}
        actual_types = set(type_prop.get("enum", []))

        assert expected_types == actual_types, f"Expected {expected_types}, got {actual_types}"

    def test_schema_defines_dimension_enum(self, latest_schema):
        """Test that schema defines expected dimension values."""
        defs = latest_schema.get("$defs", {})
        data_quality = defs.get("DataQuality", {})
        dimension_prop = data_quality.get("properties", {}).get("dimension", {})

        expected_dimensions = {
            "accuracy",
            "completeness",
            "conformity",
            "consistency",
            "coverage",
            "timeliness",
            "uniqueness",
        }
        actual_dimensions = set(dimension_prop.get("enum", []))

        assert expected_dimensions == actual_dimensions

    def test_schema_defines_metric_enum(self, latest_schema):
        """Test that schema defines expected metric values."""
        defs = latest_schema.get("$defs", {})
        library = defs.get("DataQualityLibrary", {})
        metric_prop = library.get("properties", {}).get("metric", {})

        expected_metrics = {"nullValues", "missingValues", "invalidValues", "duplicateValues", "rowCount"}
        actual_metrics = set(metric_prop.get("enum", []))

        assert expected_metrics == actual_metrics

    def test_validate_contract_with_quality_checks(self):
        """Test validation of contract with data quality checks."""
        contract_data = {
            "apiVersion": LATEST_VERSION,
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "test-contract",
            "status": "active",
            "schema": [
                {
                    "name": "orders",
                    "quality": [
                        {
                            "type": "sql",
                            "query": "SELECT COUNT(*) FROM orders",
                            "mustBeGreaterThan": 0,
                        },
                        {
                            "type": "library",
                            "metric": "nullValues",
                            "mustBe": 0,
                        },
                    ],
                    "properties": [
                        {
                            "name": "order_id",
                            "logicalType": "integer",
                            "primaryKey": True,
                            "quality": [
                                {
                                    "type": "library",
                                    "metric": "duplicateValues",
                                    "mustBe": 0,
                                }
                            ],
                        },
                    ],
                }
            ],
        }
        # Should not raise
        validate_contract(contract_data)


class TestOdcs32Support:
    """Test support for ODCS v3.2.0 (additive superset of v3.1.0)."""

    def test_v320_is_registered_and_latest(self):
        """v3.2.0 must be a supported version and the newest one."""
        assert "v3.2.0" in SUPPORTED_VERSIONS
        assert SUPPORTED_VERSIONS[0] == "v3.2.0"
        assert get_latest_version() == "v3.2.0"

    def test_v320_schema_loads(self):
        """The bundled v3.2.0 schema must be retrievable and additive over 3.1.0."""
        schema = get_schema("v3.2.0")
        assert schema["title"].startswith("Open Data Contract Standard")
        # 3.2.0 adds a top-level `context` block; the DataQuality engine contract is unchanged.
        assert "context" in schema.get("properties", {})
        assert "DataQuality" in schema.get("$defs", {})

    def test_validate_contract_with_v320_only_fields(self):
        """A contract using 3.2-only metadata validates against the v3.2.0 schema."""
        contract_data = {
            "apiVersion": "v3.2.0",
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "test-v320",
            "status": "active",
            # RFC-0038 AI/semantic context (new top-level block in 3.2.0)
            "context": {
                "instructions": "Use only for internal analytics.",
                "constraints": [{"constraint": "Do not expose raw values externally."}],
            },
            "schema": [
                {
                    "name": "documents",
                    # `deprecated` + `synonyms` promoted to element level in 3.2.0
                    "deprecated": False,
                    "synonyms": [{"synonym": "docs", "source": "glossary"}],
                    "properties": [
                        {
                            "name": "status",
                            "logicalType": "string",
                            # `enum` + `semanticType` are new property fields in 3.2.0
                            "enum": [
                                {"value": "draft"},
                                {"value": "active"},
                                {"value": "archived"},
                            ],
                            "semanticType": "dimension",
                        },
                        {
                            # `vector` is a new logicalType in 3.2.0
                            "name": "embedding",
                            "logicalType": "vector",
                        },
                    ],
                }
            ],
        }
        # Should not raise against the v3.2.0 schema.
        validate_contract(contract_data)

    def test_v320_contract_loads_and_builds_check_references(self):
        """A v3.2.0 contract loads and produces check references without error.

        The new `vector` logicalType has no SQL type check, so it degrades to a
        UserWarning during auto-check generation rather than raising.
        """
        from vowl.contracts.contract import Contract

        contract_data = {
            "apiVersion": "v3.2.0",
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "test-v320-load",
            "status": "active",
            "schema": [
                {
                    "name": "documents",
                    "properties": [
                        {"name": "id", "logicalType": "integer", "primaryKey": True},
                        {"name": "embedding", "logicalType": "vector"},
                    ],
                }
            ],
        }
        contract = Contract(contract_data)
        with pytest.warns(UserWarning, match="No type check generated"):
            refs_by_schema = contract.get_check_references_by_schema()
        assert "documents" in refs_by_schema


class TestTypedDictTypes:
    """Test that TypedDict types work for type hints."""

    def test_data_quality_type_hint(self):
        """Test that DataQuality TypedDict works as expected."""
        # DataQuality is a TypedDict, so we can use it as a type hint
        dq: DataQuality = {
            "type": "sql",
            "query": "SELECT 1",
            "mustBe": 1,
        }
        assert dq.get("type") == "sql"
        assert dq.get("query") == "SELECT 1"

    def test_data_contract_type_hint(self):
        """Test that DataContract TypedDict works as expected."""
        contract: DataContract = {
            "apiVersion": LATEST_VERSION,
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "test",
            "status": "draft",
        }
        assert contract.get("apiVersion") == LATEST_VERSION
        assert contract.get("id") == "test"


class TestClassExplosion:
    """Test that TypedDict class count is reasonable."""

    def test_odcs_types_class_count(self):
        """ODCS_types should have a reasonable number of TypedDict classes."""
        from vowl.contracts.models import ODCS_types

        classes = [
            name
            for name, obj in inspect.getmembers(ODCS_types)
            if inspect.isclass(obj) and obj.__module__ == ODCS_types.__name__
        ]
        # TypedDict approach should have ~20-30 classes
        assert len(classes) < 50, f"Too many classes: {len(classes)} classes"
        print(f"\nODCS_types class count: {len(classes)}")
