#!/usr/bin/env python3
"""
Generate Pydantic models from ODCS JSON Schema files.

This script uses datamodel-code-generator to create strongly-typed Pydantic models
from ODCS JSON schemas. Generated models support full validation of data contracts.

Usage:
    # Generate models for a specific schema (run from models/ directory)
    python generate_models.py --schema schemas/odcs-json-schema-v3.1.0.json

    # Generate models for all schemas in the schemas directory
    python generate_models.py --all

    # Or run from project root via Makefile
    make generate-models
"""

import argparse
import re

# Bandit B404: This is a trusted, repository-maintained developer utility script
# that intentionally invokes a local codegen CLI; subprocess is required here.
import subprocess  # nosec B404
import sys
from pathlib import Path

# Default paths (script is now in models/ directory)
DEFAULT_SCHEMAS_DIR = Path(__file__).parent / "schemas"
DEFAULT_MODELS_DIR = Path(__file__).parent


def extract_version_from_schema_name(schema_path: Path) -> str:
    """
    Extract version string from schema filename.

    Examples:
        odcs-json-schema-v3.1.0.json -> v3.1.0
        odcs-json-schema-v3.0.2.json -> v3.0.2
    """
    pattern = r"odcs-json-schema-(v[\d.]+)\.json"
    match = re.search(pattern, schema_path.name)
    if not match:
        raise ValueError(f"Cannot extract version from schema filename: {schema_path.name}")
    return match.group(1)


def version_to_module_name(version: str, raw: bool = False) -> str:
    """
    Convert version string to Python module name.

    Examples:
        v3.1.0 -> v3_1_0 (or v3_1_0_raw if raw=True)
        v3.0.2 -> v3_0_2 (or v3_0_2_raw if raw=True)
    """
    base = version.replace(".", "_")
    return f"{base}_raw" if raw else base


def generate_model(schema_path: Path, output_dir: Path) -> Path:
    """
    Generate a Pydantic model file from a JSON schema.

    Args:
        schema_path: Path to the JSON schema file
        output_dir: Directory to write the generated model

    Returns:
        Path to the generated Python file
    """
    version = extract_version_from_schema_name(schema_path)
    module_name = version_to_module_name(version, raw=True)
    output_file = output_dir / f"{module_name}.py"

    print(f"Generating model for {version}...")
    print(f"  Schema: {schema_path}")
    print(f"  Output: {output_file}")

    # Build datamodel-codegen command
    cmd = [
        sys.executable, "-m", "datamodel_code_generator",
        "--input", str(schema_path),
        "--output", str(output_file),
        "--input-file-type", "jsonschema",
        # Model options
        "--output-model-type", "pydantic_v2.BaseModel",
        "--use-annotated",
        "--field-constraints",
        "--use-default",
        "--use-default-kwarg",
        "--use-one-literal-as-default",
        # Naming and structure
        "--snake-case-field",
        "--use-title-as-name",
        # Reduce class explosion
        "--allof-class-hierarchy", "always",
        "--allof-merge-mode", "all",
        "--collapse-root-models",
        "--use-standard-collections",
        "--reuse-model",
        # Enum handling
        "--enum-field-as-literal", "one",
        # Extra options
        "--target-python-version", "3.10",
        "--use-double-quotes",
        "--wrap-string-literal",
    ]

    try:
        # Bandit B603: Command is constructed as a fixed argument list (no shell)
        # with trusted local inputs (schema/output paths) in this dev-only workflow.
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )  # nosec B603
        print("  Success!")
        if result.stdout:
            print(f"  {result.stdout}")
        return output_file
    except subprocess.CalledProcessError as e:
        print(f"  Error generating model: {e.stderr}", file=sys.stderr)
        raise


def update_models_init(models_dir: Path, versions: list[str]) -> None:
    """
    Update the __init__.py in models directory to expose all versions.

    Args:
        models_dir: Path to the models directory
        versions: List of version strings (e.g., ["v3.1.0", "v3.0.2"])
    """
    # Sort versions in descending order (newest first)
    sorted_versions = sorted(versions, key=lambda v: [int(x) for x in v[1:].split(".")], reverse=True)

    # Generate imports (use refactored modules, not raw)
    imports = []
    raw_imports = []
    version_map_entries = []
    for version in sorted_versions:
        module_name = version_to_module_name(version)
        raw_module_name = version_to_module_name(version, raw=True)
        imports.append(f"from . import {module_name}")
        raw_imports.append(f"from . import {raw_module_name}")
        version_map_entries.append(f'    "{version}": {module_name},')

    init_content = f'''#!/usr/bin/env python3
"""
ODCS Pydantic Models - Auto-generated from JSON schemas.

This module provides version-specific Pydantic models for ODCS data contracts.
Models are generated using datamodel-code-generator for static type checking
and validation.

Usage:
    from vowl.contracts.models import get_contract_model

    # Load model for specific version
    DataContract = get_contract_model("v3.1.0")
    contract = DataContract.model_validate(yaml_data)

    # Or use the typed model directly
    from vowl.contracts.models.v3_1_0 import OpenDataContractStandardOdcs

Supported Versions:
    {", ".join(sorted_versions)}
"""

from typing import Type

{chr(10).join(imports)}


# Version to module mapping (ordered from newest to oldest)
VERSION_MAP = {{
{chr(10).join(version_map_entries)}
}}

# Supported versions
SUPPORTED_VERSIONS = list(VERSION_MAP.keys())


def get_contract_model(api_version: str) -> Type:
    """Get the DataContract Pydantic model for the specified API version.

    Args:
        api_version: The ODCS API version (e.g., "v3.1.0", "v3.0.2")

    Returns:
        The DataContract Pydantic model class for the specified version

    Raises:
        ValueError: If the API version is not supported

    Example:
        >>> DataContract = get_contract_model("v3.1.0")
        >>> contract = DataContract.model_validate(yaml_data)
    """
    if api_version not in VERSION_MAP:
        supported = ", ".join(SUPPORTED_VERSIONS)
        raise ValueError(
            f"Unsupported API version: {{api_version}}. "
            f"Supported versions: {{supported}}"
        )

    module = VERSION_MAP[api_version]
    # datamodel-code-generator names the root model based on schema title
    # For ODCS it will be "OpenDataContractStandardOdcs"
    return module.OpenDataContractStandardOdcs


def get_latest_version() -> str:
    """Get the latest supported API version."""
    return SUPPORTED_VERSIONS[0]


__all__ = [
    "get_contract_model",
    "get_latest_version",
    "SUPPORTED_VERSIONS",
    "VERSION_MAP",
]
'''

    init_file = models_dir / "__init__.py"
    init_file.write_text(init_content)
    print(f"Updated {init_file}")


def find_all_schemas(schemas_dir: Path) -> list[Path]:
    """Find all ODCS schema files in the given directory."""
    pattern = "odcs-json-schema-v*.json"
    return list(schemas_dir.glob(pattern))


def main():
    parser = argparse.ArgumentParser(
        description="Generate Pydantic models from ODCS JSON Schema files."
    )
    parser.add_argument(
        "--schema",
        type=Path,
        help="Path to a specific JSON schema file to process."
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="process_all",
        help="Process all schema files in the schemas directory."
    )
    parser.add_argument(
        "--schemas-dir",
        type=Path,
        default=DEFAULT_SCHEMAS_DIR,
        help=f"Directory containing JSON schema files (default: {DEFAULT_SCHEMAS_DIR})"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_MODELS_DIR,
        help=f"Directory for generated model files (default: {DEFAULT_MODELS_DIR})"
    )
    parser.add_argument(
        "--no-update-init",
        action="store_true",
        help="Skip updating __init__.py in the models directory."
    )

    args = parser.parse_args()

    # Determine which schemas to process
    if args.schema:
        if not args.schema.exists():
            print(f"Error: Schema file not found: {args.schema}", file=sys.stderr)
            sys.exit(1)
        schemas = [args.schema]
    elif args.process_all:
        schemas = find_all_schemas(args.schemas_dir)
        if not schemas:
            print(f"Error: No schema files found in {args.schemas_dir}", file=sys.stderr)
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)

    # Ensure output directory exists
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Generate models
    versions = []
    for schema_path in schemas:
        try:
            generate_model(schema_path, args.output_dir)
            version = extract_version_from_schema_name(schema_path)
            versions.append(version)
        except Exception as e:
            print(f"Error processing {schema_path}: {e}", file=sys.stderr)
            sys.exit(1)

    # Update __init__.py
    if not args.no_update_init and versions:
        update_models_init(args.output_dir, versions)

    print(f"\nGenerated {len(versions)} model(s) successfully!")


if __name__ == "__main__":
    main()
