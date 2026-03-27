import yaml
from typing import Dict, Any, List, Optional, TYPE_CHECKING
import os
import re
import warnings
from urllib.parse import urlparse
import tempfile

from jsonpath_ng import parse as jsonpath_parse
import sqlglot
from sqlglot import exp

from .models import validate_contract, SUPPORTED_VERSIONS

from .models.ODCS_types import DataContract, DataQuality, Server

if TYPE_CHECKING:
    from .check_reference import CheckReference


class Contract:
    """
    Represents a data quality contract that defines validation rules and schema expectations.
    
    A Contract encapsulates the validation logic defined in a contract file and provides
    methods to extract SQL checks, table names, and metadata for data quality validation.
    
    Contracts are always validated against their ODCS apiVersion schema on creation
    using jsonschema validation.
    
    Attributes:
        contract_data (Dict[str, Any]): The parsed contract data from YAML or JSON
    """
     
    def __init__(self, contract_data: Dict[str, Any]):
        self.contract_data: DataContract = contract_data
        
        # Validate on construction using jsonschema
        api_version = contract_data.get("apiVersion")
        if not api_version:
            raise ValueError(
                "Contract does not specify an apiVersion. "
                f"Supported versions: {', '.join(SUPPORTED_VERSIONS)}"
            )
        validate_contract(contract_data, api_version)

    @classmethod
    def _fetch_from_http_url(cls, url: str) -> str:
        """
        Fetch a contract file from an HTTP(S) URL.
        
        Supports formats:
        - github.com/user/repo/blob/branch/path/file.yaml
        - raw.githubusercontent.com/user/repo/branch/path/file.yaml
        
        Args:
            url: HTTP(S) URL to the file
            
        Returns:
            File content as string
            
        Raises:
            ImportError: If requests is not installed
            IOError: If there's an error fetching the file
        """
        try:
            import requests
        except ImportError:
            raise ImportError(
                "The 'requests' package is required to load contracts from HTTP URLs. "
                "It is included in base installation; please reinstall vowl "
                "or verify your Python environment."
            )
        
        # Convert blob URLs to raw URLs
        raw_url = url
        if "github.com" in url and "/blob/" in url:
            raw_url = url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
        elif "gitlab.com" in url and "/-/blob/" in url:
            raw_url = url.replace("/-/blob/", "/-/raw/")
        
        try:
            response = requests.get(raw_url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            raise IOError(f"Error fetching contract from URL {url}: {e}")
    
    @classmethod
    def _fetch_from_s3_uri(cls, s3_path: str) -> str:
        """
        Fetch a file from S3.
        
        Supports formats:
        - s3://bucket-name/path/to/file.yaml
        
        Args:
            s3_path: S3 URI to the file
            
        Returns:
            File content as string
            
        Raises:
            ImportError: If boto3 is not installed
            IOError: If there's an error fetching the file
        """
        try:
            import boto3
        except ImportError:
            raise ImportError(
                "The 'boto3' package is required to load contracts from S3. "
                "It is included in base installation; please reinstall vowl "
                "or verify your Python environment."
            )
        
        # Parse S3 path
        match = re.match(r's3://([^/]+)/(.+)', s3_path)
        if not match:
            raise ValueError(f"Invalid S3 path format: {s3_path}")
        
        bucket_name = match.group(1)
        object_key = match.group(2)
        
        try:
            s3_client = boto3.client('s3')
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            raise IOError(f"Error fetching contract from S3 {s3_path}: {e}")
    
    @classmethod
    def load(cls, contract_file_path: str) -> "Contract":
        """
        Create a Contract instance from a local path or remote URI.
        
        The contract is automatically validated against its ODCS apiVersion schema
        using jsonschema validation.
        
        Supports loading from:
        - Local file paths
        - HTTP(S) URLs (e.g., https://github.com/user/repo/blob/main/contract.yaml)
        - S3 URIs (e.g., s3://bucket-name/path/to/contract.yaml)
        
        Args:
            contract_file_path: Path/URL/URI to the contract file (YAML or JSON)
            
        Returns:
            Contract instance with validated data
            
        Raises:
            FileNotFoundError: If the local contract file doesn't exist
            yaml.YAMLError: If the contract YAML/JSON content is malformed
            IOError: If there's an error reading/fetching the file
            ValueError: If the contract file is empty or missing apiVersion
            jsonschema.ValidationError: If the contract data is invalid
            ImportError: If required packages (requests/boto3) are not installed
        """
        # Determine the source type and fetch content
        contract_content = None
        
        # Check if it's an S3 path
        if contract_file_path.startswith("s3://"):
            contract_content = cls._fetch_from_s3_uri(contract_file_path)
        # Check if it's an HTTP(S) URL
        elif contract_file_path.startswith(("http://", "https://")):
            contract_content = cls._fetch_from_http_url(contract_file_path)
        # Otherwise, treat as local file path
        else:
            if not os.path.exists(contract_file_path):
                raise FileNotFoundError(f"Contract file not found: {contract_file_path}")
            
            try:
                with open(contract_file_path, mode="r", encoding="utf-8") as yaml_file:
                    contract_content = yaml_file.read()
            except Exception as file_reading_error:
                raise IOError(f"Error reading {contract_file_path}: {file_reading_error}")
        
        # Parse contract content (YAML parser also accepts JSON)
        try:
            contract_data = yaml.safe_load(contract_content)
            if not contract_data:
                raise ValueError(f"Contract file is empty: {contract_file_path}")
        except yaml.YAMLError as yaml_parsing_error:
            raise yaml.YAMLError(
                f"Invalid contract YAML/JSON in {contract_file_path}: {yaml_parsing_error}"
            )

        return cls(contract_data)
    
    def get_schema_properties(self) -> Dict[str, Any]:
        """
        Returns the entire dictionary of properties for the first schema entry.
        This includes name, data_domain_name, and any other custom properties.
        """
        if self.contract_data and 'schema' in self.contract_data and self.contract_data['schema']:
            return self.contract_data['schema'][0]
        return {}

    def get_version(self) -> Optional[str]:
        """Returns the version from the contract's metadata."""
        return self.contract_data.get("version")
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Extract metadata information from the contract.
        
        Returns:
            Dictionary containing contract metadata (kind, version, etc.)
        """
        metadata_field_names = ["kind", "apiVersion", "version", "status", "id", "description"]
        return {
            field_name: self.contract_data.get(field_name)
            for field_name in metadata_field_names
        }

    def get_api_version(self) -> str:
        """
        Returns the ODCS API version from the contract.
        
        Returns:
            The API version string (e.g., "v3.1.0")
        """
        return self.contract_data.get("apiVersion", "")

    def resolve(self, jsonpath: str) -> Any:
        """
        Resolve a JSONPath expression against the contract data.
        
        Args:
            jsonpath: A JSONPath expression (e.g., "$.schema[0].name")
        
        Returns:
            The resolved value, or None if not found.
            
        Example:
            >>> contract.resolve("$.schema[0].name")
            "hdb_resale_prices"
            >>> contract.resolve("$.schema[0].properties[2].logicalType")
            "integer"
        """
        try:
            expr = jsonpath_parse(jsonpath)
            matches = expr.find(self.contract_data)
            
            if len(matches) == 0:
                return None
            
            if len(matches) > 1:
                warnings.warn(
                    f"JSONPath '{jsonpath}' matched {len(matches)} elements (expected 1)",
                    UserWarning,
                    stacklevel=2,
                )
            
            # Return first match's value
            return matches[0].value
        except Exception as e:
            warnings.warn(
                f"Error resolving JSONPath '{jsonpath}': {e}",
                UserWarning,
                stacklevel=2,
            )
            return None

    def resolve_parent(self, jsonpath: str, levels: int = 1) -> str:
        """
        Get parent path by removing N segments from the end.
        
        Args:
            jsonpath: The JSONPath to get parent of.
            levels: Number of path segments to remove (default 1).
            
        Returns:
            Parent JSONPath string.
            
        Example:
            >>> contract.resolve_parent("$.schema[0].properties[2].quality[0]", 1)
            "$.schema[0].properties[2]"
            >>> contract.resolve_parent("$.schema[0].properties[2].quality[0]", 2)
            "$.schema[0]"
        """
        # Simple string-based approach: split by '.' and '[', remove levels
        # $.schema[0].properties[2].quality[0]
        # Split into segments: ['$', 'schema[0]', 'properties[2]', 'quality[0]']
        
        # Handle the path by splitting on '.' but keeping array indices
        parts = []
        current = ""
        for char in jsonpath:
            if char == '.' and current:
                parts.append(current)
                current = ""
            else:
                current += char
        if current:
            parts.append(current)
        
        # Remove 'levels' number of segments from end
        # Each "level" is one segment (e.g., "quality[0]" or "properties[2]")
        if levels >= len(parts):
            return "$"
        
        remaining = parts[:-levels]
        return ".".join(remaining)

    def get_check_references_by_schema(
        self,
    ) -> Dict[str, List["CheckReference"]]:
        """
        Extract check references grouped by schema name.
        
        Returns CheckReference objects that maintain context for navigating
        back to related contract elements.
        
        Auto-generated checks are always included and run first:
        - Type checks: for columns with logicalType (integer, number, boolean, date, timestamp, time)
        - Required checks: for columns with required: true (validates no NULLs)
        - Unique checks: for columns with unique: true (validates uniqueness)
        - Primary key checks: for columns with primaryKey: true (validates unique + not null)
        
        Returns:
            Dict mapping schema names to lists of CheckReference objects.
            
        Example:
            >>> contract = Contract.load("my_contract.yaml")
            >>> refs_by_schema = contract.get_check_references_by_schema()
            >>> for schema_name, refs in refs_by_schema.items():
            ...     for ref in refs:
            ...         print(f"{ref.get_check().get('name')}: {ref.get_logical_type()}")
        """
        from .check_reference import (
            CheckReference,
            SQLTableCheckReference,
            SQLColumnCheckReference,
            DeclaredColumnExistsCheckReference,
            LogicalTypeCheckReference,
            LogicalTypeOptionsCheckReference,
            RequiredCheckReference,
            UniqueCheckReference,
            PrimaryKeyCheckReference,
            LOGICAL_TYPE_TO_SQL,
        )
        from .check_reference_library_metrics import (
            LIBRARY_COLUMN_METRICS,
            LIBRARY_TABLE_METRICS,
        )
        from .check_reference_custom import (
            CustomTableCheckReference,
            CustomColumnCheckReference,
        )
        from .check_reference_unsupported import (
            UnsupportedTableCheckReference,
            UnsupportedColumnCheckReference,
        )

        TABLE_CHECK_TYPES = {
            "sql": SQLTableCheckReference,
            "custom": CustomTableCheckReference,
        }
        COLUMN_CHECK_TYPES = {
            "sql": SQLColumnCheckReference,
            "custom": CustomColumnCheckReference,
        }
        
        refs_by_schema: Dict[str, List[CheckReference]] = {}
        
        schema_list = self.contract_data.get('schema', [])
        for schema_idx, schema_obj in enumerate(schema_list):
            schema_name = schema_obj.get('name')
            if not schema_name:
                continue
            
            refs_by_schema[schema_name] = []
            
            # Auto-generated checks from property attributes (run first)
            properties = schema_obj.get('properties', [])
            for prop_idx, prop in enumerate(properties):
                prop_path = f"$.schema[{schema_idx}].properties[{prop_idx}]"
                prop_name = prop.get('name', f'property[{prop_idx}]')

                # Column existence checks for all declared properties.
                if prop.get('name'):
                    refs_by_schema[schema_name].append(
                        DeclaredColumnExistsCheckReference(self, prop_path)
                    )
                
                # Type checks for columns with logicalType
                logical_type = prop.get('logicalType')
                if logical_type:
                    if logical_type in LOGICAL_TYPE_TO_SQL:
                        refs_by_schema[schema_name].append(
                            LogicalTypeCheckReference(self, prop_path)
                        )
                    else:
                        # string, object, array have no SQL type check
                        warnings.warn(
                            f"No type check generated for '{prop_name}' with logicalType '{logical_type}': "
                            f"type checks only supported for {', '.join(sorted(LOGICAL_TYPE_TO_SQL.keys()))}",
                            UserWarning,
                            stacklevel=2,
                        )
                
                # LogicalTypeOptions checks
                logical_type_options = prop.get('logicalTypeOptions')
                if logical_type_options:
                    for option_key, option_value in logical_type_options.items():
                        if option_value is not None:
                            try:
                                refs_by_schema[schema_name].append(
                                    LogicalTypeOptionsCheckReference(
                                        self, prop_path, option_key, option_value
                                    )
                                )
                            except ValueError:
                                # Unsupported option - warning already issued in __init__
                                pass
                
                # Required checks for columns with required: true
                if prop.get('required') is True:
                    refs_by_schema[schema_name].append(
                        RequiredCheckReference(self, prop_path)
                    )
                
                # Unique checks for columns with unique: true
                if prop.get('unique') is True:
                    refs_by_schema[schema_name].append(
                        UniqueCheckReference(self, prop_path)
                    )
                
                # Primary key checks for columns with primaryKey: true
                if prop.get('primaryKey') is True:
                    refs_by_schema[schema_name].append(
                        PrimaryKeyCheckReference(self, prop_path)
                    )
            
            # Table-level checks
            table_quality = schema_obj.get('quality', [])
            for qual_idx in range(len(table_quality)):
                check_path = f"$.schema[{schema_idx}].quality[{qual_idx}]"
                check_type = table_quality[qual_idx].get("type", "sql")

                if check_type == "library":
                    metric = table_quality[qual_idx].get("metric")
                    metric_cls = LIBRARY_TABLE_METRICS.get(metric)
                    if metric_cls is None:
                        refs_by_schema[schema_name].append(
                            UnsupportedTableCheckReference(
                                self, check_path,
                                f"Unsupported library metric '{metric}' at schema level. "
                                f"Supported schema-level metrics: {', '.join(sorted(LIBRARY_TABLE_METRICS))}",
                            )
                        )
                    else:
                        refs_by_schema[schema_name].append(
                            metric_cls(self, check_path)
                        )
                else:
                    table_cls = TABLE_CHECK_TYPES.get(check_type)
                    if table_cls is None:
                        refs_by_schema[schema_name].append(
                            UnsupportedTableCheckReference(
                                self, check_path,
                                f"Unsupported check type '{check_type}'. "
                                f"Supported types: {', '.join(sorted(TABLE_CHECK_TYPES | {'library': None}))}",
                            )
                        )
                    else:
                        refs_by_schema[schema_name].append(
                            table_cls(self, check_path)
                        )
            
            # Column-level checks
            properties = schema_obj.get('properties', [])
            for prop_idx, prop in enumerate(properties):
                prop_path = f"$.schema[{schema_idx}].properties[{prop_idx}]"
                prop_quality = prop.get('quality', [])
                for qual_idx in range(len(prop_quality)):
                    check_path = f"$.schema[{schema_idx}].properties[{prop_idx}].quality[{qual_idx}]"
                    check_type = prop_quality[qual_idx].get("type", "sql")

                    if check_type == "library":
                        metric = prop_quality[qual_idx].get("metric")
                        metric_cls = LIBRARY_COLUMN_METRICS.get(metric)
                        if metric_cls is None:
                            refs_by_schema[schema_name].append(
                                UnsupportedColumnCheckReference(
                                    self, check_path,
                                    f"Unsupported library metric '{metric}' at property level. "
                                    f"Supported property-level metrics: {', '.join(sorted(LIBRARY_COLUMN_METRICS))}",
                                )
                            )
                        else:
                            refs_by_schema[schema_name].append(
                                metric_cls(self, check_path, prop_path)
                            )
                    else:
                        col_cls = COLUMN_CHECK_TYPES.get(check_type)
                        if col_cls is None:
                            refs_by_schema[schema_name].append(
                                UnsupportedColumnCheckReference(
                                    self, check_path,
                                    f"Unsupported check type '{check_type}'. "
                                    f"Supported types: {', '.join(sorted(COLUMN_CHECK_TYPES | {'library': None}))}",
                                )
                            )
                        else:
                            refs_by_schema[schema_name].append(
                                col_cls(self, check_path)
                            )
        
        return refs_by_schema

    def get_servers(self) -> List[Server]:
        """
        Get all servers defined in the contract.
        
        Returns:
            List of Server dicts.
        """
        return self.contract_data.get('servers', [])

    def get_server(self, server_name: Optional[str] = None) -> Server:
        """
        Get a server configuration from the contract.

        Looks up by the ``server`` field (the ODCS server identifier),
        falling back to matching ``environment`` if no ``server`` field
        matches.  Returns the first server when called with no arguments.

        Args:
            server_name: Server identifier or environment name to look up.
                If None, returns the first server.

        Returns:
            A Server dict with keys like ``server``, ``type``,
            ``environment``, ``description``, etc.

        Raises:
            ValueError: If no servers are defined or no server matches.

        Example:
            >>> contract = Contract.load("contract.yaml")
            >>> server = contract.get_server("uat-db")
            >>> server["server"]
            'uat-db'
            >>> server["type"]
            'postgres'
        """
        servers = self.get_servers()
        if not servers:
            raise ValueError(
                "No servers defined in the contract. "
                "Add a 'servers' section to your contract YAML."
            )

        if server_name is None:
            return servers[0]

        # Primary: match on the 'server' identifier field
        for server in servers:
            if server.get('server') == server_name:
                return server

        # Fallback: match on 'environment'
        for server in servers:
            if server.get('environment') == server_name:
                return server

        available = [s.get('server', '<unnamed>') for s in servers]
        raise ValueError(
            f"No server found matching '{server_name}'. "
            f"Available servers: {available}"
        )

    def get_schema_names(self) -> List[str]:
        """
        Get the names of all schemas defined in the contract.
        
        Returns:
            List of schema names
            
        Example:
            >>> contract.get_schema_names()
            ['orders', 'products', 'customers']
        """
        schema_list = self.contract_data.get('schema', [])
        return [s.get('name') for s in schema_list if s.get('name')]