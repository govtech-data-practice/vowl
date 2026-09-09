import contextvars
import ipaddress
import os
import re
import socket
import warnings
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin, urlparse

import yaml
from jsonpath_ng import parse as jsonpath_parse

from .models import SUPPORTED_VERSIONS, validate_contract
from .models.ODCS_types import DataContract, Server

if TYPE_CHECKING:
    from .check_reference import CheckReference


# Maximum number of HTTP redirects to follow when fetching a remote contract.
_MAX_HTTP_REDIRECTS = 5


class ContractURLError(ValueError):
    """Raised when a contract URL is disallowed (e.g. points at an internal host)."""


def _is_disallowed_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Return True for IPs that must not be fetched (SSRF protection).

    Blocks loopback, private (RFC1918 / ULA), link-local (incl. the cloud
    metadata address 169.254.169.254), reserved, multicast and unspecified
    addresses. IPv4-mapped IPv6 addresses are unwrapped and re-checked.
    """
    if getattr(ip, "ipv4_mapped", None) is not None:
        ip = ip.ipv4_mapped  # type: ignore[assignment]
    return ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified


# Pins the IP address that the HTTP client is allowed to connect to for the
# current fetch, keyed by hostname. Set by ``_pinned_dns`` around each request so
# the outbound connection uses the exact address we validated, closing the
# DNS-rebinding TOCTOU between validation and connection. Uses a ContextVar so it
# is isolated per thread / async task and needs no global lock.
_pinned_dns_target: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar(
    "_vowl_pinned_dns_target", default=None
)
_dns_pin_installed = False


def _install_dns_pin() -> None:
    """Install a one-time urllib3 hook that honours ``_pinned_dns_target``.

    urllib3 (used by requests) resolves the hostname again at connect time. We
    wrap its ``create_connection`` so that, whenever a pin is active for the host
    being dialled, the socket connects to the pre-validated IP instead of a
    freshly re-resolved (and possibly rebound) address. TLS SNI and certificate
    verification are unaffected because the request URL still carries the
    hostname. All other hosts pass through untouched.
    """
    global _dns_pin_installed
    if _dns_pin_installed:
        return
    import urllib3.util.connection as _u3_connection

    _original_create_connection = _u3_connection.create_connection

    def _pinned_create_connection(address, *args, **kwargs):  # type: ignore[no-untyped-def]
        pin = _pinned_dns_target.get()
        if pin is not None:
            host, port = address
            if host == pin[0]:
                address = (pin[1], port)
        return _original_create_connection(address, *args, **kwargs)

    _u3_connection.create_connection = _pinned_create_connection
    _dns_pin_installed = True


@contextmanager
def _pinned_dns(hostname: str, ip: str):
    """Pin *hostname* to *ip* for HTTP connections made within the block."""
    _install_dns_pin()
    token = _pinned_dns_target.set((hostname, ip))
    try:
        yield
    finally:
        _pinned_dns_target.reset(token)


def _validate_public_http_url(url: str) -> tuple[str, str]:
    """Validate that *url* is an http(s) URL that resolves to a public host.

    This is an SSRF guard for the contract-loading entry point: it blocks
    non-http(s) schemes and any host that resolves to an internal, loopback,
    link-local (cloud metadata) or otherwise reserved IP address.

    Returns the ``(hostname, ip)`` of a validated public address so the caller
    can pin the outbound connection to it (see ``_pinned_dns``), preventing a
    DNS-rebinding attacker from swapping in an internal address between this
    check and the actual request.

    Raises:
        ContractURLError: If the scheme or resolved address is not allowed.
    """
    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in ("http", "https"):
        raise ContractURLError(f"Unsupported URL scheme '{parsed.scheme}' for contract fetch: {url}")

    hostname = parsed.hostname
    if not hostname:
        raise ContractURLError(f"Contract URL has no host: {url}")

    port = parsed.port or (443 if scheme == "https" else 80)

    try:
        addrinfos = socket.getaddrinfo(hostname, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as e:
        raise ContractURLError(f"Could not resolve contract host '{hostname}': {e}") from e

    if not addrinfos:
        raise ContractURLError(f"Could not resolve contract host '{hostname}'")

    validated_ip: str | None = None
    for *_, sockaddr in addrinfos:
        try:
            ip = ipaddress.ip_address(sockaddr[0])
        except ValueError:
            raise ContractURLError(f"Unexpected address for contract host '{hostname}': {sockaddr[0]}") from None
        if _is_disallowed_ip(ip):
            raise ContractURLError(
                f"Refusing to fetch contract from non-public address {ip} (host '{hostname}'). "
                "Only public HTTP(S) endpoints are allowed."
            )
        if validated_ip is None:
            validated_ip = sockaddr[0]

    # Every resolved address passed the check above; pin to the first one.
    # (validated_ip is always set here because addrinfos is non-empty and every
    # entry was validated, but we raise explicitly rather than assert so the
    # invariant is enforced in optimized (-O) runs too.)
    if validated_ip is None:  # pragma: no cover - defensive, unreachable
        raise ContractURLError(f"Could not resolve contract host '{hostname}'")
    return hostname, validated_ip


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

    def __init__(self, contract_data: dict[str, Any]):
        self.contract_data: DataContract = contract_data

        # Validate on construction using jsonschema
        api_version = contract_data.get("apiVersion")
        if not api_version:
            raise ValueError(
                f"Contract does not specify an apiVersion. Supported versions: {', '.join(SUPPORTED_VERSIONS)}"
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
            ) from None

        # Convert blob URLs to raw URLs
        raw_url = url
        parsed = urlparse(url)
        hostname = parsed.hostname or ""
        if (hostname == "github.com" or hostname.endswith(".github.com")) and "/blob/" in parsed.path:
            raw_url = url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
        elif (hostname == "gitlab.com" or hostname.endswith(".gitlab.com")) and "/-/blob/" in parsed.path:
            raw_url = url.replace("/-/blob/", "/-/raw/")

        # SSRF protection: validate the target (and every redirect hop) resolves
        # to a public address before we send a request to it, then pin the
        # connection to that validated IP so a DNS-rebinding attacker cannot swap
        # in an internal address between the check and the request. Redirects are
        # followed manually so an attacker-controlled 3xx cannot bounce us to an
        # internal host or the cloud metadata endpoint.
        try:
            current_url = raw_url
            for _ in range(_MAX_HTTP_REDIRECTS + 1):
                pin_host, pin_ip = _validate_public_http_url(current_url)
                with _pinned_dns(pin_host, pin_ip):
                    response = requests.get(current_url, timeout=30, allow_redirects=False)
                if response.is_redirect or response.is_permanent_redirect:
                    location = response.headers.get("Location")
                    if not location:
                        break
                    current_url = urljoin(current_url, location)
                    continue
                response.raise_for_status()
                return response.text
            raise OSError(f"Too many redirects fetching contract from URL {url}")
        except ContractURLError:
            raise
        except requests.RequestException as e:
            raise OSError(f"Error fetching contract from URL {url}: {e}") from e

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
            ) from None

        # Parse S3 path
        match = re.match(r"s3://([^/]+)/(.+)", s3_path)
        if not match:
            raise ValueError(f"Invalid S3 path format: {s3_path}")

        bucket_name = match.group(1)
        object_key = match.group(2)

        try:
            s3_client = boto3.client("s3")
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            return response["Body"].read().decode("utf-8")
        except Exception as e:
            raise OSError(f"Error fetching contract from S3 {s3_path}: {e}") from e

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
                with open(contract_file_path, encoding="utf-8") as yaml_file:
                    contract_content = yaml_file.read()
            except Exception as file_reading_error:
                raise OSError(f"Error reading {contract_file_path}: {file_reading_error}") from file_reading_error

        # Parse contract content (YAML parser also accepts JSON)
        try:
            contract_data = yaml.safe_load(contract_content)
            if not contract_data:
                raise ValueError(f"Contract file is empty: {contract_file_path}")
        except yaml.YAMLError as yaml_parsing_error:
            raise yaml.YAMLError(
                f"Invalid contract YAML/JSON in {contract_file_path}: {yaml_parsing_error}"
            ) from yaml_parsing_error

        return cls(contract_data)

    def get_schema_properties(self) -> dict[str, Any]:
        """
        Returns the entire dictionary of properties for the first schema entry.
        This includes name, data_domain_name, and any other custom properties.
        """
        if self.contract_data and "schema" in self.contract_data and self.contract_data["schema"]:
            return self.contract_data["schema"][0]
        return {}

    def get_version(self) -> str | None:
        """Returns the version from the contract's metadata."""
        return self.contract_data.get("version")

    def get_metadata(self) -> dict[str, Any]:
        """
        Extract metadata information from the contract.

        Returns:
            Dictionary containing contract metadata (kind, version, etc.)
        """
        metadata_field_names = ["kind", "apiVersion", "version", "status", "id", "description"]
        return {field_name: self.contract_data.get(field_name) for field_name in metadata_field_names}

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
            if char == "." and current:
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
    ) -> dict[str, list["CheckReference"]]:
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
            LOGICAL_TYPE_TO_SQL,
            CheckReference,
            DeclaredColumnExistsCheckReference,
            LogicalTypeCheckReference,
            LogicalTypeOptionsCheckReference,
            PrimaryKeyCheckReference,
            RequiredCheckReference,
            SQLColumnCheckReference,
            SQLTableCheckReference,
            UniqueCheckReference,
        )
        from .check_reference_custom import (
            CustomColumnCheckReference,
            CustomTableCheckReference,
        )
        from .check_reference_library_metrics import (
            LIBRARY_COLUMN_METRICS,
            LIBRARY_TABLE_METRICS,
        )
        from .check_reference_unsupported import (
            UnsupportedColumnCheckReference,
            UnsupportedTableCheckReference,
        )

        TABLE_CHECK_TYPES = {
            "sql": SQLTableCheckReference,
            "custom": CustomTableCheckReference,
        }
        COLUMN_CHECK_TYPES = {
            "sql": SQLColumnCheckReference,
            "custom": CustomColumnCheckReference,
        }

        refs_by_schema: dict[str, list[CheckReference]] = {}

        schema_list = self.contract_data.get("schema", [])
        for schema_idx, schema_obj in enumerate(schema_list):
            schema_name = schema_obj.get("name")
            if not schema_name:
                continue

            refs_by_schema[schema_name] = []

            # Auto-generated checks from property attributes (run first)
            properties = schema_obj.get("properties", [])
            for prop_idx, prop in enumerate(properties):
                prop_path = f"$.schema[{schema_idx}].properties[{prop_idx}]"
                prop_name = prop.get("name", f"property[{prop_idx}]")

                # Column existence checks for all declared properties.
                if prop.get("name"):
                    refs_by_schema[schema_name].append(DeclaredColumnExistsCheckReference(self, prop_path))

                # Type checks for columns with logicalType
                logical_type = prop.get("logicalType")
                if logical_type:
                    if logical_type in LOGICAL_TYPE_TO_SQL:
                        refs_by_schema[schema_name].append(LogicalTypeCheckReference(self, prop_path))
                    else:
                        # string, object, array have no SQL type check
                        warnings.warn(
                            f"No type check generated for '{prop_name}' with logicalType '{logical_type}': "
                            f"type checks only supported for {', '.join(sorted(LOGICAL_TYPE_TO_SQL.keys()))}",
                            UserWarning,
                            stacklevel=2,
                        )

                # LogicalTypeOptions checks
                logical_type_options = prop.get("logicalTypeOptions")
                if logical_type_options:
                    for option_key, option_value in logical_type_options.items():
                        if option_value is not None:
                            try:
                                refs_by_schema[schema_name].append(
                                    LogicalTypeOptionsCheckReference(self, prop_path, option_key, option_value)
                                )
                            except ValueError as exc:
                                refs_by_schema[schema_name].append(
                                    UnsupportedColumnCheckReference(
                                        self,
                                        f"{prop_path}.logicalTypeOptions.{option_key}",
                                        str(exc),
                                    )
                                )

                # Required checks for columns with required: true
                if prop.get("required") is True:
                    refs_by_schema[schema_name].append(RequiredCheckReference(self, prop_path))

                # Unique checks for columns with unique: true
                if prop.get("unique") is True:
                    refs_by_schema[schema_name].append(UniqueCheckReference(self, prop_path))

                # Primary key checks for columns with primaryKey: true
                if prop.get("primaryKey") is True:
                    refs_by_schema[schema_name].append(PrimaryKeyCheckReference(self, prop_path))

            # Table-level checks
            table_quality = schema_obj.get("quality", [])
            for qual_idx in range(len(table_quality)):
                check_path = f"$.schema[{schema_idx}].quality[{qual_idx}]"
                check_type = table_quality[qual_idx].get("type", "sql")

                if check_type == "library":
                    metric = table_quality[qual_idx].get("metric")
                    metric_cls = LIBRARY_TABLE_METRICS.get(metric)
                    if metric_cls is None:
                        refs_by_schema[schema_name].append(
                            UnsupportedTableCheckReference(
                                self,
                                check_path,
                                f"Unsupported library metric '{metric}' at schema level. "
                                f"Supported schema-level metrics: {', '.join(sorted(LIBRARY_TABLE_METRICS))}",
                            )
                        )
                    else:
                        refs_by_schema[schema_name].append(metric_cls(self, check_path))
                else:
                    table_cls = TABLE_CHECK_TYPES.get(check_type)
                    if table_cls is None:
                        refs_by_schema[schema_name].append(
                            UnsupportedTableCheckReference(
                                self,
                                check_path,
                                f"Unsupported check type '{check_type}'. "
                                f"Supported types: {', '.join(sorted(TABLE_CHECK_TYPES | {'library': None}))}",
                            )
                        )
                    else:
                        refs_by_schema[schema_name].append(table_cls(self, check_path))

            # Column-level checks
            properties = schema_obj.get("properties", [])
            for prop_idx, prop in enumerate(properties):
                prop_path = f"$.schema[{schema_idx}].properties[{prop_idx}]"
                prop_quality = prop.get("quality", [])
                for qual_idx in range(len(prop_quality)):
                    check_path = f"$.schema[{schema_idx}].properties[{prop_idx}].quality[{qual_idx}]"
                    check_type = prop_quality[qual_idx].get("type", "sql")

                    if check_type == "library":
                        metric = prop_quality[qual_idx].get("metric")
                        metric_cls = LIBRARY_COLUMN_METRICS.get(metric)
                        if metric_cls is None:
                            refs_by_schema[schema_name].append(
                                UnsupportedColumnCheckReference(
                                    self,
                                    check_path,
                                    f"Unsupported library metric '{metric}' at property level. "
                                    f"Supported property-level metrics: {', '.join(sorted(LIBRARY_COLUMN_METRICS))}",
                                )
                            )
                        else:
                            refs_by_schema[schema_name].append(metric_cls(self, check_path, prop_path))
                    else:
                        col_cls = COLUMN_CHECK_TYPES.get(check_type)
                        if col_cls is None:
                            refs_by_schema[schema_name].append(
                                UnsupportedColumnCheckReference(
                                    self,
                                    check_path,
                                    f"Unsupported check type '{check_type}'. "
                                    f"Supported types: {', '.join(sorted(COLUMN_CHECK_TYPES | {'library': None}))}",
                                )
                            )
                        else:
                            refs_by_schema[schema_name].append(col_cls(self, check_path))

        return refs_by_schema

    def get_servers(self) -> list[Server]:
        """
        Get all servers defined in the contract.

        Returns:
            List of Server dicts.
        """
        return self.contract_data.get("servers", [])

    def get_server(self, server_name: str | None = None) -> Server:
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
            raise ValueError("No servers defined in the contract. Add a 'servers' section to your contract YAML.")

        if server_name is None:
            return servers[0]

        # Primary: match on the 'server' identifier field
        for server in servers:
            if server.get("server") == server_name:
                return server

        # Fallback: match on 'environment'
        for server in servers:
            if server.get("environment") == server_name:
                return server

        available = [s.get("server", "<unnamed>") for s in servers]
        raise ValueError(f"No server found matching '{server_name}'. Available servers: {available}")

    def get_schema_names(self) -> list[str]:
        """
        Get the names of all schemas defined in the contract.

        Returns:
            List of schema names

        Example:
            >>> contract.get_schema_names()
            ['orders', 'products', 'customers']
        """
        schema_list = self.contract_data.get("schema", [])
        return [s.get("name") for s in schema_list if s.get("name")]
