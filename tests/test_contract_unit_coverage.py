from __future__ import annotations

import builtins
from pathlib import Path

import pandas as pd
import pytest
import requests
import yaml

from vowl import DataSourceMapper
from vowl.contracts.check_reference import (
    SQLColumnCheckReference,
    DeclaredColumnExistsCheckReference,
    LogicalTypeCheckReference,
    PrimaryKeyCheckReference,
    RequiredCheckReference,
    SQLTableCheckReference,
    UniqueCheckReference,
)
from vowl.contracts.contract import Contract
from vowl.contracts.models import get_latest_version


def minimal_contract_data() -> dict:
    return {
        "apiVersion": get_latest_version(),
        "kind": "DataContract",
        "version": "1.0.0",
        "id": "test-contract",
        "status": "active",
        "schema": [
            {
                "name": "users",
                "properties": [
                    {"name": "id", "logicalType": "integer", "required": True},
                    {"name": "email", "logicalType": "string"},
                ],
            }
        ],
    }


def write_contract(tmp_path: Path, data: dict, name: str = "contract.yaml") -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def postgres_server(server: str, environment: str) -> dict:
    return {
        "server": server,
        "environment": environment,
        "type": "postgres",
        "host": f"{server}.example.internal",
        "port": 5432,
        "database": "analytics",
        "schema": "public",
    }


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class FakeBody:
    def __init__(self, payload: str) -> None:
        self._payload = payload.encode("utf-8")

    def read(self) -> bytes:
        return self._payload


def test_contract_init_raises_when_api_version_is_missing():
    with pytest.raises(ValueError, match="does not specify an apiVersion"):
        Contract({"kind": "DataContract"})


def test_contract_loads_valid_local_yaml(tmp_path: Path):
    path = write_contract(tmp_path, minimal_contract_data())

    contract = Contract.load(str(path))

    assert contract.get_api_version() == get_latest_version()
    assert contract.get_schema_names() == ["users"]


def test_contract_load_raises_for_missing_file(tmp_path: Path):
    missing = tmp_path / "missing.yaml"

    with pytest.raises(FileNotFoundError, match="Contract file not found"):
        Contract.load(str(missing))


def test_contract_load_raises_for_empty_file(tmp_path: Path):
    path = tmp_path / "empty.yaml"
    path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="Contract file is empty"):
        Contract.load(str(path))


def test_contract_load_raises_for_malformed_yaml(tmp_path: Path):
    path = tmp_path / "invalid.yaml"
    path.write_text("apiVersion: [", encoding="utf-8")

    with pytest.raises(yaml.YAMLError, match="Invalid contract YAML/JSON"):
        Contract.load(str(path))


def test_contract_load_wraps_file_read_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    path = tmp_path / "contract.yaml"
    path.write_text("placeholder", encoding="utf-8")
    original_open = builtins.open

    def fake_open(file, *args, **kwargs):
        if str(file) == str(path):
            raise OSError("permission denied")
        return original_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)

    with pytest.raises(OSError, match="Error reading"):
        Contract.load(str(path))


def test_contract_fetches_github_blob_urls_via_raw_url(monkeypatch: pytest.MonkeyPatch):
    called_urls: list[str] = []
    payload = yaml.safe_dump(minimal_contract_data())

    def fake_get(url: str, timeout: int):
        called_urls.append(url)
        return FakeResponse(payload)

    monkeypatch.setattr("requests.get", fake_get)

    contract = Contract.load("https://github.com/org/repo/blob/main/contract.yaml")

    assert contract.get_schema_names() == ["users"]
    assert called_urls == ["https://raw.githubusercontent.com/org/repo/main/contract.yaml"]


def test_contract_fetches_gitlab_blob_urls_via_raw_url(monkeypatch: pytest.MonkeyPatch):
    called_urls: list[str] = []
    payload = yaml.safe_dump(minimal_contract_data())

    def fake_get(url: str, timeout: int):
        called_urls.append(url)
        return FakeResponse(payload)

    monkeypatch.setattr("requests.get", fake_get)

    contract = Contract.load("https://gitlab.com/org/repo/-/blob/main/contract.yaml")

    assert contract.get_schema_names() == ["users"]
    assert called_urls == ["https://gitlab.com/org/repo/-/raw/main/contract.yaml"]


def test_contract_fetches_plain_http_urls_without_rewriting(monkeypatch: pytest.MonkeyPatch):
    called_urls: list[str] = []
    payload = yaml.safe_dump(minimal_contract_data())

    def fake_get(url: str, timeout: int):
        called_urls.append(url)
        return FakeResponse(payload)

    monkeypatch.setattr("requests.get", fake_get)

    contract = Contract.load("https://example.com/contracts/users.yaml")

    assert contract.get_schema_names() == ["users"]
    assert called_urls == ["https://example.com/contracts/users.yaml"]


def test_contract_http_fetch_failures_are_wrapped_as_io_errors(monkeypatch: pytest.MonkeyPatch):
    def fake_get(url: str, timeout: int):
        raise requests.RequestException("network down")

    monkeypatch.setattr("requests.get", fake_get)

    with pytest.raises(OSError, match="Error fetching contract from URL https://example.com/contracts/users.yaml"):
        Contract.load("https://example.com/contracts/users.yaml")


def test_contract_http_fetch_raises_import_error_when_requests_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
):
    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "requests":
            raise ImportError("requests is unavailable")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(ImportError, match="requests' package is required"):
        Contract._fetch_from_http_url("https://example.com/contracts/users.yaml")


def test_contract_fetches_from_s3(monkeypatch: pytest.MonkeyPatch):
    payload = yaml.safe_dump(minimal_contract_data())

    class FakeS3Client:
        def get_object(self, Bucket: str, Key: str):
            assert Bucket == "bucket-name"
            assert Key == "path/to/contract.yaml"
            return {"Body": FakeBody(payload)}

    monkeypatch.setattr("boto3.client", lambda service: FakeS3Client())

    contract = Contract.load("s3://bucket-name/path/to/contract.yaml")

    assert contract.get_schema_names() == ["users"]


def test_contract_fetch_from_s3_rejects_invalid_uri():
    with pytest.raises(ValueError, match="Invalid S3 path format"):
        Contract._fetch_from_s3_uri("s3://bucket-only")


def test_contract_fetch_from_s3_wraps_backend_failures(monkeypatch: pytest.MonkeyPatch):
    class FakeS3Client:
        def get_object(self, Bucket: str, Key: str):
            raise RuntimeError("access denied")

    monkeypatch.setattr("boto3.client", lambda service: FakeS3Client())

    with pytest.raises(OSError, match="Error fetching contract from S3 s3://bucket-name/path/to/contract.yaml"):
        Contract.load("s3://bucket-name/path/to/contract.yaml")


def test_contract_s3_fetch_raises_import_error_when_boto3_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
):
    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "boto3":
            raise ImportError("boto3 is unavailable")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(ImportError, match="boto3' package is required"):
        Contract._fetch_from_s3_uri("s3://bucket-name/path/to/contract.yaml")


def test_contract_get_schema_properties_and_version():
    contract = Contract(minimal_contract_data())

    assert contract.get_schema_properties()["name"] == "users"
    assert contract.get_version() == "1.0.0"


def test_contract_get_metadata_returns_expected_fields():
    contract = Contract(
        {
            **minimal_contract_data(),
            "description": {"purpose": "Contract description"},
        }
    )

    assert contract.get_metadata() == {
        "kind": "DataContract",
        "apiVersion": get_latest_version(),
        "version": "1.0.0",
        "status": "active",
        "id": "test-contract",
        "description": {"purpose": "Contract description"},
    }


def test_contract_get_schema_properties_returns_empty_dict_when_missing_schema():
    contract = Contract(
        {
            "apiVersion": get_latest_version(),
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "test-contract",
            "status": "active",
        }
    )

    assert contract.get_schema_properties() == {}


def test_contract_resolve_returns_value_for_single_match():
    contract = Contract(minimal_contract_data())

    assert contract.resolve("$.schema[0].name") == "users"


def test_contract_resolve_returns_none_for_no_matches():
    contract = Contract(minimal_contract_data())

    assert contract.resolve("$.schema[0].missing") is None


def test_contract_resolve_warns_and_returns_first_value_for_multiple_matches():
    contract = Contract(
        {
            **minimal_contract_data(),
            "schema": [
                {"name": "users", "properties": []},
                {"name": "orders", "properties": []},
            ],
        }
    )

    with pytest.warns(UserWarning, match="matched 2 elements"):
        assert contract.resolve("$.schema[*].name") == "users"


def test_contract_resolve_warns_and_returns_none_for_invalid_jsonpath():
    contract = Contract(minimal_contract_data())

    with pytest.warns(UserWarning, match="Error resolving JSONPath"):
        assert contract.resolve("$[") is None


def test_contract_resolve_parent_handles_normal_and_root_cases():
    contract = Contract(minimal_contract_data())

    assert contract.resolve_parent("$.schema[0].properties[1].quality[0]", 1) == "$.schema[0].properties[1]"
    assert contract.resolve_parent("$.schema[0].properties[1].quality[0]", 10) == "$"


def test_contract_get_servers_and_get_server_lookup_paths():
    contract = Contract(
        {
            **minimal_contract_data(),
            "servers": [
                postgres_server("uat-db", "uat"),
                postgres_server("prod-db", "prod"),
            ],
        }
    )

    assert len(contract.get_servers()) == 2
    assert contract.get_server()["server"] == "uat-db"
    assert contract.get_server("prod-db")["environment"] == "prod"
    assert contract.get_server("uat")["server"] == "uat-db"


def test_contract_get_server_raises_for_missing_servers_and_missing_match():
    contract_without_servers = Contract(minimal_contract_data())

    with pytest.raises(ValueError, match="No servers defined"):
        contract_without_servers.get_server()

    contract = Contract(
        {
            **minimal_contract_data(),
            "servers": [
                postgres_server("uat-db", "uat"),
            ],
        }
    )

    with pytest.raises(ValueError, match="No server found matching 'prod-db'"):
        contract.get_server("prod-db")


def test_contract_get_check_references_by_schema_covers_remaining_branch_paths(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr("vowl.contracts.contract.validate_contract", lambda data, version: None)
    contract = Contract(
        {
            "apiVersion": get_latest_version(),
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "test-contract",
            "status": "active",
            "schema": [
                {"properties": []},
                {
                    "name": "users",
                    "quality": [{"name": "table_check", "type": "sql", "query": "SELECT 1", "mustBe": 1}],
                    "properties": [
                        {
                            "name": "profile",
                            "logicalType": "object",
                            "logicalTypeOptions": {
                                "unsupportedOption": 10,
                                "minLength": None,
                            },
                            "quality": [
                                {"name": "column_check", "type": "sql", "query": "SELECT 1", "mustBe": 1}
                            ],
                        },
                        {
                            "name": "user_id",
                            "logicalType": "integer",
                            "required": True,
                            "unique": True,
                            "primaryKey": True,
                        },
                    ],
                },
            ],
        }
    )

    with pytest.warns(UserWarning) as warning_records:
        refs_by_schema = contract.get_check_references_by_schema()

    assert list(refs_by_schema) == ["users"]

    user_refs = refs_by_schema["users"]
    assert any(isinstance(ref, SQLTableCheckReference) for ref in user_refs)
    assert any(isinstance(ref, SQLColumnCheckReference) for ref in user_refs)
    assert any(isinstance(ref, DeclaredColumnExistsCheckReference) for ref in user_refs)
    assert any(isinstance(ref, LogicalTypeCheckReference) and ref.get_column_name() == "user_id" for ref in user_refs)
    assert any(isinstance(ref, RequiredCheckReference) for ref in user_refs)
    assert any(isinstance(ref, UniqueCheckReference) for ref in user_refs)
    assert any(isinstance(ref, PrimaryKeyCheckReference) for ref in user_refs)
    assert not any(
        isinstance(ref, LogicalTypeCheckReference) and ref.get_column_name() == "profile"
        for ref in user_refs
    )

    warning_messages = [str(record.message) for record in warning_records]
    assert any("No type check generated for 'profile' with logicalType 'object'" in message for message in warning_messages)
    assert any("Unsupported logicalTypeOptions key 'unsupportedOption'" in message for message in warning_messages)


def test_declared_column_exists_check_returns_error_when_input_column_is_missing(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr("vowl.contracts.contract.validate_contract", lambda data, version: None)
    contract = Contract(
        {
            "apiVersion": get_latest_version(),
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "test-contract",
            "status": "active",
            "schema": [
                {
                    "name": "users",
                    "properties": [
                        {"name": "id"},
                        {"name": "email"},
                    ],
                }
            ],
        }
    )

    adapter = DataSourceMapper().get_adapter(pd.DataFrame({"id": [1, 2]}), table_name="users")
    refs = contract.get_check_references_by_schema()["users"]
    results = adapter.run_checks(refs)

    results_by_name = {result.check_name: result for result in results}

    assert results_by_name["id_column_exists_check"].status == "PASSED"
    assert results_by_name["email_column_exists_check"].status == "ERROR"
    assert results_by_name["email_column_exists_check"].metadata["check_ref_type"] == "DeclaredColumnExistsCheckReference"
    assert results_by_name["email_column_exists_check"].metadata["check_path"] == "$.schema[0].properties[1].name"