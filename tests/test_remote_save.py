"""Tests for saving results to a remote location (``s3://`` and friends).

Everything runs offline. pyarrow's in-memory ``_MockFileSystem`` stands in for
the remote store, and ``_output_dir._filesystem_from_uri`` is patched where a real cloud
filesystem would otherwise be built.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.fs as pa_fs
import pyarrow.parquet as pa_pq
import pytest
from pyarrow._fs import _MockFileSystem

from vowl import validate_data as _validate_data
from vowl.contracts.contract import Contract
from vowl.validation import _output_dir
from vowl.validation.result import ValidationResult

# Keep the real function. conftest's autouse golden fixture swaps the public
# ``validate_data`` attribute, and these tests are not golden-file tests.
_run_validation = _validate_data
del _validate_data


def _contract() -> Contract:
    return Contract(
        {
            "apiVersion": "v3.1.0",
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "orders-contract",
            "status": "active",
            "schema": [{"name": "orders", "properties": [{"name": "order_id", "required": True}]}],
        }
    )


@pytest.fixture
def result() -> ValidationResult:
    """A finished result with one failed row (a NULL ``order_id``)."""
    return _run_validation(contract=_contract(), df=pd.DataFrame({"order_id": [1, None, 3]}))


def _files(fs, root: str) -> set[str]:
    infos = fs.get_file_info(pa_fs.FileSelector(root, recursive=True))
    return {i.path for i in infos if i.type == pa_fs.FileType.File}


def _read_text(fs, path: str) -> str:
    with fs.open_input_stream(path) as stream:
        return stream.read().decode("utf-8")


@pytest.fixture
def mock_s3(monkeypatch):
    """Route every URI to one in-memory filesystem, and record the URIs asked for."""
    fs = _MockFileSystem()
    seen: list[str] = []

    def from_uri(uri):
        seen.append(uri)
        return fs, uri.split("://", 1)[1]

    monkeypatch.setattr(_output_dir, "_filesystem_from_uri", from_uri)
    return SimpleNamespace(fs=fs, seen=seen)


# --------------------------------------------------------------------------- #
# save()
# --------------------------------------------------------------------------- #


def test_save_to_s3_uri_writes_into_the_remote_filesystem(result, mock_s3, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    result.save("s3://bucket/run1/", prefix="dq", output_mode="annotated")

    assert mock_s3.seen == ["s3://bucket/run1/"]
    assert _files(mock_s3.fs, "bucket") == {
        "bucket/run1/dq_check_results.csv",
        "bucket/run1/dq_orders_annotated.csv",
        "bucket/run1/dq_summary.json",
        "bucket/run1/dq_dq_metrics.json",
    }
    summary = json.loads(_read_text(mock_s3.fs, "bucket/run1/dq_summary.json"))
    assert summary == json.loads(json.dumps(result.summary, default=str))
    annotated = pa_csv.read_csv(pa.py_buffer(_read_text(mock_s3.fs, "bucket/run1/dq_orders_annotated.csv").encode()))
    assert annotated.num_rows == 3

    # The original bug: Path("s3://...") quietly made a local "s3:" folder.
    assert list(tmp_path.iterdir()) == []

    out = capsys.readouterr().out
    assert "s3://bucket/run1/dq_check_results.csv" in out
    assert "s3://bucket/run1/dq_summary.json" in out


def test_save_with_explicit_filesystem_and_plain_path(result):
    fs = _MockFileSystem()

    result.save("bucket/run2", prefix="dq", output_mode="annotated", filesystem=fs)

    assert "bucket/run2/dq_summary.json" in _files(fs, "bucket")


def test_save_with_explicit_filesystem_strips_the_uri_scheme(result, monkeypatch):
    def fail(uri):
        raise AssertionError("from_uri must not be called when filesystem= is passed")

    monkeypatch.setattr(_output_dir, "_filesystem_from_uri", fail)
    fs = _MockFileSystem()

    result.save("s3://bucket/run3", prefix="dq", output_mode="annotated", filesystem=fs)

    assert "bucket/run3/dq_check_results.csv" in _files(fs, "bucket")


def test_save_to_file_uri_writes_local_files(result, tmp_path):
    result.save(f"file://{tmp_path}/out", prefix="dq", output_mode="annotated")

    assert (tmp_path / "out" / "dq_summary.json").exists()
    assert (tmp_path / "out" / "dq_orders_annotated.csv").exists()


def test_save_to_local_path_is_unchanged(result, tmp_path, capsys):
    result.save(str(tmp_path / "local"), prefix="dq", output_mode="annotated")

    assert (tmp_path / "local" / "dq_check_results.csv").exists()
    assert str(tmp_path / "local" / "dq_summary.json") in capsys.readouterr().out


def test_unknown_scheme_raises_a_clear_error(result):
    with pytest.raises(ValueError, match=r"Can't save to 'nope://x'.*pass filesystem="):
        result.save("nope://x", output_mode="annotated")


# --------------------------------------------------------------------------- #
# Object stores and folders
# --------------------------------------------------------------------------- #


class _SpyHandler(pa_fs.FileSystemHandler):
    """Wraps the mock filesystem, reports a chosen type name, records create_dir."""

    def __init__(self, type_name: str) -> None:
        self.inner = _MockFileSystem()
        self.type_name = type_name
        self.created: list[str] = []

    def get_type_name(self):
        return self.type_name

    def create_dir(self, path, recursive):
        self.created.append(path)
        self.inner.create_dir(path, recursive=recursive)

    def open_output_stream(self, path, metadata):
        # Object stores accept a key without its "folder" existing.
        self.inner.create_dir(path.rpartition("/")[0], recursive=True)
        return self.inner.open_output_stream(path, metadata=metadata)

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other

    def get_file_info(self, paths):
        return self.inner.get_file_info(paths)

    def get_file_info_selector(self, selector):
        return self.inner.get_file_info(selector)

    def normalize_path(self, path):
        return path

    def delete_dir(self, path): ...
    def delete_dir_contents(self, path, missing_dir_ok=False): ...
    def delete_root_dir_contents(self): ...
    def delete_file(self, path): ...
    def move(self, src, dest): ...
    def copy_file(self, src, dest): ...
    def open_input_stream(self, path):
        return self.inner.open_input_stream(path)

    def open_input_file(self, path):
        return self.inner.open_input_file(path)

    def open_append_stream(self, path, metadata): ...


@pytest.mark.parametrize(("type_name", "expect_create_dir"), [("s3", False), ("gcs", False), ("hdfs", True)])
def test_create_dir_is_skipped_only_for_object_stores(result, type_name, expect_create_dir):
    handler = _SpyHandler(type_name)

    result.save("bucket/run4", prefix="dq", output_mode="annotated", filesystem=pa_fs.PyFileSystem(handler))

    assert bool(handler.created) is expect_create_dir
    assert "bucket/run4/dq_summary.json" in _files(handler.inner, "bucket")


# --------------------------------------------------------------------------- #
# save_dataframe()
# --------------------------------------------------------------------------- #


@pytest.mark.filterwarnings("ignore:ValidationResult.save_dataframe:DeprecationWarning")
@pytest.mark.parametrize("file_format", ["csv", "parquet", "json"])
def test_save_dataframe_to_s3_uri(mock_s3, file_format, capsys):
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    uri = f"s3://bucket/frames/out.{file_format}"

    ValidationResult.save_dataframe(df, uri, file_format)

    path = f"bucket/frames/out.{file_format}"
    assert mock_s3.seen == ["s3://bucket/frames"]
    if file_format == "parquet":
        with mock_s3.fs.open_input_file(path) as f:
            assert pa_pq.read_table(f).num_rows == 2
    elif file_format == "json":
        lines = _read_text(mock_s3.fs, path).splitlines()
        assert json.loads(lines[0]) == {"id": 1, "value": "a"}
    else:
        assert _read_text(mock_s3.fs, path).splitlines()[0] == '"id","value"'
    assert f"Saved to: {uri}" in capsys.readouterr().out


@pytest.mark.filterwarnings("ignore:ValidationResult.save_dataframe:DeprecationWarning")
def test_save_dataframe_rejects_an_unknown_format_before_writing(mock_s3):
    with pytest.raises(ValueError, match="Unsupported format"):
        ValidationResult.save_dataframe(pd.DataFrame({"id": [1]}), "s3://bucket/out.txt", "txt")
    assert mock_s3.seen == []
