"""Where ``ValidationResult.save()`` writes: a local folder or a remote URI.

Local paths keep the plain ``pathlib`` behaviour. A URI such as ``s3://``,
``gs://``, ``abfs://``, ``hdfs://`` or ``file://`` goes through pyarrow's own
filesystems, which pyarrow (a core dependency) already ships, so remote saving
needs no extra install. Each filesystem finds credentials the usual way for its
cloud (environment variables, ``~/.aws``, an IAM role, and so on).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as _pa_csv
import pyarrow.fs as _pa_fs
import pyarrow.parquet as _pa_pq

_URI_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")

#: Filesystems with no real folders. ``create_dir`` on these only adds empty
#: marker objects (and on S3 can try to create the bucket), so it is skipped.
_OBJECT_STORES = frozenset({"s3", "gcs", "abfs"})


def is_uri(location: str) -> bool:
    """True when *location* starts with a URI scheme like ``s3://``."""
    return bool(_URI_SCHEME_RE.match(location))


def _filesystem_from_uri(uri: str) -> tuple[Any, str]:
    return _pa_fs.FileSystem.from_uri(uri)


def _is_object_store(filesystem: Any) -> bool:
    # A Python-implemented filesystem reports its type as "py::<name>".
    return filesystem.type_name.removeprefix("py::") in _OBJECT_STORES


class OutputDir:
    """One output folder, local or remote, that files are written into by name."""

    def __init__(self, location: str, filesystem: Any | None = None) -> None:
        self._location = str(location)
        self._fs: Any | None = None
        self._base = ""

        if filesystem is None and not is_uri(self._location):
            self._local = Path(self._location)
            self._local.mkdir(parents=True, exist_ok=True)
            return

        if filesystem is not None:
            self._fs = filesystem
            self._base = _URI_SCHEME_RE.sub("", self._location)
        else:
            try:
                self._fs, self._base = _filesystem_from_uri(self._location)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                raise ValueError(
                    f"Can't save to {self._location!r}: {exc}. vowl saves to s3://, gs://, "
                    "abfs://, hdfs:// and file:// through pyarrow's filesystems. To set one up "
                    "yourself (for example a custom endpoint or explicit credentials), pass "
                    "filesystem=."
                ) from exc

        self._base = self._base.rstrip("/")
        if not _is_object_store(self._fs):
            self._fs.create_dir(self._base, recursive=True)

    def subdir(self, name: str) -> OutputDir:
        """The folder *name* inside this one, on the same filesystem, created if needed."""
        if self._fs is None:
            return OutputDir(str(self._local / name))
        sub = OutputDir.__new__(OutputDir)
        sub._location = f"{self._location.rstrip('/')}/{name}"
        sub._fs = self._fs
        sub._base = self._remote_path(name)
        if not _is_object_store(self._fs):
            self._fs.create_dir(sub._base, recursive=True)
        return sub

    def display(self, name: str) -> str:
        """The path shown to the user for the file *name*."""
        if self._fs is None:
            return str(self._local / name)
        return f"{self._location.rstrip('/')}/{name}"

    def _remote_path(self, name: str) -> str:
        return f"{self._base}/{name}" if self._base else name

    def write_csv(self, name: str, table: pa.Table, **kwargs: Any) -> str:
        if self._fs is None:
            _pa_csv.write_csv(table, str(self._local / name), **kwargs)
        else:
            with self._fs.open_output_stream(self._remote_path(name)) as stream:
                _pa_csv.write_csv(table, stream, **kwargs)
        return self.display(name)

    def write_parquet(self, name: str, table: pa.Table, **kwargs: Any) -> str:
        if self._fs is None:
            _pa_pq.write_table(table, str(self._local / name), **kwargs)
        else:
            _pa_pq.write_table(table, self._remote_path(name), filesystem=self._fs, **kwargs)
        return self.display(name)

    def write_text(self, name: str, text: str) -> str:
        if self._fs is None:
            with open(self._local / name, "w") as f:
                f.write(text)
        else:
            with self._fs.open_output_stream(self._remote_path(name)) as stream:
                stream.write(text.encode("utf-8"))
        return self.display(name)


def split_file_location(filepath: str) -> tuple[str, str]:
    """Split a file path or URI into its folder and file name."""
    filepath = str(filepath)
    if is_uri(filepath):
        folder, _, name = filepath.rpartition("/")
        return folder, name
    path = Path(filepath)
    return str(path.parent), path.name
