"""Pytest helpers for golden-file validation."""

from __future__ import annotations

import contextvars
import os
import re
import sys
from pathlib import Path
from typing import Any

import narwhals as nw
import pandas as pd
import pytest

from vowl import validate as validate_module

_vowl = sys.modules["vowl"]

# Ensure PySpark workers use the same Python as the test driver
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


_CURRENT_NODEID = contextvars.ContextVar("current_nodeid", default="unknown")
_CURRENT_CALL_INDEX = contextvars.ContextVar("current_call_index", default=0)
_ALLOWED_ERROR_SUBSTRINGS: contextvars.ContextVar[tuple[str, ...]] = contextvars.ContextVar(
    "allowed_error_substrings", default=()
)

_EXPECTED_DIR = Path(__file__).parent / "expected_outputs"
_FAILED_DIR = Path(__file__).parent / "failed_outputs"


def _sanitize_nodeid(nodeid: str) -> str:
    sanitized = nodeid.replace("::", "__")
    sanitized = sanitized.replace(os.sep, "_")
    sanitized = re.sub(r"[^A-Za-z0-9_.-]", "_", sanitized)
    return sanitized


def _should_update_golden() -> bool:
    value = os.environ.get("UPDATE_GOLDENS", "").strip().lower()
    return value in {"1", "true", "yes"}


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    # Deduplicate column names (JOIN queries can produce e.g. employee_id, employee_id)
    cols = list(normalized.columns)
    seen: dict[str, int] = {}
    new_cols = []
    for c in cols:
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}.{seen[c]}")
        else:
            seen[c] = 0
            new_cols.append(c)
    normalized.columns = new_cols
    normalized = normalized.fillna("")
    normalized = normalized.astype(str)
    normalized = normalized.reindex(sorted(normalized.columns), axis=1)
    if len(normalized.columns) > 0:
        normalized = normalized.sort_values(
            by=list(normalized.columns),
            kind="mergesort",
        )
    return normalized.reset_index(drop=True)


def _golden_file_path(kind: str, suffix: str = "") -> Path:
    """Build the golden file path for the current test/call index."""
    nodeid = _CURRENT_NODEID.get()
    call_index = _CURRENT_CALL_INDEX.get()
    _EXPECTED_DIR.mkdir(parents=True, exist_ok=True)
    label = f"{kind}{suffix}"
    return _EXPECTED_DIR / f"{_sanitize_nodeid(nodeid)}__{call_index}__{label}.csv"


def _failure_output_dir(expected_path: Path) -> Path:
    """Build the failure artifact directory for the current test/call/artifact."""
    nodeid = _CURRENT_NODEID.get()
    call_index = _CURRENT_CALL_INDEX.get()
    prefix = f"{_sanitize_nodeid(nodeid)}__{call_index}__"
    label = expected_path.stem.removeprefix(prefix)
    failure_dir = _FAILED_DIR / _sanitize_nodeid(nodeid) / str(call_index) / label
    failure_dir.mkdir(parents=True, exist_ok=True)
    return failure_dir


def _write_failure_outputs(actual_df: pd.DataFrame, expected_path: Path) -> tuple[Path, Path]:
    """Write actual and expected outputs into a per-failure directory."""
    failure_dir = _failure_output_dir(expected_path)
    actual_output_path = failure_dir / "actual.csv"
    expected_output_path = failure_dir / "expected.csv"

    actual_df.to_csv(actual_output_path, index=False)
    if expected_path.exists():
        expected_df = pd.read_csv(expected_path, dtype=str, keep_default_na=False)
        expected_df = _normalize_df(expected_df)
        expected_df.to_csv(expected_output_path, index=False)

    return actual_output_path, expected_output_path


def _to_pandas(df: Any) -> pd.DataFrame:
    """Convert any narwhals-compatible DataFrame to pandas."""
    if isinstance(df, pd.DataFrame):
        return df
    if hasattr(df, 'to_pandas'):
        return df.to_pandas()
    return nw.from_native(df, eager_only=True).to_pandas()


def _compare_or_update_single(df_to_compare: Any, expected_path: Path) -> None:
    """Compare *df_to_compare* against *expected_path*, or create the golden file."""
    raw_df = _to_pandas(df_to_compare)
    actual_df = _normalize_df(raw_df)

    if _should_update_golden():
        # Preserve the original column order in golden files; only sort rows.
        ordered = raw_df.copy()
        ordered = ordered.fillna("")
        ordered = ordered.astype(str)
        if len(ordered.columns) > 0:
            ordered = ordered.sort_values(
                by=list(ordered.columns),
                kind="mergesort",
            )
        ordered.reset_index(drop=True).to_csv(expected_path, index=False)
        return

    if not expected_path.exists():
        actual_path, expected_output_path = _write_failure_outputs(actual_df, expected_path)
        raise AssertionError(
            f"Missing golden file: {expected_path}. "
            f"Wrote actual output to: {actual_path}. "
            f"Expected output path reserved at: {expected_output_path}. "
            "Run with UPDATE_GOLDENS=1 to generate it."
        )

    expected_df = pd.read_csv(expected_path, dtype=str, keep_default_na=False)
    expected_df = _normalize_df(expected_df)

    try:
        pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False)
    except AssertionError as exc:
        actual_path, expected_output_path = _write_failure_outputs(actual_df, expected_path)
        raise AssertionError(
            f"{exc}\nActual output written to: {actual_path}\nExpected output written to: {expected_output_path}\nExpected golden: {expected_path}"
        ) from exc


def _is_expected_error(cr: Any) -> bool:
    """Return True if this ERROR check result is expected and should not fail the test."""
    if cr.metadata.get("contract_definition", {}).get("type") == "text":
        return True
    for substring in _ALLOWED_ERROR_SUBSTRINGS.get():
        if substring in (cr.details or ""):
            return True
    return False


def _assert_no_unexpected_errors(result: Any) -> None:
    """Fail the test if any check result has an unexpected ERROR status.

    Always-allowed: type=text checks.
    Per-test opt-in: tests can register error message substrings via
    ``_ALLOWED_ERROR_SUBSTRINGS`` (e.g. MSSQL lacking REGEXP_LIKE).
    """
    if not hasattr(result, "check_results"):
        return
    unexpected_errors = [
        cr for cr in result.check_results
        if cr.status == "ERROR" and not _is_expected_error(cr)
    ]
    if unexpected_errors:
        details = "\n".join(
            f"  {cr.check_name}: {cr.details}" for cr in unexpected_errors
        )
        raise AssertionError(
            f"Validation returned unexpected ERROR checks:\n{details}"
        )


def _compare_or_update_golden(result: Any) -> None:
    # Bump call index once per validate_data invocation
    call_index = _CURRENT_CALL_INDEX.get() + 1
    _CURRENT_CALL_INDEX.set(call_index)

    _assert_no_unexpected_errors(result)

    # --- per-check output (one golden per check) ---
    output_dfs = None
    if hasattr(result, "get_output_dfs"):
        output_dfs = result.get_output_dfs()

    non_empty_dfs = {k: v for k, v in output_dfs.items() if len(v) > 0} if output_dfs else {}

    if non_empty_dfs:
        for check_name, df in non_empty_dfs.items():
            safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", check_name)
            _compare_or_update_single(df, _golden_file_path("output", f"__{safe_name}"))

    # Always compare check_results (not just when there are no output DFs),
    # so stale goldens with ERROR statuses are caught.
    if hasattr(result, "get_check_results_df"):
        df_to_compare = _to_pandas(result.get_check_results_df())
        if "execution_time_ms" in df_to_compare.columns:
            df_to_compare = df_to_compare.drop(columns=["execution_time_ms"])
        _compare_or_update_single(df_to_compare, _golden_file_path("check_results"))

    # --- consolidated output (one golden per table key) ---
    if hasattr(result, "get_consolidated_output_dfs"):
        consolidated = result.get_consolidated_output_dfs()
        for table_key, cdf in consolidated.items():
            safe_key = re.sub(r"[^A-Za-z0-9_.-]", "_", table_key)
            _compare_or_update_single(cdf, _golden_file_path("consolidated", f"__{safe_key}"))


_ORIGINAL_VALIDATE_DATA = validate_module.validate_data


def _wrapped_validate_data(*args: Any, **kwargs: Any):
    result = _ORIGINAL_VALIDATE_DATA(*args, **kwargs)
    _compare_or_update_golden(result)
    return result


@pytest.fixture(autouse=True)
def _golden_validate_data(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest):
    token_nodeid = _CURRENT_NODEID.set(request.node.nodeid)
    token_index = _CURRENT_CALL_INDEX.set(0)

    monkeypatch.setattr(validate_module, "validate_data", _wrapped_validate_data)
    monkeypatch.setattr(_vowl, "validate_data", _wrapped_validate_data)

    # Ensure module-level references in test modules use the wrapped function
    if hasattr(request.module, "validate_data"):
        monkeypatch.setattr(request.module, "validate_data", _wrapped_validate_data, raising=False)

    yield

    _CURRENT_NODEID.reset(token_nodeid)
    _CURRENT_CALL_INDEX.reset(token_index)
