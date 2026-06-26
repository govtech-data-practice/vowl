"""Tests for annotated output (full in-scope table with failed rows marked).

Covers ``ValidationResult.get_annotated_output`` and the ``output_mode`` wiring
on ``save()`` / ``ValidationConfig`` introduced in the full-table-output plan.
"""

from __future__ import annotations

import json
import logging

import narwhals as nw
import pyarrow as pa
import pytest

from vowl.config import ValidationConfig
from vowl.executors.base import CheckResult
from vowl.validation.result import ValidationResult

# ---------------------------------------------------------------------------
# Fakes / builders
# ---------------------------------------------------------------------------


class _FakeAdapter:
    """Adapter stub whose export returns a preset Arrow table (or raises)."""

    def __init__(self, table: pa.Table | None, *, error: Exception | None = None):
        self._table = table
        self._error = error

    def export_table_as_arrow(self, schema_name: str) -> pa.Table:
        if self._error is not None:
            raise self._error
        return self._table


class _FakeMultiAdapter:
    """MultiSourceAdapter stub: maps schema name -> adapter (or None)."""

    def __init__(self, adapters: dict[str, _FakeAdapter | None]):
        self._adapters = adapters

    def get_adapter(self, schema_name: str):
        return self._adapters.get(schema_name)


class _FakeContract:
    contract_data: dict = {"schema": []}

    def get_api_version(self) -> str:
        return "v1"

    def get_metadata(self) -> dict:
        return {"id": "test-contract"}


def _make_check(
    name: str,
    schema_name: str,
    *,
    status: str = "FAILED",
    failed_rows: pa.Table | None = None,
    failed_rows_count: int | None = None,
    supports_row_level_output: bool = True,
    tables_in_query: str | None = None,
    target: str | None = None,
    check_definition: dict | None = None,
) -> CheckResult:
    fr = nw.from_native(failed_rows, eager_only=True) if failed_rows is not None else None
    count = failed_rows_count
    if count is None:
        count = failed_rows.num_rows if failed_rows is not None else 0
    meta: dict = {"schema_name": schema_name}
    if tables_in_query is not None:
        meta["tables_in_query"] = tables_in_query
    if target is not None:
        meta["target"] = target
    if check_definition is not None:
        meta["check_definition"] = check_definition
    return CheckResult(
        check_name=name,
        status=status,
        details="",
        failed_rows=fr,
        failed_rows_count=count,
        supports_row_level_output=supports_row_level_output,
        metadata=meta,
    )


def _make_result(
    check_results: list[CheckResult],
    adapters: dict[str, _FakeAdapter | None],
    *,
    config: ValidationConfig | None = None,
) -> ValidationResult:
    schema_names = list(adapters.keys())
    summary = {
        "validation_summary": {
            "total_checks": len(check_results),
            "passed": sum(c.status == "PASSED" for c in check_results),
            "failed": sum(c.status == "FAILED" for c in check_results),
            "errors": sum(c.status == "ERROR" for c in check_results),
            "total_execution_time_ms": 0.0,
            "success_rate": 100.0,
        }
    }
    return ValidationResult(
        summary=summary,
        check_results=check_results,
        contract=_FakeContract(),
        multi_adapter=_FakeMultiAdapter(adapters),
        schema_names=schema_names,
        config=config,
    )


def _row(df: nw.DataFrame) -> list[dict]:
    return df.to_arrow().to_pylist()


def _parse_check_info(cell: str | None) -> list[dict] | None:
    """Parse a ``check_info`` cell into a list of objects (or ``None``)."""
    if cell is None:
        return None
    parsed = json.loads(cell)
    assert isinstance(parsed, list)
    for item in parsed:
        assert isinstance(item, dict)  # uniform array-of-objects, never bare strings
    return parsed


def _check_names_of(cell: str | None) -> list[str]:
    """Ordered list of ``check_name`` values from a ``check_info`` cell."""
    parsed = _parse_check_info(cell)
    return [item["check_name"] for item in parsed] if parsed else []


# ---------------------------------------------------------------------------
# Shape / reserved keys
# ---------------------------------------------------------------------------


class TestShape:
    def test_reserved_keys_always_present(self):
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        result = _make_result([], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        assert set(out.keys()) == {"annotated", "residues"}
        assert "orders" in out["annotated"]
        assert out["residues"] == {}

    def test_all_null_check_info_when_no_failures(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        result = _make_result([], {"orders": _FakeAdapter(full)})
        annotated = result.get_annotated_output()["annotated"]["orders"]
        assert "check_info" in annotated.columns
        assert annotated["check_info"].to_list() == [None, None]
        # The legacy check_ids column is gone from annotated tables.
        assert "check_ids" not in annotated.columns
        # No tables_in_query column on annotated entries.
        assert "tables_in_query" not in annotated.columns


# ---------------------------------------------------------------------------
# Annotation correctness
# ---------------------------------------------------------------------------


class TestAnnotation:
    def test_failed_rows_marked_passing_rows_null(self):
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        failed = pa.table({"id": [2], "name": ["b"]})
        check = _make_check("not_null_name", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})

        annotated = result.get_annotated_output()["annotated"]["orders"]
        rows = {r["id"]: r["check_info"] for r in _row(annotated)}
        assert _check_names_of(rows[2]) == ["not_null_name"]
        # Default "names" preset: array of {check_name} objects only.
        assert _parse_check_info(rows[2]) == [{"check_name": "not_null_name"}]
        assert rows[1] is None and rows[3] is None

    def test_multiple_checks_one_item_per_check(self):
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        c1 = _make_check("check_a", "orders", failed_rows=pa.table({"id": [2], "name": ["b"]}))
        c2 = _make_check("check_b", "orders", failed_rows=pa.table({"id": [2], "name": ["b"]}))
        result = _make_result([c1, c2], {"orders": _FakeAdapter(full)})

        annotated = result.get_annotated_output()["annotated"]["orders"]
        rows = {r["id"]: r["check_info"] for r in _row(annotated)}
        # One object per failing check, ordered/deduped.
        assert _check_names_of(rows[2]) == ["check_a", "check_b"]
        assert rows[1] is None

    def test_no_duplicate_rows_introduced(self):
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        check = _make_check("c", "orders", failed_rows=pa.table({"id": [2], "name": ["b"]}))
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        annotated = result.get_annotated_output()["annotated"]["orders"]
        assert annotated.to_arrow().num_rows == full.num_rows

    def test_failed_null_bearing_row_is_annotated(self):
        # The core false-negative regression: a row with a NULL in a join column
        # that FAILED must be annotated, not hidden.
        full = pa.table({"id": [1, 2, 3], "name": ["a", None, "c"]})
        failed = pa.table({"id": [2], "name": pa.array([None], type=pa.string())})
        check = _make_check("not_null", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})

        annotated = result.get_annotated_output()["annotated"]["orders"]
        rows = {r["id"]: r["check_info"] for r in _row(annotated)}
        assert _check_names_of(rows[2]) == ["not_null"]

    def test_clean_null_bearing_row_not_cross_annotated(self):
        # Two rows share the NULL pattern; only one failed. The clean one stays null.
        full = pa.table({"id": [1, 2], "name": [None, None]})
        failed = pa.table({"id": [2], "name": pa.array([None], type=pa.string())})
        check = _make_check("not_null", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})

        annotated = result.get_annotated_output()["annotated"]["orders"]
        rows = {r["id"]: r["check_info"] for r in _row(annotated)}
        assert rows[1] is None
        assert _check_names_of(rows[2]) == ["not_null"]

    def test_multiple_null_columns(self):
        full = pa.table({"a": [1, 2], "b": ["x", None], "c": [None, None]})
        failed = pa.table({"a": [2], "b": pa.array([None], type=pa.string()), "c": pa.array([None], type=pa.string())})
        check = _make_check("c", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        annotated = result.get_annotated_output()["annotated"]["orders"]
        rows = {r["a"]: r["check_info"] for r in _row(annotated)}
        assert rows[1] is None
        assert _check_names_of(rows[2]) == ["c"]

    def test_original_nulls_preserved_in_output(self):
        full = pa.table({"id": [1, 2], "name": [None, "b"]})
        failed = pa.table({"id": [2], "name": ["b"]})
        check = _make_check("c", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        annotated = result.get_annotated_output()["annotated"]["orders"]
        rows = {r["id"]: r["name"] for r in _row(annotated)}
        assert rows[1] is None  # true NULL, not a fill placeholder

    def test_untyped_null_join_column_does_not_crash(self):
        # Failed-rows frame with a pa.null()-typed join column (Candidate B immune).
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        failed = pa.table({"id": [2], "name": pa.array([None], type=pa.null())})
        check = _make_check("c", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        # Must complete without ArrowInvalid.
        annotated = result.get_annotated_output()["annotated"]["orders"]
        assert annotated.to_arrow().num_rows == 2

    def test_duplicate_failed_rows_deduplicated(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        failed = pa.table({"id": [2, 2], "name": ["b", "b"]})  # duplicate
        check = _make_check("c", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        annotated = result.get_annotated_output()["annotated"]["orders"]
        rows = {r["id"]: r["check_info"] for r in _row(annotated)}
        assert rows[1] is None
        assert _check_names_of(rows[2]) == ["c"]

    def test_duplicate_full_table_rows_all_marked(self):
        # N byte-identical rows, one failed -> all N marked (safe over-flagging).
        full = pa.table({"id": [2, 2, 1], "name": ["b", "b", "a"]})
        failed = pa.table({"id": [2], "name": ["b"]})
        check = _make_check("c", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        annotated = result.get_annotated_output()["annotated"]["orders"]
        marks = [_check_names_of(r["check_info"]) for r in _row(annotated)]
        assert marks == [["c"], ["c"], []]


# ---------------------------------------------------------------------------
# Match-count invariant warning
# ---------------------------------------------------------------------------


class TestMatchCountWarning:
    def test_warns_when_failed_row_unmatched(self, caplog):
        # Failed row that does NOT exist in the full table -> count mismatch.
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        failed = pa.table({"id": [99], "name": ["zzz"]})
        check = _make_check("c", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        with caplog.at_level(logging.WARNING):
            result.get_annotated_output()
        assert any("count mismatch" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------


class TestEligibility:
    def test_cross_table_check_is_residue(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        check = _make_check(
            "join_check",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"]}),
            tables_in_query="orders, customers",
        )
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        # Annotated table present but unmarked; check survives in residues.
        assert out["annotated"]["orders"]["check_info"].to_list() == [None, None]
        assert any("join_check" in self._check_names(df) for df in out["residues"].values())

    def test_aggregation_check_is_residue(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        check = _make_check(
            "agg_check",
            "orders",
            failed_rows=pa.table({"cnt": [5]}),
            supports_row_level_output=False,
        )
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        assert out["annotated"]["orders"]["check_info"].to_list() == [None, None]
        assert any("agg_check" in self._check_names(df) for df in out["residues"].values())

    def test_column_subset_check_is_residue(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        # Failed rows have only a subset of columns -> not mergeable.
        check = _make_check("subset", "orders", failed_rows=pa.table({"id": [2]}))
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        assert out["annotated"]["orders"]["check_info"].to_list() == [None, None]
        assert any("subset" in self._check_names(df) for df in out["residues"].values())

    def test_error_check_excluded_without_fetch(self):
        # An ERROR check must never have its failed_rows fetched by eligibility.
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})

        class _Boom(CheckResult):
            @property
            def failed_rows(self):  # pragma: no cover - must not be reached
                raise AssertionError("failed_rows fetched for ERROR check")

        boom = _Boom(
            check_name="errored",
            status="ERROR",
            details="boom",
            supports_row_level_output=True,
            metadata={"schema_name": "orders"},
        )
        result = _make_result([boom], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        assert out["annotated"]["orders"]["check_info"].to_list() == [None, None]

    @staticmethod
    def _check_names(df: nw.DataFrame) -> set[str]:
        return ValidationResult._check_names_in_entry(df)


# ---------------------------------------------------------------------------
# Subsumption (the critical fix)
# ---------------------------------------------------------------------------


class TestSubsumption:
    def test_nonmergeable_single_table_check_kept(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        # Single-table, column-subset (non-mergeable), bare schema key "orders".
        check = _make_check("subset", "orders", failed_rows=pa.table({"id": [2]}))
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        residue_checks = set()
        for df in out["residues"].values():
            residue_checks |= ValidationResult._check_names_in_entry(df)
        assert "subset" in residue_checks

    def test_mergeable_plus_nonmergeable_no_duplication(self):
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        mergeable = _make_check("full_row", "orders", failed_rows=pa.table({"id": [2], "name": ["b"]}))
        nonmergeable = _make_check("subset", "orders", failed_rows=pa.table({"id": [3]}))
        result = _make_result([mergeable, nonmergeable], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()

        # Mergeable check is on the annotated table.
        rows = {r["id"]: r["check_info"] for r in _row(out["annotated"]["orders"])}
        assert _check_names_of(rows[2]) == ["full_row"]

        # Non-mergeable survives in residues; mergeable is NOT duplicated there.
        residue_checks = set()
        for df in out["residues"].values():
            residue_checks |= ValidationResult._check_names_in_entry(df)
        assert "subset" in residue_checks
        assert "full_row" not in residue_checks


# ---------------------------------------------------------------------------
# Adapter failure paths
# ---------------------------------------------------------------------------


class TestFetchFailurePaths:
    def test_none_adapter_skips_schema_keeps_residue(self):
        check = _make_check("subset", "orders", failed_rows=pa.table({"id": [2]}))
        result = _make_result([check], {"orders": None})
        out = result.get_annotated_output()
        assert "orders" not in out["annotated"]
        residue_checks = set()
        for df in out["residues"].values():
            residue_checks |= ValidationResult._check_names_in_entry(df)
        assert "subset" in residue_checks

    def test_not_implemented_export_skips_schema(self):
        adapter = _FakeAdapter(None, error=NotImplementedError("nope"))
        check = _make_check("c", "orders", failed_rows=pa.table({"id": [2]}))
        result = _make_result([check], {"orders": adapter})
        out = result.get_annotated_output()
        assert "orders" not in out["annotated"]

    def test_runtime_export_error_cached_and_skipped(self):
        adapter = _FakeAdapter(None, error=RuntimeError("backend down"))
        result = _make_result([], {"orders": adapter})
        out = result.get_annotated_output()
        assert "orders" not in out["annotated"]
        # Cached as None -> second call doesn't re-raise / re-fetch.
        assert result._full_table_cache["orders"] is None
        out2 = result.get_annotated_output()
        assert "orders" not in out2["annotated"]


# ---------------------------------------------------------------------------
# max_failed_rows truncation guard
# ---------------------------------------------------------------------------


class TestTruncationGuard:
    def test_mergeable_truncated_raises(self):
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        # Fetched 1 row but true count is 2 -> truncated.
        check = _make_check(
            "c",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"]}),
            failed_rows_count=2,
        )
        result = _make_result(
            [check],
            {"orders": _FakeAdapter(full)},
            config=ValidationConfig(max_failed_rows=1),
        )
        with pytest.raises(ValueError, match="annotated output"):
            result.get_annotated_output()

    def test_uncapped_same_scenario_does_not_raise(self):
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        check = _make_check(
            "c",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"]}),
            failed_rows_count=2,
        )
        result = _make_result(
            [check],
            {"orders": _FakeAdapter(full)},
            config=ValidationConfig(max_failed_rows=-1),
        )
        out = result.get_annotated_output()  # no raise
        assert "orders" in out["annotated"]

    def test_nonmergeable_truncated_does_not_raise(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        check = _make_check(
            "subset",
            "orders",
            failed_rows=pa.table({"id": [2]}),
            failed_rows_count=5,
            supports_row_level_output=True,
        )
        result = _make_result(
            [check],
            {"orders": _FakeAdapter(full)},
            config=ValidationConfig(max_failed_rows=1),
        )
        out = result.get_annotated_output()  # no raise: non-mergeable -> residue
        residue_checks = set()
        for df in out["residues"].values():
            residue_checks |= ValidationResult._check_names_in_entry(df)
        assert "subset" in residue_checks


# ---------------------------------------------------------------------------
# check_info presets (names / summary / full)
# ---------------------------------------------------------------------------


class TestCheckInfoPresets:
    _CHECK_DEF = {
        "name": "name_valid_check",
        "type": "sql",
        "dimension": "accuracy",
        "description": "name must be valid",
        "query": "SELECT COUNT(*) FROM orders WHERE ...",
        "mustBe": 0,
        "tags": ["vowl_generated_check"],
    }

    def _result(self, **check_kwargs):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        check = _make_check(
            "c",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"]}),
            target="orders.name",
            **check_kwargs,
        )
        return _make_result([check], {"orders": _FakeAdapter(full)})

    def test_names_preset_is_default(self):
        result = self._result()
        annotated = result.get_annotated_output()["annotated"]["orders"]
        rows = {r["id"]: r["check_info"] for r in _row(annotated)}
        assert rows[1] is None
        # Only check_name key; no dimension/tags/target.
        assert _parse_check_info(rows[2]) == [{"check_name": "c"}]

    def test_summary_preset_shape(self):
        result = self._result(check_definition=self._CHECK_DEF)
        annotated = result.get_annotated_output(check_info="summary")["annotated"]["orders"]
        rows = {r["id"]: r["check_info"] for r in _row(annotated)}
        assert rows[1] is None
        assert _parse_check_info(rows[2]) == [
            {
                "check_name": "c",
                "dimension": "accuracy",
                "tags": ["vowl_generated_check"],
                "target": "orders.name",
            }
        ]

    def test_summary_preset_tolerates_missing_definition(self):
        # No check_definition -> dimension/tags fall back to JSON null, never raises.
        result = self._result()
        annotated = result.get_annotated_output(check_info="summary")["annotated"]["orders"]
        rows = {r["id"]: r["check_info"] for r in _row(annotated)}
        assert _parse_check_info(rows[2]) == [
            {"check_name": "c", "dimension": None, "tags": None, "target": "orders.name"}
        ]

    def test_full_preset_includes_definition_plus_check_name_and_target(self):
        result = self._result(check_definition=self._CHECK_DEF)
        annotated = result.get_annotated_output(check_info="full")["annotated"]["orders"]
        rows = {r["id"]: r["check_info"] for r in _row(annotated)}
        item = _parse_check_info(rows[2])[0]
        # Full check_definition is present...
        for key, value in self._CHECK_DEF.items():
            assert item[key] == value
        # ...plus the always-populated check_name id and target.
        assert item["check_name"] == "c"
        assert item["target"] == "orders.name"

    def test_preset_resolves_from_config(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        check = _make_check(
            "c",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"]}),
            target="orders.name",
            check_definition=self._CHECK_DEF,
        )
        result = _make_result(
            [check],
            {"orders": _FakeAdapter(full)},
            config=ValidationConfig(annotated_check_info="summary"),
        )
        # No explicit check_info -> config's "summary" is used.
        annotated = result.get_annotated_output()["annotated"]["orders"]
        item = _parse_check_info({r["id"]: r["check_info"] for r in _row(annotated)}[2])[0]
        assert item["dimension"] == "accuracy"


# ---------------------------------------------------------------------------
# save() output modes
# ---------------------------------------------------------------------------


class TestSaveModes:
    def _result_with_failures(self):
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        check = _make_check(
            "c",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"]}),
            tables_in_query="orders",
        )
        return _make_result([check], {"orders": _FakeAdapter(full)})

    def test_failed_rows_mode_no_annotated(self, tmp_path):
        self._result_with_failures().save(str(tmp_path), prefix="r", output_mode="failed_rows")
        files = {p.name for p in tmp_path.iterdir()}
        assert not any("_annotated.csv" in f for f in files)
        assert "r_check_results.csv" in files

    def test_annotated_mode_writes_annotated(self, tmp_path):
        self._result_with_failures().save(str(tmp_path), prefix="r", output_mode="annotated")
        files = {p.name for p in tmp_path.iterdir()}
        assert "r_orders_annotated.csv" in files

    def test_both_mode_writes_both(self, tmp_path):
        self._result_with_failures().save(str(tmp_path), prefix="r", output_mode="both")
        files = {p.name for p in tmp_path.iterdir()}
        assert "r_orders_annotated.csv" in files
        assert "r_orders.csv" in files  # failed-rows CSV

    def test_invalid_mode_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Unknown output_mode"):
            self._result_with_failures().save(str(tmp_path), output_mode="anotated")

    def test_defaults_to_config_output_mode(self, tmp_path):
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        check = _make_check("c", "orders", failed_rows=pa.table({"id": [2], "name": ["b"]}))
        result = _make_result(
            [check],
            {"orders": _FakeAdapter(full)},
            config=ValidationConfig(output_mode="annotated"),
        )
        result.save(str(tmp_path), prefix="r")  # no explicit mode
        files = {p.name for p in tmp_path.iterdir()}
        assert "r_orders_annotated.csv" in files

    @staticmethod
    def _read_csv(path):
        import pyarrow.csv as pacsv

        return pacsv.read_csv(str(path))

    def test_annotated_csv_has_check_info_residue_keeps_check_ids(self, tmp_path):
        # A cross-table (residue) check + a mergeable check, in annotated mode:
        # the annotated CSV carries check_info; the residue CSV keeps check_ids.
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        mergeable = _make_check(
            "row_check",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"]}),
            tables_in_query="orders",
        )
        residue = _make_check(
            "join_check",
            "orders",
            failed_rows=pa.table({"id": [3], "name": ["c"]}),
            tables_in_query="orders, customers",
        )
        result = _make_result([mergeable, residue], {"orders": _FakeAdapter(full)})
        result.save(str(tmp_path), prefix="r", output_mode="annotated", check_info="summary")

        annotated_cols = self._read_csv(tmp_path / "r_orders_annotated.csv").column_names
        assert "check_info" in annotated_cols
        assert "check_ids" not in annotated_cols

        # Residue CSV (cross-table key, sorted) keeps the legacy check_ids column.
        residue_csv = tmp_path / "r_customers_orders.csv"
        assert residue_csv.exists()
        residue_cols = self._read_csv(residue_csv).column_names
        assert "check_ids" in residue_cols
        assert "check_info" not in residue_cols

    def test_failed_rows_csv_unchanged_legacy_check_ids(self, tmp_path):
        # failed_rows / both modes: standalone CSVs still emit legacy check_ids.
        self._result_with_failures().save(str(tmp_path), prefix="r", output_mode="both")
        orders_cols = self._read_csv(tmp_path / "r_orders.csv").column_names
        assert "check_ids" in orders_cols
        assert "check_info" not in orders_cols


# ---------------------------------------------------------------------------
# End-to-end with a real DuckDB-backed adapter
# ---------------------------------------------------------------------------


class TestDuckDBIntegration:
    def test_full_roundtrip_with_nulls(self):
        import ibis

        from vowl.adapters.ibis_adapter import IbisAdapter
        from vowl.adapters.multi_source_adapter import MultiSourceAdapter

        con = ibis.duckdb.connect()
        con.create_table(
            "people",
            pa.table({"id": [1, 2, 3], "email": ["a@x.com", None, "c@x.com"]}),
        )
        ibis_adapter = IbisAdapter(con)
        multi = MultiSourceAdapter({"people": ibis_adapter})

        # A not_null-style failure: the NULL-email row (id=2).
        failed = pa.table({"id": [2], "email": pa.array([None], type=pa.string())})
        check = _make_check("email_not_null", "people", failed_rows=failed)

        summary = {
            "validation_summary": {
                "total_checks": 1,
                "passed": 0,
                "failed": 1,
                "errors": 0,
                "total_execution_time_ms": 0.0,
                "success_rate": 0.0,
            }
        }
        result = ValidationResult(
            summary=summary,
            check_results=[check],
            contract=_FakeContract(),
            multi_adapter=multi,
            schema_names=["people"],
        )
        annotated = result.get_annotated_output()["annotated"]["people"]
        rows = {r["id"]: r["check_info"] for r in annotated.to_arrow().to_pylist()}
        # The failed NULL-bearing row is annotated end-to-end, not hidden.
        assert _check_names_of(rows[2]) == ["email_not_null"]
        assert rows[1] is None and rows[3] is None


# ---------------------------------------------------------------------------
# End-to-end: generated unique / primaryKey / duplicateValues checks now merge
# into the annotated table instead of falling to residues.
# ---------------------------------------------------------------------------


class TestGeneratedChecksMergeEndToEnd:
    def _validate(self, monkeypatch, properties, table_quality, table):
        import ibis

        import vowl.contracts.contract as contract_module
        from vowl.adapters.ibis_adapter import IbisAdapter
        from vowl.contracts.models import get_latest_version
        from vowl.validate import validate_data

        monkeypatch.setattr(contract_module, "validate_contract", lambda data, version: None)
        contract = contract_module.Contract(
            {
                "apiVersion": get_latest_version(),
                "kind": "DataContract",
                "version": "1.0.0",
                "id": "annotated-merge",
                "status": "active",
                "schema": [{"name": "people", "properties": properties, "quality": table_quality or []}],
            }
        )
        con = ibis.duckdb.connect()
        con.create_table("people", table)
        result = validate_data(contract, adapters={"people": IbisAdapter(con)})
        return result.get_annotated_output()

    @staticmethod
    def _residue_check_names(out) -> set[str]:
        names: set[str] = set()
        for df in out["residues"].values():
            names |= ValidationResult._check_names_in_entry(df)
        return names

    def test_unique_check_merges_full_rows(self, monkeypatch: pytest.MonkeyPatch):
        # email "a@x.com" appears 3 times -> 3 participating rows tagged.
        table = pa.table({"id": [1, 2, 3, 4], "email": ["a@x.com", "a@x.com", "a@x.com", "b@y.com"]})
        out = self._validate(
            monkeypatch,
            properties=[
                {"name": "id", "logicalType": "integer"},
                {"name": "email", "logicalType": "string", "unique": True},
            ],
            table_quality=[],
            table=table,
        )
        annotated = out["annotated"]["people"]
        # Same columns as the source table (full rows merged in).
        assert set(annotated.columns) >= {"id", "email"}
        rows = {r["id"]: r["check_info"] for r in annotated.to_arrow().to_pylist()}
        assert "email_unique_check" in _check_names_of(rows[1])
        assert rows[2] and rows[3]  # all three duplicate-group members tagged
        assert rows[4] is None  # the unique value passes
        # The unique check is NOT left in residues.
        assert "email_unique_check" not in self._residue_check_names(out)

    def test_primary_key_check_merges_nulls_and_dups(self, monkeypatch: pytest.MonkeyPatch):
        table = pa.table({"pk": pa.array([1, 1, 2, None], type=pa.int64())})
        out = self._validate(
            monkeypatch,
            properties=[{"name": "pk", "logicalType": "integer", "primaryKey": True}],
            table_quality=[],
            table=table,
        )
        annotated = out["annotated"]["people"]
        marked = [r for r in annotated.to_arrow().to_pylist() if r["check_info"]]
        # Two duplicate "1" rows + one NULL row = 3 participating rows.
        assert len(marked) == 3
        assert "pk_primary_key_check" not in self._residue_check_names(out)

    def test_duplicate_values_table_merges(self, monkeypatch: pytest.MonkeyPatch):
        table = pa.table({"a": ["x", "x", "y"], "b": ["1", "1", "2"]})
        out = self._validate(
            monkeypatch,
            properties=[
                {"name": "a", "logicalType": "string"},
                {"name": "b", "logicalType": "string"},
            ],
            table_quality=[
                {
                    "type": "library",
                    "metric": "duplicateValues",
                    "mustBe": 0,
                    "arguments": {"properties": ["a", "b"]},
                }
            ],
            table=table,
        )
        annotated = out["annotated"]["people"]
        marked = [r for r in annotated.to_arrow().to_pylist() if r["check_info"]]
        # The two identical (x, 1) rows are the duplicate group.
        assert len(marked) == 2
