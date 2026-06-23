"""Tests for annotated output (full in-scope table with failed rows marked).

Covers ``ValidationResult.get_annotated_output`` and the ``output_mode`` wiring
on ``save()`` / ``ValidationConfig`` introduced in the full-table-output plan.
"""

from __future__ import annotations

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

    def test_all_null_check_ids_when_no_failures(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        result = _make_result([], {"orders": _FakeAdapter(full)})
        annotated = result.get_annotated_output()["annotated"]["orders"]
        assert "check_ids" in annotated.columns
        assert annotated["check_ids"].to_list() == [None, None]
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
        rows = {r["id"]: r["check_ids"] for r in _row(annotated)}
        assert rows == {1: None, 2: "not_null_name", 3: None}

    def test_multiple_checks_comma_joined(self):
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        c1 = _make_check("check_a", "orders", failed_rows=pa.table({"id": [2], "name": ["b"]}))
        c2 = _make_check("check_b", "orders", failed_rows=pa.table({"id": [2], "name": ["b"]}))
        result = _make_result([c1, c2], {"orders": _FakeAdapter(full)})

        annotated = result.get_annotated_output()["annotated"]["orders"]
        rows = {r["id"]: r["check_ids"] for r in _row(annotated)}
        assert rows[2] == "check_a, check_b"
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
        rows = {r["id"]: r["check_ids"] for r in _row(annotated)}
        assert rows[2] == "not_null"

    def test_clean_null_bearing_row_not_cross_annotated(self):
        # Two rows share the NULL pattern; only one failed. The clean one stays null.
        full = pa.table({"id": [1, 2], "name": [None, None]})
        failed = pa.table({"id": [2], "name": pa.array([None], type=pa.string())})
        check = _make_check("not_null", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})

        annotated = result.get_annotated_output()["annotated"]["orders"]
        rows = {r["id"]: r["check_ids"] for r in _row(annotated)}
        assert rows == {1: None, 2: "not_null"}

    def test_multiple_null_columns(self):
        full = pa.table({"a": [1, 2], "b": ["x", None], "c": [None, None]})
        failed = pa.table({"a": [2], "b": pa.array([None], type=pa.string()), "c": pa.array([None], type=pa.string())})
        check = _make_check("c", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        annotated = result.get_annotated_output()["annotated"]["orders"]
        rows = {r["a"]: r["check_ids"] for r in _row(annotated)}
        assert rows == {1: None, 2: "c"}

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
        rows = {r["id"]: r["check_ids"] for r in _row(annotated)}
        assert rows == {1: None, 2: "c"}

    def test_duplicate_full_table_rows_all_marked(self):
        # N byte-identical rows, one failed -> all N marked (safe over-flagging).
        full = pa.table({"id": [2, 2, 1], "name": ["b", "b", "a"]})
        failed = pa.table({"id": [2], "name": ["b"]})
        check = _make_check("c", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        annotated = result.get_annotated_output()["annotated"]["orders"]
        marks = [r["check_ids"] for r in _row(annotated)]
        assert marks == ["c", "c", None]


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
        assert out["annotated"]["orders"]["check_ids"].to_list() == [None, None]
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
        assert out["annotated"]["orders"]["check_ids"].to_list() == [None, None]
        assert any("agg_check" in self._check_names(df) for df in out["residues"].values())

    def test_column_subset_check_is_residue(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        # Failed rows have only a subset of columns -> not mergeable.
        check = _make_check("subset", "orders", failed_rows=pa.table({"id": [2]}))
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        assert out["annotated"]["orders"]["check_ids"].to_list() == [None, None]
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
        assert out["annotated"]["orders"]["check_ids"].to_list() == [None, None]

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
        rows = {r["id"]: r["check_ids"] for r in _row(out["annotated"]["orders"])}
        assert rows[2] == "full_row"

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
# include_target
# ---------------------------------------------------------------------------


class TestIncludeTarget:
    def test_targets_column_added(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        check = _make_check(
            "c",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"]}),
            target="orders.name",
        )
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        annotated = result.get_annotated_output(include_target=True)["annotated"]["orders"]
        assert "targets" in annotated.columns
        rows = {r["id"]: r["targets"] for r in _row(annotated)}
        assert rows == {1: None, 2: "orders.name"}


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
        rows = {r["id"]: r["check_ids"] for r in annotated.to_arrow().to_pylist()}
        # The failed NULL-bearing row is annotated end-to-end, not hidden.
        assert rows[2] == "email_not_null"
        assert rows[1] is None and rows[3] is None
