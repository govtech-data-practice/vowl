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
        # Failed row that does NOT exist in the full table -> under-match: fewer
        # rows flagged than distinct failures. This should never happen (failed
        # rows are derived from the full table), so it warns as an internal bug.
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        failed = pa.table({"id": [99], "name": ["zzz"]})
        check = _make_check("c", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        with caplog.at_level(logging.WARNING):
            result.get_annotated_output()
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("Unexpected problem" in m and "report" in m for m in warnings)

    def test_overmatched_duplicate_rows_logs_debug_not_warning(self, caplog):
        # Two byte-identical rows, one distinct failure -> over-match: both
        # copies get flagged. Normal operation, so it is DEBUG (silent by
        # default), never a warning.
        full = pa.table({"id": [1, 1, 2], "name": ["a", "a", "b"]})
        failed = pa.table({"id": [1], "name": ["a"]})
        check = _make_check("c", "orders", failed_rows=failed)
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        with caplog.at_level(logging.DEBUG):
            result.get_annotated_output()
        # Nothing at INFO or above (this is normal, expected operation).
        assert not [r for r in caplog.records if r.levelno >= logging.INFO]
        debugs = [r.message for r in caplog.records if r.levelno == logging.DEBUG]
        assert any("duplicate rows" in m and "nothing was missed" in m for m in debugs)


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------


class TestEligibility:
    def test_cross_table_check_with_extra_columns_is_residue(self):
        # A cross-table check whose failed rows carry BOTH tables' columns
        # (the bare-JOIN SELECT * shape) does not match the anchor schema and
        # stays a residue -- backward-compatible behaviour.
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        check = _make_check(
            "join_check",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"], "ref_id": [None]}),
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
# Cross-table checks merge onto their home schema when the failed-rows column
# set matches exactly (opt-in by query shape: the author projects only the
# anchor table's columns, e.g. SELECT payroll.* FROM payroll LEFT JOIN ref ...).
# ---------------------------------------------------------------------------


class TestCrossTableMerge:
    @staticmethod
    def _residue_check_names(out) -> set[str]:
        names: set[str] = set()
        for df in out["residues"].values():
            names |= ValidationResult._check_names_in_entry(df)
        return names

    def test_exact_match_merges_onto_home_schema(self):
        # Failed rows project only the anchor (orders) columns -> merges.
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        check = _make_check(
            "orphan_check",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"]}),
            tables_in_query="orders, customers",
        )
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()

        rows = {r["id"]: r["check_info"] for r in _row(out["annotated"]["orders"])}
        assert _check_names_of(rows[2]) == ["orphan_check"]
        assert rows[1] is None and rows[3] is None
        # Merged, so NOT duplicated into residues.
        assert "orphan_check" not in self._residue_check_names(out)

    def test_partial_columns_stays_residue(self):
        # Failed rows carry only a subset of the anchor columns -> residue.
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        check = _make_check(
            "orphan_partial",
            "orders",
            failed_rows=pa.table({"id": [2]}),
            tables_in_query="orders, customers",
        )
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        assert out["annotated"]["orders"]["check_info"].to_list() == [None, None]
        assert "orphan_partial" in self._residue_check_names(out)

    def test_both_tables_columns_stays_residue(self):
        # Bare-JOIN SELECT * shape returns both tables' columns -> residue.
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        check = _make_check(
            "orphan_wide",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"], "ref_id": [None]}),
            tables_in_query="orders, customers",
        )
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        assert out["annotated"]["orders"]["check_info"].to_list() == [None, None]
        assert "orphan_wide" in self._residue_check_names(out)

    def test_wrong_schema_anchor_never_merges(self):
        # A cross-table check anchored to "orders" must never merge onto
        # "customers", even when that table's shape coincidentally matches.
        orders_full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        customers_full = pa.table({"id": [10, 20], "name": ["x", "y"]})
        check = _make_check(
            "orphan_check",
            "orders",
            failed_rows=pa.table({"id": [10], "name": ["x"]}),  # matches customers' rows
            tables_in_query="orders, customers",
        )
        result = _make_result(
            [check],
            {"orders": _FakeAdapter(orders_full), "customers": _FakeAdapter(customers_full)},
        )
        out = result.get_annotated_output()
        # customers table is untouched -- the check is not anchored there.
        assert out["annotated"]["customers"]["check_info"].to_list() == [None, None]
        # orders is the anchor; the failing row (10, x) isn't in orders, so it
        # under-matches there (nothing flagged) but the check is still consumed
        # as mergeable for its home schema and not duplicated into residues.
        assert "orphan_check" not in self._residue_check_names(out)

    def test_errored_cross_table_check_unaffected(self):
        # An errored cross-table check is neither merged nor a residue.
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        check = _make_check(
            "errored_join",
            "orders",
            status="ERROR",
            failed_rows=None,
            tables_in_query="orders, customers",
        )
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        assert out["annotated"]["orders"]["check_info"].to_list() == [None, None]
        assert out["residues"] == {}

    def test_scalar_cross_table_check_stays_summary_only(self):
        # A cross-table aggregation (single scalar) can't point to rows: it is
        # not merged and produces no residue -- summary-only, unchanged.
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        check = _make_check(
            "cross_avg",
            "orders",
            failed_rows=pa.table({"cnt": [3]}),
            supports_row_level_output=False,
            tables_in_query="orders, customers",
        )
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        assert out["annotated"]["orders"]["check_info"].to_list() == [None, None]
        # Full-column mismatch AND non-row-level: residue holds its rows since
        # it has offending rows (cnt col); but the merge path is not taken.
        assert "cross_avg" in self._residue_check_names(out)

    def test_truncation_guard_fires_for_merged_cross_table_check(self):
        # A now-mergeable cross-table check whose rows were capped must raise,
        # not silently annotate un-fetched failures as passing.
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        check = _make_check(
            "orphan_check",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"]}),
            failed_rows_count=2,
            tables_in_query="orders, customers",
        )
        result = _make_result(
            [check],
            {"orders": _FakeAdapter(full)},
            config=ValidationConfig(max_failed_rows=1),
        )
        with pytest.raises(ValueError, match="annotated output"):
            result.get_annotated_output()


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
# Per-check residues (residues are one-entry-per-check, never grouped)
# ---------------------------------------------------------------------------


class TestPerCheckResidues:
    def test_residue_keyed_by_schema_and_check(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        # Cross-table failed rows carrying both tables' columns -> non-mergeable.
        check = _make_check(
            "join_check",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"], "ref_id": [None]}),
            tables_in_query="orders, customers",
        )
        result = _make_result([check], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        assert list(out["residues"].keys()) == ["orders::join_check"]

    def test_each_residue_carries_exactly_one_check_name(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        # Two non-mergeable checks that share table AND column set: under the old
        # grouped path these collapsed into one entry; now each is its own. The
        # extra ref_id column keeps them non-mergeable (both tables' columns).
        c1 = _make_check(
            "join_a",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"], "ref_id": [None]}),
            tables_in_query="orders, customers",
        )
        c2 = _make_check(
            "join_b",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"], "ref_id": [None]}),
            tables_in_query="orders, customers",
        )
        result = _make_result([c1, c2], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        # Two separate entries, each with a single check name (never merged).
        assert set(out["residues"].keys()) == {"orders::join_a", "orders::join_b"}
        for df in out["residues"].values():
            assert len(ValidationResult._check_names_in_entry(df)) == 1

    def test_mergeable_and_aggregation_same_row_no_double_report(self):
        # Regression for the grouped-path edge: an aggregation check whose failed
        # rows share a full row with a mergeable check used to glue onto it and
        # re-surface the merged row in residues. Per-check residues prevent that.
        full = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        mergeable = _make_check("row_check", "orders", failed_rows=pa.table({"id": [2], "name": ["b"]}))
        # supports_row_level_output=False -> non-mergeable, but full-column rows.
        agg = _make_check(
            "agg_check",
            "orders",
            failed_rows=pa.table({"id": [2], "name": ["b"]}),
            supports_row_level_output=False,
        )
        result = _make_result([mergeable, agg], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()

        # row_check is annotated and does NOT reappear in any residue.
        rows = {r["id"]: r["check_info"] for r in _row(out["annotated"]["orders"])}
        assert _check_names_of(rows[2]) == ["row_check"]
        residue_checks = set()
        for df in out["residues"].values():
            residue_checks |= ValidationResult._check_names_in_entry(df)
        assert residue_checks == {"agg_check"}  # row_check absent

    def test_passing_and_error_checks_not_in_residues(self):
        full = pa.table({"id": [1, 2], "name": ["a", "b"]})
        passed = _make_check("ok", "orders", status="PASSED", failed_rows=pa.table({"id": []}))
        result = _make_result([passed], {"orders": _FakeAdapter(full)})
        out = result.get_annotated_output()
        assert out["residues"] == {}


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

    def test_annotated_csv_and_residue_both_use_check_info(self, tmp_path):
        # A cross-table (residue) check + a mergeable check, in annotated mode:
        # both the annotated CSV and the residue CSV carry check_info (uniform),
        # never the legacy check_ids.
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
            failed_rows=pa.table({"id": [3], "name": ["c"], "ref_id": [None]}),
            tables_in_query="orders, customers",
        )
        result = _make_result([mergeable, residue], {"orders": _FakeAdapter(full)})
        result.save(str(tmp_path), prefix="r", output_mode="annotated", check_info="summary")

        annotated_cols = self._read_csv(tmp_path / "r_orders_annotated.csv").column_names
        assert "check_info" in annotated_cols
        assert "check_ids" not in annotated_cols

        # Residue CSV is per-check (keyed "<schema>::<check>") and carries the
        # same check_info column (a single-element JSON array) plus
        # tables_in_query. The mergeable check is NOT written as a residue.
        residue_csv = tmp_path / "r_orders_join_check_residue.csv"
        assert residue_csv.exists()
        assert not (tmp_path / "r_orders_row_check_residue.csv").exists()
        residue_table = self._read_csv(residue_csv)
        residue_cols = residue_table.column_names
        assert "check_info" in residue_cols
        assert "tables_in_query" in residue_cols
        assert "check_ids" not in residue_cols
        # The check_info cell parses as a JSON array carrying the check name.
        cell = residue_table.column("check_info")[0].as_py()
        parsed = json.loads(cell)
        assert [item["check_name"] for item in parsed] == ["join_check"]

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


# ---------------------------------------------------------------------------
# End-to-end: percent-unit library checks execute and FAIL with a ratio.
#
# Regression for the malformed ``_wrap_percent`` SQL (aliased scalar
# subqueries) that made every percent check ERROR before it could run.  These
# go through the full validate_data pipeline against a real DuckDB so a broken
# wrap surfaces as an ERROR status here.  Percent checks are scalar verdicts:
# they stay non-mergeable and produce no annotated rows / residues -- the point
# is that they now reach a real FAILED verdict with the correct ratio.
# ---------------------------------------------------------------------------


class TestPercentChecksExecuteEndToEnd:
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
                "id": "percent-e2e",
                "status": "active",
                "schema": [{"name": "people", "properties": properties, "quality": table_quality or []}],
            }
        )
        con = ibis.duckdb.connect()
        con.create_table("people", table)
        return validate_data(contract, adapters={"people": IbisAdapter(con)})

    @staticmethod
    def _check(result, name):
        matches = [cr for cr in result.check_results if cr.check_name == name]
        assert len(matches) == 1, f"expected one {name!r} check, got {len(matches)}"
        return matches[0]

    def test_null_values_percent_fails_with_ratio(self, monkeypatch: pytest.MonkeyPatch):
        # 1 NULL of 4 rows -> 25% null, mustBe 0 -> FAILED (not ERROR).
        table = pa.table({"id": [1, 2, 3, 4], "email": ["a@x.com", "a@x.com", None, "b@y.com"]})
        result = self._validate(
            monkeypatch,
            properties=[
                {"name": "id", "logicalType": "integer"},
                {
                    "name": "email",
                    "logicalType": "string",
                    "quality": [
                        {"type": "library", "metric": "nullValues", "mustBe": 0, "unit": "percent", "name": "null_pct"}
                    ],
                },
            ],
            table_quality=[],
            table=table,
        )
        cr = self._check(result, "null_pct")
        assert cr.status == "FAILED"
        assert float(cr.actual_value) == 25.0

    def test_duplicate_values_table_percent_fails_with_ratio(self, monkeypatch: pytest.MonkeyPatch):
        # email "a@x.com" duplicated: 2 of 4 rows participate -> 50%.
        table = pa.table({"id": [1, 2, 3, 4], "email": ["a@x.com", "a@x.com", None, "b@y.com"]})
        result = self._validate(
            monkeypatch,
            properties=[
                {"name": "id", "logicalType": "integer"},
                {"name": "email", "logicalType": "string"},
            ],
            table_quality=[
                {
                    "type": "library",
                    "metric": "duplicateValues",
                    "mustBe": 0,
                    "unit": "percent",
                    "arguments": {"properties": ["email"]},
                    "name": "dup_pct",
                }
            ],
            table=table,
        )
        cr = self._check(result, "dup_pct")
        assert cr.status == "FAILED"
        assert float(cr.actual_value) == 50.0


# ---------------------------------------------------------------------------
# End-to-end: a cross-table referential check whose failed-rows query projects
# only the anchor table's columns (SELECT payroll.* over a LEFT JOIN) merges
# onto that schema's annotated table instead of falling to residues.
# Uses the employee_payroll fixtures (tests/employee/).
# ---------------------------------------------------------------------------


class TestCrossTableMergeEndToEnd:
    from pathlib import Path as _Path

    _EMPLOYEE_DIR = _Path(__file__).parent / "employee"

    def _validate(self, monkeypatch, *, referential_query):
        import ibis
        import pandas as pd

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
                "id": "cross-table-merge-e2e",
                "status": "active",
                "schema": [
                    {
                        "name": "demo_employee_payroll",
                        "properties": [
                            {"name": "employee_id", "logicalType": "string"},
                            {"name": "amount", "logicalType": "integer"},
                        ],
                        "quality": [
                            {
                                "name": "employee_id_exists_in_master_list",
                                "type": "sql",
                                "dimension": "consistency",
                                "query": referential_query,
                                "mustBe": 0,
                            }
                        ],
                    },
                    {
                        "name": "demo_employee_list",
                        "properties": [{"name": "employee_id", "logicalType": "string"}],
                        "quality": [],
                    },
                ],
            }
        )
        con = ibis.duckdb.connect()
        # e939123 in payroll is absent from the master list -> one orphan row.
        con.create_table(
            "demo_employee_payroll",
            pd.DataFrame(
                {
                    "employee_id": ["e123213", "e128903", "e939123"],
                    "amount": [100, 200, 300],
                }
            ),
        )
        con.create_table(
            "demo_employee_list",
            pd.DataFrame({"employee_id": ["e123213", "e128903"]}),
        )
        ibis_adapter = IbisAdapter(con)
        return validate_data(
            contract,
            adapters={"demo_employee_payroll": ibis_adapter, "demo_employee_list": ibis_adapter},
        )

    @staticmethod
    def _residue_check_names(out) -> set[str]:
        names: set[str] = set()
        for df in out["residues"].values():
            names |= ValidationResult._check_names_in_entry(df)
        return names

    def test_subquery_wrapped_referential_check_merges_onto_payroll(self, monkeypatch: pytest.MonkeyPatch):
        query = (
            "SELECT COUNT(*) FROM ("
            "SELECT payroll.* "
            "FROM demo_employee_payroll payroll "
            "LEFT JOIN demo_employee_list ref ON payroll.employee_id = ref.employee_id "
            "WHERE ref.employee_id IS NULL"
            ") AS orphaned_payroll"
        )
        result = self._validate(monkeypatch, referential_query=query)
        cr = next(c for c in result.check_results if c.check_name == "employee_id_exists_in_master_list")
        assert cr.status == "FAILED"

        out = result.get_annotated_output()
        annotated = out["annotated"]["demo_employee_payroll"]
        rows = {r["employee_id"]: r["check_info"] for r in annotated.to_arrow().to_pylist()}
        # The orphan payroll row is annotated on the payroll table...
        assert _check_names_of(rows["e939123"]) == ["employee_id_exists_in_master_list"]
        # ...and the matched rows are clean.
        assert rows["e123213"] is None and rows["e128903"] is None
        # Merged, so it is NOT duplicated into residues.
        assert "employee_id_exists_in_master_list" not in self._residue_check_names(out)

    def test_bare_join_referential_check_stays_residue(self, monkeypatch: pytest.MonkeyPatch):
        # A bare-JOIN SELECT COUNT(*) rewrites to a top-level SELECT * spanning
        # both tables -> columns don't match the payroll anchor -> residue.
        query = (
            "SELECT COUNT(*) "
            "FROM demo_employee_payroll payroll "
            "LEFT JOIN demo_employee_list ref ON payroll.employee_id = ref.employee_id "
            "WHERE ref.employee_id IS NULL"
        )
        result = self._validate(monkeypatch, referential_query=query)
        out = result.get_annotated_output()
        annotated = out["annotated"]["demo_employee_payroll"]
        assert all(r["check_info"] is None for r in annotated.to_arrow().to_pylist())
        assert "employee_id_exists_in_master_list" in self._residue_check_names(out)
