"""Exhaustive check-reference variation tests.

Covers every concrete CheckReference subclass, all 8 comparison operators,
library-metric argument modes, auto-generated attribute checks, custom
engine checks, unsupported types, and edge cases.

See tests/check_reference_test_plan.md for the full test matrix.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from vowl.contracts.check_reference import (
    CustomColumnCheckReference,
    CustomTableCheckReference,
    DeclaredColumnExistsCheckReference,
    DuplicateValuesColumnCheckReference,
    DuplicateValuesTableCheckReference,
    InvalidValuesCheckReference,
    LogicalTypeCheckReference,
    LogicalTypeOptionsCheckReference,
    MissingValuesCheckReference,
    NullValuesCheckReference,
    PrimaryKeyCheckReference,
    RequiredCheckReference,
    RowCountCheckReference,
    SQLColumnCheckReference,
    SQLTableCheckReference,
    UniqueCheckReference,
)
from vowl.contracts.check_reference_base import CheckReference
from vowl.contracts.check_reference_unsupported import (
    UnsupportedColumnCheckReference,
    UnsupportedTableCheckReference,
)
from vowl.contracts.contract import Contract
from vowl.contracts.models import get_latest_version

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

OPERATORS_SCALAR = [
    "mustBe",
    "mustNotBe",
    "mustBeGreaterThan",
    "mustBeGreaterOrEqualTo",
    "mustBeLessThan",
    "mustBeLessOrEqualTo",
]

OPERATORS_RANGE = [
    "mustBeBetween",
    "mustNotBeBetween",
]


def _make_contract(
    monkeypatch: pytest.MonkeyPatch,
    *,
    schema_name: str = "items",
    properties: list[dict] | None = None,
    table_quality: list[dict] | None = None,
) -> Contract:
    monkeypatch.setattr("vowl.contracts.contract.validate_contract", lambda data, version: None)
    return Contract(
        {
            "apiVersion": get_latest_version(),
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "test-variations",
            "status": "active",
            "schema": [
                {
                    "name": schema_name,
                    "properties": properties or [{"name": "col_a", "logicalType": "string"}],
                    "quality": table_quality or [],
                }
            ],
        }
    )


# ===================================================================
# Group A — evaluate() with all 8 operators (PASS and FAIL)
# ===================================================================


class TestEvaluateAllOperators:
    @pytest.mark.parametrize(
        ("operator", "actual", "expected", "want"),
        [
            # mustBe
            ("mustBe", 0, 0, True),
            ("mustBe", 1, 0, False),
            # mustNotBe
            ("mustNotBe", 1, 0, True),
            ("mustNotBe", 0, 0, False),
            # mustBeGreaterThan
            ("mustBeGreaterThan", 5, 3, True),
            ("mustBeGreaterThan", 3, 3, False),
            ("mustBeGreaterThan", 2, 3, False),
            # mustBeGreaterOrEqualTo
            ("mustBeGreaterOrEqualTo", 3, 3, True),
            ("mustBeGreaterOrEqualTo", 4, 3, True),
            ("mustBeGreaterOrEqualTo", 2, 3, False),
            # mustBeLessThan
            ("mustBeLessThan", 2, 3, True),
            ("mustBeLessThan", 3, 3, False),
            ("mustBeLessThan", 4, 3, False),
            # mustBeLessOrEqualTo
            ("mustBeLessOrEqualTo", 3, 3, True),
            ("mustBeLessOrEqualTo", 2, 3, True),
            ("mustBeLessOrEqualTo", 4, 3, False),
            # mustBeBetween — inclusive on both ends
            ("mustBeBetween", 5, [1, 10], True),
            ("mustBeBetween", 1, [1, 10], True),
            ("mustBeBetween", 10, [1, 10], True),
            ("mustBeBetween", 0, [1, 10], False),
            ("mustBeBetween", 11, [1, 10], False),
            # mustNotBeBetween
            ("mustNotBeBetween", 0, [1, 10], True),
            ("mustNotBeBetween", 11, [1, 10], True),
            ("mustNotBeBetween", 5, [1, 10], False),
            ("mustNotBeBetween", 1, [1, 10], False),
            ("mustNotBeBetween", 10, [1, 10], False),
        ],
    )
    def test_evaluate(self, operator: str, actual: int, expected: int | list[int], want: bool):
        assert CheckReference.evaluate(actual, operator, expected) is want

    def test_unknown_operator_returns_false(self):
        assert CheckReference.evaluate(0, "unknown", None) is False


# ===================================================================
# Group B — Operator × SQL check (table + column level)
# ===================================================================


def _sql_check(operator: str, value):
    """Build a minimal SQL quality block with the given operator."""
    return {
        "name": f"check_{operator}",
        "type": "sql",
        "query": "SELECT COUNT(*) FROM items WHERE col_a IS NULL",
        "dimension": "completeness",
        "severity": "error",
        operator: value,
    }


class TestSQLCheckWithAllOperators:
    @pytest.mark.parametrize("operator", OPERATORS_SCALAR)
    def test_table_level_scalar_operator(self, monkeypatch: pytest.MonkeyPatch, operator: str):
        contract = _make_contract(monkeypatch, table_quality=[_sql_check(operator, 5)])
        refs = contract.get_check_references_by_schema()["items"]
        sql_refs = [r for r in refs if isinstance(r, SQLTableCheckReference)]
        assert len(sql_refs) == 1
        op, val = sql_refs[0].get_expected_value()
        assert op == operator
        assert val == 5

    @pytest.mark.parametrize("operator", OPERATORS_RANGE)
    def test_table_level_range_operator(self, monkeypatch: pytest.MonkeyPatch, operator: str):
        contract = _make_contract(monkeypatch, table_quality=[_sql_check(operator, [0, 10])])
        refs = contract.get_check_references_by_schema()["items"]
        sql_refs = [r for r in refs if isinstance(r, SQLTableCheckReference)]
        assert len(sql_refs) == 1
        op, val = sql_refs[0].get_expected_value()
        assert op == operator
        assert val == [0, 10]

    @pytest.mark.parametrize("operator", OPERATORS_SCALAR)
    def test_column_level_scalar_operator(self, monkeypatch: pytest.MonkeyPatch, operator: str):
        prop = {"name": "col_a", "logicalType": "string", "quality": [_sql_check(operator, 5)]}
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        sql_refs = [r for r in refs if isinstance(r, SQLColumnCheckReference)]
        assert len(sql_refs) == 1
        op, val = sql_refs[0].get_expected_value()
        assert op == operator
        assert val == 5

    @pytest.mark.parametrize("operator", OPERATORS_RANGE)
    def test_column_level_range_operator(self, monkeypatch: pytest.MonkeyPatch, operator: str):
        prop = {"name": "col_a", "logicalType": "string", "quality": [_sql_check(operator, [0, 10])]}
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        sql_refs = [r for r in refs if isinstance(r, SQLColumnCheckReference)]
        assert len(sql_refs) == 1
        op, val = sql_refs[0].get_expected_value()
        assert op == operator
        assert val == [0, 10]


# ===================================================================
# Group B′ — build_result() PASSED / FAILED for SQL checks
# ===================================================================


class TestBuildResultPassedFailed:
    @pytest.mark.parametrize(
        ("operator", "expected", "actual", "want_status"),
        [
            ("mustBe", 0, 0, "PASSED"),
            ("mustBe", 0, 1, "FAILED"),
            ("mustBeGreaterThan", 5, 10, "PASSED"),
            ("mustBeGreaterThan", 5, 3, "FAILED"),
            ("mustBeBetween", [0, 10], 5, "PASSED"),
            ("mustBeBetween", [0, 10], 15, "FAILED"),
            ("mustNotBeBetween", [0, 10], 15, "PASSED"),
            ("mustNotBeBetween", [0, 10], 5, "FAILED"),
        ],
    )
    def test_sql_table_build_result(
        self,
        monkeypatch: pytest.MonkeyPatch,
        operator: str,
        expected,
        actual: int,
        want_status: str,
    ):
        contract = _make_contract(monkeypatch, table_quality=[_sql_check(operator, expected)])
        ref = [r for r in contract.get_check_references_by_schema()["items"] if isinstance(r, SQLTableCheckReference)][
            0
        ]
        result = ref.build_result(actual_value=actual, execution_time_ms=1.0)
        assert result.status == want_status


# ===================================================================
# Group C — Operator × library metric (nullValues as representative)
# ===================================================================


def _null_values_check(operator: str, value):
    return {
        "type": "library",
        "metric": "nullValues",
        "dimension": "completeness",
        operator: value,
    }


class TestLibraryMetricOperators:
    @pytest.mark.parametrize("operator", OPERATORS_SCALAR)
    def test_null_values_scalar_operator(self, monkeypatch: pytest.MonkeyPatch, operator: str):
        prop = {"name": "col_a", "logicalType": "string", "quality": [_null_values_check(operator, 5)]}
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        nv_refs = [r for r in refs if isinstance(r, NullValuesCheckReference)]
        assert len(nv_refs) == 1
        op, val = nv_refs[0].get_expected_value()
        assert op == operator
        assert val == 5

    @pytest.mark.parametrize("operator", OPERATORS_RANGE)
    def test_null_values_range_operator(self, monkeypatch: pytest.MonkeyPatch, operator: str):
        prop = {"name": "col_a", "logicalType": "string", "quality": [_null_values_check(operator, [0, 10])]}
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        nv_refs = [r for r in refs if isinstance(r, NullValuesCheckReference)]
        assert len(nv_refs) == 1
        op, val = nv_refs[0].get_expected_value()
        assert op == operator
        assert val == [0, 10]


# ===================================================================
# Group D — All library metric variations
# ===================================================================


class TestLibraryMetricVariations:
    # --- nullValues ---

    def test_null_values_plain(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [{"type": "library", "metric": "nullValues", "mustBe": 0, "dimension": "completeness"}],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        nv = [r for r in refs if isinstance(r, NullValuesCheckReference)]
        assert len(nv) == 1
        query = nv[0].get_query("duckdb")
        assert "COUNT(*)" in query
        assert "IS NULL" in query.upper()

    def test_null_values_percent(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [
                {"type": "library", "metric": "nullValues", "mustBe": 0, "unit": "percent", "dimension": "completeness"}
            ],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        nv = [r for r in refs if isinstance(r, NullValuesCheckReference)]
        assert len(nv) == 1
        query = nv[0].get_query("duckdb")
        assert "100" in query  # percentage wrapping

    # --- missingValues ---

    def test_missing_values_default(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [{"type": "library", "metric": "missingValues", "mustBe": 0, "dimension": "completeness"}],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        mv = [r for r in refs if isinstance(r, MissingValuesCheckReference)]
        assert len(mv) == 1
        query = mv[0].get_query("duckdb")
        assert "IS NULL" in query.upper()

    def test_missing_values_explicit_list(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [
                {
                    "type": "library",
                    "metric": "missingValues",
                    "mustBe": 0,
                    "dimension": "completeness",
                    "arguments": {"missingValues": ["N/A", "UNKNOWN", None]},
                }
            ],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        mv = [r for r in refs if isinstance(r, MissingValuesCheckReference)]
        assert len(mv) == 1
        query = mv[0].get_query("duckdb")
        assert "N/A" in query
        assert "UNKNOWN" in query
        assert "IS NULL" in query.upper()

    # --- invalidValues ---

    def test_invalid_values_valid_values_only(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [
                {
                    "type": "library",
                    "metric": "invalidValues",
                    "mustBe": 0,
                    "dimension": "conformity",
                    "arguments": {"validValues": ["A", "B", "C"]},
                }
            ],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        iv = [r for r in refs if isinstance(r, InvalidValuesCheckReference)]
        assert len(iv) == 1
        query = iv[0].get_query("duckdb")
        assert "A" in query and "B" in query and "C" in query

    def test_invalid_values_pattern_only(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [
                {
                    "type": "library",
                    "metric": "invalidValues",
                    "mustBe": 0,
                    "dimension": "conformity",
                    "arguments": {"pattern": "^[A-Z]+$"},
                }
            ],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        iv = [r for r in refs if isinstance(r, InvalidValuesCheckReference)]
        assert len(iv) == 1
        query = iv[0].get_query("duckdb")
        assert "^[A-Z]+$" in query

    def test_invalid_values_both_valid_values_and_pattern(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [
                {
                    "type": "library",
                    "metric": "invalidValues",
                    "mustBe": 0,
                    "dimension": "conformity",
                    "arguments": {"validValues": ["X", "Y"], "pattern": "^[A-Z]$"},
                }
            ],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        iv = [r for r in refs if isinstance(r, InvalidValuesCheckReference)]
        assert len(iv) == 1
        query = iv[0].get_query("duckdb")
        # Both valid values and pattern should appear
        assert "X" in query
        assert "^[A-Z]$" in query

    # --- duplicateValues (column) ---

    def test_duplicate_values_column_plain(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [{"type": "library", "metric": "duplicateValues", "mustBe": 0, "dimension": "uniqueness"}],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        dv = [r for r in refs if isinstance(r, DuplicateValuesColumnCheckReference)]
        assert len(dv) == 1
        query = dv[0].get_query("duckdb")
        assert "GROUP BY" in query.upper()

    def test_duplicate_values_column_percent(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [
                {
                    "type": "library",
                    "metric": "duplicateValues",
                    "mustBe": 0,
                    "unit": "percent",
                    "dimension": "uniqueness",
                }
            ],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        dv = [r for r in refs if isinstance(r, DuplicateValuesColumnCheckReference)]
        assert len(dv) == 1
        query = dv[0].get_query("duckdb")
        assert "100" in query

    # --- rowCount (table) ---

    def test_row_count(self, monkeypatch: pytest.MonkeyPatch):
        table_q = [{"type": "library", "metric": "rowCount", "mustBeGreaterThan": 0, "dimension": "completeness"}]
        contract = _make_contract(monkeypatch, table_quality=table_q)
        refs = contract.get_check_references_by_schema()["items"]
        rc = [r for r in refs if isinstance(r, RowCountCheckReference)]
        assert len(rc) == 1
        op, val = rc[0].get_expected_value()
        assert op == "mustBeGreaterThan"
        assert val == 0
        query = rc[0].get_query("duckdb")
        assert "COUNT(*)" in query

    # --- duplicateValues (table) ---

    def test_duplicate_values_table(self, monkeypatch: pytest.MonkeyPatch):
        table_q = [
            {
                "type": "library",
                "metric": "duplicateValues",
                "mustBe": 0,
                "dimension": "uniqueness",
                "arguments": {"properties": ["col_a"]},
            }
        ]
        contract = _make_contract(monkeypatch, table_quality=table_q)
        refs = contract.get_check_references_by_schema()["items"]
        dv = [r for r in refs if isinstance(r, DuplicateValuesTableCheckReference)]
        assert len(dv) == 1
        query = dv[0].get_query("duckdb")
        assert "GROUP BY" in query.upper()
        assert "col_a" in query


# ===================================================================
# Group C2 — percent-unit metrics produce VALID SQL
#
# Regression guard: ``_wrap_percent`` once attached table aliases
# (``AS _cnt`` / ``AS _tot``) to scalar subqueries used in an arithmetic
# expression, which is invalid SQL in every dialect -- DuckDB raised
# ``syntax error at or near "AS"`` and sqlglot could not even re-parse its
# own output, so every percent check came back as ERROR instead of a real
# verdict.  The string-only assertions above (``"100" in query``) did not
# catch this.  These tests re-parse the rendered SQL so a malformed wrap
# fails loudly and dialect-independently, without needing a live backend.
# ===================================================================


class TestPercentMetricsValidSql:
    """Every percent-unit metric must render SQL that round-trips through sqlglot."""

    def _percent_query(self, monkeypatch, *, properties=None, table_quality=None, cls):
        contract = _make_contract(monkeypatch, properties=properties, table_quality=table_quality)
        refs = contract.get_check_references_by_schema()["items"]
        matches = [r for r in refs if isinstance(r, cls)]
        assert len(matches) == 1
        return matches[0].get_query("duckdb")

    @staticmethod
    def _assert_valid_sql(query: str) -> None:
        import sqlglot

        assert "100" in query  # the percentage wrapping is present...
        # ...and the wrapped query is actually valid SQL (re-parses cleanly).
        sqlglot.parse_one(query, dialect="duckdb")

    def test_null_values_percent_sql_valid(self, monkeypatch: pytest.MonkeyPatch):
        query = self._percent_query(
            monkeypatch,
            cls=NullValuesCheckReference,
            properties=[
                {
                    "name": "col_a",
                    "logicalType": "string",
                    "quality": [{"type": "library", "metric": "nullValues", "mustBe": 0, "unit": "percent"}],
                }
            ],
        )
        self._assert_valid_sql(query)

    def test_missing_values_percent_sql_valid(self, monkeypatch: pytest.MonkeyPatch):
        query = self._percent_query(
            monkeypatch,
            cls=MissingValuesCheckReference,
            properties=[
                {
                    "name": "col_a",
                    "logicalType": "string",
                    "quality": [
                        {
                            "type": "library",
                            "metric": "missingValues",
                            "mustBe": 0,
                            "unit": "percent",
                            "arguments": {"missingValues": ["N/A", None]},
                        }
                    ],
                }
            ],
        )
        self._assert_valid_sql(query)

    def test_invalid_values_percent_sql_valid(self, monkeypatch: pytest.MonkeyPatch):
        query = self._percent_query(
            monkeypatch,
            cls=InvalidValuesCheckReference,
            properties=[
                {
                    "name": "col_a",
                    "logicalType": "string",
                    "quality": [
                        {
                            "type": "library",
                            "metric": "invalidValues",
                            "mustBe": 0,
                            "unit": "percent",
                            "arguments": {"validValues": ["A", "B"]},
                        }
                    ],
                }
            ],
        )
        self._assert_valid_sql(query)

    def test_duplicate_values_column_percent_sql_valid(self, monkeypatch: pytest.MonkeyPatch):
        query = self._percent_query(
            monkeypatch,
            cls=DuplicateValuesColumnCheckReference,
            properties=[
                {
                    "name": "col_a",
                    "logicalType": "string",
                    "quality": [{"type": "library", "metric": "duplicateValues", "mustBe": 0, "unit": "percent"}],
                }
            ],
        )
        self._assert_valid_sql(query)

    def test_duplicate_values_table_percent_sql_valid(self, monkeypatch: pytest.MonkeyPatch):
        query = self._percent_query(
            monkeypatch,
            cls=DuplicateValuesTableCheckReference,
            table_quality=[
                {
                    "type": "library",
                    "metric": "duplicateValues",
                    "mustBe": 0,
                    "unit": "percent",
                    "arguments": {"properties": ["col_a"]},
                }
            ],
        )
        self._assert_valid_sql(query)


# ===================================================================
# Group D2 — duplicate/unique/PK checks are annotated-mergeable
#
# These count *participating rows* (not duplicate groups), so their scalar
# query stays a single top-level COUNT(*) (aggregation_type == "count") and the
# auto-derived failed-rows query is ``SELECT * FROM table WHERE <pred>`` --
# full rows, identical columns to the source table, hence mergeable into the
# annotated output rather than forced into residues.
# ===================================================================


class TestDuplicateUniquePkMergeable:
    def _ref(self, monkeypatch, cls, *, properties, table_quality=None):
        contract = _make_contract(monkeypatch, properties=properties, table_quality=table_quality)
        refs = contract.get_check_references_by_schema()["items"]
        matches = [r for r in refs if isinstance(r, cls)]
        assert len(matches) == 1
        return matches[0]

    def test_unique_is_row_level_count(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._ref(
            monkeypatch,
            UniqueCheckReference,
            properties=[{"name": "col_a", "logicalType": "string", "unique": True}],
        )
        assert ref.aggregation_type == "count"
        assert ref.supports_row_level_output is True
        failed = ref.get_failed_rows_query("duckdb")
        assert failed.upper().startswith("SELECT *")

    def test_primary_key_is_row_level_count(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._ref(
            monkeypatch,
            PrimaryKeyCheckReference,
            properties=[{"name": "col_a", "logicalType": "string", "primaryKey": True}],
        )
        assert ref.aggregation_type == "count"
        assert ref.supports_row_level_output is True
        failed = ref.get_failed_rows_query("duckdb")
        assert failed.upper().startswith("SELECT *")
        # PK violations = NULL keys OR duplicate-group members.
        assert "IS NULL" in failed.upper()

    def test_duplicate_values_column_is_row_level_count(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._ref(
            monkeypatch,
            DuplicateValuesColumnCheckReference,
            properties=[
                {
                    "name": "col_a",
                    "logicalType": "string",
                    "quality": [{"type": "library", "metric": "duplicateValues", "mustBe": 0}],
                }
            ],
        )
        assert ref.aggregation_type == "count"
        assert ref.supports_row_level_output is True
        assert ref.get_failed_rows_query("duckdb").upper().startswith("SELECT *")

    def test_duplicate_values_table_is_row_level_count(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._ref(
            monkeypatch,
            DuplicateValuesTableCheckReference,
            properties=[
                {"name": "col_a", "logicalType": "string"},
                {"name": "col_b", "logicalType": "string"},
            ],
            table_quality=[
                {
                    "type": "library",
                    "metric": "duplicateValues",
                    "mustBe": 0,
                    "arguments": {"properties": ["col_a", "col_b"]},
                }
            ],
        )
        assert ref.aggregation_type == "count"
        assert ref.supports_row_level_output is True
        failed = ref.get_failed_rows_query("duckdb")
        assert failed.upper().startswith("SELECT *")
        # Multi-column duplicates use a correlated EXISTS (portable to SQL Server).
        assert "EXISTS" in failed.upper()

    def test_percent_duplicate_values_stays_non_row_level(self, monkeypatch: pytest.MonkeyPatch):
        """The percent unit wraps the count in a ratio and remains non-mergeable."""
        ref = self._ref(
            monkeypatch,
            DuplicateValuesColumnCheckReference,
            properties=[
                {
                    "name": "col_a",
                    "logicalType": "string",
                    "quality": [{"type": "library", "metric": "duplicateValues", "mustBe": 0, "unit": "percent"}],
                }
            ],
        )
        assert ref.supports_row_level_output is False

    def test_row_count_is_not_row_level(self, monkeypatch: pytest.MonkeyPatch):
        """rowCount is a whole-table aggregate: no failure predicate, so it is
        not mergeable into the annotated table and contributes no failing rows."""
        ref = self._ref(
            monkeypatch,
            RowCountCheckReference,
            properties=[{"name": "col_a", "logicalType": "string"}],
            table_quality=[{"type": "library", "metric": "rowCount", "mustBeGreaterThan": 0}],
        )
        assert ref.supports_row_level_output is False
        # A failing rowCount reports zero failing rows (count is table size, not violations).
        assert ref.compute_failed_rows_count(1000) == 0


# ===================================================================
# Group D3 — cross-table checks whose failed-rows query projects only the
# anchor table's columns are annotated-mergeable.
#
# The COUNT(*) -> SELECT * rewrite only touches the OUTER select list, so a
# subquery-wrapped ``SELECT payroll.*`` still governs the projection: the
# failed-rows query returns payroll-only columns, matching the anchor schema.
# A bare JOIN's ``SELECT *`` returns both tables' columns and stays a residue.
# ===================================================================


class TestCrossTableFailedRowsProjection:
    _SUBQUERY_WRAPPED = (
        "SELECT COUNT(*) FROM ("
        "SELECT payroll.* "
        "FROM demo_employee_payroll payroll "
        "LEFT JOIN demo_employee_list ref ON payroll.employee_id = ref.employee_id "
        "WHERE ref.employee_id IS NULL"
        ") AS orphaned_payroll"
    )
    _BARE_JOIN = (
        "SELECT COUNT(*) "
        "FROM demo_employee_payroll payroll "
        "LEFT JOIN demo_employee_list ref ON payroll.employee_id = ref.employee_id "
        "WHERE ref.employee_id IS NULL"
    )

    def _ref(self, monkeypatch, query):
        contract = _make_contract(
            monkeypatch,
            schema_name="demo_employee_payroll",
            properties=[{"name": "employee_id", "logicalType": "string"}],
            table_quality=[
                {
                    "name": "employee_id_exists_in_master_list",
                    "type": "sql",
                    "query": query,
                    "dimension": "consistency",
                    "mustBe": 0,
                }
            ],
        )
        refs = contract.get_check_references_by_schema()["demo_employee_payroll"]
        matches = [r for r in refs if isinstance(r, SQLTableCheckReference)]
        assert len(matches) == 1
        return matches[0]

    def test_subquery_wrapped_count_rewrites_to_select_star_over_subquery(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._ref(monkeypatch, self._SUBQUERY_WRAPPED)
        failed = ref.get_failed_rows_query("duckdb")
        upper = failed.upper()
        # Outer COUNT(*) becomes SELECT *, but the inner projection is untouched.
        assert upper.startswith("SELECT *")
        assert "SELECT PAYROLL.*" in upper
        assert "FROM (" in upper

    def test_subquery_wrapped_failed_rows_yield_anchor_only_columns(self, monkeypatch: pytest.MonkeyPatch):
        # Execute the rewritten failed-rows query against a real DuckDB and
        # assert the resulting columns are exactly the payroll table's columns.
        import ibis

        ref = self._ref(monkeypatch, self._SUBQUERY_WRAPPED)
        con = ibis.duckdb.connect()
        con.create_table(
            "demo_employee_payroll",
            pa.table({"employee_id": ["e1", "e2", "e3"], "amount": [10, 20, 30]}),
        )
        con.create_table("demo_employee_list", pa.table({"employee_id": ["e1", "e2"]}))

        failed_sql = ref.get_failed_rows_query("duckdb")
        rows = con.sql(failed_sql).to_pyarrow()
        # Orphan row e3 only; columns match the payroll (anchor) table exactly.
        assert set(rows.column_names) == {"employee_id", "amount"}
        assert rows.to_pylist() == [{"employee_id": "e3", "amount": 30}]

    def test_bare_join_select_star_spans_both_tables(self, monkeypatch: pytest.MonkeyPatch):
        # The backward-compatible case: a bare JOIN's SELECT * spans both tables,
        # so its columns won't match the anchor schema and it stays a residue.
        # The rewritten query is a top-level SELECT * over the JOIN (no subquery
        # projection to constrain it), so both tables' columns are returned.
        ref = self._ref(monkeypatch, self._BARE_JOIN)
        failed = ref.get_failed_rows_query("duckdb")
        upper = failed.upper()
        assert upper.startswith("SELECT *")
        # No wrapping subquery projection: the JOIN is at the top level.
        assert "FROM DEMO_EMPLOYEE_PAYROLL" in upper
        assert "LEFT JOIN DEMO_EMPLOYEE_LIST" in upper
        assert "FROM (" not in upper


# ===================================================================
# Group E — Auto-generated attribute checks
# ===================================================================


class TestAutoGeneratedChecks:
    def test_fully_decorated_property_produces_all_generated_refs(self, monkeypatch: pytest.MonkeyPatch):
        """A column with all attributes should produce every auto-generated check type."""
        prop = {
            "name": "id",
            "logicalType": "integer",
            "logicalTypeOptions": {"minimum": 1, "maximum": 100},
            "required": True,
            "unique": True,
            "primaryKey": True,
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]

        types_found = {type(r) for r in refs}
        assert DeclaredColumnExistsCheckReference in types_found
        assert LogicalTypeCheckReference in types_found
        assert LogicalTypeOptionsCheckReference in types_found
        assert RequiredCheckReference in types_found
        assert UniqueCheckReference in types_found
        assert PrimaryKeyCheckReference in types_found

    def test_declared_column_exists_is_always_generated(self, monkeypatch: pytest.MonkeyPatch):
        """Even a bare property with just a name produces a column-exists check."""
        prop = {"name": "bare_col"}
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        exists = [r for r in refs if isinstance(r, DeclaredColumnExistsCheckReference)]
        assert len(exists) == 1
        assert exists[0].get_column_name() == "bare_col"
        assert exists[0].is_generated() is True

    def test_logical_type_check_generated_when_logical_type_present(self, monkeypatch: pytest.MonkeyPatch):
        prop = {"name": "age", "logicalType": "integer"}
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        lt = [r for r in refs if isinstance(r, LogicalTypeCheckReference)]
        assert len(lt) == 1
        assert lt[0].get_logical_type() == "integer"
        assert lt[0].is_generated() is True

    def test_required_check_not_generated_when_not_required(self, monkeypatch: pytest.MonkeyPatch):
        prop = {"name": "opt_col", "logicalType": "string"}
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        req = [r for r in refs if isinstance(r, RequiredCheckReference)]
        assert len(req) == 0

    def test_unique_check_not_generated_when_not_unique(self, monkeypatch: pytest.MonkeyPatch):
        prop = {"name": "dup_col", "logicalType": "string"}
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        uniq = [r for r in refs if isinstance(r, UniqueCheckReference)]
        assert len(uniq) == 0

    def test_primary_key_check_not_generated_when_not_pk(self, monkeypatch: pytest.MonkeyPatch):
        prop = {"name": "non_pk", "logicalType": "string"}
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        pk = [r for r in refs if isinstance(r, PrimaryKeyCheckReference)]
        assert len(pk) == 0

    @pytest.mark.parametrize(
        ("option_key", "option_value"),
        [
            ("minLength", 2),
            ("maxLength", 50),
            ("pattern", "^[A-Z]"),
            ("minimum", 0),
            ("maximum", 999),
            ("exclusiveMinimum", -1),
            ("exclusiveMaximum", 1000),
            ("multipleOf", 5),
        ],
    )
    def test_logical_type_options_generates_per_key(
        self, monkeypatch: pytest.MonkeyPatch, option_key: str, option_value
    ):
        logical_type = "string" if option_key in {"minLength", "maxLength", "pattern"} else "integer"
        prop = {
            "name": "col",
            "logicalType": logical_type,
            "logicalTypeOptions": {option_key: option_value},
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        lto = [r for r in refs if isinstance(r, LogicalTypeOptionsCheckReference)]
        assert len(lto) == 1
        assert lto[0].is_generated() is True
        # Should produce valid SQL
        query = lto[0].get_query("duckdb")
        assert "COUNT(*)" in query


# ===================================================================
# Group F — Unsupported / unknown types
# ===================================================================


class TestUnsupportedCheckReferences:
    def test_unknown_table_type_produces_unsupported_ref(self, monkeypatch: pytest.MonkeyPatch):
        table_q = [{"name": "bad_check", "type": "sparkql", "query": "bogus", "mustBe": 0}]
        contract = _make_contract(monkeypatch, table_quality=table_q)
        refs = contract.get_check_references_by_schema()["items"]
        unsup = [r for r in refs if isinstance(r, UnsupportedTableCheckReference)]
        assert len(unsup) == 1

    def test_unknown_column_type_produces_unsupported_ref(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [{"name": "bad_check", "type": "graphql", "query": "bogus", "mustBe": 0}],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        unsup = [r for r in refs if isinstance(r, UnsupportedColumnCheckReference)]
        assert len(unsup) == 1

    def test_unknown_library_metric_produces_unsupported_ref(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [{"type": "library", "metric": "entropy", "mustBe": 0, "dimension": "accuracy"}],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        unsup = [r for r in refs if isinstance(r, UnsupportedColumnCheckReference)]
        assert len(unsup) == 1


# ===================================================================
# Group G — Custom engine checks
# ===================================================================


class TestCustomEngineChecks:
    def test_custom_table_check(self, monkeypatch: pytest.MonkeyPatch):
        table_q = [
            {
                "name": "custom_tbl",
                "type": "custom",
                "engine": "great_expectations",
                "implementation": "expect_table_row_count_to_be_between",
                "mustBeGreaterThan": 0,
            }
        ]
        contract = _make_contract(monkeypatch, table_quality=table_q)
        refs = contract.get_check_references_by_schema()["items"]
        custom = [r for r in refs if isinstance(r, CustomTableCheckReference)]
        assert len(custom) == 1
        assert custom[0].get_engine() == "great_expectations"
        assert custom[0].get_implementation() == "expect_table_row_count_to_be_between"
        assert custom[0].get_execution_engine() == "great_expectations"
        op, val = custom[0].get_expected_value()
        assert op == "mustBeGreaterThan"
        assert val == 0

    def test_custom_column_check(self, monkeypatch: pytest.MonkeyPatch):
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "quality": [
                {
                    "name": "custom_col",
                    "type": "custom",
                    "engine": "dbt",
                    "implementation": {"macro": "test_not_null"},
                    "mustBe": 0,
                }
            ],
        }
        contract = _make_contract(monkeypatch, properties=[prop])
        refs = contract.get_check_references_by_schema()["items"]
        custom = [r for r in refs if isinstance(r, CustomColumnCheckReference)]
        assert len(custom) == 1
        assert custom[0].get_engine() == "dbt"
        assert custom[0].get_implementation() == {"macro": "test_not_null"}
        op, val = custom[0].get_expected_value()
        assert op == "mustBe"
        assert val == 0


# ===================================================================
# Group H — Edge cases
# ===================================================================


class TestEdgeCases:
    def test_no_operator_gives_unknown(self, monkeypatch: pytest.MonkeyPatch):
        """A check with no operator key returns ('unknown', None)."""
        table_q = [
            {
                "name": "no_op",
                "type": "sql",
                "query": "SELECT COUNT(*) FROM items",
            }
        ]
        contract = _make_contract(monkeypatch, table_quality=table_q)
        refs = contract.get_check_references_by_schema()["items"]
        sql_refs = [r for r in refs if isinstance(r, SQLTableCheckReference)]
        assert len(sql_refs) == 1
        op, val = sql_refs[0].get_expected_value()
        assert op == "unknown"
        assert val is None

    def test_between_at_exact_boundaries(self):
        """mustBeBetween is inclusive on both ends."""
        assert CheckReference.evaluate(1, "mustBeBetween", [1, 1]) is True
        assert CheckReference.evaluate(0, "mustBeBetween", [1, 1]) is False
        assert CheckReference.evaluate(2, "mustBeBetween", [1, 1]) is False

    def test_not_between_at_exact_boundaries(self):
        """mustNotBeBetween excludes the boundaries themselves."""
        assert CheckReference.evaluate(1, "mustNotBeBetween", [1, 1]) is False
        assert CheckReference.evaluate(0, "mustNotBeBetween", [1, 1]) is True
        assert CheckReference.evaluate(2, "mustNotBeBetween", [1, 1]) is True

    def test_multiple_quality_blocks_ordering_preserved(self, monkeypatch: pytest.MonkeyPatch):
        """When a schema has multiple quality blocks, their order is preserved."""
        table_q = [
            {"name": "first", "type": "sql", "query": "SELECT 1", "mustBe": 1},
            {"name": "second", "type": "sql", "query": "SELECT 2", "mustBe": 2},
            {"name": "third", "type": "sql", "query": "SELECT 3", "mustBe": 3},
        ]
        contract = _make_contract(monkeypatch, table_quality=table_q)
        refs = contract.get_check_references_by_schema()["items"]
        sql_refs = [r for r in refs if isinstance(r, SQLTableCheckReference)]
        assert len(sql_refs) == 3
        names = [r.get_check()["name"] for r in sql_refs]
        assert names == ["first", "second", "third"]

    def test_severity_and_dimension_round_trip_in_metadata(self, monkeypatch: pytest.MonkeyPatch):
        """severity and dimension from the YAML appear in result metadata."""
        table_q = [
            {
                "name": "meta_check",
                "type": "sql",
                "query": "SELECT COUNT(*) FROM items",
                "mustBe": 0,
                "severity": "error",
                "dimension": "accuracy",
            }
        ]
        contract = _make_contract(monkeypatch, table_quality=table_q)
        refs = contract.get_check_references_by_schema()["items"]
        sql_ref = [r for r in refs if isinstance(r, SQLTableCheckReference)][0]
        metadata = sql_ref.get_result_metadata()
        # check_definition should contain the original check dict
        assert metadata["check_definition"]["severity"] == "error"
        assert metadata["check_definition"]["dimension"] == "accuracy"

    def test_build_error_result(self, monkeypatch: pytest.MonkeyPatch):
        """build_error_result creates an ERROR status result."""
        table_q = [{"name": "err_check", "type": "sql", "query": "SELECT 1", "mustBe": 0}]
        contract = _make_contract(monkeypatch, table_quality=table_q)
        ref = [r for r in contract.get_check_references_by_schema()["items"] if isinstance(r, SQLTableCheckReference)][
            0
        ]
        result = ref.build_error_result(error_message="timeout", execution_time_ms=100.0)
        assert result.status == "ERROR"
        assert "timeout" in result.details

    def test_mixed_check_types_on_same_schema(self, monkeypatch: pytest.MonkeyPatch):
        """A schema can mix SQL, library, and custom checks simultaneously."""
        table_q = [
            {"name": "sql_check", "type": "sql", "query": "SELECT COUNT(*) FROM items", "mustBe": 0},
            {"type": "library", "metric": "rowCount", "mustBeGreaterThan": 0, "dimension": "completeness"},
            {"name": "custom_check", "type": "custom", "engine": "dbt", "implementation": "test", "mustBe": 0},
        ]
        prop = {
            "name": "col_a",
            "logicalType": "string",
            "required": True,
            "quality": [
                {
                    "name": "col_sql",
                    "type": "sql",
                    "query": "SELECT COUNT(*) FROM items WHERE col_a IS NULL",
                    "mustBe": 0,
                },
                {"type": "library", "metric": "nullValues", "mustBe": 0, "dimension": "completeness"},
            ],
        }
        contract = _make_contract(monkeypatch, properties=[prop], table_quality=table_q)
        refs = contract.get_check_references_by_schema()["items"]
        types_found = {type(r) for r in refs}
        assert SQLTableCheckReference in types_found
        assert RowCountCheckReference in types_found
        assert CustomTableCheckReference in types_found
        assert SQLColumnCheckReference in types_found
        assert NullValuesCheckReference in types_found
        assert DeclaredColumnExistsCheckReference in types_found
        assert RequiredCheckReference in types_found
