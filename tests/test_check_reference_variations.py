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
    ArrayItemsCheckReference,
    CustomColumnCheckReference,
    CustomTableCheckReference,
    DeclaredColumnExistsCheckReference,
    DuplicateValuesColumnCheckReference,
    DuplicateValuesTableCheckReference,
    EnumCheckReference,
    InvalidValuesCheckReference,
    LogicalTypeCheckReference,
    LogicalTypeOptionsCheckReference,
    MissingValuesCheckReference,
    NullValuesCheckReference,
    PrimaryKeyCheckReference,
    PropertyForeignKeyCheckReference,
    RequiredCheckReference,
    RowCountCheckReference,
    SchemaForeignKeyCheckReference,
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
# Group E2 — Auto-generated enum (allowed value set) checks
# ===================================================================


class TestEnumCheck:
    def _enum_ref(self, monkeypatch, prop) -> EnumCheckReference:
        contract = _make_contract(monkeypatch, schema_name="orders", properties=[prop])
        refs = contract.get_check_references_by_schema()["orders"]
        matches = [r for r in refs if isinstance(r, EnumCheckReference)]
        assert len(matches) == 1
        return matches[0]

    def test_string_enum_renders_not_in_with_null_guard(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._enum_ref(
            monkeypatch,
            {"name": "status", "logicalType": "string", "enum": [{"value": "active"}, {"value": "inactive"}]},
        )
        sql = ref._build_ast().sql("postgres")
        # sqlglot renders "col NOT IN (...)" canonically as "NOT col IN (...)"
        # (same as "IS NOT NULL" -> "NOT col IS NULL" for the sibling checks).
        assert "NOT" in sql and "IN (" in sql
        assert "'active'" in sql and "'inactive'" in sql
        assert "IS NULL" in sql
        check = ref.get_check()
        assert check["name"] == "status_enum_check"
        assert check["dimension"] == "conformity"
        assert check["mustBe"] == 0
        assert ref.is_generated() is True

    def test_semantic_shape_is_not_wrapping_in(self, monkeypatch: pytest.MonkeyPatch):
        """The predicate must be NOT(col IN (...)), i.e. an exp.Not around exp.In."""
        import sqlglot
        from sqlglot import exp

        ref = self._enum_ref(
            monkeypatch,
            {"name": "status", "logicalType": "string", "enum": [{"value": "a"}, {"value": "b"}]},
        )
        parsed = sqlglot.parse_one(ref.get_query("duckdb"), read="duckdb")
        not_in = [n for n in parsed.find_all(exp.Not) if isinstance(n.this, exp.In)]
        assert len(not_in) == 1

    def test_integer_enum_renders_numeric_literals(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._enum_ref(
            monkeypatch,
            {"name": "code", "logicalType": "integer", "enum": [{"value": 1}, {"value": 2}, {"value": 3}]},
        )
        sql = ref._build_ast().sql("postgres")
        assert "IN (1, 2, 3)" in sql
        # Numeric, unquoted — no string quoting of the values.
        assert "'1'" not in sql

    def test_boolean_enum_renders_true_false(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._enum_ref(
            monkeypatch,
            {"name": "flag", "logicalType": "boolean", "enum": [{"value": True}, {"value": False}]},
        )
        sql = ref._build_ast().sql("postgres")
        assert "TRUE" in sql and "FALSE" in sql
        assert "'1'" not in sql and "'0'" not in sql

    def test_injection_value_is_single_escaped_literal(self, monkeypatch: pytest.MonkeyPatch):
        """A SQL-injection payload renders as one escaped string literal and
        cannot break out of the IN list (parity with tests/test_sql_security.py)."""
        payload = "x') OR 1=1--"
        ref = self._enum_ref(
            monkeypatch,
            {"name": "status", "logicalType": "string", "enum": [{"value": payload}]},
        )
        sql = ref._build_ast().sql("postgres")
        # The single quote inside the payload is doubled (escaped); the trailing
        # "--" and "OR 1=1" stay inside the quoted literal, not as SQL.
        assert "'x'') OR 1=1--'" in sql
        assert "OR 1=1" not in sql.replace("'x'') OR 1=1--'", "")

    def test_null_enum_entry_filtered_out(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._enum_ref(
            monkeypatch,
            {
                "name": "status",
                "logicalType": "string",
                "enum": [{"value": "active"}, {"value": None}, {"value": "inactive"}],
            },
        )
        sql = ref._build_ast().sql("postgres")
        assert "IN ('active', 'inactive')" in sql
        assert "NULL)" not in sql  # None was dropped, not emitted as a literal

    def test_empty_after_filter_degrades_to_unsupported(self, monkeypatch: pytest.MonkeyPatch):
        """An enum with no usable allowed values degrades to an unsupported ref,
        without raising out of get_check_references_by_schema()."""
        contract = _make_contract(
            monkeypatch,
            schema_name="orders",
            properties=[{"name": "status", "logicalType": "string", "enum": [{"value": None}]}],
        )
        refs = contract.get_check_references_by_schema()["orders"]
        assert not any(isinstance(r, EnumCheckReference) for r in refs)
        assert any(isinstance(r, UnsupportedColumnCheckReference) for r in refs)

    def test_non_scalar_value_degrades_to_unsupported(self, monkeypatch: pytest.MonkeyPatch):
        contract = _make_contract(
            monkeypatch,
            schema_name="orders",
            properties=[{"name": "status", "logicalType": "string", "enum": [{"value": {"nested": 1}}]}],
        )
        refs = contract.get_check_references_by_schema()["orders"]
        assert not any(isinstance(r, EnumCheckReference) for r in refs)
        assert any(isinstance(r, UnsupportedColumnCheckReference) for r in refs)

    def test_executes_against_duckdb_counts_violations(self, monkeypatch: pytest.MonkeyPatch):
        import ibis

        ref = self._enum_ref(
            monkeypatch,
            {"name": "status", "logicalType": "string", "enum": [{"value": "active"}, {"value": "inactive"}]},
        )
        con = ibis.duckdb.connect()
        con.create_table(
            "orders",
            pa.table({"status": ["active", "inactive", "archived", "deleted", None]}),
        )
        # Count query: "archived" and "deleted" are out of set; NULL not counted.
        count = con.sql(ref.get_query("duckdb")).to_pyarrow().to_pylist()
        assert count == [{"count_star()": 2}]

        # Failed-rows query (COUNT(*) -> SELECT *) returns exactly the violators.
        assert ref.supports_row_level_output is True
        failed_sql = ref.get_failed_rows_query("duckdb")
        assert failed_sql.upper().startswith("SELECT *")
        rows = con.sql(failed_sql).to_pyarrow().to_pylist()
        assert sorted(r["status"] for r in rows) == ["archived", "deleted"]

    @pytest.mark.parametrize("dialect", ["duckdb", "mysql", "sqlite", "bigquery"])
    def test_cross_dialect_transpiles_preserving_membership(self, monkeypatch: pytest.MonkeyPatch, dialect: str):
        import sqlglot
        from sqlglot import exp

        ref = self._enum_ref(
            monkeypatch,
            {"name": "status", "logicalType": "string", "enum": [{"value": "active"}, {"value": "inactive"}]},
        )
        query = ref.get_query(dialect)
        parsed = sqlglot.parse_one(query, read=dialect)
        # Membership test preserved across dialects as NOT(col IN (...)).
        not_in = [n for n in parsed.find_all(exp.Not) if isinstance(n.this, exp.In)]
        assert len(not_in) == 1


# ===================================================================
# Group E3 — Auto-generated native array checks
# ===================================================================


class TestArrayChecks:
    """Native array-type auto-checks: minItems/maxItems/uniqueItems cardinality
    (``LogicalTypeOptionsCheckReference``) and element validation via the
    ``items`` sub-schema (``ArrayItemsCheckReference``).

    Array checks are emitted only when the property declares ``logicalType:
    array`` (a metadata gate in ``contract.py``); on a scalar column they
    degrade to an unsupported reference rather than emit array-only SQL.
    """

    def _array_refs(self, monkeypatch, prop):
        contract = _make_contract(monkeypatch, properties=[prop])
        return contract.get_check_references_by_schema()["items"]

    def _cardinality_ref(self, monkeypatch, option_key, option_value, *, items=None):
        prop = {
            "name": "tags",
            "logicalType": "array",
            "logicalTypeOptions": {option_key: option_value},
        }
        if items is not None:
            prop["items"] = items
        refs = self._array_refs(monkeypatch, prop)
        matches = [
            r
            for r in refs
            if isinstance(r, LogicalTypeOptionsCheckReference) and r._option_key == option_key
        ]
        assert len(matches) == 1
        return matches[0]

    @staticmethod
    def _list_table(values):
        con = __import__("ibis").duckdb.connect()
        con.create_table("items", pa.table({"tags": pa.array(values, type=pa.list_(pa.string()))}))
        return con

    @staticmethod
    def _count(con, query):
        return con.sql(query).to_pyarrow().to_pylist()[0]["count_star()"]

    # ---- cardinality: AST / SQL shape -----------------------------------

    def test_min_items_uses_array_size(self, monkeypatch: pytest.MonkeyPatch):
        import sqlglot
        from sqlglot import exp

        ref = self._cardinality_ref(monkeypatch, "minItems", 2)
        parsed = sqlglot.parse_one(ref._build_ast().sql("postgres"), read="postgres")
        assert len(list(parsed.find_all(exp.ArraySize))) == 1
        check = ref.get_check()
        assert check["name"] == "tags_logical_type_options_minItems_check"
        assert check["dimension"] == "conformity"
        assert check["mustBe"] == 0

    def test_unique_items_uses_array_distinct(self, monkeypatch: pytest.MonkeyPatch):
        import sqlglot
        from sqlglot import exp

        ref = self._cardinality_ref(monkeypatch, "uniqueItems", True)
        parsed = sqlglot.parse_one(ref._build_ast().sql("postgres"), read="postgres")
        assert len(list(parsed.find_all(exp.ArrayDistinct))) == 1

    def test_unique_items_false_degrades_to_unsupported(self, monkeypatch: pytest.MonkeyPatch):
        refs = self._array_refs(
            monkeypatch,
            {"name": "tags", "logicalType": "array", "logicalTypeOptions": {"uniqueItems": False}},
        )
        assert not any(isinstance(r, LogicalTypeOptionsCheckReference) for r in refs)
        assert any(isinstance(r, UnsupportedColumnCheckReference) for r in refs)

    # ---- cardinality: execution -----------------------------------------

    def test_min_items_executes_pass_and_fail(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._cardinality_ref(monkeypatch, "minItems", 2)
        # [] and ["x"] are under 2; ["a","b"] passes; NULL is skipped.
        con = self._list_table([["a", "b"], ["x"], [], None])
        assert self._count(con, ref.get_query("duckdb")) == 2

    def test_max_items_executes_pass_and_fail(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._cardinality_ref(monkeypatch, "maxItems", 2)
        con = self._list_table([["a", "b"], ["a", "b", "c"], None])
        assert self._count(con, ref.get_query("duckdb")) == 1

    def test_unique_items_executes_pass_and_fail(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._cardinality_ref(monkeypatch, "uniqueItems", True)
        # ["dup","dup"] has a duplicate; ["a","b"] and [] are fine; NULL skipped.
        con = self._list_table([["dup", "dup"], ["a", "b"], [], None])
        assert self._count(con, ref.get_query("duckdb")) == 1

    def test_min_items_flags_empty_array(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._cardinality_ref(monkeypatch, "minItems", 1)
        con = self._list_table([[], ["a"], None])
        # Empty array violates minItems: 1; the NULL row is skipped.
        assert self._count(con, ref.get_query("duckdb")) == 1

    def test_failed_rows_returns_offending_arrays(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._cardinality_ref(monkeypatch, "minItems", 2)
        con = self._list_table([["a", "b"], ["x"], None])
        assert ref.supports_row_level_output is True
        failed_sql = ref.get_failed_rows_query("duckdb")
        assert failed_sql.upper().startswith("SELECT *")
        rows = con.sql(failed_sql).to_pyarrow().to_pylist()
        assert [r["tags"] for r in rows] == [["x"]]

    # ---- items (element) validation -------------------------------------

    def _items_ref(self, monkeypatch, items, *, kind, option_key=None):
        refs = self._array_refs(
            monkeypatch,
            {"name": "tags", "logicalType": "array", "items": items},
        )
        matches = [
            r
            for r in refs
            if isinstance(r, ArrayItemsCheckReference) and r._kind == kind and r._option_key == option_key
        ]
        assert len(matches) == 1
        return matches[0]

    def test_items_logical_type_cast_executes(self, monkeypatch: pytest.MonkeyPatch):
        import ibis

        ref = self._items_ref(monkeypatch, {"logicalType": "integer"}, kind="logicalType")
        assert ref.get_check()["name"] == "tags_array_items_logical_type_check"
        con = ibis.duckdb.connect()
        # "12" casts to integer; "x" and "1.5" do not (non-integral / non-numeric).
        con.create_table(
            "items",
            pa.table({"tags": pa.array([["12", "34"], ["x"], ["1.5"], None], type=pa.list_(pa.string()))}),
        )
        assert self._count(con, ref.get_query("duckdb")) == 2

    def test_items_min_length_executes(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._items_ref(
            monkeypatch,
            {"logicalType": "string", "logicalTypeOptions": {"minLength": 2}},
            kind="option",
            option_key="minLength",
        )
        assert ref.get_check()["name"] == "tags_array_items_minLength_check"
        con = self._list_table([["ab", "cd"], ["x"], [], None])
        # ["x"] has a length-1 element; empty passes vacuously; NULL skipped.
        assert self._count(con, ref.get_query("duckdb")) == 1

    def test_items_enum_executes(self, monkeypatch: pytest.MonkeyPatch):
        ref = self._items_ref(
            monkeypatch,
            {"enum": [{"value": "a"}, {"value": "b"}]},
            kind="enum",
        )
        assert ref.get_check()["name"] == "tags_array_items_enum_check"
        con = self._list_table([["a", "b"], ["a", "z"], [], None])
        # ["a","z"] contains "z" (not allowed); empty passes; NULL skipped.
        assert self._count(con, ref.get_query("duckdb")) == 1

    def test_items_uses_unnest_exists(self, monkeypatch: pytest.MonkeyPatch):
        import sqlglot
        from sqlglot import exp

        ref = self._items_ref(
            monkeypatch,
            {"logicalType": "string", "logicalTypeOptions": {"minLength": 2}},
            kind="option",
            option_key="minLength",
        )
        parsed = sqlglot.parse_one(ref._build_ast().sql("postgres"), read="postgres")
        assert len(list(parsed.find_all(exp.Unnest))) == 1
        assert len(list(parsed.find_all(exp.Exists))) == 1

    # ---- metadata gate + degradation ------------------------------------

    def test_cardinality_on_scalar_degrades_to_unsupported(self, monkeypatch: pytest.MonkeyPatch):
        refs = self._array_refs(
            monkeypatch,
            {"name": "code", "logicalType": "string", "logicalTypeOptions": {"minItems": 1}},
        )
        # No array cardinality check on a scalar column; degrades instead.
        assert not any(
            isinstance(r, LogicalTypeOptionsCheckReference) and r._option_key == "minItems" for r in refs
        )
        unsup = [r for r in refs if isinstance(r, UnsupportedColumnCheckReference)]
        assert any("requires logicalType: array" in r.error_message for r in unsup)

    def test_items_on_scalar_degrades_to_unsupported(self, monkeypatch: pytest.MonkeyPatch):
        refs = self._array_refs(
            monkeypatch,
            {"name": "code", "logicalType": "string", "items": {"logicalType": "integer"}},
        )
        assert not any(isinstance(r, ArrayItemsCheckReference) for r in refs)
        unsup = [r for r in refs if isinstance(r, UnsupportedColumnCheckReference)]
        assert any("items validation requires logicalType: array" in r.error_message for r in unsup)

    def test_array_logical_type_emits_no_standalone_type_check(self, monkeypatch: pytest.MonkeyPatch):
        import warnings as _warnings

        # logicalType: array alone is silent — no cast check, no warning (unlike
        # object, which still warns that no type check is generated).
        with _warnings.catch_warnings():
            _warnings.simplefilter("error")
            refs = self._array_refs(monkeypatch, {"name": "tags", "logicalType": "array"})
        assert not any(isinstance(r, LogicalTypeCheckReference) for r in refs)

    @pytest.mark.parametrize("payload", ["1); DROP TABLE x;--", "abc", "1 OR 1=1"])
    def test_injection_in_min_items_degrades_to_unsupported(
        self, monkeypatch: pytest.MonkeyPatch, payload: str
    ):
        refs = self._array_refs(
            monkeypatch,
            {"name": "tags", "logicalType": "array", "logicalTypeOptions": {"minItems": payload}},
        )
        # A non-numeric minItems is rejected at coercion -> unsupported, never SQL.
        assert not any(
            isinstance(r, LogicalTypeOptionsCheckReference) and r._option_key == "minItems" for r in refs
        )
        assert any(isinstance(r, UnsupportedColumnCheckReference) for r in refs)

    def test_items_unknown_logical_type_degrades(self, monkeypatch: pytest.MonkeyPatch):
        # An element logicalType with no SQL cast check (object/array) produces
        # no items logical-type check; it degrades to unsupported. (string, like
        # the scalar types, does have a VARCHAR cast and would be supported.)
        refs = self._array_refs(
            monkeypatch,
            {"name": "tags", "logicalType": "array", "items": {"logicalType": "object"}},
        )
        assert not any(isinstance(r, ArrayItemsCheckReference) for r in refs)
        assert any(isinstance(r, UnsupportedColumnCheckReference) for r in refs)


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


# ===================================================================
# Group G — Foreign-key / relationship checks (ODCS v3.2.0)
# ===================================================================


def _make_fk_contract(monkeypatch: pytest.MonkeyPatch, schemas: list[dict]) -> Contract:
    """Build a multi-schema contract with validation disabled, for FK tests."""
    monkeypatch.setattr("vowl.contracts.contract.validate_contract", lambda data, version: None)
    return Contract(
        {
            "apiVersion": get_latest_version(),
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "test-fk",
            "status": "active",
            "schema": schemas,
        }
    )


def _fk_refs(contract: Contract, schema_name: str) -> list:
    """Return only the foreign-key references filed under ``schema_name``."""
    refs = contract.get_check_references_by_schema()[schema_name]
    return [r for r in refs if isinstance(r, (PropertyForeignKeyCheckReference, SchemaForeignKeyCheckReference))]


def _scalar(con, query: str) -> int:
    rows = con.sql(query).to_pyarrow().to_pylist()
    assert len(rows) == 1
    return next(iter(rows[0].values()))


class TestForeignKeyCheck:
    """Referential-integrity checks auto-generated from ``relationships``."""

    # -- property-level, single column -------------------------------------

    def _orders_customers(self) -> list[dict]:
        return [
            {
                "name": "customers",
                "properties": [{"name": "id", "logicalType": "integer", "primaryKey": True}],
            },
            {
                "name": "orders",
                "properties": [
                    {"name": "order_id", "logicalType": "integer"},
                    {
                        "name": "customer_id",
                        "logicalType": "integer",
                        "relationships": [{"type": "foreignKey", "to": "customers.id"}],
                    },
                ],
            },
        ]

    def test_property_fk_shape(self, monkeypatch: pytest.MonkeyPatch):
        contract = _make_fk_contract(monkeypatch, self._orders_customers())
        fks = _fk_refs(contract, "orders")
        assert len(fks) == 1
        ref = fks[0]
        assert isinstance(ref, PropertyForeignKeyCheckReference)
        check = ref.get_check()
        assert check["name"] == "orders_customer_id_foreign_key_check"
        assert check["dimension"] == "consistency"
        assert check["type"] == "sql"
        assert check["mustBe"] == 0
        # Anti-join shape: NOT EXISTS + NULL exclusion (MATCH SIMPLE).
        sql = ref._build_ast().sql("postgres").upper()
        assert "NOT EXISTS" in sql
        # MATCH SIMPLE null exclusion (postgres renders "NOT col IS NULL").
        assert "IS NULL" in sql
        assert "COUNT(*)" in sql

    def test_property_fk_pass_and_null_skip(self, monkeypatch: pytest.MonkeyPatch):
        import ibis

        contract = _make_fk_contract(monkeypatch, self._orders_customers())
        ref = _fk_refs(contract, "orders")[0]

        con = ibis.duckdb.connect()
        con.create_table("customers", pa.table({"id": [1, 2, 3]}))
        # Every non-null customer_id exists; the NULL row is skipped (MATCH SIMPLE).
        con.create_table("orders", pa.table({"order_id": [10, 11, 12], "customer_id": [1, 2, None]}))
        assert _scalar(con, ref.get_query("duckdb")) == 0

    def test_property_fk_fail_on_orphan(self, monkeypatch: pytest.MonkeyPatch):
        import ibis

        contract = _make_fk_contract(monkeypatch, self._orders_customers())
        ref = _fk_refs(contract, "orders")[0]

        con = ibis.duckdb.connect()
        con.create_table("customers", pa.table({"id": [1, 2, 3]}))
        # 99 has no matching customer -> exactly one violating row.
        con.create_table("orders", pa.table({"order_id": [10, 11], "customer_id": [1, 99]}))
        assert _scalar(con, ref.get_query("duckdb")) == 1

        # Failed-rows projection returns only the offending from-table row.
        assert ref.supports_row_level_output is True
        failed_sql = ref.get_failed_rows_query("duckdb")
        assert failed_sql.upper().startswith("SELECT *")
        rows = con.sql(failed_sql).to_pyarrow().to_pylist()
        assert [r["customer_id"] for r in rows] == [99]

    # -- schema-level, composite -------------------------------------------

    def _composite_contract(self) -> list[dict]:
        return [
            {
                "name": "regions",
                "properties": [
                    {"name": "country", "logicalType": "string", "primaryKey": True},
                    {"name": "zone", "logicalType": "string", "primaryKey": True},
                ],
            },
            {
                "name": "stores",
                "properties": [
                    {"name": "store_country", "logicalType": "string"},
                    {"name": "store_zone", "logicalType": "string"},
                ],
                "relationships": [
                    {
                        "type": "foreignKey",
                        "from": ["stores.store_country", "stores.store_zone"],
                        "to": ["regions.country", "regions.zone"],
                    }
                ],
            },
        ]

    def test_composite_schema_fk_pass_and_fail(self, monkeypatch: pytest.MonkeyPatch):
        import ibis

        contract = _make_fk_contract(monkeypatch, self._composite_contract())
        fks = _fk_refs(contract, "stores")
        assert len(fks) == 1
        ref = fks[0]
        assert isinstance(ref, SchemaForeignKeyCheckReference)
        assert ref.get_check()["name"] == "stores_store_country_store_zone_foreign_key_check"

        con = ibis.duckdb.connect()
        con.create_table("regions", pa.table({"country": ["SG", "MY"], "zone": ["A", "B"]}))
        # ("SG","A") matches; the ("SG","B") pair does not.
        con.create_table(
            "stores",
            pa.table({"store_country": ["SG", "SG"], "store_zone": ["A", "B"]}),
        )
        assert _scalar(con, ref.get_query("duckdb")) == 1

    # -- self-referential --------------------------------------------------

    def test_self_referential_fk(self, monkeypatch: pytest.MonkeyPatch):
        import ibis

        schemas = [
            {
                "name": "employees",
                "properties": [
                    {"name": "id", "logicalType": "integer", "primaryKey": True},
                    {
                        "name": "manager_id",
                        "logicalType": "integer",
                        "relationships": [{"type": "foreignKey", "to": "employees.id"}],
                    },
                ],
            }
        ]
        contract = _make_fk_contract(monkeypatch, schemas)
        ref = _fk_refs(contract, "employees")[0]

        con = ibis.duckdb.connect()
        # Row 1 is a root (NULL manager -> skipped); 2->1, 3->1 resolve.
        con.create_table("employees", pa.table({"id": [1, 2, 3], "manager_id": [None, 1, 1]}))
        assert _scalar(con, ref.get_query("duckdb")) == 0
        # Break it: manager 42 does not exist.
        con2 = ibis.duckdb.connect()
        con2.create_table("employees", pa.table({"id": [1, 2], "manager_id": [None, 42]}))
        assert _scalar(con2, ref.get_query("duckdb")) == 1

    # -- notation equivalence ----------------------------------------------

    def test_shorthand_equals_fqn(self, monkeypatch: pytest.MonkeyPatch):
        fqn_schemas = [
            {
                "id": "cust_schema",
                "name": "customers",
                "properties": [{"id": "cust_id", "name": "id", "logicalType": "integer", "primaryKey": True}],
            },
            {
                "name": "orders",
                "properties": [
                    {
                        "name": "customer_id",
                        "logicalType": "integer",
                        "relationships": [{"type": "foreignKey", "to": "/schema/cust_schema/properties/cust_id"}],
                    }
                ],
            },
        ]
        contract_fqn = _make_fk_contract(monkeypatch, fqn_schemas)
        ref_fqn = _fk_refs(contract_fqn, "orders")[0]

        contract_sh = _make_fk_contract(monkeypatch, self._orders_customers())
        ref_sh = _fk_refs(contract_sh, "orders")[0]

        # Both notations resolve to the same target and thus the same SQL.
        assert ref_fqn._build_ast().sql("postgres") == ref_sh._build_ast().sql("postgres")

    # -- degrade / warning paths -------------------------------------------

    def test_target_not_unique_warns_but_generates(self, monkeypatch: pytest.MonkeyPatch):
        schemas = [
            {"name": "customers", "properties": [{"name": "id", "logicalType": "integer"}]},
            {
                "name": "orders",
                "properties": [
                    {
                        "name": "customer_id",
                        "logicalType": "integer",
                        "relationships": [{"type": "foreignKey", "to": "customers.id"}],
                    }
                ],
            },
        ]
        contract = _make_fk_contract(monkeypatch, schemas)
        with pytest.warns(UserWarning, match="not declared unique or primaryKey"):
            fks = _fk_refs(contract, "orders")
        assert len(fks) == 1
        assert isinstance(fks[0], PropertyForeignKeyCheckReference)

    def test_unresolvable_target_degrades(self, monkeypatch: pytest.MonkeyPatch):
        schemas = [
            {
                "name": "orders",
                "properties": [
                    {
                        "name": "customer_id",
                        "logicalType": "integer",
                        "relationships": [{"type": "foreignKey", "to": "customers.id"}],
                    }
                ],
            }
        ]
        contract = _make_fk_contract(monkeypatch, schemas)
        refs = contract.get_check_references_by_schema()["orders"]
        assert any(isinstance(r, UnsupportedColumnCheckReference) for r in refs)
        assert not any(isinstance(r, PropertyForeignKeyCheckReference) for r in refs)

    def test_composite_property_level_degrades(self, monkeypatch: pytest.MonkeyPatch):
        schemas = [
            {
                "name": "regions",
                "properties": [
                    {"name": "country", "logicalType": "string", "primaryKey": True},
                    {"name": "zone", "logicalType": "string", "primaryKey": True},
                ],
            },
            {
                "name": "stores",
                "properties": [
                    {
                        "name": "loc",
                        "logicalType": "string",
                        # Composite target on a property-level relationship is unsupported.
                        "relationships": [{"type": "foreignKey", "to": ["regions.country", "regions.zone"]}],
                    }
                ],
            },
        ]
        contract = _make_fk_contract(monkeypatch, schemas)
        refs = contract.get_check_references_by_schema()["stores"]
        assert any(isinstance(r, UnsupportedColumnCheckReference) for r in refs)

    def test_external_ref_without_origin_degrades(self, monkeypatch: pytest.MonkeyPatch):
        schemas = [
            {
                "name": "orders",
                "properties": [
                    {
                        "name": "customer_id",
                        "logicalType": "integer",
                        "relationships": [{"type": "foreignKey", "to": "other.yaml#/schema/c/properties/id"}],
                    }
                ],
            }
        ]
        # In-memory contract has origin=None -> external ref cannot resolve.
        contract = _make_fk_contract(monkeypatch, schemas)
        assert contract.origin is None
        refs = contract.get_check_references_by_schema()["orders"]
        unsup = [r for r in refs if isinstance(r, UnsupportedColumnCheckReference)]
        assert len(unsup) == 1

    def test_generated_fk_sql_is_injection_safe(self, monkeypatch: pytest.MonkeyPatch):
        from vowl.executors.security import validate_query_security

        contract = _make_fk_contract(monkeypatch, self._orders_customers())
        ref = _fk_refs(contract, "orders")[0]
        # The generated anti-join query passes the security validator.
        validate_query_security(ref.get_query("duckdb"), "duckdb")
