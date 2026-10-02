"""Tests for ``ValidationConfig.row_count_accuracy`` and the full-table match.

Under ``"accurate"`` every counted check that is not a certified row filter is
matched onto the exported table, so a row's copies come from the table. See
"Row count accuracy" in design/row-quality-statistics.md.
"""

from __future__ import annotations

import datetime
import decimal
import json

import pyarrow as pa
import pytest
import test_row_quality as rq

from vowl.adapters.ibis_adapter import IbisAdapter
from vowl.config import ValidationConfig
from vowl.validation.result import ValidationResult
from vowl.validation.result_row_quality import match_onto_table, row_keys, table_key_index
from vowl.validation.row_quality import pushdown
from vowl.validation.row_quality.merge import FetchedRows, merge_onto_table
from vowl.validation.row_quality.selection import (
    REASON_NO_EXPORT,
    REASON_TRUNCATED,
    REASON_UNKEYABLE,
    REASON_UNMATCHED,
)

_skip_contract_validation = rq._skip_contract_validation
unkeyed = rq.unkeyed

_ALL_FAILED = ["c < 0", "c > 5", "c = 2", "c IS NULL"]


def _level(level: str, **kwargs) -> ValidationConfig:
    return ValidationConfig(row_count_accuracy=level, **kwargs)


def _flagged(result, schema: str = "t") -> int:
    cells = result.get_annotated_output()["annotated"][schema].to_arrow().column("check_info").to_pylist()
    return sum(cell is not None for cell in cells)


def _spy_exports(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    original = IbisAdapter.export_table_as_arrow

    def export(self, schema_name: str):
        calls.append(schema_name)
        return original(self, schema_name)

    monkeypatch.setattr(IbisAdapter, "export_table_as_arrow", export)
    return calls


# ---------------------------------------------------------------------------
# Config


def test_config_rejects_an_unknown_level():
    with pytest.raises(ValueError, match="row_count_accuracy"):
        ValidationConfig(row_count_accuracy="exact")


def test_config_defaults_to_accurate_and_serialises():
    config = ValidationConfig()
    assert config.row_count_accuracy == "accurate"
    assert config.to_dict()["row_count_accuracy"] == "accurate"


# ---------------------------------------------------------------------------
# The three levels on the mixed checks


@pytest.mark.parametrize(
    ("level", "route"),
    [("accurate", "client_lookup"), ("balanced", "server_lookup"), ("fast", "server_lookup")],
)
@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_each_level_counts_the_mixed_checks(backend: str, level: str, route: str):
    con, result = rq._mixed(backend, _level(level))

    schema = rq._schema_row(result)
    assert (schema["total_rows"], schema["failed_rows"], schema["exact"]) == (11, rq._truth(con, _ALL_FAILED), True)
    checks = rq._check_rows(result)
    assert checks["negative"]["route"] == "server_predicate"
    assert checks["twos_distinct"]["route"] == route
    assert checks["twos_distinct"]["failed_rows"] == 3


def test_fast_never_exports(monkeypatch: pytest.MonkeyPatch):
    calls = _spy_exports(monkeypatch)
    _, result = rq._mixed("duckdb", _level("fast"))
    result.get_row_quality_df()

    assert calls == []


def test_balanced_matches_only_what_fast_counts_from_fetched_rows(unkeyed):
    con, balanced = rq._mixed("duckdb", _level("balanced"))
    _, fast = rq._mixed("duckdb", _level("fast"))

    assert rq._check_rows(fast)["twos_distinct"]["route"] == "client_returned_rows"
    twos = rq._check_rows(balanced)["twos_distinct"]
    assert (twos["route"], twos["failed_rows"], twos["exact"]) == ("client_lookup", 3, True)
    assert rq._schema_row(balanced)["failed_rows"] == rq._truth(con, _ALL_FAILED)


def test_certified_checks_alone_do_not_export(monkeypatch: pytest.MonkeyPatch):
    calls = _spy_exports(monkeypatch)
    con, result = rq._certified("duckdb")

    assert rq._schema_row(result)["failed_rows"] == rq._truth(con, ["c < 0", "c > 5", "c IS NULL"])
    assert calls == []


def test_counting_and_annotated_output_export_once(monkeypatch: pytest.MonkeyPatch):
    calls = _spy_exports(monkeypatch)
    _, result = rq._mixed("duckdb")
    result.get_row_quality_df()
    result.get_annotated_output()

    assert calls == ["t"]


# ---------------------------------------------------------------------------
# What the full-table match fixes


def _table(rows: str, columns: str = "id INTEGER, c INTEGER"):
    con = rq._connect("duckdb")
    con.raw_sql(f"CREATE TABLE t ({columns})")
    con.raw_sql(f"INSERT INTO t VALUES {rows}")
    return con


def test_distinct_counts_every_copy():
    con = _table("(1, 2), (1, 2), (1, 2), (2, 3)")
    check = {"name": "d", "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c = 2) AS s", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check])])

    row = rq._check_rows(result)["d"]
    assert (row["route"], row["failed_rows"], row["exact"]) == ("client_lookup", 3, True)
    assert rq._schema_row(result)["failed_rows"] == _flagged(result) == 3


def test_join_fan_out_does_not_inflate_a_pushdown_neighbour():
    con = _table("(1, -1), (1, -1), (2, -2), (3, 3)")
    con.raw_sql("CREATE TABLE l (id INTEGER)")
    con.raw_sql("INSERT INTO l VALUES (1), (1), (1), (2)")
    fan = {"name": "fan", "query": "SELECT t.* FROM t JOIN l ON t.id = l.id WHERE t.c < 0", "mustBe": 0}
    checks = [fan, rq._check("negative", "c < 0")]

    result = rq._validate(con, [rq._schema("t", checks)])

    rows = rq._check_rows(result)
    assert rows["fan"]["failed_rows"] == rows["negative"]["failed_rows"] == 3
    schema = rq._schema_row(result)
    assert (schema["failed_rows"], schema["exact"]) == (rq._truth(con, ["c < 0"]), True)
    assert _flagged(result) == 3


def test_a_cross_source_join_counts_the_true_rows():
    t_con = _table("(1, -1), (1, -1), (2, 5), (3, 5)")
    u_con = rq._connect("duckdb")
    u_con.raw_sql("CREATE TABLE u (id INTEGER)")
    u_con.raw_sql("INSERT INTO u VALUES (1), (1), (2)")
    joined = {"name": "in_u", "query": "SELECT t.* FROM t JOIN u ON t.id = u.id", "mustBe": 0}
    checks = [joined, rq._check("negative", "c < 0")]
    schemas = [rq._schema("t", checks), rq._schema("u", [], [{"name": "id"}])]

    result = rq._validate(t_con, schemas, adapters={"t": IbisAdapter(t_con), "u": IbisAdapter(u_con)})

    rows = rq._check_rows(result)
    assert (rows["in_u"]["route"], rows["in_u"]["failed_rows"], rows["in_u"]["exact"]) == ("client_lookup", 3, True)
    assert rows["negative"]["failed_rows"] == 2
    assert rq._schema_row(result)["failed_rows"] == 3


@pytest.mark.parametrize(
    "select",
    ["id, c * 1.5 AS c", "id, c + 100 AS c"],
)
def test_transformed_values_are_reported_as_unmatched(select: str):
    con = _table("(1, -1), (2, -2), (3, 3)")
    check = {"name": "moved", "query": f"SELECT COUNT(*) FROM (SELECT {select} FROM t WHERE c < 0) AS s", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check])])

    row = rq._check_rows(result)["moved"]
    assert (row["route"], row["reason"], row["exact"]) == ("client_lookup", REASON_UNMATCHED, False)
    assert rq._schema_row(result)["exact"] is False


def test_lowered_strings_partly_match():
    con = _table("('a'), ('A'), ('b')", "s VARCHAR")
    check = {"name": "low", "query": "SELECT COUNT(*) FROM (SELECT lower(s) AS s FROM t) AS q", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check], [{"name": "s"}])])

    row = rq._check_rows(result)["low"]
    # The lowered 'A' is 'a', a table row, so every key matches. The count
    # is the rows that hold the returned values, not the rows the check read.
    assert (row["route"], row["failed_rows"], row["exact"]) == ("client_lookup", 2, True)


def test_upper_strings_match_no_table_row():
    con = _table("('a'), ('b')", "s VARCHAR")
    check = {"name": "up", "query": "SELECT COUNT(*) FROM (SELECT upper(s) AS s FROM t) AS q", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check], [{"name": "s"}])])

    row = rq._check_rows(result)["up"]
    assert (row["failed_rows"], row["reason"], row["exact"]) == (0, REASON_UNMATCHED, False)


# ---------------------------------------------------------------------------
# Fallbacks


def test_a_truncated_local_check_is_counted_in_sql():
    con = _table("(1, 2), (1, 2), (2, 2), (3, 2), (4, -1)")
    distinct = {"name": "d", "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c = 2) AS s", "mustBe": 0}
    joined = {"name": "j", "query": "SELECT t.* FROM t JOIN t AS o ON t.id = o.id WHERE t.c < 0", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [distinct, joined])], ValidationConfig(max_failed_rows=1))

    rows = rq._check_rows(result)
    # Its fetched rows are cut short, so table_match counts it without them.
    assert (rows["d"]["route"], rows["d"]["failed_rows"], rows["d"]["exact"]) == ("server_lookup", 4, True)
    assert (rows["j"]["route"], rows["j"]["failed_rows"]) == ("client_lookup", 1)
    assert rq._schema_row(result)["failed_rows"] == rq._truth(con, ["c = 2", "c < 0"])


def test_a_truncated_cross_source_check_keeps_fetched_rows_and_its_neighbour_is_matched():
    _, result = rq._two_sources(ValidationConfig(max_failed_rows=1))

    rows = rq._check_rows(result)
    truncated = rows["id_in_u"]
    assert (truncated["route"], truncated["reason"], truncated["exact"]) == (
        "client_returned_rows",
        REASON_TRUNCATED,
        False,
    )
    assert rows["twos_distinct"]["route"] == "server_lookup"  # also cut to one row
    assert rq._schema_row(result)["exact"] is False


def test_a_failed_export_falls_back(monkeypatch: pytest.MonkeyPatch):
    # The cross-source check reads u through the export, so only the
    # result's export of t fails.
    monkeypatch.setattr(ValidationResult, "_fetch_full_table", lambda self, schema_name: None)
    _, result = rq._two_sources()

    row = rq._check_rows(result)["id_in_u"]
    assert (row["route"], row["reason"]) == ("client_returned_rows", REASON_NO_EXPORT)
    # twos_distinct keeps table_match, which counts it in SQL.
    assert rq._check_rows(result)["twos_distinct"]["route"] == "server_lookup"


def test_unkeyable_rows_fall_back(monkeypatch: pytest.MonkeyPatch):
    import vowl.validation.result as result_module

    def refuse(table, columns):
        raise TypeError("simulated: a value cannot be keyed")

    monkeypatch.setattr(result_module, "table_key_index", refuse)
    _, result = rq._two_sources()

    row = rq._check_rows(result)["id_in_u"]
    assert (row["route"], row["reason"]) == ("client_returned_rows", REASON_UNKEYABLE)


def test_the_total_comes_from_the_export():
    class StaleTotals(IbisAdapter):
        def get_total_rows(self, schema_name: str, max_rows: int = -1) -> int:
            return 99

        def run_arrow_query(self, sql: str):
            raise NotImplementedError

    con = _table("(1, -1), (2, -2), (3, 4)")
    result = rq._run_validation(
        rq._contract([rq._schema("t", [rq._check("negative", "c < 0")])]), adapters={"t": StaleTotals(con)}
    )

    schema = rq._schema_row(result)
    assert (schema["total_rows"], schema["failed_rows"], schema["passed_rows"], schema["exact"]) == (3, 2, 1, True)


# ---------------------------------------------------------------------------
# Values


def test_nan_negative_zero_null_and_case_count_as_their_own_rows():
    con = _table(
        "('nan'::DOUBLE, 'a'), ('nan'::DOUBLE, 'a'), (-0.0, 'A'), (0.0, 'A'), (NULL, NULL), (1.0, 'b')",
        "x DOUBLE, s VARCHAR",
    )
    query = "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE x IS NULL OR isnan(x) OR x = 0) AS q"
    check = {"name": "odd", "query": query, "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check], [{"name": "x"}, {"name": "s"}])])

    row = rq._check_rows(result)["odd"]
    assert (row["route"], row["failed_rows"], row["exact"]) == ("client_lookup", 5, True)
    assert _flagged(result) == 5


def _typed_table():
    con = rq._connect("duckdb")
    con.raw_sql(
        "CREATE TABLE t (s VARCHAR, i BIGINT, f DOUBLE, d DECIMAL(10, 2), dt DATE, tm TIME, ts TIMESTAMP,"
        " b BOOLEAN, n INTEGER[], flag INTEGER)"
    )
    con.raw_sql(
        "INSERT INTO t VALUES"
        " ('x', 1, 1.5, 1.25, DATE '2024-01-01', TIME '01:02:03', TIMESTAMP '2024-01-01 01:02:03', true, [1, 2], 1),"
        " ('x', 1, 1.5, 1.25, DATE '2024-01-01', TIME '01:02:03', TIMESTAMP '2024-01-01 01:02:03', true, [1, 2], 1),"
        " ('y', 2, 2.5, 2.50, DATE '2024-02-02', TIME '04:05:06', TIMESTAMP '2024-02-02 04:05:06', false, [3], 1),"
        " ('z', 3, 3.5, 3.75, DATE '2024-03-03', TIME '07:08:09', TIMESTAMP '2024-03-03 07:08:09', NULL, NULL, 0)"
    )
    return con


_TYPED_PROPERTIES = [{"name": name} for name in ("s", "i", "f", "d", "dt", "tm", "ts", "b", "n", "flag")]


def test_every_column_type_counts_and_annotates():
    con = _typed_table()
    distinct = {
        "name": "flagged_distinct",
        "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE flag = 1) AS q",
        "mustBe": 0,
    }

    result = rq._validate(con, [rq._schema("t", [distinct], _TYPED_PROPERTIES)])

    row = rq._check_rows(result)["flagged_distinct"]
    assert (row["route"], row["failed_rows"], row["exact"]) == ("client_lookup", 3, True)
    assert rq._schema_row(result)["failed_rows"] == _flagged(result) == 3


def test_annotated_output_concatenates_checks_with_different_types():
    con = _table("(1, -1), (2, -2), (3, 3)", "id BIGINT, c BIGINT")
    narrowed = {
        "name": "narrow",
        "query": "SELECT COUNT(*) FROM (SELECT CAST(id AS INTEGER) AS id, CAST(c AS INTEGER) AS c FROM t"
        " WHERE c < 0) AS q",
        "mustBe": 0,
    }
    checks = [narrowed, rq._check("negative", "c < 0")]

    result = rq._validate(con, [rq._schema("t", checks)])

    rows = result.get_annotated_output()["annotated"]["t"].to_arrow().to_pylist()
    flagged = {
        row["id"]: {item["check_name"] for item in json.loads(row["check_info"])} for row in rows if row["check_info"]
    }
    assert flagged == {1: {"narrow", "negative"}, 2: {"narrow", "negative"}}
    assert rq._schema_row(result)["failed_rows"] == 2


# ---------------------------------------------------------------------------
# Helpers


def test_date32_keys_equal_the_same_dates_read_back():
    days = pa.table({"d": pa.array([datetime.date(2024, 1, 1), None], type=pa.date32())})
    as_dates = pa.table({"d": pa.array([datetime.date(2024, 1, 1), None], type=pa.date64())})

    assert row_keys(days, ["d"]) == row_keys(as_dates.cast(days.schema), ["d"])


def test_match_onto_table_returns_every_copy_and_counts_missing_keys():
    table = pa.table({"a": [1, 1, 2, 3, 4]})
    rows = pa.table({"a": pa.array([1, 4, 4, 9], type=pa.int32())})

    assert match_onto_table(table, rows, ["a"]) == ([0, 1, 4], 1)


def test_merge_onto_table_ors_checks_onto_table_rows():
    table = pa.table({"a": [1, 1, 2, 3], "b": [decimal.Decimal("1.0")] * 4})
    index = table_key_index(table, ["a", "b"])
    fetched = [
        FetchedRows(check_id=0, table=table.slice(0, 1), key_columns=["a", "b"]),
        FetchedRows(check_id=1, table=table.slice(1, 2), key_columns=["a", "b"]),
    ]

    entries, exact, matched = merge_onto_table(None, fetched, ["a", "b"], table, index)

    assert exact is True
    assert sorted(entries) == [(2, 1), (3, 2)]
    assert matched == {0: (2, 0), 1: (3, 0)}


def test_annotated_output_concatenates_checks_with_unpromotable_types():
    con = _table("(1, -1), (2, -2), (3, 3)")
    as_text = {
        "name": "as_text",
        "query": "SELECT COUNT(*) FROM (SELECT id, CAST(c AS VARCHAR) AS c FROM t WHERE c < 0) AS q",
        "mustBe": 0,
    }
    scaled = {
        "name": "scaled",
        "query": "SELECT COUNT(*) FROM (SELECT id, c * 1.5 AS c FROM t WHERE c = 3) AS q",
        "mustBe": 0,
    }
    checks = [as_text, scaled, rq._check("negative", "c < 0")]

    result = rq._validate(con, [rq._schema("t", checks)])

    rows = result.get_annotated_output()["annotated"]["t"].to_arrow().to_pylist()
    flagged = {
        row["id"]: {item["check_name"] for item in json.loads(row["check_info"])} for row in rows if row["check_info"]
    }
    # The text '-1' casts back to the integer column, the scaled value matches no row.
    assert flagged == {1: {"as_text", "negative"}, 2: {"as_text", "negative"}}
    assert rq._check_rows(result)["scaled"]["reason"] == REASON_UNMATCHED


# ---------------------------------------------------------------------------
# Table match: values equal under their type but keyed apart

_NEG_ZERO = "CAST('-0.0' AS DOUBLE)"

# Values that are equal under their column type but print apart, so a check
# can catch one without the other. The wrong count depended on which of the
# two came first, so both orders are listed.
_EQUAL_BUT_APART = [
    pytest.param("DOUBLE", f"(1, {_NEG_ZERO}), (1, 0.0), (1, 0.0)", "c = 0 AND signbit(c)", id="negative_zero_first"),
    pytest.param("DOUBLE", f"(1, 0.0), (1, 0.0), (1, {_NEG_ZERO})", "c = 0 AND signbit(c)", id="negative_zero_last"),
    pytest.param("DOUBLE", f"(1, {_NEG_ZERO}), (1, 0.0), (1, 0.0)", "c = 0 AND NOT signbit(c)", id="positive_zero"),
    pytest.param(
        "REAL", "(1, CAST('-0.0' AS REAL)), (1, CAST(0.0 AS REAL))", "c = 0 AND signbit(c)", id="real_negative_zero"
    ),
    pytest.param(
        "DOUBLE", "(1, CAST('nan' AS DOUBLE)), (1, -CAST('nan' AS DOUBLE))", "isnan(c) AND signbit(c)", id="nan_sign"
    ),
    pytest.param(
        "INTERVAL",
        "(1, INTERVAL '1 month'), (1, INTERVAL '30 days')",
        "CAST(c AS VARCHAR) LIKE '%month%'",
        id="interval",
    ),
    pytest.param(
        "STRUCT(x DOUBLE)", f"(1, {{'x': {_NEG_ZERO}}}), (1, {{'x': 0.0}})", "signbit(c.x)", id="struct_negative_zero"
    ),
]


@pytest.mark.parametrize("level", ["balanced", "fast"])
@pytest.mark.parametrize(("column_type", "rows", "predicate"), _EQUAL_BUT_APART)
def test_table_match_keeps_values_equal_under_their_type_apart(level, column_type, rows, predicate):
    con = rq._connect("duckdb")
    con.raw_sql(f"CREATE TABLE t (id INTEGER, c {column_type})")
    con.raw_sql(f"INSERT INTO t VALUES {rows}, (2, NULL)")
    query = f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE {predicate}) AS q"
    check = {"name": "distinct", "query": query, "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check])], ValidationConfig(row_count_accuracy=level))

    truth = rq._truth(con, [predicate])
    row = rq._check_rows(result)["distinct"]
    assert (row["route"], row["failed_rows"], row["exact"]) == ("server_lookup", truth, True)
    assert rq._schema_row(result)["failed_rows"] == truth


def test_table_match_correlates_on_the_keys_not_the_raw_columns():
    spec = pushdown.KeySpec("duckdb", "SELECT * FROM t", ["id", "c"], [None, None], with_values=True)
    sql = pushdown.union_branch_sql(spec, pushdown.Branch(0, "server_lookup", "SELECT * FROM t WHERE c = 0"), 0)

    # DuckDB deduplicates the outer columns an EXISTS references before it
    # runs the subquery, so the subquery may only see the computed keys.
    correlation = sql.split("WHERE EXISTS", 1)[1]
    assert '"_vowl_a"' not in correlation
    assert '"_vowl_k"._vowl_c1' in correlation


def _table_on(backend: str, rows: str, columns: str = "id INTEGER, c INTEGER"):
    con = rq._connect(backend)
    con.raw_sql(f"CREATE TABLE t ({columns})")
    con.raw_sql(f"INSERT INTO t VALUES {rows}")
    return con


# ---------------------------------------------------------------------------
# Table match: failed keys that match no table row


@pytest.mark.parametrize("level", ["balanced", "fast"])
@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
@pytest.mark.parametrize("select", ["id, c * 1.0 AS c", "id, c * 1.5 AS c", "id, c + 100 AS c"])
def test_table_match_reports_transformed_values_as_unmatched(backend: str, level: str, select: str):
    con = _table_on(backend, "(1, 2), (1, 2), (2, 2), (3, 5)")
    inner = f"SELECT {select} FROM t WHERE c = 2"
    check = {"name": "moved", "query": f"SELECT COUNT(*) FROM ({inner}) AS s", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check])], _level(level))

    row = rq._check_rows(result)["moved"]
    assert (row["route"], row["reason"], row["exact"]) == ("server_lookup", REASON_UNMATCHED, False)
    assert row["failed_rows"] == 0  # the truth is 3
    assert rq._schema_row(result)["exact"] is False


@pytest.mark.parametrize("level", ["balanced", "fast"])
@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_table_match_reports_a_partial_match(backend: str, level: str):
    con = _table_on(backend, "(1, 2), (1, 2), (2, -1), (3, 5)")
    inner = "SELECT id, CASE WHEN c = 2 THEN c ELSE c + 100 END AS c FROM t WHERE c = 2 OR c < 0"
    check = {"name": "some", "query": f"SELECT COUNT(*) FROM ({inner}) AS s", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check])], _level(level))

    row = rq._check_rows(result)["some"]
    # The unchanged key finds both copies. The changed one finds no row.
    assert (row["failed_rows"], row["reason"], row["exact"]) == (2, REASON_UNMATCHED, False)


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_table_match_upper_strings_match_no_table_row(backend: str):
    con = _table_on(backend, "('a'), ('a'), ('b')", "s VARCHAR")
    check = {"name": "up", "query": "SELECT COUNT(*) FROM (SELECT upper(s) AS s FROM t) AS q", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check], [{"name": "s"}])], _level("balanced"))

    row = rq._check_rows(result)["up"]
    assert (row["route"], row["failed_rows"], row["reason"], row["exact"]) == (
        "server_lookup",
        0,
        REASON_UNMATCHED,
        False,
    )


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_table_match_lowered_strings_partly_match(backend: str):
    con = _table_on(backend, "('a'), ('A'), ('B')", "s VARCHAR")
    check = {"name": "low", "query": "SELECT COUNT(*) FROM (SELECT lower(s) AS s FROM t) AS q", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check], [{"name": "s"}])], _level("fast"))

    row = rq._check_rows(result)["low"]
    # 'a' is a table row, 'b' is not.
    assert (row["failed_rows"], row["reason"], row["exact"]) == (1, REASON_UNMATCHED, False)


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_table_match_cannot_see_a_value_that_lands_on_another_row(backend: str):
    con = _table_on(backend, "(1, -2), (1, -1), (1, -1), (2, 5)")
    check = {
        "name": "shifted",
        "query": "SELECT COUNT(*) FROM (SELECT id, c + 1 AS c FROM t WHERE c = -2) AS s",
        "mustBe": 0,
    }

    result = rq._validate(con, [rq._schema("t", [check])], _level("balanced"))

    row = rq._check_rows(result)["shifted"]
    # (1, -1) is a real row, so the key matches it and the check stays exact,
    # though the row the check read is (1, -2). Key matching cannot tell.
    assert (row["failed_rows"], row["exact"]) == (2, True)
    assert rq._truth(con, ["c = -2"]) == 1


@pytest.mark.parametrize("level", ["balanced", "fast"])
@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_table_match_stays_exact_when_every_key_matches(backend: str, level: str):
    con = _table_on(backend, "(1, 2), (1, 2), (2, 2), (3, NULL), (3, NULL), (4, 5)")
    checks = [
        {"name": "d", "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c = 2) AS s", "mustBe": 0},
        {"name": "n", "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c IS NULL) AS s", "mustBe": 0},
    ]

    result = rq._validate(con, [rq._schema("t", checks)], _level(level))

    rows = rq._check_rows(result)
    assert (rows["d"]["route"], rows["d"]["failed_rows"], rows["d"]["exact"]) == ("server_lookup", 3, True)
    # NULL keys match NULL table rows.
    assert (rows["n"]["failed_rows"], rows["n"]["exact"]) == (2, True)
    schema = rq._schema_row(result)
    assert (schema["failed_rows"], schema["exact"]) == (rq._truth(con, ["c = 2", "c IS NULL"]), True)


def test_table_match_is_not_exact_when_the_unmatched_count_fails(monkeypatch: pytest.MonkeyPatch):
    rq._spy(monkeypatch, fail_on="_vowl_unmatched")
    con = _table_on("duckdb", "(1, 2), (1, 2), (2, 2)")
    check = {"name": "d", "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c = 2) AS s", "mustBe": 0}

    result = rq._validate(con, [rq._schema("t", [check])], _level("fast"))

    row = rq._check_rows(result)["d"]
    assert (row["route"], row["failed_rows"], row["exact"]) == ("server_lookup", 3, False)
    assert row["reason"] != REASON_UNMATCHED
