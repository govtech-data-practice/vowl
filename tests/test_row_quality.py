"""Tests for the row-quality component (``vowl.validation.row_quality``).

Every number is compared with a truth computed directly on the connection, for
example ``SELECT COUNT(*) FROM t WHERE p1 OR p2``. See
design/row-quality-statistics.md.
"""

from __future__ import annotations

import json
import os
import warnings
from pathlib import Path

import conftest as test_conftest
import ibis
import pyarrow as pa
import pytest
import sqlglot

import vowl.contracts.contract as contract_module
from vowl.adapters.ibis_adapter import IbisAdapter
from vowl.config import ValidationConfig
from vowl.contracts.models import get_latest_version
from vowl.validation.row_quality import keys, pushdown
from vowl.validation.row_quality.certify import certify_row_query, certify_scalar_query
from vowl.validation.row_quality.selection import (
    REASON_COUNTS_VALUES,
    REASON_CROSS_SOURCE,
    REASON_ERROR,
    REASON_NO_MATCH_KEY,
    REASON_NOT_ROW_LEVEL,
    REASON_OPERATOR,
    REASON_PASSED_NOT_ATTRIBUTED,
    REASON_QUERY_FAILED,
    REASON_TRUNCATED,
    identifies_bad_rows,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# These tests assert each number against its truth, so they skip the golden
# file comparison the conftest wraps around validate_data.
_run_validation = test_conftest._ORIGINAL_VALIDATE_DATA


def _connect(backend: str):
    return ibis.duckdb.connect() if backend == "duckdb" else ibis.sqlite.connect()


def _contract(schemas: list[dict]) -> contract_module.Contract:
    return contract_module.Contract(
        {
            "apiVersion": get_latest_version(),
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "row-quality",
            "status": "active",
            "schema": schemas,
        }
    )


def _schema(name: str, checks: list[dict], properties: list[dict] | None = None) -> dict:
    return {
        "name": name,
        "properties": properties if properties is not None else [{"name": "id"}, {"name": "c"}],
        "quality": [{"type": "sql", **check} for check in checks],
    }


def _check(name: str, where: str, *, table: str = "t", dimension: str = "validity", **operator) -> dict:
    return {
        "name": name,
        "dimension": dimension,
        "query": f"SELECT COUNT(*) FROM {table} WHERE {where}",
        **(operator or {"mustBe": 0}),
    }


def _count(con, sql: str) -> int:
    return int(con.raw_sql(sql).fetchone()[0])


def _truth(con, predicates: list[str], table: str = "t") -> int:
    where = " OR ".join(f"({p})" for p in predicates)
    return _count(con, f"SELECT COUNT(*) FROM {table} WHERE {where}")


def _validate(con, schemas: list[dict], config: ValidationConfig | None = None, adapters=None):
    adapters = adapters or {schema["name"]: IbisAdapter(con) for schema in schemas}
    return _run_validation(_contract(schemas), adapters=adapters, config=config)


def _schema_row(result, schema: str = "t") -> dict:
    rows = result.get_dq_metrics_df(by="schema").to_arrow().to_pylist()
    return next(row for row in rows if row["schema_name"] == schema)


def _dimension_rows(result, schema: str = "t") -> dict[str, dict]:
    rows = result.get_dq_metrics_df(by="dimension").to_arrow().to_pylist()
    return {row["dimension"]: row for row in rows if row["schema_name"] == schema}


def _check_rows(result) -> dict[str, dict]:
    return {row["check_name"]: row for row in result.get_dq_metrics_df(by="check").to_arrow().to_pylist()}


@pytest.fixture(autouse=True)
def _skip_contract_validation(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(contract_module, "validate_contract", lambda data, version: None)


_ROWS = "(1,-1),(1,-1),(2,6),(3,2),(3,2),(3,2),(4,3),(5,1),(6,-2),(7,NULL),(7,NULL)"

_MIXED_CHECKS = [
    _check("negative", "c < 0"),
    _check("too_big", "c > 5", mustBeLessThan=1),
    {
        "name": "twos_distinct",
        "dimension": "uniqueness",
        "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c = 2) AS s",
        "mustBe": 0,
    },
    _check("threes_tolerated", "c = 3", dimension="completeness", mustBeLessThan=100),
    _check("ones_inverted", "c = 1", mustBeGreaterThan=100),
    _check("null_c", "c IS NULL", dimension="completeness"),
]


def _mixed(backend: str, config: ValidationConfig | None = None):
    con = _connect(backend)
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql(f"INSERT INTO t VALUES {_ROWS}")
    return con, _validate(con, [_schema("t", _MIXED_CHECKS)], config)


# ---------------------------------------------------------------------------
# Truth
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_counts_match_the_truth_per_schema_and_dimension(backend: str):
    con, result = _mixed(backend)

    schema = _schema_row(result)
    assert schema["total_rows"] == 11
    assert schema["failed_rows"] == _truth(con, ["c < 0", "c > 5", "c = 2", "c IS NULL"]) == 9
    assert schema["passed_rows"] == 2
    assert schema["pass_rate"] == pytest.approx(2 / 11)
    assert schema["approximate"] is False

    dimensions = _dimension_rows(result)
    assert dimensions["validity"]["failed_rows"] == _truth(con, ["c < 0", "c > 5"])
    assert dimensions["uniqueness"]["failed_rows"] == 3
    assert dimensions["completeness"]["failed_rows"] == 2

    checks = _check_rows(result)
    assert checks["negative"]["route"] == "server_predicate"
    assert checks["negative"]["attributed_rows"] == 3
    # DISTINCT returns one row for three copies, so it is matched onto the table.
    assert checks["twos_distinct"]["route"] == "server_lookup"
    assert checks["twos_distinct"]["reason"] == "not certified for pushdown: uses a FROM that is not the table itself"
    assert checks["twos_distinct"]["attributed_rows"] == 3
    assert checks["ones_inverted"]["row_level"] is False
    assert checks["ones_inverted"]["reason"] == REASON_OPERATOR
    threes = checks["threes_tolerated"]
    assert (threes["route"], threes["reason"]) == ("", REASON_PASSED_NOT_ATTRIBUTED)
    assert (threes["scalar_count"], threes["attributed_rows"], threes["approximate"]) == (1, None, False)


def test_attribute_tolerated_rows_counts_passed_checks_as_failed():
    con, result = _mixed("duckdb", ValidationConfig(attribute_tolerated_rows=True))

    schema = _schema_row(result)
    assert schema["failed_rows"] == _truth(con, ["c < 0", "c > 5", "c = 2", "c IS NULL", "c = 3"]) == 10
    threes = _check_rows(result)["threes_tolerated"]
    assert (threes["route"], threes["reason"], threes["attributed_rows"]) == ("server_predicate", "", 1)


def test_passed_checks_run_no_query_by_default(monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []
    original = IbisAdapter.run_arrow_query
    monkeypatch.setattr(IbisAdapter, "run_arrow_query", lambda self, sql: calls.append(sql) or original(self, sql))
    _, result = _mixed("duckdb")

    result.get_dq_metrics_df()

    assert calls and not any("c = 3" in sql for sql in calls)


def test_a_dimension_with_only_passed_checks_has_no_failed_rows():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (2, 3), (3, 4)")
    checks = [_check("negative", "c < 0"), _check("threes", "c = 3", dimension="completeness", mustBeLessThan=5)]

    result = _validate(con, [_schema("t", checks)])

    completeness = _dimension_rows(result)["completeness"]
    assert (completeness["failed_rows"], completeness["pass_rate"], completeness["approximate"]) == (0, 1.0, False)
    assert (completeness["checks_row_level"], completeness["checks_not_attributable"]) == (1, 0)
    assert _schema_row(result)["failed_rows"] == 1


def test_a_passed_check_with_no_count_is_not_attributed(monkeypatch: pytest.MonkeyPatch):
    import vowl.validation.row_quality.selection as selection_module

    original = selection_module.scalar_row_count
    monkeypatch.setattr(
        selection_module,
        "scalar_row_count",
        lambda cr: None if cr.check_name == "threes_tolerated" else original(cr),
    )
    _, result = _mixed("duckdb")

    threes = _check_rows(result)["threes_tolerated"]
    assert (threes["reason"], threes["attributed_rows"], threes["approximate"]) == (
        REASON_PASSED_NOT_ATTRIBUTED,
        None,
        False,
    )
    assert _schema_row(result)["approximate"] is False


def test_print_summary_sums_failed_rows_without_attributing(capsys: pytest.CaptureFixture[str]):
    _, result = _mixed("duckdb")

    result.print_summary()

    # The basic tier adds up each check's own count. twos_distinct reports one
    # row for three copies, so the sum is 8 while the attributed count is 9.
    assert "Failed Rows (approximate): 8" in capsys.readouterr().out


def test_get_check_results_df_has_the_resolved_dimension():
    _, result = _mixed("duckdb")

    rows = {row["check_name"]: row for row in result.get_check_results_df().to_arrow().to_pylist()}

    assert rows["twos_distinct"]["dimension"] == "uniqueness"
    assert rows["id_column_exists_check"]["dimension"] == "conformity"


def test_get_dq_metrics_df_rejects_an_unknown_grouping():
    _, result = _mixed("duckdb")

    with pytest.raises(ValueError, match="by must be one of"):
        result.get_dq_metrics_df(by="table")


# ---------------------------------------------------------------------------
# Adversarial fixtures: every one must match its truth
# ---------------------------------------------------------------------------


def test_duckdb_negative_zero_and_nan_are_kept_apart():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c DOUBLE)")
    con.raw_sql("INSERT INTO t VALUES (1, -0.0), (1, 0.0), (2, 'NaN'), (2, 'NaN'), (3, 1.5)")
    checks = [
        _check("neg_zero", "CAST(c AS VARCHAR) = '-0.0'"),
        _check("pos_zero", "CAST(c AS VARCHAR) = '0.0'"),
        _check("nan", "isnan(c)"),
    ]

    result = _validate(con, [_schema("t", checks)])

    assert _schema_row(result)["failed_rows"] == 4


def test_duckdb_nested_and_blob_columns():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER[], s STRUCT(a VARCHAR), b BLOB)")
    con.raw_sql(
        "INSERT INTO t VALUES "
        "(1, [1, 2], {'a': 'x, y'}, 'q'::BLOB), (1, [1, 2], {'a': 'x, y'}, 'q'::BLOB), "
        "(2, [3], {'a': 'x'}, 'r'::BLOB), (3, [], {'a': 'y'}, NULL)"
    )
    checks = [_check("first", "len(c) = 2"), _check("second", "s.a = 'x'"), _check("third", "b IS NULL")]
    properties = [{"name": "id"}, {"name": "c"}, {"name": "s"}, {"name": "b"}]

    result = _validate(con, [_schema("t", checks, properties)])

    assert _schema_row(result)["failed_rows"] == 4
    assert _schema_row(result)["approximate"] is False


def test_duckdb_data_columns_named_like_vowl_aliases_do_not_collide():
    con = _connect("duckdb")
    con.raw_sql('CREATE TABLE t ("_vowl_copies" INTEGER, "_vowl_c0" INTEGER)')
    con.raw_sql("INSERT INTO t VALUES (10, 1), (10, 1), (10, 1), (5, 2)")
    checks = [_check("big", '"_vowl_copies" > 5'), _check("one", '"_vowl_c0" = 1')]

    result = _validate(con, [_schema("t", checks, [{"name": "_vowl_copies"}, {"name": "_vowl_c0"}])])

    assert _schema_row(result)["failed_rows"] == 3


def test_sqlite_nocase_values_caught_by_different_checks_stay_apart():
    con = _connect("sqlite")
    con.raw_sql("CREATE TABLE t (id INTEGER, c TEXT COLLATE NOCASE)")
    con.raw_sql("INSERT INTO t VALUES (1, 'a'), (1, 'A'), (1, 'a '), (1, 'a')")
    checks = [
        _check("lower", "unicode(c) = 97 AND length(c) = 1"),
        _check("upper", "unicode(c) = 65"),
        _check("padded", "length(c) = 2"),
    ]

    result = _validate(con, [_schema("t", checks)])

    assert _schema_row(result)["failed_rows"] == 4


def test_sqlite_rtrim_values_caught_by_different_checks_stay_apart():
    con = _connect("sqlite")
    con.raw_sql("CREATE TABLE t (id INTEGER, c TEXT COLLATE RTRIM)")
    con.raw_sql("INSERT INTO t VALUES (1, 'a'), (1, 'a '), (1, 'a  '), (1, 'a')")
    checks = [
        _check("bare", "length(c) = 1"),
        _check("one_space", "length(c) = 2"),
        _check("two_spaces", "length(c) = 3"),
    ]

    result = _validate(con, [_schema("t", checks)])

    assert all(row["route"] == "server_predicate" for row in _check_rows(result).values() if row["status"] == "FAILED")
    assert _schema_row(result)["failed_rows"] == 4


def test_duckdb_interval_and_bit_columns_are_counted_without_an_annotated_table():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTERVAL, b BIT)")
    con.raw_sql(
        "INSERT INTO t VALUES (1, INTERVAL 1 DAY, '101'::BIT), (1, INTERVAL 1 DAY, '101'::BIT), "
        "(2, INTERVAL 2 DAY, '1'::BIT), (3, INTERVAL 3 DAY, '0'::BIT)"
    )
    checks = [_check("one_day", "c = INTERVAL 1 DAY"), _check("zero", "b = '0'::BIT")]
    properties = [{"name": "id"}, {"name": "c"}, {"name": "b"}]

    result = _validate(con, [_schema("t", checks, properties)])

    assert _check_rows(result)["one_day"]["route"] == "server_predicate"
    assert _schema_row(result)["failed_rows"] == 3
    assert _schema_row(result)["approximate"] is False
    # Arrow cannot export INTERVAL, so annotated output keeps the checks as residues.
    output = result.get_annotated_output()
    assert "t" not in output["annotated"]
    assert {"t::one_day", "t::zero"} <= set(output["residues"])


def test_sqlite_mixed_types_in_one_column_stay_apart():
    con = _connect("sqlite")
    con.raw_sql("CREATE TABLE t (id, c)")
    con.raw_sql("INSERT INTO t VALUES (1, 1), (1, 1.0), (1, '1'), (1, 1), (2, 0.30000000000000004), (2, 0.3)")
    checks = [
        _check("integer", "typeof(c) = 'integer'"),
        _check("real", "typeof(c) = 'real' AND c > 0.9"),
        _check("text", "typeof(c) = 'text'"),
        _check("close_to_point_three", "typeof(c) = 'real' AND c < 0.30000000000000003"),
        _check("point_three", "typeof(c) = 'real' AND c > 0.3 AND c < 0.5"),
    ]

    result = _validate(con, [_schema("t", checks)])

    assert _schema_row(result)["failed_rows"] == 6


def test_duplicates_count_once_per_copy_not_once_per_check():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, 1), (1, 1), (1, 1), (2, 2)")
    checks = [_check("a", "c = 1"), _check("b", "id = 1"), _check("c", "c < 2")]

    result = _validate(con, [_schema("t", checks)])

    assert _schema_row(result)["failed_rows"] == 3
    assert {row["attributed_rows"] for row in _check_rows(result).values() if row["row_level"] and row["route"]} == {3}


# ---------------------------------------------------------------------------
# Certification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM t WHERE c > 1",
        "SELECT t.* FROM t WHERE c IS NULL",
        "SELECT * FROM t",
        "SELECT * FROM t AS a WHERE a.c IN (SELECT c FROM t WHERE c IS NOT NULL GROUP BY c HAVING COUNT(*) > 1)",
        "SELECT * FROM t AS f WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = f.id)",
    ],
)
def test_certification_accepts_pure_row_filters(query: str):
    assert certify_row_query(query, "t", "duckdb") == (True, "")


@pytest.mark.parametrize(
    ("query", "rule"),
    [
        ("SELECT DISTINCT * FROM t", "DISTINCT"),
        ("SELECT * FROM t GROUP BY c", "GROUP BY"),
        ("SELECT * FROM t LIMIT 5", "LIMIT"),
        ("SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY c) = 1", "QUALIFY"),
        ("SELECT * FROM t JOIN u ON t.id = u.id", "a join"),
        ("SELECT * FROM t, u", "a join"),
        ("SELECT * FROM t UNION ALL SELECT * FROM t", "a set operation"),
        ("WITH x AS (SELECT * FROM t) SELECT * FROM x", "WITH"),
        ("SELECT c FROM t", "a select list that is not *"),
        ("SELECT * FROM u", "a FROM that is not the table itself"),
        ("SELECT * FROM t WHERE random() < 0.5", "a nondeterministic function"),
        ("SELECT * FROM t WHERE c < now()", "a nondeterministic function"),
        ("SELECT * FROM t TABLESAMPLE 10%", "TABLESAMPLE"),
        ("SELECT * FROM t HAVING COUNT(*) > 1", "HAVING"),
        ("SELECT * FROM t OFFSET 5", "LIMIT"),
        ("SELECT * FROM t ORDER BY ROW_NUMBER() OVER ()", "a window function"),
        ("SELECT * FROM t ORDER BY (SELECT 1)", "a subquery outside WHERE"),
        ("SELECT * FROM t, LATERAL (SELECT 1) AS x", "a join"),
        ("SELECT * FROM t PIVOT (SUM(c) FOR k IN (1, 2))", "PIVOT"),
        ("SELECT * FROM other.t", "a FROM that is not the table itself"),
    ],
)
def test_certification_rejects_other_shapes(query: str, rule: str):
    assert certify_row_query(query, "t", "duckdb") == (False, rule)


@pytest.mark.parametrize(
    ("query", "dialect"),
    [("SELECT TOP 5 * FROM t", "tsql"), ("SELECT * FROM t FETCH FIRST 5 ROWS ONLY", "postgres")],
)
def test_certification_rejects_top_and_fetch(query: str, dialect: str):
    assert certify_row_query(query, "t", dialect) == (False, "LIMIT")


def test_certification_matches_the_qualifiers_the_query_writes():
    assert certify_row_query("SELECT * FROM t", "db.t", "duckdb") == (True, "")
    assert certify_row_query("SELECT * FROM db.t", "db.t", "duckdb") == (True, "")
    assert certify_row_query("SELECT * FROM x.t", "db.t", "duckdb")[0] is False


def test_certification_accepts_a_single_count():
    assert certify_scalar_query("SELECT COUNT(*) FROM t", "count", "duckdb") == (True, "")
    assert certify_scalar_query("SELECT COUNT(1) FROM t", "count", "duckdb") == (True, "")
    assert certify_scalar_query("SELECT COUNT(c) FROM t", "count", "duckdb") == (True, "")
    assert certify_scalar_query("SELECT COUNT(*) AS n FROM t", "count", "duckdb") == (True, "")
    assert certify_scalar_query("SELECT COUNT(*) + COUNT(c) FROM t", "count", "duckdb") == (
        False,
        "more than one COUNT",
    )
    assert certify_scalar_query("SELECT * FROM t", "none", "duckdb") == (True, "")


_COMPOSITE_KEY = [
    {"name": "a", "logicalType": "integer", "primaryKey": True, "primaryKeyPosition": 1},
    {"name": "b", "logicalType": "integer", "primaryKey": True, "primaryKeyPosition": 2},
]


@pytest.mark.parametrize("dialect", ["duckdb", "sqlite", "postgres", "spark", "tsql", "bigquery", "trino"])
def test_composite_primary_key_certifies_as_a_row_filter(dialect: str):
    from vowl.contracts.check_reference import CompositePrimaryKeyCheckReference
    from vowl.validation.row_quality.certify import certify_check

    refs = _contract([_schema("t", [], properties=_COMPOSITE_KEY)]).get_check_references_by_schema()["t"]
    (ref,) = [r for r in refs if isinstance(r, CompositePrimaryKeyCheckReference)]
    assert certify_check(ref, "t", dialect, use_try_cast=False) == (True, "")
    assert certify_row_query(ref.get_row_query(dialect), "t", dialect) == (True, "")


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_composite_primary_key_is_pushed_down(backend: str):
    con = _connect(backend)
    con.raw_sql("CREATE TABLE t (a INTEGER, b INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, 1), (1, 2), (1, 1), (NULL, 3), (2, 1)")

    result = _validate(con, [_schema("t", [], properties=_COMPOSITE_KEY)])

    check = _check_rows(result)["t_a_b_primary_key_check"]
    assert (check["status"], check["route"], check["attributed_rows"]) == ("FAILED", "server_predicate", 3)
    assert _schema_row(result)["failed_rows"] == 3


def test_count_of_an_expression_skips_null_rows_and_is_pushed_down():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, 1), (2, NULL), (3, 5), (4, 2)")
    checks = [{"name": "count_c", "query": "SELECT COUNT(c) FROM t WHERE c < 3 OR c IS NULL", "mustBe": 0}]

    result = _validate(con, [_schema("t", checks)])

    row = _check_rows(result)["count_c"]
    assert row["route"] == "server_predicate"
    assert row["attributed_rows"] == 2
    assert _schema_row(result)["failed_rows"] == 2
    assert _schema_row(result)["approximate"] is False
    annotated = result.get_annotated_output()["annotated"]["t"].to_arrow().to_pylist()
    assert sorted(row["id"] for row in annotated if row["check_info"]) == [1, 4]


def test_aliased_count_is_counted_and_annotated():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (2, 3), (3, -5)")
    checks = [{"name": "negative", "query": "SELECT COUNT(*) AS n FROM t WHERE c < 0", "mustBe": 0}]

    result = _validate(con, [_schema("t", checks)])

    row = _check_rows(result)["negative"]
    assert row["route"] == "server_predicate"
    assert row["attributed_rows"] == 2
    assert _schema_row(result)["approximate"] is False
    output = result.get_annotated_output()
    assert "t::negative" not in output["residues"]
    annotated = output["annotated"]["t"].to_arrow().to_pylist()
    assert sorted(row["id"] for row in annotated if row["check_info"]) == [1, 3]


def test_every_generated_check_is_certified():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c VARCHAR, r INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, 'a', 1), (1, NULL, 9), (2, 'zz', 3)")
    properties = [
        {"name": "id", "logicalType": "integer", "unique": True},
        {
            "name": "c",
            "logicalType": "string",
            "required": True,
            "logicalTypeOptions": {"maxLength": 1},
            "quality": [
                {"type": "library", "metric": "invalidValues", "arguments": {"validValues": ["a"]}, "mustBe": 0}
            ],
        },
        {"name": "r", "logicalType": "integer"},
    ]

    result = _validate(con, [_schema("t", [], properties)])

    failed = [row for row in _check_rows(result).values() if row["status"] == "FAILED"]
    assert failed
    assert all(row["route"] == "server_predicate" for row in failed), failed
    assert _schema_row(result)["failed_rows"] == _truth(con, ["id = 1", "c IS NULL", "length(c) > 1"])


# ---------------------------------------------------------------------------
# Chunking, probe and scale
# ---------------------------------------------------------------------------


def _many_checks(con, count: int, rows: int) -> list[dict]:
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    values = ", ".join(f"({i}, {i % (count + 5)})" for i in range(rows))
    con.raw_sql(f"INSERT INTO t VALUES {values}")
    # Every row twice, so duplicates spread across chunks.
    con.raw_sql("INSERT INTO t SELECT * FROM t")
    return [_check(f"c_is_{k}", f"c = {k} OR c = {(k + 1) % count}") for k in range(count)]


@pytest.mark.parametrize("max_branches", [1, 63, 64, 250])
def test_chunk_size_does_not_change_the_numbers(monkeypatch: pytest.MonkeyPatch, max_branches: int):
    monkeypatch.setattr(pushdown, "MAX_BRANCHES", max_branches)
    con = _connect("duckdb")
    checks = _many_checks(con, 70, 200)

    result = _validate(con, [_schema("t", checks)])

    assert _schema_row(result)["failed_rows"] == _count(con, "SELECT COUNT(*) FROM t WHERE c < 70")
    assert _schema_row(result)["approximate"] is False
    counts = {cr.check_name: cr.failed_rows_count for cr in result.check_results}
    assert all(row["attributed_rows"] == counts[name] for name, row in _check_rows(result).items() if row["route"])


def test_more_than_five_hundred_checks_on_sqlite():
    # SQLite rejects more than 500 terms in one compound SELECT.
    con = _connect("sqlite")
    checks = _many_checks(con, 510, 1100)

    result = _validate(con, [_schema("t", checks)])

    assert _schema_row(result)["failed_rows"] == _count(con, "SELECT COUNT(*) FROM t WHERE c < 510")
    assert _schema_row(result)["approximate"] is False


def test_a_branch_that_fails_is_not_attributable(monkeypatch: pytest.MonkeyPatch):
    original = IbisAdapter.run_arrow_query

    def failing(self, sql: str):
        if "777" in sql:
            raise RuntimeError("simulated invalid branch")
        return original(self, sql)

    monkeypatch.setattr(IbisAdapter, "run_arrow_query", failing)
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (2, 777), (3, 9)")
    checks = [_check("negative", "c < 0"), _check("marker", "c = 777"), _check("nine", "c = 9")]

    result = _validate(con, [_schema("t", checks)])

    checks_df = _check_rows(result)
    marker = checks_df["marker"]
    assert (marker["row_level"], marker["route"], marker["reason"]) == (True, "", REASON_QUERY_FAILED)
    assert (marker["scalar_count"], marker["attributed_rows"], marker["approximate"]) == (1, None, True)
    schema = _schema_row(result)
    # The marker's row is left out, so the numbers are approximate.
    assert schema["failed_rows"] == 2
    assert schema["approximate"] is True
    assert (schema["checks_not_row_level"], schema["checks_not_attributable"]) == (0, 1)


def test_single_scan_form_matches_the_union_form(monkeypatch: pytest.MonkeyPatch):
    statements: list[str] = []
    original = IbisAdapter.run_arrow_query

    def spy(self, sql: str):
        statements.append(sql)
        return original(self, sql)

    monkeypatch.setattr(IbisAdapter, "run_arrow_query", spy)
    monkeypatch.setattr(pushdown, "SINGLE_SCAN_DIALECTS", {"duckdb"})
    con, result = _mixed("duckdb")

    assert _schema_row(result)["failed_rows"] == 9
    assert _dimension_rows(result)["validity"]["failed_rows"] == 4
    assert any("_vowl_scan" in sql for sql in statements)


# ---------------------------------------------------------------------------
# Cap independence, routes and keys
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cap", [-1, 0, 1])
def test_max_failed_rows_does_not_change_pushdown_numbers(cap: int):
    _, result = _mixed("duckdb", ValidationConfig(max_failed_rows=cap))

    assert _schema_row(result)["failed_rows"] == 9
    assert _schema_row(result)["approximate"] is False


# ---------------------------------------------------------------------------
# Dialects without a key entry
# ---------------------------------------------------------------------------

_CERTIFIED_CHECKS = [check for check in _MIXED_CHECKS if check["name"] != "twos_distinct"]


@pytest.fixture
def unkeyed(monkeypatch: pytest.MonkeyPatch):
    """Treat every dialect as one without a key entry, which keys on plain columns."""
    monkeypatch.setattr(keys, "KEYED_DIALECTS", frozenset())
    monkeypatch.setattr(pushdown, "key_expression", lambda column_sql, dtype, dialect: column_sql)


def _spy(monkeypatch: pytest.MonkeyPatch, fail_on: str | None = None) -> list[str]:
    statements: list[str] = []
    original = IbisAdapter.run_arrow_query

    def spy(self, sql: str):
        statements.append(sql)
        if fail_on is not None and fail_on in sql:
            raise RuntimeError("simulated failure")
        return original(self, sql)

    monkeypatch.setattr(IbisAdapter, "run_arrow_query", spy)
    return statements


def _certified(backend: str, config: ValidationConfig | None = None):
    con = _connect(backend)
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql(f"INSERT INTO t VALUES {_ROWS}")
    return con, _validate(con, [_schema("t", _CERTIFIED_CHECKS)], config)


def test_collation_does_not_merge_rows_without_a_key_entry(unkeyed, monkeypatch: pytest.MonkeyPatch):
    statements = _spy(monkeypatch)
    con = _connect("sqlite")
    con.raw_sql("CREATE TABLE t (name TEXT COLLATE NOCASE)")
    con.raw_sql("INSERT INTO t VALUES ('a'), ('A'), ('A'), ('b')")
    checks = [_check("lower_a", "unicode(name) = 97"), _check("upper_a", "unicode(name) = 65")]

    result = _validate(con, [_schema("t", checks, properties=[{"name": "name"}])])

    checks_df = _check_rows(result)
    assert (checks_df["lower_a"]["route"], checks_df["lower_a"]["attributed_rows"]) == ("server_predicate", 1)
    assert (checks_df["upper_a"]["route"], checks_df["upper_a"]["attributed_rows"]) == ("server_predicate", 2)
    schema = _schema_row(result)
    # Grouping on the plain NOCASE column merges the three rows into one of two copies.
    assert (schema["total_rows"], schema["failed_rows"], schema["approximate"]) == (4, 3, False)
    assert not any("_vowl_per_row" in sql for sql in statements)


def test_plain_column_keys_merge_rows_when_the_keyless_count_cannot_run(unkeyed, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pushdown.PushdownRunner, "run_mask_histogram", lambda self, branches: None)
    con = _connect("sqlite")
    con.raw_sql("CREATE TABLE t (name TEXT COLLATE NOCASE)")
    con.raw_sql("INSERT INTO t VALUES ('a'), ('A'), ('A'), ('b')")
    checks = [_check("lower_a", "unicode(name) = 97"), _check("upper_a", "unicode(name) = 65")]

    result = _validate(con, [_schema("t", checks, properties=[{"name": "name"}])])

    schema = _schema_row(result)
    assert (schema["failed_rows"], schema["approximate"]) == (2, True)


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_certified_checks_are_exact_without_a_key_entry(backend: str, request: pytest.FixtureRequest):
    _, keyed = _certified(backend)
    request.getfixturevalue("unkeyed")
    con, unkeyed_result = _certified(backend)

    schema = _schema_row(unkeyed_result)
    assert schema["failed_rows"] == _truth(con, ["c < 0", "c > 5", "c IS NULL"]) == _schema_row(keyed)["failed_rows"]
    assert schema["total_rows"] == 11
    assert schema["approximate"] is False
    assert _dimension_rows(unkeyed_result) == _dimension_rows(keyed)
    assert not any(row["approximate"] for row in _check_rows(unkeyed_result).values() if row["row_level"])


@pytest.mark.parametrize("cap", [0, 1])
def test_max_failed_rows_does_not_change_keyless_numbers(unkeyed, cap: int):
    con, result = _certified("duckdb", ValidationConfig(max_failed_rows=cap))

    schema = _schema_row(result)
    assert schema["failed_rows"] == _truth(con, ["c < 0", "c > 5", "c IS NULL"])
    assert schema["approximate"] is False


def test_uncertified_check_without_a_key_entry_is_exact_on_the_table(unkeyed):
    con, result = _mixed("duckdb")

    twos = _check_rows(result)["twos_distinct"]
    assert (twos["route"], twos["attributed_rows"], twos["approximate"]) == ("client_lookup", 3, False)
    schema = _schema_row(result)
    assert schema["failed_rows"] == _truth(con, ["c < 0", "c > 5", "c = 2", "c IS NULL"])
    assert schema["approximate"] is False


def test_uncertified_check_is_not_exact_with_attribution_disabled():
    con, result = _mixed("duckdb", ValidationConfig(row_counts="scalar"))

    twos = _check_rows(result)["twos_distinct"]
    assert (twos["route"], twos["approximate"]) == ("server_scalar", True)
    schema = _schema_row(result)
    # DISTINCT returns one of the three copies, so the count is low and flagged.
    assert schema["failed_rows"] < _truth(con, ["c < 0", "c > 5", "c = 2", "c IS NULL"])
    assert schema["approximate"] is True


@pytest.mark.parametrize("fallback", ["chunk_limit", "statement_fails"])
def test_keyless_fallback_is_not_exact(unkeyed, monkeypatch: pytest.MonkeyPatch, fallback: str):
    if fallback == "chunk_limit":
        monkeypatch.setattr(pushdown, "MAX_BRANCHES", 1)
    else:
        # Only the keyless statement lacks key columns in its scan.
        _spy(monkeypatch, fail_on="WITH _vowl_scan AS (SELECT CAST(")

    con, result = _certified("duckdb")

    schema = _schema_row(result)
    assert schema["failed_rows"] == _truth(con, ["c < 0", "c > 5", "c IS NULL"])
    assert schema["approximate"] is True


def _two_sources(config: ValidationConfig | None = None):
    """t and u on separate connections, so a check that reads both runs in Mode 2."""
    t_con, u_con = _connect("duckdb"), _connect("duckdb")
    t_con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    t_con.raw_sql("INSERT INTO t VALUES (1, -1), (2, 2), (2, 2), (3, 2), (4, 5), (4, 5), (9, 0)")
    u_con.raw_sql("CREATE TABLE u (id INTEGER)")
    u_con.raw_sql("INSERT INTO u VALUES (1), (2), (3)")
    checks = [
        _check("negative", "c < 0"),
        {
            "name": "twos_distinct",
            "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c = 2) AS s",
            "mustBe": 0,
        },
        {
            "name": "id_in_u",
            "dimension": "consistency",
            "query": "SELECT COUNT(*) FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
            "mustBe": 0,
        },
    ]
    schemas = [_schema("t", checks), _schema("u", [], [{"name": "id"}])]
    adapters = {"t": IbisAdapter(t_con), "u": IbisAdapter(u_con)}
    return t_con, _run_validation(_contract(schemas), adapters=adapters, config=config)


def test_pushdown_table_match_and_cross_source_rows_merge():
    con, result = _two_sources()

    checks = _check_rows(result)
    assert checks["negative"]["route"] == "server_predicate"
    assert checks["twos_distinct"]["route"] == "server_lookup"
    assert checks["id_in_u"]["route"] == "client_lookup"
    assert checks["id_in_u"]["reason"] == REASON_CROSS_SOURCE
    # Rows (4, 5) twice and (9, 0) have no match in u. (2, 2) and (3, 2) have one.
    assert _schema_row(result)["failed_rows"] == _truth(con, ["c < 0", "c = 2", "id NOT IN (1, 2, 3)"]) == 7
    assert _schema_row(result)["approximate"] is False


def test_join_fan_out_counts_each_table_row_once():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (2, -2), (3, -3), (4, 4)")
    con.raw_sql("CREATE TABLE l (id INTEGER, k INTEGER)")
    con.raw_sql("INSERT INTO l VALUES (1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)")
    checks = [{"name": "fan", "query": "SELECT t.* FROM t JOIN l ON t.id = l.id WHERE t.c < 0", "mustBe": 0}]

    result = _validate(con, [_schema("t", checks)])

    row = _check_rows(result)["fan"]
    assert row["route"] == "server_lookup"
    assert row["attributed_rows"] == 3
    assert _schema_row(result)["failed_rows"] == _truth(con, ["c < 0"]) == 3
    annotated = result.get_annotated_output()["annotated"]["t"].to_arrow().to_pylist()
    assert sorted(row["id"] for row in annotated if row["check_info"]) == [1, 2, 3]


def test_a_truncated_check_is_not_attributable():
    _, result = _two_sources(ValidationConfig(max_failed_rows=1))

    row = _check_rows(result)["id_in_u"]
    assert (row["route"], row["reason"], row["row_level"], row["attributed_rows"]) == ("", REASON_TRUNCATED, True, None)
    # Its rows are left out of the row counts.
    schema = _schema_row(result)
    assert (schema["approximate"], schema["checks_not_attributable"]) == (True, 1)


def _not_attributable_pair():
    """t has a truncated client_lookup check and a check with no match key.

    Both are row-level, neither is attributable."""
    t_con, u_con = _connect("duckdb"), _connect("duckdb")
    t_con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    t_con.raw_sql("INSERT INTO t VALUES (1, -1), (2, -2), (3, 3), (5, 4), (6, 9)")
    u_con.raw_sql("CREATE TABLE u (id INTEGER)")
    u_con.raw_sql("INSERT INTO u VALUES (1), (2), (3)")
    checks = [
        _check("nine", "c = 9"),
        {
            "name": "ids_negative",
            "dimension": "validity",
            "query": "SELECT COUNT(*) FROM (SELECT id FROM t WHERE c < 0) AS s",
            "mustBe": 0,
        },
        {
            "name": "id_in_u",
            "dimension": "consistency",
            "query": "SELECT COUNT(*) FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
            "mustBe": 0,
        },
    ]
    schemas = [_schema("t", checks), _schema("u", [], [{"name": "id"}])]
    adapters = {"t": IbisAdapter(t_con), "u": IbisAdapter(u_con)}
    return _run_validation(_contract(schemas), adapters=adapters, config=ValidationConfig(max_failed_rows=1))


def test_checks_not_attributable_is_counted_at_every_level():
    result = _not_attributable_pair()

    checks = _check_rows(result)
    assert (checks["id_in_u"]["reason"], checks["ids_negative"]["reason"]) == (REASON_TRUNCATED, REASON_NO_MATCH_KEY)
    for name in ("id_in_u", "ids_negative"):
        assert (checks[name]["row_level"], checks[name]["route"], checks[name]["attributed_rows"]) == (True, "", None)
        assert checks[name]["approximate"] is True
    assert checks["nine"]["attributed_rows"] == 1

    # Only the attributable check adds rows. The other two are flagged.
    schema = _schema_row(result)
    assert (schema["failed_rows"], schema["checks_not_attributable"], schema["approximate"]) == (1, 2, True)
    dimensions = _dimension_rows(result)
    validity = dimensions["validity"]
    assert (validity["failed_rows"], validity["checks_not_attributable"], validity["approximate"]) == (1, 1, True)
    assert (dimensions["conformity"]["checks_not_attributable"], dimensions["conformity"]["approximate"]) == (0, False)
    # A dimension whose row-level checks are all not attributable has no row numbers.
    assert (dimensions["consistency"]["failed_rows"], dimensions["consistency"]["checks_not_attributable"]) == (None, 1)
    assert dimensions["consistency"]["approximate"] is True

    from vowl.validation.dq_metrics import run_row_counts

    assert run_row_counts(result)[2:] == (True, 2)


def test_without_attribution_every_row_level_check_is_not_attributable():
    _, result = _two_sources(ValidationConfig(row_counts="scalar"))

    schema = _schema_row(result)
    checks = _check_rows(result).values()
    failed = sum(row["row_level"] and row["status"] == "FAILED" for row in checks)
    # Passed checks are never attributed, so they are not counted as not attributable.
    assert schema["checks_not_attributable"] == failed > 0
    assert all(row["attributed_rows"] is None for row in checks)


def test_primary_key_lets_a_column_subset_check_merge():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (2, -2), (3, 3), (4, 9)")
    subset = {"name": "ids_negative", "query": "SELECT COUNT(*) FROM (SELECT id FROM t WHERE c < 0) AS s", "mustBe": 0}
    checks = [subset, _check("nine", "c = 9")]
    with_pk = [{"name": "id", "primaryKey": True, "primaryKeyPosition": 1}, {"name": "c"}]

    keyed = _validate(con, [_schema("t", checks, with_pk)])
    unkeyed = _validate(con, [_schema("t", checks)])

    assert _check_rows(keyed)["ids_negative"]["route"] == "server_lookup"
    assert _schema_row(keyed)["failed_rows"] == 3
    assert _check_rows(unkeyed)["ids_negative"]["reason"] == REASON_NO_MATCH_KEY
    assert _schema_row(unkeyed)["failed_rows"] == 1

    # Annotated output merges the same checks on the same key.
    keyed_output = keyed.get_annotated_output()
    assert "t::ids_negative" not in keyed_output["residues"]
    flagged = {
        row["id"]: {item["check_name"] for item in json.loads(row["check_info"])}
        for row in keyed_output["annotated"]["t"].to_arrow().to_pylist()
        if row["check_info"]
    }
    assert flagged == {1: {"ids_negative"}, 2: {"ids_negative"}, 4: {"nine"}}
    unkeyed_output = unkeyed.get_annotated_output()
    assert "t::ids_negative" in unkeyed_output["residues"]
    assert sum(1 for row in unkeyed_output["annotated"]["t"].to_arrow().to_pylist() if row["check_info"]) == 1


def test_a_duplicated_primary_key_is_not_trusted():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (1, 3)")
    subset = {"name": "ids_negative", "query": "SELECT COUNT(*) FROM (SELECT id FROM t WHERE c < 0) AS s", "mustBe": 0}
    with_pk = [{"name": "id", "primaryKey": True}, {"name": "c"}]

    result = _validate(con, [_schema("t", [subset], with_pk)])

    from vowl.validation.row_quality.selection import REASON_PK_NOT_UNIQUE

    assert _check_rows(result)["ids_negative"]["reason"] == REASON_PK_NOT_UNIQUE
    assert "t::ids_negative" in result.get_annotated_output()["residues"]


def test_filter_conditions_apply_to_the_total_and_the_rows():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (2, -2), (3, 3), (10, -5), (11, 5)")
    adapter = IbisAdapter(con, filter_conditions={"t": {"field": "id", "operator": "<", "value": 10}})
    distinct = {
        "name": "distinct",
        "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c < 0) s",
        "mustBe": 0,
    }
    checks = [_check("negative", "c < 0"), distinct]

    result = _run_validation(_contract([_schema("t", checks)]), adapters={"t": adapter})

    schema = _schema_row(result)
    assert (schema["total_rows"], schema["failed_rows"]) == (3, 2)


# ---------------------------------------------------------------------------
# Operator rule, tolerance and trust metadata
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("operator", "expected", "row_level"),
    [
        (None, None, True),
        ("mustBeLessThan", 5, True),
        ("mustBeLessOrEqualTo", 5, True),
        ("mustBe", 0, True),
        ("mustBe", 3, False),
        ("mustBeBetween", [0, 5], True),
        ("mustBeBetween", [1, 5], False),
        ("mustNotBe", 0, False),
        ("mustNotBeBetween", [0, 5], False),
        ("mustBeGreaterThan", 0, False),
        ("mustBeGreaterOrEqualTo", 1, False),
        ("unknown", 0, False),
    ],
)
def test_operator_rule(operator, expected, row_level: bool):
    assert identifies_bad_rows(operator, expected) is row_level


@pytest.mark.parametrize("attribute_tolerated", [False, True])
def test_tolerated_rows_in_annotated_output(attribute_tolerated: bool):
    _, result = _mixed("duckdb", ValidationConfig(attribute_tolerated_rows=attribute_tolerated))

    annotated = result.get_annotated_output()["annotated"]["t"].to_arrow().to_pylist()
    items = [item for row in annotated if row["check_info"] for item in json.loads(row["check_info"])]
    tolerated = [item for item in items if item["check_name"] == "threes_tolerated"]
    if attribute_tolerated:
        assert tolerated == [{"check_name": "threes_tolerated", "tolerated": True}]
    else:
        assert tolerated == []
    # The inverted check's matched rows are the good rows: never flagged.
    assert not any(item["check_name"] == "ones_inverted" for item in items)
    assert "t::ones_inverted" not in result.get_annotated_output()["residues"]


def test_annotated_flagged_rows_equal_failed_rows():
    _, result = _mixed("duckdb")

    annotated = result.get_annotated_output()["annotated"]["t"].to_arrow().to_pylist()

    assert sum(1 for row in annotated if row["check_info"]) == _schema_row(result)["failed_rows"]


def test_a_passed_check_without_pushdown_is_not_fetched():
    class NoPushdown(IbisAdapter):
        def run_arrow_query(self, sql: str):
            raise NotImplementedError

    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql(f"INSERT INTO t VALUES {_ROWS}")
    result = _run_validation(_contract([_schema("t", _MIXED_CHECKS)]), adapters={"t": NoPushdown(con)})

    checks = _check_rows(result)
    tolerated = checks["threes_tolerated"]
    assert (tolerated["route"], tolerated["reason"]) == ("", REASON_PASSED_NOT_ATTRIBUTED)
    assert tolerated["attributed_rows"] is None
    assert tolerated["scalar_count"] == _truth(con, ["c = 3"])
    assert checks["negative"]["route"] == "client_lookup"
    schema = _schema_row(result)
    # Without pushdown every check is matched onto the table, so DISTINCT keeps its copies.
    assert schema["failed_rows"] == _truth(con, ["c < 0", "c > 5", "c = 2", "c IS NULL"]) == 9
    # A passed check is never attributed, so the numbers stay exact.
    assert (schema["approximate"], schema["checks_not_attributable"]) == (False, 0)


def test_error_check_makes_the_numbers_inexact():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (2, 3)")
    checks = [_check("negative", "c < 0"), _check("broken", "missing_column > 0", dimension="accuracy")]

    result = _validate(con, [_schema("t", checks)])

    rows = _check_rows(result)
    assert rows["broken"]["reason"] == REASON_ERROR
    schema = _schema_row(result)
    assert (schema["failed_rows"], schema["approximate"]) == (1, True)
    dimensions = _dimension_rows(result)
    # accuracy has no row-level check: no pass rate, not 100%.
    assert dimensions["accuracy"]["pass_rate"] is None
    assert dimensions["accuracy"]["approximate"] is True
    assert dimensions["validity"]["approximate"] is False


def test_a_dimension_with_only_excluded_checks_has_no_pass_rate():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, 1)")
    result = _validate(con, [_schema("t", [_check("inverted", "c = 1", dimension="accuracy", mustBeGreaterThan=5)])])

    accuracy = _dimension_rows(result)["accuracy"]
    assert (accuracy["failed_rows"], accuracy["pass_rate"], accuracy["checks_row_level"]) == (None, None, 0)
    assert accuracy["checks_not_row_level"] == 1


def test_an_empty_table_has_no_pass_rate():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")

    result = _validate(con, [_schema("t", [_check("negative", "c < 0")])])

    schema = _schema_row(result)
    assert (schema["total_rows"], schema["failed_rows"], schema["pass_rate"]) == (0, 0, None)


def test_non_row_level_checks_are_not_row_level():
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, 1), (2, 5)")
    checks = [{"name": "average", "query": "SELECT AVG(c) FROM t", "mustBeLessThan": 1}]

    result = _validate(con, [_schema("t", checks)])

    assert _check_rows(result)["average"]["reason"] == REASON_NOT_ROW_LEVEL


def test_statistics_turned_off(capsys: pytest.CaptureFixture[str]):
    _, result = _mixed("duckdb", ValidationConfig(row_counts="off"))

    schema = _schema_row(result)
    assert (schema["total_rows"], schema["failed_rows"], schema["pass_rate"]) == (None, None, None)
    result.print_summary()
    assert "Failed Rows (approximate): 8" in capsys.readouterr().out


def test_capped_statistics_no_longer_cap_the_total():
    with pytest.warns(DeprecationWarning, match="max_rows_for_statistics"):
        config = ValidationConfig(max_rows_for_statistics=2)
    _, result = _mixed("duckdb", config)

    assert _schema_row(result)["total_rows"] == 11
    assert _schema_row(result)["approximate"] is False


def test_the_report_is_computed_once(monkeypatch: pytest.MonkeyPatch):
    _, result = _mixed("duckdb")
    calls: list[str] = []
    original = IbisAdapter.run_arrow_query
    monkeypatch.setattr(IbisAdapter, "run_arrow_query", lambda self, sql: calls.append(sql) or original(self, sql))

    result.get_dq_metrics_df()
    first = len(calls)
    result.get_dq_metrics_df(by="dimension")
    result.print_summary()

    assert first > 0
    assert len(calls) == first


def test_otel_row_gauges_carry_no_trust_attributes():
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    _, result = _mixed("duckdb")
    reader = InMemoryMetricReader()
    result.export_otel(signals=("metrics",), metric_provider=MeterProvider(metric_readers=[reader]))

    points = {}
    for resource_metrics in reader.get_metrics_data().resource_metrics:
        for scope in resource_metrics.scope_metrics:
            for metric in scope.metrics:
                if metric.name.endswith((".row.count", ".row.pass_rate")):
                    points[metric.name] = [(dict(p.attributes), p.value) for p in metric.data.data_points]
    failed = [value for attrs, value in points["vowl.schema.row.count"] if attrs["status"] == "FAILED"]
    assert failed == [9]
    assert all(
        not any(key.startswith("vowl.row_quality.") for key in attrs)
        for series in points.values()
        for attrs, _ in series
    )


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark_session():
    if "JAVA_HOME" not in os.environ:
        homebrew_java = Path("/opt/homebrew/opt/openjdk@17")
        if homebrew_java.exists():
            os.environ["JAVA_HOME"] = str(homebrew_java)
    pytest.importorskip("pyspark", reason="PySpark not installed")
    from pyspark.sql import SparkSession

    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("test_row_quality")
            .config("spark.driver.memory", "512m")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"PySpark could not start: {exc}")
    yield spark
    spark.stop()


def test_spark_single_scan_keeps_negative_zero_and_nan_apart(spark_session):
    rows = [(1, -0.0), (1, 0.0), (2, float("nan")), (2, float("nan")), (3, 1.5), (3, 1.5), (4, -1.0)]
    spark_session.createDataFrame(rows, "id INT, c DOUBLE").createOrReplaceTempView("t")
    con = ibis.pyspark.connect(session=spark_session)
    checks = [
        _check("neg_zero", "CAST(c AS STRING) = '-0.0'"),
        _check("pos_zero", "CAST(c AS STRING) = '0.0'"),
        _check("nan", "isnan(c)"),
        _check("negative", "c < 0"),
        {
            "name": "distinct",
            "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c > 1.2 AND NOT isnan(c)) AS s",
            "mustBe": 0,
        },
    ]

    result = _run_validation(_contract([_schema("t", checks)]), adapters={"t": IbisAdapter(con)})

    schema = _schema_row(result)
    assert (schema["total_rows"], schema["failed_rows"], schema["approximate"]) == (7, 7, False)
    rows_by_check = _check_rows(result)
    assert rows_by_check["neg_zero"]["route"] == "server_predicate"
    # The default matches it onto the exported table, so the export must keep NaN.
    assert rows_by_check["distinct"]["route"] == "client_lookup"
    assert rows_by_check["distinct"]["failed_rows"] == 2


def test_spark_utf8_lcase_values_caught_by_different_checks_stay_apart(spark_session):
    spark_session.sql(
        "CREATE OR REPLACE TEMP VIEW t AS SELECT id, collate(c, 'UTF8_LCASE') AS c "
        "FROM VALUES (1, 'a'), (1, 'A'), (1, 'a'), (2, 'b') AS v(id, c)"
    )
    # The collation makes 'a' and 'A' one group, so a plain column key would merge them.
    assert spark_session.sql("SELECT COUNT(DISTINCT c) FROM t WHERE id = 1").collect()[0][0] == 1
    con = ibis.pyspark.connect(session=spark_session)
    checks = [
        _check("lower", "CAST(c AS STRING) COLLATE UTF8_BINARY = 'a'"),
        _check("upper", "CAST(c AS STRING) COLLATE UTF8_BINARY = 'A'", dimension="conformity"),
    ]

    result = _run_validation(_contract([_schema("t", checks)]), adapters={"t": IbisAdapter(con)})

    schema = _schema_row(result)
    assert (schema["total_rows"], schema["failed_rows"], schema["approximate"]) == (4, 3, False)
    # With a plain column key the merged group lands in one dimension, so they would not split 2 and 1.
    dimensions = _dimension_rows(result)
    assert (dimensions["validity"]["failed_rows"], dimensions["conformity"]["failed_rows"]) == (2, 1)
    assert {row["route"] for row in _check_rows(result).values() if row["status"] == "FAILED"} == {"server_predicate"}


# ---------------------------------------------------------------------------
# Statement shapes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dialect", ["duckdb", "sqlite", "spark", "postgres"])
def test_statements_are_single_selects_the_validator_accepts(dialect: str):
    from vowl.executors.security import validate_query_security

    spec = pushdown.KeySpec(dialect, "SELECT * FROM t", ["id", "c"], [None, None])
    branches = [
        pushdown.Branch(0, "server_predicate", "SELECT * FROM t WHERE c < 0"),
        pushdown.Branch(1, "server_lookup", "SELECT DISTINCT * FROM t WHERE c = 2"),
    ]
    chunk = pushdown.Chunk(branches)
    for sql in (
        pushdown.per_row_statement(spec, chunk),
        pushdown.histogram_statement(spec, chunk),
        pushdown.preflight_statement(spec),
        pushdown.duplicate_key_statement(spec),
        pushdown.count_statement(spec.anchor_sql, dialect),
    ):
        validate_query_security(sql, dialect=dialect)
        assert isinstance(sqlglot.parse_one(sql, dialect=dialect), sqlglot.exp.Select)
    # The preflight also groups on the keys, so an ungroupable type fails it.
    assert "GROUP BY" in pushdown.preflight_statement(spec)


def test_mysql_casts_use_its_own_type_names():
    spec = pushdown.KeySpec("mysql", "SELECT * FROM t", ["id"], [None])
    chunk = pushdown.Chunk([pushdown.Branch(0, "server_predicate", "SELECT * FROM t WHERE id < 0")])

    sql = pushdown.histogram_statement(spec, chunk)

    assert "AS BIGINT" not in sql
    assert "AS SIGNED" in sql


def test_chunks_stay_within_the_byte_budget(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pushdown, "MAX_SQL_BYTES", 20_000)
    columns = [f"column_{i}" for i in range(32)]
    spec = pushdown.KeySpec("duckdb", "SELECT * FROM t", columns, [None] * 32)
    branches = [
        pushdown.Branch(
            i, "server_lookup" if i % 2 else "server_predicate", f"SELECT * FROM t WHERE column_{i % 32} > {i}"
        )
        for i in range(60)
    ]

    chunks = pushdown.plan_chunks(spec, branches)

    assert len(chunks) > 1
    assert sum(len(chunk.branches) for chunk in chunks) == 60
    for chunk in chunks:
        assert len(pushdown.histogram_statement(spec, chunk).encode()) <= 20_000


def test_a_small_byte_budget_does_not_change_the_numbers(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pushdown, "MAX_SQL_BYTES", 3_000)
    con, result = _mixed("duckdb")

    assert _schema_row(result)["failed_rows"] == 9
    assert _schema_row(result)["approximate"] is False


def test_an_ungroupable_key_sends_the_schema_to_fetched_rows(monkeypatch: pytest.MonkeyPatch):
    original = IbisAdapter.run_arrow_query

    def reject_grouped_keys(self, sql: str):
        if 'AS "_vowl_k" GROUP BY' in sql:
            raise RuntimeError("simulated: key type cannot be grouped")
        return original(self, sql)

    monkeypatch.setattr(IbisAdapter, "run_arrow_query", reject_grouped_keys)
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (1, -1), (2, 3)")

    result = _validate(con, [_schema("t", [_check("negative", "c < 0")])])

    assert _check_rows(result)["negative"]["route"] == "client_lookup"
    assert _schema_row(result)["failed_rows"] == 2


def test_a_zero_total_from_a_failed_count_is_counted_again():
    class ZeroTotals(IbisAdapter):
        def get_total_rows(self, schema_name: str, max_rows: int = -1) -> int:
            return 0

    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (2, 3), (3, 4)")

    result = _run_validation(_contract([_schema("t", [_check("negative", "c < 0")])]), adapters={"t": ZeroTotals(con)})

    schema = _schema_row(result)
    assert (schema["total_rows"], schema["failed_rows"], schema["approximate"]) == (3, 1, False)


def test_a_total_below_a_checks_rows_is_not_exact():
    class LowTotalsNoPushdown(IbisAdapter):
        def get_total_rows(self, schema_name: str, max_rows: int = -1) -> int:
            return 1

        def run_arrow_query(self, sql: str):
            raise NotImplementedError

    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql("INSERT INTO t VALUES (1, -1), (2, -2), (3, 4)")
    adapters = {"t": LowTotalsNoPushdown(con)}
    contract = _contract([_schema("t", [_check("negative", "c < 0")])])

    fast = _run_validation(contract, adapters=adapters, config=ValidationConfig(row_counts="scalar"))
    assert _schema_row(fast)["approximate"] is True

    # The exported table gives the true total.
    attributed = _run_validation(contract, adapters=adapters)
    schema = _schema_row(attributed)
    assert (schema["total_rows"], schema["failed_rows"], schema["approximate"]) == (3, 2, False)


def test_arrow_values_survive_the_cross_route_merge():
    from vowl.validation.result_row_quality import table_key_index
    from vowl.validation.row_quality.merge import merge_onto_table

    table = pa.table({"id": pa.array([1, 1, 2, 3, 3], pa.int64())})
    outcome = pushdown.PushdownOutcome()
    values = pa.table({"_vowl_v0": pa.array([1, 2], pa.int64())})
    outcome.value_tables.append(values)
    outcome.rows = {(b"1",): [0b01, 2, (0, 0)], (b"2",): [0b01, 1, (0, 1)]}
    fetched = [(1, pa.table({"id": pa.array([1, 3], pa.int32())}))]

    entries, approximate, matched = merge_onto_table(outcome, fetched, ["id"], table, table_key_index(table, ["id"]))

    assert approximate is False
    assert sorted(entries) == [(0b01, 1), (0b10, 2), (0b11, 2)]
    assert matched == {1: (4, 0)}


# ---------------------------------------------------------------------------
# HDB resale parity
# ---------------------------------------------------------------------------

_HDB_DIR = Path(__file__).parent / "hdb_resale"


@pytest.fixture(scope="module")
def hdb_frame():
    import pandas as pd

    return pd.read_csv(_HDB_DIR / "HDBResaleWithErrors.csv").fillna("").astype(str)


@pytest.mark.parametrize("cap", [None, 1])
def test_hdb_resale_numbers_match_annotated_output(hdb_frame, cap: int | None):
    config = ValidationConfig(max_failed_rows=cap) if cap is not None else None

    result = _run_validation(str(_HDB_DIR / "hdb_resale.yaml"), df=hdb_frame, config=config)

    schema = _schema_row(result, "hdb_resale_prices")
    assert schema["failed_rows"] == 10_571
    assert schema["approximate"] is False
    dimensions = _dimension_rows(result, "hdb_resale_prices")
    assert dimensions["uniqueness"]["failed_rows"] == 10_551
    assert dimensions["conformity"]["failed_rows"] == 10
    assert dimensions["consistency"]["failed_rows"] == 12
    if cap is None:
        annotated = result.get_annotated_output()["annotated"]["hdb_resale_prices"]
        assert sum(1 for info in annotated.to_arrow().column("check_info").to_pylist() if info) == 10_571


# ---------------------------------------------------------------------------
# Employee parity
# ---------------------------------------------------------------------------

_EMPLOYEE_DIR = Path(__file__).parent / "employee"


@pytest.mark.parametrize("sources", ["one_connection", "two_connections"])
def test_employee_numbers_match_annotated_output(sources: str):
    import pandas as pd

    frames = {
        name: pd.read_csv(_EMPLOYEE_DIR / f"{name}.csv") for name in ("demo_employee_payroll", "demo_employee_list")
    }
    contract = str(_EMPLOYEE_DIR / "employee_payroll_datacontract.yaml")
    if sources == "one_connection":
        con = ibis.duckdb.connect()
        for name, frame in frames.items():
            con.create_table(name, frame)
        result = _run_validation(contract, adapter=IbisAdapter(con))
    else:
        adapters = {}
        for name, frame in frames.items():
            con = ibis.duckdb.connect()
            con.create_table(name, frame)
            adapters[name] = IbisAdapter(con)
        result = _run_validation(contract, adapters=adapters)

    annotated = result.get_annotated_output()["annotated"]
    for name, failed in {"demo_employee_payroll": 3, "demo_employee_list": 2}.items():
        flagged = sum(1 for info in annotated[name].to_arrow().column("check_info").to_pylist() if info)
        assert _schema_row(result, name)["failed_rows"] == flagged == failed
    routes = {row["route"] for row in _check_rows(result).values() if row["status"] == "FAILED"}
    # The joins are matched onto the table, in the data source when the
    # tables share one connection and onto the exported table when they do not.
    assert routes == {"server_predicate", "server_lookup" if sources == "one_connection" else "client_lookup"}


# ---------------------------------------------------------------------------
# COUNT(DISTINCT) checks
# ---------------------------------------------------------------------------

# The rows with c = 2 hold one distinct value on three rows.
_DISTINCT_CHECKS = [
    {"name": "distinct_twos", "query": "SELECT COUNT(DISTINCT c) FROM t WHERE c = 2", "mustBe": 0},
    {
        "name": "distinct_twos_sub",
        "query": "SELECT COUNT(DISTINCT c) FROM (SELECT * FROM t WHERE c = 2) AS s",
        "mustBe": 0,
    },
    {"name": "distinct_nulls", "query": "SELECT COUNT(DISTINCT c) FROM t WHERE c IS NULL", "mustBe": 0},
]


def _distinct(config: ValidationConfig | None = None, checks: list[dict] | None = None):
    con = _connect("duckdb")
    con.raw_sql("CREATE TABLE t (id INTEGER, c INTEGER)")
    con.raw_sql(f"INSERT INTO t VALUES {_ROWS}")
    return con, _validate(con, [_schema("t", checks or _DISTINCT_CHECKS)], config)


def test_count_distinct_checks_count_the_rows_holding_the_values():
    con, result = _distinct(ValidationConfig())

    checks = _check_rows(result)
    for name in ("distinct_twos", "distinct_twos_sub"):
        assert checks[name]["row_level"] is True
        assert (checks[name]["scalar_count"], checks[name]["attributed_rows"], checks[name]["approximate"]) == (
            1,
            3,
            False,
        )
    # COUNT(DISTINCT c) skips NULLs, so the check catches no rows and passes.
    assert checks["distinct_nulls"]["status"] == "PASSED"
    assert (checks["distinct_nulls"]["scalar_count"], checks["distinct_nulls"]["attributed_rows"]) == (0, None)
    assert _schema_row(result)["failed_rows"] == _truth(con, ["c = 2"])
    assert _schema_row(result)["approximate"] is False


def test_count_distinct_checks_are_not_exact_with_attribution_disabled():
    _, result = _distinct(ValidationConfig(row_counts="scalar"))

    check = _check_rows(result)["distinct_twos"]
    assert (check["route"], check["scalar_count"], check["attributed_rows"], check["approximate"]) == (
        "server_scalar",
        1,
        None,
        True,
    )
    assert check["reason"] == REASON_COUNTS_VALUES
    assert _schema_row(result)["approximate"] is True


def test_count_distinct_annotates_every_row_holding_the_value():
    _, result = _distinct(checks=_DISTINCT_CHECKS[:1])

    annotated = result.get_annotated_output()["annotated"]["t"].to_arrow()
    flagged = [
        c for c, info in zip(annotated["c"].to_pylist(), annotated["check_info"].to_pylist(), strict=True) if info
    ]
    assert flagged == [2, 2, 2]


def test_errored_count_distinct_check_is_inexact():
    checks = [
        _check("negative", "c < 0"),
        {"name": "broken", "query": "SELECT COUNT(DISTINCT nope) FROM t WHERE c = 2", "mustBe": 0},
    ]
    _, result = _distinct(checks=checks)

    assert _check_rows(result)["broken"]["reason"] == REASON_ERROR
    assert _schema_row(result)["approximate"] is True


@pytest.mark.parametrize(("cap", "truncated"), [(2, True), (3, False)])
def test_truncation_is_detected_from_the_rows_not_the_check_count(cap: int, truncated: bool):
    # The check counts 1 value on 3 rows, so comparing its count with the rows
    # fetched could never see the cap. The fetch asks for one row more instead.
    _, result = _distinct(ValidationConfig(max_failed_rows=cap), checks=_DISTINCT_CHECKS[1:2])

    check = _check_rows(result)["distinct_twos_sub"]
    # server_lookup counts in SQL, so the cap only reaches the annotated output.
    assert (check["route"], check["attributed_rows"], check["approximate"]) == ("server_lookup", 3, False)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        annotated = result.get_annotated_output()["annotated"]["t"].to_arrow()
    assert any("incomplete" in str(w.message) for w in caught) is truncated
    infos = [json.loads(info) for info in annotated["check_info"].to_pylist() if info]
    # The three rows are copies, so the fetched rows still flag all of them.
    assert len(infos) == 3
    assert all(item[0].get("truncated", False) is truncated for item in infos)


def test_by_check_actual_is_the_check_count():
    _, result = _mixed("duckdb")

    checks = _check_rows(result)
    assert (checks["twos_distinct"]["scalar_count"], checks["twos_distinct"]["attributed_rows"]) == (1, 3)
    assert (checks["negative"]["scalar_count"], checks["negative"]["attributed_rows"]) == (3, 3)
    assert checks["ones_inverted"]["scalar_count"] is None
