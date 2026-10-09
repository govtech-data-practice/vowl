"""Row-quality key tests against a real Postgres (testcontainers).

Each number is compared with a truth computed on the connection. The cases
are the ones a plain column key gets wrong in Postgres: ``-0.0 = 0.0``, float
text that does not round-trip, and a case-insensitive collation. See "Key" in
design/row-quality-statistics.md.
"""

from __future__ import annotations

import ibis
import pytest
import test_row_quality as rq
from test_database_backends import _configure_docker_env, _docker_available, _ibis_backend_available

from vowl.adapters.ibis_adapter import IbisAdapter

pytestmark = pytest.mark.docker_integration

# Shared helpers and the contract-validation bypass from the DuckDB and SQLite tests.
_skip_contract_validation = rq._skip_contract_validation


@pytest.fixture(scope="module")
def postgres_container():
    if not _ibis_backend_available("postgres"):
        pytest.skip("Ibis Postgres backend not installed")
    if not _docker_available():
        pytest.skip("Docker not available")
    _configure_docker_env()

    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer("postgres:16-alpine")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture
def con(postgres_container):
    connection = ibis.postgres.connect(
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        user=postgres_container.username,
        password=postgres_container.password,
        database=postgres_container.dbname,
    )
    for table in ("t", "u"):
        connection.raw_sql(f"DROP TABLE IF EXISTS {table}")
    yield connection
    connection.disconnect()


def _run(con, sql: str) -> None:
    con.raw_sql(sql).close()


def _count(con, sql: str) -> int:
    cursor = con.raw_sql(sql)
    try:
        return int(cursor.fetchone()[0])
    finally:
        cursor.close()


def _truth(con, predicates: list[str]) -> int:
    return _count(con, "SELECT COUNT(*) FROM t WHERE " + " OR ".join(f"({p})" for p in predicates))


def test_negative_zero_nan_and_close_floats_are_kept_apart(con):
    _run(con, "CREATE TABLE t (id INTEGER, c DOUBLE PRECISION)")
    _run(
        con,
        "INSERT INTO t VALUES (1, '-0'), (1, 0), (2, 'NaN'), (2, 'NaN'), (3, 0.30000000000000004), (3, 0.3), (4, 1.5)",
    )
    predicates = {
        "neg_zero": "CAST(c AS TEXT) = '-0'",
        "pos_zero": "CAST(c AS TEXT) = '0'",
        "nan": "c = 'NaN'",
        "above_point_three": "c > 0.3 AND c < 0.31",
        "point_three": "c = 0.3",
    }
    checks = [rq._check(name, where) for name, where in predicates.items()]

    result = rq._validate(con, [rq._schema("t", checks)])

    schema = rq._schema_row(result)
    assert schema["failed_rows"] == _truth(con, list(predicates.values())) == 6
    assert schema["approximate"] is False
    assert {row["attribution_method"] for row in rq._check_rows(result).values() if row["status"] == "FAILED"} == {
        "server_predicate"
    }


def test_case_insensitive_collation_values_stay_apart(con):
    _run(
        con,
        "CREATE COLLATION IF NOT EXISTS vowl_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
    )
    _run(con, "CREATE TABLE t (id INTEGER, c TEXT COLLATE vowl_ci)")
    _run(con, "INSERT INTO t VALUES (1, 'a'), (1, 'A'), (1, 'a')")
    # The collation makes 'a' and 'A' one group, so a plain column key would merge them.
    assert _count(con, "SELECT COUNT(DISTINCT c) FROM t") == 1
    predicates = {"lower": "c COLLATE \"C\" = 'a'", "upper": "c COLLATE \"C\" = 'A'"}
    checks = [rq._check(name, where) for name, where in predicates.items()]

    result = rq._validate(con, [rq._schema("t", checks)])

    schema = rq._schema_row(result)
    assert schema["failed_rows"] == _truth(con, list(predicates.values())) == 3
    assert schema["approximate"] is False


def test_duplicates_count_once_per_copy_not_once_per_check(con):
    _run(con, "CREATE TABLE t (id INTEGER, c INTEGER)")
    _run(con, "INSERT INTO t VALUES (1, 1), (1, 1), (1, 1), (2, 2)")
    checks = [rq._check("a", "c = 1"), rq._check("b", "id = 1"), rq._check("c", "c < 2")]

    result = rq._validate(con, [rq._schema("t", checks)])

    assert rq._schema_row(result)["failed_rows"] == 3
    assert {row["failed_rows"] for row in rq._check_rows(result).values() if row["row_level"] and row["attribution_method"]} == {3}


def test_an_uncertified_check_goes_by_table_match(con):
    _run(con, "CREATE TABLE t (id INTEGER, c INTEGER)")
    _run(con, "INSERT INTO t VALUES (1, -1), (2, 2), (2, 2), (3, 2), (4, 5)")
    checks = [
        rq._check("negative", "c < 0"),
        {
            "name": "twos_distinct",
            "query": "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t WHERE c = 2) AS s",
            "mustBe": 0,
        },
    ]

    result = rq._validate(con, [rq._schema("t", checks)])

    rows = rq._check_rows(result)
    assert rows["negative"]["attribution_method"] == "server_predicate"
    assert rows["twos_distinct"]["attribution_method"] == "client_lookup"
    assert rows["twos_distinct"]["failed_rows"] == 3
    schema = rq._schema_row(result)
    assert schema["failed_rows"] == _truth(con, ["c < 0", "c = 2"]) == 4
    assert schema["approximate"] is False


def test_boolean_json_and_bytea_columns_merge_with_fetched_rows(con):
    """A cross-source check brings fetched rows, so the statement also carries the values."""
    _run(con, "CREATE TABLE t (id INTEGER, flag BOOLEAN, doc JSON, b BYTEA)")
    _run(
        con,
        "INSERT INTO t VALUES "
        "(1, TRUE, '{\"a\": 1}', '\\x01'), (1, TRUE, '{\"a\": 1}', '\\x01'), "
        "(2, FALSE, '{\"a\": 2}', '\\x02'), (3, FALSE, '[1, 2]', NULL)",
    )
    other = ibis.duckdb.connect()
    other.raw_sql("CREATE TABLE u (id INTEGER)")
    other.raw_sql("INSERT INTO u VALUES (1), (2)")
    checks = [
        rq._check("flagged", "flag"),
        {
            "name": "id_in_u",
            "dimension": "consistency",
            "query": "SELECT COUNT(*) FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
            "mustBe": 0,
        },
    ]
    properties = [{"name": "id"}, {"name": "flag"}, {"name": "doc"}, {"name": "b"}]
    schemas = [rq._schema("t", checks, properties), rq._schema("u", [], [{"name": "id"}])]

    result = rq._validate(con, schemas, adapters={"t": IbisAdapter(con), "u": IbisAdapter(other)})

    rows = rq._check_rows(result)
    assert rows["flagged"]["attribution_method"] == "server_predicate"
    assert rows["id_in_u"]["attribution_method"] == "client_lookup"
    # Rows 1 (twice) are flagged, row 3 has no match in u.
    schema = rq._schema_row(result)
    assert schema["failed_rows"] == _truth(con, ["flag", "id NOT IN (1, 2)"]) == 3
    assert schema["approximate"] is False
