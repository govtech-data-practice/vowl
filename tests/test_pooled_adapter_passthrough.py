"""PooledAdapters passed straight to ``validate_data``.

A PooledAdapter used to be accepted only inside a user-built
``MultiSourceAdapter``. Passing one through ``adapters=`` or ``adapter=``
raised ``TypeError: Unsupported adapter type``. These tests pin that both
paths now take any ``BaseAdapter`` as given, including for cross-schema joins.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import duckdb
import ibis
import pytest

from vowl import validate_data
from vowl.adapters import IbisAdapter
from vowl.adapters.pooled_adapter import PooledAdapter
from vowl.validation.runner import ValidationRunner

T1_SQL = "CREATE TABLE t1 AS SELECT * FROM (VALUES (1, 10), (2, 20), (3, 150)) v(id, amount)"
T2_SQL = "CREATE TABLE t2 AS SELECT * FROM (VALUES (1, 1), (2, 2), (3, 9)) v(id, t1_id)"

# One t1 row has amount over 100, and one t2 row points at a t1 id that does
# not exist, so each FAILED check below finds exactly 1 row.
CHECKS = {
    "t1": {
        "no_negative_amount": "SELECT COUNT(*) FROM t1 WHERE amount < 0",
        "amount_at_most_100": "SELECT COUNT(*) FROM t1 WHERE amount > 100",
    },
    "t2": {
        "no_negative_id": "SELECT COUNT(*) FROM t2 WHERE id < 0",
        "t2_orphans": "SELECT COUNT(*) FROM t2 LEFT JOIN t1 ON t2.t1_id = t1.id WHERE t1.id IS NULL",
    },
}
COLUMNS = {"t1": ("id", "amount"), "t2": ("id", "t1_id")}


def _make_db(path: Path, statements: list[str]) -> str:
    con = duckdb.connect(str(path))
    for statement in statements:
        con.execute(statement)
    con.close()
    return str(path)


@pytest.fixture
def split_dbs(tmp_path: Path) -> tuple[str, str]:
    """t1 and t2 in separate database files."""
    return _make_db(tmp_path / "db1.duckdb", [T1_SQL]), _make_db(tmp_path / "db2.duckdb", [T2_SQL])


@pytest.fixture
def combined_db(tmp_path: Path) -> str:
    """t1 and t2 in one database file."""
    return _make_db(tmp_path / "combined.duckdb", [T1_SQL, T2_SQL])


@pytest.fixture
def contract(tmp_path: Path) -> str:
    lines = [
        "kind: DataContract",
        "apiVersion: v3.0.2",
        "version: 1.0.0",
        "id: pooled-passthrough",
        "status: draft",
        "name: pooled-passthrough",
        "schema:",
    ]
    for schema, checks in CHECKS.items():
        lines += [f"  - name: {schema}", "    physicalType: table", "    properties:"]
        for column in COLUMNS[schema]:
            lines += [f"      - name: {column}", "        logicalType: integer"]
        lines.append("    quality:")
        for name, query in checks.items():
            lines += [
                "      - type: sql",
                f"        name: {name}",
                "        mustBe: 0",
                f'        query: "{query}"',
            ]
    path = tmp_path / "contract.yaml"
    path.write_text("\n".join(lines) + "\n")
    return str(path)


def _pooled(db_path: str, max_concurrency: int = 2) -> PooledAdapter:
    return PooledAdapter(
        lambda: IbisAdapter(ibis.duckdb.connect(db_path, read_only=True)),
        max_concurrency=max_concurrency,
    )


def _run(contract: str, **kwargs) -> dict[str, object]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = validate_data(contract, **kwargs)
    return {cr.check_name: cr for cr in result.check_results if cr.check_name in CHECKS["t1"] | CHECKS["t2"]}


def _assert_expected(results: dict[str, object]) -> None:
    assert results["no_negative_amount"].status == "PASSED"
    assert results["amount_at_most_100"].status == "FAILED"
    assert results["amount_at_most_100"].actual_value == 1
    assert results["no_negative_id"].status == "PASSED"
    assert results["t2_orphans"].status == "FAILED"
    assert results["t2_orphans"].actual_value == 1


def test_adapters_dict_of_pooled_adapters_is_used_as_given(contract, split_dbs):
    db1, db2 = split_dbs
    pooled = {"t1": _pooled(db1), "t2": _pooled(db2)}

    multi = ValidationRunner(contract, adapters=pooled)._resolve_adapters()

    assert multi.adapters["t1"] is pooled["t1"]
    assert multi.adapters["t2"] is pooled["t2"]


def test_adapters_dict_of_pooled_adapters_runs(contract, split_dbs):
    db1, db2 = split_dbs

    results = _run(contract, adapters={"t1": _pooled(db1), "t2": _pooled(db2)})

    _assert_expected(results)


def test_single_pooled_adapter_serves_multi_schema_contract(contract, combined_db):
    results = _run(contract, adapter=_pooled(combined_db))

    _assert_expected(results)


def test_cross_schema_join_across_pooled_adapters(contract, split_dbs):
    db1, db2 = split_dbs

    results = _run(contract, adapters={"t1": _pooled(db1), "t2": _pooled(db2)})

    # t2_orphans joins t2 (db2) with t1 (db1), so it can only pass through
    # both pools. The one orphan is the t2 row pointing at t1_id 9.
    orphans = results["t2_orphans"]
    assert orphans.status == "FAILED"
    assert orphans.actual_value == 1
    assert orphans.failed_rows_count == 1


def test_pooled_and_plain_ibis_adapters_mix(contract, split_dbs):
    db1, db2 = split_dbs

    results = _run(contract, adapters={"t1": _pooled(db1), "t2": IbisAdapter(ibis.duckdb.connect(db2, read_only=True))})

    _assert_expected(results)
