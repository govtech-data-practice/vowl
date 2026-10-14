"""One PooledAdapter serving several schemas.

``MultiSourceAdapter`` gives each schema its own shallow copy of a shared
pool. These tests pin that the copies act as one pool: their single-table
checks share the pool's workers, joins between their tables run on a leased
connection, and ``max_concurrency`` caps the connections of all copies
together.
"""

from __future__ import annotations

import copy
import threading
import time
import warnings
from pathlib import Path

import duckdb
import ibis
import pytest

from vowl import validate_data
from vowl.adapters import IbisAdapter
from vowl.adapters.pooled_adapter import PooledAdapter
from vowl.executors.ibis_sql_executor import IbisSQLExecutor

SCHEMAS = ("s0", "s1", "s2", "s3")
ORPHANS = "SELECT COUNT(*) FROM {s} x LEFT JOIN s0 y ON x.ref = y.id WHERE y.id IS NULL"


@pytest.fixture
def db(tmp_path: Path) -> str:
    """Four tables. s0 holds ids 1 to 4, the others point at ids 1 to 5."""
    path = tmp_path / "shared.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE s0 AS SELECT range AS id, range AS ref FROM range(1, 5)")
    for schema in SCHEMAS[1:]:
        con.execute(f"CREATE TABLE {schema} AS SELECT range AS id, range AS ref FROM range(1, 6)")
    con.close()
    return str(path)


def write_contract(tmp_path: Path, singles: int, joins: bool) -> str:
    lines = [
        "kind: DataContract",
        "apiVersion: v3.0.2",
        "version: 1.0.0",
        "id: shared-pool",
        "status: draft",
        "name: shared-pool",
        "schema:",
    ]
    for schema in SCHEMAS:
        lines += [
            f"  - name: {schema}",
            "    physicalType: table",
            "    properties:",
            "      - name: id",
            "        logicalType: integer",
            "      - name: ref",
            "        logicalType: integer",
        ]
        checks = {f"{schema}_single_{i}": f"SELECT COUNT(*) FROM {schema} WHERE id < -{i}" for i in range(singles)}
        if joins and schema != "s0":
            checks[f"{schema}_orphans"] = ORPHANS.format(s=schema)
        if checks:
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


def run(contract: str, **kwargs):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return validate_data(contract, **kwargs)


def pooled(db: str, max_concurrency: int = 4, **adapter_kwargs) -> PooledAdapter:
    return PooledAdapter(
        lambda: IbisAdapter(ibis.duckdb.connect(db, read_only=True), **adapter_kwargs),
        max_concurrency=max_concurrency,
    )


@pytest.fixture
def in_flight(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Record the peak number of checks, schemas and joins running at once."""
    state = {"checks": 0, "peak_checks": 0, "peak_schemas": 0, "joins": 0, "peak_joins": 0}
    per_schema: dict[str, int] = {}
    lock = threading.Lock()
    original = IbisSQLExecutor.run_single_check

    def recording(self, check_ref):
        schema = check_ref.get_schema_name()
        join = check_ref.get_check_name().endswith("_orphans")
        with lock:
            state["checks"] += 1
            state["joins"] += join
            state["peak_joins"] = max(state["peak_joins"], state["joins"])
            per_schema[schema] = per_schema.get(schema, 0) + 1
            state["peak_checks"] = max(state["peak_checks"], state["checks"])
            state["peak_schemas"] = max(state["peak_schemas"], sum(1 for n in per_schema.values() if n))
        try:
            # Stand-in for a slow warehouse query, so checks overlap
            time.sleep(0.05)
            return original(self, check_ref)
        finally:
            with lock:
                state["checks"] -= 1
                state["joins"] -= join
                per_schema[schema] -= 1

    monkeypatch.setattr(IbisSQLExecutor, "run_single_check", recording)
    return state


@pytest.fixture
def downloads(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every table copied out of a database for a join."""
    calls: list[str] = []
    original = IbisAdapter.export_table_as_arrow

    def recording(self, schema_name):
        calls.append(schema_name)
        return original(self, schema_name)

    monkeypatch.setattr(IbisAdapter, "export_table_as_arrow", recording)
    return calls


def test_schemas_sharing_a_pool_run_side_by_side(tmp_path, db, in_flight):
    pool = pooled(db, max_concurrency=4)

    result = run(write_contract(tmp_path, singles=2, joins=False), adapter=pool)

    assert all(cr.status == "PASSED" for cr in result.check_results)
    assert in_flight["peak_schemas"] > 1
    assert in_flight["peak_checks"] <= 4
    assert pool._created_count <= 4


def test_shared_pool_keeps_result_order(tmp_path, db):
    contract = write_contract(tmp_path, singles=2, joins=True)

    plain = run(contract, adapter=IbisAdapter(ibis.duckdb.connect(db, read_only=True)))
    pool = run(contract, adapter=pooled(db, max_concurrency=4))

    assert [cr.check_name for cr in pool.check_results] == [cr.check_name for cr in plain.check_results]
    assert [cr.status for cr in pool.check_results] == [cr.status for cr in plain.check_results]


def test_join_on_one_pool_runs_in_the_database(tmp_path, db, downloads, in_flight):
    result = run(write_contract(tmp_path, singles=0, joins=True), adapter=pooled(db, max_concurrency=2))

    orphans = {cr.check_name: cr for cr in result.check_results if cr.check_name.endswith("_orphans")}
    assert len(orphans) == 3
    # Every table points at ref 5, which s0 does not hold
    assert all(cr.status == "FAILED" and cr.actual_value == 1 for cr in orphans.values())
    assert downloads == []
    # The joins run on leased connections side by side
    assert in_flight["peak_joins"] == 2


def test_filtered_join_on_one_pool_applies_the_filters(tmp_path, db, downloads):
    # Keeping s0 ids from 3 up leaves 3 and 4, so refs 1, 2 and 5 orphan
    only_from_3 = {"s0": {"field": "id", "operator": ">=", "value": 3}}

    result = run(
        write_contract(tmp_path, singles=0, joins=True),
        adapter=pooled(db, max_concurrency=2, filter_conditions=only_from_3),
    )

    orphans = [cr for cr in result.check_results if cr.check_name.endswith("_orphans")]
    assert len(orphans) == 3
    assert all(cr.actual_value == 3 for cr in orphans)
    assert downloads == []


def test_separate_pools_still_copy_the_join(tmp_path, db, downloads):
    adapters = {schema: pooled(db, max_concurrency=2) for schema in SCHEMAS}

    result = run(write_contract(tmp_path, singles=0, joins=True), adapters=adapters)

    orphans = [cr for cr in result.check_results if cr.check_name.endswith("_orphans")]
    assert all(cr.status == "FAILED" and cr.actual_value == 1 for cr in orphans)
    assert "s0" in downloads


@pytest.mark.parametrize("pooled_schema", ["s0", "s1"])
def test_pool_and_plain_adapter_on_one_connection_copy_the_join(tmp_path, db, downloads, pooled_schema):
    con = ibis.duckdb.connect(db, read_only=True)
    # The pool hands out adapters on the plain adapter's own connection
    adapters = {schema: IbisAdapter(con) for schema in SCHEMAS}
    adapters[pooled_schema] = PooledAdapter(lambda: IbisAdapter(con), max_concurrency=1)

    result = run(write_contract(tmp_path, singles=0, joins=True), adapters=adapters)

    orphans = [cr for cr in result.check_results if cr.check_name.endswith("_orphans")]
    assert all(cr.status == "FAILED" and cr.actual_value == 1 for cr in orphans)
    assert downloads


class TestPoolCopies:
    def test_copies_share_the_connection_limit(self, db):
        pool = pooled(db, max_concurrency=2)
        first, second, third = pool, copy.copy(pool), copy.copy(pool)
        held = [first._checkout(), second._checkout()]
        got: list[object] = []

        waiter = threading.Thread(target=lambda: got.append(third._checkout()))
        waiter.start()
        waiter.join(timeout=0.2)

        # Both connections are out, so the third copy waits for one
        assert waiter.is_alive()
        assert third._created_count == 2
        second._return(held.pop())
        waiter.join(timeout=5)
        assert not waiter.is_alive()
        assert got[0] in pool._all_instances
        assert len(pool._all_instances) == 2

    def test_cleanup_on_a_copy_clears_the_pool(self, db):
        pool = pooled(db, max_concurrency=2)
        other = copy.copy(pool)
        with other._lease():
            pass

        other.cleanup()

        assert pool._created_count == 0
        assert pool._primary is None
        assert pool._pool.empty()

    def test_copies_are_compatible_and_other_adapters_are_not(self, db):
        pool = pooled(db)

        assert pool.is_compatible_with(pool) is True
        assert pool.is_compatible_with(copy.copy(pool)) is True
        assert copy.copy(pool).is_compatible_with(pool) is True
        assert pool.is_compatible_with(pooled(db)) is False
        assert pool.is_compatible_with(IbisAdapter(ibis.duckdb.connect(db, read_only=True))) is False
