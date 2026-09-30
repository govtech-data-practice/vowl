"""Checks that read tables the contract does not declare as schemas.

A lookup table such as ``audit_log`` is served by the adapter of the schema
the check sits under. These tests pin that rule for single-table checks and
for joins, and record which tables get downloaded so the execution mode is
visible: no download means the check ran natively on the source connection.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import duckdb
import ibis
import pytest

from vowl import validate_data
from vowl.adapters import IbisAdapter, MultiSourceAdapter
from vowl.adapters.pooled_adapter import PooledAdapter

# db1 holds t1 and a lookup table audit_log that no schema declares.
# db2 holds t2 and its own, different audit_log.
DB1_SQL = [
    "CREATE TABLE t1 AS SELECT * FROM (VALUES (1), (2), (3)) v(id)",
    "CREATE TABLE audit_log AS SELECT * FROM (VALUES (1, 0), (2, 0)) v(record_id, flagged)",
]
DB2_SQL = [
    "CREATE TABLE t2 AS SELECT * FROM (VALUES (1), (2), (3)) v(id)",
    "CREATE TABLE audit_log AS SELECT * FROM (VALUES (1, 1), (2, 1), (3, 1)) v(record_id, flagged)",
]

# Rows of the joined table with no audit_log entry. t1 and t2 both hold ids
# 1 to 3, so the db1 copy of audit_log leaves 1 row out and the db2 copy 0.
MISSING_FROM_AUDIT = (
    "SELECT COUNT(*) FROM {table} LEFT JOIN audit_log a ON {table}.id = a.record_id WHERE a.record_id IS NULL"
)
FLAGGED_JOIN = "SELECT COUNT(*) FROM {table} JOIN audit_log a ON {table}.id = a.record_id WHERE a.flagged = 1"


@pytest.fixture
def dbs(tmp_path: Path) -> tuple[str, str]:
    paths = []
    for name, statements in (("db1", DB1_SQL), ("db2", DB2_SQL)):
        path = str(tmp_path / f"{name}.duckdb")
        con = duckdb.connect(path)
        for statement in statements:
            con.execute(statement)
        con.close()
        paths.append(path)
    return paths[0], paths[1]


@pytest.fixture
def downloads(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int]]:
    """Record every ``(table, connection id)`` exported for a local copy."""
    calls: list[tuple[str, int]] = []
    original = IbisAdapter.export_table_as_arrow

    def recording(self, schema_name):
        calls.append((schema_name, id(self._con)))
        return original(self, schema_name)

    monkeypatch.setattr(IbisAdapter, "export_table_as_arrow", recording)
    return calls


def write_contract(tmp_path: Path, checks_by_schema: dict[str, dict[str, str]]) -> str:
    lines = [
        "kind: DataContract",
        "apiVersion: v3.0.2",
        "version: 1.0.0",
        "id: undeclared",
        "status: draft",
        "name: undeclared",
        "schema:",
    ]
    for schema, checks in checks_by_schema.items():
        lines += [
            f"  - name: {schema}",
            "    physicalType: table",
            "    properties:",
            "      - name: id",
            "        logicalType: integer",
        ]
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


def run(contract: str, **kwargs) -> dict[str, object]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = validate_data(contract, **kwargs)
    return {cr.check_name: cr for cr in result.check_results if not cr.check_name.startswith("id_")}


def test_single_adapter_join_with_lookup_table_runs_natively(tmp_path, dbs, downloads):
    db1, _ = dbs
    contract = write_contract(tmp_path, {"t1": {"join_lookup": FLAGGED_JOIN.format(table="t1")}})

    results = run(contract, adapter=IbisAdapter(ibis.duckdb.connect(db1)))

    assert results["join_lookup"].status == "PASSED"
    assert downloads == []


def test_join_with_lookup_table_on_own_connection_runs_natively(tmp_path, dbs, downloads):
    db1, db2 = dbs
    contract = write_contract(
        tmp_path,
        {"t1": {"join_lookup": MISSING_FROM_AUDIT.format(table="t1")}, "t2": {}},
    )

    results = run(
        contract,
        adapters={"t1": IbisAdapter(ibis.duckdb.connect(db1)), "t2": IbisAdapter(ibis.duckdb.connect(db2))},
    )

    assert results["join_lookup"].status == "FAILED"
    assert results["join_lookup"].failed_rows_count == 1
    assert downloads == []


def test_cross_connection_join_downloads_lookup_table_through_owner(tmp_path, dbs, downloads):
    db1, db2 = dbs
    con1, con2 = ibis.duckdb.connect(db1), ibis.duckdb.connect(db2)
    contract = write_contract(
        tmp_path,
        {"t1": {"t2_vs_db1_audit": MISSING_FROM_AUDIT.format(table="t2")}, "t2": {}},
    )

    results = run(contract, adapters={"t1": IbisAdapter(con1), "t2": IbisAdapter(con2)})

    # t2 comes from db2 and audit_log from db1, the connection of schema t1.
    # Compared as a set because fetching failed rows later downloads again.
    assert results["t2_vs_db1_audit"].status == "FAILED"
    assert results["t2_vs_db1_audit"].failed_rows_count == 1
    assert set(downloads) == {("t2", id(con2)), ("audit_log", id(con1))}


def _crossed_contract(tmp_path: Path) -> str:
    # Each check joins the other schema's table, so both run in local DuckDB,
    # and each reads audit_log through its own schema's connection.
    return write_contract(
        tmp_path,
        {
            "t1": {"t2_vs_db1_audit": MISSING_FROM_AUDIT.format(table="t2")},
            "t2": {"t1_vs_db2_audit": MISSING_FROM_AUDIT.format(table="t1")},
        },
    )


def test_lookup_table_with_same_name_resolves_per_owning_schema(tmp_path, dbs, downloads):
    db1, db2 = dbs
    con1, con2 = ibis.duckdb.connect(db1), ibis.duckdb.connect(db2)

    results = run(_crossed_contract(tmp_path), adapters={"t1": IbisAdapter(con1), "t2": IbisAdapter(con2)})

    assert results["t2_vs_db1_audit"].failed_rows_count == 1
    assert results["t1_vs_db2_audit"].status == "PASSED"
    assert ("audit_log", id(con1)) in downloads
    assert ("audit_log", id(con2)) in downloads


def test_lookup_table_with_same_name_resolves_per_owning_schema_in_parallel(tmp_path, dbs):
    db1, db2 = dbs
    adapters = MultiSourceAdapter(
        {
            "t1": PooledAdapter(lambda: IbisAdapter(ibis.duckdb.connect(db1, read_only=True)), max_concurrency=2),
            "t2": PooledAdapter(lambda: IbisAdapter(ibis.duckdb.connect(db2, read_only=True)), max_concurrency=2),
        }
    )

    results = run(_crossed_contract(tmp_path), adapters=adapters)

    assert results["t2_vs_db1_audit"].failed_rows_count == 1
    assert results["t1_vs_db2_audit"].status == "PASSED"


def test_lookup_table_missing_from_owner_connection_reports_database_error(tmp_path, dbs):
    db1, db2 = dbs
    # audit_log lives in db1 and db2 in the fixture, so drop it from db2.
    con = duckdb.connect(db2)
    con.execute("DROP TABLE audit_log")
    con.close()
    contract = write_contract(
        tmp_path,
        {"t1": {}, "t2": {"t1_vs_db2_audit": MISSING_FROM_AUDIT.format(table="t1")}},
    )

    from conftest import _ALLOWED_ERROR_SUBSTRINGS

    token = _ALLOWED_ERROR_SUBSTRINGS.set(("audit_log does not exist",))
    try:
        results = run(
            contract,
            adapters={"t1": IbisAdapter(ibis.duckdb.connect(db1)), "t2": IbisAdapter(ibis.duckdb.connect(db2))},
        )
    finally:
        _ALLOWED_ERROR_SUBSTRINGS.reset(token)

    assert results["t1_vs_db2_audit"].status == "ERROR"
    assert "audit_log" in results["t1_vs_db2_audit"].details
    assert "No adapter configured" not in results["t1_vs_db2_audit"].details
