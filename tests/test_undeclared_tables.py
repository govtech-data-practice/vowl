"""Checks that read tables the contract does not declare as schemas.

A lookup table such as ``currencies`` is served by the adapter of the schema
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

# The sales database holds orders and a currencies lookup that no schema
# declares. The CRM database holds customers and its own copy of currencies.
# The sales copy is out of date and has no EUR row.
SALES_SQL = [
    "CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'SGD'), (2, 'USD'), (3, 'EUR')) v(order_id, currency)",
    "CREATE TABLE currencies AS SELECT * FROM (VALUES ('SGD', TRUE), ('USD', TRUE)) v(code, is_active)",
]
CRM_SQL = [
    "CREATE TABLE customers AS SELECT * FROM (VALUES (1, 'SGD'), (2, 'USD'), (3, 'EUR')) v(customer_id, currency)",
    "CREATE TABLE currencies AS SELECT * FROM (VALUES ('SGD', TRUE), ('USD', TRUE), ('EUR', TRUE)) v(code, is_active)",
]

# Rows of the joined table whose currency has no currencies entry. orders and
# customers both use SGD, USD and EUR, so the sales copy of currencies leaves
# 1 row out and the CRM copy 0.
UNKNOWN_CURRENCY = (
    "SELECT COUNT(*) FROM {table} LEFT JOIN currencies c ON {table}.currency = c.code WHERE c.code IS NULL"
)
INACTIVE_CURRENCY = "SELECT COUNT(*) FROM {table} JOIN currencies c ON {table}.currency = c.code WHERE NOT c.is_active"

KEY_COLUMNS = {"orders": "order_id", "customers": "customer_id"}


@pytest.fixture
def dbs(tmp_path: Path) -> tuple[str, str]:
    paths = []
    for name, statements in (("sales", SALES_SQL), ("crm", CRM_SQL)):
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
            f"      - name: {KEY_COLUMNS[schema]}",
            "        logicalType: integer",
            "      - name: currency",
            "        logicalType: string",
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
    return {cr.check_name: cr for cr in result.check_results if not cr.metadata.get("is_generated")}


def test_single_adapter_join_with_lookup_table_runs_natively(tmp_path, dbs, downloads):
    sales_db, _ = dbs
    contract = write_contract(tmp_path, {"orders": {"inactive_currency": INACTIVE_CURRENCY.format(table="orders")}})

    results = run(contract, adapter=IbisAdapter(ibis.duckdb.connect(sales_db)))

    assert results["inactive_currency"].status == "PASSED"
    assert downloads == []


def test_join_with_lookup_table_on_own_connection_runs_natively(tmp_path, dbs, downloads):
    sales_db, crm_db = dbs
    contract = write_contract(
        tmp_path,
        {"orders": {"unknown_currency": UNKNOWN_CURRENCY.format(table="orders")}, "customers": {}},
    )

    results = run(
        contract,
        adapters={
            "orders": IbisAdapter(ibis.duckdb.connect(sales_db)),
            "customers": IbisAdapter(ibis.duckdb.connect(crm_db)),
        },
    )

    assert results["unknown_currency"].status == "FAILED"
    assert results["unknown_currency"].failed_rows_count == 1
    assert downloads == []


def test_cross_connection_join_downloads_lookup_table_through_owner(tmp_path, dbs, downloads):
    sales_db, crm_db = dbs
    sales_con, crm_con = ibis.duckdb.connect(sales_db), ibis.duckdb.connect(crm_db)
    contract = write_contract(
        tmp_path,
        {"orders": {"customers_vs_sales_currencies": UNKNOWN_CURRENCY.format(table="customers")}, "customers": {}},
    )

    results = run(contract, adapters={"orders": IbisAdapter(sales_con), "customers": IbisAdapter(crm_con)})

    # customers comes from the CRM database and currencies from the sales
    # database, the connection of schema orders.
    # Compared as a set because fetching failed rows later downloads again.
    assert results["customers_vs_sales_currencies"].status == "FAILED"
    assert results["customers_vs_sales_currencies"].failed_rows_count == 1
    assert set(downloads) == {("customers", id(crm_con)), ("currencies", id(sales_con))}


def _crossed_contract(tmp_path: Path) -> str:
    # Each check joins the other schema's table, so both run in local DuckDB,
    # and each reads currencies through its own schema's connection.
    return write_contract(
        tmp_path,
        {
            "orders": {"customers_vs_sales_currencies": UNKNOWN_CURRENCY.format(table="customers")},
            "customers": {"orders_vs_crm_currencies": UNKNOWN_CURRENCY.format(table="orders")},
        },
    )


def test_lookup_table_with_same_name_resolves_per_owning_schema(tmp_path, dbs, downloads):
    sales_db, crm_db = dbs
    sales_con, crm_con = ibis.duckdb.connect(sales_db), ibis.duckdb.connect(crm_db)

    results = run(
        _crossed_contract(tmp_path), adapters={"orders": IbisAdapter(sales_con), "customers": IbisAdapter(crm_con)}
    )

    assert results["customers_vs_sales_currencies"].failed_rows_count == 1
    assert results["orders_vs_crm_currencies"].status == "PASSED"
    assert ("currencies", id(sales_con)) in downloads
    assert ("currencies", id(crm_con)) in downloads


def test_lookup_table_with_same_name_resolves_per_owning_schema_in_parallel(tmp_path, dbs):
    sales_db, crm_db = dbs
    adapters = MultiSourceAdapter(
        {
            "orders": PooledAdapter(
                lambda: IbisAdapter(ibis.duckdb.connect(sales_db, read_only=True)), max_concurrency=2
            ),
            "customers": PooledAdapter(
                lambda: IbisAdapter(ibis.duckdb.connect(crm_db, read_only=True)), max_concurrency=2
            ),
        }
    )

    results = run(_crossed_contract(tmp_path), adapters=adapters)

    assert results["customers_vs_sales_currencies"].failed_rows_count == 1
    assert results["orders_vs_crm_currencies"].status == "PASSED"


def test_lookup_table_missing_from_owner_connection_reports_database_error(tmp_path, dbs):
    sales_db, crm_db = dbs
    # currencies lives in both databases in the fixture, so drop the CRM copy.
    con = duckdb.connect(crm_db)
    con.execute("DROP TABLE currencies")
    con.close()
    contract = write_contract(
        tmp_path,
        {"orders": {}, "customers": {"orders_vs_crm_currencies": UNKNOWN_CURRENCY.format(table="orders")}},
    )

    from conftest import _ALLOWED_ERROR_SUBSTRINGS

    token = _ALLOWED_ERROR_SUBSTRINGS.set(("currencies does not exist",))
    try:
        results = run(
            contract,
            adapters={
                "orders": IbisAdapter(ibis.duckdb.connect(sales_db)),
                "customers": IbisAdapter(ibis.duckdb.connect(crm_db)),
            },
        )
    finally:
        _ALLOWED_ERROR_SUBSTRINGS.reset(token)

    assert results["orders_vs_crm_currencies"].status == "ERROR"
    assert "currencies" in results["orders_vs_crm_currencies"].details
    assert "No adapter configured" not in results["orders_vs_crm_currencies"].details
