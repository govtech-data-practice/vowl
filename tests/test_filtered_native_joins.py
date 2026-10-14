"""Joins on one connection run natively even when adapters have filters.

Each table in a native join keeps the filter conditions of the adapter that
serves it, the same conditions a local-copy run would export it with. These
tests record which tables get downloaded so the execution mode is visible,
and compare every native answer with a baseline that forces local copies by
giving each schema its own connection object to the same database file.

The data is chosen so that dropping any single filter changes the count.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import duckdb
import ibis
import pytest

from vowl import validate_data
from vowl.adapters import IbisAdapter, MultiSourceAdapter
from vowl.adapters.models import FilterCondition
from vowl.adapters.pooled_adapter import PooledAdapter
from vowl.contracts.contract import Contract
from vowl.contracts.sql_transforms import matching_filter_conditions
from vowl.executors.multi_source_sql_executor import MultiSourceSQLExecutor

DB_SQL = [
    "CREATE TABLE customers AS SELECT * FROM (VALUES "
    "(1, true, 1), (2, true, 2), (3, false, 3), (7, true, 8), (8, true, 9)"
    ") v(id, active, score)",
    "CREATE TABLE orders AS SELECT * FROM (VALUES "
    "(1, 1, DATE '2024-03-01', 6), (2, 2, DATE '2024-03-01', 7), (3, 3, DATE '2024-03-01', 5), "
    "(4, 4, DATE '2023-01-01', 1), (5, 5, DATE '2024-03-01', 9), (6, 3, DATE '2023-01-01', 2), "
    "(7, 6, DATE '2023-06-01', 8), (8, 7, DATE '2024-05-01', 5), (9, 8, DATE '2023-02-01', 6)"
    ") v(id, customer_id, created_at, score)",
    # A lookup table the contract does not declare.
    "CREATE TABLE blocklist AS SELECT * FROM (VALUES "
    "(1, true), (2, false), (3, true), (7, true), (8, true)"
    ") v(customer_id, active)",
]

ORPHAN_ORDERS = "SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON o.customer_id = c.id WHERE c.id IS NULL"
BLOCKED_ORDERS = (
    "SELECT COUNT(*) FROM orders o JOIN customers c ON o.customer_id = c.id JOIN blocklist b ON b.customer_id = c.id"
)

RECENT = FilterCondition(field="created_at", operator=">=", value="2024-01-01")
ACTIVE = {"field": "active", "operator": "=", "value": True}

# Orphan orders for each filter combination on this data:
#   no filters 3, orders only 1, customers only 5, both 2.
BOTH_FILTERS = {"orders": RECENT, "customers": ACTIVE}


@pytest.fixture
def db(tmp_path: Path) -> str:
    path = str(tmp_path / "shop.duckdb")
    con = duckdb.connect(path)
    for statement in DB_SQL:
        con.execute(statement)
    con.close()
    return path


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


def connect(path: str):
    # Read-only everywhere, so the native and baseline connections to one
    # file share a DuckDB configuration.
    return ibis.duckdb.connect(path, read_only=True)


def write_contract(tmp_path: Path, checks_by_schema: dict[str, dict[str, str]]) -> str:
    lines = [
        "kind: DataContract",
        "apiVersion: v3.0.2",
        "version: 1.0.0",
        "id: filtered_joins",
        "status: draft",
        "name: filtered_joins",
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


def outcome(result) -> tuple[object, object, object]:
    return result.status, result.actual_value, result.failed_rows_count


def baseline(contract: str, db: str, filters_by_schema: dict[str, dict]) -> dict[str, object]:
    """Run with one connection object per schema, which forces local copies."""
    return run(contract, adapters={schema: IbisAdapter(connect(db), f) for schema, f in filters_by_schema.items()})


def orphan_contract(tmp_path: Path) -> str:
    return write_contract(tmp_path, {"orders": {"orphans": ORPHAN_ORDERS}, "customers": {}})


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


def test_matching_filter_conditions_collects_exact_and_glob_matches_in_order():
    first = FilterCondition(field="a", operator="=", value=1)
    second = FilterCondition(field="b", operator="=", value=2)
    third = {"field": "c", "operator": "=", "value": 3}
    filters = {"ord*": [first, second], "customers": third, "*": third, "orders": first}

    assert matching_filter_conditions("orders", filters) == [first, second, third, first]
    assert matching_filter_conditions("customers", filters) == [third, third]
    assert matching_filter_conditions("other", {"orders": first}) == []
    assert matching_filter_conditions("orders", {}) == []
    assert matching_filter_conditions("orders", None) == []


def test_ibis_adapter_compatibility_ignores_filter_conditions(db):
    con = connect(db)
    filtered = IbisAdapter(con, {"orders": RECENT})

    assert filtered.is_compatible_with(IbisAdapter(con)) is True
    assert IbisAdapter(con).is_compatible_with(filtered) is True
    assert filtered.is_compatible_with(IbisAdapter(con, {"customers": ACTIVE})) is True
    assert filtered.is_compatible_with(IbisAdapter(connect(db), {"orders": RECENT})) is False


def test_with_filter_conditions_returns_copy_on_same_connection(db):
    con = connect(db)
    adapter = IbisAdapter(con, {"orders": RECENT})
    adapter.max_failed_rows = 7
    merged = {"orders": [RECENT], "customers": [ACTIVE]}

    clone = adapter.with_filter_conditions(merged)

    assert clone is not adapter
    assert isinstance(clone, IbisAdapter)
    assert clone.get_connection() is con
    assert clone.filter_conditions == merged
    assert clone.max_failed_rows == 7
    # The original keeps its own filters, and later edits to the input dict
    # do not reach the copy.
    assert adapter.filter_conditions == {"orders": RECENT}
    merged["blocklist"] = [ACTIVE]
    assert "blocklist" not in clone.filter_conditions
    assert adapter.with_filter_conditions(None).filter_conditions == {}


# ---------------------------------------------------------------------------
# Native joins with filters
# ---------------------------------------------------------------------------


def test_single_adapter_with_filters_runs_join_natively(tmp_path, db, downloads):
    contract = orphan_contract(tmp_path)

    results = run(contract, adapter=IbisAdapter(connect(db), BOTH_FILTERS))

    assert downloads == []
    assert outcome(results["orphans"]) == ("FAILED", 2, 2)

    expected = baseline(contract, db, {"orders": BOTH_FILTERS, "customers": BOTH_FILTERS})
    assert downloads
    assert outcome(results["orphans"]) == outcome(expected["orphans"])


def test_adapters_with_equal_filters_on_one_connection_run_natively(tmp_path, db, downloads):
    contract = orphan_contract(tmp_path)
    con = connect(db)

    results = run(
        contract,
        adapters={"orders": IbisAdapter(con, BOTH_FILTERS), "customers": IbisAdapter(con, BOTH_FILTERS)},
    )

    assert downloads == []
    assert outcome(results["orphans"]) == ("FAILED", 2, 2)

    expected = baseline(contract, db, {"orders": BOTH_FILTERS, "customers": BOTH_FILTERS})
    assert downloads
    assert outcome(results["orphans"]) == outcome(expected["orphans"])


def test_adapters_with_different_filters_on_one_connection_apply_both(tmp_path, db, downloads):
    contract = orphan_contract(tmp_path)
    con = connect(db)
    filters = {"orders": {"orders": RECENT}, "customers": {"customers": ACTIVE}}

    results = run(contract, adapters={schema: IbisAdapter(con, f) for schema, f in filters.items()})

    # 1 without the customers filter, 5 without the orders filter.
    assert downloads == []
    assert outcome(results["orphans"]) == ("FAILED", 2, 2)
    rendered = results["orphans"].metadata["rendered_implementation"]
    assert "created_at" in rendered
    assert "active" in rendered

    expected = baseline(contract, db, filters)
    assert downloads
    assert outcome(results["orphans"]) == outcome(expected["orphans"])


def test_wildcard_filters_apply_per_table_adapter(tmp_path, db, downloads):
    contract = orphan_contract(tmp_path)
    con = connect(db)
    # Both adapters key a different condition on the same column under "*".
    # Orphans are 4 with each table filtered by its own adapter, 5 or 1 if
    # one adapter's condition reached both tables.
    filters = {
        "orders": {"*": FilterCondition(field="score", operator=">=", value=5)},
        "customers": {"*": FilterCondition(field="score", operator="<", value=5)},
    }

    results = run(contract, adapters={schema: IbisAdapter(con, f) for schema, f in filters.items()})

    assert downloads == []
    assert outcome(results["orphans"]) == ("FAILED", 4, 4)

    expected = baseline(contract, db, filters)
    assert downloads
    assert outcome(results["orphans"]) == outcome(expected["orphans"])


def test_undeclared_lookup_table_takes_owner_adapter_filters(tmp_path, db, downloads):
    contract = write_contract(tmp_path, {"orders": {"blocked": BLOCKED_ORDERS}, "customers": {}})
    con = connect(db)
    # blocklist is served by the orders adapter, the schema the check sits
    # under. Dropping any one of the three filters gives 3 instead of 2.
    filters = {
        "orders": {"orders": RECENT, "blocklist": ACTIVE},
        "customers": {"customers": ACTIVE, "blocklist": {"field": "active", "operator": "=", "value": False}},
    }

    results = run(contract, adapters={schema: IbisAdapter(con, f) for schema, f in filters.items()})

    assert downloads == []
    assert outcome(results["blocked"]) == ("FAILED", 2, 2)

    orders_con, customers_con = connect(db), connect(db)
    expected = run(
        contract,
        adapters={
            "orders": IbisAdapter(orders_con, filters["orders"]),
            "customers": IbisAdapter(customers_con, filters["customers"]),
        },
    )
    assert ("blocklist", id(orders_con)) in downloads
    assert ("blocklist", id(customers_con)) not in downloads
    assert outcome(results["blocked"]) == outcome(expected["blocked"])


def test_adapter_without_filter_override_falls_back_to_local_copies(tmp_path, db, downloads):
    class NoOverride(IbisAdapter):
        with_filter_conditions = None

    contract = orphan_contract(tmp_path)
    con = connect(db)

    results = run(
        contract,
        adapters={"orders": NoOverride(con, {"orders": RECENT}), "customers": NoOverride(con, {"customers": ACTIVE})},
    )

    assert {table for table, _ in downloads} == {"orders", "customers"}
    assert outcome(results["orphans"]) == ("FAILED", 2, 2)


# ---------------------------------------------------------------------------
# Parallel path
# ---------------------------------------------------------------------------


def test_pooled_adapters_with_filters_give_filtered_answers(tmp_path, db):
    contract = write_contract(
        tmp_path,
        {"orders": {"orphans": ORPHAN_ORDERS, "blocked": BLOCKED_ORDERS}, "customers": {}},
    )
    filters = {
        "orders": {"orders": RECENT, "blocklist": ACTIVE},
        "customers": {"customers": ACTIVE},
    }
    adapters = MultiSourceAdapter(
        {
            schema: PooledAdapter(lambda f=f: IbisAdapter(connect(db), f), max_concurrency=2)
            for schema, f in filters.items()
        }
    )

    results = run(contract, adapters=adapters)

    assert outcome(results["orphans"]) == ("FAILED", 2, 2)
    assert outcome(results["blocked"]) == ("FAILED", 2, 2)
    expected = baseline(contract, db, filters)
    assert outcome(results["orphans"]) == outcome(expected["orphans"])
    assert outcome(results["blocked"]) == outcome(expected["blocked"])


def _cross_refs(contract_path: str) -> list:
    refs = Contract.load(contract_path).get_check_references_by_schema()["orders"]
    return [ref for ref in refs if ref.get_check_name() in {"orphans", "blocked"}]


def test_parallel_path_runs_filtered_same_connection_checks_natively(tmp_path, db, downloads):
    contract = write_contract(
        tmp_path,
        {"orders": {"orphans": ORPHAN_ORDERS, "blocked": BLOCKED_ORDERS}, "customers": {}},
    )
    con = connect(db)
    multi = MultiSourceAdapter(
        {
            "orders": IbisAdapter(con, {"orders": RECENT, "blocklist": ACTIVE}),
            "customers": IbisAdapter(con, {"customers": ACTIVE}),
        }
    )
    executor = MultiSourceSQLExecutor(multi)

    results = {r.check_name: r for r in executor._run_parallel_mode2(_cross_refs(contract), 1)}

    assert downloads == []
    assert outcome(results["orphans"]) == ("FAILED", 2, 2)
    assert outcome(results["blocked"]) == ("FAILED", 2, 2)


def test_parallel_path_downloads_tables_when_native_run_is_not_possible(tmp_path, db, downloads):
    class NoOverride(IbisAdapter):
        with_filter_conditions = None

    contract = write_contract(
        tmp_path,
        {"orders": {"orphans": ORPHAN_ORDERS, "blocked": BLOCKED_ORDERS}, "customers": {}},
    )
    con = connect(db)
    multi = MultiSourceAdapter(
        {
            "orders": NoOverride(con, {"orders": RECENT, "blocklist": ACTIVE}),
            "customers": NoOverride(con, {"customers": ACTIVE}),
        }
    )
    executor = MultiSourceSQLExecutor(multi)

    # One worker, since both adapters share a connection that is not safe
    # to use from several threads at once.
    results = {r.check_name: r for r in executor._run_parallel_mode2(_cross_refs(contract), 1)}

    assert {table for table, _ in downloads} == {"orders", "customers", "blocklist"}
    assert outcome(results["orphans"]) == ("FAILED", 2, 2)
    assert outcome(results["blocked"]) == ("FAILED", 2, 2)
