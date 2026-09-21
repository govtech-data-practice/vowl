"""End-to-end scenarios for the auto-generated foreignKey (``relationships``) check.

These tests complement the AST/unit coverage in
``tests/test_check_reference_variations.py::TestForeignKeyCheck`` by driving the
ODCS v3.2.0 ``relationships`` (foreign key) feature through the **public**
:func:`vowl.validate_data` API against real DuckDB connections.  The fixtures in
``tests/relationships/`` use a generic orders/customers/products domain and
carry DELIBERATE orphan rows (so the check FAILS), passing rows, and NULL
foreign-key values (so the MATCH SIMPLE null-skip is exercised).

Scenarios covered:

1. Property-level, single-column FK executes and FAILS on the orphan while the
   NULL row is skipped; asserts the generated check name, FAILED status,
   ``consistency`` dimension, from-table ``schema_name`` and both tables in
   ``tables_in_query``.
2. The FK's failed rows MERGE onto the annotated from-table via
   ``get_output_dfs`` / ``get_annotated_output``: only the orphan row is
   flagged (not the null-skipped or passing rows) and only from-table columns
   appear.
3. Cross-source FK across TWO separate DuckDB connections (one adapter per
   schema): proves the MultiSourceExecutor path and FAILS on the orphan.
4. Composite, schema-level FK written in fully-qualified (FQN) notation: FAILS
   on the composite orphan and skips the single-null-key row.
5. Self-referential FK (single adapter, one physical table): FAILS on the
   dangling manager id and skips the NULL root.
6. External-file FK resolution: the main contract is loaded from disk so its
   ``origin`` is set, the external target's adapter is registered by name, and
   the resolved external target gates the rows (FAILS on the orphan).
7. Fully-passing contract: the same FK reports PASSED with zero failed rows.
"""

from __future__ import annotations

import ast
import json
import warnings
from pathlib import Path

import pandas as pd
import pytest

TEST_DIR = Path(__file__).parent
REL_DIR = TEST_DIR / "relationships"

RETAIL_CONTRACT = REL_DIR / "retail_datacontract.yaml"
CATALOG_CONTRACT = REL_DIR / "catalog_datacontract.yaml"
ORG_CONTRACT = REL_DIR / "org_datacontract.yaml"
ORDERS_EXTERNAL_CONTRACT = REL_DIR / "orders_external_datacontract.yaml"

# CSV dtypes chosen so that empty cells survive as real SQL NULL (not "").  The
# pandas ``Int64``/``string`` nullable dtypes preserve missing values end-to-end
# into DuckDB, which is required to prove the MATCH SIMPLE null-skip behaviour.
_ORDERS_DTYPES = {"order_id": "Int64", "customer_id": "Int64", "amount": "Int64"}
_CUSTOMERS_DTYPES = {"customer_id": "Int64", "customer_name": "string"}
_PRODUCTS_DTYPES = {"category": "string", "sku": "string", "product_name": "string"}
_ORDER_ITEMS_DTYPES = {"item_id": "Int64", "category": "string", "sku": "string", "qty": "Int64"}
_EMPLOYEES_DTYPES = {"employee_id": "Int64", "manager_id": "Int64", "employee_name": "string"}


# ============================================================================
# Helpers
# ============================================================================


def _load_csv(name: str, dtypes: dict[str, str]) -> pd.DataFrame:
    """Read a fixture CSV preserving NULLs via nullable dtypes."""
    return pd.read_csv(REL_DIR / name, dtype=dtypes)


def assert_no_check_errors(results):
    """Fail if any check reported ERROR status (as opposed to PASSED/FAILED).

    Mirrors the helper in ``tests/test_usage_patterns.py`` so a check that blew
    up during execution never masquerades as a passing scenario.
    """
    results_df = results.get_check_results_df().to_pandas()
    error_checks = results_df[results_df["status"].str.upper() == "ERROR"]
    if len(error_checks) > 0:
        error_info = error_checks[["check_name", "status", "message"]].to_dict("records")
        pytest.fail(f"Checks returned ERROR: {error_info}")


def _fk_row(results, check_name: str, *, include_definition: bool = False) -> dict:
    """Return the single results-df row for ``check_name`` as a dict."""
    df = results.get_check_results_df(include_check_definition=include_definition).to_pandas()
    matches = df[df["check_name"] == check_name]
    assert len(matches) == 1, f"expected exactly one {check_name!r} check, found {len(matches)}"
    return matches.iloc[0].to_dict()


def _tables_in_query(cell) -> set[str]:
    """Normalise a ``tables_in_query`` cell (list or repr-string) to a set."""
    if isinstance(cell, (list, tuple)):
        return set(cell)
    if isinstance(cell, str) and cell.strip().startswith("["):
        return set(ast.literal_eval(cell))
    return {cell} if cell else set()


def _dimension_of(row: dict) -> str:
    """Extract the ``dimension`` from a row's check_definition (dict or JSON)."""
    definition = row.get("check_definition")
    if isinstance(definition, str):
        definition = json.loads(definition)
    return definition.get("dimension")


def _check_names_of(cell) -> list[str]:
    """Ordered check names from an annotated ``check_info`` JSON cell."""
    if cell is None:
        return []
    parsed = json.loads(cell) if isinstance(cell, str) else cell
    return [item["check_name"] for item in parsed]


# ============================================================================
# 1. Property-level single-column FK: FAILS on orphan, skips NULL
# ============================================================================


class TestPropertyLevelForeignKey:
    """orders.customer_id -> customers.customer_id (shorthand notation)."""

    FK_NAME = "orders_customer_id_foreign_key_check"

    def _validate(self):
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        customers = _load_csv("customers.csv", _CUSTOMERS_DTYPES)
        orders = _load_csv("orders.csv", _ORDERS_DTYPES)

        con = ibis.duckdb.connect()
        con.create_table("customers", customers)
        con.create_table("orders", orders)

        return validate_data(
            contract=str(RETAIL_CONTRACT),
            adapters={"customers": IbisAdapter(con), "orders": IbisAdapter(con)},
        )

    def test_fk_fails_on_orphan_and_skips_null(self):
        results = self._validate()
        assert_no_check_errors(results)

        row = _fk_row(results, self.FK_NAME, include_definition=True)

        # The check executed and failed on exactly the one orphan row (order
        # 103 -> customer 99).  The NULL-customer row (102) is skipped by
        # MATCH SIMPLE, and the three valid rows pass.
        assert row["status"] == "FAILED"
        assert row["failed_rows_count"] == 1

        # Referential-integrity checks are the "consistency" dimension.
        assert _dimension_of(row) == "consistency"

        # Anchored on the from-table; the query spans both tables.
        assert row["schema_name"] == "orders"
        assert _tables_in_query(row["tables_in_query"]) == {"orders", "customers"}


# ============================================================================
# 2. Failed FK rows merge onto the annotated from-table
# ============================================================================


class TestForeignKeyAnnotatedMerge:
    """The FK's ``SELECT *`` failed-rows projection carries only the from-table
    columns, so it MERGES onto the annotated ``orders`` table rather than
    landing in residues."""

    FK_NAME = "orders_customer_id_foreign_key_check"

    def _validate(self):
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table("customers", _load_csv("customers.csv", _CUSTOMERS_DTYPES))
        con.create_table("orders", _load_csv("orders.csv", _ORDERS_DTYPES))
        return validate_data(
            contract=str(RETAIL_CONTRACT),
            adapters={"customers": IbisAdapter(con), "orders": IbisAdapter(con)},
        )

    def test_output_dfs_contains_only_orphan_row(self):
        results = self._validate()
        assert_no_check_errors(results)

        outputs = results.get_output_dfs()
        key = f"orders::{self.FK_NAME}"
        assert key in outputs, f"missing {key!r} in {list(outputs)}"

        failed = outputs[key].to_arrow().to_pylist()
        # Only the orphan order participates; null-skip and passing rows absent.
        assert [r["order_id"] for r in failed] == [103]
        assert {r["customer_id"] for r in failed} == {99}

    def test_annotated_output_flags_only_orphan_from_row(self):
        results = self._validate()
        out = results.get_annotated_output()

        annotated = out["annotated"]["orders"]
        # Only the from-table's own columns are present (no customers columns
        # bled in), confirming the merge onto the from-table.
        assert set(annotated.columns) >= {"order_id", "customer_id", "amount"}
        assert "customer_name" not in annotated.columns

        rows = {r["order_id"]: r["check_info"] for r in annotated.to_arrow().to_pylist()}
        # Orphan flagged...
        assert _check_names_of(rows[103]) == [self.FK_NAME]
        # ...null-skipped row (102) and passing rows (100/101/104) are not.
        assert rows[102] is None
        assert rows[100] is None and rows[101] is None and rows[104] is None

        # Because the check merged onto the from-table it is NOT a residue.
        residue_names: set[str] = set()
        for df in out["residues"].values():
            residue_names |= {n for cell in df["check_info"].to_list() for n in _check_names_of(cell)}
        assert self.FK_NAME not in residue_names


# ============================================================================
# 3. Cross-source FK across two separate adapters/connections
# ============================================================================


class TestCrossSourceForeignKey:
    """The two tables live in two independent DuckDB connections, so the check
    is routed through the MultiSourceExecutor."""

    FK_NAME = "orders_customer_id_foreign_key_check"

    def test_fk_fails_across_separate_connections(self):
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        customers_con = ibis.duckdb.connect()
        customers_con.create_table("customers", _load_csv("customers.csv", _CUSTOMERS_DTYPES))
        orders_con = ibis.duckdb.connect()
        orders_con.create_table("orders", _load_csv("orders.csv", _ORDERS_DTYPES))

        # Distinct adapters per schema -> genuinely cross-source join.
        results = validate_data(
            contract=str(RETAIL_CONTRACT),
            adapters={
                "customers": IbisAdapter(customers_con),
                "orders": IbisAdapter(orders_con),
            },
        )
        assert_no_check_errors(results)

        row = _fk_row(results, self.FK_NAME)
        assert row["status"] == "FAILED"
        assert row["failed_rows_count"] == 1
        assert _tables_in_query(row["tables_in_query"]) == {"orders", "customers"}


# ============================================================================
# 4. Composite, schema-level FK in fully-qualified (FQN) notation
# ============================================================================


class TestCompositeForeignKeyFqn:
    """order_items (category, sku) -> products (category, sku), where the target
    columns are addressed by FQN (``/schema/<id>/properties/<id>``)."""

    FK_NAME = "order_items_category_sku_foreign_key_check"

    def test_composite_fk_fails_on_orphan_and_skips_null_key(self):
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table("products", _load_csv("products.csv", _PRODUCTS_DTYPES))
        con.create_table("order_items", _load_csv("order_items.csv", _ORDER_ITEMS_DTYPES))

        results = validate_data(
            contract=str(CATALOG_CONTRACT),
            adapters={"products": IbisAdapter(con), "order_items": IbisAdapter(con)},
        )
        assert_no_check_errors(results)

        row = _fk_row(results, self.FK_NAME)
        # Composite orphan (books, B9) fails; the (books, NULL) row is skipped
        # because MATCH SIMPLE ignores rows with any NULL key column.
        assert row["status"] == "FAILED"
        assert row["failed_rows_count"] == 1
        assert row["schema_name"] == "order_items"
        assert _tables_in_query(row["tables_in_query"]) == {"order_items", "products"}


# ============================================================================
# 5. Self-referential FK (single physical table, single adapter)
# ============================================================================


class TestSelfReferentialForeignKey:
    """employees.manager_id -> employees.employee_id.  Only one table is
    detected, so the check stays single-table."""

    FK_NAME = "employees_manager_id_foreign_key_check"

    def test_self_referential_fk_fails_and_skips_root(self):
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table("employees", _load_csv("employees.csv", _EMPLOYEES_DTYPES))

        results = validate_data(
            contract=str(ORG_CONTRACT),
            adapters={"employees": IbisAdapter(con)},
        )
        assert_no_check_errors(results)

        row = _fk_row(results, self.FK_NAME)
        # The dangling manager (42) fails; the NULL-manager root is skipped.
        assert row["status"] == "FAILED"
        assert row["failed_rows_count"] == 1
        assert row["schema_name"] == "employees"
        # Self-referential -> exactly one table detected in the query.
        assert _tables_in_query(row["tables_in_query"]) == {"employees"}


# ============================================================================
# 6. External-file FK resolution
# ============================================================================


class TestExternalFileForeignKey:
    """The orders contract targets customers.customer_id defined in a SEPARATE
    file via ``file.yaml#/schema/.../properties/...``.  Resolving that reference
    needs the contract's ``origin``, so it must be loaded from disk."""

    FK_NAME = "orders_customer_id_foreign_key_check"

    def test_external_target_gates_rows(self):
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table("customers", _load_csv("customers.csv", _CUSTOMERS_DTYPES))
        con.create_table("orders", _load_csv("orders.csv", _ORDERS_DTYPES))
        adapter = IbisAdapter(con)

        # The main contract only declares the ``orders`` schema; ``customers``
        # lives in the external target.  Supplying the extra ``customers``
        # adapter makes it available for the resolved cross-file join.  Because
        # ``customers`` is a resolved foreign-key target it is NOT flagged as an
        # unknown adapter key: no "no schema with that name" warning is raised.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            results = validate_data(
                contract=str(ORDERS_EXTERNAL_CONTRACT),
                adapters={"orders": adapter, "customers": adapter},
            )
        assert not [w for w in caught if "no schema with that name" in str(w.message)], (
            "external FK target adapter should not trigger the unknown-schema warning"
        )
        assert_no_check_errors(results)

        row = _fk_row(results, self.FK_NAME)
        # The externally-resolved customers target still gates the rows: the
        # orphan fails and the NULL row is skipped.
        assert row["status"] == "FAILED"
        assert row["failed_rows_count"] == 1
        assert row["schema_name"] == "orders"
        assert _tables_in_query(row["tables_in_query"]) == {"orders", "customers"}

    def test_typo_adapter_key_still_warns(self):
        """A genuinely unknown adapter key (not a schema or FK target) still
        warns, so the check kept its typo-catching value after external targets
        were whitelisted.

        Exercised at the adapter-resolution layer so the assertion targets the
        warning without the full-run golden-output comparison.
        """
        import ibis

        from vowl.adapters import IbisAdapter
        from vowl.validation.runner import ValidationRunner

        adapter = IbisAdapter(ibis.duckdb.connect())
        runner = ValidationRunner(
            contract=str(ORDERS_EXTERNAL_CONTRACT),
            # ``orders`` is declared, ``customers`` is a resolved external FK
            # target, but ``custmoers`` is a typo that names neither.
            adapters={"orders": adapter, "customers": adapter, "custmoers": adapter},
        )
        with pytest.warns(UserWarning, match="'custmoers' but no schema with that name"):
            runner._resolve_adapters()


# ============================================================================
# 7. Fully-passing contract reports the FK check as PASSED
# ============================================================================


class TestForeignKeyPasses:
    """A dataset with no orphan rows reports the FK check as PASSED."""

    FK_NAME = "orders_customer_id_foreign_key_check"

    def test_fk_passes_when_all_references_resolve(self):
        import ibis

        from vowl import validate_data
        from vowl.adapters import IbisAdapter

        customers = _load_csv("customers.csv", _CUSTOMERS_DTYPES)
        orders = _load_csv("orders.csv", _ORDERS_DTYPES)
        # Drop the deliberate orphan (order 103 -> customer 99); the remaining
        # rows either resolve or carry a NULL customer_id (skipped).
        orders = orders[orders["order_id"] != 103].reset_index(drop=True)

        con = ibis.duckdb.connect()
        con.create_table("customers", customers)
        con.create_table("orders", orders)

        results = validate_data(
            contract=str(RETAIL_CONTRACT),
            adapters={"customers": IbisAdapter(con), "orders": IbisAdapter(con)},
        )
        assert_no_check_errors(results)

        row = _fk_row(results, self.FK_NAME)
        assert row["status"] == "PASSED"
        assert row["failed_rows_count"] == 0
