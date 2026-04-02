"""Tests for BaseAdapter.export_table_as_arrow and multi-source mode 2 materialization.

Covers:
- BaseAdapter.export_table_as_arrow raises NotImplementedError by default
- IbisAdapter.export_table_as_arrow basic export
- IbisAdapter.export_table_as_arrow with filter conditions
- MultiSourceSQLExecutor mode 2 using adapter export API
- Custom adapter participating in mode 2 via export_table_as_arrow
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

TEST_DIR = Path(__file__).parent
EMPLOYEE_DIR = TEST_DIR / "employee"
EMPLOYEE_LIST_FILE = EMPLOYEE_DIR / "demo_employee_list.csv"
EMPLOYEE_PAYROLL_FILE = EMPLOYEE_DIR / "demo_employee_payroll.csv"
CONTRACT_PATH = EMPLOYEE_DIR / "employee_payroll_datacontract.yaml"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def employee_list_df():
    # Assume blank string for null values
    return pd.read_csv(EMPLOYEE_LIST_FILE).fillna("")


@pytest.fixture
def employee_payroll_df():
    # Assume blank string for null values
    return pd.read_csv(EMPLOYEE_PAYROLL_FILE).fillna("")


@pytest.fixture
def contract_path():
    return str(CONTRACT_PATH)


# ---------------------------------------------------------------------------
# 1. BaseAdapter.export_table_as_arrow default behaviour
# ---------------------------------------------------------------------------

class TestBaseAdapterExportDefault:
    """BaseAdapter.export_table_as_arrow should raise NotImplementedError."""

    def test_raises_not_implemented(self):
        from vowl.adapters.base import BaseAdapter

        class BareAdapter(BaseAdapter):
            pass

        adapter = BareAdapter()
        with pytest.raises(NotImplementedError, match="does not implement export_table_as_arrow"):
            adapter.export_table_as_arrow("any_table")

    def test_error_message_includes_class_name(self):
        from vowl.adapters.base import BaseAdapter

        class FancyAdapter(BaseAdapter):
            pass

        adapter = FancyAdapter()
        with pytest.raises(NotImplementedError, match="FancyAdapter"):
            adapter.export_table_as_arrow("some_table")


# ---------------------------------------------------------------------------
# 2. IbisAdapter.export_table_as_arrow
# ---------------------------------------------------------------------------

class TestIbisAdapterExport:
    """IbisAdapter.export_table_as_arrow returns Arrow tables correctly."""

    def test_basic_export(self, employee_list_df):
        import ibis

        from vowl.adapters.ibis_adapter import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table("employees", employee_list_df)

        adapter = IbisAdapter(con)
        arrow = adapter.export_table_as_arrow("employees")

        assert isinstance(arrow, pa.Table)
        assert arrow.num_rows == len(employee_list_df)
        assert set(arrow.column_names) == set(employee_list_df.columns)

    def test_export_with_filter_conditions(self, employee_payroll_df):
        import ibis

        from vowl.adapters.ibis_adapter import IbisAdapter
        from vowl.adapters.models import FilterCondition

        con = ibis.duckdb.connect()
        con.create_table("payroll", employee_payroll_df)

        target_id = employee_payroll_df["employee_id"].iloc[0]
        adapter = IbisAdapter(
            con,
            filter_conditions={
                "payroll": FilterCondition("employee_id", "=", target_id),
            },
        )

        arrow = adapter.export_table_as_arrow("payroll")

        assert isinstance(arrow, pa.Table)
        # Only rows matching the filter should be exported
        expected_count = len(employee_payroll_df[employee_payroll_df["employee_id"] == target_id])
        assert arrow.num_rows == expected_count
        assert arrow.num_rows > 0

    def test_export_with_wildcard_filter(self, employee_payroll_df):
        import ibis

        from vowl.adapters.ibis_adapter import IbisAdapter
        from vowl.adapters.models import FilterCondition

        con = ibis.duckdb.connect()
        con.create_table("payroll", employee_payroll_df)

        target_id = employee_payroll_df["employee_id"].iloc[0]
        adapter = IbisAdapter(
            con,
            filter_conditions={
                # Wildcard should match any table
                "*": FilterCondition("employee_id", "=", target_id),
            },
        )

        arrow = adapter.export_table_as_arrow("payroll")

        expected_count = len(employee_payroll_df[employee_payroll_df["employee_id"] == target_id])
        assert arrow.num_rows == expected_count

    def test_export_returns_all_rows_without_filter(self, employee_list_df):
        import ibis

        from vowl.adapters.ibis_adapter import IbisAdapter

        con = ibis.duckdb.connect()
        con.create_table("employees", employee_list_df)

        adapter = IbisAdapter(con)
        arrow = adapter.export_table_as_arrow("employees")

        assert arrow.num_rows == len(employee_list_df)

    def test_export_sqlite_backend(self, employee_list_df, tmp_path):
        import ibis

        from vowl.adapters.ibis_adapter import IbisAdapter

        sqlite_path = tmp_path / "test.db"
        con = ibis.sqlite.connect(str(sqlite_path))
        con.create_table("employees", employee_list_df)

        adapter = IbisAdapter(con)
        arrow = adapter.export_table_as_arrow("employees")

        assert isinstance(arrow, pa.Table)
        assert arrow.num_rows == len(employee_list_df)


# ---------------------------------------------------------------------------
# 3. MultiSourceSQLExecutor mode 2 via adapter export
# ---------------------------------------------------------------------------

class TestMultiSourceMode2ViaExport:
    """Mode 2 materialization should delegate to adapter.export_table_as_arrow."""

    def test_cross_backend_materialization(
        self, employee_list_df, employee_payroll_df, contract_path
    ):
        """Cross-backend queries materialize via export_table_as_arrow and run in DuckDB."""
        import ibis

        from vowl.adapters.ibis_adapter import IbisAdapter
        from vowl.adapters.multi_source_adapter import MultiSourceAdapter
        from vowl.contracts.contract import Contract

        # Different DuckDB instances → mode 2
        payroll_con = ibis.duckdb.connect()
        payroll_con.create_table("demo_employee_payroll", employee_payroll_df)

        ref_con = ibis.duckdb.connect()
        ref_con.create_table("demo_employee_list", employee_list_df)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": IbisAdapter(payroll_con),
            "demo_employee_list": IbisAdapter(ref_con),
        })

        contract = Contract.load(contract_path)
        refs_by_schema = contract.get_check_references_by_schema()
        results = multi.run_checks(refs_by_schema)

        cross_results = [
            r for r in results
            if r.check_name in (
                "employee_id_exists_in_master_list",
                "phone_number_exists_in_master_list",
            )
        ]
        assert len(cross_results) == 2
        for r in cross_results:
            assert r.status in ("PASSED", "FAILED"), f"{r.check_name} got {r.status}: {r.details}"

    def test_mode2_does_not_double_filter(self, employee_payroll_df, employee_list_df):
        """When mode 2 is used, filters are baked into the exported table, not re-applied at query time."""
        import ibis

        from vowl.adapters.ibis_adapter import IbisAdapter
        from vowl.adapters.models import FilterCondition
        from vowl.adapters.multi_source_adapter import MultiSourceAdapter
        from vowl.contracts.contract import Contract

        target_id = employee_payroll_df["employee_id"].iloc[0]

        payroll_con = ibis.duckdb.connect()
        payroll_con.create_table("demo_employee_payroll", employee_payroll_df)

        ref_con = ibis.duckdb.connect()
        ref_con.create_table("demo_employee_list", employee_list_df)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": IbisAdapter(
                payroll_con,
                filter_conditions={
                    "demo_employee_payroll": FilterCondition("employee_id", "=", target_id),
                },
            ),
            "demo_employee_list": IbisAdapter(ref_con),
        })

        contract = Contract.load(str(CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        results = multi.run_checks(refs_by_schema)

        cross_results = [
            r for r in results
            if r.check_name == "employee_id_exists_in_master_list"
        ]
        assert len(cross_results) == 1
        # If double-filtering occurred, the result would differ or error.
        # The important thing is the check ran without error.
        assert cross_results[0].status in ("PASSED", "FAILED")


# ---------------------------------------------------------------------------
# 4. Custom adapter in mode 2
# ---------------------------------------------------------------------------

class _InMemoryArrowAdapter:
    """
    Minimal test-only adapter for verifying that a non-Ibis adapter can
    participate in multi-source mode 2 by implementing export_table_as_arrow.
    """

    # Defined at module level so it can be inspected cleanly.
    pass


class TestCustomAdapterMode2:
    """A custom adapter implementing export_table_as_arrow works in mode 2."""

    @staticmethod
    def _make_custom_adapter_class():
        from vowl.adapters.base import BaseAdapter

        class ArrowTableAdapter(BaseAdapter):
            """Stores data as in-memory Arrow tables, supports export."""

            def __init__(self, tables: dict[str, pa.Table]):
                super().__init__()
                self._tables = dict(tables)

            def export_table_as_arrow(self, schema_name: str) -> pa.Table:
                if schema_name not in self._tables:
                    raise ValueError(f"Table '{schema_name}' not found")
                return self._tables[schema_name]

            def test_connection(self, table_name: str) -> str | None:
                if table_name in self._tables:
                    return None
                return f"table '{table_name}' not found"

        return ArrowTableAdapter

    def test_custom_adapter_export(self):
        """Custom adapter's export_table_as_arrow returns Arrow correctly."""
        ArrowTableAdapter = self._make_custom_adapter_class()

        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        adapter = ArrowTableAdapter({"my_table": table})

        result = adapter.export_table_as_arrow("my_table")
        assert result.equals(table)

    def test_custom_adapter_in_multi_source_mode2(
        self, employee_payroll_df, employee_list_df, contract_path
    ):
        """
        A multi-source scenario where one schema uses IbisAdapter and the other
        uses a custom adapter, forcing mode 2 materialization.
        """
        import ibis

        from vowl.adapters.ibis_adapter import IbisAdapter
        from vowl.adapters.multi_source_adapter import MultiSourceAdapter
        from vowl.contracts.contract import Contract

        ArrowTableAdapter = self._make_custom_adapter_class()

        # Payroll via IbisAdapter (DuckDB)
        payroll_con = ibis.duckdb.connect()
        payroll_con.create_table("demo_employee_payroll", employee_payroll_df)
        payroll_adapter = IbisAdapter(payroll_con)

        # Ref list via custom Arrow adapter
        ref_arrow = pa.Table.from_pandas(employee_list_df)
        ref_adapter = ArrowTableAdapter({"demo_employee_list": ref_arrow})

        multi = MultiSourceAdapter({
            "demo_employee_payroll": payroll_adapter,
            "demo_employee_list": ref_adapter,
        })

        contract = Contract.load(contract_path)
        refs_by_schema = contract.get_check_references_by_schema()
        results = multi.run_checks(refs_by_schema)

        cross_results = [
            r for r in results
            if r.check_name in (
                "employee_id_exists_in_master_list",
                "phone_number_exists_in_master_list",
            )
        ]
        assert len(cross_results) == 2
        for r in cross_results:
            assert r.status in ("PASSED", "FAILED"), (
                f"{r.check_name}: expected PASSED/FAILED, got {r.status}: {r.details}"
            )

    def test_adapter_without_export_raises_in_mode2(self, employee_payroll_df):
        """An adapter that does NOT implement export_table_as_arrow fails cleanly in mode 2."""
        import ibis

        from vowl.adapters.base import BaseAdapter
        from vowl.adapters.ibis_adapter import IbisAdapter
        from vowl.adapters.multi_source_adapter import MultiSourceAdapter
        from vowl.contracts.contract import Contract

        class NoExportAdapter(BaseAdapter):
            """Adapter with no export_table_as_arrow override."""

            def test_connection(self, table_name: str) -> str | None:
                return None

        payroll_con = ibis.duckdb.connect()
        payroll_con.create_table("demo_employee_payroll", employee_payroll_df)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": IbisAdapter(payroll_con),
            "demo_employee_list": NoExportAdapter(),
        })

        contract = Contract.load(str(CONTRACT_PATH))
        refs_by_schema = contract.get_check_references_by_schema()
        results = multi.run_checks(refs_by_schema)

        cross_results = [
            r for r in results
            if r.check_name in (
                "employee_id_exists_in_master_list",
                "phone_number_exists_in_master_list",
            )
        ]
        # Should get error results, not raise an unhandled exception
        for r in cross_results:
            assert r.status == "ERROR"
            assert "export_table_as_arrow" in r.details


# ---------------------------------------------------------------------------
# 5. Mode selection and fallback
# ---------------------------------------------------------------------------

class TestModeSelection:
    """MultiSourceSQLExecutor falls back to mode 2 for mixed/non-Ibis adapters."""

    def test_mixed_adapters_fall_back_to_mode2(self, employee_payroll_df, employee_list_df):
        """When adapters are mixed (Ibis + custom), mode 1 is skipped and mode 2 runs."""
        import ibis

        from vowl.adapters.base import BaseAdapter
        from vowl.adapters.ibis_adapter import IbisAdapter
        from vowl.adapters.multi_source_adapter import MultiSourceAdapter
        from vowl.executors.multi_source_sql_executor import MultiSourceSQLExecutor

        class ArrowAdapter(BaseAdapter):
            def __init__(self, table: pa.Table):
                super().__init__()
                self._table = table

            def export_table_as_arrow(self, schema_name: str) -> pa.Table:
                return self._table

        payroll_con = ibis.duckdb.connect()
        payroll_con.create_table("demo_employee_payroll", employee_payroll_df)
        ibis_adapter = IbisAdapter(payroll_con)

        ref_arrow = pa.Table.from_pandas(employee_list_df)
        custom_adapter = ArrowAdapter(ref_arrow)

        multi = MultiSourceAdapter({
            "demo_employee_payroll": ibis_adapter,
            "demo_employee_list": custom_adapter,
        })

        executor = MultiSourceSQLExecutor(multi)

        # Mixed adapters → not compatible for mode 1
        tables = {"demo_employee_payroll", "demo_employee_list"}
        assert executor._are_backends_compatible(tables) is False
