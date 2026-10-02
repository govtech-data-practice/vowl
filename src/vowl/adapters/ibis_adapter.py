from __future__ import annotations

import copy
from typing import Any

import pyarrow as pa
import sqlglot
from ibis.backends.sql import SQLBackend
from sqlglot import exp

from vowl.adapters.base import BaseAdapter
from vowl.adapters.models import FilterCondition
from vowl.executors.ibis_sql_executor import IbisSQLExecutor

# Type alias for filter conditions - can be a single FilterCondition, list of them, or dict
FilterConditionType = FilterCondition | list[FilterCondition] | dict[str, Any]


class IbisAdapter(BaseAdapter):
    """
    Adapter for connecting to various databases using Ibis framework.

    Wraps an Ibis connection (SQLBackend) and provides it to executors
    for running data quality checks against database backends supported
    by Ibis (DuckDB, PostgreSQL, Snowflake, etc.).


    Supports filter conditions for scoping data quality checks to specific
    subsets of data (e.g., recent records only).

    Filter conditions support glob-style wildcard patterns:
    - "*" matches any sequence of characters
    - "?" matches any single character
    - "[seq]" matches any character in seq

    Example:
        >>> # Simple usage - query table as named in contract
        >>> adapter = IbisAdapter(ibis.duckdb.connect())

        >>> # With filter conditions - only validate recent data
        >>> adapter = IbisAdapter(
        ...     con=ibis.postgres.connect(...),
        ...     filter_conditions={
        ...         "raw_orders": {
        ...             "field": "created_at",
        ...             "operator": ">=",
        ...             "value": "2024-01-01",
        ...         }
        ...     },
        ... )

        >>> # With wildcard filter - apply to all tables matching pattern
        >>> adapter = IbisAdapter(
        ...     con=ibis.postgres.connect(...),
        ...     filter_conditions={
        ...         "emp*": FilterCondition("date_dt", ">=", "2024-01-01"),
        ...         "*": FilterCondition("tenant_id", "=", 123),  # All tables
        ...     },
        ... )
    """

    # Map Ibis backend names to sqlglot dialect names
    _IBIS_TO_SQLGLOT: dict[str, str] = {
        "duckdb": "duckdb",
        "sqlite": "sqlite",
        "postgres": "postgres",
        "pyspark": "spark",
        "databricks": "databricks",
        "snowflake": "snowflake",
        "mysql": "mysql",
        "bigquery": "bigquery",
        "trino": "trino",
        "clickhouse": "clickhouse",
        "mssql": "tsql",
        "oracle": "oracle",
        "datafusion": "datafusion",
    }

    def __init__(
        self,
        con: SQLBackend,
        filter_conditions: dict[str, FilterConditionType] | None = None,
    ) -> None:
        """
        Initialize the Ibis adapter.

        Args:
            con: An Ibis SQLBackend connection instance.
            filter_conditions: Optional mapping from table names to filter conditions.
                Each condition can be a FilterCondition object, a list of FilterCondition
                objects (combined with AND), or a dict with {field, operator, value} keys.
                Supports glob-style patterns for table name matching.
                Example: {"orders": {"field": "date_dt", "operator": ">=", "value": "2024-01-01"}}
        """
        super().__init__(
            executors={
                "sql": IbisSQLExecutor,
            }
        )
        self._con = con
        self._filter_conditions: dict[str, FilterConditionType] = filter_conditions.copy() if filter_conditions else {}

    @property
    def filter_conditions(self) -> dict[str, FilterConditionType]:
        """Filter conditions to apply to queries, keyed by table name."""
        return self._filter_conditions.copy()

    @property
    def has_filter_conditions(self) -> bool:
        """Whether this adapter has any active filter conditions."""
        return bool(self._filter_conditions)

    def with_filter_conditions(self, filter_conditions: dict[str, FilterConditionType] | None) -> IbisAdapter:
        """Return a copy of this adapter that applies other filter conditions.

        The copy shares this adapter's connection and settings. The
        multi-source executor uses it to run a join natively with each
        table's own filters.

        Args:
            filter_conditions: The filter conditions for the copy, in the
                same form the constructor accepts.

        Returns:
            A shallow copy with its filter conditions replaced.
        """
        clone = copy.copy(self)
        clone._filter_conditions = filter_conditions.copy() if filter_conditions else {}
        return clone

    def is_compatible_with(self, other: BaseAdapter) -> bool:
        """Two IbisAdapters are compatible when they share the same
        backend type and connection instance.

        Filter conditions do not affect compatibility. The multi-source
        executor gives each table the filters of the adapter that serves it
        when it runs a join on the shared connection."""
        if not isinstance(other, IbisAdapter):
            return False
        return self._con is other._con

    def get_sql_dialect(self) -> str:
        """Return the SQL dialect name for this adapter's Ibis backend."""
        backend_name = getattr(self._con, "name", "")
        return self._IBIS_TO_SQLGLOT.get(backend_name, "postgres")

    def get_connection(self) -> SQLBackend:
        """
        Retrieve the Ibis connection object.

        Returns:
            The Ibis SQLBackend connection instance.
        """
        return self._con

    def get_total_rows(self, schema_name: str, max_rows: int = -1) -> int:
        """
        Get the total row count for a table, optionally capped.

        Args:
            schema_name: The table/schema name to count rows for.
            max_rows: If >= 0, cap the count at this value.

        Returns:
            Total row count, or 0 on error.
        """
        from vowl.contracts.check_reference import SQLCheckReference
        from vowl.executors.security import to_table_expression, validate_query_security

        try:
            table = to_table_expression(schema_name)
            dialect = self.get_sql_dialect()
            filter_conditions = self.filter_conditions

            if max_rows is not None and max_rows >= 0:
                inner = sqlglot.select(exp.Literal.number(1)).from_(table).limit(max_rows)
                query = (
                    sqlglot.select(exp.Count(this=exp.Star())).from_(inner.subquery(alias="sub")).sql(dialect=dialect)
                )
            else:
                query = sqlglot.select(exp.Count(this=exp.Star())).from_(table).sql(dialect=dialect)

            if filter_conditions:
                query = SQLCheckReference.apply_filters(query, dialect, filter_conditions)

            validate_query_security(query, dialect=dialect)

            result = self._con.raw_sql(query)
            # Spark Connect DataFrames resolve any attribute to a Column via
            # __getattr__, so hasattr() is misleadingly True. Guard on
            # callable() so a Column is skipped and the real method is used.
            if callable(getattr(result, "fetchone", None)):
                row = result.fetchone()
                return int(row[0]) if row else 0
            elif callable(getattr(result, "collect", None)):
                rows = result.collect()
                return int(rows[0][0]) if rows else 0
            return 0
        except Exception:
            return 0

    def test_connection(self, table_name: str) -> str | None:
        """
        Test if the adapter can connect and access a table.

        Args:
            table_name: The table name to test access for.

        Returns:
            None on success, error message string on failure.
        """
        from vowl.executors.security import to_table_expression

        try:
            table = to_table_expression(table_name)
            # Quote the table identifier to preserve case on case-sensitive
            # backends (e.g. Oracle uppercases unquoted identifiers).
            if table.this:
                table.this.set("quoted", True)
            query = sqlglot.select(exp.Literal.number(1)).from_(table).limit(1).sql(dialect=self.get_sql_dialect())
            result = self._con.raw_sql(query)
            # Spark Connect DataFrames resolve any attribute to a Column via
            # __getattr__, so hasattr() is misleadingly True. Guard on
            # callable() so a Column is skipped and the real method is used.
            if callable(getattr(result, "fetchone", None)):
                result.fetchone()
            elif callable(getattr(result, "collect", None)):
                result.collect()
            return None
        except Exception as e:
            return str(e)

    def export_table_as_arrow(self, schema_name: str) -> pa.Table:
        """
        Export a logical table as a PyArrow table for local materialization.

        Applies any filter conditions defined on this adapter before export,
        so the returned table contains only the rows that match the filters.

        Args:
            schema_name: The logical table name to export.

        Returns:
            A PyArrow Table containing the (optionally filtered) data.
        """
        from vowl.contracts.check_reference import SQLCheckReference
        from vowl.executors.security import to_table_expression, validate_query_security

        table = to_table_expression(schema_name)
        dialect = self.get_sql_dialect()

        query = sqlglot.select(exp.Star()).from_(table).sql(dialect=dialect)

        filter_conditions = self.filter_conditions
        if filter_conditions:
            query = SQLCheckReference.apply_filters(query, dialect, filter_conditions)

        validate_query_security(query, dialect=dialect)

        if getattr(self._con, "name", "") == "pyspark":
            # Ibis's to_pyarrow goes through pandas on PySpark, which turns NaN
            # into NULL. Spark's own Arrow export keeps it, as the row-quality
            # statements do.
            from vowl.executors.ibis_sql_executor import raw_result_to_arrow

            table = raw_result_to_arrow(self._con.raw_sql(query))
            if table is not None:
                return table
        return self._con.sql(query).to_pyarrow()

    def _filtered_table_query(self, schema_name: str) -> str:
        """Return ``SELECT * FROM <table>`` with this adapter's filters applied."""
        from vowl.contracts.check_reference import SQLCheckReference
        from vowl.executors.security import to_table_expression

        dialect = self.get_sql_dialect()
        query = sqlglot.select(exp.Star()).from_(to_table_expression(schema_name)).sql(dialect=dialect)
        if self.filter_conditions:
            query = SQLCheckReference.apply_filters(query, dialect, self.filter_conditions)
        return query

    def run_arrow_query(self, sql: str) -> pa.Table:
        """Run a read-only query and return its rows as a PyArrow table.

        Used by the row-quality component to run its pushdown statements. The
        query passes the same security validation as every check query.

        Args:
            sql: A SELECT query in this adapter's dialect.

        Returns:
            The query result as a PyArrow table.

        Raises:
            RuntimeError: If the backend returned a result of unknown shape.
        """
        from vowl.executors.ibis_sql_executor import raw_result_to_arrow
        from vowl.executors.security import validate_query_security

        validate_query_security(sql, dialect=self.get_sql_dialect())
        # raw_sql, not con.sql: con.sql infers the result schema first, which
        # fails on SQLite for computed columns of an empty result.
        table = raw_result_to_arrow(self._con.raw_sql(sql))
        if table is None:
            raise RuntimeError(f"{type(self._con).__name__}.raw_sql returned a result vowl cannot read as Arrow")
        return table

    def get_column_types(self, schema_name: str) -> dict[str, Any]:
        """Return the column names and Ibis data types of a table.

        Args:
            schema_name: The logical table name.

        Returns:
            An ordered mapping of column name to Ibis data type. The type is
            None when the backend cannot infer it, for example an untyped
            SQLite column.
        """
        from vowl.executors.security import validate_query_security

        query = self._filtered_table_query(schema_name)
        validate_query_security(query, dialect=self.get_sql_dialect())
        try:
            return dict(self._con.sql(query).schema().items())
        except Exception:
            # Fall back to the column names alone, read from an empty result.
            probe = f"SELECT * FROM ({query}) AS _vowl_p WHERE 1 = 0"
            return dict.fromkeys(self.run_arrow_query(probe).column_names)
