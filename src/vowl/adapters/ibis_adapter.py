from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from ibis.backends.sql import SQLBackend
import pyarrow as pa
import sqlglot
from sqlglot import exp

from vowl.adapters.base import BaseAdapter
from vowl.adapters.models import FilterCondition
from vowl.executors.ibis_sql_executor import IbisSQLExecutor

# Type alias for filter conditions - can be a single FilterCondition, list of them, or dict
FilterConditionType = Union[FilterCondition, List[FilterCondition], Dict[str, Any]]


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
    _IBIS_TO_SQLGLOT: Dict[str, str] = {
        "duckdb": "duckdb",
        "sqlite": "sqlite",
        "postgres": "postgres",
        "pyspark": "spark",
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
        filter_conditions: Optional[Dict[str, FilterConditionType]] = None,
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
        super().__init__(executors={
            "sql": IbisSQLExecutor,
        })
        self._con = con
        self._filter_conditions: Dict[str, FilterConditionType] = filter_conditions.copy() if filter_conditions else {}

    @property
    def filter_conditions(self) -> Dict[str, FilterConditionType]:
        """Filter conditions to apply to queries, keyed by table name."""
        return self._filter_conditions.copy()

    @property
    def has_filter_conditions(self) -> bool:
        """Whether this adapter has any active filter conditions."""
        return bool(self._filter_conditions)

    def is_compatible_with(self, other: "BaseAdapter") -> bool:
        """Two IbisAdapters are compatible when they share the same
        backend type and connection instance, and neither has filter
        conditions (filters require per-adapter materialization)."""
        if not isinstance(other, IbisAdapter):
            return False
        if self._con is not other._con:
            return False
        if self._filter_conditions or other._filter_conditions:
            return False
        return True

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
        from vowl.executors.security import validate_query_security, to_table_expression

        try:
            table = to_table_expression(schema_name)
            dialect = self.get_sql_dialect()
            filter_conditions = self.filter_conditions

            if max_rows is not None and max_rows >= 0:
                inner = (
                    sqlglot.select(exp.Literal.number(1))
                    .from_(table)
                    .limit(max_rows)
                )
                query = sqlglot.select(exp.Count(this=exp.Star())).from_(
                    inner.subquery(alias="sub")
                ).sql(dialect=dialect)
            else:
                query = sqlglot.select(exp.Count(this=exp.Star())).from_(
                    table
                ).sql(dialect=dialect)

            if filter_conditions:
                query = SQLCheckReference.apply_filters(
                    query, dialect, filter_conditions
                )

            validate_query_security(query, dialect=dialect)

            result = self._con.raw_sql(query)
            if hasattr(result, 'fetchone'):
                row = result.fetchone()
                return int(row[0]) if row else 0
            elif hasattr(result, 'collect'):
                rows = result.collect()
                return int(rows[0][0]) if rows else 0
            return 0
        except Exception:
            return 0

    def test_connection(self, table_name: str) -> Optional[str]:
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
            query = (
                sqlglot.select(exp.Literal.number(1))
                .from_(table)
                .limit(1)
                .sql(dialect=self.get_sql_dialect())
            )
            result = self._con.raw_sql(query)
            if hasattr(result, 'fetchone'):
                result.fetchone()
            elif hasattr(result, 'collect'):
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
        from vowl.executors.security import validate_query_security, to_table_expression

        table = to_table_expression(schema_name)
        dialect = self.get_sql_dialect()

        query = sqlglot.select(exp.Star()).from_(table).sql(dialect=dialect)

        filter_conditions = self.filter_conditions
        if filter_conditions:
            query = SQLCheckReference.apply_filters(query, dialect, filter_conditions)

        validate_query_security(query, dialect=dialect)

        return self._con.sql(query).to_pyarrow()

