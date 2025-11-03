from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple, Union
import re

from ..contracts.contract import Contract


class CheckResult:
    """
    Represents the result of a single data quality validation check.
    """
    
    def __init__(self, check_name: str, status: str, details: str, 
                 actual_value: Any = None, expected_value: Any = None,
                 failed_row_indices: Any = None, metadata: Dict[str, Any] = None,
                 execution_time_ms: float = 0.0):

        """
        Initialize a validation result.
        
        Args:
            check_name: Name of the validation check that was executed
            status: Result status ('PASSED' or 'FAILED')
            details: Description of the validation outcome
            actual_value: The actual value returned by the validation query
            expected_value: The expected value that the check should have returned
            failed_row_indices: Row identifiers that failed this check (list for pandas, DataFrame for Spark)
            metadata: Additional metadata about the validation check
        """
        self.check_name = check_name
        self.status = status
        self.details = details
        self.actual_value = actual_value
        self.expected_value = expected_value
        self.failed_row_indices = failed_row_indices
        self.metadata = metadata or {}
        self.execution_time_ms = execution_time_ms


class BaseExecutor(ABC):
    """
    Abstract base class for data quality validation executors.
    """
    
    def __init__(self, contract_path: str):
        """
        Initialize the executor with a data quality contract.
        
        Args:
            contract_path: Path to the YAML contract file containing validation rules
            
        Raises:
            ValueError: If no contract path is provided
        """
        if not contract_path:
            raise ValueError("A contract path must be provided.")
        self.contract = Contract.from_yaml(contract_path)

    def __enter__(self):
        """
        Enters the context for the executor.
        This can be used for setup tasks like creating temp views.
        By default, it does nothing.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exits the context for the executor.
        This is used for cleanup tasks. By default, it does nothing.
        """
        pass

    @abstractmethod
    def validate(self, dataframe: Any) -> Tuple[Dict[str, Any], Any]:
        """
        Validate a DataFrame against the loaded data quality contract.
        
        Args:
            dataframe: The DataFrame to validate
            
        Returns:
            A tuple containing validation summary report and enhanced DataFrame
        """
        pass

    @abstractmethod
    def _run_check(self, dataframe: Any, check: Dict[str, Any]) -> CheckResult:
        """
        Execute a single validation check against a DataFrame.
        
        Args:
            dataframe: DataFrame prepared for validation with necessary columns
            check: Dictionary containing the validation check definition
            
        Returns:
            CheckResult object containing the outcome of the validation check
        """
        pass

    # def _to_row_query(self, aggregate_query: str, table_name: str) -> Union[str, None]:
    #     """
    #     Convert COUNT(*) query to row-level query for failure tracking.
        
    #     Args:
    #         aggregate_query: The original SQL query that returns an aggregate count
    #         table_name: Name of the table being queried
            
    #     Returns:
    #         A SQL query that returns failing rows, or None if conversion is not possible
    #     """
    #     query_lower = aggregate_query.lower().strip()
        
    #     if "count(*)" in query_lower and "where" in query_lower:
    #         where_start = query_lower.find("where") + 5
    #         where_clause = aggregate_query[where_start:].strip()
    #         if where_clause.endswith(';'):
    #             where_clause = where_clause[:-1]
                
    #         # For pandas, use __row_index; for Spark, use __row_id
    #         row_identifier = self._get_row_identifier()
    #         return f"SELECT {row_identifier} FROM {table_name} WHERE {where_clause}"
        
    #     return None

    def _to_row_query(self, aggregate_query: str, table_name: str) -> Union[str, None]:
        """
        Convert COUNT(*) query to row-level query for failure tracking.
        
        Args:
            aggregate_query: The original SQL query that returns COUNT(*)
            table_name: Name of the table/view being queried
            
        Returns:
            SQL query that returns rows with row identifiers, or None if conversion fails
        """
        query_lower = aggregate_query.lower().strip()
        
        if "count(*)" not in query_lower:
            return None
        
        try:
            # Replace COUNT(*) with *
            modified_query = self._replace_count_with_star(aggregate_query)
            
            if not modified_query:
                return None
            
            # # Get engine-specific configurations
            # row_identifier = self._get_row_identifier()
            # window_clause = self._get_window_clause() 
            
            # # Wrap in CTE and add row tracking
            # row_query = f"""
            #     WITH dqmk_failed_rows AS (
            #         {modified_query}
            #     )
            #     SELECT 
            #         ROW_NUMBER() OVER {window_clause} as {row_identifier},
            #         *
            #     FROM dqmk_failed_rows
            # """
            
            # return row_query
            row_query = self._build_row_selection_query(modified_query)
        
            return row_query
            
        except Exception as e:
            print(f"    Warning: Could not create row-level query: {e}")
            return None
    
    def _build_row_selection_query(self, modified_query: str) -> str:
        """
        Build the final row selection query with CTE wrapper.
        Uses ROW_NUMBER() to generate sequential IDs.
        Works for: Pandas (DuckDB), Redshift, PostgreSQL, BigQuery, Snowflake.
        OVERRIDE: Spark uses existing __row_id from temp view.
        
        Template structure:
            WITH dqmk_failed_rows AS (
                {modified_query}  ← Input: query with SELECT *
            )
            SELECT 
                ROW_NUMBER() OVER {window_clause} as {row_identifier}
            FROM dqmk_failed_rows
        
        Args:
            modified_query: Query with COUNT(*) replaced by SELECT *
            
        Returns:
            Complete SQL query for row identification
        """
        row_identifier = self._get_row_identifier()
        window_clause = self._get_window_clause()
        
        # Generate new row numbers using ROW_NUMBER()
        row_query = f"""
            WITH dqmk_failed_rows AS (
                {modified_query}
            )
            SELECT 
                ROW_NUMBER() OVER {window_clause} as {row_identifier}
            FROM dqmk_failed_rows
        """
        
        return row_query

    def _replace_count_with_star(self, query: str) -> Union[str, None]:
        """
        Replace SELECT COUNT(*) with SELECT *.
        
        Handles:
        - Simple: SELECT COUNT(*) FROM table
        - With spacing: SELECT  COUNT(  *  ) FROM table
        - Multiple SELECTs (CTEs): Only replaces the LAST one
        - Case insensitive
        
        Returns:
            Modified query or None if COUNT(*) not found
        """
        pattern = r'\bselect\s+count\s*\(\s*\*\s*\)'
        
        # Find all matches
        matches = list(re.finditer(pattern, query, re.IGNORECASE))
        
        if not matches:
            return None
        
        # Replace only the last occurrence
        last_match = matches[-1]
        
        modified_query = (
            query[:last_match.start()] +
            'SELECT *' +
            query[last_match.end():]
        )
        
        return modified_query
    
    def _get_window_clause(self) -> str:
        """
        Get the OVER clause for ROW_NUMBER() window function.
        
        Different SQL engines have different requirements:
        - DuckDB/PostgreSQL/Redshift: OVER () is valid (no ORDER BY needed)
        - Spark SQL/Databricks: Requires ORDER BY clause
        
        Returns:
            String for the OVER clause
            
        Examples:
            Pandas/Redshift: "()"
            Spark: "(ORDER BY (SELECT NULL))"
        """
        # default implementation: works for most SQL engines
        return "()"


    @abstractmethod
    def _get_row_identifier(self) -> str:
        """
        Get the column name used for row identification in failure tracking.
        
        Returns:
            Column name for row identification
        """
        pass

    def _build_summary(self, results: List[CheckResult], sql_checks: List[Dict[str, Any]], 
                      passed: int, failed: int, total_rows: int, failed_rows: int) -> Dict[str, Any]:
        """
        Build validation summary report.
        
        Args:
            results: List of CheckResult objects from all executed checks
            sql_checks: List of validation check definitions from the contract
            passed: Number of validation checks that passed
            failed: Number of validation checks that failed
            total_rows: Total number of rows in the dataset
            failed_rows: Number of rows that failed at least one validation
            
        Returns:
            Dictionary containing validation summary statistics and detailed results
        """

        # Calculate total execution time
        total_execution_time_ms = sum(result.execution_time_ms for result in results)

        return {
            "validation_summary": {
                "total_checks": len(sql_checks),
                "passed": passed,
                "failed": failed,
                "success_rate": (passed / len(sql_checks) * 100) if sql_checks else 100,
                "total_rows": total_rows,
                "failed_rows": failed_rows,
                "total_execution_time_ms": round(total_execution_time_ms, 2),
                "average_execution_time_ms": round(total_execution_time_ms / len(sql_checks), 2) if sql_checks else 0
            },
            "check_results": [
                {
                    "name": r.check_name,
                    "status": r.status,
                    "details": r.details,
                    "failed_rows_count": self._get_failed_rows_count(r.failed_row_indices),
                    "execution_time_ms": round(r.execution_time_ms, 2)
                }
                for r in results
            ],
            "contract_metadata": self.contract.get_metadata()
        }

    @abstractmethod
    def _get_failed_rows_count(self, failed_row_indices: Any) -> int:
        """
        Get the count of failed rows from the failed_row_indices object.
        
        Args:
            failed_row_indices: Row identifiers that failed (format depends on executor)
            
        Returns:
            Number of failed rows
        """
        pass