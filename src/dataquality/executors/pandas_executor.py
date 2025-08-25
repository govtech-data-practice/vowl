import pandas as pd
from typing import Dict, Any, List, Tuple, Union
import duckdb
import time

from .base_executor import BaseExecutor, CheckResult


class PandasExecutor(BaseExecutor):
    """
    SQL-based data quality validator for pandas DataFrames using DuckDB.
    """
    
    def validate(self, dataframe: pd.DataFrame) -> Tuple[Dict[str, Any], pd.DataFrame]:
        """
        Validate a pandas DataFrame against the loaded data quality contract.
        
        Args:
            dataframe: The pandas DataFrame to validate
            
        Returns:
            A tuple containing validation summary report and enhanced DataFrame
        """
        overall_start_time = time.time()
      
        sql_checks = self.contract.get_sql_checks()
        enhanced_df = self._prepare_df_for_validation(dataframe)
        
        results = []
        passed_count = 0
        failed_count = 0
        
        for check in sql_checks:
            result = self._run_check(enhanced_df, check)
            results.append(result)
            
            if result.failed_row_indices:
                self._mark_failed_rows(enhanced_df, result.failed_row_indices, check['name'])
            
            if result.status == "PASSED":
                passed_count += 1
            else:
                failed_count += 1
        
        total_rows = len(enhanced_df)
        failed_rows = len(enhanced_df[enhanced_df['dq_validation_status'] == 'FAILED'])
        
        summary = self._build_summary(results, sql_checks, passed_count, failed_count, total_rows, failed_rows)
        
        overall_execution_time_ms = (time.time() - overall_start_time) * 1000
        summary["validation_summary"]["overall_execution_time_ms"] = round(overall_execution_time_ms, 2)
        
        return summary, enhanced_df

    def _prepare_df_for_validation(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Add data quality tracking columns to DataFrame.
        
        Args:
            dataframe: The original pandas DataFrame to validate
            
        Returns:
            Enhanced DataFrame with data quality tracking columns added
        """
        enhanced_df = dataframe.copy()
        enhanced_df['dq_failed_tests'] = [[] for _ in range(len(enhanced_df))]
        enhanced_df['dq_validation_status'] = 'PASSED'
        return enhanced_df

    def _run_check(self, dataframe: pd.DataFrame, check: Dict[str, Any]) -> CheckResult:
        """
        Execute a single validation check.
        
        Args:
            dataframe: DataFrame that includes data quality tracking columns
            check: Dictionary containing the validation check definition
            
        Returns:
            CheckResult object containing the outcome of the validation check
        """
        start_time = time.time()

        check_name = check.get("name", "Unnamed Check")
        query = check.get("query")
        expected_value = check.get("mustBe")
        table_name = self.contract.get_table_name()

        if not query:
            execution_time_ms = (time.time() - start_time) * 1000
            return CheckResult(check_name, "FAILED", "No SQL query provided.", 
                                  expected_value=expected_value, execution_time_ms=execution_time_ms)

        try:
            conn = duckdb.connect()
            df_with_index = self._add_row_indices(dataframe)
            conn.register(table_name, df_with_index)
            
            result = conn.execute(query).fetchone()
            actual_value = result[0] if result else 0
            test_passed = (actual_value == expected_value)
            failed_rows = []
            
            if not test_passed:
                failed_rows = self._find_failed_rows(conn, query, table_name)
            
            conn.close()
            
            # Calculate execution time
            execution_time_ms = (time.time() - start_time) * 1000
            
            status = "PASSED" if test_passed else "FAILED"
            details = f"Expected {expected_value}, got {actual_value}"
            
            return CheckResult(check_name, status, details, actual_value, expected_value, 
                                  failed_rows, execution_time_ms=execution_time_ms)

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            return CheckResult(check_name, "FAILED", f"SQL Error: {e}", 
                                  expected_value=expected_value, execution_time_ms=execution_time_ms)

    def _add_row_indices(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Add row index column for failure tracking.
        
        Args:
            dataframe: The pandas DataFrame to add indices to
            
        Returns:
            DataFrame with row index column added
        """
        df_with_index = dataframe.copy()
        df_with_index['__row_index'] = range(len(df_with_index))
        return df_with_index

    def _find_failed_rows(self, conn, query: str, table_name: str) -> List[int]:
        """
        Find specific rows that caused validation failure.
        
        Args:
            conn: DuckDB connection object
            query: The original SQL validation query
            table_name: Name of the table being queried
            
        Returns:
            List of row indices that failed the validation
        """
        row_query = self._to_row_query(query, table_name)
        if row_query:
            try:
                return [row[0] for row in conn.execute(row_query).fetchall()]
            except Exception:
                pass
        return []

    def _mark_failed_rows(self, df: pd.DataFrame, failed_indices: List[int], check_name: str):
        """
        Mark specific rows as failed for a given check.
        
        Args:
            df: The DataFrame with data quality columns
            failed_indices: List of row indices that failed the validation check
            check_name: Name of the validation check that failed
        """
        for row_idx in failed_indices:
            df.at[row_idx, 'dq_failed_tests'].append(check_name)
            df.at[row_idx, 'dq_validation_status'] = 'FAILED'

    def _get_row_identifier(self) -> str:
        """
        Get the column name used for row identification in pandas DataFrames.
        
        Returns:
            Column name for row identification in pandas DataFrames
        """
        return "__row_index"

    def _get_failed_rows_count(self, failed_row_indices: List[int]) -> int:
        """
        Get the count of failed rows from a list of row indices.
        
        Args:
            failed_row_indices: List of row indices that failed
            
        Returns:
            Number of failed rows
        """
        return len(failed_row_indices) if failed_row_indices else 0