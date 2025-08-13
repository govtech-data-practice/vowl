import pandas as pd
from typing import Dict, Any, List, Tuple, Union
import os

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

from ..contracts.contract import Contract


class ValidationResult:
    
    def __init__(self, check_name: str, status: str, details: str, 
                 actual_value: Any = None, expected_value: Any = None,
                 failed_row_indices: List[int] = None, metadata: Dict[str, Any] = None):
        self.check_name = check_name
        self.status = status
        self.details = details
        self.actual_value = actual_value
        self.expected_value = expected_value
        self.failed_row_indices = failed_row_indices or []
        self.metadata = metadata or {}


class SqlValidator:
    def __init__(self, contract_path: str = None, contract_name: str = None):

        if not DUCKDB_AVAILABLE:
            raise ImportError("DuckDB is required. Please install it with: pip install duckdb")
            
        if contract_name and contract_path:
            raise ValueError("Provide either 'contract_name' or 'contract_path', not both.")
        
        if contract_name:
            contract_dir = os.path.join(os.path.dirname(__file__), "..", "contracts")
            contract_path = os.path.join(contract_dir, f"{contract_name}.yaml")
            
        if not contract_path:
            raise ValueError("A contract must be provided via 'contract_name' or 'contract_path'.")
            
        self.contract = Contract.from_yaml(contract_path)

    def validate(self, dataframe: pd.DataFrame) -> Tuple[Dict[str, Any], pd.DataFrame]:
        
        if hasattr(dataframe, "toPandas"):
            dataframe = dataframe.toPandas()
        
        sql_checks = self.contract.get_sql_checks()
        
        enhanced_df = dataframe.copy()
        enhanced_df['dq_failed_tests'] = [[] for _ in range(len(enhanced_df))]
        enhanced_df['dq_validation_status'] = 'PASSED'
        
        results = []
        passed_count = 0
        failed_count = 0
        
        for check in sql_checks:
            result = self._execute_sql_check(enhanced_df, check)
            results.append(result)
            
            if result.failed_row_indices:
                for row_idx in result.failed_row_indices:
                    enhanced_df.at[row_idx, 'dq_failed_tests'].append(check['name'])
                    enhanced_df.at[row_idx, 'dq_validation_status'] = 'FAILED'
            
            if result.status == "PASSED":
                passed_count += 1
            else:
                failed_count += 1
        
        
        summary_report = self._build_summary_report(
            results, sql_checks, passed_count, failed_count, enhanced_df
        )
        
        return summary_report, enhanced_df

    def _execute_sql_check(self, dataframe: pd.DataFrame, check: Dict[str, Any]) -> ValidationResult:
        check_name = check.get("name", "Unnamed Check")
        query = check.get("query")
        expected_value = check.get("mustBe")
        table_name = self.contract.get_table_name()

        if not query:
            return ValidationResult(check_name, "FAILED", "No SQL query provided.", expected_value=expected_value)

        try:
            conn = duckdb.connect()
            
            df_with_index = dataframe.copy()
            df_with_index['__row_index'] = range(len(df_with_index))
            conn.register(table_name, df_with_index)
            
            result = conn.execute(query).fetchone()
            actual_value = result[0] if result else 0

            test_passed = (actual_value == expected_value)
            failed_rows = []
            
            if not test_passed:
                row_level_query = self._convert_to_row_level_query(query, table_name)
                if row_level_query:
                    try:
                        failed_rows = [row[0] for row in conn.execute(row_level_query).fetchall()]
                    except Exception:
                        pass  
            
            conn.close()

            status = "PASSED" if test_passed else "FAILED"
            details = f"Expected {expected_value}, got {actual_value}"
            
            return ValidationResult(
                check_name, status, details, actual_value, expected_value, failed_rows
            )

        except Exception as e:
            return ValidationResult(check_name, "FAILED", f"SQL Error: {e}", expected_value=expected_value)

    def _convert_to_row_level_query(self, aggregate_query: str, table_name: str) -> Union[str, None]:
        """Converts a COUNT(*) aggregate query to a query that returns failing row indices."""
        query_lower = aggregate_query.lower().strip()
        
        if "count(*)" in query_lower and "where" in query_lower:
            where_clause = aggregate_query[query_lower.find("where") + 5:].strip()
            if where_clause.endswith(';'):
                where_clause = where_clause[:-1]
            return f"SELECT __row_index FROM {table_name} WHERE {where_clause}"
        
        return None

    def _build_summary_report(self, results, sql_checks, passed, failed, df):
        return {
            "validation_summary": {
                "total_checks": len(sql_checks),
                "passed": passed,
                "failed": failed,
                "success_rate": (passed / len(sql_checks) * 100) if sql_checks else 100,
                "total_rows": len(df),
                "failed_rows": len(df[df['dq_validation_status'] == 'FAILED'])
            },
            "check_results": [
                {
                    "name": r.check_name,
                    "status": r.status,
                    "details": r.details,
                    "failed_rows_count": len(r.failed_row_indices),
                }
                for r in results
            ],
            "contract_metadata": self.contract.get_metadata()
        }


def validate_data(dataframe: pd.DataFrame, contract_path: str = None, contract_name: str = None) -> Tuple[Dict[str, Any], pd.DataFrame]:
    validator = SqlValidator(contract_path=contract_path, contract_name=contract_name)
    return validator.validate(dataframe)
