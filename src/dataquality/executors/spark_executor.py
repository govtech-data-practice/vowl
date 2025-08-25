from typing import Dict, Any, List, Tuple, Union
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType
import time

from .base_executor import BaseExecutor, CheckResult


class SparkExecutor(BaseExecutor):
    """
    SQL-based data quality validator for Spark DataFrames using Spark SQL.
    """
    
    def __init__(self, contract_path: str):
        """
        Initialize the Spark executor with a data quality contract.
        
        Args:
            contract_path: Path to the YAML contract file containing validation rules
            
        Raises:
            ValueError: If no contract path is provided
            RuntimeError: If Spark session is not available
        """
        super().__init__(contract_path)
        try:
            self.spark = SparkSession.getActiveSession()
            if self.spark is None:
                self.spark = SparkSession.builder.getOrCreate()
        except Exception as e:
            raise RuntimeError(f"Failed to get or create Spark session: {e}")

    def validate(self, dataframe: DataFrame) -> Tuple[Dict[str, Any], DataFrame]:
        """
        Validate a Spark DataFrame against the loaded data quality contract.
        
        Args:
            dataframe: The Spark DataFrame to validate
            
        Returns:
            A tuple containing validation summary report and enhanced Spark DataFrame
        """
        overall_start_time = time.time()

        sql_checks = self.contract.get_sql_checks()
        table_name = self.contract.get_table_name()
        
        # Prepare DataFrame with data quality tracking columns and register as temp view
        enhanced_df = self._prepare_df_for_validation(dataframe)
        enhanced_df.createOrReplaceTempView(table_name)
        
        results = []
        passed_count = 0
        failed_count = 0
        
        # Execute each validation check
        for check in sql_checks:
            result = self._run_check(table_name, check)
            results.append(result)
            
            # Update row-level failure tracking for failed checks
            if result.failed_row_indices is not None:
                enhanced_df = self._mark_failed_rows(enhanced_df, result.failed_row_indices, check['name'])
                # Update the temp view with the modified DataFrame
                enhanced_df.createOrReplaceTempView(table_name)
            
            # Update check counters
            if result.status == "PASSED":
                passed_count += 1
            else:
                failed_count += 1
        
        # Calculate summary statistics
        total_rows = enhanced_df.count()
        failed_rows = enhanced_df.filter(F.col("dq_validation_status") == "FAILED").count()
        
        # Generate validation summary report
        summary = self._build_summary(results, sql_checks, passed_count, failed_count, total_rows, failed_rows)
    
        # Add overall timing to summary
        overall_execution_time_ms = (time.time() - overall_start_time) * 1000
        summary["validation_summary"]["overall_execution_time_ms"] = round(overall_execution_time_ms, 2)
        
        # Clean up temp view
        self.spark.catalog.dropTempView(table_name)
        
        return summary, enhanced_df

    def _prepare_df_for_validation(self, dataframe: DataFrame) -> DataFrame:
        """
        Add data quality tracking columns to DataFrame.
        
        Args:
            dataframe: The original Spark DataFrame to validate
            
        Returns:
            Enhanced Spark DataFrame with data quality tracking columns added
        """
        # Add unique row identifier and data quality tracking columns
        enhanced_df = dataframe.withColumn(
            "__row_id", F.monotonically_increasing_id()
        ).withColumn(
            "dq_failed_tests", F.array().cast(ArrayType(StringType()))
        ).withColumn(
            "dq_validation_status", F.lit("PASSED")
        )
        
        return enhanced_df

    def _run_check(self, table_name: str, check: Dict[str, Any]) -> CheckResult:
        """
        Execute a single validation check.
        
        Args:
            table_name: Name of the temporary view for the DataFrame
            check: Dictionary containing the validation check definition
            
        Returns:
            CheckResult object containing the outcome of the validation check
        """
        start_time = time.time()
    
        check_name = check.get("name", "Unnamed Check")
        query = check.get("query")
        expected_value = check.get("mustBe")

        if not query:
            execution_time_ms = (time.time() - start_time) * 1000
            return CheckResult(check_name, "FAILED", "No SQL query provided.", 
                                expected_value=expected_value, execution_time_ms=execution_time_ms)

        try:
            # Execute the validation query using Spark SQL
            result_df = self.spark.sql(query)
            actual_value = result_df.collect()[0][0]

            # Determine if the check passed
            test_passed = (actual_value == expected_value)
            failed_rows_df = None
            
            # If check failed, identify which rows caused the failure
            if not test_passed:
                row_query = self._to_row_query(query, table_name)
                if row_query:
                    try:
                        failed_rows_df = self.spark.sql(row_query)
                        # Cache the result since we'll use it multiple times
                        failed_rows_df.cache()
                    except Exception:
                        # If row-level query fails, we still have the aggregate result
                        pass

            # Calculate execution time
            execution_time_ms = (time.time() - start_time) * 1000

            # Prepare validation result
            status = "PASSED" if test_passed else "FAILED"
            details = f"Expected {expected_value}, got {actual_value}"
            
            return CheckResult(check_name, status, details, actual_value, expected_value, 
                                failed_rows_df, execution_time_ms=execution_time_ms)

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            return CheckResult(check_name, "FAILED", f"Spark SQL Error: {e}", 
                                expected_value=expected_value, execution_time_ms=execution_time_ms)
    
    def _mark_failed_rows(self, enhanced_df: DataFrame, failed_rows_df: DataFrame, check_name: str) -> DataFrame:
        """
        Mark specific rows as failed for a given check.
        
        Args:
            enhanced_df: The DataFrame with data quality columns
            failed_rows_df: DataFrame containing rows that failed the validation
            check_name: Name of the validation check that failed
            
        Returns:
            Updated Spark DataFrame with failure information added
        """
        if failed_rows_df is None:
            return enhanced_df
        
        # Add check name to failed rows
        failed_rows_with_check = failed_rows_df.withColumn(
            "failed_check_name", F.lit(check_name)
        ).select("__row_id", "failed_check_name")
        
        # Join with main DataFrame and update failed tests array
        updated_df = enhanced_df.join(
            failed_rows_with_check, on="__row_id", how="left"
        ).withColumn(
            "dq_failed_tests",
            F.when(
                F.col("failed_check_name").isNotNull(),
                F.array_union(F.col("dq_failed_tests"), F.array(F.col("failed_check_name")))
            ).otherwise(F.col("dq_failed_tests"))
        ).withColumn(
            "dq_validation_status",
            F.when(
                F.col("failed_check_name").isNotNull(),
                F.lit("FAILED")
            ).otherwise(F.col("dq_validation_status"))
        ).drop("failed_check_name")
        
        return updated_df

    def _get_row_identifier(self) -> str:
        """
        Get the column name used for row identification in Spark DataFrames.
        
        Returns:
            Column name for row identification in Spark DataFrames
        """
        return "__row_id"

    def _get_failed_rows_count(self, failed_row_indices: DataFrame) -> int:
        """
        Get the count of failed rows from a Spark DataFrame.
        
        Args:
            failed_row_indices: Spark DataFrame containing failed rows
            
        Returns:
            Number of failed rows
        """
        if failed_row_indices is None:
            return 0
        return failed_row_indices.count()