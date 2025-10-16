# File: dataquality/executors/spark_executor.py

from typing import Dict, Any, Tuple
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
        super().__init__(contract_path)
        try:
            self.spark = SparkSession.getActiveSession()
            if self.spark is None:
                self.spark = SparkSession.builder.getOrCreate()
        except Exception as e:
            raise RuntimeError(f"Failed to get or create Spark session: {e}")

    def __enter__(self):
        """Sets up the context for validation by defining the temp view name."""
        self.temp_view_name = f"__dqmk_temp_view_{int(time.time())}"
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Tears down the context by dropping the temp view."""
        if hasattr(self, 'temp_view_name') and self.spark.catalog.tableExists(self.temp_view_name):
            self.spark.catalog.dropTempView(self.temp_view_name)

    def validate(self, dataframe: DataFrame) -> Tuple[Dict[str, Any], DataFrame]:
        """
        Validate a Spark DataFrame against the loaded data quality contract.
        
        Args:
            dataframe: The Spark DataFrame to validate
            
        Returns:
            A tuple containing:
            - summary (dict): Validation results summary with statistics and check details
            - result_bundle (dict): Dictionary with keys:
                - 'source_df': Original DataFrame with '__row_id' column added
                - 'failed_rows_df': Spark DataFrame of failed rows with 'description' column
        """

        overall_start_time = time.time()
        sql_checks = self.contract.get_sql_checks()
        real_table_name = self.contract.get_table_name()
        temp_view_name = self.temp_view_name

        # Add a unique ID to the original data for joining later
        source_df_with_id = dataframe.withColumn("__row_id", F.monotonically_increasing_id())
        source_df_with_id.createOrReplaceTempView(temp_view_name)

        results, passed_count, failed_count, list_of_failed_dfs = [], 0, 0, []

        # run checks and collect failures in a list
        for check in sql_checks:
            result = self._run_check(real_table_name, temp_view_name, check)
            results.append(result)
            if result.status == "FAILED" and result.failed_row_indices is not None:
                failed_df_for_check = result.failed_row_indices.withColumn("description", F.lit(check['name']))
                list_of_failed_dfs.append(failed_df_for_check)

            if result.status == "PASSED": passed_count += 1
            else: failed_count += 1

        # create one simple DataFrame of all failures and join it back
        full_failed_rows_df = None
        if list_of_failed_dfs:
            failures_df = list_of_failed_dfs[0]
            if len(list_of_failed_dfs) > 1:
                for i in range(1, len(list_of_failed_dfs)):
                    failures_df = failures_df.unionByName(list_of_failed_dfs[i])
            
            # join the source data with the failures (one row per failed test)
            full_failed_rows_df = source_df_with_id.join(
                failures_df,
                "__row_id",
                "inner"
            )

        total_rows = source_df_with_id.count()
        # Cache the final failed df
        if full_failed_rows_df is not None:
            full_failed_rows_df = full_failed_rows_df.cache()

        failed_rows_count = 0
        if full_failed_rows_df is not None:
            failed_rows_count = full_failed_rows_df.select("__row_id").distinct().count()
            
        summary = self._build_summary(results, sql_checks, passed_count, failed_count, total_rows, failed_rows_count)
        overall_execution_time_ms = (time.time() - overall_start_time) * 1000
        summary["validation_summary"]["total_execution_time_ms"] = round(overall_execution_time_ms, 2)
        
        # Return the complete failed DataFrame that includes failure descriptions
        final_result_bundle = {"source_df": source_df_with_id, "failed_rows_df": full_failed_rows_df}

        return summary, final_result_bundle

    def _prepare_df_for_validation(self, dataframe: DataFrame) -> DataFrame:
        """Adds unique row identifier and data quality tracking columns."""
        return dataframe.withColumn("__row_id", F.monotonically_increasing_id()) \
                        .withColumn("dq_failed_tests", F.array().cast(ArrayType(StringType()))) \
                        .withColumn("dq_validation_status", F.lit("PASSED"))

    def _run_check(self, real_table_name: str, temp_view_name: str, check: Dict[str, Any]) -> CheckResult:
        """Executes a single validation check and returns its result."""
        start_time = time.time()
        check_name, query, expected_value = check.get("name", "Unnamed Check"), check.get("query"), check.get("mustBe")

        if not query:
            return CheckResult(check_name, "FAILED", "No SQL query provided.", expected_value=expected_value)

        try:
            # Execute the check query and get the result
            execution_query = query.replace('{table_name}', temp_view_name)
            actual_value = self.spark.sql(execution_query).collect()[0][0]
            test_passed = (actual_value == expected_value)
            failed_rows_df = None
            status = "PASSED" if test_passed else "FAILED"

            # If the test failed, try to get the specific rows that failed
            if not test_passed:
                row_query = self._to_row_query(execution_query, temp_view_name)
                if row_query:
                    try:
                        failed_rows_df = self.spark.sql(row_query).select("__row_id")
                    except Exception as e:
                        print(f"Warning: Could not get row-level detail for check '{check_name}'. Error: {e}")

            execution_time_ms = (time.time() - start_time) * 1000
            details = f"Expected {expected_value}, got {actual_value}"
            return CheckResult(check_name, status, details, actual_value, expected_value, failed_rows_df, execution_time_ms)
        except Exception as e:
            return CheckResult(check_name, "FAILED", f"Spark SQL Error: {e}", expected_value=expected_value)

    def _get_row_identifier(self) -> str:
        """Returns the column name used for row identification in Spark."""
        return "__row_id"

    def _get_failed_rows_count(self, failed_row_indices: DataFrame) -> int:
        """Returns the count of failed rows from a Spark DataFrame."""
        if failed_row_indices is None: return 0
        return failed_row_indices.count()