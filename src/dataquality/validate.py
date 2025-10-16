import pandas as pd
from typing import Any, Dict, Union, TYPE_CHECKING
import json 
from pathlib import Path

from .contracts.contract import Contract 
from .executors.base_executor import BaseExecutor

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    import pyspark.sql.functions as F

try:
    from pyspark.sql import DataFrame as SparkDataFrame
    import pyspark.sql.functions as F
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    # Create dummy types so isinstance() doesn't fail at runtime
    SparkDataFrame = type(None) 
    F = None

class ValidationResult:
    """
    A container for the results of a validation run.

    This object is returned by the `validate_data` function and holds both the
    summary statistics and the enhanced DataFrame. It provides a clean interface
    for displaying, saving, and retrieving results.
    """
    def __init__(self, summary: Dict[str, Any], enhanced_df: Any, contract: Contract):
        """
        Initialize a ValidationResult container.
        
        Args:
            summary: Validation summary dictionary containing statistics and check results
            enhanced_df: Either a DataFrame or a dict with 'source_df' and 'failed_rows_df' keys
            contract: The Contract object used for validation
        
        Attributes:
            summary: Complete validation summary dictionary
            contract: The data quality contract used
            source_df: Original DataFrame with row tracking column (__row_index or __row_id)
            passed: Boolean property - True if all checks passed
        """
        self.summary = summary
        # self.enhanced_df = enhanced_df
        self.contract = contract
        self._vs = summary['validation_summary']

        if isinstance(enhanced_df, dict) and 'source_df' in enhanced_df:
            self.source_df = enhanced_df['source_df']
            self._failed_rows_df = enhanced_df['failed_rows_df']
        else:
            self.source_df = enhanced_df
            self._failed_rows_df = None

    @property
    def passed(self) -> bool:
        """A boolean property that is True if all checks passed, False otherwise."""
        return self._vs['failed'] == 0
    
    @staticmethod
    def _is_spark_df(df) -> bool:
        """Check if DataFrame is a Spark DataFrame."""
        if SPARK_AVAILABLE and SparkDataFrame is not None:
            return isinstance(df, SparkDataFrame)
        return False
    
    def print_summary(self) -> 'ValidationResult':
        """
        Prints a high-level statistical summary of the validation run.
        Returns the ValidationResult object to allow for method chaining.
        """
        vs = self._vs
    
        # Calculate row-level quality score
        failed_rows = vs['failed_rows']
        total_rows = vs['total_rows']
        clean_rows = total_rows - failed_rows
        row_quality_pct = (clean_rows / total_rows * 100) if total_rows > 0 else 100
        
        print("\n=== Data Quality Validation Results ===")
        
        print(f"\n OVERALL DATA QUALITY")
        print(f"   Data Quality:     {row_quality_pct:.3f}%")
        print(f"   Clean Records:         {clean_rows:,} of {total_rows:,} rows")
        print(f"   Records with Issues:   {failed_rows:,} row(s)")
        
        print(f"\n VALIDATION CHECKS")
        print(f"   Total Rules Executed:  {vs['total_checks']}")
        print(f"   Rules Passed:          {vs['passed']}")
        print(f"   Rules Failed:          {vs['failed']}")
        print(f"   Check Pass Rate:       {vs['success_rate']:.1f}%")
        
        print(f"\n PERFORMANCE")
        print(f"   Total Execution:       {vs['total_execution_time_ms']:.2f} ms")
        # print(f"   Average per Check:     {vs['average_execution_time_ms']:.2f} ms")
        
        return self

    def show_failed_rows(self, max_rows: int = 5) -> 'ValidationResult':
        """
        Displays a sample of the rows that failed validation in the console.
        Returns the ValidationResult object to allow for method chaining.
        """
        # is_spark_df = hasattr(self.enhanced_df, 'toPandas')
        # failed_df = self.get_failed_rows()
        
        # failed_count = failed_df.count() if is_spark_df else len(failed_df)
            
        # if failed_count > 0:
        #     print(f"\n=== Sample of Failed Rows ({min(max_rows, failed_count)} of {failed_count} total) ===")
        #     if is_spark_df:
        #         failed_df.show(max_rows, truncate=False)
        #     else:
        #         print(failed_df.head(max_rows).to_string(index=False))
        # else:
        #     print("\n✅ No failed rows found!")
        # return self
        is_spark_df = hasattr(self.source_df, 'toPandas')
        failed_df = self.get_failed_rows()
        
        if failed_df is None:
            print("\n✅ No failed rows found!")
            return self

        failed_count = failed_df.count() if is_spark_df else len(failed_df)
            
        if failed_count > 0:
            print(f"\n=== Sample of Failed Rows ({min(max_rows, failed_count)} of {failed_count} total) ===")
            # Drop internal columns for display
            display_df = failed_df
            if is_spark_df:
                display_df = failed_df.drop("__row_id")
            else:
                # For pandas, we can drop the index column for a cleaner display
                if "__row_index" in display_df.columns:
                    display_df = display_df.drop(columns=["__row_index"])

            if is_spark_df:
                display_df.show(max_rows, truncate=False)
            else:
                print(display_df.head(max_rows).to_string())
        else:
            print("\n✅ No failed rows found!")
        return self

    def get_enhanced_df(self) -> Union[pd.DataFrame, Any]:
        """
        Returns the source DataFrame enhanced with data quality columns.
        
        Enhanced columns added:
        - dq_validation_status: 'PASSED' or 'FAILED'
        - dq_failed_tests: Comma-separated list of failed test names (empty for passed rows)
                
        Returns:
            Enhanced DataFrame with data quality status columns
        """
        is_spark_df = ValidationResult._is_spark_df(self.source_df)
        
        if is_spark_df:
            # Spark version
            if self._failed_rows_df is not None:
                failed_tests_agg = self._failed_rows_df.groupBy("__row_id").agg(
                    F.collect_list("description").alias("dq_failed_tests")
                )
                
                failed_tests_agg = failed_tests_agg.withColumn(
                    "dq_failed_tests",
                    F.concat_ws(", ", F.col("dq_failed_tests"))
                ).withColumn("dq_validation_status", F.lit("FAILED"))
                
                enhanced_df = self.source_df.join(
                    failed_tests_agg,
                    on="__row_id",
                    how="left_outer"
                )
                
                return enhanced_df.fillna("PASSED", subset=["dq_validation_status"]) \
                                  .fillna("", subset=["dq_failed_tests"]) \
                                  .drop("__row_id")
            else:
                return self.source_df.withColumn("dq_validation_status", F.lit("PASSED")) \
                                     .withColumn("dq_failed_tests", F.lit("")) \
                                     .drop("__row_id")
        else:
            # Pandas version - FIXED ✅
            enhanced_df = self.source_df.copy()
            
            if self._failed_rows_df is not None:
                # Aggregate failed tests per row
                failed_tests_agg = self._failed_rows_df.groupby('__row_index')['description'].apply(
                    lambda x: ', '.join(x)
                ).reset_index()
                failed_tests_agg.columns = ['__row_index', 'dq_failed_tests']
                
                # Merge with source data
                enhanced_df = enhanced_df.merge(
                    failed_tests_agg,
                    on='__row_index',
                    how='left'
                )
                
                # Add validation status column
                enhanced_df['dq_validation_status'] = 'PASSED'
                enhanced_df.loc[enhanced_df['dq_failed_tests'].notna(), 'dq_validation_status'] = 'FAILED'
                
                # Fill NaN values in dq_failed_tests with empty string (for passed rows)
                enhanced_df['dq_failed_tests'] = enhanced_df['dq_failed_tests'].fillna('')
                
            else:
                # All rows passed - add columns with default values
                enhanced_df['dq_validation_status'] = 'PASSED'
                enhanced_df['dq_failed_tests'] = ''
            
            # Drop internal tracking column
            if '__row_index' in enhanced_df.columns:
                enhanced_df = enhanced_df.drop(columns=['__row_index'])
            
            return enhanced_df



    def save(self, output_dir: str = ".", prefix: str = "dq_results") -> 'ValidationResult':
        """
        Saves the validation summary (JSON) and the full enhanced DataFrame (CSV).
        Returns the ValidationResult object to allow for method chaining.
        """
        # output_path = Path(output_dir)
        # output_path.mkdir(parents=True, exist_ok=True)
        
        # csv_path = output_path / f"{prefix}_enhanced_data.csv"
        # json_path = output_path / f"{prefix}_summary.json"
        
        # if hasattr(self.enhanced_df, 'toPandas'):
        #     self.enhanced_df.toPandas().to_csv(csv_path, index=False)
        # else:
        #     self.enhanced_df.to_csv(csv_path, index=False)
        
        # with open(json_path, 'w') as f:
        #     json.dump(self.summary, f, indent=2)
        
        # print(f"\n Results saved:")
        # print(f"   - Data:    {csv_path}")
        # print(f"   - Summary: {json_path}")
        # return self
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        csv_path = output_path / f"{prefix}_enhanced_data.csv"
        json_path = output_path / f"{prefix}_summary.json"
        
        # Get enhanced DataFrame
        df_to_save = self.get_enhanced_df()
        
        # Save CSV
        is_spark_df = ValidationResult._is_spark_df(df_to_save)
        if is_spark_df:
            df_to_save.toPandas().to_csv(csv_path, index=False)
        else:
            df_to_save.to_csv(csv_path, index=False)
        
        # Save summary JSON
        with open(json_path, 'w') as f:
            json.dump(self.summary, f, indent=2)
        
        print(f"\nResults saved:")
        print(f"   - Data:    {csv_path}")
        print(f"   - Summary: {json_path}")
        return self

    @staticmethod
    def save_dataframe(df: Union[pd.DataFrame, Any], 
                       filepath: str, 
                       format: str = 'csv',
                       **kwargs) -> None:
        """
        Save any DataFrame (pandas or Spark) to a file.
        
        Args:
            df: DataFrame to save (pandas or Spark)
            filepath: Path where to save the file
            format: File format ('csv', 'parquet', 'json', 'excel')
            **kwargs: Additional arguments passed to the save method
        """
        # Ensure directory exists
        output_dir = Path(filepath).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        is_spark = ValidationResult._is_spark_df(df)
        
        if format.lower() == 'csv':
            if is_spark:
                df.toPandas().to_csv(filepath, index=False, **kwargs)
            else:
                df.to_csv(filepath, index=False, **kwargs)
            print(f"✓ Saved to: {filepath}")
            
        elif format.lower() == 'parquet':
            if is_spark:
                df.write.mode("overwrite").parquet(filepath, **kwargs)
            else:
                df.to_parquet(filepath, index=False, **kwargs)
            print(f"✓ Saved to: {filepath}")
            
        elif format.lower() == 'json':
            if is_spark:
                df.toPandas().to_json(filepath, orient='records', lines=True, **kwargs)
            else:
                df.to_json(filepath, orient='records', lines=True, **kwargs)
            print(f"✓ Saved to: {filepath}")
            
        elif format.lower() == 'excel':
            if is_spark:
                df.toPandas().to_excel(filepath, index=False, **kwargs)
            else:
                df.to_excel(filepath, index=False, **kwargs)
            print(f"✓ Saved to: {filepath}")
            
        else:
            raise ValueError(f"Unsupported format: {format}. Use 'csv', 'parquet', 'json', or 'excel'")
        

    def display_full_report(self) -> 'ValidationResult':
        """
        A convenience method that prints a summary, shows failed rows, and saves results.
        """
        self.print_summary().show_failed_rows()
        return self

    def get_failed_rows(self) -> Union[pd.DataFrame, Any]:
        """
        Returns a new DataFrame containing only the rows that failed validation.
        Includes the original schema plus 'description' column showing which test failed.
        """
        if self._failed_rows_df is None:
            return None
        
        is_spark_df = hasattr(self._failed_rows_df, 'toPandas')
        
        # Drop internal tracking columns before returning
        if is_spark_df:
            return self._failed_rows_df.drop("__row_id")
        else:
            return self._failed_rows_df.drop(columns=["__row_index"])

    def get_passed_rows(self) -> Union[pd.DataFrame, Any]:
        """
        Returns a new DataFrame containing only the rows that passed all checks.
        """
        # if hasattr(self.enhanced_df, 'toPandas'):
        #     return self.enhanced_df.filter(self.enhanced_df.dq_validation_status == 'PASSED')
        # else:
        #     return self.enhanced_df[self.enhanced_df['dq_validation_status'] == 'PASSED'].copy()
        is_spark_df = hasattr(self._failed_rows_df, 'toPandas')

        if is_spark_df:
            if self._failed_rows_df is not None:
                # Anti join to get rows that passed, then drop tracking column
                return self.source_df.join(
                    self._failed_rows_df.select("__row_id").distinct(),
                    "__row_id",
                    "left_anti"
                ).drop("__row_id")
            else:
                # All rows passed, drop tracking column
                return self.source_df.drop("__row_id")
        else:  # Pandas
            if self._failed_rows_df is not None:
                passed_df = self.source_df[~self.source_df['__row_index'].isin(self._failed_rows_df['__row_index'])].copy()
                return passed_df.drop(columns=['__row_index'])
            else:
                # All rows passed, drop tracking column
                return self.source_df.drop(columns=['__row_index'])

    def compute_metrics(self) -> Union[pd.DataFrame, Any]:
        """
        Compute aggregated data quality metrics by rule.
        
        Returns a DataFrame with the following columns:
        - source_table: Name of the table being validated
        - dimension: Data quality dimension (completeness, accuracy, conformity, validity)
        - dq_rule: Name of the validation rule
        - failed_row_count: Number of unique rows that failed this rule
        - total_row_count: Total number of rows in the dataset
        - pass_rate: Percentage of rows that passed (0-100)
        - status: PASSED or FAILED
        
        This provides rule-level granularity. Users can aggregate by dimension themselves:
        
        Example (Pandas):
            metrics = result.compute_metrics()
            by_dimension = metrics.groupby('dimension').agg({
                'dq_rule': 'count',
                'failed_row_count': 'sum',
                'pass_rate': 'mean'
            })
        
        Example (Spark):
            metrics = result.compute_metrics()
            by_dimension = metrics.groupBy('dimension').agg(
                F.count('dq_rule').alias('total_rules'),
                F.sum('failed_row_count').alias('total_failures')
            )
        
        Returns:
            DataFrame containing rule-level metrics
        """
        sql_checks = self.contract.get_sql_checks()
        total_rows = self._vs['total_rows']
        source_table = self.contract.get_table_name()
        
        # Build metrics from check results
        metrics_data = []
        
        for check_result in self.summary['check_results']:
            check_name = check_result['name']
            failed_count = check_result['failed_rows_count']
            
            # Find the corresponding check definition to get dimension
            check_def = next((c for c in sql_checks if c['name'] == check_name), None)
            dimension = check_def.get('dimension', 'UNKNOWN') if check_def else 'UNKNOWN'
            
            pass_rate = ((total_rows - failed_count) / total_rows * 100) if total_rows > 0 else 100
            
            metrics_data.append({
                'source_table': source_table,
                'dimension': dimension.upper(),
                'dq_rule': check_name,
                'failed_row_count': failed_count,
                'total_row_count': total_rows,
                'pass_rate': round(pass_rate, 4),
                'status': check_result['status']
            })
        
        # Return as appropriate DataFrame type
        is_spark = ValidationResult._is_spark_df(self.source_df)
        
        if is_spark:
            return self.source_df.sparkSession.createDataFrame(metrics_data)
        else:
            return pd.DataFrame(metrics_data)



class ValidationContextManager:
    """A context manager to handle the lifecycle of a validation run."""
    def __init__(self, executor: BaseExecutor, dataframe: Any):
        self.executor = executor
        self.dataframe = dataframe

    def __enter__(self) -> 'ValidationResult':
        """Enters the context, runs validation, and returns the result."""
        self.executor.__enter__()
        summary, enhanced_df = self.executor.validate(self.dataframe)
        contract = self.executor.contract
        return ValidationResult(summary, enhanced_df, contract)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exits the context, triggering the executor's cleanup."""
        self.executor.__exit__(exc_type, exc_val, exc_tb)


def validate_data(
    dataframe: Any,
    contract_path: str
) -> 'ValidationContextManager':
    """
    Acts as a context manager for a data quality validation run.

    Args:
        dataframe: DataFrame to validate (pandas.DataFrame or pyspark.sql.DataFrame)
        contract_path: Path to the YAML contract file defining validation rules
    
    Returns:
        ValidationContextManager that yields a ValidationResult object when used
        with a 'with' statement
    
    Raises:
        TypeError: If dataframe is not a supported type (pandas or Spark DataFrame)
        FileNotFoundError: If contract_path doesn't exist
        ValueError: If contract YAML is malformed
    """
    dataframe_type = type(dataframe).__name__
    dataframe_module = type(dataframe).__module__

    if "spark" in dataframe_module.lower() and "DataFrame" in dataframe_type:
        from .executors.spark_executor import SparkExecutor           
        executor = SparkExecutor(contract_path=contract_path)
        return ValidationContextManager(executor, dataframe)
    elif isinstance(dataframe, pd.DataFrame):
        # Pandas executor doesn't need a context manager, but we can support it for consistency
        from .executors.pandas_executor import PandasExecutor
        executor = PandasExecutor(contract_path=contract_path)
        return ValidationContextManager(executor, dataframe)
    else:
        raise TypeError(f"Unsupported DataFrame type: {dataframe_type}")