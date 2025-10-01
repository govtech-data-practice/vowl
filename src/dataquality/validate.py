import pandas as pd
from typing import Any, Dict, Union
import json 
from pathlib import Path
import pyspark.sql.functions as F

from .contracts.contract import Contract 
from .executors.base_executor import BaseExecutor

class ValidationResult:
    """
    A container for the results of a validation run.

    This object is returned by the `validate_data` function and holds both the
    summary statistics and the enhanced DataFrame. It provides a clean interface
    for displaying, saving, and retrieving results.
    """
    def __init__(self, summary: Dict[str, Any], enhanced_df: Any, contract: Contract):
        self.summary = summary
        # self.enhanced_df = enhanced_df
        self.contract = contract
        self._vs = summary['validation_summary']

        if isinstance(enhanced_df, dict) and 'source_df' in enhanced_df:
            self.source_df = enhanced_df['source_df']
            self._failed_rows_df = enhanced_df['failed_rows_df']
        else:
            # This case should no longer happen but is kept as a fallback.
            self.source_df = enhanced_df
            self._failed_rows_df = None

    @property
    def passed(self) -> bool:
        """A boolean property that is True if all checks passed, False otherwise."""
        return self._vs['failed'] == 0

    def print_summary(self) -> 'ValidationResult':
        """
        Prints a high-level statistical summary of the validation run.
        Returns the ValidationResult object to allow for method chaining.
        """
        print("\n=== Validation Summary ===")
        print(f"Total Checks:           {self._vs['total_checks']}")
        print(f"Passed:                 {self._vs['passed']}")
        print(f"Failed:                 {self._vs['failed']}")
        print(f"Success Rate:           {self._vs['success_rate']:.1f}%")
        print(f"Total Rows:             {self._vs['total_rows']:,}")
        print(f"Rows with Failures:     {self._vs['failed_rows']:,}")
        print(f"Total Execution Time:   {self._vs['total_execution_time_ms']:.2f} ms")
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
        
        # Create a simple enhanced DataFrame for saving
        is_spark_df = hasattr(self.source_df, 'toPandas')
        
        if is_spark_df and self._failed_rows_df is not None:
            # Mark failed rows
            failed_ids = self._failed_rows_df.select("__row_id").distinct().withColumn("dq_validation_status", F.lit("FAILED"))
            df_to_save = self.source_df.join(failed_ids, on="__row_id", how="left_outer") \
                                     .fillna("PASSED", subset=["dq_validation_status"]) \
                                     .drop("__row_id")
        elif not is_spark_df and self._failed_rows_df is not None:
            # Pandas logic for marking failed rows
            df_to_save = self.source_df.copy()
            df_to_save['dq_validation_status'] = 'PASSED'
            df_to_save.loc[df_to_save['__row_index'].isin(self._failed_rows_df['__row_index']), 'dq_validation_status'] = 'FAILED'
            df_to_save = df_to_save.drop(columns=['__row_index'])
        elif is_spark_df:
            # All rows passed (Spark)
            df_to_save = self.source_df.withColumn("dq_validation_status", F.lit("PASSED")).drop("__row_id")
        else:
            # All rows passed (Pandas)
            df_to_save = self.source_df.copy()
            df_to_save['dq_validation_status'] = 'PASSED'
            if '__row_index' in df_to_save.columns:
                df_to_save = df_to_save.drop(columns=['__row_index'])

        if is_spark_df:
            df_to_save.toPandas().to_csv(csv_path, index=False)
        else:
            df_to_save.to_csv(csv_path, index=False)
        
        with open(json_path, 'w') as f:
            json.dump(self.summary, f, indent=2)
        
        print(f"\nResults saved:")
        print(f"   - Data:    {csv_path}")
        print(f"   - Summary: {json_path}")
        return self

    def display_full_report(self) -> 'ValidationResult':
        """
        A convenience method that prints a summary, shows failed rows, and saves results.
        """
        self.print_summary().show_failed_rows().save()
        return self

    def get_failed_rows(self) -> Union[pd.DataFrame, Any]:
        """
        Returns a new DataFrame containing only the rows that failed validation.
        """
        # if hasattr(self.enhanced_df, 'toPandas'):
        #     return self.enhanced_df.filter(self.enhanced_df.dq_validation_status == 'FAILED')
        # else:
        #     return self.enhanced_df[self.enhanced_df['dq_validation_status'] == 'FAILED'].copy()
        return self._failed_rows_df

    def get_passed_rows(self) -> Union[pd.DataFrame, Any]:
        """
        Returns a new DataFrame containing only the rows that passed all checks.
        """
        # if hasattr(self.enhanced_df, 'toPandas'):
        #     return self.enhanced_df.filter(self.enhanced_df.dq_validation_status == 'PASSED')
        # else:
        #     return self.enhanced_df[self.enhanced_df['dq_validation_status'] == 'PASSED'].copy()
        is_spark_df = hasattr(self.source_df, 'join')

        if is_spark_df:
            if self._failed_rows_df is not None:
                # An anti join returns rows from the left side that are NOT in the right side.
                return self.source_df.join(
                    self._failed_rows_df.select("__row_id").distinct(),
                    "__row_id",
                    "left_anti"
                )
            else: # If there are no failures, all rows passed.
                return self.source_df
        else: # Pandas logic
            if self._failed_rows_df is not None:
                return self.source_df[~self.source_df['__row_index'].isin(self._failed_rows_df['__row_index'])].copy()
            else:
                return self.source_df.copy()


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