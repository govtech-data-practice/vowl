import pandas as pd
from typing import Any, Dict, Union
import json 
from pathlib import Path

class ValidationResult:
    """
    A container for the results of a validation run.

    This object is returned by the `validate_data` function and holds both the
    summary statistics and the enhanced DataFrame. It provides a clean interface
    for displaying, saving, and retrieving results.
    """
    def __init__(self, summary: Dict[str, Any], enhanced_df: Any):
        self.summary = summary
        self.enhanced_df = enhanced_df
        self._vs = summary['validation_summary']

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
        is_spark_df = hasattr(self.enhanced_df, 'toPandas')
        failed_df = self.get_failed_rows()
        
        failed_count = failed_df.count() if is_spark_df else len(failed_df)
            
        if failed_count > 0:
            print(f"\n=== Sample of Failed Rows ({min(max_rows, failed_count)} of {failed_count} total) ===")
            if is_spark_df:
                failed_df.show(max_rows, truncate=False)
            else:
                print(failed_df.head(max_rows).to_string(index=False))
        else:
            print("\n✅ No failed rows found!")
        return self

    def save(self, output_dir: str = ".", prefix: str = "dq_results") -> 'ValidationResult':
        """
        Saves the validation summary (JSON) and the full enhanced DataFrame (CSV).
        Returns the ValidationResult object to allow for method chaining.
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        csv_path = output_path / f"{prefix}_enhanced_data.csv"
        json_path = output_path / f"{prefix}_summary.json"
        
        if hasattr(self.enhanced_df, 'toPandas'):
            self.enhanced_df.toPandas().to_csv(csv_path, index=False)
        else:
            self.enhanced_df.to_csv(csv_path, index=False)
        
        with open(json_path, 'w') as f:
            json.dump(self.summary, f, indent=2)
        
        print(f"\n Results saved:")
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
        if hasattr(self.enhanced_df, 'toPandas'):
            return self.enhanced_df.filter(self.enhanced_df.dq_validation_status == 'FAILED')
        else:
            return self.enhanced_df[self.enhanced_df['dq_validation_status'] == 'FAILED'].copy()

    def get_passed_rows(self) -> Union[pd.DataFrame, Any]:
        """
        Returns a new DataFrame containing only the rows that passed all checks.
        """
        if hasattr(self.enhanced_df, 'toPandas'):
            return self.enhanced_df.filter(self.enhanced_df.dq_validation_status == 'PASSED')
        else:
            return self.enhanced_df[self.enhanced_df['dq_validation_status'] == 'PASSED'].copy()


def validate_data(dataframe: Any, contract_path: str) -> ValidationResult:
    """
    Automatically detect DataFrame type and validate against a data quality contract.

    Args:
        dataframe (Any): The pandas or Spark DataFrame to validate.
        contract_path (str): The file path to the YAML data contract.

    Returns:
        ValidationResult: An object containing the results and helper methods.
    """
    dataframe_type = type(dataframe).__name__
    dataframe_module = type(dataframe).__module__

    if "spark" in dataframe_module.lower() and "DataFrame" in dataframe_type:
        from .executors.spark_executor import SparkExecutor           
        executor = SparkExecutor(contract_path=contract_path)
        summary, enhanced_df = executor.validate(dataframe)
    elif isinstance(dataframe, pd.DataFrame):
        from .executors.pandas_executor import PandasExecutor
        executor = PandasExecutor(contract_path=contract_path)
        summary, enhanced_df = executor.validate(dataframe)
    else:
        raise TypeError(f"Unsupported DataFrame type: {dataframe_type}")

    return ValidationResult(summary, enhanced_df)