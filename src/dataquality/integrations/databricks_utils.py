from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, explode, current_timestamp
from typing import Optional
import pyspark.sql.functions as F

from ..contracts.contract import Contract
from ..validate import ValidationResult

def save_contract_to_databricks(
    spark: SparkSession,
    contract_path: str,
    target_mapping_table: str
):
    """
    Parses a data quality contract and saves its checks to a Databricks exception mapping table.
    """
    print(f"Parsing contract from {contract_path}...")
    contract = Contract.from_yaml(contract_path)
    
    
    # Get all metadata directly from the contract object
    sql_checks = contract.get_sql_checks()
    schema_props = contract.get_schema_properties() # Get the whole schema block
    base_table_name = schema_props.get("name")
    data_domain_name = schema_props.get("data_domain_name")
    exception_table_name = schema_props.get("exception_table_name")
    contract_version = contract.get_version()

    # Define the exact column order as specified
    column_order = [
        "dps_dq_domain_name", "data_domain_name", "base_table_name", "base_table_fk_name",
        "target_table_name", "target_table_fk_name", "target_table_partition_name",
        "exception_table_name", "exception_table_fk_name", "exception_table_partition_name",
        "exception_table_desc_name", "exception_number", "exception_desc",
        "exception_handling_type", "target_to_exception_rel_type", "version",
        "record_created_timestamp"
    ]
    
    contract_rows = []
    for i, check in enumerate(sql_checks):
        row = {
            "dps_dq_domain_name": check.get("dimension", "UNKNOWN").upper(),
            "data_domain_name": data_domain_name,
            "base_table_name": base_table_name,
            "base_table_fk_name": schema_props.get("base_table_fk_name"),
            "target_table_name": schema_props.get("target_table_name", base_table_name),
            "target_table_fk_name": schema_props.get("target_table_fk_name"),
            "target_table_partition_name": schema_props.get("target_table_partition_name"),
            "exception_table_name": exception_table_name,
            "exception_table_fk_name": schema_props.get("exception_table_fk_name"),
            "exception_table_partition_name": schema_props.get("exception_table_partition_name"),
            "exception_table_desc_name": "description",
            "exception_number": str(i + 1),
            "exception_desc": check.get("name"),
            "exception_handling_type": check.get("exception_handling_type", "DISPLAYED_BUT_FLAGGED"),
            "target_to_exception_rel_type": schema_props.get("target_to_exception_rel_type"),
            "version": str(contract_version),
        }
        contract_rows.append(row)
    if not contract_rows:
        print("No SQL checks found in the contract. Nothing to save.")
        return

    # Create a DataFrame and ensure correct column order
    temp_df = spark.createDataFrame(contract_rows)
    df_to_save = temp_df.withColumn("record_created_timestamp", current_timestamp()) \
                        .select(column_order)
    
    print(f"Saving {len(contract_rows)} checks to Databricks table: {target_mapping_table}")
    df_to_save.write.mode("append").saveAsTable(target_mapping_table)
    print("Successfully saved contract checks to Databricks.")


def format_failed_rows_for_save(validation_result: ValidationResult) -> Optional[DataFrame]:
    """
    Takes a ValidationResult, extracts the failed rows, and formats them for saving.

    This function performs the explode and removes internal
    tracking columns, returning a clean DataFrame ready to be written to a table.

    Args:
        validation_result: The result object from a `validate_data` run.

    Returns:
        A Spark DataFrame containing the formatted failed rows, or None if no rows failed.
    """
    if validation_result.passed:
        print("✅ No failed rows found.")
        return None

    # Get the complete failed rows DataFrame (already contains all data + dq_failed_tests)
    failed_df = validation_result.get_failed_rows()
    
    if failed_df is None:
        print("⚠️ No failed rows found.")
        return None

    initial_failed_count = failed_df.count()
    print(f"DEBUG: Initial unique failed rows retrieved from result: {initial_failed_count}")
    
    # Explode the array of failed test names into one row per failure
    exceptions_df = failed_df.withColumn("description", F.explode(F.col("dq_failed_tests")))

    exploded_count = exceptions_df.count()
    print(f"DEBUG: Row count after exploding failed tests: {exploded_count}")

    # Add timestamp and drop internal columns
    final_df = exceptions_df.withColumn("time_generated", F.current_timestamp()) \
                            .drop("__row_id", "dq_failed_tests")
    
    final_count = final_df.count()
    print(f"DEBUG: Final row count before writing to table: {final_count}")

    return final_df