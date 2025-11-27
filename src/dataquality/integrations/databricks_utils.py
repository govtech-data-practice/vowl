from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, explode, current_timestamp
from typing import Optional
import pyspark.sql.functions as F

from ..contracts.contract import Contract
from ..validate import ValidationResult


def save_contract_to_mapping_table(
    spark: SparkSession,
    contract_path: str
) -> Optional[DataFrame]:
    """
    Parses a data quality contract and returns a DataFrame formatted for the exception mapping table.

    This function reads the contract, transforms its rules into the required schema,
    and returns a DataFrame. The caller is responsible for writing this DataFrame to a table.

    Args:
        spark: The active SparkSession.
        contract_path: The file path to the data quality contract YAML file.

    Returns:
        A Spark DataFrame containing the contract rules formatted for the mapping table,
        or None if no SQL checks are found in the contract.
    """
    print(f"Parsing contract from {contract_path}...")
    contract = Contract.from_yaml(contract_path)
    
    # Get all metadata directly from the contract object
    sql_checks = contract.get_sql_checks()
    if not sql_checks:
        print("No SQL checks found in the contract. Returning None.")
        return None
    schema_props = contract.get_schema_properties() # Get the whole schema block
    
    # Validate that the schema properties were found in the contract
    if schema_props is None:
        raise ValueError(f"Contract file '{contract_path}' is missing the required 'schema' block or is malformed.")

    base_table_name = schema_props.get("name")
    data_domain_name = schema_props.get("data_domain_name")
    exception_table_name = schema_props.get("exception_table_name")
    contract_version = contract.get_version()
    exception_prefix = schema_props.get("exception_number_prefix", "DQ")


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
            "exception_number": f"{exception_prefix}{i+1:03d}",
            "exception_desc": check.get("description"),
            "exception_handling_type": check.get("exception_handling_type", "DISPLAYED_BUT_FLAGGED"),
            "target_to_exception_rel_type": schema_props.get("target_to_exception_rel_type"),
            "version": str(contract_version),
        }
        contract_rows.append(row)

    print(f"Creating DataFrame with {len(contract_rows)} checks from contract...")
    # Create a DataFrame and ensure correct column order
    temp_df = spark.createDataFrame(contract_rows)
    mapping_df = temp_df.withColumn("record_created_timestamp", current_timestamp()) \
                        .select(column_order)
    
    return mapping_df


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
        print("No failed rows found.")
        return None

    failed_df = validation_result.get_failed_rows()
    
    if failed_df is None:
        print("No failed rows found.")
        return None

    # Create a Python dictionary to map check names to their full descriptions
    contract = validation_result.contract
    check_map = {
        check['name']: check.get('description', check['name'])
        for check in contract.get_sql_checks()
    }

    # Explode the array of failed test names
    # exceptions_df = failed_df.withColumn("description", F.explode(F.col("dq_failed_tests")))

    exceptions_df = failed_df.replace(to_replace=check_map, subset=['description'])

    # Add timestamp 
    final_df = exceptions_df.withColumn("time_generated", F.current_timestamp())
                            # .drop("__row_id", "dq_failed_tests")
    
    print(f"Formatted {final_df.count()} failed check instances for saving.")

    return final_df