import yaml
from typing import Dict, Any, List
import os


class Contract:
    """
    Represents a data quality contract that defines validation rules and schema expectations.
    
    A Contract encapsulates the validation logic defined in a YAML file and provides
    methods to extract SQL checks, table names, and metadata for data quality validation.
    
    Attributes:
        contract_data (Dict[str, Any]): The parsed contract data from YAML
    """
     
    def __init__(self, contract_data: Dict[str, Any]):
        self.contract_data = contract_data

    @classmethod
    def from_yaml(cls, contract_file_path: str) -> "Contract":
        """
        Create a Contract instance from a YAML file.
        
        Args:
            contract_file_path: Path to the YAML contract file
            
        Returns:
            Contract instance loaded from the YAML file
            
        Raises:
            FileNotFoundError: If the contract file doesn't exist
            yaml.YAMLError: If the YAML file is malformed
            IOError: If there's an error reading the file
            ValueError: If the contract file is empty
        """
        if not os.path.exists(contract_file_path):
            raise FileNotFoundError(f"Contract file not found: {contract_file_path}")

        try:
            with open(contract_file_path, mode="r", encoding="utf-8") as yaml_file:
                contract_data = yaml.safe_load(yaml_file)
                if not contract_data:
                    raise ValueError(f"Contract file is empty: {contract_file_path}")
        except yaml.YAMLError as yaml_parsing_error:
            raise yaml.YAMLError(f"Invalid YAML in {contract_file_path}: {yaml_parsing_error}")
        except Exception as file_reading_error:
            raise IOError(f"Error reading {contract_file_path}: {file_reading_error}")

        return cls(contract_data)

    def get_sql_checks(self) -> List[Dict[str, Any]]:
        """
        Extract all SQL-based quality checks from the contract.
        
        Returns:
            List of dictionaries containing SQL check definitions
        """
        sql_quality_checks = []
        schema_definitions = self.contract_data.get("schema", [])
        
        for schema_item in schema_definitions:
            table_name = schema_item.get("name", "data_table")
            
            # Column-level checks
            for column_property in schema_item.get("properties", []):
                for quality_check in column_property.get("quality", []):
                    if quality_check.get("type") == "sql":
                        quality_check["table"] = table_name
                        sql_quality_checks.append(quality_check)
            
            # Table-level checks
            for table_quality_check in schema_item.get("quality", []):
                if table_quality_check.get("type") == "sql":
                    table_quality_check["table"] = table_name
                    sql_quality_checks.append(table_quality_check)

        return sql_quality_checks

    def get_table_name(self) -> str:
        """
        Get the primary table name from the contract schema.
        
        Returns:
            Table name from the first schema item, or 'data_table' as default
        """
        if self.contract_data.get("schema"):
            return self.contract_data["schema"][0].get("name", "data_table")
        return "data_table"

    def get_metadata(self) -> Dict[str, Any]:
        """
        Extract metadata information from the contract.
        
        Returns:
            Dictionary containing contract metadata (kind, version, etc.)
        """
        metadata_field_names = ["kind", "apiVersion", "version", "status", "id", "description"]
        return {
            field_name: self.contract_data.get(field_name, "")
            for field_name in metadata_field_names
        }
