import yaml
from typing import Dict, Any, List
import os


class Contract:

    def __init__(self, contract_data: Dict[str, Any]):
        self.contract_data = contract_data
        self._validate_contract()

    @classmethod
    def from_yaml(cls, filepath: str) -> "Contract":
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Contract file not found: {filepath}")

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                if not data:
                    raise ValueError(f"Contract file is empty: {filepath}")
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Invalid YAML in {filepath}: {e}")
        except Exception as e:
            raise IOError(f"Error reading {filepath}: {e}")

        return cls(data)

    def _validate_contract(self):
        if not isinstance(self.contract_data, dict):
            raise ValueError("Contract must be a dictionary.")
        if "schema" not in self.contract_data:
            raise ValueError("Contract must have a 'schema' section.")

    def get_sql_checks(self) -> List[Dict[str, Any]]:
        """Extracts all SQL-based quality checks from the contract."""
        sql_checks = []
        schema = self.contract_data.get("schema", [])
        
        for item in schema:
            table_name = item.get("name", "data_table")
            
            # Column-level checks
            for prop in item.get("properties", []):
                for check in prop.get("quality", []):
                    if check.get("type") == "sql":
                        check["table"] = table_name
                        sql_checks.append(check)
            
            # Table-level checks
            for check in item.get("quality", []):
                if check.get("type") == "sql":
                    check["table"] = table_name
                    sql_checks.append(check)

        return sql_checks

    def get_table_name(self) -> str:
        if self.contract_data.get("schema"):
            return self.contract_data["schema"][0].get("name", "data_table")
        return "data_table"

    def get_metadata(self) -> Dict[str, Any]:
        return {
            k: self.contract_data.get(k, "")
            for k in ["kind", "apiVersion", "version", "status", "id", "description"]
        }
