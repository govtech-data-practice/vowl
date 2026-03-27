"""Validation API helpers."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Type, Union

from ..adapters.base import BaseAdapter
from ..adapters.multi_source_adapter import MultiSourceAdapter
from ..config import ValidationConfig
from ..contracts.contract import Contract
from .result import ValidationResult
from .runner import ValidationRunner

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def validate_data(
    contract: Union[Contract, str, Path],
    *,
    adapter: Optional[BaseAdapter] = None,
    df: Optional[Any] = None,
    connection_str: Optional[str] = None,
    spark_session: Optional["SparkSession"] = None,
    adapters: Optional[Dict[str, BaseAdapter]] = None,
    config: Optional[ValidationConfig] = None,
    runner_cls: Type[ValidationRunner] = ValidationRunner,
    contract_cls: Type[Contract] = Contract,
    multi_adapter_cls: Type[MultiSourceAdapter] = MultiSourceAdapter,
) -> ValidationResult:
    sources = {
        'adapter': adapter,
        'df': df,
        'connection_str': connection_str,
        'spark_session': spark_session,
        'adapters': adapters,
    }
    provided = [name for name, value in sources.items() if value is not None]

    if len(provided) == 0:
        raise ValueError(
            "A data source must be provided. Use one of: adapter, df, "
            "connection_str, spark_session, or adapters."
        )

    if len(provided) > 1:
        raise ValueError(
            f"Only one data source can be provided. Got multiple: {provided}. "
            "Use 'adapters' dict for multi-source validation."
        )

    if isinstance(contract, contract_cls):
        resolved_contract = contract
    else:
        resolved_contract = contract_cls.load(str(contract))

    if adapters is not None:
        runner = runner_cls(
            contract=resolved_contract,
            adapters=adapters,
            config=config,
        )
        return runner.run()

    schema_names = resolved_contract.get_schema_names()
    if not schema_names:
        raise ValueError(
            "Contract has no schemas with names defined. Cannot infer table name."
        )

    if adapter is not None:
        if isinstance(adapter, multi_adapter_cls):
            raise TypeError(
                "Pass MultiSourceAdapter via 'adapters=' parameter, not 'adapter='."
            )
        data_source = adapter
    else:
        data_source = next(v for v in (df, connection_str, spark_session) if v is not None)

    runner = runner_cls(
        contract=resolved_contract,
        adapters={schema_name: data_source for schema_name in schema_names},
        config=config,
    )
    return runner.run()