"""Validation orchestration internals."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

from ..adapters.ibis_adapter import IbisAdapter
from ..adapters.multi_source_adapter import MultiSourceAdapter
from ..config import ValidationConfig
from ..contracts.contract import Contract
from ..executors.base import CheckResult
from ..mapper import DataSourceMapper
from .result import ValidationResult


class ValidationRunner:
    contract_cls: type[Contract] = Contract
    mapper_cls: type[DataSourceMapper] = DataSourceMapper
    adapter_cls: type[IbisAdapter] = IbisAdapter
    multi_adapter_cls: type[MultiSourceAdapter] = MultiSourceAdapter
    result_cls: type[ValidationResult] = ValidationResult
    config_cls: type[ValidationConfig] = ValidationConfig

    def __init__(
        self,
        contract: Contract | str | Path,
        adapters: MultiSourceAdapter | dict[str, Any],
        config: ValidationConfig | None = None,
    ) -> None:
        if isinstance(contract, self.contract_cls):
            self._contract: Contract | None = contract
        else:
            self._contract = self.contract_cls.load(str(contract))
        self._adapters_input = adapters
        self._config = config or self.config_cls()
        self._multi_adapter: MultiSourceAdapter | None = None
        self._schema_names: list[str] = []

    def _resolve_adapters(self) -> MultiSourceAdapter:
        if isinstance(self._adapters_input, self.multi_adapter_cls):
            self._schema_names = list(self._adapters_input._adapters.keys())
            return self._adapters_input

        if not isinstance(self._adapters_input, dict):
            raise TypeError(
                f"Expected dict or MultiSourceAdapter, got {type(self._adapters_input).__name__}"
            )

        self._schema_names = self._contract.get_schema_names()

        if not self._schema_names:
            raise ValueError("Contract has no schemas with names defined")

        mapper = self.mapper_cls()
        resolved: dict[str, IbisAdapter] = {}

        for key, adapter_input in self._adapters_input.items():
            schema_name = str(key)
            if schema_name not in self._schema_names:
                warnings.warn(
                    f"Adapter provided for '{schema_name}' but no schema with that name "
                    f"exists in the contract. Available schemas: {self._schema_names}",
                    UserWarning,
                    stacklevel=3,
                )

            if isinstance(adapter_input, self.adapter_cls):
                resolved[schema_name] = adapter_input
            else:
                resolved[schema_name] = mapper.get_adapter(adapter_input, schema_name)

        missing = set(self._schema_names) - set(resolved.keys())
        if missing:
            raise ValueError(
                f"No adapter provided for schema(s): {missing}. "
                f"Provide adapters for all schemas in the contract."
            )

        return self.multi_adapter_cls(resolved)

    def _build_summary(
        self,
        check_results: list[CheckResult],
        total_rows_by_schema: dict[str, int],
        connection_results: dict[str, dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        passed = sum(1 for cr in check_results if cr.status == 'PASSED')
        failed = sum(1 for cr in check_results if cr.status == 'FAILED')
        errors = sum(1 for cr in check_results if cr.status == 'ERROR')
        total_time = sum(cr.execution_time_ms for cr in check_results)
        failed_rows = sum(
            cr.failed_rows_count
            for cr in check_results
            if cr.failed_rows_count
            and cr.supports_row_level_output
        )

        check_results_dicts = [
            {
                'name': cr.check_name,
                'status': cr.status,
                'details': cr.details,
                'expected_value': cr.expected_value,
                'actual_value': cr.actual_value,
                'failed_rows_count': cr.failed_rows_count,
                'execution_time_ms': cr.execution_time_ms,
                **cr.metadata,
            }
            for cr in check_results
        ]

        return {
            'validation_summary': {
                'total_checks': len(check_results),
                'passed': passed,
                'failed': failed,
                'errors': errors,
                'total_rows_by_schema': total_rows_by_schema,
                'config': self._config.to_dict(),
                'failed_rows': failed_rows,
                'total_execution_time_ms': total_time,
                'success_rate': (passed / len(check_results) * 100) if check_results else 100,
                'connection_results': connection_results or {},
            },
            'check_results': check_results_dicts,
            'contract_metadata': self._contract.get_metadata() if self._contract else {},
        }

    def run(self) -> ValidationResult:
        self._multi_adapter = self._resolve_adapters()
        self._multi_adapter.max_failed_rows = self._config.max_failed_rows
        self._multi_adapter.use_try_cast = self._config.use_try_cast
        for adapter in self._multi_adapter.adapters.values():
            adapter.max_failed_rows = self._config.max_failed_rows
            adapter.use_try_cast = self._config.use_try_cast

        check_refs_by_schema = self._contract.get_check_references_by_schema()
        connection_results = self._multi_adapter.test_connections(check_refs_by_schema)
        check_results = self._multi_adapter.run_checks(check_refs_by_schema)

        total_rows_by_schema: dict[str, int] = {}
        if self._config.enable_additional_schema_statistics:
            total_rows_by_schema = self._multi_adapter.get_total_rows_by_schema(
                self._config.max_rows_for_statistics,
            )

        summary = self._build_summary(check_results, total_rows_by_schema, connection_results)
        schema_names = list(self._multi_adapter.adapters.keys())

        return self.result_cls(
            summary=summary,
            check_results=check_results,
            contract=self._contract,
            multi_adapter=self._multi_adapter,
            schema_names=schema_names,
        )
