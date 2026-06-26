"""Validation result container and reporting helpers."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

import narwhals as nw
import pyarrow as pa
import pyarrow.csv as _pa_csv
import pyarrow.parquet as _pa_pq

from ..config import CheckInfoPreset, OutputMode, ValidationConfig
from ..contracts.contract import Contract
from ..contracts.models.ODCS_types import DataContract
from ..executors.base import CheckResult
from .result_models import (
    CheckStatusSummary,
    MultiTableSummary,
    SchemaValidationBreakdown,
    SingleTableSummary,
)
from .result_rendering import (
    STATUS_ORDER,
    build_check_results_section,
    build_summary_section,
    format_ascii_table,
    get_field_label,
    get_tables_in_query,
    is_cross_table_check,
)
from .result_row_quality import (
    build_row_quality_summary,
    get_eligible_schema_names,
    iter_unique_failed_row_keys,
    select_relevant_failed_row_columns,
)

if TYPE_CHECKING:
    from ..adapters.multi_source_adapter import MultiSourceAdapter

logger = logging.getLogger(__name__)

#: Metadata columns that tag failed-rows output but are not part of the
#: underlying table's identity.  Stripped before matching annotated rows.
#: Legacy ``check_id``/``check_ids`` are kept (still used by the untouched
#: consolidated/residue path); ``check_info``/``check_info_item`` are the
#: annotated path's enriched columns.
_METADATA_COLUMNS = (
    "check_id",
    "check_ids",
    "check_info",
    "check_info_item",
    "tables_in_query",
)


class ValidationResult:
    """Container for validation results and reporting helpers."""

    _ROW_QUALITY_EXCLUDED_COLUMNS = (
        "check_id",
        "check_ids",
        "check_info",
        "check_info_item",
        "tables_in_query",
    )

    def __init__(
        self,
        summary: dict[str, Any],
        check_results: list[CheckResult],
        contract: Contract,
        multi_adapter: MultiSourceAdapter,
        schema_names: Sequence[str],
        config: ValidationConfig | None = None,
    ):
        self.summary = summary
        self.check_results = check_results
        self.contract = contract
        self._multi_adapter: MultiSourceAdapter = multi_adapter
        self._schema_names = list(schema_names)
        self._config = config or ValidationConfig()
        self._vs = summary["validation_summary"]
        self._row_quality_summary_by_schema: dict[str, dict[str, Any]] | None = None
        self._schema_validation_breakdown: dict[str, SchemaValidationBreakdown] | None = None
        self._schema_column_names: dict[str, list[str]] = {}
        self._full_table_cache: dict[str, nw.DataFrame | None] = {}

    def __repr__(self) -> str:
        total = self._vs["total_checks"]
        passed = self._vs["passed"]
        failed = self._vs["failed"]
        return f"ValidationResult(passed={self.passed}, checks={total}, passed_checks={passed}, failed_checks={failed})"

    @staticmethod
    def _supports_row_level_output(check_result: CheckResult) -> bool:
        """True when the result can participate in row-level summaries/output."""
        return check_result.supports_row_level_output

    def _get_row_quality_eligible_checks(self) -> list[CheckResult]:
        return [
            check_result
            for check_result in self.check_results
            if check_result.status != "ERROR"
            and not is_cross_table_check(check_result)
            and self._supports_row_level_output(check_result)
        ]

    def _get_checks_for_schema(self, schema_name: str) -> list[CheckResult]:
        return [
            check_result
            for check_result in self.check_results
            if check_result.metadata.get("schema_name") == schema_name
        ]

    def _get_failed_checks_summary_by_schema(self) -> dict[str, dict[str, list[CheckResult]]]:
        summary: dict[str, dict[str, list[CheckResult]]] = {}
        for schema_name in self._schema_names:
            single_checks, multi_checks = self._split_checks_by_scope(
                check_result
                for check_result in self._get_checks_for_schema(schema_name)
                if check_result.status == "FAILED"
            )
            if single_checks or multi_checks:
                summary[schema_name] = {
                    "single_checks": single_checks,
                    "multi_checks": multi_checks,
                }
        return summary

    @staticmethod
    def _split_checks_by_scope(
        check_results: Iterable[CheckResult],
    ) -> tuple[list[CheckResult], list[CheckResult]]:
        single_checks: list[CheckResult] = []
        multi_checks: list[CheckResult] = []
        for check_result in check_results:
            if is_cross_table_check(check_result):
                multi_checks.append(check_result)
            else:
                single_checks.append(check_result)
        return single_checks, multi_checks

    def _get_sorted_check_results(self) -> list[CheckResult]:
        return sorted(
            self.check_results,
            key=lambda result: (
                STATUS_ORDER.index(result.status) if result.status in STATUS_ORDER else len(STATUS_ORDER),
                self._schema_names.index(result.metadata.get("schema_name"))
                if result.metadata.get("schema_name") in self._schema_names
                else len(self._schema_names),
                "Multi" if is_cross_table_check(result) else "Single",
                result.check_name,
            ),
        )

    def _get_schema_column_names(self, schema_name: str) -> list[str]:
        if schema_name in self._schema_column_names:
            return self._schema_column_names[schema_name]

        contract_data = getattr(self.contract, "contract_data", None)
        schema_entries = contract_data.get("schema", []) if isinstance(contract_data, dict) else []
        column_names = []

        for schema_entry in schema_entries:
            if not isinstance(schema_entry, dict) or schema_entry.get("name") != schema_name:
                continue

            properties = schema_entry.get("properties") or []
            column_names = [
                property_data["name"]
                for property_data in properties
                if isinstance(property_data, dict) and property_data.get("name")
            ]
            break

        self._schema_column_names[schema_name] = column_names
        return column_names

    def _get_row_quality_summary_by_schema(self) -> dict[str, dict[str, Any]]:
        if self._row_quality_summary_by_schema is not None:
            return self._row_quality_summary_by_schema

        total_rows_by_schema = self._vs.get("total_rows_by_schema", {})
        if not total_rows_by_schema:
            return {}

        eligible_checks = self._get_row_quality_eligible_checks()
        eligible_schemas = get_eligible_schema_names(eligible_checks, total_rows_by_schema)
        if not eligible_schemas:
            return {}

        schema_columns = {schema_name: self._get_schema_column_names(schema_name) for schema_name in eligible_schemas}
        unique_rows_by_schema = self._collect_unique_failed_rows_by_schema(
            eligible_checks,
            eligible_schemas,
            schema_columns,
        )
        self._row_quality_summary_by_schema = {
            schema_name: asdict(
                build_row_quality_summary(
                    total_rows,
                    len(unique_rows_by_schema.get(schema_name, set())),
                )
            )
            for schema_name, total_rows in total_rows_by_schema.items()
            if schema_name in eligible_schemas
        }
        return self._row_quality_summary_by_schema

    def _collect_unique_failed_rows_by_schema(
        self,
        eligible_checks: Iterable[CheckResult],
        eligible_schemas: set[str],
        schema_columns: dict[str, list[str]],
    ) -> dict[str, set[tuple[Any, ...]]]:
        unique_rows_by_schema: dict[str, set[tuple[Any, ...]]] = {
            schema_name: set() for schema_name in eligible_schemas
        }
        for check_result in eligible_checks:
            if check_result.status != "FAILED":
                continue

            schema_name = check_result.metadata.get("schema_name")
            if not isinstance(schema_name, str) or schema_name not in unique_rows_by_schema:
                continue

            failed_rows = check_result.failed_rows
            if len(failed_rows) == 0:
                continue

            relevant_columns = select_relevant_failed_row_columns(
                schema_name,
                failed_rows,
                schema_columns,
                self._ROW_QUALITY_EXCLUDED_COLUMNS,
            )
            if not relevant_columns:
                continue

            unique_rows_by_schema[schema_name].update(iter_unique_failed_row_keys(failed_rows, relevant_columns))
        return unique_rows_by_schema

    def _get_schema_validation_breakdown(self) -> dict[str, SchemaValidationBreakdown]:
        if self._schema_validation_breakdown is not None:
            return self._schema_validation_breakdown

        total_rows_by_schema = self._vs.get("total_rows_by_schema", {})
        row_quality_summary_by_schema = self._get_row_quality_summary_by_schema()
        breakdown: dict[str, SchemaValidationBreakdown] = {}

        for schema_name in self._schema_names:
            schema_checks = self._get_checks_for_schema(schema_name)
            breakdown[schema_name] = self._build_schema_breakdown(
                schema_name,
                schema_checks,
                total_rows_by_schema,
                row_quality_summary_by_schema,
            )

        self._schema_validation_breakdown = breakdown
        return self._schema_validation_breakdown

    @staticmethod
    def _summarize_check_statuses(check_results: Sequence[CheckResult]) -> CheckStatusSummary:
        return CheckStatusSummary(
            passed_checks=sum(check_result.status == "PASSED" for check_result in check_results),
            error_checks=sum(check_result.status == "ERROR" for check_result in check_results),
            total_checks=len(check_results),
        )

    def _build_schema_breakdown(
        self,
        schema_name: str,
        schema_checks: Sequence[CheckResult],
        total_rows_by_schema: dict[str, int],
        row_quality_summary_by_schema: dict[str, dict[str, Any]],
    ) -> SchemaValidationBreakdown:
        single_table_checks, multi_table_checks = self._split_checks_by_scope(schema_checks)
        single_table_row_summary = row_quality_summary_by_schema.get(schema_name)
        total_rows = total_rows_by_schema.get(schema_name)
        failed_unique_rows = single_table_row_summary["records_with_issues"] if single_table_row_summary else 0
        passed_unique_rows = max((total_rows or 0) - failed_unique_rows, 0)
        passed_row_percentage = passed_unique_rows / total_rows * 100 if total_rows else None

        overall = self._summarize_check_statuses(schema_checks)
        single_status = self._summarize_check_statuses(single_table_checks)
        multi_status = self._summarize_check_statuses(multi_table_checks)
        return SchemaValidationBreakdown(
            overall=overall,
            single_table=SingleTableSummary(
                **asdict(single_status),
                failed_unique_rows=failed_unique_rows,
                passed_unique_rows=passed_unique_rows,
                total_rows=total_rows,
                passed_row_percentage=passed_row_percentage,
            ),
            multi_table=MultiTableSummary(
                **asdict(multi_status),
                failed_non_unique_rows=sum(
                    (check_result.failed_rows_count or 0)
                    for check_result in multi_table_checks
                    if check_result.status == "FAILED" and self._supports_row_level_output(check_result)
                ),
            ),
        )

    @property
    def passed(self) -> bool:
        return self._vs["failed"] == 0

    @property
    def api_version(self) -> str:
        return self.contract.get_api_version()

    @property
    def contract_id(self) -> str:
        return self.contract.get_metadata().get("id") or "unknown"

    @property
    def contract_data(self) -> DataContract:
        return self.contract.contract_data

    def print_summary(self) -> ValidationResult:
        summary_section = build_summary_section(
            total_checks=self._vs["total_checks"],
            passed_checks=self._vs["passed"],
            error_checks=self._vs.get("errors", 0),
            check_pass_rate=self._vs.get("success_rate", 100.0),
            schema_names=self._schema_names,
            schema_validation_breakdown=self._get_schema_validation_breakdown(),
        )
        check_results_section = build_check_results_section(
            self._get_sorted_check_results(),
        )
        report = f"""
=== Data Quality Validation Results ===
   Contract Version:      {self.api_version}
   Contract ID:           {self.contract_id}
   Schemas:               {", ".join(self._schema_names)}
{summary_section}
{check_results_section}Total Execution:       {self._vs["total_execution_time_ms"]:.2f} ms"""

        print("\n" + report.rstrip())

        return self

    def show_failed_checks(self) -> ValidationResult:
        failed_checks = [cr for cr in self.check_results if cr.status == "FAILED"]

        if not failed_checks:
            print("\n All checks passed!")
            return self

        print(f"\n=== Failed Checks ({len(failed_checks)} total) ===")
        for check in failed_checks:
            print(f"\n  {check.check_name}")
            print(f"    Status: {check.status}")
            print(f"    Operator: {check.metadata.get('operator', '')}")
            print(f"    Expected: {check.expected_value}")
            print(f"    Actual: {check.actual_value}")
            if check.details:
                print(f"    Details: {check.details}")

        return self

    def show_failed_rows(self, max_rows: int = 5) -> ValidationResult:
        failed_checks_summary = self._get_failed_checks_summary_by_schema()
        if not failed_checks_summary:
            print("\n No failed rows found!")
            return self

        mode_label = "all" if max_rows == -1 else f"up to {max_rows} row(s) per failed check"
        print(f"\n=== Failed Checks and Rows ({mode_label}) ===")

        for schema_name in self._schema_names:
            if schema_name not in failed_checks_summary:
                continue

            schema_summary = failed_checks_summary[schema_name]
            print(f"\n  {schema_name}")

            for section_label, check_key in (("Single checks", "single_checks"), ("Multi checks", "multi_checks")):
                check_results = schema_summary[check_key]
                if not check_results:
                    continue

                print(f"    {section_label}")
                for check_result in check_results:
                    self._print_failed_check_rows(
                        check_result,
                        max_rows=max_rows,
                    )

        return self

    @staticmethod
    def _print_failed_check_rows(
        check_result: CheckResult,
        *,
        max_rows: int,
    ) -> None:
        target_label = get_field_label(check_result)
        rule = check_result.metadata.get("rendered_implementation")

        operator = check_result.metadata.get("operator", "")

        print(f"\n      [{check_result.check_name}]")
        print(f"        Operator:   {operator}")
        print(f"        Expected:   {check_result.expected_value}")
        print(f"        Actual:     {check_result.actual_value}")

        if target_label:
            print(f"        Target:   {target_label}")
        if check_result.details:
            print(f"        Details:  {check_result.details}")
        if rule:
            print(f"        Rule:     {rule}")

        df = check_result.failed_rows
        if len(df) == 0:
            print("        No failed rows returned.")
            return

        sample_size = len(df) if max_rows == -1 else min(len(df), max_rows)
        sample = df.head(sample_size).to_arrow()
        print(f"        Rows shown: {sample_size} of {len(df)}")
        print(format_ascii_table(sample))

    @staticmethod
    def _append_output_metadata(df: nw.DataFrame, check_name: str, tables_str: str) -> nw.DataFrame:
        if len(df) > 0:
            return df.with_columns(
                nw.lit(check_name).alias("check_id"),
                nw.lit(tables_str).alias("tables_in_query"),
            )

        arrow_df = df.to_arrow()
        arrow_df = arrow_df.append_column("check_id", pa.array([], type=pa.utf8())).append_column(
            "tables_in_query", pa.array([], type=pa.utf8())
        )
        return nw.from_native(arrow_df, eager_only=True)

    def _output_key(self, cr: CheckResult) -> str:
        schema = cr.metadata.get("schema_name", "")
        return f"{schema}::{cr.check_name}" if schema else cr.check_name

    def get_output_dfs(self, checks: Sequence[str] | None = None) -> dict[str, nw.DataFrame]:
        result: dict[str, nw.DataFrame] = {}
        checks_set = set(checks) if checks else None

        for cr in self.check_results:
            if cr.status == "ERROR":
                continue
            if checks_set and cr.check_name not in checks_set:
                continue

            tables = get_tables_in_query(cr)
            tables_str = ", ".join(sorted(tables)) if tables else ""
            result[self._output_key(cr)] = self._append_output_metadata(cr.failed_rows, cr.check_name, tables_str)

        return dict(sorted(result.items()))

    def get_consolidated_output_dfs(self, checks: Sequence[str] | None = None) -> dict[str, nw.DataFrame]:
        """Group failed rows by (tables_in_query, column_set), deduplicating
        identical rows and combining their check IDs.

        Unlike ``get_annotated_output``, this does **not** filter out cross-table
        checks. They appear under their composite table key (e.g.
        ``"table_a, table_b"``).  See docs/known-issues.md for details.
        """
        per_check = self.get_output_dfs(checks=checks)
        per_check = {k: v for k, v in per_check.items() if len(v) > 0}
        if not per_check:
            return {}

        groups: dict[tuple, list[nw.DataFrame]] = {}
        for df in per_check.values():
            tables_key = df["tables_in_query"][0] if len(df) > 0 else ""
            cols_key = frozenset(c for c in df.columns if c not in ("check_id", "tables_in_query"))
            group_key = (tables_key, cols_key)
            groups.setdefault(group_key, []).append(df)

        raw_results: dict[str, list[nw.DataFrame]] = {}
        for (tables_key, _cols_key), dfs in groups.items():
            arrow_tables = [df.to_arrow() for df in dfs]
            combined = pa.concat_tables(arrow_tables, promote_options="default")
            grouped = self._consolidate_grouped_output(nw.from_native(combined, eager_only=True))
            key = tables_key or "unknown"
            raw_results.setdefault(key, []).append(grouped)

        result: dict[str, nw.DataFrame] = {}
        for key, dfs_list in sorted(raw_results.items()):
            if len(dfs_list) == 1:
                result[key] = dfs_list[0]
            else:
                for idx, df in enumerate(dfs_list, start=1):
                    result[f"{key}__{idx}"] = df

        return result

    @staticmethod
    def _consolidate_grouped_output(combined: nw.DataFrame) -> nw.DataFrame:
        data_cols = [column for column in combined.columns if column not in ("check_id", "tables_in_query")]

        if not data_cols:
            return nw.from_native(
                pa.table(
                    {
                        "check_ids": [", ".join(sorted(set(combined["check_id"].to_list())))],
                        "tables_in_query": [combined["tables_in_query"][0]],
                    }
                ),
                eager_only=True,
            )

        arrow_table = combined.to_arrow()
        check_id_col = arrow_table.column("check_id")
        tables_col = arrow_table.column("tables_in_query")
        data_arrow_cols = [arrow_table.column(column) for column in data_cols]

        row_groups: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row_index in range(arrow_table.num_rows):
            row_key = tuple(column[row_index].as_py() for column in data_arrow_cols)
            group = row_groups.setdefault(
                row_key,
                {
                    "check_ids": set(),
                    "tables_in_query": tables_col[row_index].as_py(),
                },
            )
            group["check_ids"].add(check_id_col[row_index].as_py())

        result_data: dict[str, list] = {column: [] for column in data_cols}
        result_data["check_ids"] = []
        result_data["tables_in_query"] = []

        for row_key, group in row_groups.items():
            for column, value in zip(data_cols, row_key, strict=False):
                result_data[column].append(value)
            result_data["check_ids"].append(", ".join(sorted(group["check_ids"])))
            result_data["tables_in_query"].append(group["tables_in_query"])

        return nw.from_native(pa.table(result_data), eager_only=True)

    # ------------------------------------------------------------------
    # Annotated output (full in-scope table with failed rows marked).
    # ------------------------------------------------------------------

    def _fetch_full_table(self, schema_name: str) -> nw.DataFrame | None:
        """Return the full in-scope table for a schema, or ``None``.

        The table is exported via the schema's adapter, which applies the
        adapter's filter conditions, so the result reflects the same in-scope
        rows the checks ran against, not the raw source.

        ``None`` is returned (and cached) when there is no adapter for the
        schema or the adapter cannot export it.  Callers treat ``None`` as
        "skip the annotated table for this schema; its residues survive".
        """
        if schema_name not in self._full_table_cache:
            result: nw.DataFrame | None = None
            adapter = self._multi_adapter.get_adapter(schema_name)
            if adapter is None:
                logger.warning("No adapter for schema %r; skipping annotated output.", schema_name)
            else:
                try:
                    arrow_table = adapter.export_table_as_arrow(schema_name)
                    result = nw.from_native(arrow_table, eager_only=True)
                except Exception as exc:  # NotImplementedError + any backend export error
                    logger.warning("Annotated export failed for %r: %s", schema_name, exc)
            self._full_table_cache[schema_name] = result
        return self._full_table_cache[schema_name]

    @staticmethod
    def _is_mergeable_for_full_table(cr: CheckResult, full_table_columns: set[str]) -> bool:
        """True when *cr*'s failed rows can be annotated onto the full table.

        Predicates are ordered cheapest-first so the lazy ``failed_rows`` fetch
        (criterion 4) is only triggered for checks that pass 1-3.
        """
        if cr.status == "ERROR":
            return False
        if is_cross_table_check(cr):
            return False
        if not cr.supports_row_level_output:
            return False
        # Column match -- the only fetch-triggering predicate, evaluated last.
        failed_cols = set(cr.failed_rows.columns) - set(_METADATA_COLUMNS)
        return failed_cols == full_table_columns

    def _failed_rows_truncated(self, cr: CheckResult) -> bool:
        """True when *cr*'s fetched failed rows were capped by ``max_failed_rows``.

        ``failed_rows_count`` is the true count from the aggregate SQL and is
        not subject to the ``LIMIT``; only the fetched frame is.  So a true
        count exceeding the fetched length means the sample was truncated.
        """
        cap = self._config.max_failed_rows
        count = cr.failed_rows_count
        return cap >= 0 and count is not None and count > len(cr.failed_rows)

    @staticmethod
    def _check_info_item_json(cr: CheckResult, preset: CheckInfoPreset) -> str:
        """JSON-encode one failing check's per-row item for the given preset.

        Every preset returns a JSON **object** (a single array element), so the
        collapsed ``check_info`` column is a uniform array-of-objects regardless
        of preset.  Missing ``check_definition`` keys yield JSON ``null``; this
        never raises.

        - ``"names"``   -> ``{"check_name": ...}``
        - ``"summary"`` -> ``{check_name, dimension, tags, target}``
        - ``"full"``    -> full ``check_definition`` + ``check_name`` + ``target``
        """
        if preset == "names":
            return json.dumps({"check_name": cr.check_name})
        check_definition = cr.metadata.get("check_definition") or {}
        target = get_field_label(cr)
        if preset == "summary":
            obj = {
                "check_name": cr.check_name,
                "dimension": check_definition.get("dimension"),
                "tags": check_definition.get("tags"),
                "target": target,
            }
        else:  # "full"
            obj = dict(check_definition)
            obj["check_name"] = cr.check_name
            obj["target"] = target
        return json.dumps(obj, default=str)

    @staticmethod
    def _join_check_info_items(items: Iterable[str]) -> str:
        """Collapse already-JSON-encoded items (ordered, deduped) into a JSON array."""
        return "[" + ", ".join(items) + "]"

    @classmethod
    def _group_check_ids_by_row(cls, combined: nw.DataFrame) -> nw.DataFrame:
        """Group identical data rows, collapsing per-row ``check_info_item``
        JSON objects into a single JSON-array ``check_info`` column.  Does NOT
        require or emit ``tables_in_query``.

        Items are deduplicated and emitted in first-seen order so multi-check
        rows produce one stable array element per distinct failing check.
        """
        data_cols = [c for c in combined.columns if c not in _METADATA_COLUMNS]
        arrow_table = combined.to_arrow()
        check_info_col = arrow_table.column("check_info_item")
        data_arrow_cols = [arrow_table.column(c) for c in data_cols]

        row_groups: dict[tuple[Any, ...], list[str]] = {}
        for i in range(arrow_table.num_rows):
            row_key = tuple(c[i].as_py() for c in data_arrow_cols)
            items = row_groups.setdefault(row_key, [])
            item = check_info_col[i].as_py()
            if item is not None and item not in items:
                items.append(item)

        result_data: dict[str, list] = {c: [] for c in data_cols}
        result_data["check_info"] = []
        for row_key, items in row_groups.items():
            for c, v in zip(data_cols, row_key, strict=False):
                result_data[c].append(v)
            result_data["check_info"].append(cls._join_check_info_items(items))
        return nw.from_native(pa.table(result_data), eager_only=True)

    @staticmethod
    def _check_names_in_entry(df: nw.DataFrame) -> set[str]:
        """Parse the distinct check names from an entry's ``check_ids`` column.

        Assumes check names contain no commas -- they are identifiers, so this
        holds.  If that ever changes, ``check_ids`` needs a different delimiter.
        """
        if "check_ids" not in df.columns:
            return set()
        names: set[str] = set()
        for cell in df["check_ids"].to_list():
            if cell:
                names.update(p.strip() for p in cell.split(",") if p.strip())
        return names

    @staticmethod
    def _annotate_full_table(
        full_table: nw.DataFrame,
        consolidated: nw.DataFrame,
        data_cols: list[str],
        *,
        schema_name: str,
        extra_cols: Sequence[str] = (),
    ) -> nw.DataFrame:
        """Attach ``check_info`` (and any *extra_cols*) to matching full-table rows.

        Uses Python dict matching on the row value-tuple (Candidate B).  Python's
        ``None == None`` is ``True`` and tuples containing ``None`` hash/compare
        correctly, so NULLs match without masks or placeholders, and there is no
        ``pa.null()`` join-key crash.  This mirrors the existing
        ``_consolidate_grouped_output`` row-grouping pattern.

        A value-based matcher cannot distinguish N byte-identical full-table
        rows: if one such row failed, all N receive ``check_info`` (the safe
        over-flagging direction).  See the plan's matching caveats §2.
        """
        marker_cols = ["check_info", *extra_cols]
        consolidated_arrow = consolidated.to_arrow()
        key_cols = [consolidated_arrow.column(c) for c in data_cols]
        marker_arrow = {c: consolidated_arrow.column(c) for c in marker_cols}

        failed_map: dict[tuple[Any, ...], tuple[Any, ...]] = {}
        for i in range(consolidated_arrow.num_rows):
            row_key = tuple(c[i].as_py() for c in key_cols)
            failed_map[row_key] = tuple(marker_arrow[c][i].as_py() for c in marker_cols)

        full_arrow = full_table.to_arrow()
        full_key_cols = [full_arrow.column(c) for c in data_cols]
        outputs: dict[str, list] = {c: [] for c in marker_cols}
        annotated_rows = 0
        for i in range(full_arrow.num_rows):
            row_key = tuple(c[i].as_py() for c in full_key_cols)
            match = failed_map.get(row_key)
            if match is not None:
                annotated_rows += 1
            for col_index, col_name in enumerate(marker_cols):
                outputs[col_name].append(match[col_index] if match is not None else None)

        # Match-quality / NULL-join safety net (NOT a truncation guard): the
        # distinct annotated rows must equal the distinct failed rows fed in.
        distinct_failed = len(failed_map)
        if annotated_rows != distinct_failed:
            logger.warning(
                "Annotated row count mismatch for schema %r: annotated %d distinct "
                "row(s) but %d distinct failed row(s) were provided. Some failed "
                "rows may not have matched a full-table row.",
                schema_name,
                annotated_rows,
                distinct_failed,
            )

        result = full_arrow
        for col_name in marker_cols:
            result = result.append_column(col_name, pa.array(outputs[col_name], type=pa.string()))
        return nw.from_native(result, eager_only=True)

    def get_annotated_output(
        self,
        checks: Sequence[str] | None = None,
        *,
        check_info: CheckInfoPreset | None = None,
    ) -> dict[str, dict[str, nw.DataFrame]]:
        """Return the full in-scope tables with failed rows annotated.

        The result is a nested dict with two fixed reserved top-level keys::

            {
                "annotated": {<schema>: <full table + check_info>, ...},
                "residues":  {<key>: <failed rows + check_ids + tables_in_query>, ...},
            }

        - ``"annotated"`` -- one entry per schema with an available adapter,
          always present even when no eligible check failed (all-null
          ``check_info``).  Inner keys are plain schema names.  The
          ``check_info`` column holds a JSON array of objects per failing row,
          shaped by the ``check_info`` preset (see :data:`CheckInfoPreset`);
          passing rows are ``null``.
        - ``"residues"`` -- **one entry per non-mergeable check** (cross-table,
          aggregation, column-subset, or any check on a schema with no
          adapter).  Keyed by ``"<schema>::<check_name>"``.  Empty dict when
          there are none.  Residues are **per-check, not grouped across
          checks**: a check whose failed rows were annotated onto a full table
          never reappears here, and two non-mergeable checks are never folded
          into one entry even when they share a table and column set.  Each
          residue carries a ``check_ids`` column (holding just that check's
          name) and ``tables_in_query``, so in ``output_mode="annotated"``
          annotated tables carry ``check_info`` while residues carry
          ``check_ids`` -- an accepted, documented within-mode asymmetry.

          (The standalone ``failed_rows``/``both`` CSVs still come from the
          grouped :meth:`get_consolidated_output_dfs` and are unchanged; only
          annotated-mode residues are per-check.)

        Args:
            checks: Optional check-name filter.
            check_info: Preset controlling the ``check_info`` column contents
                (``"names"`` / ``"summary"`` / ``"full"``).  When ``None`` the
                config's ``annotated_check_info`` is used.

        Raises:
            ValueError: When a mergeable check's failed rows were truncated by
                ``max_failed_rows`` -- the un-fetched failures would be
                annotated as passing.  Set ``max_failed_rows=-1`` or use
                ``output_mode="failed_rows"``.
        """
        preset = check_info if check_info is not None else self._config.annotated_check_info
        checks_set = set(checks) if checks else None

        # Step 1: build one annotated table per schema, tracking which checks
        # were merged in (by output key, so same-named checks across schemas
        # stay distinct).
        annotated: dict[str, nw.DataFrame] = {}
        merged_check_keys: set[str] = set()

        for schema_name in self._schema_names:
            full_table = self._fetch_full_table(schema_name)
            if full_table is None:
                continue  # no adapter/export -- leave residues intact
            full_table_cols = set(full_table.columns)

            eligible_failed = [
                cr
                for cr in self.check_results
                if cr.metadata.get("schema_name") == schema_name
                and (not checks_set or cr.check_name in checks_set)
                and cr.status == "FAILED"
                and self._is_mergeable_for_full_table(cr, full_table_cols)
                and len(cr.failed_rows) > 0
            ]

            # Guard: a mergeable failure whose rows were capped would annotate
            # the un-fetched failures as passing. Raise rather than emit a
            # quietly-wrong table. No-op when max_failed_rows == -1 (default).
            for cr in eligible_failed:
                if self._failed_rows_truncated(cr):
                    raise ValueError(
                        f"Cannot produce annotated output for schema {schema_name!r}: check "
                        f"{cr.check_name!r} returned {cr.failed_rows_count} failed rows but only "
                        f"{len(cr.failed_rows)} were fetched (max_failed_rows={self._config.max_failed_rows}). "
                        f"Annotated rows beyond the cap would be silently shown as passing. "
                        f"Set max_failed_rows=-1 or use output_mode='failed_rows'."
                    )

            if not eligible_failed:
                annotated[schema_name] = self._with_null_marker(full_table)
                continue

            tagged_failures: list[nw.DataFrame] = []
            for cr in eligible_failed:
                rows = self._strip_metadata_cols(cr.failed_rows).unique()
                item = self._check_info_item_json(cr, preset)
                tagged_failures.append(rows.with_columns(nw.lit(item).alias("check_info_item")))
                merged_check_keys.add(self._output_key(cr))

            # Collapse duplicate rows into a JSON-array check_info column.
            union = pa.concat_tables([df.to_arrow() for df in tagged_failures], promote_options="default")
            consolidated = self._group_check_ids_by_row(nw.from_native(union, eager_only=True))

            data_cols = [c for c in consolidated.columns if c != "check_info"]
            annotated[schema_name] = self._annotate_full_table(
                full_table,
                consolidated,
                data_cols,
                schema_name=schema_name,
            )

        # Step 2: residues = one entry per FAILED check that was NOT merged onto
        # an annotated table. Per-check (never grouped across checks), so a
        # merged check can never reappear and two non-mergeable checks are never
        # folded together. Each entry is row-deduped within its own check and
        # carries the legacy single-name check_ids + tables_in_query columns.
        residues: dict[str, nw.DataFrame] = {}
        for cr in self.check_results:
            if cr.status != "FAILED":
                continue  # PASSED/ERROR checks have no residue rows
            if checks_set and cr.check_name not in checks_set:
                continue
            if self._output_key(cr) in merged_check_keys:
                continue  # already annotated onto a full table -> not a residue
            if len(cr.failed_rows) == 0:
                continue  # scalar aggregations etc. -- no rows to emit

            tables = get_tables_in_query(cr)
            tables_str = ", ".join(sorted(tables)) if tables else ""
            tagged = self._append_output_metadata(cr.failed_rows, cr.check_name, tables_str)
            residues[self._output_key(cr)] = self._consolidate_grouped_output(tagged)

        return {"annotated": annotated, "residues": residues}

    @staticmethod
    def _strip_metadata_cols(df: nw.DataFrame) -> nw.DataFrame:
        """Drop output-metadata columns, keeping only the underlying data cols."""
        to_drop = [c for c in df.columns if c in _METADATA_COLUMNS]
        return df.drop(to_drop) if to_drop else df

    @staticmethod
    def _with_null_marker(full_table: nw.DataFrame) -> nw.DataFrame:
        """Return *full_table* with an all-null ``check_info`` column."""
        arrow_table = full_table.to_arrow()
        n = arrow_table.num_rows
        arrow_table = arrow_table.append_column("check_info", pa.array([None] * n, type=pa.string()))
        return nw.from_native(arrow_table, eager_only=True)

    # Preferred column order for check results output.
    _CHECK_RESULTS_COLUMN_ORDER: list[str] = [
        "check_name",
        "target",
        "schema_name",
        "engine",
        "type",
        "dimension",
        "description",
        "status",
        "severity",
        "operator",
        "actual_value",
        "expected_value",
        "failed_rows_count",
        "aggregation_type",
        "message",
        "rendered_implementation",
        "tables_in_query",
        "check_path",
        "check_ref_type",
        "logical_type",
        "is_generated",
        "check_definition",
        "contract_definition",
    ]

    @staticmethod
    def _arrow_safe(value):
        """Coerce metadata values to strings so Arrow columns stay consistently typed."""
        if value is None:
            return None
        return str(value)

    def get_check_results_df(
        self,
        *,
        include_check_definition: bool = False,
        include_contract_definition: bool = False,
    ) -> nw.DataFrame:
        """Return a DataFrame with one row per check result.

        Args:
            include_check_definition: When *True* a
                ``check_definition`` column is appended containing the
                resolved/generated check definition serialised as JSON.
            include_contract_definition: When *True* a
                ``contract_definition`` column is appended containing the
                raw ODCS contract content at the check's JSONPath.
        """
        _safe = self._arrow_safe
        data = []
        extra_keys: list[str] = []
        for cr in self.check_results:
            raw_meta = dict(cr.metadata)
            check_def = raw_meta.pop("check_definition", {})
            contract_def = raw_meta.pop("contract_definition", {})
            flat_meta = {k: _safe(v) for k, v in raw_meta.items()}
            row = {
                "check_name": cr.check_name,
                "status": cr.status,
                "expected_value": str(cr.expected_value) if cr.expected_value is not None else None,
                "actual_value": str(cr.actual_value) if cr.actual_value is not None else None,
                "failed_rows_count": cr.failed_rows_count,
                "message": cr.details if cr.status == "ERROR" else "",
                "execution_time_ms": cr.execution_time_ms,
                **flat_meta,
            }
            if include_check_definition:
                row["check_definition"] = json.dumps(check_def, default=str) if check_def else None
            if include_contract_definition:
                row["contract_definition"] = json.dumps(contract_def, default=str) if contract_def else None
            data.append(row)
            for key in row:
                if key not in self._CHECK_RESULTS_COLUMN_ORDER and key not in extra_keys:
                    extra_keys.append(key)
        ordered_keys = [
            k for k in self._CHECK_RESULTS_COLUMN_ORDER if data and k in data[0] or any(k in r for r in data)
        ]
        ordered_keys += [k for k in extra_keys if k not in ordered_keys]
        return nw.from_native(
            pa.table({key: [row.get(key) for row in data] for key in ordered_keys}) if data else pa.table({}),
            eager_only=True,
        )

    def save(
        self,
        output_dir: str = ".",
        prefix: str = "vowl_results",
        *,
        include_check_definition: bool = False,
        include_contract_definition: bool = False,
        output_mode: OutputMode | None = None,
        check_info: CheckInfoPreset | None = None,
    ) -> ValidationResult:
        mode = output_mode if output_mode is not None else self._config.output_mode
        if mode not in ("failed_rows", "annotated", "both"):
            raise ValueError(f"Unknown output_mode: {mode!r}. Expected one of 'failed_rows', 'annotated', 'both'.")

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        check_csv = output_path / f"{prefix}_check_results.csv"
        _pa_csv.write_csv(
            self.get_check_results_df(
                include_check_definition=include_check_definition,
                include_contract_definition=include_contract_definition,
            ).to_arrow(),
            str(check_csv),
        )

        saved_files = [str(check_csv)]

        if mode in ("failed_rows", "both"):
            for table_key, df in self.get_consolidated_output_dfs().items():
                safe_key = table_key.replace(", ", "_").replace(" ", "_")
                csv_path = output_path / f"{prefix}_{safe_key}.csv"
                _pa_csv.write_csv(df.to_arrow(), str(csv_path))
                saved_files.append(str(csv_path))

        if mode in ("annotated", "both"):
            out = self.get_annotated_output(check_info=check_info)
            for schema, df in out["annotated"].items():
                safe_key = schema.replace(", ", "_").replace(" ", "_")
                csv_path = output_path / f"{prefix}_{safe_key}_annotated.csv"
                _pa_csv.write_csv(df.to_arrow(), str(csv_path))
                saved_files.append(str(csv_path))
            # In "annotated" mode, residues cover the non-mergeable checks and
            # the standalone failed-rows CSVs were NOT written, so emit them
            # here. In "both" mode, those same failed rows are already in the
            # grouped failed-rows CSVs written above, so skip to avoid emitting
            # the same rows twice in a different (per-check) shape.
            if mode == "annotated":
                for residue_key, df in out["residues"].items():
                    safe_key = residue_key.replace("::", "_").replace(", ", "_").replace(" ", "_")
                    csv_path = output_path / f"{prefix}_{safe_key}_residue.csv"
                    _pa_csv.write_csv(df.to_arrow(), str(csv_path))
                    saved_files.append(str(csv_path))

        json_path = output_path / f"{prefix}_summary.json"
        with open(json_path, "w") as f:
            json.dump(self.summary, f, indent=2, default=str)

        print("\nResults saved:")
        for fp in saved_files:
            print(f"   - {fp}")
        print(f"   - {json_path}")
        return self

    @staticmethod
    def save_dataframe(df: Any, filepath: str, file_format: str = "csv", **kwargs) -> None:
        output_dir = Path(filepath).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        if isinstance(df, pa.Table):
            arrow_table = df
        elif isinstance(df, nw.DataFrame):
            arrow_table = df.to_arrow()
        elif hasattr(df, "to_arrow"):
            arrow_table = df.to_arrow()
        else:
            arrow_table = nw.from_native(df, eager_only=True).to_arrow()

        fmt = file_format.lower()
        if fmt == "csv":
            _pa_csv.write_csv(arrow_table, filepath, **kwargs)
        elif fmt == "parquet":
            _pa_pq.write_table(arrow_table, filepath, **kwargs)
        elif fmt == "json":
            rows = arrow_table.to_pylist()
            with open(filepath, "w") as f:
                for row in rows:
                    f.write(json.dumps(row, default=str) + "\n")
        else:
            raise ValueError(f"Unsupported format: {file_format}. Use 'csv', 'parquet', or 'json'")

        print(f"Saved to: {filepath}")

    def display_full_report(self, max_rows: int = 5) -> ValidationResult:
        self.print_summary().show_failed_rows(max_rows=max_rows)
        return self
