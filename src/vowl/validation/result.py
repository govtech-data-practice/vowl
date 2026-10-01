"""Validation result container and reporting helpers."""

from __future__ import annotations

import json
import logging
import re
import warnings
from collections.abc import Iterable, Sequence
from dataclasses import asdict, fields
from typing import TYPE_CHECKING, Any

import narwhals as nw
import pyarrow as pa

from ..config import CheckInfoPreset, OutputMode, ValidationConfig
from ..contracts.contract import Contract
from ..contracts.models.ODCS_types import DataContract
from ..executors.base import CheckResult
from ._output_dir import OutputDir, split_file_location
from .dq_metrics import build_document, new_run_id
from .result_models import (
    CheckStatusSummary,
    MultiTableSummary,
    OverallSummary,
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
    align_to_schema,
    first_occurrence_indices,
    row_keys,
)
from .row_quality import (
    CheckRowQuality,
    DimensionRowQuality,
    RowQuality,
    RowQualityReport,
    SchemaRowQuality,
    flagged_checks,
)
from .row_quality.mergeable import METADATA_COLUMNS, rows_mergeable
from .row_quality.selection import REASON_OPERATOR, resolve_check_dimension

if TYPE_CHECKING:
    from ..adapters.multi_source_adapter import MultiSourceAdapter

logger = logging.getLogger(__name__)

#: Metadata columns that tag failed-rows output but are not part of the
#: underlying table's identity. Stripped before matching annotated rows.
_METADATA_COLUMNS = METADATA_COLUMNS

# Characters allowed in a generated output filename component. Everything else
# (path separators, "..", NUL, etc.) is collapsed to "_" so that table/schema
# names taken from a contract cannot escape the output directory.
_FILENAME_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_filename_component(value: str, *, fallback: str = "output") -> str:
    """Sanitize a string for use as a single output-filename component.

    Replaces path separators and other unsafe characters with underscores,
    neutralizes ``..`` parent-directory references, collapses runs of
    underscores, and strips leading/trailing separators. The result cannot
    traverse directories (``..``, ``/etc/...``) or be interpreted as an
    option. Empty or all-separator inputs collapse to *fallback*.
    """
    cleaned = _FILENAME_SAFE_RE.sub("_", str(value))
    # Neutralize any remaining parent-directory references.
    cleaned = cleaned.replace("..", "_")
    # Collapse repeated underscores and trim leading/trailing separators.
    cleaned = re.sub(r"_+", "_", cleaned).strip("._-")
    if not cleaned:
        return fallback
    return cleaned


#: Accepted values of ``get_row_quality_df(by=...)``.
_ROW_QUALITY_GROUPINGS = ("schema", "dimension", "check")


class ValidationResult:
    """Container for validation results and reporting helpers."""

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
        self._row_quality_component: RowQuality | None = None
        self._schema_validation_breakdown: dict[str, SchemaValidationBreakdown] | None = None
        self._schema_column_names: dict[str, list[str]] = {}
        self._full_table_cache: dict[str, nw.DataFrame | None] = {}
        #: This run's id. ``save`` and ``export_otel`` both use it, so the saved
        #: files and the telemetry of one run share it. Set it to use your own.
        self.run_id: str = new_run_id()
        # Wall-clock run window in epoch nanoseconds, set by the runner.
        self._run_started_ns: int | None = None
        self._run_finished_ns: int | None = None

    def __repr__(self) -> str:
        total = self._vs["total_checks"]
        passed = self._vs["passed"]
        failed = self._vs["failed"]
        return f"ValidationResult(passed={self.passed}, checks={total}, passed_checks={passed}, failed_checks={failed})"

    @staticmethod
    def _supports_row_level_output(check_result: CheckResult) -> bool:
        """True when the result can participate in row-level summaries/output."""
        return check_result.supports_row_level_output

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

    def _row_quality(self) -> RowQuality:
        """The row-quality component of this result, created on first use."""
        if self._row_quality_component is None:
            self._row_quality_component = RowQuality(self)
        return self._row_quality_component

    def _row_quality_report(self) -> RowQualityReport:
        """Every row-quality number of this run, computed once and cached."""
        return self._row_quality().report()

    def _get_schema_validation_breakdown(self) -> dict[str, SchemaValidationBreakdown]:
        if self._schema_validation_breakdown is not None:
            return self._schema_validation_breakdown

        report = self._row_quality_report()
        breakdown: dict[str, SchemaValidationBreakdown] = {}
        for schema_name in self._schema_names:
            breakdown[schema_name] = self._build_schema_breakdown(
                self._get_checks_for_schema(schema_name),
                report.schema(schema_name),
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
        schema_checks: Sequence[CheckResult],
        row_quality: SchemaRowQuality | None,
    ) -> SchemaValidationBreakdown:
        single_table_checks, multi_table_checks = self._split_checks_by_scope(schema_checks)
        pass_rate = row_quality.pass_rate if row_quality is not None else None
        overall = OverallSummary(
            **asdict(self._summarize_check_statuses(schema_checks)),
            failed_rows=row_quality.failed_rows if row_quality is not None else None,
            passed_rows=row_quality.passed_rows if row_quality is not None else None,
            total_rows=row_quality.total_rows if row_quality is not None else None,
            passed_row_percentage=pass_rate * 100 if pass_rate is not None else None,
            exact=row_quality.exact if row_quality is not None else False,
        )
        multi_status = self._summarize_check_statuses(multi_table_checks)
        return SchemaValidationBreakdown(
            overall=overall,
            single_table=SingleTableSummary(**asdict(self._summarize_check_statuses(single_table_checks))),
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

        Cross-table checks are grouped here by their ``tables_in_query`` and
        appear under a composite table key (e.g. ``"table_a, table_b"``),
        regardless of column shape.  This differs from ``get_annotated_output``,
        which routes each cross-table check individually -- merging it onto its
        anchor table when the failed rows carry only that schema's columns, or
        emitting a per-check residue otherwise.  See docs/known-issues.md for
        details.

        .. deprecated::
            Prefer :meth:`get_annotated_output`, which returns your full
            in-scope tables with failing rows flagged in place (plus per-check
            residues for the checks that cannot be merged).  This grouped view
            will be removed in a future release.
        """
        warnings.warn(
            "get_consolidated_output_dfs() is deprecated and will be removed in a "
            "future release. Use get_annotated_output() instead, which returns your "
            "full tables with failing rows flagged in place plus per-check residues. "
            "Note the output shape differs: annotated tables carry a 'check_info' "
            "JSON-array column (per row) rather than the grouped 'check_ids' "
            "comma-joined string, and include passing rows too.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._get_consolidated_output_dfs(checks=checks)

    def _get_consolidated_output_dfs(self, checks: Sequence[str] | None = None) -> dict[str, nw.DataFrame]:
        """Implementation of the grouped failed-rows view.

        Kept as a private method so internal callers (e.g. ``save_outputs`` in
        ``failed_rows``/``both`` mode) can reuse the grouping without emitting the
        public method's ``DeprecationWarning``.
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
        check_ids = arrow_table.column("check_id").to_pylist()
        tables = arrow_table.column("tables_in_query").to_pylist()

        first_index: dict[tuple[Any, ...], int] = {}
        check_ids_by_key: dict[tuple[Any, ...], set[str]] = {}
        for row_index, row_key in enumerate(row_keys(arrow_table, data_cols)):
            first_index.setdefault(row_key, row_index)
            check_ids_by_key.setdefault(row_key, set()).add(check_ids[row_index])

        # Rebuild from the original Arrow rows rather than from Python values,
        # so types such as timestamp[ns] and uint64 are kept exactly.
        indices = list(first_index.values())
        result = arrow_table.select(data_cols).take(pa.array(indices, type=pa.int64()))
        result = result.append_column(
            "check_ids",
            pa.array([", ".join(sorted(ids)) for ids in check_ids_by_key.values()], type=pa.string()),
        ).append_column("tables_in_query", pa.array([tables[i] for i in indices]))
        return nw.from_native(result, eager_only=True)

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
    def _is_mergeable_for_full_table(
        rows: nw.DataFrame, full_table_columns: set[str], key_columns: Sequence[str] | None = None
    ) -> bool:
        """True when a check's failed *rows* can be annotated onto the full table.

        This single gate handles both single- and cross-table checks: the
        failed-rows column set must exactly equal the anchor table's columns.
        A **cross-table** check (one that JOINs against a reference table) is
        not rejected outright. If the author shaped its failed-rows query to
        project only the anchor schema's columns (e.g. ``SELECT payroll.* FROM
        payroll LEFT JOIN ref ...``), those rows match the anchor table and
        merge. A bare ``JOIN`` whose ``SELECT *`` returns both tables' columns
        does not match and stays a residue. The caller only ever evaluates a
        check against its own ``schema_name`` full table, so a cross-table
        check can never merge onto an unrelated schema whose shape happens to
        coincide.

        When *key_columns* is given, the schema's rows are matched on its
        declared primary key instead (see :meth:`RowQuality.merge_key`), and
        rows that carry every key column merge. The rule is
        :func:`~vowl.validation.row_quality.mergeable.rows_mergeable`, the same
        one the row numbers use.
        """
        return rows_mergeable(rows.columns, full_table_columns, key_columns)

    def _failed_rows_truncated(self, row_count: int | None, rows: nw.DataFrame) -> bool:
        """True when a check's fetched failed rows were capped by ``max_failed_rows``.

        *row_count* is the true count from the aggregate SQL and is not subject
        to the ``LIMIT``. Only the fetched frame is. So a true count exceeding
        the fetched length means the sample was truncated.
        """
        cap = self._config.max_failed_rows
        return cap >= 0 and row_count is not None and row_count > len(rows)

    @staticmethod
    def _check_info_item_json(cr: CheckResult, preset: CheckInfoPreset, *, tolerated: bool = False) -> str:
        """JSON-encode one failing check's per-row item for the given preset.

        Every preset returns a JSON **object** (a single array element), so the
        collapsed ``check_info`` column is a uniform array-of-objects regardless
        of preset.  Missing ``check_definition`` keys yield JSON ``null``; this
        never raises.

        - ``"names"``   -> ``{"check_name": ...}``
        - ``"summary"`` -> ``{check_name, dimension, tags, target}``
        - ``"full"``    -> full ``check_definition`` + ``check_name`` + ``target``

        Under ``row_issue_scope="all_violations"``, the item of a check that
        passed within its tolerance also carries ``"tolerated": true``.
        """
        if preset == "names":
            obj: dict[str, Any] = {"check_name": cr.check_name}
            if tolerated:
                obj["tolerated"] = True
            return json.dumps(obj)
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
        if tolerated:
            obj["tolerated"] = True
        return json.dumps(obj, default=str)

    @staticmethod
    def _join_check_info_items(items: Iterable[str]) -> str:
        """Collapse already-JSON-encoded items (ordered, deduped) into a JSON array."""
        return "[" + ", ".join(items) + "]"

    def _build_residue_with_check_info(
        self,
        cr: CheckResult,
        rows: nw.DataFrame,
        preset: CheckInfoPreset,
        tables_str: str,
        *,
        tolerated: bool = False,
    ) -> nw.DataFrame:
        """Build a per-check residue: deduped failed rows + ``check_info`` + ``tables_in_query``.

        Residues are per-check, so ``check_info`` is a single-element JSON array
        shaped by *preset* -- the same shape the annotated table uses.  Emitting
        ``check_info`` here (rather than the legacy ``check_ids``) keeps every
        file produced in ``output_mode="annotated"`` uniform: annotated tables
        and residues are read the same way.  ``tables_in_query`` is retained
        because residues are non-mergeable (often cross-table) and the source
        tables are useful context.
        """
        arrow_table = self._strip_metadata_cols(rows).to_arrow()
        if arrow_table.num_columns:
            # Key-based dedupe instead of .unique(), which cannot hash nested
            # types and treats NaN and -0.0 differently from the annotated merge.
            first = first_occurrence_indices(row_keys(arrow_table, arrow_table.column_names))
            arrow_table = arrow_table.take(pa.array(list(first.values()), type=pa.int64()))
        check_info = self._join_check_info_items([self._check_info_item_json(cr, preset, tolerated=tolerated)])
        n = arrow_table.num_rows
        arrow_table = arrow_table.append_column(
            "check_info", pa.array([check_info] * n, type=pa.string())
        ).append_column("tables_in_query", pa.array([tables_str] * n, type=pa.string()))
        return nw.from_native(arrow_table, eager_only=True)

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
        check_info_items = arrow_table.column("check_info_item").to_pylist()

        first_index: dict[tuple[Any, ...], int] = {}
        row_groups: dict[tuple[Any, ...], list[str]] = {}
        for i, row_key in enumerate(row_keys(arrow_table, data_cols)):
            first_index.setdefault(row_key, i)
            items = row_groups.setdefault(row_key, [])
            item = check_info_items[i]
            if item is not None and item not in items:
                items.append(item)

        check_info = pa.array([cls._join_check_info_items(items) for items in row_groups.values()], type=pa.string())
        if not data_cols:
            return nw.from_native(pa.table({"check_info": check_info}), eager_only=True)
        # Rebuild from the original Arrow rows rather than from Python values.
        # Re-inferring types from Python values truncated timestamp[ns] to
        # microseconds and overflowed uint64 values above 2^63.
        indices = pa.array(list(first_index.values()), type=pa.int64())
        result = arrow_table.select(data_cols).take(indices).append_column("check_info", check_info)
        return nw.from_native(result, eager_only=True)

    @staticmethod
    def _check_names_in_entry(df: nw.DataFrame) -> set[str]:
        """Parse the distinct check names from an entry's marker column.

        Reads the ``check_info`` JSON-array column emitted by annotated tables
        and per-check residues (``item["check_name"]`` per element).  Falls back
        to the legacy comma-joined ``check_ids`` column for entries produced by
        the untouched consolidated/failed-rows path.
        """
        names: set[str] = set()
        if "check_info" in df.columns:
            for cell in df["check_info"].to_list():
                if not cell:
                    continue
                for item in json.loads(cell):
                    name = item.get("check_name") if isinstance(item, dict) else None
                    if name:
                        names.add(name)
            return names
        if "check_ids" in df.columns:
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

        Uses Python dict matching on normalised row keys from ``row_keys``
        (Candidate B).  NULLs match because ``None == None``, NaN matches NaN,
        ``-0.0`` stays apart from ``0.0``, nested values are compared element by
        element and temporal values at full precision.  The failed-row key
        columns are first cast to the full table's types where they differ, so
        both sides build keys from the same Arrow types.

        A value-based matcher cannot distinguish N byte-identical full-table
        rows: if one such row failed, all N receive ``check_info`` (the safe
        over-flagging direction).  See the plan's matching caveats §2.
        """
        marker_cols = ["check_info", *extra_cols]
        consolidated_arrow = consolidated.to_arrow()
        full_arrow = full_table.to_arrow()
        key_table = align_to_schema(consolidated_arrow, full_arrow.schema, data_cols)
        marker_values = [consolidated_arrow.column(c).to_pylist() for c in marker_cols]

        failed_map: dict[tuple[Any, ...], tuple[Any, ...]] = {}
        for i, row_key in enumerate(row_keys(key_table, data_cols)):
            failed_map[row_key] = tuple(values[i] for values in marker_values)

        outputs: dict[str, list] = {c: [] for c in marker_cols}
        annotated_rows = 0
        for row_key in row_keys(full_arrow, data_cols):
            match = failed_map.get(row_key)
            if match is not None:
                annotated_rows += 1
            for col_index, col_name in enumerate(marker_cols):
                outputs[col_name].append(match[col_index] if match is not None else None)

        # Match-quality / NULL-join safety net (NOT a truncation guard): the
        # distinct annotated rows must equal the distinct failed rows fed in.
        # A mismatch can go either way, and the two directions mean very
        # different things, so report them separately and in plain language.
        distinct_failed = len(failed_map)
        if annotated_rows > distinct_failed:
            # More rows flagged than distinct failures: the table contains
            # duplicate rows, and a single failure matched all of its copies.
            # This is normal, expected operation, so it is not surfaced by
            # default. It is kept at DEBUG only as a breadcrumb explaining why
            # the annotated flagged-row count exceeds the summary's
            # failed_rows_count when duplicate rows are present.
            logger.debug(
                "Table %r contains duplicate rows, so the annotated table "
                "flagged %d rows for %d unique failing rows. Every copy of a "
                "failing row is flagged. This is expected; nothing was missed.",
                schema_name,
                annotated_rows,
                distinct_failed,
            )
        elif annotated_rows < distinct_failed:
            # Fewer rows flagged than distinct failures. Failed rows are derived
            # FROM the full table, so by construction every failing value-tuple
            # should exist in it. Under-matching means the matcher itself failed
            # (e.g. NULL/type handling differing between the failed-rows and
            # full-table export paths). This should not happen; treat it as an
            # internal bug and ask the user to report it.
            logger.warning(
                "Unexpected problem while building the annotated table for %r: "
                "%d unique failing rows were found but only %d could be matched "
                "back to the full table, so %d failing row(s) are missing from "
                "the annotated output. This should not happen. Please report "
                "this as an issue (the failed-rows output still has the complete "
                "list of failures).",
                schema_name,
                distinct_failed,
                annotated_rows,
                distinct_failed - annotated_rows,
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
                "residues":  {<key>: <failed rows + check_info + tables_in_query>, ...},
            }

        - ``"annotated"`` -- one entry per schema with an available adapter,
          always present even when no eligible check failed (all-null
          ``check_info``).  Inner keys are plain schema names.  The
          ``check_info`` column holds a JSON array of objects per failing row,
          shaped by the ``check_info`` preset (see :data:`CheckInfoPreset`);
          passing rows are ``null``.
        - ``"residues"`` -- **one entry per non-mergeable check that still has
          offending rows to emit** (column-subset checks that do not hold a
          unique declared primary key, cross-table checks whose failed rows
          carry columns beyond the anchor schema, or any check on a schema with
          no adapter).  A cross-table check whose failed-rows
          query projects only the anchor schema's columns is *mergeable* and
          annotates onto that schema's table instead (see
          :meth:`_is_mergeable_for_full_table`).  Keyed by
          ``"<schema>::<check_name>"``.
          Empty dict when there are none.  A check with *no* rows to flag -- a
          scalar aggregation (``AVG``/``SUM``/``MIN``/``MAX``, ``rowCount``),
          an errored check, or an inverted check whose matched rows are the
          good ones (``mustBeGreaterThan`` and so on) -- produces **no
          residue**. Its failure is recorded only in the summary, not in any
          CSV.  Residues are
          **per-check, not grouped across checks**: a check whose failed rows
          were annotated onto a full table never reappears here, and two
          non-mergeable checks are never folded into one entry even when they
          share a table and column set.  Each residue carries a ``check_info``
          column (a single-element JSON array shaped by the same ``check_info``
          preset as the annotated tables) plus ``tables_in_query``, so every
          file produced in ``output_mode="annotated"`` -- annotated tables and
          residues alike -- is read the same way.

          Under ``row_issue_scope="all_violations"``, rows of checks that
          passed within their tolerance are flagged too, and their
          ``check_info`` items carry ``"tolerated": true``.

          (The standalone ``failed_rows``/``both`` CSVs still come from the
          grouped :meth:`get_consolidated_output_dfs`, which is unchanged and
          keeps its legacy comma-joined ``check_ids`` column; only annotated
          output uses ``check_info``.)

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
        row_quality = self._row_quality()

        # The checks whose rows are flagged: the row-quality component's
        # counted checks under the configured row_issue_scope. Inverted and
        # table-level checks are left to the summary. Tolerated checks join
        # only under row_issue_scope="all_violations".
        flagged = [
            selection
            for selection in flagged_checks(row_quality.selections)
            if not checks_set or selection.result.check_name in checks_set
        ]
        flagged_ids = {id(selection.result) for selection in flagged}
        # A FAILED inverted check's matched rows are the good rows, so they
        # are neither flagged nor kept as a residue.
        inverted_ids = {
            id(selection.result) for selection in row_quality.selections if selection.reason == REASON_OPERATOR
        }

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
            # The primary key the row numbers matched on, so both merge the same checks.
            key_columns = row_quality.merge_key(schema_name)
            if key_columns and not set(key_columns) <= full_table_cols:
                key_columns = None

            mergeable = [
                selection
                for selection in flagged
                if selection.schema_name == schema_name
                and self._is_mergeable_for_full_table(row_quality.rows_for(selection), full_table_cols, key_columns)
            ]

            # Guard: a mergeable failure whose rows were capped would annotate
            # the un-fetched failures as passing. Raise rather than emit a
            # quietly-wrong table. No-op when max_failed_rows == -1 (default).
            # Runs before the empty-rows filter below so that max_failed_rows=0,
            # which fetches no rows at all, is caught too.
            for selection in mergeable:
                rows = row_quality.rows_for(selection)
                if self._failed_rows_truncated(selection.row_count, rows):
                    raise ValueError(
                        f"Cannot produce annotated output for schema {schema_name!r}: check "
                        f"{selection.result.check_name!r} returned {selection.row_count} failed rows but only "
                        f"{len(rows)} were fetched (max_failed_rows={self._config.max_failed_rows}). "
                        f"Annotated rows beyond the cap would be silently shown as passing. "
                        f"Set max_failed_rows=-1 or use output_mode='failed_rows'."
                    )

            eligible = [selection for selection in mergeable if len(row_quality.rows_for(selection)) > 0]

            if not eligible:
                annotated[schema_name] = self._with_null_marker(full_table)
                continue

            tagged_failures: list[nw.DataFrame] = []
            for selection in eligible:
                # No .unique() here: _group_check_ids_by_row collapses duplicate
                # rows itself, and .unique() cannot hash nested column types.
                rows = self._strip_metadata_cols(row_quality.rows_for(selection))
                if key_columns:
                    rows = rows.select(key_columns)
                item = self._check_info_item_json(selection.result, preset, tolerated=selection.tolerated)
                tagged_failures.append(rows.with_columns(nw.lit(item).alias("check_info_item")))
                merged_check_keys.add(self._output_key(selection.result))

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

        # Step 2: residues = one entry per flagged check that was NOT merged
        # onto an annotated table, plus the other FAILED checks that returned
        # rows, except inverted ones.
        # Per-check (never grouped across checks), so a merged check can never
        # reappear and two non-mergeable checks are never folded together.
        # Each entry is row-deduped within its own check and carries the same
        # check_info column as the annotated tables (a single-element JSON
        # array) plus tables_in_query.
        candidates = [(selection.result, selection.tolerated, row_quality.rows_for(selection)) for selection in flagged]
        candidates += [
            (cr, False, cr.failed_rows)
            for cr in self.check_results
            if cr.status == "FAILED"
            and id(cr) not in flagged_ids
            and id(cr) not in inverted_ids
            and (not checks_set or cr.check_name in checks_set)
        ]
        residues: dict[str, nw.DataFrame] = {}
        for cr, tolerated, rows in candidates:
            if self._output_key(cr) in merged_check_keys:
                continue  # already annotated onto a full table -> not a residue
            if len(rows) == 0:
                continue  # no rows to emit

            tables = get_tables_in_query(cr)
            tables_str = ", ".join(sorted(tables)) if tables else ""
            residues[self._output_key(cr)] = self._build_residue_with_check_info(
                cr, rows, preset, tables_str, tolerated=tolerated
            )

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

    def get_row_quality_df(self, by: str = "schema") -> nw.DataFrame:
        """Return the row-quality numbers: how many rows of each table have issues.

        Every surface reads the same cached numbers: ``print_summary``, the
        OTEL gauges and annotated output agree with this frame. See
        docs/design-considerations/failed-rows/levels.md.

        Args:
            by: ``"schema"`` for one row per schema, ``"dimension"`` for one
                row per (schema, dimension), or ``"check"`` for one row per
                check, with the route its rows took and why it was or was not
                counted.

        Columns for ``"schema"`` and ``"dimension"``: ``schema_name``,
        ``dimension`` (``"dimension"`` only), ``total_rows``, ``failed_rows``,
        ``tolerated_rows``, ``passed_rows``, ``pass_rate`` (0 to 1),
        ``exact``, ``checks_counted`` and ``checks_not_counted``. A missing
        value (null) means the number is unavailable, for example a dimension
        with no counted checks.

        Columns for ``"check"``: ``schema_name``, ``check_name``,
        ``dimension``, ``status``, ``counted``, ``tolerated``, ``route``,
        ``reason``, ``failed_rows`` and ``exact``.

        Raises:
            ValueError: If *by* is not one of the values above.
        """
        if by not in _ROW_QUALITY_GROUPINGS:
            raise ValueError(f"by must be one of {', '.join(_ROW_QUALITY_GROUPINGS)}, got {by!r}")
        report = self._row_quality_report()
        items: Sequence[Any]
        if by == "schema":
            items, item_type = report.schemas, SchemaRowQuality
        elif by == "dimension":
            items, item_type = report.dimensions, DimensionRowQuality
        else:
            items, item_type = report.checks, CheckRowQuality
        names = [f.name for f in fields(item_type)]
        rows = [asdict(item) for item in items]
        arrow_types = {
            "total_rows": pa.int64(),
            "failed_rows": pa.int64(),
            "tolerated_rows": pa.int64(),
            "passed_rows": pa.int64(),
            "pass_rate": pa.float64(),
            "exact": pa.bool_(),
            "counted": pa.bool_(),
            "tolerated": pa.bool_(),
            "checks_counted": pa.int64(),
            "checks_not_counted": pa.int64(),
        }
        table = pa.table(
            {name: pa.array([row[name] for row in rows], type=arrow_types.get(name, pa.string())) for name in names}
        )
        return nw.from_native(table, eager_only=True)

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
            flat_meta["dimension"] = resolve_check_dimension(cr)
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

    def get_dq_metrics(self) -> dict[str, Any]:
        """Return this run's DQ metrics: the content of ``dq_metrics.json``.

        One point per metric reading, at check, dimension, schema and run
        level, with the same names, types, units and attributes as the
        OpenTelemetry metrics of :meth:`export_otel`. See
        docs/dq-metrics/json-export.md.

        Keys: ``schema_version``, ``run`` (run identity attributes, including
        ``vowl.run.id``), ``run_started_at`` and ``run_finished_at`` (ISO 8601
        UTC, or None), and ``points``, a list of ``{"name", "type", "unit",
        "value", "attributes"}``.
        """
        return build_document(self)

    def export_otel(
        self,
        *,
        signals: Sequence[str] = ("metrics", "traces", "logs"),
        endpoint: str | None = None,
        protocol: str | None = None,
        service_name: str = "vowl",
        prefix: str = "vowl",
        headers: dict[str, str] | None = None,
        custom_attributes: dict[str, Any] | None = None,
        run_id: str | None = None,
        max_failed_rows_sample: int = 0,
        use_global_providers: bool = False,
        metric_provider: Any | None = None,
        tracer_provider: Any | None = None,
        logger_provider: Any | None = None,
    ) -> str:
        """Export this run's results to OpenTelemetry (metrics, traces, logs).

        Requires the optional ``[otel]`` extra (``pip install vowl[otel]``).
        Reads this finished result only. Nothing is re-run against the data.
        Returns the run id (``vowl.run.id``), which is :attr:`run_id` unless
        *run_id* is passed. See docs/dq-metrics/otel-export.md.

        Args:
            signals: Which signals to emit, any subset of ``"metrics"``,
                ``"traces"``, ``"logs"``. All three are on by default.
            endpoint: OTLP endpoint. When omitted, the standard
                ``OTEL_EXPORTER_OTLP_*`` env vars are used. For
                ``"http/protobuf"`` vowl adds the ``/v1/<signal>`` path.
            protocol: ``"grpc"`` or ``"http/protobuf"``. When omitted, uses
                ``OTEL_EXPORTER_OTLP_PROTOCOL``, then ``"grpc"``.
            service_name: ``service.name`` context attribute (default
                ``"vowl"``). Identifies where the validation is running.
            prefix: Prefix for metric names, span names and vowl's attribute
                keys (default ``"vowl"``). ``"myorg"`` turns
                ``vowl.check.check.count`` into ``myorg.check.check.count``.
            headers: Optional OTLP headers (for example auth).
            custom_attributes: Additional attributes merged onto every data
                point, span, and log record. vowl never inspects or reroutes
                a key.
            run_id: Id for this export. When omitted, :attr:`run_id` is used,
                the same id :meth:`save` writes into ``dq_metrics.json``.
            max_failed_rows_sample: Max failing rows to attach per check to
                logs and span events. ``0`` (default) exports no cell values.
                A positive value is also capped by the run's ``max_failed_rows``.
            use_global_providers: Record into the process's already-configured
                global providers instead of building OTLP exporters. vowl then
                owns no lifecycle (no flush or shutdown).
            metric_provider / tracer_provider / logger_provider: Explicit
                providers for full control. They take precedence over
                everything else.

        Raises:
            ImportError: When the ``[otel]`` extra is not installed.
            ValueError: On an unknown signal or protocol, or when a signal vowl
                has to build a provider for has no endpoint configured.

        Warns:
            RuntimeWarning: When the backend rejected the data or could not be
                reached, for any provider vowl built itself.
        """
        try:
            from ..otel import OtelExporter
        except ImportError as exc:
            raise ImportError(
                "OpenTelemetry export requires the optional 'otel' extra. Install it with:  pip install vowl[otel]"
            ) from exc

        exporter = OtelExporter(
            signals=tuple(signals),
            endpoint=endpoint,
            protocol=protocol,
            service_name=service_name,
            prefix=prefix,
            headers=headers,
            custom_attributes=custom_attributes,
            run_id=run_id,
            max_failed_rows_sample=max_failed_rows_sample,
            use_global_providers=use_global_providers,
            metric_provider=metric_provider,
            tracer_provider=tracer_provider,
            logger_provider=logger_provider,
        )
        return exporter.export(self)

    def save(
        self,
        output_dir: str = ".",
        prefix: str = "vowl_results",
        *,
        include_check_definition: bool = False,
        include_contract_definition: bool = False,
        output_mode: OutputMode | None = None,
        check_info: CheckInfoPreset | None = None,
        filesystem: Any | None = None,
    ) -> ValidationResult:
        """Write the check-results CSV, per-mode row outputs, the summary JSON
        and the DQ metrics JSON (``<prefix>_dq_metrics.json``, see
        :meth:`get_dq_metrics`).

        ``output_dir`` is a local folder or a URI such as
        ``s3://bucket/dq-results/run-1/``. URIs (``s3://``, ``gs://``,
        ``abfs://``, ``hdfs://``, ``file://``) are written through pyarrow's
        filesystems, which find credentials the usual way for each cloud. Pass
        ``filesystem=`` (a ``pyarrow.fs.FileSystem``) to set one up yourself,
        for example an S3-compatible store with a custom endpoint. ``output_dir``
        is then a path inside that filesystem.

        ``output_mode`` selects the row output shape. When it is ``None`` the
        config's ``output_mode`` is used, which defaults to ``"annotated"``:

        - ``"annotated"`` -- **default.** Full in-scope tables with failing
          rows flagged in place via a per-row ``check_info`` column, plus
          per-check residues for non-mergeable checks.
        - ``"failed_rows"`` -- *deprecated.* The legacy consolidated CSVs
          (failing rows only, grouped per table, comma-joined ``check_ids``).
        - ``"both"`` -- writes annotated tables *and* the deprecated
          failed-rows CSVs; a migration bridge.

        .. deprecated::
            ``output_mode="failed_rows"`` (and the ``"failed_rows"`` half of
            ``"both"``) is deprecated and will be removed in a future release.
            Either value emits a ``DeprecationWarning``, whether it is passed
            to ``save()`` or set through ``ValidationConfig(output_mode=...)``.
        """
        mode = output_mode if output_mode is not None else self._config.output_mode
        if mode not in ("failed_rows", "annotated", "both"):
            raise ValueError(f"Unknown output_mode: {mode!r}. Expected one of 'failed_rows', 'annotated', 'both'.")

        # The mode may come from the argument or from ValidationConfig, so
        # the messages below fit both.
        if mode in ("failed_rows", "both"):
            if mode == "both":
                # Annotated CSVs are written too, so this is a valid migration
                # bridge; just flag the deprecated half.
                warnings.warn(
                    "output_mode='both' still writes the deprecated consolidated failed-rows "
                    "CSVs alongside the annotated tables. The failed-rows shape will be removed "
                    "in a future release; migrate to output_mode='annotated' when ready.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            else:
                # output_mode="failed_rows": honoured, but deprecated.
                warnings.warn(
                    "output_mode='failed_rows' writes the deprecated consolidated failed-rows "
                    "CSVs (grouped rows with a 'check_ids' column) and will be removed in a "
                    "future release. Use output_mode='annotated' instead (full tables with a "
                    "per-row 'check_info' column plus per-check residues).",
                    DeprecationWarning,
                    stacklevel=2,
                )

        target = OutputDir(output_dir, filesystem)

        # Sanitize the caller-supplied prefix so it cannot traverse directories.
        prefix = _safe_filename_component(prefix, fallback="vowl_results")

        check_csv = target.write_csv(
            f"{prefix}_check_results.csv",
            self.get_check_results_df(
                include_check_definition=include_check_definition,
                include_contract_definition=include_contract_definition,
            ).to_arrow(),
        )

        saved_files = [check_csv]

        if mode in ("failed_rows", "both"):
            for table_key, df in self._get_consolidated_output_dfs().items():
                safe_key = _safe_filename_component(table_key.replace(", ", "_").replace(" ", "_"))
                saved_files.append(target.write_csv(f"{prefix}_{safe_key}.csv", df.to_arrow()))

        if mode in ("annotated", "both"):
            out = self.get_annotated_output(check_info=check_info)
            for schema, df in out["annotated"].items():
                safe_key = _safe_filename_component(schema.replace(", ", "_").replace(" ", "_"))
                saved_files.append(target.write_csv(f"{prefix}_{safe_key}_annotated.csv", df.to_arrow()))
            # In "annotated" mode, residues cover the non-mergeable checks and
            # the standalone failed-rows CSVs were NOT written, so emit them
            # here. In "both" mode, those same failed rows are already in the
            # grouped failed-rows CSVs written above, so skip to avoid emitting
            # the same rows twice in a different (per-check) shape.
            if mode == "annotated":
                for residue_key, df in out["residues"].items():
                    safe_key = _safe_filename_component(
                        residue_key.replace("::", "_").replace(", ", "_").replace(" ", "_")
                    )
                    saved_files.append(target.write_csv(f"{prefix}_{safe_key}_residue.csv", df.to_arrow()))

        saved_files.append(target.write_text(f"{prefix}_summary.json", json.dumps(self.summary, indent=2, default=str)))
        saved_files.append(
            target.write_text(f"{prefix}_dq_metrics.json", json.dumps(self.get_dq_metrics(), indent=2, default=str))
        )

        print("\nResults saved:")
        for fp in saved_files:
            print(f"   - {fp}")
        return self

    @staticmethod
    def save_dataframe(
        df: Any,
        filepath: str,
        file_format: str = "csv",
        *,
        filesystem: Any | None = None,
        **kwargs,
    ) -> None:
        """Write *df* to *filepath*, a local path or a URI (see :meth:`save`)."""
        fmt = file_format.lower()
        if fmt not in ("csv", "parquet", "json"):
            raise ValueError(f"Unsupported format: {file_format}. Use 'csv', 'parquet', or 'json'")

        folder, name = split_file_location(filepath)
        target = OutputDir(folder, filesystem)

        if isinstance(df, pa.Table):
            arrow_table = df
        elif isinstance(df, nw.DataFrame):
            arrow_table = df.to_arrow()
        elif hasattr(df, "to_arrow"):
            arrow_table = df.to_arrow()
        else:
            arrow_table = nw.from_native(df, eager_only=True).to_arrow()

        if fmt == "csv":
            saved = target.write_csv(name, arrow_table, **kwargs)
        elif fmt == "parquet":
            saved = target.write_parquet(name, arrow_table, **kwargs)
        else:
            saved = target.write_text(
                name, "".join(json.dumps(row, default=str) + "\n" for row in arrow_table.to_pylist())
            )

        print(f"Saved to: {saved}")

    def display_full_report(self, max_rows: int = 5) -> ValidationResult:
        self.print_summary().show_failed_rows(max_rows=max_rows)
        return self
