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

from ..config import CheckInfoPreset, SaveOutput, ValidationConfig, _resolve_outputs
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
    table_key_index,
)
from .row_quality import (
    CheckRowQuality,
    DimensionRowQuality,
    RowQuality,
    RowQualityReport,
    SchemaRowQuality,
    attributed_checks,
)
from .row_quality.match_key import METADATA_COLUMNS, has_match_key
from .row_quality.selection import REASON_OPERATOR, resolve_check_dimension, select_check

# Columns get_output_dfs adds to each check's rows.
_OUTPUT_METADATA_COLUMNS = ("check_id", "tables_in_query", "tolerated", "status")

#: Accepted values of ``get_output_dfs(scope=...)``.
_OUTPUT_SCOPES = ("failed", "all")

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


def _check_column(cr: CheckResult) -> str:
    """The column a check is on, or ``""`` for a schema-level check.

    Read from the ``target`` metadata, which is ``<schema>.<column>`` for a
    column-level check and ``<schema>`` for a schema-level one.
    """
    target = cr.metadata.get("target") or ""
    schema = cr.metadata.get("schema_name") or ""
    if not schema:
        return target
    return target[len(schema) + 1 :] if target.startswith(f"{schema}.") else ""


def _check_label(cr: CheckResult) -> str:
    """Where a check runs, ``<schema>.<column>`` or ``<schema>``, for messages and keys."""
    schema = cr.metadata.get("schema_name") or ""
    column = _check_column(cr)
    return f"{schema}.{column}" if schema and column else schema or column


# Comparison operators spelled out in file names, longest first, so that
# "amount > 0" and "amount < 0" do not both clean to "amount_0".
_OPERATOR_WORDS = (
    (">=", "_gte_"),
    ("<=", "_lte_"),
    ("!=", "_ne_"),
    ("<>", "_ne_"),
    ("==", "_eq_"),
    (">", "_gt_"),
    ("<", "_lt_"),
    ("=", "_eq_"),
)


def _file_name_part(value: str) -> str:
    """Clean one part of a check's file name, spelling out comparison operators."""
    for operator, word in _OPERATOR_WORDS:
        value = value.replace(operator, word)
    return _safe_filename_component(value)


def _check_file_stem(cr: CheckResult) -> str:
    """The file name stem of one check's file, before clashes are numbered.

    ``<schema>__<column>__<check>`` for a column-level check and
    ``<schema>__<check>`` for a schema-level one. Each part is cleaned on its
    own. A cleaned part never holds ``__``, so the join can't be read two
    ways.
    """
    parts = (cr.metadata.get("schema_name") or "", _check_column(cr), cr.check_name)
    return "__".join(_file_name_part(part) for part in parts if part)


def _check_file_stems(checks: Sequence[CheckResult]) -> dict[int, str]:
    """The file name stem of each check, keyed by ``id()``.

    Stems are compared with ``casefold()``, because names that differ only in
    case are one file on a case-insensitive file system. A stem already taken
    gets ``_2``, ``_3`` and so on, in run order, skipping any stem another
    check has. Every check gets a stem, whatever its status, so the name
    depends on the contract alone.
    """
    base = [_check_file_stem(cr) for cr in checks]
    reserved = {stem.casefold() for stem in base}
    used: set[str] = set()
    stems: dict[int, str] = {}
    for cr, stem in zip(checks, base, strict=True):
        if stem.casefold() in used:
            n = 2
            while f"{stem}_{n}".casefold() in used | reserved:
                n += 1
            stem = f"{stem}_{n}"
        used.add(stem.casefold())
        stems[id(cr)] = stem
    return stems


def _check_entry(cr: CheckResult) -> dict[str, Any]:
    """Who a ``saved_outputs`` entry is about: schema, column (when there is one) and check."""
    entry: dict[str, Any] = {"schema": cr.metadata.get("schema_name") or ""}
    column = _check_column(cr)
    if column:
        entry["column"] = column
    entry["check"] = cr.check_name
    return entry


#: Accepted values of ``get_dq_metrics_df(by=...)``.
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
        self._key_index_cache: dict[tuple[str, tuple[str, ...]], dict[Any, list[int]]] = {}
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
        tolerated = self._tolerated_check_ids()
        for schema_name in self._schema_names:
            single_checks, multi_checks = self._split_checks_by_scope(
                check_result
                for check_result in self._get_checks_for_schema(schema_name)
                if check_result.status == "FAILED" or id(check_result) in tolerated
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

    def _tolerated_check_ids(self) -> set[int]:
        """``id()`` of each check whose tolerated rows the outputs report.

        Empty unless ``fetch_tolerated_rows`` is set. A check counts when
        it PASSED within its threshold but its row query may still return
        rows, by the same rules the row-quality numbers use.
        """
        if not self._config.fetch_tolerated_rows:
            return set()
        return {
            id(selection.result)
            for selection in self._row_quality().selections
            if selection.tolerated and selection.in_scope
        }

    def _reported_rows(self, check_result: CheckResult, tolerated: set[int]) -> nw.DataFrame | None:
        """The rows the outputs report for a check, or ``None`` to leave it out.

        A FAILED check reports its failed rows. A tolerated check (see
        :meth:`_tolerated_check_ids`) reports the rows its row query returned,
        through the same fetch annotated output and the metrics use. Any
        other check is left out, without running its row query.
        """
        if check_result.status == "FAILED":
            return check_result.failed_rows
        if id(check_result) not in tolerated:
            return None
        selection = next(s for s in self._row_quality().selections if s.result is check_result)
        return self._row_quality().rows_for(selection)

    def _row_quality_report(self) -> RowQualityReport:
        """Every row-quality number of this run, computed once and cached."""
        return self._row_quality().report()

    def _get_schema_validation_breakdown(self) -> dict[str, SchemaValidationBreakdown]:
        if self._schema_validation_breakdown is not None:
            return self._schema_validation_breakdown

        self._schema_validation_breakdown = {
            schema_name: self._build_schema_breakdown(self._get_checks_for_schema(schema_name))
            for schema_name in self._schema_names
        }
        return self._schema_validation_breakdown

    @staticmethod
    def _summarize_check_statuses(check_results: Sequence[CheckResult]) -> CheckStatusSummary:
        return CheckStatusSummary(
            passed_checks=sum(check_result.status == "PASSED" for check_result in check_results),
            error_checks=sum(check_result.status == "ERROR" for check_result in check_results),
            total_checks=len(check_results),
        )

    def _build_schema_breakdown(self, schema_checks: Sequence[CheckResult]) -> SchemaValidationBreakdown:
        single_table_checks, multi_table_checks = self._split_checks_by_scope(schema_checks)
        row_level = [cr for cr in schema_checks if self._supports_row_level_output(cr)]
        overall = OverallSummary(
            **asdict(self._summarize_check_statuses(schema_checks)),
            failed_rows_approximate=sum(cr.failed_rows_count or 0 for cr in row_level) if row_level else None,
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

        tolerated = self._tolerated_check_ids()
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
                        self._reported_rows(check_result, tolerated),
                        max_rows=max_rows,
                    )

        return self

    @staticmethod
    def _print_failed_check_rows(
        check_result: CheckResult,
        df: nw.DataFrame | None,
        *,
        max_rows: int,
    ) -> None:
        target_label = get_field_label(check_result)
        rule = check_result.metadata.get("rendered_implementation")

        operator = check_result.metadata.get("operator", "")

        # A check that PASSED within its threshold is listed only under
        # fetch_tolerated_rows, labelled so it does not read as a failure.
        label = " (tolerated)" if check_result.status == "PASSED" else ""
        print(f"\n      [{check_result.check_name}]{label}")
        print(f"        Operator:   {operator}")
        print(f"        Expected:   {check_result.expected_value}")
        print(f"        Actual:     {check_result.actual_value}")

        if target_label:
            print(f"        Target:   {target_label}")
        if check_result.details:
            print(f"        Details:  {check_result.details}")
        if rule:
            print(f"        Rule:     {rule}")

        if df is None or len(df) == 0:
            print("        No failed rows returned.")
            return

        sample_size = len(df) if max_rows == -1 else min(len(df), max_rows)
        sample = df.head(sample_size).to_arrow()
        print(f"        Rows shown: {sample_size} of {len(df)}")
        print(format_ascii_table(sample))

    @staticmethod
    def _append_output_metadata(
        df: nw.DataFrame,
        check_name: str,
        tables_str: str,
        tolerated: bool | None = None,
        status: str | None = None,
    ) -> nw.DataFrame:
        """Add ``check_id`` and ``tables_in_query``, and ``tolerated`` and ``status`` unless they are ``None``."""
        if len(df) > 0:
            columns = [nw.lit(check_name).alias("check_id"), nw.lit(tables_str).alias("tables_in_query")]
            if tolerated is not None:
                columns.append(nw.lit(tolerated).alias("tolerated"))
            if status is not None:
                columns.append(nw.lit(status).alias("status"))
            return df.with_columns(*columns)

        arrow_df = df.to_arrow()
        arrow_df = arrow_df.append_column("check_id", pa.array([], type=pa.utf8())).append_column(
            "tables_in_query", pa.array([], type=pa.utf8())
        )
        if tolerated is not None:
            arrow_df = arrow_df.append_column("tolerated", pa.array([], type=pa.bool_()))
        if status is not None:
            arrow_df = arrow_df.append_column("status", pa.array([], type=pa.utf8()))
        return nw.from_native(arrow_df, eager_only=True)

    def _output_key(self, cr: CheckResult) -> str:
        label = _check_label(cr)
        return f"{label}::{cr.check_name}" if label else cr.check_name

    def get_output_dfs(self, checks: Sequence[str] | None = None, scope: str = "failed") -> dict[str, nw.DataFrame]:
        """Each check's rows, keyed by ``<schema>.<column>::<check>``.

        A schema-level check has no column, so its key is
        ``<schema>::<check>``.

        ``scope="failed"`` (default) gives the rows of each FAILED check.
        Under ``fetch_tolerated_rows=True`` a check that PASSED within its
        threshold gives its tolerated rows too, and every frame carries a
        ``tolerated`` column saying which kind it is. A FAILED check whose
        operator sets no upper limit (``mustBeGreaterThan``, ``mustBe: 5``)
        is left out, because its row query returns the good rows. This is
        what ``save(outputs=["failed_query_outputs"])`` writes.

        ``scope="all"`` gives what the row query of every row-level check
        returned, whatever its status, with a ``status`` column. It runs one
        row query per check, ignores ``fetch_tolerated_rows`` and includes
        checks with no upper limit. This is what
        ``save(outputs=["all_query_outputs"])`` writes.

        Two checks with the same name on one column, or at the schema level
        of one schema, share a key, so only the later one is returned, with
        a ``UserWarning``.
        """
        result: dict[str, nw.DataFrame] = {}
        for cr, df in self._per_check_outputs(checks, scope):
            key = self._output_key(cr)
            if key in result:
                warnings.warn(
                    f"Two checks share the key {key!r}, so get_output_dfs() returns only the later one. "
                    "Rename one of them.",
                    UserWarning,
                    stacklevel=2,
                )
            result[key] = df
        return dict(sorted(result.items()))

    def _per_check_outputs(self, checks: Sequence[str] | None, scope: str) -> list[tuple[CheckResult, nw.DataFrame]]:
        """The frames of :meth:`get_output_dfs`, one per check in run order, duplicates kept."""
        if scope not in _OUTPUT_SCOPES:
            raise ValueError(f"Unknown scope: {scope!r}. Expected one of {list(_OUTPUT_SCOPES)}.")
        checks_set = set(checks) if checks else None
        outputs: list[tuple[CheckResult, nw.DataFrame]] = []

        if scope == "all":
            for cr in self.check_results:
                if checks_set and cr.check_name not in checks_set:
                    continue
                selection = select_check(cr)
                if selection is None or not (selection.row_level or selection.reason == REASON_OPERATOR):
                    continue
                tables = get_tables_in_query(cr)
                tables_str = ", ".join(sorted(tables)) if tables else ""
                outputs.append((cr, self._append_output_metadata(cr.rows, cr.check_name, tables_str, status=cr.status)))
            return outputs

        tolerated = self._tolerated_check_ids()
        mark_tolerated = self._config.fetch_tolerated_rows
        # A FAILED check whose operator sets no upper limit matched the good
        # rows, so they are left out, as in annotated output.
        inverted = {
            id(selection.result) for selection in self._row_quality().selections if selection.reason == REASON_OPERATOR
        }
        for cr in self.check_results:
            if checks_set and cr.check_name not in checks_set:
                continue
            if id(cr) in inverted:
                continue
            rows = self._reported_rows(cr, tolerated)
            if rows is None:
                continue

            tables = get_tables_in_query(cr)
            tables_str = ", ".join(sorted(tables)) if tables else ""
            outputs.append(
                (
                    cr,
                    self._append_output_metadata(
                        rows, cr.check_name, tables_str, (id(cr) in tolerated) if mark_tolerated else None
                    ),
                )
            )
        return outputs

    def get_consolidated_output_dfs(self, checks: Sequence[str] | None = None) -> dict[str, nw.DataFrame]:
        """Group failed rows by ``tables_in_query``, deduplicating identical
        rows and combining their check IDs.

        There is one frame per table set. Checks that return different
        columns are stacked with nulls in the columns a check did not
        return. Such a row does not merge with a full row of the same
        record, so it appears once per shape. For each check's exact rows
        and columns, use :meth:`get_output_dfs` or the per-check files of
        ``save(outputs=["failed_query_outputs"])``.

        Cross-table checks are grouped here by their ``tables_in_query`` and
        appear under a composite table key (e.g. ``"table_a, table_b"``),
        regardless of column shape.  This differs from ``get_annotated_output``,
        which routes each cross-table check individually -- merging it onto its
        anchor table when the failed rows carry only that schema's columns, or
        emitting a per-check residue otherwise.  See docs/known-issues.md for
        details.

        This is the view ``save(outputs=["consolidated_query_outputs"])``
        writes. It runs no extra queries and never attributes rows, so it
        stays cheap on large tables.

        Under ``fetch_tolerated_rows=True`` it also holds the rows of
        checks that PASSED within their threshold. A group they contributed
        to gets a ``tolerated_check_ids`` column listing those checks, next
        to ``check_ids``. A row picked out by a failed check A and a
        tolerated check B has ``check_ids = "A, B"`` and
        ``tolerated_check_ids = "B"``.
        """
        return self._get_consolidated_output_dfs(checks=checks)

    def _get_consolidated_output_dfs(self, checks: Sequence[str] | None = None) -> dict[str, nw.DataFrame]:
        """Implementation of the grouped failed-rows view."""
        per_check = [df for _cr, df in self._per_check_outputs(checks, "failed") if len(df) > 0]
        if not per_check:
            return {}

        groups: dict[str, list[nw.DataFrame]] = {}
        for df in per_check:
            groups.setdefault(df["tables_in_query"][0], []).append(df)

        result: dict[str, nw.DataFrame] = {}
        for tables_key, dfs in sorted(groups.items()):
            # Columns a check did not return come out as null.
            combined = pa.concat_tables([df.to_arrow() for df in dfs], promote_options="default")
            result[tables_key or "unknown"] = self._consolidate_grouped_output(
                nw.from_native(combined, eager_only=True)
            )
        return result

    @staticmethod
    def _consolidate_grouped_output(combined: nw.DataFrame) -> nw.DataFrame:
        data_cols = [column for column in combined.columns if column not in _OUTPUT_METADATA_COLUMNS]
        arrow_table = combined.to_arrow()
        check_ids = arrow_table.column("check_id").to_pylist()
        tables = arrow_table.column("tables_in_query").to_pylist()
        tolerated_flags = arrow_table.column("tolerated").to_pylist() if "tolerated" in arrow_table.column_names else []
        # Only a group a tolerated check contributed to gets the column, so
        # the default output keeps its format.
        has_tolerated = any(tolerated_flags)

        def joined(ids: Iterable[str]) -> str:
            return ", ".join(sorted(set(ids)))

        if not data_cols:
            columns: dict[str, list[Any]] = {"check_ids": [joined(check_ids)]}
            if has_tolerated:
                columns["tolerated_check_ids"] = [
                    joined(c for c, t in zip(check_ids, tolerated_flags, strict=True) if t)
                ]
            columns["tables_in_query"] = [tables[0]]
            return nw.from_native(pa.table(columns), eager_only=True)

        first_index: dict[tuple[Any, ...], int] = {}
        check_ids_by_key: dict[tuple[Any, ...], set[str]] = {}
        tolerated_by_key: dict[tuple[Any, ...], set[str]] = {}
        for row_index, row_key in enumerate(row_keys(arrow_table, data_cols)):
            first_index.setdefault(row_key, row_index)
            check_ids_by_key.setdefault(row_key, set()).add(check_ids[row_index])
            tolerated_ids = tolerated_by_key.setdefault(row_key, set())
            if has_tolerated and tolerated_flags[row_index]:
                tolerated_ids.add(check_ids[row_index])

        # Rebuild from the original Arrow rows rather than from Python values,
        # so types such as timestamp[ns] and uint64 are kept exactly.
        indices = list(first_index.values())
        result = arrow_table.select(data_cols).take(pa.array(indices, type=pa.int64()))
        result = result.append_column(
            "check_ids", pa.array([joined(ids) for ids in check_ids_by_key.values()], type=pa.string())
        )
        if has_tolerated:
            result = result.append_column(
                "tolerated_check_ids",
                pa.array([joined(ids) for ids in tolerated_by_key.values()], type=pa.string()),
            )
        result = result.append_column("tables_in_query", pa.array([tables[i] for i in indices]))
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
        schema or the adapter cannot export it.  Annotated output then skips
        the schema and keeps its residues. In the row-quality numbers its
        ``client_lookup`` checks are then not attributable, with the reason
        ``the table could not be downloaded``. Its plain row filters are still
        counted in the data source.

        The row-quality numbers and annotated output share this cache, so a
        table is exported at most once per result.
        """
        if schema_name not in self._full_table_cache:
            result: nw.DataFrame | None = None
            get_adapter = getattr(self._multi_adapter, "get_adapter", None)
            adapter = get_adapter(schema_name) if get_adapter is not None else None
            if adapter is None:
                logger.warning("No adapter for schema %r, so its table cannot be downloaded.", schema_name)
            else:
                try:
                    arrow_table = adapter.export_table_as_arrow(schema_name)
                    result = nw.from_native(arrow_table, eager_only=True)
                except Exception as exc:  # NotImplementedError + any backend export error
                    logger.warning("Could not download the table of %r: %s", schema_name, exc)
            self._full_table_cache[schema_name] = result
        return self._full_table_cache[schema_name]

    @staticmethod
    def _concat_failures(tables: Sequence[pa.Table]) -> pa.Table:
        """Concatenate the checks' tagged rows, whose column types can differ.

        A column a check transformed (``c * 1.5`` of an integer column) keeps
        its own type. Types that cannot be promoted to one are compared as
        text instead. Those rows match no table row either way.
        """
        try:
            return pa.concat_tables(tables, promote_options="permissive")
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            pass
        types: dict[str, set[pa.DataType]] = {}
        for table in tables:
            for field in table.schema:
                types.setdefault(field.name, set()).add(field.type)
        mixed = {name for name, found in types.items() if len(found) > 1}
        as_text = []
        for table in tables:
            for name in mixed & set(table.column_names):
                index = table.schema.get_field_index(name)
                column = pa.array(
                    [None if value is None else str(value) for value in table.column(index).to_pylist()], pa.string()
                )
                table = table.set_column(index, pa.field(name, pa.string()), column)
            as_text.append(table)
        return pa.concat_tables(as_text, promote_options="permissive")

    def _table_key_index(self, schema_name: str, columns: Sequence[str]) -> dict[Any, list[int]] | None:
        """The exported table's row keys on *columns*, built once per result.

        Maps each key to the indices of the rows that have it. ``None`` when
        the table cannot be exported. Raises what ``row_keys`` raises.
        """
        full_table = self._fetch_full_table(schema_name)
        if full_table is None:
            return None
        cache_key = (schema_name, tuple(columns))
        if cache_key not in self._key_index_cache:
            self._key_index_cache[cache_key] = table_key_index(full_table.to_arrow(), columns)
        return self._key_index_cache[cache_key]

    @staticmethod
    def _is_mergeable_for_full_table(
        rows: nw.DataFrame, full_table_columns: set[str], key_columns: Sequence[str] | None = None
    ) -> bool:
        """True when a check's failed *rows* can be annotated onto the full table.

        This single gate handles both single- and cross-table checks: the
        failed-rows column set must exactly equal the anchor table's columns.
        A **cross-table** check (one that JOINs against a reference table) is
        not rejected outright. If the author shaped its row query to
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
        :func:`~vowl.validation.row_quality.match_key.has_match_key`, the same
        one the row counts use.
        """
        return has_match_key(rows.columns, full_table_columns, key_columns)

    @staticmethod
    def _check_info_item_json(
        cr: CheckResult, preset: CheckInfoPreset, *, tolerated: bool = False, truncated: bool = False
    ) -> str:
        """JSON-encode one failing check's per-row item for the given preset.

        Every preset returns a JSON **object** (a single array element), so the
        collapsed ``check_info`` column is a uniform array-of-objects regardless
        of preset.  Missing ``check_definition`` keys yield JSON ``null``; this
        never raises.

        - ``"names"``   -> ``{"check_name": ...}``
        - ``"summary"`` -> ``{check_name, dimension, tags, target}``
        - ``"full"``    -> full ``check_definition`` + ``check_name`` + ``target``

        Under ``fetch_tolerated_rows=True``, the item of a check that
        passed within its tolerance also carries ``"tolerated": true``. The
        item of a check whose failed rows ``max_failed_rows`` cut short carries
        ``"truncated": true``.
        """
        flags = {"tolerated": tolerated, "truncated": truncated}
        if preset == "names":
            obj: dict[str, Any] = {"check_name": cr.check_name}
            obj.update((name, True) for name, on in flags.items() if on)
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
        obj.update((name, True) for name, on in flags.items() if on)
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
        truncated: bool = False,
    ) -> nw.DataFrame:
        """Build a per-check residue: deduped failed rows + ``check_info`` + ``tables_in_query``.

        Residues are per-check, so ``check_info`` is a single-element JSON array
        shaped by *preset* -- the same shape the annotated table uses.  Emitting
        ``check_info`` here (rather than the legacy ``check_ids``) keeps every
        file produced by ``save(outputs=["annotated_table"])`` uniform: annotated tables
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
        check_info = self._join_check_info_items(
            [self._check_info_item_json(cr, preset, tolerated=tolerated, truncated=truncated)]
        )
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
        index: dict[Any, list[int]] | None = None,
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

        *index* is ``table_key_index(full_table, data_cols)``, when already
        built. The row-quality numbers build the same one.
        """
        marker_cols = ["check_info", *extra_cols]
        consolidated_arrow = consolidated.to_arrow()
        full_arrow = full_table.to_arrow()
        key_table = align_to_schema(consolidated_arrow, full_arrow.schema, data_cols)
        marker_values = [consolidated_arrow.column(c).to_pylist() for c in marker_cols]

        failed_map: dict[tuple[Any, ...], tuple[Any, ...]] = {}
        for i, row_key in enumerate(row_keys(key_table, data_cols)):
            failed_map[row_key] = tuple(values[i] for values in marker_values)

        if index is None:
            index = table_key_index(full_arrow, data_cols)
        outputs: dict[str, list] = {c: [None] * full_arrow.num_rows for c in marker_cols}
        annotated_rows = 0
        for row_key, match in failed_map.items():
            for position in index.get(row_key, ()):
                annotated_rows += 1
                for col_index, col_name in enumerate(marker_cols):
                    outputs[col_name][position] = match[col_index]

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
          ``"<schema>.<column>::<check_name>"``, or
          ``"<schema>::<check_name>"`` for a schema-level check.
          Empty dict when there are none.  A check with *no* rows to flag -- a
          scalar aggregation (``AVG``/``SUM``/``MIN``/``MAX``),
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
          file ``save(outputs=["annotated_table"])`` writes -- annotated
          tables and residues alike -- is read the same way. Two checks with
          the same name on one column, or at the schema level of one schema,
          share a key, so only the later residue is returned, with a
          ``UserWarning``.

          Under ``fetch_tolerated_rows=True``, rows of checks that
          passed within their tolerance are flagged too, and their
          ``check_info`` items carry ``"tolerated": true``.

          (The ``consolidated_query_outputs`` CSVs come from the grouped
          :meth:`get_consolidated_output_dfs`, which keeps its comma-joined
          ``check_ids`` column. Only annotated output uses ``check_info``.)

        Args:
            checks: Optional check-name filter.
            check_info: Preset controlling the ``check_info`` column contents
                (``"names"`` / ``"summary"`` / ``"full"``).  When ``None`` the
                config's ``annotated_check_info`` is used.

          When ``max_failed_rows`` cut a check's failed rows short, the rows
          beyond the cap may not be flagged, so they look like passing rows. A
          ``UserWarning`` says so, and the check's ``check_info`` items carry
          ``"truncated": true``. Set ``max_failed_rows=-1`` to flag every row.
        """
        annotated, residue_list = self._annotated_output(checks, check_info)
        residues: dict[str, nw.DataFrame] = {}
        for cr, df in residue_list:
            key = self._output_key(cr)
            if key in residues:
                warnings.warn(
                    f"Two checks share the key {key!r}, so get_annotated_output() returns only the "
                    "later residue. Rename one of them.",
                    UserWarning,
                    stacklevel=2,
                )
            residues[key] = df
        return {"annotated": annotated, "residues": residues}

    def _annotated_output(
        self, checks: Sequence[str] | None, check_info: CheckInfoPreset | None
    ) -> tuple[dict[str, nw.DataFrame], list[tuple[CheckResult, nw.DataFrame]]]:
        """The annotated tables, and the residues one per check in run order, duplicates kept."""
        preset = check_info if check_info is not None else self._config.annotated_check_info
        checks_set = set(checks) if checks else None
        row_quality = self._row_quality()

        # The checks whose rows are flagged: the row-quality component's
        # failed row-level checks. Inverted and table-level checks are left to
        # the summary. Passed checks join only under fetch_tolerated_rows.
        flagged = [
            selection
            for selection in attributed_checks(row_quality.selections)
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
            # The primary key the row counts attributed on, so both merge the same checks.
            key_columns = row_quality.merge_key(schema_name)
            if key_columns and not set(key_columns) <= full_table_cols:
                key_columns = None

            mergeable = [
                selection
                for selection in flagged
                if selection.schema_name == schema_name
                and self._is_mergeable_for_full_table(row_quality.rows_for(selection), full_table_cols, key_columns)
            ]

            # A capped check leaves its un-fetched failures looking like
            # passing rows. Warn, and mark its check_info items, so the table
            # is not taken as complete.
            truncated_ids = {id(selection.result) for selection in mergeable if row_quality.rows_truncated(selection)}
            for selection in mergeable:
                if id(selection.result) in truncated_ids:
                    warnings.warn(
                        f"Annotated output for schema {schema_name!r} is incomplete: max_failed_rows="
                        f"{self._config.max_failed_rows} cut short the failed rows of check "
                        f"{selection.result.check_name!r}, so its rows beyond the cap may not be flagged. "
                        f'Its check_info items carry "truncated": true. Set max_failed_rows=-1 to flag every row.',
                        UserWarning,
                        stacklevel=2,
                    )

            eligible = [selection for selection in mergeable if len(row_quality.rows_for(selection)) > 0]

            if not eligible:
                annotated[schema_name] = self._with_null_marker(full_table)
                continue

            full_schema = full_table.to_arrow().schema
            tagged_failures: list[pa.Table] = []
            for selection in eligible:
                # No .unique() here: _group_check_ids_by_row collapses duplicate
                # rows itself, and .unique() cannot hash nested column types.
                rows = self._strip_metadata_cols(row_quality.rows_for(selection))
                if key_columns:
                    rows = rows.select(key_columns)
                item = self._check_info_item_json(
                    selection.result,
                    preset,
                    tolerated=selection.tolerated,
                    truncated=id(selection.result) in truncated_ids,
                )
                # Cast to the table's types first, so checks that return a
                # column with different types can be concatenated.
                tagged = align_to_schema(rows.to_arrow(), full_schema, rows.columns)
                tagged_failures.append(tagged.append_column("check_info_item", pa.array([item] * tagged.num_rows)))
                merged_check_keys.add(self._output_key(selection.result))

            # Collapse duplicate rows into a JSON-array check_info column.
            union = self._concat_failures(tagged_failures)
            consolidated = self._group_check_ids_by_row(nw.from_native(union, eager_only=True))

            data_cols = [c for c in consolidated.columns if c != "check_info"]
            try:
                index = self._table_key_index(schema_name, data_cols)
            except Exception:
                index = None
            annotated[schema_name] = self._annotate_full_table(
                full_table,
                consolidated,
                data_cols,
                schema_name=schema_name,
                index=index,
            )

        # Step 2: residues = one entry per flagged check that was NOT merged
        # onto an annotated table, plus the other FAILED checks that returned
        # rows, except inverted ones.
        # Per-check (never grouped across checks), so a merged check can never
        # reappear and two non-mergeable checks are never folded together.
        # Each entry is row-deduped within its own check and carries the same
        # check_info column as the annotated tables (a single-element JSON
        # array) plus tables_in_query.
        candidates = [
            (
                selection.result,
                selection.tolerated,
                row_quality.rows_truncated(selection),
                row_quality.rows_for(selection),
            )
            for selection in flagged
        ]
        candidates += [
            (cr, False, cr.failed_rows_truncated, cr.failed_rows)
            for cr in self.check_results
            if cr.status == "FAILED"
            and id(cr) not in flagged_ids
            and id(cr) not in inverted_ids
            and (not checks_set or cr.check_name in checks_set)
        ]
        residues: list[tuple[CheckResult, nw.DataFrame]] = []
        for cr, tolerated, truncated, rows in candidates:
            if self._output_key(cr) in merged_check_keys:
                continue  # already annotated onto a full table -> not a residue
            if len(rows) == 0:
                continue  # no rows to emit

            tables = get_tables_in_query(cr)
            tables_str = ", ".join(sorted(tables)) if tables else ""
            residues.append(
                (
                    cr,
                    self._build_residue_with_check_info(
                        cr, rows, preset, tables_str, tolerated=tolerated, truncated=truncated
                    ),
                )
            )

        return annotated, residues

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

    def get_dq_metrics_df(self, by: str = "schema") -> nw.DataFrame:
        """Return the row-quality numbers: how many rows of each table have issues.

        Every surface reads the same cached numbers: ``print_summary``, the
        OTEL gauges and annotated output agree with this frame. See
        docs/design-considerations/checks/how-attributed-rows-work.md.

        Args:
            by: ``"schema"`` for one row per schema, ``"dimension"`` for one
                row per (schema, dimension), or ``"check"`` for one row per
                check, with the attribution method its rows took and a note on
                why it was or was not row-level or attributable.

        Columns for ``"schema"`` and ``"dimension"``: ``schema_name``,
        ``dimension`` (``"dimension"`` only), ``total_rows``, ``failed_rows``,
        ``passed_rows``, ``pass_rate`` (0 to 1),
        ``approximate``, ``checks_row_level``, ``checks_not_row_level`` and
        ``checks_not_attributable``. ``failed_rows`` is the rows of the table
        that failed, from the attributed rows of the row-level checks. A counted
        check that is not attributable adds nothing and is counted in
        ``checks_not_attributable``. A missing value (null) means the number is
        unavailable, for example a dimension with no attributable checks.

        Columns for ``"check"``: ``schema_name``, ``check_name``,
        ``dimension``, ``status``, ``row_level``, ``attribution_method``, ``attribution_note``,
        ``scalar_count``, ``attributed_rows`` and ``approximate``. ``scalar_count`` is the count the check's own query
        returned. ``attributed_rows`` is the rows of the table the check
        caught, which differs when the query does not return each such row
        once, for example under ``DISTINCT``. It is null when the check is not
        attributed. A passed check is not attributed unless
        ``fetch_tolerated_rows`` is set.

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
            "scalar_count": pa.int64(),
            "attributed_rows": pa.int64(),
            "failed_rows": pa.int64(),
            "passed_rows": pa.int64(),
            "pass_rate": pa.float64(),
            "approximate": pa.bool_(),
            "row_level": pa.bool_(),
            "checks_row_level": pa.int64(),
            "checks_not_row_level": pa.int64(),
            "checks_not_attributable": pa.int64(),
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

        The schema and dimension row gauges use the same row counts as
        :meth:`get_dq_metrics_df`. If they were not computed yet, they are
        computed now, which can download a table. The ``vowl.validate`` and
        ``vowl.check`` spans carry ``row.approximate``, which says whether the
        row numbers are approximate. The metrics carry it as the
        ``vowl.<level>.row.approximate`` gauges.

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
                the same id ``save()`` writes into ``dq_metrics.json``.
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
        outputs: Sequence[SaveOutput] | None = None,
        include_check_definition: bool = False,
        include_contract_definition: bool = False,
        check_info: CheckInfoPreset | None = None,
        filesystem: Any | None = None,
        output_mode: str | None = None,
    ) -> ValidationResult:
        """Write ``<prefix>_check_results.csv``, ``<prefix>_summary.json`` and the chosen outputs.

        The two summary files are always written. ``outputs`` picks the rest.
        When it is ``None`` the config's ``outputs`` is used, which defaults
        to every output except ``"failed_query_outputs"``, whose files
        ``"all_query_outputs"`` already writes:

        - ``"failed_query_outputs"`` -- one CSV per FAILED check,
          ``<prefix>_checks/<schema>__<column>__<check>.csv``, with ``check_id``,
          ``tables_in_query`` and ``tolerated`` columns (see
          :meth:`get_output_dfs`). Under ``fetch_tolerated_rows=True`` the
          checks that passed within their threshold get a file too. Checks
          whose operator sets no upper limit are left out.
        - ``"all_query_outputs"`` -- the same files for every row-level check,
          whatever its status, with a ``status`` column
          (``get_output_dfs(scope="all")``). It runs one row query per check.
          It cannot be combined with ``"failed_query_outputs"``.
        - ``"consolidated_query_outputs"`` -- the failed rows grouped per
          table set, ``<prefix>_<tables>.csv``, with a comma-joined
          ``check_ids`` column (see :meth:`get_consolidated_output_dfs`).
          Runs no extra queries and never exports a table.
        - ``"annotated_table"`` -- each in-scope table with a ``check_info``
          column, ``<prefix>_<schema>_annotated.csv``, plus one
          ``<prefix>_<schema>__<column>__<check>_residue.csv`` per check that cannot be
          marked on its table (see :meth:`get_annotated_output`). Exports
          the tables and attributes the rows.
        - ``"dq_metrics"`` -- ``<prefix>_dq_metrics.json`` (see
          :meth:`get_dq_metrics`). It shares the table export and the row
          attribution with ``"annotated_table"``.

        ``outputs=[]`` writes only the two summary files. ``summary.json``
        lists the files each output wrote under ``saved_outputs``. A check
        with no rows writes no file and is listed with ``"rows": 0``.

        Per-check and residue file names come from the schema, column and
        check names, each cleaned on its own and joined with ``__``. A
        schema-level check has no column part. When two checks clean to the
        same file name, compared without case, the later one in the contract
        gets ``_2``, the next ``_3`` and so on, skipping any name another
        check already has.

        ``check_info`` shapes the annotated ``check_info`` column. Passing it
        without ``"annotated_table"`` warns.

        ``output_dir`` is a local folder or a URI such as
        ``s3://bucket/dq-results/run-1/``. URIs (``s3://``, ``gs://``,
        ``abfs://``, ``hdfs://``, ``file://``) are written through pyarrow's
        filesystems, which find credentials the usual way for each cloud. Pass
        ``filesystem=`` (a ``pyarrow.fs.FileSystem``) to set one up yourself,
        for example an S3-compatible store with a custom endpoint. ``output_dir``
        is then a path inside that filesystem.

        ``output_mode`` is deprecated. Its v0.0.6 names still work with a
        ``FutureWarning``, mapped to outputs: ``"failed_rows"`` to
        ``["consolidated_query_outputs"]``, ``"annotated"`` to
        ``["annotated_table", "dq_metrics"]`` and ``"both"`` to all three.
        It will be removed in a future release.
        """
        chosen = set(_resolve_outputs(outputs, output_mode, stacklevel=2, default=self._config.outputs))
        if check_info is not None and "annotated_table" not in chosen:
            warnings.warn(
                "check_info only shapes the annotated_table output, which this save() does not write.",
                UserWarning,
                stacklevel=2,
            )

        query_scope = "all" if "all_query_outputs" in chosen else "failed" if "failed_query_outputs" in chosen else None
        per_check = self._per_check_outputs(None, query_scope) if query_scope else []
        annotated: dict[str, nw.DataFrame] = {}
        residues: list[tuple[CheckResult, nw.DataFrame]] = []
        if "annotated_table" in chosen:
            annotated, residues = self._annotated_output(None, check_info)
        file_stems = _check_file_stems(self.check_results)

        target = OutputDir(output_dir, filesystem)

        # Sanitize the caller-supplied prefix so it cannot traverse directories.
        prefix = _safe_filename_component(prefix, fallback="vowl_results")

        saved_files = [
            target.write_csv(
                f"{prefix}_check_results.csv",
                self.get_check_results_df(
                    include_check_definition=include_check_definition,
                    include_contract_definition=include_contract_definition,
                ).to_arrow(),
            )
        ]
        saved_outputs: dict[str, list[dict[str, Any]]] = {}

        if query_scope:
            listed = saved_outputs.setdefault(f"{query_scope}_query_outputs", [])
            checks_folder = f"{prefix}_checks"
            checks_dir = target.subdir(checks_folder) if any(len(df) > 0 for _cr, df in per_check) else None
            for cr, df in per_check:
                entry = {**_check_entry(cr), "rows": len(df)}
                if checks_dir is not None and len(df) > 0:
                    name = f"{file_stems[id(cr)]}.csv"
                    saved_files.append(checks_dir.write_csv(name, df.to_arrow()))
                    entry["file"] = f"{checks_folder}/{name}"
                listed.append(entry)

        if "consolidated_query_outputs" in chosen:
            listed = saved_outputs.setdefault("consolidated_query_outputs", [])
            for table_key, df in self._get_consolidated_output_dfs().items():
                name = f"{prefix}_{_safe_filename_component(table_key.replace(', ', '_').replace(' ', '_'))}.csv"
                saved_files.append(target.write_csv(name, df.to_arrow()))
                listed.append({"tables": table_key, "rows": len(df), "file": name})

        if "annotated_table" in chosen:
            listed = saved_outputs.setdefault("annotated_table", [])
            for schema, df in annotated.items():
                name = f"{prefix}_{_safe_filename_component(schema)}_annotated.csv"
                saved_files.append(target.write_csv(name, df.to_arrow()))
                listed.append({"schema": schema, "rows": len(df), "file": name})
            for cr, df in residues:
                name = f"{prefix}_{file_stems[id(cr)]}_residue.csv"
                saved_files.append(target.write_csv(name, df.to_arrow()))
                listed.append({**_check_entry(cr), "rows": len(df), "file": name})

        if "dq_metrics" in chosen:
            name = f"{prefix}_dq_metrics.json"
            saved_files.append(target.write_text(name, json.dumps(self.get_dq_metrics(), indent=2, default=str)))
            saved_outputs["dq_metrics"] = [{"file": name}]

        summary = {**self.summary, "saved_outputs": saved_outputs}
        saved_files.append(target.write_text(f"{prefix}_summary.json", json.dumps(summary, indent=2, default=str)))

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
        """Write *df* to *filepath*, a local path or a URI (see :meth:`save`).

        .. deprecated::
            Write the frame with its own library instead, for example
            ``pyarrow.parquet.write_table(df.to_arrow(), "s3://...", filesystem=fs)``.
            This helper will be removed in a future release.
        """
        warnings.warn(
            "ValidationResult.save_dataframe() is deprecated and will be removed in a "
            "future release. Write the frame with its own library instead, for example "
            "pyarrow.parquet.write_table(df.to_arrow(), path, filesystem=fs), "
            "df.to_native().to_parquet(path) or df.to_native().write_parquet(path).",
            DeprecationWarning,
            stacklevel=2,
        )
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
