"""The row-quality component: how many rows of each table have issues.

One component computes the numbers lazily and caches them on the
``ValidationResult``. ``print_summary``, the OTEL gauges,
``get_row_quality_df`` and annotated output all read from it, so every surface
reports the same counts of attributed rows, every copy counted. The process is described in
``design/row-quality-statistics.md``:

1. Total rows per schema.
2. Select the row-level checks (:mod:`.selection`).
3. Collect each check's attributed rows, copies included, by one of four routes:

   - ``server_predicate``: certified row filters (:mod:`.certify`), counted
     inside the data source (:mod:`.pushdown`).
   - ``server_lookup``: other checks on a tested source's own connection,
     counted in SQL as the table rows the check's failed rows are attributed to.
   - ``client_lookup``: other checks, whose fetched failed rows are attributed
     to the exported table, so each row's copies come from the table.
   - ``server_scalar``: the check's scalar count, from its count query. Nothing
     is run, but a sum of these counts cannot see overlapping rows. Used only
     under ``ValidationConfig(row_counts="scalar")``, where every failed
     row-level check takes it.

   A check that is not a certified row filter uses ``server_lookup`` on a
   tested source and ``client_lookup`` elsewhere. A check whose rows cannot be
   attributed this run is not attributable: it adds nothing to the row counts,
   which become approximate when it caught rows in scope. A passed check is
   out of scope unless ``attribute_tolerated_rows`` is set, and adds nothing.

4. Merge the routes into one entry per attributed row (:mod:`.merge`).
5. Roll up per schema, dimension and check (:mod:`.rollup`).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import narwhals as nw
import pyarrow as pa
import sqlglot
from sqlglot import exp

from ...contracts.sql_transforms import apply_filters
from ...executors.base import CheckResult
from ...executors.security import to_table_expression
from . import pushdown as _pushdown
from .certify import certify_check
from .keys import key_is_exact, primary_key_columns
from .match_key import METADATA_COLUMNS, has_match_key, returns_table_values
from .merge import merge_onto_table, pushdown_entries
from .pushdown import (
    Branch,
    KeySpec,
    PushdownOutcome,
    PushdownRunner,
    column_probe_statement,
    count_statement,
    duplicate_key_statement,
    lift_scan,
    preflight_statement,
)
from .rollup import (
    CheckRowQuality,
    CheckState,
    DimensionRowQuality,
    RowQualityReport,
    SchemaRowQuality,
    attributed_rows_from_entries,
    bucket_rows,
    roll_up_bucket,
)
from .selection import (
    REASON_COUNTS_VALUES,
    REASON_CROSS_SOURCE,
    REASON_MATCH_KEYS_FAILED,
    REASON_NO_EXPORT,
    REASON_NO_FETCH,
    REASON_NO_MATCH_KEY,
    REASON_NO_PUSHDOWN,
    REASON_PASSED_NOT_ATTRIBUTED,
    REASON_PK_NOT_UNIQUE,
    REASON_PK_UNCHECKED,
    REASON_QUERY_FAILED,
    REASON_TRUNCATED,
    REASON_UNATTRIBUTED,
    CheckSelection,
    attributed_checks,
    resolve_check_dimension,
    select,
    uncertified_reason,
)

if TYPE_CHECKING:
    from ..result import ValidationResult

logger = logging.getLogger(__name__)

__all__ = [
    "CheckRowQuality",
    "CheckSelection",
    "DimensionRowQuality",
    "RowQuality",
    "RowQualityReport",
    "SchemaRowQuality",
    "attributed_checks",
    "resolve_check_dimension",
    "select",
]

ROUTE_SERVER_PREDICATE = "server_predicate"
ROUTE_SERVER_LOOKUP = "server_lookup"
ROUTE_CLIENT_LOOKUP = "client_lookup"
ROUTE_SERVER_SCALAR = "server_scalar"

# The outcome of the duplicate-key probe of a declared primary key.
PK_UNIQUE = "unique"
PK_DUPLICATED = "duplicated"
PK_UNCHECKED = "unchecked"


def strip_metadata_columns(table: pa.Table) -> pa.Table:
    """Drop the output-metadata columns vowl adds to failed rows."""
    keep = [name for name in table.column_names if name not in METADATA_COLUMNS]
    return table.select(keep) if len(keep) != table.num_columns else table


@dataclass
class _Work:
    """The in-progress state of one check of a schema."""

    selection: CheckSelection
    check_id: int
    row_level: bool
    # Its rows belong in the row counts.
    in_scope: bool = True
    # Its attributed rows are in the row counts.
    attributable: bool = True
    route: str = ""
    reason: str = ""
    approximate: bool = False
    certified: bool = False
    # Runs on the table's own connection, which can run the pushdown.
    local: bool = False
    # Its select list returns values other than the table's own columns.
    transformed: bool = False
    columns: list[str] | None = None
    rows: pa.Table | None = None
    attributed_rows: int | None = None
    # Count by matching the rows onto the exported table.
    want_full: bool = False

    @property
    def result(self) -> CheckResult:
        return self.selection.result

    @property
    def row_source(self) -> Any:
        return getattr(self.selection.result, "row_source", None)

    @property
    def needs_rows(self) -> bool:
        count = self.selection.scalar_count
        return self.row_level and self.in_scope and (count is None or count > 0)

    def not_attributable(self, reason: str, *, approximate: bool = False) -> None:
        """Leave the check out of the row counts. It stays row-level."""
        self.attributable = False
        self.route = ""
        self.rows, self.want_full = None, False
        self.attributed_rows = None
        if reason:
            self.reason = reason
        self.approximate = approximate


def _safe(call: Any, *args: Any) -> Any:
    try:
        return call(*args)
    except Exception:
        return None


def _schema_properties(result: ValidationResult, schema_name: str) -> list[dict[str, Any]]:
    contract_data = getattr(result.contract, "contract_data", None)
    entries = contract_data.get("schema", []) if isinstance(contract_data, dict) else []
    for entry in entries:
        if isinstance(entry, dict) and entry.get("name") == schema_name:
            return [prop for prop in entry.get("properties") or [] if isinstance(prop, dict)]
    return []


def _arrow_schema(column_types: dict[str, Any] | None) -> pa.Schema | None:
    if not column_types:
        return None
    try:
        return pa.schema([(name, dtype.to_pyarrow()) for name, dtype in column_types.items()])
    except Exception:
        return None


class RowQuality:
    """Computes the :class:`RowQualityReport` of a validation result, once."""

    def __init__(self, result: ValidationResult) -> None:
        self._result = result
        self._config = result._config
        # Passed checks are attributed only on the attributed route.
        attribute_tolerated = self._config.attribute_tolerated_rows and self._config.row_counts == "attributed"
        self._selections = select(result.check_results, attribute_tolerated)
        self._report: RowQualityReport | None = None
        self._by_check: dict[int, CheckRowQuality] = {}
        self._tolerated_rows: dict[int, nw.DataFrame] = {}
        self._merge_keys: dict[str, list[str]] = {}
        # Schemas counted without a query, whose key is found only when asked.
        self._key_probes: dict[str, _SchemaComputation] = {}

    @property
    def selections(self) -> list[CheckSelection]:
        """The step 2 verdict for every check anchored to a schema."""
        return self._selections

    def report(self) -> RowQualityReport:
        """The row-quality report, computed on first use."""
        if self._report is None:
            self._report = self._compute()
        return self._report

    def check_rows(self) -> dict[int, CheckRowQuality]:
        """Each check's entry of :meth:`report`, keyed by ``id(check)``."""
        self.report()
        return self._by_check

    def merge_key(self, schema_name: str) -> list[str] | None:
        """The primary key a schema's rows are matched on, or ``None`` for all columns.

        Set whenever the schema declares a primary key that is shorter than
        its columns, the data source can run the pushdown, and the key was
        found unique. Annotated output matches on the same key, so both merge
        the same checks.
        """
        self.report()
        probe = self._key_probes.pop(schema_name, None)
        if probe is not None:
            found = probe.annotation_key()
            if found is not None:
                self._merge_keys[schema_name] = found
        key = self._merge_keys.get(schema_name)
        return list(key) if key is not None else None

    def rows_for(self, selection: CheckSelection) -> nw.DataFrame:
        """A row-level check's failing rows, as annotated output flags them.

        FAILED checks use their own lazily fetched failed rows. A passed
        check has no fetcher of its own, so its rows are fetched through its
        row source, once.
        """
        check_result = selection.result
        if check_result.status != "PASSED":
            return check_result.failed_rows
        key = id(check_result)
        if key not in self._tolerated_rows:
            fetch = getattr(getattr(check_result, "row_source", None), "fetch", None)
            fetched = _safe(fetch) if fetch is not None else None
            self._tolerated_rows[key] = (
                fetched if fetched is not None else nw.from_native(pa.table({}), eager_only=True)
            )
        return self._tolerated_rows[key]

    def rows_truncated(self, selection: CheckSelection) -> bool:
        """Whether ``max_failed_rows`` cut :meth:`rows_for` short (fetches the rows first)."""
        self.rows_for(selection)
        if selection.result.status != "PASSED":
            return selection.result.failed_rows_truncated
        fetch = getattr(getattr(selection.result, "row_source", None), "fetch", None)
        return bool(getattr(fetch, "truncated", False))

    # ------------------------------------------------------------------
    # Computation
    # ------------------------------------------------------------------

    def _compute(self) -> RowQualityReport:
        result = self._result
        by_schema: dict[str, list[CheckSelection]] = {name: [] for name in result._schema_names}
        for selection in self._selections:
            if selection.schema_name in by_schema:
                by_schema[selection.schema_name].append(selection)

        if self._config.row_counts == "off":
            return self._disabled_report(by_schema)

        schemas: list[SchemaRowQuality] = []
        dimensions: list[DimensionRowQuality] = []
        checks: list[CheckRowQuality] = []
        for schema_name, selections in by_schema.items():
            schema, schema_dimensions, schema_checks = _SchemaComputation(self, schema_name, selections).run()
            for selection, entry in zip(selections, schema_checks, strict=True):
                self._by_check[id(selection.result)] = entry
            schemas.append(schema)
            dimensions.extend(schema_dimensions)
            checks.extend(schema_checks)

        return RowQualityReport(schemas=schemas, dimensions=dimensions, checks=checks)

    def _disabled_report(self, by_schema: dict[str, list[CheckSelection]]) -> RowQualityReport:
        schemas = [
            SchemaRowQuality(
                schema_name=schema_name,
                total_rows=None,
                failed_rows=None,
                passed_rows=None,
                pass_rate=None,
                approximate=True,
                checks_row_level=sum(item.row_level for item in selections),
                checks_not_row_level=sum(not item.row_level for item in selections),
                # No check is attributed with row counts off.
                checks_not_attributable=sum(item.row_level and item.in_scope for item in selections),
            )
            for schema_name, selections in by_schema.items()
        ]
        checks = [
            CheckRowQuality(
                schema_name=item.schema_name,
                check_name=item.result.check_name,
                dimension=item.dimension,
                status=item.result.status,
                row_level=item.row_level,
                route="",
                reason=item.reason,
                scalar_count=item.scalar_count,
                attributed_rows=None,
                approximate=True,
            )
            for selections in by_schema.values()
            for item in selections
        ]
        selections = [item for items in by_schema.values() for item in items]
        for selection, entry in zip(selections, checks, strict=True):
            self._by_check[id(selection.result)] = entry
        return RowQualityReport(schemas=schemas, checks=checks, enabled=False)


class _SchemaComputation:
    """Steps 1 to 5 for one schema."""

    def __init__(self, component: RowQuality, schema_name: str, selections: Sequence[CheckSelection]) -> None:
        self._component = component
        self._result = component._result
        self._schema = schema_name
        self._scalar_only: bool = component._config.row_counts == "scalar"
        self._work = [
            _Work(
                selection=selection,
                check_id=index,
                row_level=selection.row_level,
                in_scope=selection.in_scope,
                reason=selection.reason,
            )
            for index, selection in enumerate(selections)
        ]
        for work in self._work:
            if not work.row_level:
                continue
            if not work.in_scope:
                # A passed check adds nothing, so it is never attributed.
                work.not_attributable(REASON_PASSED_NOT_ATTRIBUTED)
            elif not work.needs_rows:
                # A row-level check that matched no rows adds nothing.
                work.attributed_rows = 0
        self._adapter = _safe(
            getattr(self._result._multi_adapter, "get_adapter", None) or (lambda _: None), schema_name
        )
        # Empty when unknown.
        self._dialect: str = _safe(getattr(self._adapter, "get_sql_dialect", None) or (lambda: None)) or ""
        self._column_types: dict[str, Any] = self._load_column_types() or {}
        properties = _schema_properties(self._result, schema_name)
        if self._column_types:
            self._columns = list(self._column_types)
        else:
            self._columns = [prop["name"] for prop in properties if prop.get("name")]
        self._primary_key = [name for name in primary_key_columns(properties) if name in self._columns]
        self._anchor_sql: str = self._filtered_anchor() or ""
        self._total_approximate = False
        # The duplicate-key probe's verdict: None until it runs, then one of
        # PK_UNIQUE, PK_DUPLICATED or PK_UNCHECKED.
        self._pk_status: str | None = None
        self._pk_duplicates: int | None = None
        # The exported table, once a check is matched onto it.
        self._export: pa.Table | None = None

    # -- setup ---------------------------------------------------------

    def _load_column_types(self) -> dict[str, Any] | None:
        getter = getattr(self._adapter, "get_column_types", None)
        if getter is None:
            return None
        try:
            types = getter(self._schema)
        except Exception:
            return None
        return dict(types) if types else None

    def _filtered_anchor(self) -> str | None:
        if not self._dialect:
            return None
        try:
            query = sqlglot.select(exp.Star()).from_(to_table_expression(self._schema)).sql(dialect=self._dialect)
            filters = getattr(self._adapter, "filter_conditions", None)
            return apply_filters(query, self._dialect, filters) if filters else query
        except Exception:
            return None

    def _run_query(self, sql: str) -> pa.Table:
        return self._adapter.run_arrow_query(sql)

    # -- steps ---------------------------------------------------------

    def run(self) -> tuple[SchemaRowQuality, list[DimensionRowQuality], list[CheckRowQuality]]:
        pending = [work for work in self._work if work.needs_rows]
        if self._scalar_only:
            return self._run_scalars(pending)
        spec = self._full_spec() if pending else None
        pushdown_ok = spec is not None and self._preflight(spec)

        for work in pending:
            self._assign_route(work, pushdown_ok)
        for work in pending:
            if work.route == ROUTE_SERVER_LOOKUP:
                self._probe_columns(work)
            elif work.route == ROUTE_CLIENT_LOOKUP:
                self._fetch(work)
        self._export_table(pending)

        key_columns = self._choose_key(pushdown_ok)
        if len(key_columns) < len(self._columns):
            self._component._merge_keys[self._schema] = list(key_columns)
        for work in pending:
            self._check_match_key(work, key_columns)
        index = self._export_index(pending, key_columns) if self._export is not None else None
        if index is not None:
            return self._run_onto_table(pending, spec, key_columns, index)

        # Without the table every client_lookup check is not attributable.
        engine_work = [
            work for work in pending if work.row_level and work.route in (ROUTE_SERVER_PREDICATE, ROUTE_SERVER_LOOKUP)
        ]
        outcome: PushdownOutcome | None = None
        if engine_work and spec is not None:
            spec = KeySpec(
                dialect=spec.dialect,
                anchor_sql=spec.anchor_sql,
                columns=list(key_columns),
                dtypes=[self._column_types.get(name) for name in key_columns],
            )
            runner = PushdownRunner(self._run_query, spec)
            branches = [self._branch(work) for work in engine_work]
            if self._keyless(engine_work):
                outcome = runner.run_mask_histogram(branches)
                if outcome is not None:
                    for work in engine_work:
                        work.approximate = False
            if outcome is None:
                outcome = runner.run(branches, histogram=True)
            for work in engine_work:
                if work.check_id in outcome.dropped:
                    self._leave_unattributed(work, REASON_QUERY_FAILED)
            self._flag_unmatched(runner, engine_work)

        entries = pushdown_entries(outcome)
        engine_ids = [work.check_id for work in engine_work if work.row_level and work.attributable]
        for check_id, rows in attributed_rows_from_entries(entries, engine_ids).items():
            self._work[check_id].attributed_rows = rows

        total_rows = self._total_rows(outcome)
        return self._roll_up(entries, total_rows, False)

    def _run_scalars(
        self, pending: Sequence[_Work]
    ) -> tuple[SchemaRowQuality, list[DimensionRowQuality], list[CheckRowQuality]]:
        """Count every failed check from its scalar count, as ``row_counts="scalar"`` asks. No query runs.

        Every row-level check is then not attributable.
        """
        pending_ids = {work.check_id for work in pending}
        for work in self._work:
            if work.check_id in pending_ids:
                ok, rule = self._certify(work)
                self._leave_unattributed(work, "" if ok else rule)
            elif work.row_level and work.in_scope:
                self._leave_unattributed(work)
        # Annotated output can still ask for the primary key to match on.
        self._component._key_probes[self._schema] = self
        return self._roll_up([], self._recorded_total(), False)

    def annotation_key(self) -> list[str] | None:
        """The primary key annotated output matches on, when no count ran to find it."""
        spec = self._full_spec()
        key = self._choose_key(spec is not None and self._preflight(spec))
        return key if len(key) < len(self._columns) else None

    # -- routes ----------------------------------------------------------

    def _certify(self, work: _Work) -> tuple[bool, str]:
        """Certify *work* as a plain row filter. Returns ``(ok, reason)``."""
        row_source = work.row_source
        if row_source is None:
            work.transformed = True
            return False, REASON_NO_PUSHDOWN
        rendered = None
        if not row_source.filter_conditions:
            # The queries the check ran are the unfiltered ones.
            rendered = (row_source.row_query, work.result.metadata.get("rendered_implementation"))
        ok, rule = certify_check(
            row_source.check_ref, self._schema, row_source.dialect, row_source.use_try_cast, rendered=rendered
        )
        work.certified = ok
        work.transformed = not returns_table_values(row_source.row_query, row_source.dialect)
        return ok, "" if ok else uncertified_reason(rule)

    def _assign_route(self, work: _Work, pushdown_ok: bool) -> None:
        ok, reason = self._certify(work)
        row_source = work.row_source
        if row_source is None:
            work.route, work.reason = ROUTE_CLIENT_LOOKUP, reason
            return
        local = pushdown_ok and row_source.dialect == self._dialect and bool(row_source.row_query)
        work.local = local and not row_source.cross_source
        if not work.local:
            work.route = ROUTE_CLIENT_LOOKUP
            work.reason = REASON_CROSS_SOURCE if row_source.cross_source else REASON_NO_PUSHDOWN
            return
        if ok:
            work.route = ROUTE_SERVER_PREDICATE
            work.columns = list(self._columns)
            # A dialect without a key entry groups on plain columns. The
            # keyless mask histogram clears this when it can count the schema.
            work.approximate = not key_is_exact(self._dialect)
        elif key_is_exact(self._dialect):
            work.route, work.reason = ROUTE_SERVER_LOOKUP, reason
        else:
            work.route, work.reason = ROUTE_CLIENT_LOOKUP, reason

    def _leave_unattributed(self, work: _Work, reason: str = "") -> None:
        """Leave *work* out of the attributed rows, explaining why.

        Under ``row_counts="scalar"`` it is counted from its scalar
        count on server_scalar, approximate unless it is a plain row filter or a
        zero count. Otherwise it is not attributable and adds nothing to the row counts.
        """
        # One value can sit on many rows, so a count of values is a lower bound.
        counts_values = work.result.metadata.get("aggregation_type") == "count_distinct"
        work.not_attributable(reason or (REASON_COUNTS_VALUES if counts_values else ""))
        if not self._scalar_only:
            return
        work.route = ROUTE_SERVER_SCALAR
        count = work.selection.scalar_count
        work.approximate = count is None or (count > 0 and (counts_values or not work.certified))

    def _leave_lookup_unattributed(self, work: _Work, reason: str) -> None:
        """Mark a client_lookup check whose rows cannot be attributed to the table as not attributable, explaining why.

        A local check on a tested source already takes server_lookup, so
        client_lookup only holds checks that server_lookup cannot count. A
        server_predicate check that was to be matched stays where it is.
        """
        work.want_full = False
        if work.route != ROUTE_CLIENT_LOOKUP:
            return
        self._leave_unattributed(work, reason)

    def _probe_columns(self, work: _Work) -> None:
        query = work.row_source.row_query
        try:
            table = self._run_query(column_probe_statement(query, self._dialect))
        except Exception:
            self._leave_unattributed(work, REASON_QUERY_FAILED)
            return
        work.columns = [name for name in table.column_names if name not in METADATA_COLUMNS]

    def _fetch(self, work: _Work) -> None:
        """Fetch a client_lookup check's rows to match onto the exported table."""
        selection = work.selection
        table = _safe(lambda: strip_metadata_columns(self._fetched_frame(work).to_arrow()))
        if table is None or table.num_columns == 0:
            self._leave_lookup_unattributed(work, REASON_NO_FETCH)
            return
        work.columns = list(table.column_names)
        if self._component.rows_truncated(selection):
            self._leave_lookup_unattributed(work, REASON_TRUNCATED)
            return
        work.rows = table
        work.want_full = True

    def _fetched_frame(self, work: _Work) -> nw.DataFrame:
        return self._component.rows_for(work.selection)

    # -- the full-table match --------------------------------------------

    def _fetch_for_match(self, work: _Work) -> None:
        """Fetch a server_predicate check's rows to match onto the table.

        A check whose rows cannot be fetched whole stays on server_predicate.
        """
        table = _safe(lambda: strip_metadata_columns(self._fetched_frame(work).to_arrow()))
        if table is None or table.num_columns == 0 or self._component.rows_truncated(work.selection):
            return
        work.rows = table
        work.columns = list(table.column_names)
        work.want_full = True

    def _export_table(self, pending: Sequence[_Work]) -> None:
        """Export the table once, when a check is to be matched onto it."""
        wanted = [work for work in pending if work.want_full and work.row_level and work.rows is not None]
        if not wanted:
            return
        frame = _safe(self._result._fetch_full_table, self._schema)
        table = _safe(lambda: frame.to_arrow()) if frame is not None else None
        if table is None:
            for work in wanted:
                self._leave_lookup_unattributed(work, REASON_NO_EXPORT)
            return
        self._export = table
        # Match on the exported columns, as annotated output does.
        self._columns = list(table.column_names)
        properties = _schema_properties(self._result, self._schema)
        self._primary_key = [name for name in primary_key_columns(properties) if name in self._columns]
        for work in pending:
            if work.route == ROUTE_SERVER_PREDICATE:
                work.columns = list(self._columns)
        if not key_is_exact(self._dialect):
            # Without a key entry the pushdown groups on plain columns, which
            # can merge distinct values, and the keyless mask histogram cannot
            # place the matched checks. The table is here, so match these too.
            for work in pending:
                if work.row_level and work.route == ROUTE_SERVER_PREDICATE:
                    self._fetch_for_match(work)

    def _export_index(self, pending: Sequence[_Work], key_columns: Sequence[str]) -> dict[Any, list[int]] | None:
        try:
            index = self._result._table_key_index(self._schema, key_columns)
        except Exception as exc:
            logger.debug("Rows of %r could not be keyed: %s", self._schema, exc)
            index = None
        if index is None:
            self._export = None
            for work in pending:
                if work.want_full:
                    self._leave_lookup_unattributed(work, REASON_MATCH_KEYS_FAILED)
        return index

    def _run_onto_table(
        self,
        pending: Sequence[_Work],
        spec: KeySpec | None,
        key_columns: Sequence[str],
        index: dict[Any, list[int]],
    ) -> tuple[SchemaRowQuality, list[DimensionRowQuality], list[CheckRowQuality]]:
        """Count the schema on the exported table: each check marks its rows there."""
        table = self._export
        assert table is not None
        engine_work = [
            work
            for work in pending
            if work.row_level and not work.want_full and work.route in (ROUTE_SERVER_PREDICATE, ROUTE_SERVER_LOOKUP)
        ]
        matched_work = [work for work in pending if work.row_level and work.want_full and work.rows is not None]

        outcome: PushdownOutcome | None = None
        if engine_work and spec is not None:
            spec = KeySpec(
                dialect=spec.dialect,
                anchor_sql=spec.anchor_sql,
                columns=list(key_columns),
                dtypes=[self._column_types.get(name) for name in key_columns],
                with_values=True,
            )
            runner = PushdownRunner(self._run_query, spec)
            outcome = runner.run([self._branch(work) for work in engine_work], histogram=False)
            for work in engine_work:
                if work.check_id in outcome.dropped:
                    self._leave_unattributed(work, REASON_QUERY_FAILED)
            self._flag_unmatched(runner, engine_work)

        fetched = [(work.check_id, work.rows) for work in matched_work if work.row_level and work.attributable]
        entries, merge_approximate, matched = merge_onto_table(outcome, fetched, key_columns, table, index)

        engine_ids = [work.check_id for work in engine_work if work.row_level and work.attributable]
        for check_id, rows in attributed_rows_from_entries(entries, engine_ids).items():
            self._work[check_id].attributed_rows = rows
        for work in matched_work:
            if not (work.row_level and work.attributable):
                continue
            match = matched.get(work.check_id)
            if match is None:
                # The pushdown already ran, so server_lookup is too late.
                self._leave_unattributed(work, REASON_MATCH_KEYS_FAILED)
                continue
            work.attributed_rows, missing = match
            work.route = ROUTE_CLIENT_LOOKUP
            work.approximate = missing > 0
            if missing:
                work.reason = REASON_UNATTRIBUTED

        total_rows = outcome.total_rows if outcome is not None and outcome.total_rows is not None else table.num_rows
        self._total_approximate = False
        return self._roll_up(entries, total_rows, merge_approximate)

    def _flag_unmatched(self, runner: PushdownRunner, engine_work: Sequence[_Work]) -> None:
        """Mark a server_lookup check approximate when some of its failed keys match no table row.

        server_lookup counts the table rows that hold a failed row's key, so a
        check that returns changed values (``c * 1.5``, ``upper(s)``) finds
        fewer rows than it failed. This is the same test as client_lookup:
        per distinct key, on the same key expressions. A changed value that
        equals another row's key matches that row and is not detected, which
        is why such checks are also marked by their select list.
        """
        works = [work for work in engine_work if work.row_level and work.route == ROUTE_SERVER_LOOKUP]
        if not works:
            return
        unmatched = runner.count_unmatched([self._branch(work) for work in works])
        for work in works:
            count = unmatched.get(work.check_id)
            if count is None:
                # Not known whether every key matched.
                work.approximate = True
            elif count:
                work.approximate, work.reason = True, REASON_UNATTRIBUTED

    def _full_spec(self) -> KeySpec | None:
        if not (self._adapter and self._dialect and self._column_types and self._anchor_sql):
            return None
        spec = KeySpec(
            dialect=self._dialect,
            anchor_sql=self._anchor_sql,
            columns=list(self._columns),
            dtypes=[self._column_types.get(name) for name in self._columns],
        )
        return spec if spec.can_push_down() else None

    def _preflight(self, spec: KeySpec) -> bool:
        try:
            self._run_query(preflight_statement(spec))
        except NotImplementedError:
            return False
        except Exception as exc:
            logger.debug("Row-quality pushdown unavailable for %r: %s", self._schema, exc)
            return False
        return True

    def _choose_key(self, pushdown_ok: bool) -> list[str]:
        """Key on the declared primary key when it is proven unique, else the full columns.

        A unique primary key groups the rows exactly as the full columns do,
        with a narrower GROUP BY, smaller server_lookup statements and no
        comparison of float, nested or temporal values outside the key. It
        also lets checks that return only some columns, the key among them,
        merge. It is used when the data source can run the pushdown and the
        duplicate-key probe finds no key value twice.
        """
        if pushdown_ok and self._primary_key and len(self._primary_key) < len(self._columns):
            if self._primary_key_status() == PK_UNIQUE:
                return list(self._primary_key)
        return list(self._columns)

    def _primary_key_status(self) -> str:
        """Probe the declared primary key for duplicates, once per schema."""
        if self._pk_status is not None:
            return self._pk_status
        spec = KeySpec(
            dialect=self._dialect,
            anchor_sql=self._anchor_sql,
            columns=list(self._primary_key),
            dtypes=[self._column_types.get(name) for name in self._primary_key],
        )
        try:
            table = self._run_query(duplicate_key_statement(spec))
            values = table.column(0).to_pylist()
            duplicates = int(values[0]) if values and values[0] is not None else None
        except Exception as exc:
            logger.debug("Primary key of %r could not be checked for duplicates: %s", self._schema, exc)
            duplicates = None
        if duplicates is None:
            self._pk_status = PK_UNCHECKED
        elif duplicates == 0:
            self._pk_status = PK_UNIQUE
        else:
            # Not a warning: the generated primary key check reports it.
            logger.debug(
                "Primary key of %r has %d duplicated values, so its rows are matched on every column.",
                self._schema,
                duplicates,
            )
            self._pk_status = PK_DUPLICATED
        self._pk_duplicates = duplicates
        return self._pk_status

    def _check_match_key(self, work: _Work, key_columns: Sequence[str]) -> None:
        """Leave a check whose rows can never be attributed to the table out. Its rows become residues."""
        if not (work.row_level and work.attributable) or work.route in ("", ROUTE_SERVER_SCALAR):
            return
        columns = set(work.columns or [])
        key = key_columns if len(key_columns) < len(self._columns) else None
        if has_match_key(columns, self._columns, key):
            return
        reason = REASON_NO_MATCH_KEY
        if key is None and self._primary_key and set(self._primary_key) <= columns:
            # The check could have merged on the primary key, had it been unique.
            if self._pk_status == PK_DUPLICATED:
                reason = REASON_PK_NOT_UNIQUE
            elif self._pk_status == PK_UNCHECKED:
                reason = REASON_PK_UNCHECKED
        # Without the adapter's column types the columns come from the
        # contract, which can differ from the table's. Annotated output
        # compares with the exported table instead, so it may mark rows this
        # count leaves out, and the numbers are approximate.
        work.not_attributable(reason, approximate=not self._column_types)

    def _keyless(self, engine_work: Sequence[_Work]) -> bool:
        """Whether the schema can be counted without a key: every check a certified row filter."""
        return not key_is_exact(self._dialect) and all(work.route == ROUTE_SERVER_PREDICATE for work in engine_work)

    def _branch(self, work: _Work) -> Branch:
        query = work.row_source.row_query
        scan = self._dialect in _pushdown.SINGLE_SCAN_DIALECTS or not key_is_exact(self._dialect)
        if work.route == ROUTE_SERVER_PREDICATE and scan:
            lifted = lift_scan(query, self._dialect)
            if lifted is not None:
                scan_from, scan_alias, predicate = lifted
                return Branch(work.check_id, "server_predicate", query, scan_from, scan_alias, predicate)
        return Branch(
            work.check_id, "server_predicate" if work.route == ROUTE_SERVER_PREDICATE else "server_lookup", query
        )

    def _total_rows(self, outcome: PushdownOutcome | None) -> int | None:
        if outcome is not None and outcome.total_rows is not None:
            return outcome.total_rows
        total = self._recorded_or_fetched_total()
        if total == 0:
            # get_total_rows returns 0 when its query fails, so a 0 is
            # counted again where the data source can run the count itself.
            recount = self._count_anchor()
            if recount is not None:
                self._total_approximate = False
                return recount
        return total

    def _recorded_total(self) -> int | None:
        """The row count the validation run recorded, without running a query."""
        stats = self._result._vs.get("total_rows_by_schema", {}) or {}
        recorded = stats.get(self._schema)
        if not isinstance(recorded, int) or isinstance(recorded, bool):
            self._total_approximate = True
            return None
        if self._result._config.max_rows_for_statistics >= 0:
            # Capped by max_rows_for_statistics, so possibly too low.
            self._total_approximate = True
        return recorded

    def _recorded_or_fetched_total(self) -> int | None:
        stats = self._result._vs.get("total_rows_by_schema", {}) or {}
        recorded = stats.get(self._schema)
        if isinstance(recorded, int) and self._result._config.max_rows_for_statistics < 0:
            return recorded
        getter = getattr(self._adapter, "get_total_rows", None)
        if getter is not None:
            total = _safe(getter, self._schema, -1)
            if isinstance(total, int) and not isinstance(total, bool):
                return total
        if isinstance(recorded, int):
            # Capped by max_rows_for_statistics, so possibly too low.
            self._total_approximate = True
            return recorded
        return None

    def _count_anchor(self) -> int | None:
        if not (self._adapter and self._dialect and self._anchor_sql):
            return None
        try:
            table = self._run_query(count_statement(self._anchor_sql, self._dialect))
        except Exception:
            return None
        values = table.column(0).to_pylist()
        return int(values[0]) if values and values[0] is not None else None

    # -- rollup --------------------------------------------------------

    @staticmethod
    def _makes_approximate(work: _Work) -> bool:
        if work.approximate or work.selection.approximate:
            return True
        # A lookup matches the values a check returns, so a changed value can
        # land on another row or on none.
        return work.row_level and work.transformed and work.route in (ROUTE_SERVER_LOOKUP, ROUTE_CLIENT_LOOKUP)

    def _check_approximate(self, work: _Work) -> bool:
        if work.row_level and work.in_scope and not work.attributable and work.route != ROUTE_SERVER_SCALAR:
            # Its rows are not known.
            return True
        return self._makes_approximate(work)

    def _state(self, work: _Work) -> CheckState:
        selection = work.selection
        return CheckState(
            check_id=work.check_id,
            dimension=selection.dimension,
            row_level=work.row_level,
            in_scope=work.in_scope,
            collected=work.row_level and work.attributable and work.route not in ("", ROUTE_SERVER_SCALAR),
            attributable=work.attributable,
            from_scalar=work.row_level and work.route == ROUTE_SERVER_SCALAR,
            approximate=self._makes_approximate(work),
            attributed_rows=work.attributed_rows,
            scalar_count=selection.scalar_count,
        )

    def _roll_up(
        self, entries: list[tuple[int, int]], total_rows: int | None, merge_approximate: bool
    ) -> tuple[SchemaRowQuality, list[DimensionRowQuality], list[CheckRowQuality]]:
        states = [self._state(work) for work in self._work]
        schema_approximate = merge_approximate or self._total_approximate
        if total_rows is not None and any(
            (bucket_rows(state) or 0) > total_rows for state in states if state.row_level and state.in_scope
        ):
            # A check found more rows than the table has, so the total is off.
            schema_approximate = True

        failed, passed, rate, approximate, row_level, not_attributable = roll_up_bucket(
            states, entries, total_rows, approximate=schema_approximate
        )
        schema = SchemaRowQuality(
            schema_name=self._schema,
            total_rows=total_rows,
            failed_rows=failed,
            passed_rows=passed,
            pass_rate=rate,
            approximate=approximate,
            checks_row_level=row_level,
            checks_not_row_level=sum(not state.row_level for state in states),
            checks_not_attributable=not_attributable,
        )

        dimensions: list[DimensionRowQuality] = []
        for dimension in sorted({state.dimension for state in states}):
            bucket = [state for state in states if state.dimension == dimension]
            failed, passed, rate, approximate, row_level, not_attributable = roll_up_bucket(
                bucket, entries, total_rows, approximate=schema_approximate
            )
            dimensions.append(
                DimensionRowQuality(
                    schema_name=self._schema,
                    dimension=dimension,
                    total_rows=total_rows,
                    failed_rows=failed,
                    passed_rows=passed,
                    pass_rate=rate,
                    approximate=approximate,
                    checks_row_level=row_level,
                    checks_not_row_level=sum(not state.row_level for state in bucket),
                    checks_not_attributable=not_attributable,
                )
            )

        checks = [
            CheckRowQuality(
                schema_name=self._schema,
                check_name=work.result.check_name,
                dimension=work.selection.dimension,
                status=work.result.status,
                row_level=work.row_level,
                route=work.route if work.row_level else "",
                reason=work.reason,
                scalar_count=work.selection.scalar_count,
                attributed_rows=work.attributed_rows if work.row_level and work.attributable else None,
                approximate=self._check_approximate(work),
            )
            for work in self._work
        ]
        return schema, dimensions, checks
