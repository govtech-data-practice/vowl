"""The row-quality component: how many rows of each table have issues.

One component computes the numbers lazily and caches them on the
``ValidationResult``. ``print_summary``, the OTEL gauges,
``get_row_quality_df`` and annotated output all read from it, so every surface
reports the same physical-row counts. The process is described in
``design/row-quality-statistics.md``:

1. Total rows per schema.
2. Select the counted checks (:mod:`.selection`).
3. Collect each check's failing rows, copies included, by one of three routes:

   - ``pushdown``: certified row filters (:mod:`.certify`), counted inside the
     data source (:mod:`.pushdown`).
   - ``table_match``: other checks on the table's own connection, counted as
     the table rows that match the check's failed rows.
   - ``full_table``: other checks, whose fetched failed rows are matched onto
     the exported table, so each row's copies come from the table.
   - ``fetched_rows``: everything else, from the check's fetched failed rows.

   ``ValidationConfig.row_count_accuracy`` picks between them. ``"accurate"``
   uses ``full_table`` for every check that is not a certified row filter,
   ``"balanced"`` only where ``fetched_rows`` would be used, and ``"fast"``
   never exports the table.

4. Merge the routes into one entry per physical row (:mod:`.merge`).
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
from ..result_rendering import is_cross_table_check
from . import pushdown as _pushdown
from .certify import certify_check
from .keys import key_is_exact, primary_key_columns
from .merge import FetchedRows, merge_onto_table, merge_routes
from .mergeable import METADATA_COLUMNS, rows_mergeable
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
    own_rows_from_entries,
    roll_up_bucket,
    run_level_rates,
)
from .selection import (
    REASON_CROSS_SOURCE,
    REASON_NO_EXPORT,
    REASON_NO_PUSHDOWN,
    REASON_NOT_MERGEABLE,
    REASON_PK_NOT_UNIQUE,
    REASON_PK_UNCHECKED,
    REASON_PROBE_FAILURE,
    REASON_TOLERATED_NOT_FETCHED,
    REASON_TRUNCATED,
    REASON_UNKEYABLE,
    REASON_UNMATCHED,
    CheckSelection,
    flagged_checks,
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
    "flagged_checks",
    "resolve_check_dimension",
    "select",
]

ROUTE_SERVER_PREDICATE = "server_predicate"
ROUTE_SERVER_LOOKUP = "server_lookup"
ROUTE_CLIENT_RETURNED_ROWS = "client_returned_rows"
ROUTE_CLIENT_LOOKUP = "client_lookup"

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
    counted: bool
    route: str = ""
    reason: str = ""
    inexact: bool = False
    skipped: bool = False
    certified: bool = False
    columns: list[str] | None = None
    rows: pa.Table | None = None
    own_rows: int | None = None
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
        count = self.selection.row_count
        return self.counted and (count is None or count > 0)

    def drop(self, reason: str, *, inexact: bool) -> None:
        self.counted = False
        self.route = ""
        self.reason = reason
        self.inexact = self.inexact or inexact


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
        self._scope = self._config.row_issue_scope
        self._selections = select(result.check_results, self._scope)
        self._report: RowQualityReport | None = None
        self._tolerated_rows: dict[int, nw.DataFrame] = {}
        self._merge_keys: dict[str, list[str]] = {}

    @property
    def selections(self) -> list[CheckSelection]:
        """The step 2 verdict for every check anchored to a schema."""
        return self._selections

    def report(self) -> RowQualityReport:
        """The row-quality report, computed on first use."""
        if self._report is None:
            self._report = self._compute()
        return self._report

    def merge_key(self, schema_name: str) -> list[str] | None:
        """The primary key a schema's rows are matched on, or ``None`` for all columns.

        Set whenever the schema declares a primary key that is shorter than
        its columns, the data source can run the pushdown, and the key was
        found unique. Annotated output matches on the same key, so both merge
        the same checks.
        """
        self.report()
        key = self._merge_keys.get(schema_name)
        return list(key) if key is not None else None

    def rows_for(self, selection: CheckSelection) -> nw.DataFrame:
        """A counted check's failing rows, as annotated output flags them.

        FAILED checks use their own lazily fetched failed rows. A tolerated
        check has no fetcher of its own, so its rows are fetched through its
        row source, once.
        """
        check_result = selection.result
        if not selection.tolerated:
            return check_result.failed_rows
        key = id(check_result)
        if key not in self._tolerated_rows:
            fetch = getattr(getattr(check_result, "row_source", None), "fetch", None)
            fetched = _safe(fetch) if fetch is not None else None
            self._tolerated_rows[key] = (
                fetched if fetched is not None else nw.from_native(pa.table({}), eager_only=True)
            )
        return self._tolerated_rows[key]

    # ------------------------------------------------------------------
    # Computation
    # ------------------------------------------------------------------

    def _compute(self) -> RowQualityReport:
        result = self._result
        by_schema: dict[str, list[CheckSelection]] = {name: [] for name in result._schema_names}
        for selection in self._selections:
            if selection.schema_name in by_schema:
                by_schema[selection.schema_name].append(selection)

        if not self._config.enable_additional_schema_statistics:
            return self._disabled_report(by_schema)

        schemas: list[SchemaRowQuality] = []
        dimensions: list[DimensionRowQuality] = []
        checks: list[CheckRowQuality] = []
        for schema_name, selections in by_schema.items():
            schema, schema_dimensions, schema_checks = _SchemaComputation(self, schema_name, selections).run()
            schemas.append(schema)
            dimensions.extend(schema_dimensions)
            checks.extend(schema_checks)

        weighted, mean = run_level_rates(schemas)
        return RowQualityReport(
            schemas=schemas,
            dimensions=dimensions,
            checks=checks,
            weighted_pass_rate=weighted,
            mean_pass_rate=mean,
        )

    def _disabled_report(self, by_schema: dict[str, list[CheckSelection]]) -> RowQualityReport:
        schemas = [
            SchemaRowQuality(
                schema_name=schema_name,
                total_rows=None,
                failed_rows=None,
                tolerated_rows=None,
                passed_rows=None,
                pass_rate=None,
                exact=False,
                checks_counted=sum(item.counted for item in selections),
                checks_not_counted=sum(not item.counted for item in selections),
            )
            for schema_name, selections in by_schema.items()
        ]
        checks = [
            CheckRowQuality(
                schema_name=item.schema_name,
                check_name=item.result.check_name,
                dimension=item.dimension,
                status=item.result.status,
                counted=item.counted,
                tolerated=item.tolerated,
                route="",
                reason=item.reason,
                failed_rows=None,
                exact=False,
            )
            for selections in by_schema.values()
            for item in selections
        ]
        return RowQualityReport(schemas=schemas, checks=checks, enabled=False)


class _SchemaComputation:
    """Steps 1 to 5 for one schema."""

    def __init__(self, component: RowQuality, schema_name: str, selections: Sequence[CheckSelection]) -> None:
        self._component = component
        self._result = component._result
        self._scope = component._scope
        self._schema = schema_name
        self._work = [
            _Work(selection=selection, check_id=index, counted=selection.counted, reason=selection.reason)
            for index, selection in enumerate(selections)
        ]
        for work in self._work:
            if work.counted and not work.needs_rows:
                # A counted check that matched no rows adds nothing.
                work.own_rows = 0
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
        self._total_exact = True
        # The duplicate-key probe's verdict: None until it runs, then one of
        # PK_UNIQUE, PK_DUPLICATED or PK_UNCHECKED.
        self._pk_status: str | None = None
        self._pk_duplicates: int | None = None
        self._accuracy: str = getattr(component._config, "row_count_accuracy", "accurate")
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
        spec = self._full_spec() if pending else None
        pushdown_ok = spec is not None and self._preflight(spec)

        for work in pending:
            self._assign_route(work, pushdown_ok)
            self._mark_want_full(work)
        for work in pending:
            if work.route == ROUTE_SERVER_LOOKUP and work.want_full:
                self._fetch_for_match(work)
            if work.route == ROUTE_SERVER_LOOKUP and not work.want_full:
                self._probe_columns(work)
        for work in pending:
            if work.route == ROUTE_CLIENT_RETURNED_ROWS:
                self._fetch(work)
                if work.reason == REASON_TRUNCATED or work.rows is None:
                    work.want_full = False
        self._export_table(pending)

        key_columns, use_prefix = self._choose_key(pushdown_ok)
        if not use_prefix and len(key_columns) < len(self._columns):
            self._component._merge_keys[self._schema] = list(key_columns)
        for work in pending:
            self._check_mergeable(work, key_columns, use_prefix)
        index = self._export_index(pending, key_columns) if self._export is not None and not use_prefix else None
        if index is not None:
            return self._run_onto_table(pending, spec, key_columns, index)

        engine_work = [
            work for work in pending if work.counted and work.route in (ROUTE_SERVER_PREDICATE, ROUTE_SERVER_LOOKUP)
        ]
        fetched_work = [
            work
            for work in pending
            if work.counted and work.route == ROUTE_CLIENT_RETURNED_ROWS and work.rows is not None
        ]

        outcome: PushdownOutcome | None = None
        if engine_work and spec is not None:
            spec = KeySpec(
                dialect=spec.dialect,
                anchor_sql=spec.anchor_sql,
                columns=list(key_columns),
                dtypes=[self._column_types.get(name) for name in key_columns],
                with_values=bool(fetched_work),
            )
            runner = PushdownRunner(self._run_query, spec)
            branches = [self._branch(work) for work in engine_work]
            if self._keyless(engine_work, fetched_work):
                outcome = runner.run_mask_histogram(branches)
                if outcome is not None:
                    for work in engine_work:
                        work.inexact = False
            if outcome is None:
                outcome = runner.run(branches, histogram=not fetched_work)
            for work in engine_work:
                if work.check_id in outcome.dropped:
                    work.drop(REASON_PROBE_FAILURE, inexact=True)
            self._flag_unmatched(runner, engine_work)

        fetched = [
            FetchedRows(
                check_id=work.check_id,
                table=work.rows,
                key_columns=list(work.columns or []) if use_prefix else list(key_columns),
                prefix=use_prefix,
            )
            for work in fetched_work
            if work.counted
        ]
        entries, merge_exact = merge_routes(outcome, fetched, key_columns, _arrow_schema(self._column_types))

        engine_ids = [work.check_id for work in engine_work if work.counted]
        for check_id, rows in own_rows_from_entries(entries, engine_ids).items():
            self._work[check_id].own_rows = rows
        for work in fetched_work:
            if work.counted and work.rows is not None:
                work.own_rows = work.rows.num_rows

        total_rows = self._total_rows(outcome)
        return self._roll_up(entries, total_rows, merge_exact)

    # -- the full-table match --------------------------------------------

    def _mark_want_full(self, work: _Work) -> None:
        """Mark a check to be counted on the exported table, per ``row_count_accuracy``."""
        if not work.counted or not work.route or work.route == ROUTE_SERVER_PREDICATE:
            return
        if work.selection.tolerated and self._scope == "failed_checks":
            # Its rows are not fetched under this scope.
            return
        if self._accuracy == "accurate" or (self._accuracy == "balanced" and work.route == ROUTE_CLIENT_RETURNED_ROWS):
            work.want_full = True

    def _fetch_for_match(self, work: _Work) -> None:
        """Fetch a table-match check's rows to match onto the table.

        A check whose rows cannot be fetched whole keeps its route, which
        counts in SQL without fetching.
        """
        table = _safe(lambda: strip_metadata_columns(self._fetched_frame(work).to_arrow()))
        count = work.selection.row_count
        if table is None or table.num_columns == 0 or (count is not None and table.num_rows < count):
            work.want_full = False
            return
        work.rows = table
        work.columns = list(table.column_names)

    def _export_table(self, pending: Sequence[_Work]) -> None:
        """Export the table once, when a check is to be matched onto it."""
        wanted = [work for work in pending if work.want_full and work.counted and work.rows is not None]
        if not wanted:
            return
        frame = _safe(self._result._fetch_full_table, self._schema)
        table = _safe(lambda: frame.to_arrow()) if frame is not None else None
        if table is None:
            self._fall_back(wanted, REASON_NO_EXPORT)
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
                if work.counted and work.route == ROUTE_SERVER_PREDICATE:
                    work.want_full = True
                    self._fetch_for_match(work)

    def _export_index(self, pending: Sequence[_Work], key_columns: Sequence[str]) -> dict[Any, list[int]] | None:
        try:
            index = self._result._table_key_index(self._schema, key_columns)
        except Exception as exc:
            logger.debug("Rows of %r could not be keyed: %s", self._schema, exc)
            index = None
        if index is None:
            self._export = None
            self._fall_back([work for work in pending if work.want_full], REASON_UNKEYABLE)
        return index

    @staticmethod
    def _fall_back(works: Sequence[_Work], reason: str) -> None:
        """Count *works* as ``"fast"`` does, with its exactness, explaining why."""
        for work in works:
            work.want_full = False
            if work.counted and work.route == ROUTE_CLIENT_RETURNED_ROWS:
                work.reason = reason

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
            if work.counted and not work.want_full and work.route in (ROUTE_SERVER_PREDICATE, ROUTE_SERVER_LOOKUP)
        ]
        matched_work = [
            work
            for work in pending
            if work.counted and work.rows is not None and (work.want_full or work.route == ROUTE_CLIENT_RETURNED_ROWS)
        ]

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
                    work.drop(REASON_PROBE_FAILURE, inexact=True)
            self._flag_unmatched(runner, engine_work)

        fetched = [
            FetchedRows(check_id=work.check_id, table=work.rows, key_columns=list(key_columns))
            for work in matched_work
            if work.counted
        ]
        entries, merge_exact, matched = merge_onto_table(outcome, fetched, key_columns, table, index)

        engine_ids = [work.check_id for work in engine_work if work.counted]
        for check_id, rows in own_rows_from_entries(entries, engine_ids).items():
            self._work[check_id].own_rows = rows
        for work in matched_work:
            if not work.counted:
                continue
            match = matched.get(work.check_id)
            if match is None:
                work.route, work.reason, work.inexact = ROUTE_CLIENT_RETURNED_ROWS, REASON_UNKEYABLE, True
                work.own_rows = work.rows.num_rows
                continue
            work.own_rows, missing = match
            if not work.want_full:
                # Truncated, so a lower bound. It stays inexact.
                continue
            work.route = ROUTE_CLIENT_LOOKUP
            work.inexact = missing > 0
            if missing:
                work.reason = REASON_UNMATCHED

        total_rows = outcome.total_rows if outcome is not None and outcome.total_rows is not None else table.num_rows
        self._total_exact = True
        return self._roll_up(entries, total_rows, merge_exact)

    def _flag_unmatched(self, runner: PushdownRunner, engine_work: Sequence[_Work]) -> None:
        """Mark a table-match check not exact when some of its failed keys match no table row.

        Table match counts the table rows that hold a failed row's key, so a
        check that returns changed values (``c * 1.5``, ``upper(s)``) finds
        fewer rows than it failed. This is the same test as the full-table
        match: per distinct key, on the same key expressions. A changed value
        that equals another row's key matches that row and is not detected.
        """
        works = [work for work in engine_work if work.counted and work.route == ROUTE_SERVER_LOOKUP]
        if not works:
            return
        unmatched = runner.count_unmatched([self._branch(work) for work in works])
        for work in works:
            count = unmatched.get(work.check_id)
            if count is None:
                # Not known whether every key matched.
                work.inexact = True
            elif count:
                work.inexact, work.reason = True, REASON_UNMATCHED

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

    def _assign_route(self, work: _Work, pushdown_ok: bool) -> None:
        row_source = work.row_source
        if row_source is None:
            work.route, work.reason = ROUTE_CLIENT_RETURNED_ROWS, REASON_NO_PUSHDOWN
            return
        rendered = None
        if not row_source.filter_conditions:
            # The queries the check ran are the unfiltered ones.
            rendered = (row_source.failed_rows_query, work.result.metadata.get("rendered_implementation"))
        ok, rule = certify_check(
            row_source.check_ref, self._schema, row_source.dialect, row_source.use_try_cast, rendered=rendered
        )
        local = pushdown_ok and row_source.dialect == self._dialect and bool(row_source.failed_rows_query)
        if row_source.cross_source or not local:
            # Fetched rows of a check that is not a pure row filter can miss
            # copies (DISTINCT) or repeat them (a join), so they are not exact.
            work.route = ROUTE_CLIENT_RETURNED_ROWS
            work.reason = REASON_CROSS_SOURCE if row_source.cross_source else REASON_NO_PUSHDOWN
            work.inexact = not ok
            return
        work.certified = ok
        if ok:
            work.route = ROUTE_SERVER_PREDICATE
            work.columns = list(self._columns)
            # A dialect without a key entry groups on plain columns. The
            # keyless mask histogram clears this when it can count the schema.
            work.inexact = not key_is_exact(self._dialect)
        elif key_is_exact(self._dialect):
            work.route, work.reason = ROUTE_SERVER_LOOKUP, uncertified_reason(rule)
        else:
            work.route, work.reason, work.inexact = ROUTE_CLIENT_RETURNED_ROWS, uncertified_reason(rule), True

    def _probe_columns(self, work: _Work) -> None:
        query = work.row_source.failed_rows_query
        try:
            table = self._run_query(column_probe_statement(query, self._dialect))
        except Exception:
            # The fetch may still work where the wrapped probe does not.
            work.route, work.inexact = ROUTE_CLIENT_RETURNED_ROWS, True
            self._fetch(work)
            return
        work.columns = [name for name in table.column_names if name not in METADATA_COLUMNS]

    def _fetch(self, work: _Work) -> None:
        selection = work.selection
        if selection.tolerated and self._scope == "failed_checks":
            # Not needed for the headline number, and possibly large.
            work.route, work.reason, work.skipped = "", REASON_TOLERATED_NOT_FETCHED, True
            return
        if selection.tolerated:
            fetch = getattr(work.row_source, "fetch", None)
            if fetch is None:
                work.route, work.reason, work.skipped, work.inexact = "", REASON_NO_PUSHDOWN, True, True
                return
        table = strip_metadata_columns(self._fetched_frame(work).to_arrow())
        if table.num_columns == 0:
            work.drop(REASON_PROBE_FAILURE, inexact=True)
            return
        work.rows = table
        work.columns = list(table.column_names)
        count = selection.row_count
        if count is not None and table.num_rows < count:
            work.reason, work.inexact = REASON_TRUNCATED, True

    def _fetched_frame(self, work: _Work) -> nw.DataFrame:
        selection = work.selection
        return self._component.rows_for(selection) if selection.tolerated else work.result.failed_rows

    def _choose_key(self, pushdown_ok: bool) -> tuple[list[str], bool]:
        """Key on the declared primary key when it is proven unique, else the full columns.

        A unique primary key groups the rows exactly as the full columns do,
        with a narrower GROUP BY, smaller table-match statements and no
        comparison of float, nested or temporal values outside the key. It
        also lets checks that return only some columns, the key among them,
        merge. It is used when the data source can run the pushdown and the
        duplicate-key probe finds no key value twice.
        """
        if not self._columns:
            return [], True
        if pushdown_ok and self._primary_key and len(self._primary_key) < len(self._columns):
            if self._primary_key_status() == PK_UNIQUE:
                return list(self._primary_key), False
        return list(self._columns), False

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

    def _check_mergeable(self, work: _Work, key_columns: Sequence[str], use_prefix: bool) -> None:
        if not work.counted or not work.route:
            return
        if use_prefix:
            # The table's columns are unknown, so mergeability cannot be
            # checked. Rows of a check that reads other tables likely carry
            # their columns too, so such checks are left out.
            if is_cross_table_check(work.result):
                work.drop(REASON_NOT_MERGEABLE, inexact=False)
                work.rows = None
            else:
                work.inexact = True
            return
        columns = set(work.columns or [])
        key = key_columns if len(key_columns) < len(self._columns) else None
        if rows_mergeable(columns, self._columns, key):
            return
        reason = REASON_NOT_MERGEABLE
        if key is None and self._primary_key and set(self._primary_key) <= columns:
            # The check could have merged on the primary key, had it been unique.
            if self._pk_status == PK_DUPLICATED:
                reason = REASON_PK_NOT_UNIQUE
            elif self._pk_status == PK_UNCHECKED:
                reason = REASON_PK_UNCHECKED
        # Without the adapter's column types the columns come from the
        # contract, which can differ from the table's. Annotated output
        # compares with the exported table instead, so it may mark rows this
        # count leaves out, and the numbers are not exact.
        work.drop(reason, inexact=not self._column_types)
        work.rows = None

    def _keyless(self, engine_work: Sequence[_Work], fetched_work: Sequence[_Work]) -> bool:
        """Whether the schema can be counted without a key: every check a certified row filter."""
        return (
            not key_is_exact(self._dialect)
            and not fetched_work
            and all(work.route == ROUTE_SERVER_PREDICATE for work in engine_work)
        )

    def _branch(self, work: _Work) -> Branch:
        query = work.row_source.failed_rows_query
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
                self._total_exact = True
                return recount
        return total

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
            self._total_exact = False
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

    def _state(self, work: _Work) -> CheckState:
        selection = work.selection
        return CheckState(
            check_id=work.check_id,
            dimension=selection.dimension,
            counted=work.counted,
            failed=work.result.status == "FAILED",
            collected=work.counted and bool(work.route),
            skipped=work.skipped,
            inexact=work.inexact or selection.blocks_exact,
            own_rows=work.own_rows,
        )

    def _roll_up(
        self, entries: list[tuple[int, int]], total_rows: int | None, merge_exact: bool
    ) -> tuple[SchemaRowQuality, list[DimensionRowQuality], list[CheckRowQuality]]:
        states = [self._state(work) for work in self._work]
        schema_exact = merge_exact and self._total_exact
        if total_rows is not None and any((work.own_rows or 0) > total_rows for work in self._work if work.counted):
            # A check found more rows than the table has, so the total is off.
            schema_exact = False

        failed, tolerated, passed, rate, exact, counted = roll_up_bucket(
            states,
            entries,
            total_rows,
            scope=self._scope,
            exact=schema_exact,
        )
        schema = SchemaRowQuality(
            schema_name=self._schema,
            total_rows=total_rows,
            failed_rows=failed,
            tolerated_rows=tolerated,
            passed_rows=passed,
            pass_rate=rate,
            exact=exact,
            checks_counted=counted,
            checks_not_counted=sum(not state.counted for state in states),
        )

        dimensions: list[DimensionRowQuality] = []
        for dimension in sorted({state.dimension for state in states}):
            bucket = [state for state in states if state.dimension == dimension]
            failed, tolerated, passed, rate, exact, counted = roll_up_bucket(
                bucket,
                entries,
                total_rows,
                scope=self._scope,
                exact=schema_exact,
            )
            dimensions.append(
                DimensionRowQuality(
                    schema_name=self._schema,
                    dimension=dimension,
                    total_rows=total_rows,
                    failed_rows=failed,
                    tolerated_rows=tolerated,
                    passed_rows=passed,
                    pass_rate=rate,
                    exact=exact,
                    checks_counted=counted,
                    checks_not_counted=sum(not state.counted for state in bucket),
                )
            )

        checks = [
            CheckRowQuality(
                schema_name=self._schema,
                check_name=work.result.check_name,
                dimension=work.selection.dimension,
                status=work.result.status,
                counted=work.counted,
                tolerated=work.selection.tolerated,
                route=work.route if work.counted else "",
                reason=work.reason,
                failed_rows=work.own_rows if work.counted else None,
                exact=not (work.inexact or work.selection.blocks_exact),
            )
            for work in self._work
        ]
        return schema, dimensions, checks
