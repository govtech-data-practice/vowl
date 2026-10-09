"""DQ metrics: the check, dimension, schema and run numbers of one run.

One computation feeds every output that reports these numbers: the
OpenTelemetry metrics (:mod:`vowl.otel`) and the ``<prefix>_dq_metrics.json``
file written by :meth:`ValidationResult.save`. Both read the points built here,
so the two can never disagree. See docs/dq-metrics/index.md.

Every metric is named ``<prefix>.<level>.<unit>.<measure>``:

- ``level`` is the slice the reading is for: ``check``, ``dimension``,
  ``schema`` or ``run``.
- ``unit`` is what is counted: ``check``, ``row`` or ``schema``.
- ``measure`` is ``count`` or ``pass_rate``. Timings are ``duration``. The
  check level also has ``scalar_count`` and ``scalar_pass_rate``, from the
  check's scalar count rather than its attributed rows.
- ``row.approximate`` is a 0/1 flag next to the row counts: ``1`` when they
  could be off. It is its own metric rather than an attribute, so a flag that
  flips between runs never splits the row count series.

Check and schema counts add up across levels and across runs, so they are
counters. Row counts do not (a row can fail several checks, and a re-run checks
the same rows again), so they are gauges, like every pass rate and flag.

Nothing here imports ``opentelemetry``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from .row_quality.selection import resolve_check_dimension

if TYPE_CHECKING:
    from ..executors.base import CheckResult
    from .result import ValidationResult

#: Version of the ``dq_metrics.json`` layout. Bump it on any breaking change.
SCHEMA_VERSION = 1

COUNTER = "counter"
GAUGE = "gauge"
HISTOGRAM = "histogram"

#: Every status a check can end in. Check counts send one point per status.
CHECK_STATUSES = ("PASSED", "FAILED", "ERROR")

#: Attribute values OTEL accepts natively. Anything else is coerced with ``str``.
_NATIVE_ATTR_TYPES = (str, bool, int, float)

AttrValue = str | bool | int | float


@dataclass(frozen=True)
class MetricPoint:
    """One reading of one metric.

    Attributes:
        name: Full metric name, prefix included (``vowl.run.check.count``).
        type: ``"counter"``, ``"gauge"`` or ``"histogram"``.
        unit: UCUM unit: ``{check}``, ``{row}``, ``{schema}``, ``1`` or ``ms``.
        value: The reading. For a counter, this run's increment.
        attributes: What the reading is for, such as ``schema_name`` and
            ``status``. Run identity attributes are not included.
    """

    name: str
    type: str
    unit: str
    value: int | float
    attributes: dict[str, AttrValue]


def new_run_id() -> str:
    """A fresh run id, so a consumer can tell runs apart and de-duplicate retries."""
    return str(uuid.uuid4())


# --------------------------------------------------------------------------- #
# Attributes
# --------------------------------------------------------------------------- #


def coerce_attr(value: Any) -> AttrValue | None:
    """Coerce *value* to an OTEL-safe attribute value, or ``None`` to drop it."""
    if value is None:
        return None
    if isinstance(value, _NATIVE_ATTR_TYPES):
        return value
    return str(value)


def clean_attrs(attrs: dict[str, Any]) -> dict[str, AttrValue]:
    """Drop ``None`` values and coerce the rest to OTEL-safe attribute values."""
    cleaned: dict[str, AttrValue] = {}
    for key, raw in attrs.items():
        value = coerce_attr(raw)
        if value is not None:
            cleaned[key] = value
    return cleaned


def contract_attributes(result: ValidationResult, prefix: str = "vowl") -> dict[str, Any]:
    """Contract identity attributes, absent keys omitted.

    A fixed list of contract fields: ``id``, ``name``, ``version``,
    ``apiVersion``, ``status``, ``contractCreatedTs``, ``domain``,
    ``dataProduct`` and ``tenant``. The list is kept short and stable on purpose
    so the attribute set is predictable. Other fields can be passed through
    ``custom_attributes``.
    """
    contract = result.contract
    metadata = contract.get_metadata()
    contract_data = getattr(contract, "contract_data", {}) or {}

    p = prefix
    attrs = {
        f"{p}.contract.id": metadata.get("id"),
        f"{p}.contract.name": contract_data.get("name"),
        f"{p}.contract.version": contract.get_version(),
        f"{p}.contract.api_version": result.api_version,
        f"{p}.contract.status": metadata.get("status"),
        f"{p}.contract.created_ts": contract_data.get("contractCreatedTs"),
        f"{p}.domain": contract_data.get("domain"),
        f"{p}.data_product": contract_data.get("dataProduct"),
        f"{p}.tenant": contract_data.get("tenant"),
    }
    return {key: value for key, value in attrs.items() if value not in (None, "")}


def run_identity_attributes(
    result: ValidationResult,
    *,
    run_id: str,
    version: str,
    prefix: str = "vowl",
) -> dict[str, AttrValue]:
    """The vowl version, the run id and the contract identity of one run."""
    attrs: dict[str, Any] = {
        f"{prefix}.version": version,
        f"{prefix}.run.id": run_id,
    }
    attrs.update(contract_attributes(result, prefix=prefix))
    return clean_attrs(attrs)


def check_dimension(check_result: Any) -> str:
    """A check's DQ dimension, ``"unknown"`` when it has none."""
    return resolve_check_dimension(check_result)


def check_severity(check_result: Any) -> Any:
    """A check's severity from its metadata or its ``check_definition``."""
    metadata = check_result.metadata
    definition = metadata.get("check_definition") or {}
    return metadata.get("severity") or definition.get("severity")


def check_attributes(check_result: Any) -> dict[str, AttrValue]:
    """The bounded, low-cardinality attributes of one check, shared by every signal."""
    metadata = check_result.metadata
    return clean_attrs(
        {
            "check_name": check_result.check_name,
            "status": check_result.status,
            "schema_name": metadata.get("schema_name"),
            "dimension": check_dimension(check_result),
            "severity": check_severity(check_result),
            "engine": metadata.get("engine"),
        }
    )


# --------------------------------------------------------------------------- #
# Check counting
# --------------------------------------------------------------------------- #


def check_status_counts(check_results: Iterable[CheckResult]) -> dict[str, int]:
    """Checks per status, every status present (zeros included)."""
    counts = dict.fromkeys(CHECK_STATUSES, 0)
    for check_result in check_results:
        if check_result.status in counts:
            counts[check_result.status] += 1
    return counts


def check_pass_rate(counts: dict[str, int]) -> float | None:
    """Passed checks over all checks, ``ERROR`` included. ``None`` for no checks."""
    total = sum(counts.values())
    return counts["PASSED"] / total if total else None


def schema_status(counts: dict[str, int]) -> str:
    """One status for a schema from its check counts.

    ``FAILED`` when any check found bad data, else ``ERROR`` when any check
    could not run, else ``PASSED``. A known data problem outranks a check that
    could not say.
    """
    if counts["FAILED"]:
        return "FAILED"
    if counts["ERROR"]:
        return "ERROR"
    return "PASSED"


def run_duration_ms(result: ValidationResult) -> float:
    """Wall-clock run time when the runner recorded it, else summed check time."""
    started = getattr(result, "_run_started_ns", None)
    finished = getattr(result, "_run_finished_ns", None)
    if started is not None and finished is not None and finished >= started:
        return (finished - started) / 1_000_000
    return float(result._vs.get("total_execution_time_ms", 0.0) or 0.0)


def check_level_attributes(check_result: Any) -> dict[str, AttrValue]:
    """The attributes every check-level metric carries: :func:`check_attributes` without ``status``."""
    return {key: value for key, value in check_attributes(check_result).items() if key != "status"}


@dataclass(frozen=True)
class CheckRows:
    """The row numbers of one check.

    Attributes:
        total_rows: The rows of the check's table.
        scalar_count: The check's scalar count. A check that passed within its
            tolerance reports the rows it caught. It can exceed ``total_rows``
            (a join that fans out).
        attributed_rows: The rows of the table the check caught, the number
            the higher levels use. ``0`` for a passed check that is not
            attributed, since it adds nothing to the row counts. ``None`` when
            the check is not attributable.
    """

    total_rows: int
    scalar_count: int
    attributed_rows: int | None


def check_row_counts(result: ValidationResult) -> dict[int, CheckRows]:
    """The :class:`CheckRows` of each check that gets row counts, keyed by ``id(check)``.

    Only the checks the row-quality statistics count get them: a row-level
    check that did not end in ERROR and whose operator identifies bad rows (see
    :func:`~vowl.validation.row_quality.selection.select_check`). Any other
    check, such as an aggregate or a lower bound on a count, gets check counts
    only.

    A passed check is attributed only under ``fetch_tolerated_rows``.
    Otherwise it adds nothing to the row counts, so its ``attributed_rows`` is
    ``0``, as at the dimension and schema levels.
    """
    total_by_schema = {
        item.schema_name: item.total_rows
        for item in result._row_quality_report().schemas
        if item.total_rows is not None
    }

    check_rows = result._row_quality().check_rows()
    counts: dict[int, CheckRows] = {}
    for selection in result._row_quality().selections:
        total = total_by_schema.get(selection.schema_name)
        if not (total and selection.row_level):
            continue
        entry = check_rows.get(id(selection.result))
        attributed = entry.attributed_rows if entry is not None else None
        if attributed is None and not selection.in_scope:
            attributed = 0
        counts[id(selection.result)] = CheckRows(total, selection.scalar_count or 0, attributed)
    return counts


def attributed_pass_rate(rows: CheckRows) -> float | None:
    """Passed rows over the table's rows from the attributed rows, from 0 to 1."""
    if rows.attributed_rows is None:
        return None
    return max(rows.total_rows - rows.attributed_rows, 0) / rows.total_rows


def scalar_pass_rate(rows: CheckRows) -> float:
    """Passed rows over the table's rows from the scalar count. Negative when the count exceeds the table."""
    return (rows.total_rows - rows.scalar_count) / rows.total_rows


def run_row_counts(result: ValidationResult) -> tuple[int, int, bool, int] | None:
    """``(total_rows, failed_rows, approximate, checks_not_attributable)`` for the whole run, ``None`` without row counts.

    Tables hold different rows, so the schema row counts add up. Only schemas
    with row counts take part, and the sum is approximate when any one of
    them is. ``checks_not_attributable`` is summed over every schema.
    """
    rows = [
        item
        for item in result._row_quality_report().schemas
        if item.total_rows is not None and item.failed_rows is not None
    ]
    if not rows:
        return None
    total = sum(item.total_rows or 0 for item in rows)
    failed = sum(item.failed_rows or 0 for item in rows)
    not_attributable = sum(item.checks_not_attributable for item in result._row_quality_report().schemas)
    return total, failed, any(item.approximate for item in rows), not_attributable


def schema_status_counts(result: ValidationResult) -> dict[str, int]:
    """Schemas per status (see :func:`schema_status`), every status present."""
    counts = dict.fromkeys(CHECK_STATUSES, 0)
    for bucket in _checks_by_schema(result).values():
        counts[schema_status(check_status_counts(bucket))] += 1
    return counts


# --------------------------------------------------------------------------- #
# Points
# --------------------------------------------------------------------------- #


class _Points:
    """Collects points under one prefix.

    Two points with the same name and attributes, such as two checks that
    share a name in one schema, are merged the way an OpenTelemetry SDK
    would merge them: counters add up and the last gauge reading wins.
    Histogram readings are all kept. This keeps ``dq_metrics.json`` equal to
    what OpenTelemetry exports.
    """

    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.items: list[MetricPoint] = []
        self._index: dict[tuple[str, tuple[tuple[str, AttrValue], ...]], int] = {}

    def add(self, suffix: str, kind: str, unit: str, value: int | float, attrs: dict[str, Any]) -> None:
        point = MetricPoint(f"{self.prefix}.{suffix}", kind, unit, value, clean_attrs(attrs))
        if kind == HISTOGRAM:
            self.items.append(point)
            return
        key = (point.name, tuple(sorted(point.attributes.items())))
        at = self._index.get(key)
        if at is None:
            self._index[key] = len(self.items)
            self.items.append(point)
        elif kind == COUNTER:
            self.items[at] = replace(point, value=self.items[at].value + value)
        else:
            self.items[at] = point

    def check_counts(self, suffix: str, counts: dict[str, int], attrs: dict[str, Any]) -> None:
        """One counter point per check status, zeros included."""
        for status in CHECK_STATUSES:
            self.add(suffix, COUNTER, "{check}", counts[status], {**attrs, "status": status})

    def row_counts(self, suffix: str, total: int, failed: int, attrs: dict[str, Any], *, clamp: bool = True) -> None:
        """One ``PASSED`` and one ``FAILED`` gauge point, zeros included.

        Both points are always sent so a dashboard showing the latest value
        never keeps a failure count from an earlier run. With ``clamp=False``
        ``PASSED`` is ``total - failed`` as is, negative when ``failed`` exceeds
        ``total``.
        """
        passed = max(total - failed, 0) if clamp else total - failed
        self.add(suffix, GAUGE, "{row}", passed, {**attrs, "status": "PASSED"})
        self.add(suffix, GAUGE, "{row}", failed, {**attrs, "status": "FAILED"})

    def approximate(self, suffix: str, approximate: bool, attrs: dict[str, Any]) -> None:
        """A 0/1 gauge. ``0`` is sent too, so an exact count reads as exact, not unknown."""
        self.add(suffix, GAUGE, "1", int(approximate), attrs)

    def pass_rate(self, suffix: str, rate: float | None, attrs: dict[str, Any]) -> None:
        if rate is not None:
            self.add(suffix, GAUGE, "1", rate, attrs)


def _schema_names(result: ValidationResult) -> list[str]:
    """The run's schemas, then any other schema a check names, in order."""
    names = list(getattr(result, "_schema_names", []) or [])
    for check_result in result.check_results:
        schema = check_result.metadata.get("schema_name")
        if isinstance(schema, str) and schema not in names:
            names.append(schema)
    return names


def _checks_by_schema(result: ValidationResult) -> dict[str, list[CheckResult]]:
    """The run's checks grouped by schema, every schema present."""
    by_schema: dict[str, list[CheckResult]] = {name: [] for name in _schema_names(result)}
    for cr in result.check_results:
        schema = cr.metadata.get("schema_name")
        if isinstance(schema, str):
            by_schema[schema].append(cr)
    return by_schema


def _check_level(points: _Points, result: ValidationResult) -> None:
    # Every check-level metric carries the same attributes, so they can be
    # filtered and joined the same way.
    for cr in result.check_results:
        points.check_counts("check.check.count", check_status_counts([cr]), check_level_attributes(cr))

    for cr in result.check_results:
        points.add("check.duration", HISTOGRAM, "ms", float(cr.execution_time_ms or 0.0), check_level_attributes(cr))

    # row.count holds the attributed rows, like the higher levels, so it never
    # exceeds the table. row.scalar_count holds the scalar count, which can, and
    # is reported as is so an overcount stays visible.
    row_counts = check_row_counts(result)
    rated = [(cr, row_counts[id(cr)]) for cr in result.check_results if id(cr) in row_counts]
    attributed = [(cr, rows) for cr, rows in rated if rows.attributed_rows is not None]
    for cr, rows in attributed:
        points.row_counts("check.row.count", rows.total_rows, rows.attributed_rows, check_level_attributes(cr))
    for cr, rows in attributed:
        points.pass_rate("check.row.pass_rate", attributed_pass_rate(rows), check_level_attributes(cr))
    for cr, rows in rated:
        points.row_counts(
            "check.row.scalar_count", rows.total_rows, rows.scalar_count, check_level_attributes(cr), clamp=False
        )
    for cr, rows in rated:
        points.pass_rate("check.row.scalar_pass_rate", scalar_pass_rate(rows), check_level_attributes(cr))

    # Whether the check made its schema's row counts approximate, the same flag
    # as on its vowl.check span. Sent for every check with a scalar count, since
    # a not-attributable check, the usual cause, has no row.count.
    check_rows = result._row_quality().check_rows()
    for cr, _ in rated:
        entry = check_rows.get(id(cr))
        if entry is not None:
            points.approximate("check.row.approximate", entry.approximate, check_level_attributes(cr))


def _dimension_level(points: _Points, result: ValidationResult) -> None:
    by_dimension: dict[tuple[str, str], list[CheckResult]] = {}
    for cr in result.check_results:
        schema = cr.metadata.get("schema_name")
        if isinstance(schema, str):
            by_dimension.setdefault((schema, check_dimension(cr)), []).append(cr)
    counts = {key: check_status_counts(checks) for key, checks in by_dimension.items()}

    for (schema, dimension), bucket in counts.items():
        points.check_counts("dimension.check.count", bucket, {"schema_name": schema, "dimension": dimension})
    for (schema, dimension), bucket in counts.items():
        points.pass_rate(
            "dimension.check.pass_rate", check_pass_rate(bucket), {"schema_name": schema, "dimension": dimension}
        )

    # Row counts come from the row-quality component, the same numbers as
    # print_summary and get_dq_metrics_df. A bucket without them (no row-level
    # checks) is left out rather than reported as 100%.
    rows = [
        item
        for item in result._row_quality_report().dimensions
        if item.total_rows is not None and item.failed_rows is not None
    ]
    for item in rows:
        attrs = {"schema_name": item.schema_name, "dimension": item.dimension}
        points.row_counts("dimension.row.count", item.total_rows, item.failed_rows, attrs)
    for item in rows:
        attrs = {"schema_name": item.schema_name, "dimension": item.dimension}
        points.pass_rate("dimension.row.pass_rate", item.pass_rate, attrs)
    for item in rows:
        attrs = {"schema_name": item.schema_name, "dimension": item.dimension}
        points.approximate("dimension.row.approximate", item.approximate, attrs)


def _schema_level(points: _Points, result: ValidationResult, counts: dict[str, dict[str, int]]) -> None:
    for schema, bucket in counts.items():
        points.check_counts("schema.check.count", bucket, {"schema_name": schema})
    for schema, bucket in counts.items():
        points.pass_rate("schema.check.pass_rate", check_pass_rate(bucket), {"schema_name": schema})

    # Same rule as the dimension rows: counts need a total and a failed count,
    # the rate also needs a non-empty table.
    rows = [
        item
        for item in result._row_quality_report().schemas
        if item.total_rows is not None and item.failed_rows is not None
    ]
    for item in rows:
        attrs = {"schema_name": item.schema_name}
        points.row_counts("schema.row.count", item.total_rows, item.failed_rows, attrs)
    for item in rows:
        attrs = {"schema_name": item.schema_name}
        points.pass_rate("schema.row.pass_rate", item.pass_rate, attrs)
    for item in rows:
        points.approximate("schema.row.approximate", item.approximate, {"schema_name": item.schema_name})


def _run_level(points: _Points, result: ValidationResult, counts: dict[str, dict[str, int]]) -> None:
    schema_counts = dict.fromkeys(CHECK_STATUSES, 0)
    for bucket in counts.values():
        schema_counts[schema_status(bucket)] += 1
    for status in CHECK_STATUSES:
        points.add("run.schema.count", COUNTER, "{schema}", schema_counts[status], {"status": status})

    run_counts = check_status_counts(result.check_results)
    points.check_counts("run.check.count", run_counts, {})
    points.pass_rate("run.check.pass_rate", check_pass_rate(run_counts), {})

    run_rows = run_row_counts(result)
    if run_rows is not None:
        total, failed, approximate, _ = run_rows
        points.row_counts("run.row.count", total, failed, {})
        points.pass_rate("run.row.pass_rate", max(total - failed, 0) / total if total else None, {})
        points.approximate("run.row.approximate", approximate, {})

    points.add("run.duration", HISTOGRAM, "ms", run_duration_ms(result), {})


def compute_points(result: ValidationResult, *, prefix: str = "vowl") -> list[MetricPoint]:
    """Every DQ metric point of *result*: check, dimension, schema, then run level."""
    points = _Points(prefix)
    schema_counts = {schema: check_status_counts(checks) for schema, checks in _checks_by_schema(result).items()}

    _check_level(points, result)
    _dimension_level(points, result)
    _schema_level(points, result, schema_counts)
    _run_level(points, result, schema_counts)
    return points.items


def _iso(ns: int | None) -> str | None:
    if ns is None:
        return None
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc).isoformat()


def build_document(result: ValidationResult, *, points: Sequence[MetricPoint] | None = None) -> dict[str, Any]:
    """The ``dq_metrics.json`` document of *result*.

    ``run`` holds the run identity attributes, keyed as on the OpenTelemetry
    signals. ``points`` holds one entry per metric point, with the same names,
    types, units and attributes the OpenTelemetry metrics carry.
    """
    from .. import __version__

    if points is None:
        points = compute_points(result)
    return {
        "schema_version": SCHEMA_VERSION,
        "run": run_identity_attributes(result, run_id=result.run_id, version=__version__),
        "run_started_at": _iso(getattr(result, "_run_started_ns", None)),
        "run_finished_at": _iso(getattr(result, "_run_finished_ns", None)),
        "points": [asdict(point) for point in points],
    }
