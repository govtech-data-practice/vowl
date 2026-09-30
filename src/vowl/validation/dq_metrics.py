"""DQ metrics: the check, dimension, schema and run numbers of one run.

One computation feeds every output that reports these numbers: the
OpenTelemetry metrics (:mod:`vowl.otel`) and the ``<prefix>_dq_metrics.json``
file written by :meth:`ValidationResult.save`. Both read the points built here,
so the two can never disagree. See docs/dq-metrics/index.md.

Every metric is named ``<prefix>.<level>.<unit>.<measure>``:

- ``level`` is the slice the reading is for: ``check``, ``dimension``,
  ``schema`` or ``run``.
- ``unit`` is what is counted: ``check``, ``row`` or ``schema``.
- ``measure`` is ``count`` or ``pass_rate``. Timings are ``duration``.

Check and schema counts add up across levels and across runs, so they are
counters. Row counts do not (a row can fail several checks, and a re-run checks
the same rows again), so they are gauges, like every pass rate.

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


def check_row_numbers(result: ValidationResult) -> dict[int, tuple[int, int]]:
    """``(total_rows, failed_rows)`` of each check that gets row numbers, keyed by ``id(check)``.

    Only checks that report failing rows get them. An aggregate check (or one
    that errored) would always show every row as passing, so it is left out.
    """
    # The row-quality totals are uncapped. Fall back to the run's recorded
    # totals when row statistics are off.
    total_by_schema = dict(result._vs.get("total_rows_by_schema", {}) or {})
    for item in result._row_quality_report().schemas:
        if item.total_rows is not None:
            total_by_schema[item.schema_name] = item.total_rows

    numbers: dict[int, tuple[int, int]] = {}
    for cr in result.check_results:
        schema = cr.metadata.get("schema_name")
        total = total_by_schema.get(schema) if schema else None
        if total and cr.status != "ERROR" and cr.supports_row_level_output:
            numbers[id(cr)] = (total, cr.failed_rows_count or 0)
    return numbers


def run_row_numbers(result: ValidationResult) -> tuple[int, int, bool] | None:
    """``(total_rows, failed_rows, exact)`` for the whole run, ``None`` without row numbers.

    Tables hold different rows, so the schema row counts add up. Only schemas
    with row numbers take part, and the sum is exact only when every one of
    them is.
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
    return total, failed, all(item.exact for item in rows)


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

    @property
    def exact_key(self) -> str:
        """Attribute saying whether a row number is exact (``vowl.row_quality.exact``)."""
        return f"{self.prefix}.row_quality.exact"

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

    def row_counts(self, suffix: str, total: int, failed: int, attrs: dict[str, Any]) -> None:
        """One ``PASSED`` and one ``FAILED`` gauge point, zeros included.

        Both points are always sent so a dashboard showing the latest value
        never keeps a failure count from an earlier run.
        """
        self.add(suffix, GAUGE, "{row}", max(total - failed, 0), {**attrs, "status": "PASSED"})
        self.add(suffix, GAUGE, "{row}", failed, {**attrs, "status": "FAILED"})

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

    numbers = check_row_numbers(result)
    rated = [(cr, *numbers[id(cr)]) for cr in result.check_results if id(cr) in numbers]
    for cr, total, failed in rated:
        points.row_counts("check.row.count", total, failed, check_level_attributes(cr))
    for cr, total, failed in rated:
        points.pass_rate("check.row.pass_rate", (total - failed) / total, check_level_attributes(cr))


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

    # Row numbers come from the row-quality component, the same numbers as
    # print_summary and get_row_quality_df. A bucket without them (no counted
    # checks, or statistics off) is left out rather than reported as 100%.
    rows = [
        item
        for item in result._row_quality_report().dimensions
        if item.total_rows is not None and item.failed_rows is not None
    ]
    for item in rows:
        attrs = {"schema_name": item.schema_name, "dimension": item.dimension, points.exact_key: item.exact}
        points.row_counts("dimension.row.count", item.total_rows, item.failed_rows, attrs)
    for item in rows:
        attrs = {"schema_name": item.schema_name, "dimension": item.dimension, points.exact_key: item.exact}
        points.pass_rate("dimension.row.pass_rate", item.pass_rate, attrs)


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
        attrs = {"schema_name": item.schema_name, points.exact_key: item.exact}
        points.row_counts("schema.row.count", item.total_rows, item.failed_rows, attrs)
    for item in rows:
        points.pass_rate(
            "schema.row.pass_rate", item.pass_rate, {"schema_name": item.schema_name, points.exact_key: item.exact}
        )


def _run_level(points: _Points, result: ValidationResult, counts: dict[str, dict[str, int]]) -> None:
    schema_counts = dict.fromkeys(CHECK_STATUSES, 0)
    for bucket in counts.values():
        schema_counts[schema_status(bucket)] += 1
    for status in CHECK_STATUSES:
        points.add("run.schema.count", COUNTER, "{schema}", schema_counts[status], {"status": status})

    run_counts = check_status_counts(result.check_results)
    points.check_counts("run.check.count", run_counts, {})
    points.pass_rate("run.check.pass_rate", check_pass_rate(run_counts), {})

    run_rows = run_row_numbers(result)
    if run_rows is not None:
        total, failed, exact = run_rows
        attrs = {points.exact_key: exact}
        points.row_counts("run.row.count", total, failed, attrs)
        points.pass_rate("run.row.pass_rate", max(total - failed, 0) / total if total else None, attrs)

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
