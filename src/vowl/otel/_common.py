"""Shared helpers for the OTEL exporter: the resource and the attributes.

Everything in :mod:`vowl.otel` imports ``opentelemetry`` at module load, so this
package is imported lazily from :meth:`ValidationResult.export_otel` and never by
``import vowl`` (see the guard test).
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any

from opentelemetry._logs import SeverityNumber

if TYPE_CHECKING:
    from ..validation.result import ValidationResult

#: Attribute values OTEL accepts natively. Anything else is coerced with ``str``.
_NATIVE_ATTR_TYPES = (str, bool, int, float)

#: How deep :func:`flatten_check_definition` recurses into nested object/array
#: values before falling back to a compact JSON string. Bounds attribute
#: fan-out on a span/log from a pathologically nested custom property while
#: still exposing normal nesting as queryable dotted subkeys.
_MAX_FLATTEN_DEPTH = 4

#: Log severity per check status. Passing checks are silent (absent here).
#: FAILED is a data issue (WARN), ERROR means the check machinery broke (ERROR),
#: so a ``severity >= ERROR`` alert catches "vowl is broken" without paging on
#: routine data violations. See the design doc's Decisions section.
_SEVERITY_BY_STATUS: dict[str, tuple[SeverityNumber, str]] = {
    "FAILED": (SeverityNumber.WARN, "WARN"),
    "ERROR": (SeverityNumber.ERROR, "ERROR"),
}


def new_run_id() -> str:
    """A fresh per-run id so a consumer can de-duplicate retried runs."""
    return str(uuid.uuid4())


def coerce_attr(value: Any) -> str | bool | int | float | None:
    """Coerce *value* to an OTEL-safe attribute value, or ``None`` to drop it."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, _NATIVE_ATTR_TYPES):
        return value
    return str(value)


def _clean_attrs(attrs: dict[str, Any]) -> dict[str, str | bool | int | float]:
    """Drop ``None`` values and coerce the rest to OTEL-safe attribute values."""
    cleaned: dict[str, str | bool | int | float] = {}
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


def build_context_attributes(
    result: ValidationResult,
    *,
    service_name: str,
    run_id: str,
    version: str,
    prefix: str = "vowl",
    custom_attributes: dict[str, Any] | None,
) -> dict[str, str | bool | int | float]:
    """Build the context attribute dict attached to every data point.

    Combines run identity (service name, version, run ID), contract identity,
    and user-supplied ``custom_attributes``. Used for signal-level attributes
    on every metric, span, and log record, and additionally for constructing
    the OTEL Resource in self-contained mode.
    """
    attrs: dict[str, Any] = {
        "service.name": service_name,
        f"{prefix}.version": version,
        f"{prefix}.run.id": run_id,
    }
    attrs.update(contract_attributes(result, prefix=prefix))
    if custom_attributes:
        attrs.update(custom_attributes)
    return _clean_attrs(attrs)


def build_resource(
    result: ValidationResult,
    *,
    service_name: str,
    run_id: str,
    version: str,
    prefix: str = "vowl",
    custom_attributes: dict[str, Any] | None,
) -> Any:
    """Build the shared OTEL Resource attached to every signal."""
    from opentelemetry.sdk.resources import Resource

    attrs = build_context_attributes(
        result,
        service_name=service_name,
        run_id=run_id,
        version=version,
        prefix=prefix,
        custom_attributes=custom_attributes,
    )
    return Resource.create(attrs)


def check_dimension(check_result: Any) -> str:
    """Resolve a check's DQ dimension, defaulting to ``"unknown"``.

    Delegates to the resolver the result's dimension rollups use, so every
    signal buckets a check the same way.
    """
    from ..validation.result import _resolve_check_dimension

    return _resolve_check_dimension(check_result)


def check_severity(check_result: Any) -> Any:
    """Resolve a check's severity from metadata or its ``check_definition``."""
    metadata = check_result.metadata
    definition = metadata.get("check_definition") or {}
    return metadata.get("severity") or definition.get("severity")


def check_query(check_result: Any) -> str | None:
    """The SQL a check ran, for span/log diagnostics (``None`` if it has none).

    Prefers ``rendered_implementation`` (the engine-rendered statement actually
    executed, with casts and substitutions applied), falling back to the
    authored ``check_definition["query"]``.  Deliberately kept out of
    :func:`check_attributes`, and so out of metric labels: query text is
    high-cardinality and belongs on traces and logs only.
    """
    metadata = check_result.metadata
    definition = metadata.get("check_definition") or {}
    return metadata.get("rendered_implementation") or definition.get("query")


def _flatten_value(key: str, value: Any, out: dict[str, Any], depth: int) -> None:
    """Flatten *value* under *key* into *out*, recursing into dicts/lists.

    Scalars are coerced; a list whose items are all scalars becomes a native
    OTEL array (stringified only if its element types are mixed, since OTEL
    arrays must be homogeneous); dicts and lists-of-objects recurse with dotted
    or indexed subkeys down to ``_MAX_FLATTEN_DEPTH``, below which the whole
    subtree collapses to one compact JSON string so nothing is lost.
    """
    if value is None:
        return
    if isinstance(value, dict):
        if depth >= _MAX_FLATTEN_DEPTH:
            out[key] = json.dumps(value, default=str)
            return
        for sub_key, sub_value in value.items():
            _flatten_value(f"{key}.{sub_key}", sub_value, out, depth + 1)
        return
    if isinstance(value, (list, tuple)):
        if value and all(not isinstance(item, (dict, list, tuple)) for item in value):
            coerced = [coerce_attr(item) for item in value if coerce_attr(item) is not None]
            if coerced:
                out[key] = coerced if len({type(item) for item in coerced}) == 1 else [str(item) for item in coerced]
            return
        if depth >= _MAX_FLATTEN_DEPTH:
            out[key] = json.dumps(value, default=str)
            return
        for index, item in enumerate(value):
            _flatten_value(f"{key}.{index}", item, out, depth + 1)
        return
    scalar = coerce_attr(value)
    if scalar is not None:
        out[key] = scalar


def _flatten_custom_properties(props: Any, out: dict[str, Any]) -> None:
    """Reshape the ODCS ``customProperties`` array into keyed attributes.

    Each ``{"property": name, "value": ...}`` entry becomes
    ``check.definition.custom.<name>`` keyed by the author's name verbatim (the
    spec only recommends camelCase and enforces no pattern, so ``owner`` and
    ``Owner`` stay distinct). Duplicate names are last-wins; an entry missing
    ``property`` falls back to its array index. Only the ``value`` is carried;
    per-entry ``id``/``description``/``vendor`` are intentionally dropped so the
    projection reads as ``name -> value``.
    """
    if not isinstance(props, (list, tuple)):
        _flatten_value("check.definition.customProperties", props, out, depth=1)
        return
    for index, entry in enumerate(props):
        if isinstance(entry, dict) and entry.get("property") is not None:
            _flatten_value(f"check.definition.custom.{entry['property']}", entry.get("value"), out, depth=1)
        else:
            _flatten_value(f"check.definition.custom.{index}", entry, out, depth=1)


def flatten_check_definition(check_result: Any) -> dict[str, Any]:
    """Flatten a check's full ODCS ``check_definition`` into span/log attributes.

    Produces dotted ``check.definition.*`` keys so an operator can filter and
    group on any authored field, including author-defined ``customProperties``
    (reshaped by name; see :func:`_flatten_custom_properties`). This is a
    faithful-but-opinionated projection of the open-ended ODCS quality rule, not
    a fixed allowlist, so custom and unforeseen fields ride along too. It is
    never called by :func:`check_attributes`, so it never reaches metric labels,
    where its open key set would blow up cardinality.
    """
    definition = check_result.metadata.get("check_definition") or {}
    out: dict[str, Any] = {}
    for key, value in definition.items():
        if key == "customProperties":
            _flatten_custom_properties(value, out)
        else:
            _flatten_value(f"check.definition.{key}", value, out, depth=1)
    return out


def check_attributes(check_result: Any) -> dict[str, str | bool | int | float]:
    """The bounded, low-cardinality attribute set shared across signals.

    Uses bare keys (``check_name``, ``schema_name``, ...) per the
    semantic-convention tables, which are the downstream ``GROUP BY`` axes.
    """
    metadata = check_result.metadata
    return _clean_attrs(
        {
            "check_name": check_result.check_name,
            "status": check_result.status,
            "schema_name": metadata.get("schema_name"),
            "dimension": check_dimension(check_result),
            "severity": check_severity(check_result),
            "engine": metadata.get("engine"),
        }
    )


def severity_for(status: str) -> tuple[SeverityNumber, str] | None:
    """Return ``(SeverityNumber, text)`` for a loggable status, else ``None``."""
    return _SEVERITY_BY_STATUS.get(status)
