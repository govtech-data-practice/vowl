"""
Validation configuration for data quality checks.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from typing import Literal

#: Outputs ``save()`` can write, picked as a list. ``check_results.csv`` and
#: ``summary.json`` are always written and are not in the list:
#:
#: - ``"failed_query_outputs"``       -- one CSV per check under
#:                                       ``<prefix>_checks/``, holding the rows
#:                                       of each FAILED check (and of tolerated
#:                                       checks under ``fetch_tolerated_rows``).
#: - ``"all_query_outputs"``          -- the same folder, holding what every
#:                                       row-level check's row query returned,
#:                                       whatever its status. Replaces
#:                                       ``"failed_query_outputs"``.
#: - ``"consolidated_query_outputs"`` -- the failed rows grouped per table set,
#:                                       with a comma-joined ``check_ids``.
#: - ``"annotated_table"``            -- each table with a ``check_info``
#:                                       column, plus residues.
#: - ``"dq_metrics"``                 -- ``dq_metrics.json``.
SaveOutput = Literal[
    "failed_query_outputs",
    "all_query_outputs",
    "consolidated_query_outputs",
    "annotated_table",
    "dq_metrics",
]

SAVE_OUTPUTS: tuple[str, ...] = (
    "failed_query_outputs",
    "all_query_outputs",
    "consolidated_query_outputs",
    "annotated_table",
    "dq_metrics",
)

#: What ``save()`` writes when no outputs are given: everything, with
#: ``"all_query_outputs"`` in place of ``"failed_query_outputs"`` as the two
#: write the same folder.
DEFAULT_SAVE_OUTPUTS: tuple[str, ...] = (
    "all_query_outputs",
    "consolidated_query_outputs",
    "annotated_table",
    "dq_metrics",
)

#: The ``output_mode`` values v0.0.6 shipped, mapped to the outputs that write
#: the same files.
_DEPRECATED_OUTPUT_MODES: dict[str, tuple[str, ...]] = {
    "failed_rows": ("consolidated_query_outputs",),
    "annotated": ("annotated_table", "dq_metrics"),
    "both": ("consolidated_query_outputs", "annotated_table", "dq_metrics"),
}


def _resolve_outputs(
    outputs: Iterable[str] | None,
    output_mode: str | None,
    *,
    stacklevel: int,
    default: Iterable[str] | None = None,
) -> tuple[str, ...]:
    """Return the outputs to write, checked and in a fixed order.

    ``output_mode`` is the deprecated way to choose them. It maps to a set of
    outputs with a ``FutureWarning``. When neither is given, *default* (or
    :data:`DEFAULT_SAVE_OUTPUTS`) is used.
    """
    if output_mode is not None:
        if outputs is not None:
            raise ValueError("Pass outputs= or the deprecated output_mode=, not both.")
        if output_mode not in _DEPRECATED_OUTPUT_MODES:
            raise ValueError(
                f"Unknown output_mode: {output_mode!r}. output_mode is deprecated, pick outputs= from {list(SAVE_OUTPUTS)}."
            )
        mapped = _DEPRECATED_OUTPUT_MODES[output_mode]
        warnings.warn(
            f"output_mode={output_mode!r} is deprecated, use outputs={list(mapped)!r}. "
            "output_mode will be removed in a future release.",
            FutureWarning,
            stacklevel=stacklevel + 1,
        )
        outputs = mapped
    if outputs is None:
        outputs = DEFAULT_SAVE_OUTPUTS if default is None else default
    if isinstance(outputs, str):
        raise TypeError(f"outputs must be a list of output names, not the string {outputs!r}.")
    chosen = set(outputs)
    unknown = sorted(chosen - set(SAVE_OUTPUTS))
    if unknown:
        raise ValueError(f"Unknown output(s): {unknown}. Expected names from {list(SAVE_OUTPUTS)}.")
    if {"failed_query_outputs", "all_query_outputs"} <= chosen:
        raise ValueError(
            "Pick one of 'failed_query_outputs' and 'all_query_outputs'. They write the same files, "
            "and 'all_query_outputs' already holds the failed rows."
        )
    return tuple(name for name in SAVE_OUTPUTS if name in chosen)


#: Presets controlling the contents of the annotated table's ``check_info``
#: column.  Every preset emits a JSON **array of objects** (uniform type) so
#: consumers parse the same way (``item["check_name"]``); they differ only in
#: how many keys each object carries:
#:
#: - ``"names"``   -- ``[{"check_name": ...}, ...]`` (default; the legacy
#:                    ``check_ids`` content as a list of dicts).
#: - ``"summary"`` -- ``[{check_name, dimension, tags, target}, ...]``.
#: - ``"full"``    -- ``[<full check_definition> + check_name + target, ...]``.
CheckInfoPreset = Literal["names", "summary", "full"]


@dataclass
class ValidationConfig:
    """
    Configuration for a data quality validation run.

    Controls statistics collection and other tunables that apply across
    the entire validation.

    Attributes:
        max_rows_for_statistics: **Deprecated** and has no effect.  Table
            totals are counted without a cap.  Any value other than ``-1``
            emits a ``DeprecationWarning``.
        enable_additional_schema_statistics: **Deprecated** and has no
            effect.  The row counts are computed only when a DQ-metrics
            method asks for them.  Setting it emits a ``DeprecationWarning``.
        max_failed_rows: Maximum number of failed rows to fetch per check
            when deriving row-level failure details.  ``-1`` (default)
            means fetch all failing rows (no cap).
        use_try_cast: When ``True`` (default), CAST expressions in
            generated and user-written SQL checks are converted to
            TRY_CAST, and column-vs-literal comparisons are proactively
            wrapped in TRY_CAST.  This prevents type-mismatch errors
            from aborting a check and surfaces them as failed rows instead.
        outputs: The outputs ``ValidationResult.save()`` writes, as a list
            of :data:`SaveOutput` names. ``None`` (default) means
            :data:`DEFAULT_SAVE_OUTPUTS`, every output except
            ``"failed_query_outputs"``, whose files ``"all_query_outputs"``
            already writes. ``save(outputs=...)`` overrides this per
            call. ``check_results.csv`` and ``summary.json`` are always
            written.
        output_mode: **Deprecated**, use ``outputs``. The v0.0.6 names map
            to outputs with a ``FutureWarning``: ``"failed_rows"`` to
            ``["consolidated_query_outputs"]``, ``"annotated"`` to
            ``["annotated_table", "dq_metrics"]`` and ``"both"`` to all
            three. It will be removed in a future release.
        annotated_check_info: Preset controlling the annotated table's
            ``check_info`` column.  One of ``"names"`` (default),
            ``"summary"``, or ``"full"``.  See :data:`CheckInfoPreset`.
            ``get_annotated_output(check_info=...)`` / ``save(check_info=...)``
            override this per call; when their argument is ``None`` this config
            value is used.
        fetch_tolerated_rows: When ``False`` (default), vowl runs no row
            query for a check that passed, so only the rows of FAILED checks
            are fetched and attributed. The summary still shows every
            check's scalar count. Set to ``True`` to also fetch the rows of
            row-level checks that PASSED within their tolerance. This costs
            an extra query per such check, and can export its table. The
            check's status and ``failed_rows_count`` do not change. Every
            output then holds the rows: annotated output marks
            their ``check_info`` items ``"tolerated": true``,
            ``get_output_dfs()`` adds a ``tolerated`` column, the grouped
            failed-rows view adds ``tolerated_check_ids`` and
            ``show_failed_rows()`` labels them ``(tolerated)``. A row picked
            out only by such a check counts as failing in the DQ metrics.
            OpenTelemetry's ``failed_rows_sample`` still holds failed checks
            only.
    """

    max_rows_for_statistics: int = -1
    enable_additional_schema_statistics: bool | None = None
    max_failed_rows: int = -1
    use_try_cast: bool = True
    outputs: Sequence[SaveOutput] | None = None
    output_mode: str | None = None
    annotated_check_info: CheckInfoPreset = "names"
    fetch_tolerated_rows: bool = False

    def __post_init__(self) -> None:
        self.outputs = list(_resolve_outputs(self.outputs, self.output_mode, stacklevel=3))
        self.output_mode = None
        if self.enable_additional_schema_statistics is not None:
            warnings.warn(
                "enable_additional_schema_statistics is deprecated and has no effect.",
                DeprecationWarning,
                stacklevel=3,
            )
        if self.max_rows_for_statistics != -1:
            warnings.warn(
                "max_rows_for_statistics is deprecated and has no effect.",
                DeprecationWarning,
                stacklevel=3,
            )

    def to_dict(self) -> dict:
        """Return a plain dict representation of the config.

        The deprecated ``enable_additional_schema_statistics`` and
        ``output_mode`` are left out. ``output_mode`` is already resolved into
        ``outputs``.
        """
        data = asdict(self)
        data.pop("enable_additional_schema_statistics", None)
        data.pop("output_mode", None)
        return data
