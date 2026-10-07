"""
Validation configuration for data quality checks.
"""

from __future__ import annotations

import warnings
from dataclasses import asdict, dataclass
from typing import Literal

#: Output styles ``save()`` can write. These are mutually exclusive modes,
#: not independent toggles, which is why this is an enum rather than a boolean:
#:
#: - ``"annotated"``    -- full in-scope tables with a ``check_info`` column,
#:                          residues and ``dq_metrics.json`` (default).
#: - ``"failed_rows"``  -- grouped failed-rows CSVs only. The cheap mode: no
#:                          table export, no row attribution and no
#:                          ``dq_metrics.json``.
#: - ``"both"``         -- everything ``"annotated"`` writes, plus the
#:                          failed-rows CSVs.
OutputMode = Literal["failed_rows", "annotated", "both"]

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
        output_mode: Selects what ``ValidationResult.save()`` writes.  One of
            ``"annotated"`` (default), ``"failed_rows"``, or ``"both"``.  See
            :data:`OutputMode`.  ``save(output_mode=...)`` overrides this per
            call. When its argument is ``None`` this config value is used.
            ``"failed_rows"`` is the cheap mode for large tables.
        annotated_check_info: Preset controlling the annotated table's
            ``check_info`` column.  One of ``"names"`` (default),
            ``"summary"``, or ``"full"``.  See :data:`CheckInfoPreset`.
            ``get_annotated_output(check_info=...)`` / ``save(check_info=...)``
            override this per call; when their argument is ``None`` this config
            value is used.
        attribute_tolerated_rows: When ``False`` (default), only the rows of
            FAILED checks are attributed, and passed checks add nothing to
            the row counts or the annotated output.  Set to ``True`` to also
            attribute the rows of row-level checks that PASSED within their
            tolerance. Every output then holds them: annotated output marks
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
    output_mode: OutputMode = "annotated"
    annotated_check_info: CheckInfoPreset = "names"
    attribute_tolerated_rows: bool = False

    def __post_init__(self) -> None:
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

        The deprecated ``enable_additional_schema_statistics`` is left out,
        because it has no effect.
        """
        data = asdict(self)
        data.pop("enable_additional_schema_statistics", None)
        return data
