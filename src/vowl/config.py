"""
Validation configuration for data quality checks.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

#: Output styles ``save()`` can write. These are mutually exclusive modes,
#: not independent toggles, which is why this is an enum rather than a boolean:
#:
#: - ``"failed_rows"``  -- existing consolidated failed-rows CSVs (default).
#: - ``"annotated"``    -- full in-scope tables with a ``check_ids`` column
#:                          plus residues; no standalone failed-rows CSVs.
#: - ``"both"``         -- failed-rows CSVs *and* annotated tables.
OutputMode = Literal["failed_rows", "annotated", "both"]


@dataclass
class ValidationConfig:
    """
    Configuration for a data quality validation run.

    Controls statistics collection and other tunables that apply across
    the entire validation.

    Attributes:
        max_rows_for_statistics: Cap on the number of rows counted when
            computing per-schema row statistics.  ``-1`` (default) means
            count all rows with no cap.
        enable_additional_schema_statistics: When ``True`` (default),
            per-schema row counts are included in the validation summary.
            Set to ``False`` to skip row counting entirely.
        max_failed_rows: Maximum number of failed rows to fetch per check
            when deriving row-level failure details.  ``-1`` (default)
            means fetch all failing rows (no cap).
        use_try_cast: When ``True`` (default), CAST expressions in
            generated and user-written SQL checks are converted to
            TRY_CAST, and column-vs-literal comparisons are proactively
            wrapped in TRY_CAST.  This prevents type-mismatch errors
            from aborting a check and surfaces them as failed rows instead.
        output_mode: Selects what ``ValidationResult.save()`` writes.  One of
            ``"failed_rows"`` (default), ``"annotated"``, or ``"both"``.  See
            :data:`OutputMode`.  ``save(output_mode=...)`` overrides this per
            call; when its argument is ``None`` this config value is used.
    """

    max_rows_for_statistics: int = -1
    enable_additional_schema_statistics: bool = True
    max_failed_rows: int = -1
    use_try_cast: bool = True
    output_mode: OutputMode = "failed_rows"

    def to_dict(self) -> dict:
        """Return a plain dict representation of the config."""
        return asdict(self)
