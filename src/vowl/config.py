"""
Validation configuration for data quality checks.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


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
    """

    max_rows_for_statistics: int = -1
    enable_additional_schema_statistics: bool = True
    max_failed_rows: int = -1
    use_try_cast: bool = True

    def to_dict(self) -> dict:
        """Return a plain dict representation of the config."""
        return asdict(self)
