"""OpenTelemetry export of vowl validation results (metrics, traces, logs).

Optional. Requires the ``[otel]`` extra (``pip install vowl[otel]``) and is
imported lazily from :meth:`vowl.ValidationResult.export_otel`, so ``import
vowl`` never imports ``opentelemetry``. See docs/otel-export.md.
"""

from __future__ import annotations

from ._exporter import DEFAULT_SIGNALS, OtelExporter, export

__all__ = ["OtelExporter", "export", "DEFAULT_SIGNALS"]
