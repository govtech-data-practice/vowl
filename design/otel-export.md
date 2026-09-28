---
title: "Design: OpenTelemetry Export"
description: >-
  Internal design record for exporting vowl data-quality validation results to
  OpenTelemetry (metrics, traces, and logs) via an optional [otel] extra.
status: Implemented
---

# Design: OpenTelemetry (OTEL) Export

Internal design record for `ValidationResult.export_otel(...)`. It captures why
the exporter is built the way it is. The user-facing guide, with the full
attribute reference, a worked example, and the warehouse rollup, is
`docs/otel-export.md`. This document does not repeat that catalogue.

## Goal

Ship the results of a vowl validation run to any OpenTelemetry backend
(Grafana/Tempo/Mimir, Datadog, Honeycomb, New Relic, an OTEL Collector, and so
on) so data-quality outcomes are dashboarded, alerted on, and correlated with the
pipelines that produced them. The export is:

- **Read-only** over a finished `ValidationResult`. It observes a completed run
  and never changes how checks execute.
- **Opt-in and zero-weight when unused.** No OTEL SDK in the core dependency set,
  and `import vowl` never imports `opentelemetry`.
- **Modular by signal.** Metrics, traces, and logs are independent emitters, each
  enabled on its own.
- **Composable** with a host app's existing OTEL setup, or self-contained via its
  own OTLP exporter.
- **Aggregation-ready.** The emitted metrics survive landing in a warehouse and
  support windowed rollups across many runs.

## Scope boundary

vowl emits OTEL telemetry. It does not define, aggregate into, or POST any
downstream data-quality report (for example an agency DQ-submission payload).
Report shape, endpoint, auth, and windowing belong to the consumer. vowl's job is
to emit metrics clean enough that such a report is straightforward to build
downstream, typically by landing the OTLP data in a warehouse and rolling it up
with SQL. The rollup path is covered for users under "Aggregating across runs" in
the guide, and the properties that make it work are recorded in
[Design decisions](#design-decisions) below.

## Why it fits cleanly

Everything the exporter needs is already computed on the result object, so no new
plumbing runs through the engine.

| Source | What it provides |
| --- | --- |
| [`CheckResult`](https://github.com/govtech-data-practice/vowl/blob/main/src/vowl/executors/base.py) | Per-check `status`, `failed_rows_count`, `execution_time_ms`, and a `metadata` dict (`schema_name`, `engine`, `target`, `dimension`, `severity`, `tags`, `type`, `logical_type`, `operator`, and more) |
| `ValidationResult.summary["validation_summary"]` | `total_checks`, `passed`, `failed`, `errors`, `success_rate`, `failed_rows`, `total_execution_time_ms`, `total_rows_by_schema` |
| `ValidationResult._get_schema_validation_breakdown()` | Per-schema check counts, `failed_unique_rows`, `passed_row_percentage` |
| `RowQualitySummary.data_quality` | Per-schema data-quality ratio (0 to 1), a first-class gauge |
| `ValidationResult.contract_id` / `.api_version` | Contract identity for context attributes |

## Architecture

A single façade, `OtelExporter`, dispatches to three independent single-signal
emitters. Each emitter owns exactly one OTEL signal and can be turned on or off.

```
                         ValidationResult
                                │  (read-only)
                                ▼
                 ┌───────────────────────────────┐
                 │        OtelExporter            │  façade / orchestrator
                 │  - builds shared Resource      │
                 │  - resolves providers          │
                 │  - fan-out to enabled emitters │
                 └───────────────────────────────┘
                    │            │             │
        ┌───────────┘            │             └───────────┐
        ▼                        ▼                         ▼
┌──────────────┐        ┌──────────────┐          ┌──────────────┐
│ MetricEmitter│        │ TraceEmitter │          │  LogEmitter  │
│ counters,    │        │ run span +   │          │ structured   │
│ gauges,      │        │ per-check    │          │ log record   │
│ histograms   │        │ child spans  │          │ per failure  │
└──────────────┘        └──────────────┘          └──────────────┘
        │                        │                         │
        └────────────────────────┼─────────────────────────┘
                                 ▼
                    MeterProvider / TracerProvider / LoggerProvider
                    (ambient global  OR  vowl-configured OTLP)
```

### Shared concerns (owned by the façade)

- **Context attributes** attached to every data point: `service.name`,
  `vowl.version`, the `vowl.contract.*` keys, and any user `custom_attributes`.
- **Common check attributes** derived once per check and reused across signals:
  `schema`, `check_name`, `dimension`, `severity`, `engine`, `status`. Tags are
  normalised into a bounded attribute to avoid unbounded cardinality.
- **Provider resolution**, described under [Provider resolution](#provider-resolution).

### Emitters

- **`MetricEmitter`** is the primary use case (dashboards and alerting). It
  records the v1 instruments (`vowl.check.count`, `vowl.check.failed_rows`,
  `vowl.check.duration`, `vowl.dimension.failed_rows`,
  `vowl.dimension.check_pass_rate`, `vowl.dimension.row_pass_rate`,
  `vowl.schema.failed_rows`, `vowl.schema.rows_total`,
  `vowl.schema.check_pass_rate`, `vowl.schema.row_pass_rate`,
  `vowl.run.duration`).
- **`TraceEmitter`** covers pipeline observability and correlation. It emits one
  `vowl.validate` root span per run and one `vowl.check` child span per check.
  Failed checks set span status ERROR and record `failed_rows_count`. Spans join
  any active context, so a run nested inside an instrumented orchestrator
  (Airflow, Dagster, and the like) attaches to the parent trace automatically.
- **`LogEmitter`** routes failures into a log pipeline. It emits one structured
  record per FAILED or ERROR check, carrying the `trace_id`/`span_id` of the
  check span so an alert links back to the trace. Passing checks are silent.

## Public API

The entry point is a method on `ValidationResult`, consistent with the existing
reporting helpers (`save`, `get_check_results_df`, `print_summary`).
`vowl.otel.export(result, ...)` wraps the same logic for callers who prefer a
function over reaching through the result object. The full parameter reference is
in the guide. The design-relevant behaviour follows.

```python
result = validate_data(df, "contract.yaml")

# 1) Self-contained: vowl configures OTLP exporters and flushes on return.
result.export_otel(
    signals=("metrics", "traces", "logs"),   # any subset. Default is ("metrics", "traces")
    endpoint="http://otel-collector:4317",   # OTLP, or read OTEL_EXPORTER_OTLP_* env
    namespace="vowl",                         # metric/span name prefix, default "vowl"
    service_name="orders-dq",
    max_failed_rows_sample=0,                 # default 0. no row contents leave the process
    custom_attributes={                     # pass-through, attached to every signal
        "env": "prod",
        "vowl.artifact.uri": "s3://dq/run=0f2c9e1a/",   # optional pointer back to saved rows
    },
)

# 2) Ambient: record into the host app's already-configured global providers.
result.export_otel(signals=("metrics", "traces"), use_global_providers=True)

# 3) Explicit: pass your own providers (full control, custom readers).
result.export_otel(metric_provider=my_meter_provider, tracer_provider=my_tracer_provider)
```

### Prefix

The `prefix` parameter (default `"vowl"`) applies to metric names, span names,
and context attribute keys. Setting it to `"myorg"` changes `vowl.checks` to
`myorg.checks`, `vowl.contract.id` to `myorg.contract.id`, etc. The knob
exists for orgs with naming conventions for their telemetry, and is meant to be
set once at the org level rather than per run.

### Provider resolution

`export_otel` resolves providers per signal in this precedence:

1. An explicitly passed provider (`metric_provider=`, and so on) is used as-is.
   vowl owns no lifecycle for it.
2. `use_global_providers=True` records into the process globals
   (`opentelemetry.metrics.get_meter_provider()` and friends). vowl neither
   configures nor shuts anything down.
3. Otherwise vowl builds its own provider with an OTLP exporter from `endpoint`
   or the standard `OTEL_EXPORTER_OTLP_*` env vars, emits, then force-flushes and
   shuts down so a short-lived batch job delivers before the process exits. This
   flush-on-exit is the key correctness detail for fire-and-forget validation
   jobs.

Run identity attributes (`vowl.contract.id`, `vowl.run.id`,
`custom_attributes`, and the rest) are emitted as signal-level attributes on
every metric data point, span, and log record in all three modes (see design
decision 11). In mode 3, the same attributes are additionally placed on the
OTEL `Resource` for backends that surface resource metadata separately. Delta
temporality is set on the metric reader when vowl builds the provider, and is
the host's responsibility in the other two modes.

### How the emitters read the result

Each emitter walks the finished `ValidationResult` read-only and records into
OTEL instruments. Nothing new runs against the data source. The metric emitter,
for instance, adds one `vowl.checks` point per check and adds the deduplicated
unique failing rows per `(schema, dimension)` to `vowl.failed_rows`, while
`vowl.rows.total` gets one contribution per schema. The OTEL SDK batches the
points and ships them over OTLP to a collector or backend. vowl never talks to a
warehouse.

## Packaging

An optional extra, mirroring the existing `[spark]` / `[aws]` pattern:

```toml
[project.optional-dependencies]
otel = [
    "opentelemetry-sdk>=1.27.0",
    "opentelemetry-exporter-otlp>=1.27.0",
]
```

- The core install stays lean, and `import vowl` never imports OTEL.
- `export_otel` imports the SDK lazily and raises a clear, actionable error when
  the extra is missing (`pip install vowl[otel]`).
- `src/vowl/otel/` holds the façade and the three emitters, imported only on
  demand.

## Design decisions

The choices that shape the semantic convention and the exporter's behaviour, with
their rationale.

1. **All three signals are supported, each toggleable.** Metrics, traces, and
   logs are independent emitters.
2. **All three signals are on by default.** When a caller does not pass
   `signals`, they get metrics, traces, and logs. Logs emit only on failures
   (FAILED or ERROR checks), so their volume is bounded by the number of failing
   checks, which is less than traces (one span per check, passing or failing).
3. **Log severity separates data issues from broken checks.** A FAILED check logs
   at `WARN` (a data issue, expected in normal operation) and an ERROR check logs
   at `ERROR` (the check machinery broke). Passing checks are silent. This keeps
   `severity >= ERROR` meaning "vowl is broken" so routine data violations do not
   page like an outage. A consumer that wants every failure to page filters on the
   `status` attribute instead.
4. **Failed-row contents are off by default.** `max_failed_rows_sample` defaults
   to `0`, so no cell values leave the process. A positive value attaches a
   bounded sample to logs and span events only, never metrics (row values as
   metric attributes would explode cardinality). The sample is double-capped by
   the flag and by the run's existing `max_failed_rows`, and it reuses the lazy
   `failed_rows` fetch so a caller who leaves it at `0` never pays to materialise
   rows. Full failed-row output stays the job of `save()` /
   `get_annotated_output()`.
5. **Custom attributes are pure pass-through on all signals.** vowl attaches every
   `custom_attributes` key to every signal and never inspects or reroutes a key.
   The recommended keys (`vowl.artifact.uri`, `vowl.link.<name>`) are naming
   guidance only, with no special code behaviour, so behaviour is predictable
   whether or not a caller follows the convention.
6. **Metrics use delta temporality.** Batch validation runs are discrete events,
   so additive counters report "this run had N" and a warehouse rollup is a plain
   `SUM(...) WHERE time BETWEEN`. Cumulative temporality assumes a continuous
   process and resets per run, which is painful to unpick downstream. This is the
   single most consequential choice for warehouse friendliness.
7. **Metrics emit components, never pre-divided rates.** Rates are not additive,
   so vowl exports the numerator (`failed_rows`) and denominator (`rows.total`)
   as separate counters and lets the consumer recompute the windowed rate. The
   per-run rate gauges are a live-dashboard convenience, never the aggregation
   primitive.
8. **Dimension is reported by the check and passed through verbatim.** Authored
   `quality` rules export their author-asserted dimension, and auto-generated
   checks export the dimension vowl's generator assigns them (`required` to
   `completeness`, `unique` to `consistency`, column-existence to `conformity`).
   The exporter never overrides or rebuckets. `"unknown"` appears only when a
   check genuinely reports no dimension. Because the mapping is deliberate and
   documented, per-dimension rejected-row counts and rates cover every check,
   authored and auto-generated alike. On ODCS v3 the dimension is a fixed enum,
   which makes it an especially safe rollup axis. ODCS v2.2.x does not define it.
9. **No dedicated freshness feature.** Data freshness is expressed as an ordinary
   ODCS `quality` rule with `dimension: timeliness` (for example "`MAX(updated_at)`
   within 24h"). It flows through the normal check path, and because a timestamp
   is not a numeric metric, the raw value rides on the check's span and log record
   as `actual_value` while metrics stay numeric. Processing duration is the
   separate `vowl.run.duration` metric.
10. **A CLI hook is deferred.** vowl has no CLI yet (see
    [Roadmap](../docs/roadmap.md)). Export is a Python API. When the planned CLI
    lands it can add `--otel-*` flags, but nothing in this design depends on it.
11. **Run identity attributes are always signal-level.** Contract identity
    (`vowl.contract.id`, `vowl.run.id`, etc.) and user-supplied
    `custom_attributes` are attached directly to every metric data point,
    span, and log record. This follows the pattern used by major OTEL
    instrumentation libraries (Flask, Django, etc.) which use signal-level
    attributes because they do not own the provider. Without this, modes 1 and
    2 (explicit and global providers) silently lose the run identity because
    the provider's OTEL `Resource` is immutable after creation. In mode 3
    (self-contained), the same attributes also go on the Resource for backends
    that surface resource metadata separately.

## Context attribute methodology

Context attributes describe the contract and the run. The inclusion rule is:
**include every first-level scalar field from the contract that has a value.**

Fields beyond the first level (nested objects, arrays) are excluded because
they can have unbounded cardinality. Users who need any of these can pass them
via `custom_attributes`. `kind` is also excluded because it is always
`"DataContract"` and carries zero information.

This rule is mechanical: when a new first-level scalar field is added to ODCS,
it gets a `vowl.*` context attribute. No per-field judgment call needed.

## Cardinality

Metric label cardinality is the main operational footgun. The defaults keep it
bounded:

- `check_name`, `schema_name`, `dimension`, `severity`, and `engine` are bounded
  by the contract, so they are safe as metric attributes.
- Row-level failed-row contents are off metrics entirely, and off all signals by
  default. An opt-in bounded sample can ride logs and span events. Full
  failed-row output stays the job of `save()` / `get_annotated_output()`.
- High-cardinality per-event values, such as the engine-rendered `query` and the
  flattened `check.definition.*` keys, ride on spans and logs only, never on
  metric labels.
- Per-run identity attributes (`vowl.run.id`, `vowl.contract.id`, and any custom
  `custom_attributes`) are signal-level attributes on every data point in all
  provider modes. They fork the metric series per run, which suits the warehouse
  path where each run is a row. For a Prometheus/Mimir-style TSDB, drop them
  with an OTEL Collector relabel or transform on that path.

## Testing

- Each emitter is unit-tested with the OTEL in-memory readers and exporters
  (`InMemoryMetricReader`, `InMemorySpanExporter`, and the in-memory log
  exporter), asserting instrument names, values, and attributes against a
  synthetic `ValidationResult`.
- A guard test asserts that core `import vowl` does not import `opentelemetry`.
- A test asserts the missing-extra path raises the friendly install error.
- A flush/shutdown test asserts the self-contained mode delivers all points
  before returning.
- A test asserts `max_failed_rows_sample=0` (the default) exports no row contents
  on any signal, and that a positive value is capped by both the flag and
  `max_failed_rows`.

## Not included

- **Defining or submitting any downstream DQ report** (payload shape, endpoint,
  auth, windowing). vowl emits telemetry, and consumers build reports from it.
- **Landing metrics in a warehouse.** That is the OTEL Collector's job, and vowl
  emits OTLP only.
- **Bulk or default-on failed-row export.** A bounded opt-in sample is supported
  via `max_failed_rows_sample`. Unbounded or default-on row export is not.
- **Per-column redaction or allowlisting of the opt-in row sample.**
- **A long-running or streaming metrics mode.** vowl runs are batch by nature.
- **Backend-specific exporters beyond OTLP** (a Prometheus scrape endpoint and
  the like). Users point an OTEL Collector at whatever backend they run.
