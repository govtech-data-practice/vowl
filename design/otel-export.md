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
attribute reference, is `docs/dq-metrics/otel-export.md`. The metric
vocabulary shared with `dq_metrics.json`, with the full metric list and a
worked example, is `docs/dq-metrics/index.md`. This document does not repeat
those catalogues.

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
with SQL. The properties that make that rollup work are recorded in
[Design decisions](#design-decisions) below.

## Why it fits cleanly

Everything the exporter needs is already on the result object, so no new
plumbing runs through the engine. `vowl.validation.dq_metrics.compute_points()`
reads these sources once, and both `MetricEmitter` and `dq_metrics.json` use
its points.

| Source                                                                                              | What it provides                                                                                                                                                                                                                                                                                                                 |
| --------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`CheckResult`](https://github.com/govtech-data-practice/vowl/blob/main/src/vowl/executors/base.py) | Per-check `status`, `failed_rows_count`, `execution_time_ms`, and a `metadata` dict (`schema_name`, `engine`, `target`, `dimension`, `severity`, `tags`, `type`, `logical_type`, `operator`, and more). The check counts at every level are counted from these.                                                                  |
| `ValidationResult._row_quality_report()`                                                            | Per-schema and per-dimension failed rows, total rows, pass rates, `approximate` and `checks_not_attributable`, exported as the row-count and row pass-rate gauges. `approximate` and `checks_not_attributable` go on the spans only. The run level adds up the schemas. See [Row-Quality Statistics](row-quality-statistics.md). |
| `ValidationResult._run_started_ns` / `._run_finished_ns`                                            | The run's start and end, for `vowl.run.duration` and the span timestamps                                                                                                                                                                                                                                                         |
| `ValidationResult.run_id`                                                                           | The run ID, set when the run is made                                                                                                                                                                                                                                                                                             |
| `ValidationResult.contract` / `.api_version`                                                        | Contract identity for context attributes: id, name, version, status, domain and more                                                                                                                                                                                                                                             |

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
  `vowl.run.id` is attached to spans and log records but not to metric points
  (see design decision 11).
- **Common check attributes** derived once per check and reused across signals:
  `schema_name`, `check_name`, `dimension`, `severity`, `engine`, `status`.
- **Provider resolution**, described under [Provider resolution](#provider-resolution).

### Emitters

- **`MetricEmitter`** is the primary use case (dashboards and alerting). It
  does no arithmetic of its own. It replays the points of
  `vowl.validation.dq_metrics.compute_points` into OTEL instruments, creating
  one instrument per metric name (see design decision 16). The points are the
  DQ metrics, named `vowl.<level>.<unit>.<measure>` at check, dimension,
  schema and run level. Check and schema counts are counters, because they
  add up across levels and across runs. Row counts and pass rates are per-run
  gauges (see design decision 7). The two durations are histograms because a
  duration is a timing, not something to sum.
- **`TraceEmitter`** covers pipeline observability and correlation. It emits one
  `vowl.validate` root span per run and one `vowl.check` child span per check.
  Only errored checks set span status ERROR, and so does the root span when
  any check errored. Failed checks keep status OK and carry `status=FAILED`
  (see design decision 15). Spans join
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
result = validate_data("contract.yaml", df=df)

# 1) Self-contained: vowl configures OTLP exporters and flushes on return.
result.export_otel(
    signals=("metrics", "traces", "logs"),   # any subset. Default is all three
    endpoint="http://otel-collector:4317",   # OTLP, or read OTEL_EXPORTER_OTLP_* env
    protocol=None,                            # None reads OTEL_EXPORTER_OTLP_PROTOCOL, else "grpc"
    prefix="vowl",                            # metric/span name prefix, default "vowl"
    service_name="orders-dq",
    run_id="nightly-2026-09-29",              # optional. Default is result.run_id
    max_failed_rows_sample=0,                 # default 0. No row contents leave the process
    custom_attributes={                       # pass-through, attached to every signal
        "env": "prod",
        "vowl.artifact.uri": "s3://my-bucket/dq-results/nightly-2026-09-29/",  # optional pointer back to saved rows
    },
)

# 2) Ambient: record into the host app's already-configured global providers.
result.export_otel(signals=("metrics", "traces"), use_global_providers=True)

# 3) Explicit: pass your own providers (full control, custom readers).
result.export_otel(metric_provider=my_meter_provider, tracer_provider=my_tracer_provider)
```

### Prefix

The `prefix` parameter (default `"vowl"`) applies to metric names, span names,
and context attribute keys. Setting it to `"myorg"` changes `vowl.check.check.count` to
`myorg.check.check.count`, `vowl.contract.id` to `myorg.contract.id`, and so on. The knob
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

Resolution is per signal, so a caller can pass a tracer provider and let vowl
build the metrics provider. The endpoint check follows the same rule: it only
raises for a signal vowl has to build a provider for and that has no
`endpoint`, no `OTEL_EXPORTER_OTLP_ENDPOINT`, and no
`OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT`.

Contract identity attributes (`vowl.contract.id` and the rest) and
`custom_attributes` are emitted as signal-level attributes on every metric data
point, span, and log record in all three modes. `vowl.run.id` rides on spans
and log records only (see design decision 11). In mode 3, the same attributes
are also placed on the OTEL `Resource` for backends that surface resource
metadata separately. `vowl.run.id` is on the traces and logs Resource only, not
on the Resource of the meter provider vowl builds. Delta temporality is set on the
metric reader when vowl builds the provider, and is the host's responsibility
in the other two modes.

### How the emitters read the result

Each emitter walks the finished `ValidationResult` read-only and records into
OTEL instruments. Nothing new runs against the data source. The metric emitter,
for instance, adds one `vowl.check.check.count` point per check and sets
`vowl.dimension.row.count` to the deduplicated passing and failing rows per
`(schema_name, dimension)`. The OTEL SDK batches the
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
6. **Additive metrics use delta temporality.** Batch validation runs are
   discrete events, so the check and schema counters and the duration histograms report
   "this run had N", and summing over a time range gives the total for the
   runs in that range. Cumulative temporality assumes a continuous process and
   resets per run, which is painful to unpick downstream. Gauges have no
   temporality, so this choice does not affect row counts or rates.
7. **Row counts are gauges with a `status` label, not counters.** OTEL reserves
   counters for values where a sum is meaningful, and Prometheus asks that
   `sum()` over every attribute make sense. Row counts fail both tests. A row
   that fails two checks is counted under each, and a run that re-validates a
   whole table counts the same rows every run. So each level has one gauge,
   `vowl.{check,dimension,schema,run}.row.count`, with a `PASSED` and a `FAILED`
   point per run. Every level counts attributed rows, clamped to the table.
   The check level also has `vowl.check.row.scalar_count` and
   `vowl.check.row.scalar_pass_rate`, which use the scalar count and are not
   clamped. This is a breaking change: `vowl.check.row.count` used to hold the
   scalar count. The two points sum to the table's row count, which serves as
   the denominator, so no separate total metric is needed. Both points are
   always sent, zeros included, so a latest-value panel never keeps a failure
   count from an earlier run. Summing is still possible where it is sound:
   across schemas (disjoint rows), or deliberately across runs for incremental
   loads, where a windowed rate is `sum(FAILED) / sum(PASSED + FAILED)`. The
   rate gauges are a live-dashboard convenience. Row counts and row pass rates
   are only emitted for checks, dimensions, and schemas that report row-level
   failures. An aggregate check (such as a row count) has no failing rows to
   count, so it would always show every row as passing and mislead. The known
   cost is that a gauge keeps only its last value per export interval, and
   `vowl.run.id` is not a metric attribute. In modes 1 and 2, two runs of the
   same contract inside one interval collapse to the later one. Mode 3 flushes
   per run and is unaffected.
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
   is not a numeric metric, the raw value rides on the check's span as
   `actual_value` while metrics stay numeric. Processing duration is the
   separate `vowl.run.duration` metric.
10. **A CLI hook is deferred.** vowl has no CLI yet (see
    [Roadmap](../docs/roadmap.md)). Export is a Python API. When the planned CLI
    lands it can add `--otel-*` flags, but nothing in this design depends on it.
11. **Identity attributes are signal-level, and the run id stays off metrics.**
    Contract identity (`vowl.contract.id` and the rest) and user-supplied
    `custom_attributes` are attached directly to every metric data point,
    span, and log record. This follows the pattern used by major OTEL
    instrumentation libraries (Flask, Django, and others), which use
    signal-level attributes because they do not own the provider. Without
    this, modes 1 and 2 (explicit and global providers) silently lose the
    identity because the provider's OTEL `Resource` is immutable after
    creation. `vowl.run.id` is the exception. It is a new value every run, so
    on a metric point it would start a new metric series every run, and the
    number of stored series would grow without limit. It goes on spans, log
    records, and (in mode 3) the Resource of the tracer and logger providers,
    which is where a single run is looked up. The meter provider vowl builds
    in mode 3 gets a Resource without it. In mode 3 the other identity
    attributes also go on every Resource for backends that surface resource
    metadata separately.
12. **OTLP/HTTP endpoints get the per-signal path added.** The OTLP/HTTP
    exporters use an explicit `endpoint=` exactly as given and only add
    `/v1/traces`, `/v1/metrics`, or `/v1/logs` when the URL comes from the
    generic `OTEL_EXPORTER_OTLP_ENDPOINT` env var. vowl takes one endpoint for
    all three signals, so for `protocol="http/protobuf"` it appends the right
    path per signal, unless the URL already ends with it. gRPC endpoints are
    passed through unchanged.
13. **A failed delivery warns instead of passing silently.** `force_flush`
    only reports that the flush finished, and the OTEL SDK only logs export
    failures. For the exporters vowl builds itself, vowl watches each export
    result and, after shutdown, raises a `RuntimeWarning` naming the signals
    that did not arrive. Every owned provider is shut down even if an earlier
    one raises. Explicit and global providers are left alone, because their
    owner decides how to handle failures.
14. **The root span uses the real run window, and child spans are laid out
    approximately.** The runner records the wall-clock start and end of a
    run, and the root span uses them. Per-check start times are not recorded,
    only durations, and checks may run in parallel. So child spans keep their
    real durations and are laid out one after another from the run start. If
    that would run past the end of the run (because checks overlapped), every
    child starts at the run start instead. The root span's status is ERROR
    when any check errored (see decision 15). It does not use
    `result.passed`, which ignores ERROR checks.
15. **Span status ERROR means the check broke, not that the data was bad.**
    In OpenTelemetry, span status ERROR marks an operation that failed, and
    backends build error-rate views and alerts on it. A FAILED check ran
    correctly and found bad data, so its span is OK and the `status`
    attribute carries `FAILED`. Only an ERROR check (one that could not run)
    sets span status ERROR, with its message as the status description. The
    root span follows the same rule. This matches the log severities, where
    FAILED is WARN and ERROR is ERROR, so teams alerting on span error rates
    are not paged for ordinary data problems.
16. **One computation feeds the OTEL metrics and `dq_metrics.json`.**
    `vowl.validation.dq_metrics` builds every metric point, with its name,
    type, unit, value and attributes, and imports no OpenTelemetry.
    `MetricEmitter` records those points into instruments, and
    `ValidationResult.save` writes them to `<prefix>_dq_metrics.json`. The
    attribute helpers (`check_attributes`, `contract_attributes`,
    `run_identity_attributes`) live there too and `_common` re-exports them.
    So there is one version of every number to maintain, and a test asserts
    the two outputs match point for point. Spans and log records carry the
    same numbers from the same helpers (see decision 19).
17. **Check counts are counters at every level, and every status is sent.**
    Each check sits in exactly one dimension and one schema, so check counts
    add up from check to run level and across runs, which is what a counter
    is for. The cost is that "failed checks in the latest run" needs
    `increase()` on a cumulative backend. Each reading sends `PASSED`,
    `FAILED` and `ERROR`, zeros included (`add(0)`), so each status series
    exists from the first run. The check pass rates keep `ERROR` in the
    denominator, and `vowl.run.schema.count` gives a schema `FAILED` if any
    check failed, else `ERROR` if any errored, else `PASSED`.
18. **The run id is made when the run runs, not when it is exported.**
    `ValidationResult.run_id` is a UUID set when the result is built at the
    end of a run. `export_otel` and `dq_metrics.json` both default to it, so
    the saved files and the telemetry of one run share an id without the
    caller choosing one first. `export_otel(run_id=...)` still overrides it
    for one export. Setting `result.run_id` changes it for every output.
19. **Spans and logs carry the metrics' numbers under the metrics' names.**
    The run span carries every run-level number (`check.count.*`,
    `check.pass_rate`, `schema.count.*`, `row.count.*`, `row.pass_rate`,
    `row.approximate`, `row.checks_not_attributable`), and each check span and log record carries the
    check-level row counts (`row.count.passed`, `row.count.failed`,
    `row.pass_rate`) and how the check took part in them
    (`row.approximate`, `row.attribution_method`, `row.attribution_note`).
    The approximate flag is not on the metrics, so the spans are where to find
    which check made a number approximate. Each is named like its metric without the level, with
    the status moved into the name because a span holds one value per key.
    Row attributes go only on the checks the check-level row metrics cover,
    so an aggregate or errored check gets no row counts instead of a `0`
    that reads as every row passing. This replaced the earlier
    `failed_rows_count` attribute, which was always sent and matched no
    metric. Every check-level metric also carries the same attributes
    (`check_attributes` without `status`), so a dashboard can filter and join
    them the same way and move from a metric to its span on those keys.

## Context attribute methodology

Context attributes describe the contract and the run. vowl includes a fixed
list of contract fields, each only when it has a value:

| Contract field      | Attribute                   |
| ------------------- | --------------------------- |
| `id`                | `vowl.contract.id`          |
| `name`              | `vowl.contract.name`        |
| `version`           | `vowl.contract.version`     |
| `apiVersion`        | `vowl.contract.api_version` |
| `status`            | `vowl.contract.status`      |
| `contractCreatedTs` | `vowl.contract.created_ts`  |
| `domain`            | `vowl.domain`               |
| `dataProduct`       | `vowl.data_product`         |
| `tenant`            | `vowl.tenant`               |

The list is fixed rather than "every top-level field" on purpose. These fields
identify the contract and do not change from run to run, so they are safe to
put on every metric point. A generic rule would pick up any new top-level
field ODCS adds, including free text, without anyone checking what it does to
metric cardinality. Nested objects and arrays are left out. `kind` is left out
because it is always `"DataContract"`. Users who need another field can pass it
via `custom_attributes`. Adding a field to the list is a deliberate change to
`contract_attributes` in `src/vowl/validation/dq_metrics.py`.

## Cardinality

Metric label cardinality is the main operational footgun. The defaults keep it
bounded:

- `check_name`, `schema_name`, `dimension`, `severity`, and `engine` are bounded
  by the contract, so they are safe as metric attributes. `status` has at most
  three values (`PASSED`, `FAILED`, `ERROR`), and only two on row counts.
- Row-level failed-row contents are off metrics entirely, and off all signals by
  default. An opt-in bounded sample can ride logs and span events. Full
  failed-row output stays the job of `save()` / `get_annotated_output()`.
- High-cardinality per-event values, such as the engine-rendered `query` and the
  flattened `check.definition.*` keys, ride on spans and logs only, never on
  metric labels.
- `vowl.run.id` is never a metric attribute. A new value every run would start
  a new metric series every run. It rides on spans and logs, where one run is
  looked up.
- Contract identity attributes are bounded by the number of contracts, so they
  are safe on metrics. `custom_attributes` are attached to metrics verbatim, so
  their cardinality is the caller's responsibility. A caller who puts a per-run
  value there (such as `vowl.artifact.uri`) forks the metric series per run. For
  a Prometheus/Mimir-style TSDB, drop such keys with an OTEL Collector relabel
  or transform on that path.

## Testing

All in `tests/test_otel_export.py`:

- Each emitter is unit-tested with the OTEL in-memory readers and exporters
  (`InMemoryMetricReader`, `InMemorySpanExporter`, and the in-memory log
  exporter), asserting instrument names, types, values, and attributes against
  a `ValidationResult` from a real run.
- Instrument-type tests assert that check and schema counts are counters and
  row counts and pass rates are gauges at every level, that every check status
  is sent, and that a clean run still sends `FAILED` row-count points of 0.
- A parity test asserts that the OTEL metric points equal the
  `dq_metrics.json` points. `tests/test_dq_metrics.py` covers the computation
  and the file without OpenTelemetry installed.
- A guard test asserts that core `import vowl` does not import `opentelemetry`.
- A test asserts the missing-extra path raises the friendly install error.
- Lifecycle tests assert that only vowl-owned providers are flushed and shut
  down, and that every owned provider is shut down even if one raises.
- Error-path tests assert that an ERROR check logs at ERROR severity and turns
  its own span and the root span ERROR, while a FAILED check leaves every
  span OK.
- Timing tests assert that the runner records the run window and the root span
  uses it.
- Endpoint tests assert the OTLP/HTTP per-signal paths, the protocol and
  per-signal endpoint env vars, the per-signal endpoint check, and the
  `RuntimeWarning` on a failed delivery (using stub exporters, no network).
- Identity tests assert that `vowl.run.id` is on spans and logs but not
  metrics, that it defaults to `result.run_id`, and that a caller-supplied
  `run_id` is used and returned.
- Row-sample tests assert that the default exports no row contents, and that a
  positive `max_failed_rows_sample` is capped by both the flag and the run's
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
