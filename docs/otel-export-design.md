---
title: "Design Proposal: OpenTelemetry Export"
description: >-
  Proposed design for exporting vowl data-quality validation results to
  OpenTelemetry (metrics, traces, and logs) via an optional [otel] extra.
status: Draft, for review
---

# Design Proposal: OpenTelemetry (OTEL) Export

**Status:** Draft, for review. Nothing here is implemented yet.

## Goal

Let users ship the results of a `vowl` validation run to any OpenTelemetry
backend (Grafana/Tempo/Mimir, Datadog, Honeycomb, New Relic, an OTEL Collector,
etc.) so data-quality outcomes can be dashboarded, alerted on, and correlated
with the pipelines that produced them.

The export must be:

- **Read-only** over the existing `ValidationResult`. It observes a finished run
  and does not change how checks execute.
- **Opt-in and zero-weight when unused.** No OTEL SDK in the core dependency set.
- **Modular by signal.** Metrics, traces, and logs are emitted by independent
  components that can be enabled individually.
- **Composable with a host app's existing OTEL setup**, or self-contained via its
  own OTLP exporter.
- **Aggregation-ready downstream.** The emitted metrics must survive landing in a
  warehouse and support windowed rollups across many runs. See
  [Designing for downstream aggregation](#designing-for-downstream-aggregation-warehouse).

### Scope boundary

vowl **emits** OTEL telemetry. It does **not** define, aggregate into, or POST any
downstream data-quality *report* (e.g. an agency DQ-submission API payload). Report
shape, endpoint, auth, and windowing belong to the consumer. vowl's obligation is
to emit metrics clean enough that such a report can be built downstream, typically
by landing the OTEL data in a warehouse table and rolling it up with SQL. That
constraint drives several design requirements below. The report itself is out of
scope.

## Why it fits cleanly

Everything an exporter needs is already computed and structured on the result
object. No new plumbing through the engine is required.

| Source | What it provides |
| --- | --- |
| [`CheckResult`](https://github.com/govtech-data-practice/vowl/blob/main/src/vowl/executors/base.py) | Per-check `status`, `failed_rows_count`, `execution_time_ms`, and a `metadata` dict (`schema_name`, `engine`, `target`, `dimension`, `severity`, `tags`, `type`, `logical_type`, `operator`, …) |
| `ValidationResult.summary["validation_summary"]` | `total_checks`, `passed`, `failed`, `errors`, `success_rate`, `failed_rows`, `total_execution_time_ms`, `total_rows_by_schema` |
| `ValidationResult._get_schema_validation_breakdown()` | Per-schema check counts, `failed_unique_rows`, `passed_row_percentage` |
| `RowQualitySummary.data_quality` | Per-schema data-quality ratio (0 to 1), a first-class gauge |
| `ValidationResult.contract_id` / `.api_version` | Contract identity for resource attributes |

## Architecture

A single façade dispatches to three independent, single-signal emitters. Each
emitter is responsible for exactly one OTEL signal and can be turned on or off.

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

- **Resource attributes**, attached to every signal:
  `service.name` (configurable, default `vowl`), `vowl.version`,
  `vowl.contract.id`, `vowl.contract.version`, plus any user-supplied
  `resource_attributes`.
- **Common signal attributes** derived once per check and reused across signals:
  `vowl.schema`, `vowl.check_name`, `vowl.dimension`, `vowl.severity`,
  `vowl.engine`, `vowl.status`. Tags are joined and normalised into a bounded
  attribute to avoid unbounded cardinality (see [Cardinality](#cardinality)).
- **Provider resolution** (see [Export path](#export-path)).

### Emitter responsibilities

**`MetricEmitter`**, the primary use case (dashboards and alerting). The
authoritative instrument list is in [Semantic convention](#semantic-convention-v1).
The illustrative subset:

| Instrument | Type | Attributes |
| --- | --- | --- |
| `vowl.checks` | Counter | `status`, `schema`, `dimension`, `severity`, `check_name` |
| `vowl.rejected_rows` | Counter | `schema`, `check_name`, `dimension` |
| `vowl.rows.total` | Counter | `schema` |
| `vowl.check.duration` (ms) | Histogram | `schema`, `engine`, `check_name` |
| `vowl.dq.data_quality` | Gauge | `schema` |
| `vowl.dq.pass_rate` | Gauge | `schema`, `dimension` |

**`TraceEmitter`**, pipeline observability and correlation:

- One root span per run: `vowl.validate` (duration = `total_execution_time_ms`,
  status = OK when `passed` else ERROR).
- One child span per check carrying the check's attributes. Failed checks set
  span status ERROR and record `failed_rows_count`.
- Spans respect any active context, so a run nested inside an instrumented
  orchestrator (Airflow, Dagster, …) attaches to the parent trace automatically.

**`LogEmitter`**, routing failures into a log pipeline:

- One structured log record per FAILED/ERROR check (severity mapping in
  [Decisions](#decisions-locked-for-v1)), body = the check's `details`/message,
  attributes = check metadata.
- Optionally carries a bounded sample of failing rows when the caller opts in
  (see [Failed rows](#failed-rows)).
- Passing checks are not logged.

## Public API (proposed)

The entry point is a method on `ValidationResult`, keeping it consistent with the
existing reporting helpers (`save`, `get_check_results_df`, `print_summary`).

```python
result = validate_data(df, contract)

# 1) Self-contained: vowl configures OTLP exporters and flushes on return.
result.export_otel(
    signals=("metrics", "traces", "logs"),   # any subset. Default is ("metrics", "traces")
    endpoint="http://otel-collector:4317",   # OTLP, or read OTEL_EXPORTER_OTLP_* env
    protocol="grpc",                          # "grpc" or "http/protobuf"
    namespace="vowl",                         # metric/span name prefix, default "vowl"
    service_name="my-pipeline",
    max_failed_rows_sample=0,                 # default 0. no row contents leave the process
    resource_attributes={                     # pass-through, attached to every signal
        "env": "prod",
        "team": "data-platform",
        "vowl.artifact.uri": "s3://dq/run=0f2c9e1a/",   # optional pointer back to saved rows
    },
)

# 2) Ambient: record into the host app's already-configured global providers.
#    vowl ships nothing and flushes nothing, it just emits.
result.export_otel(signals=("metrics", "traces"), use_global_providers=True)

# 3) Advanced: pass your own providers (full control, e.g. custom readers).
result.export_otel(
    metric_provider=my_meter_provider,
    tracer_provider=my_tracer_provider,
)
```

A thin functional form (`vowl.otel.export(result, ...)`) can wrap the same
logic for callers who prefer not to reach through the result object.

### Namespace

Instrument and span names are prefixed by a `namespace` argument that defaults to
`vowl`, so the out-of-the-box names match the
[Semantic convention](#semantic-convention-v1) and every consumer is
interoperable.

```python
result.export_otel(namespace="acme_dq")
# emits acme_dq.checks, acme_dq.rejected_rows, acme_dq.check.duration,
# span acme_dq.validate, and so on
```

The knob exists for collision avoidance (another DQ producer already writing
`vowl.*`, or a house convention) and is meant to be set once at the org level
rather than toggled per run. Downstream warehouse schemas and dashboards key off
whatever namespace is chosen, so a custom value trades interoperability for local
convention. The namespace changes metric and span **names only**. Resource
attribute keys stay `vowl.*` because they identify the producing tool, which is
stable regardless of the metric namespace.

### Export path

`export_otel` resolves providers per signal in this precedence:

1. An explicitly passed provider (`metric_provider=`, …).
2. `use_global_providers=True` → `opentelemetry.metrics.get_meter_provider()` etc.
   vowl records but neither configures nor shuts down exporters (the host owns
   lifecycle).
3. Otherwise vowl builds its own provider with an OTLP exporter from `endpoint`
   or standard `OTEL_EXPORTER_OTLP_*` env vars, emits, then **force-flushes and
   shuts down** so short-lived batch jobs actually deliver before the process
   exits. This flush-on-exit behaviour is the key correctness detail for
   fire-and-forget validation jobs.

### How it works in Python

A caller does three things: install the extra, run a validation, call
`export_otel(...)`. There are three modes.

**Mode A, self-contained (batch jobs, the common case).** vowl builds the OTLP
exporters, emits, force-flushes, and shuts down before returning, so a script
that exits immediately still delivers.

```python
from vowl import validate_data

result = validate_data(df, "contract.yaml")
result.export_otel(
    endpoint="http://otel-collector:4317",   # or set OTEL_EXPORTER_OTLP_ENDPOINT
    signals=("metrics", "traces", "logs"),
    resource_attributes={"env": "prod", "subscription_id": "4998..."},
)
```

**Mode B, ambient.** vowl records into the host application's already-configured
global providers and touches no lifecycle. The host owns flush and shutdown.

```python
result.export_otel(use_global_providers=True)
```

**Mode C, explicit providers.** Full control, for custom readers or a bespoke
pipeline.

```python
result.export_otel(metric_provider=my_mp, tracer_provider=my_tp)
```

In Modes B and C the caller owns the provider, and an OTEL `Resource` is fixed at
provider construction. vowl therefore cannot attach its resource attributes
(`vowl.contract.id`, `vowl.run.id`, `resource_attributes`, ...) to a provider it
did not build. The signal attributes (`schema`, `dimension`, `check_name`, ...)
still attach per point, and the returned `run.id` is still available to the
caller. A caller who needs the vowl resource on a shared provider sets those keys
on the provider's own `Resource` at construction, or uses Mode A.

Under the hood, each emitter walks the finished `ValidationResult` read-only and
records into OTEL instruments. Nothing new runs against the data source.

```python
# Simplified interior of the metric emitter.
meter = meter_provider.get_meter("vowl")
checks = meter.create_counter("vowl.checks", unit="{check}")
rejected = meter.create_counter("vowl.rejected_rows", unit="{row}")

for cr in result.check_results:
    checks.add(1, {
        "status": cr.status,
        "schema": cr.metadata.get("schema_name"),
        # Dimension is read exactly as the check reports it (decision 7):
        # authored rules and auto-generated checks alike. See check_dimension().
        "dimension": check_dimension(cr),
        "check_name": cr.check_name,
    })

for (schema, dimension), unique_failed in rejected_rows_by_dimension(result):
    rejected.add(unique_failed, {"schema": schema, "dimension": dimension})
```

The OTEL SDK batches these points and ships them over OTLP to a collector or
backend. vowl never talks to a warehouse. The collector does that downstream.
Delta temporality is set on the metric reader when vowl builds the provider in
Mode A, and is the host's responsibility in Modes B and C.

## Packaging

Optional extra, mirroring the existing `[spark]` / `[aws]` pattern:

```toml
[project.optional-dependencies]
otel = [
    "opentelemetry-sdk>=1.27.0",
    "opentelemetry-exporter-otlp>=1.27.0",
]
```

- Core install stays lean. `import vowl` never imports OTEL.
- `export_otel` imports the SDK lazily and raises a clear, actionable error if
  the extra is missing (`pip install vowl[otel]`).
- A new `src/vowl/otel/` package holds the façade and the three emitters, and it
  is imported only on demand.

## Semantic convention (v1)

The versioned contract downstream consumers build on. Metric names, attribute
keys, units, and temporality are stable within a major version, and renames go
through a deprecation window. This section supersedes the illustrative table in
[Emitter responsibilities](#emitter-responsibilities). Names below use the default
`vowl` namespace, which a custom `namespace` replaces in instrument and span names
(see [Namespace](#namespace)).

### Resource attributes (all signals)

| Key | Source | Notes |
| --- | --- | --- |
| `service.name` | config, default `vowl` | |
| `vowl.version` | package version | |
| `vowl.run.id` | generated per run (UUID) | lets a consumer de-duplicate retried runs |
| `vowl.contract.id` | contract id | |
| `vowl.contract.version` | contract author's `version` | omitted when absent, never backfilled from `apiVersion` |
| `vowl.contract.api_version` | ODCS spec version (`apiVersion`, e.g. `v3.2.0`) | distinct field from `version` |
| `vowl.contract.status` | ODCS `status` | |
| `vowl.domain` | ODCS `domain` | v3 only, omitted when absent |
| `vowl.data_product` | ODCS `dataProduct` | v3 only, omitted when absent |
| `vowl.tenant` | ODCS `tenant` | omitted when absent |
| (user) | `resource_attributes` arg | pass-through, e.g. `env`, `team`, `subscription_id`, pointers |

User attributes are pure pass-through and attach to every signal. vowl never
inspects or reroutes a key. For pointer keys such as `vowl.artifact.uri`, see
[Correlation and pointers](#correlation-and-pointers).

### Metrics

Temporality is **delta** for all additive instruments, so each run contributes
its own counts and a windowed rollup is a plain sum. Attributes below are in
addition to the resource attributes.

| Metric | Instrument | Unit | Attributes | Windowed rollup |
| --- | --- | --- | --- | --- |
| `vowl.checks` | Counter | `{check}` | `status`, `schema`, `dimension`, `severity`, `check_name` | sum |
| `vowl.rejected_rows` | Counter | `{row}` | `schema`, `dimension` | sum (the numerator) |
| `vowl.rows.total` | Counter | `{row}` | `schema` | sum (the denominator, one contribution per schema per run) |
| `vowl.check.duration` | Histogram | `ms` | `schema`, `engine`, `check_name` | percentiles |
| `vowl.run.duration` | Counter | `ms` | resource only | sum, maps to `PROCESSING_DURATION` |
| `vowl.dq.pass_rate` | Gauge | `1` | `schema`, `dimension` | do NOT aggregate, per-run convenience |
| `vowl.dq.data_quality` | Gauge | `1` | `schema` | do NOT aggregate, per-run convenience |

Rates are convenience gauges for live dashboards and are never the aggregation
primitive. Windowed rates are recomputed downstream from `rejected_rows` and
`rows.total` (see
[Designing for downstream aggregation](#designing-for-downstream-aggregation-warehouse)).

`rejected_rows` counts unique failing rows per `(schema, dimension)`,
deduplicated with the existing row-quality logic partitioned by dimension. A row
failing two checks within the same dimension counts once for that dimension.
Auto-generated checks contribute under the dimension they report (e.g.
`required` -> `completeness`, `unique` -> `consistency`, column-existence ->
`conformity`), same as authored rules (see
[Decisions](#decisions-locked-for-v1)).

### Traces

- Root span `vowl.validate` per run. Duration is the run wall time. Status is OK
  when the run passed and ERROR otherwise. Attributes include `total_checks`,
  `passed`, `failed`, `errors`, `success_rate`.
- Child span `vowl.check` per check. Attributes include `check_name`, `schema`,
  `dimension`, `severity`, `status`, `engine`, `operator`, `query`,
  `expected_value`, `actual_value`, `failed_rows_count`. Span status is ERROR on
  a FAILED or ERROR check. For a timeliness rule, `actual_value` carries the
  freshness timestamp. `query` is the engine-rendered SQL the check actually ran
  (falling back to the authored query), so a failing span shows the exact
  statement. It rides on spans and logs only, never on metric labels, because
  query text is high-cardinality.
- The check's full authored ODCS definition is flattened onto the span as
  `check.definition.*` keys, so an operator can filter and group on any authored
  field, including author-defined `customProperties`. ODCS quality rules are
  open-ended (`customProperties` is explicitly extensible), so a fixed flat
  allowlist could never guarantee completeness; this flatten is a faithful
  projection of the whole rule instead. `customProperties` is a `[{property,
  value}, ...]` array in the spec and is reshaped to `check.definition.custom.<name>`
  keyed by the author's name verbatim (the spec only recommends camelCase and
  enforces no pattern, so `owner` and `Owner` stay distinct); duplicate names are
  last-wins, and an entry with no `property` falls back to its array index.
  Nested object/array values recurse into further dotted subkeys (for example
  `check.definition.custom.sla.tier`) down to a depth cap, below which the
  subtree collapses to a compact JSON string so nothing is lost. Like `query`,
  this rides on spans and logs only, never on metric labels, where its open key
  set would blow up cardinality.
- Spans join any active context, so a run nested inside an instrumented
  orchestrator attaches to the parent trace.
- When the caller opts into a row sample, it rides here as span events (see
  [Failed rows](#failed-rows)).

### Logs

- One record per FAILED or ERROR check (severity mapping in
  [Decisions](#decisions-locked-for-v1)). Body is the check message or details,
  and attributes match the check span. Passing checks are silent.
- Each record carries the `trace_id`/`span_id` of its check span, so an alert
  links back to the trace.
- Carries the opt-in row sample when enabled (see [Failed rows](#failed-rows)).

## Worked example

A run validates one schema `orders` on an ODCS v3 contract with five checks: two
authored `quality` rules (completeness, timeliness), two auto-generated checks
(`required`, `unique`), and one check that fails to run. Two pass, two fail, one
errors.

### Traces cover every check as a timed tree

```
Trace 7f3a…  (joins the active pipeline trace if one exists)
│
└─ SPAN  vowl.validate                              status=ERROR   1840ms
   │   vowl.run.id           = 0f2c9e1a-…        (dedupes retried runs)
   │   vowl.contract.id      = customer_orders
   │   vowl.contract.version = 3.0.0
   │   vowl.contract.api_version = v3.2.0
   │   vowl.domain           = sales
   │   vowl.tenant           = agency-a
   │   total_checks=5  passed=2  failed=2  errors=1  success_rate=40.0
   │
   ├─ SPAN vowl.check  "orders.email not null"       status=ERROR    38ms
   │     schema=orders  dimension=completeness  severity=error
   │     status=FAILED  engine=duckdb  operator=not_null
   │     query='SELECT COUNT(*) FROM "orders" WHERE "email" IS NULL'
   │     failed_rows_count=42
   │     check.definition.custom.owner=risk-team    (author customProperty, keyed by name)
   │     check.definition.custom.sla.minutes=30     (nested value recursed into subkeys)
   │
   ├─ SPAN vowl.check  "orders.updated_at < 24h"      status=OK       51ms
   │     dimension=timeliness  status=PASSED
   │     expected_value="<= 24h"
   │     actual_value=2026-09-23T04:12:00Z        (freshness timestamp rides here)
   │
   ├─ SPAN vowl.check  "orders.order_id required"     status=OK       12ms
   │     dimension=completeness  status=PASSED     (auto-check, own dimension)
   │
   ├─ SPAN vowl.check  "orders.order_id unique"       status=ERROR    44ms
   │     dimension=consistency  status=FAILED  failed_rows_count=3
   │
   └─ SPAN vowl.check  "orders.region in enum"        status=ERROR     9ms
         dimension=conformity  status=ERROR          (the check could not run)
         details="Binder Error: column region not found"
```

The span **status** is ERROR for any FAILED or ERROR check, so a data violation
shows red in a trace UI, while the `status` **attribute** keeps FAILED and ERROR
distinct. Every check appears, passes included.

### Logs cover failures only, at severity, with a backlink

The same run produces three log records. The two PASSED checks are dropped.

```
[WARN]  vowl.check failed: orders.email not null
        trace_id=7f3a…  span_id=a11c…      (click through to the span)
        schema=orders  dimension=completeness  status=FAILED
        check_name=orders.email not null  failed_rows_count=42
        query='SELECT COUNT(*) FROM "orders" WHERE "email" IS NULL'
        check.definition.custom.owner=risk-team
        body="42 rows have null email"

[WARN]  vowl.check failed: orders.order_id unique
        trace_id=7f3a…  span_id=b02d…
        schema=orders  dimension=consistency  status=FAILED  failed_rows_count=3
        body="3 duplicate order_id values"

[ERROR] vowl.check errored: orders.region in enum
        trace_id=7f3a…  span_id=c73e…
        schema=orders  dimension=conformity  status=ERROR
        body="Binder Error: column region not found"
```

The two dirty-data failures land at `WARN`, the broken check at `ERROR`, so an
on-call rule of `severity >= ERROR` catches "vowl is broken" without paging on
routine data issues. Body carries the message, never the 42 failing rows (unless
the caller opts into a sample, see [Failed rows](#failed-rows)).

## Failed rows

Row *counts* are always exported (`failed_rows_count` on spans and logs,
`vowl.rejected_rows` as a metric). Row *contents* are not exported by default.
vowl cannot assume the caller saved or exported the failing rows anywhere, so the
export offers three independent tiers and the caller chooses how much leaves the
process.

1. **Counts only (always on).** You learn that 42 rows failed the completeness
   dimension on `orders`, with no cell values.
2. **A bounded row sample (opt-in, default off).** `max_failed_rows_sample`
   defaults to `0`, so no cell values leave the process unless asked. Set it to N
   and each FAILED/ERROR check attaches up to N sample rows to its **log record**
   (or span event), never to metrics (row values as metric attributes would
   explode cardinality). The sample is double-capped by this flag and by the
   run's existing `max_failed_rows` config, and it reuses the lazy `failed_rows`
   fetch so a caller who leaves it at 0 never pays to materialise rows.
3. **A pointer to your own store (opt-in).** If you saved rows yourself, attach a
   pointer so an alert deep-links to them. See
   [Correlation and pointers](#correlation-and-pointers).

**Sensitivity caveat for tier 2:** sampled rows can contain PII, they land in an
observability backend with telemetry-grade retention, and OTLP has payload size
limits. That is why the default is `0`. Per-column redaction or allowlisting is a
reasonable future addition, out of scope for v1.

## Correlation and pointers

vowl exports counts, not row contents. To get from an alert back to the data or
its context, attach pointers through `resource_attributes`. That bag is pure
pass-through: vowl treats every key as an opaque string, attaches it to every
signal, and never inspects or reroutes any key. Nothing is "recognized," so
behaviour is fully predictable whether or not you follow any naming convention.

The keys below are **naming guidance only**, so different teams spell the same
pointer the same way and it lands as a portable warehouse column. They carry no
special code behaviour.

| Recommended key | Meaning | Example |
| --- | --- | --- |
| `vowl.artifact.uri` | where this run's output was saved, if it was (failed rows, annotated output, report) | `s3://dq/run=0f2c9e1a/` |
| `vowl.link.<name>` | any correlation link, the suffix is yours to name | `vowl.link.runbook`, `vowl.link.ticket`, `vowl.link.pipeline` |

`vowl.run.id` (a resource attribute vowl always sets) already correlates a run
with an artifact your job saved under that id, even when no pointer is set.

Because these are ordinary resource attributes, a per-run-unique value such as
`vowl.artifact.uri` forks the metric series per run, the same as `vowl.run.id`
already does. That suits the warehouse path, where each run is a row. See the
[Cardinality](#cardinality) note for the TSDB case.

## Cardinality

Metric label cardinality is the main operational footgun. Guidance baked into
the defaults:

- `check_name`, `schema`, `dimension`, `severity`, `engine` are bounded by the
  contract → safe as attributes.
- `target` (column-level) is higher cardinality → included on **traces/logs**
  (per-event, cheap) but **off by default on metrics**, gated by an
  `include_target_attribute` flag.
- `tags` are normalised to a sorted, comma-joined string and only attached where
  the contract keeps them bounded.
- Row-level failed-row *contents* are off metrics entirely, and off all signals by
  default. An opt-in bounded sample can ride logs/span events (see
  [Failed rows](#failed-rows)). Full failed-row output stays the job of `save()` /
  `get_annotated_output()`.
- Per-run resource attributes (`vowl.run.id` and any custom keys) fork the metric
  series per run. This is intended for the warehouse path, where each run is a row.
  For a Prometheus/Mimir-style TSDB, drop them with an OTEL Collector
  relabel/transform on that path.

## Testing strategy

- Unit tests per emitter using the OTEL **in-memory** readers/exporters
  (`InMemoryMetricReader`, `InMemorySpanExporter`, in-memory log exporter):
  assert instrument names, values, and attributes against a synthetic
  `ValidationResult`.
- A guard test asserting core `import vowl` does **not** import `opentelemetry`.
- A test asserting the missing-extra path raises the friendly install error.
- Flush/shutdown test: self-contained mode delivers all points before returning.
- A test asserting `max_failed_rows_sample=0` (the default) exports no row
  contents on any signal, and that a positive value is capped by both the flag
  and `max_failed_rows`.

## Designing for downstream aggregation (warehouse)

The DQ **submission** is out of scope (see [Scope boundary](#scope-boundary)), but
the export must be *shaped* so a consumer can aggregate it. The expected common
path, and the one this design optimises for, is a warehouse:

```
vowl runs ──OTLP──▶ OTEL Collector ──exporter──▶ warehouse table ──SQL rollup──▶ consumer's submitter
```

vowl emits OTLP only. The Collector lands it in ClickHouse / BigQuery / Snowflake
via an off-the-shelf exporter. **vowl takes no warehouse dependency and defines no
report.** For that path to actually work, the export must satisfy the following.
These are requirements, not nice-to-haves.

### 1. Delta temporality for additive metrics

Batch validation runs are discrete events. Additive counters (`rejected_rows`,
`total_rows`) are exported with **delta** temporality, so each run contributes
"this run had N" and a warehouse rollup is a plain `SUM(value) … WHERE time
BETWEEN`. Cumulative temporality assumes a continuous process and resets per run,
which is painful to un-pick downstream. *(This is the single most consequential
choice for warehouse friendliness.)*

### 2. Emit components, never pre-divided rates

Rates are not additive: averaging per-run percentages across differently-sized
runs is wrong. Export the **numerator and denominator** as separate counters and
let the consumer recompute `rate = (Σtotal − Σrejected) / Σtotal` over the window.
A convenience per-run rate gauge may also be emitted, but it is never the
aggregation primitive.

### 3. Bounded, stable attribute set = warehouse columns

Downstream `GROUP BY` axes are exactly the OTEL attributes. Warehouse exporters
land them as a map/columns, so the set must be **low-cardinality and stable across
releases**:

```
subscription_id?, contract.id, contract.version, domain, data_product,
tenant, schema, dimension, severity, status
```

High-cardinality fields (row values, per-column `target`) stay off metrics. See
[Cardinality](#cardinality).

`dimension` is an especially safe rollup axis on ODCS v3 contracts, where the
standard **constrains it to a fixed enum**: `accuracy`, `completeness`,
`conformity`, `consistency`, `coverage`, `timeliness`, `uniqueness`. A consumer
grouping by `dimension` gets a stable, standardized set with no normalisation
needed. Two caveats, tracked in [Decisions](#decisions-locked-for-v1):
auto-generated `required` / `unique` / type checks report a dimension too (the
one vowl's generator assigns them, e.g. `completeness` for `required`), which
the exporter passes through, and ODCS v2.2.x does not define it.

### 4. Run identity + timestamp for windowing and dedupe

Each run carries a `vowl.run.id` resource attribute and every point carries its
event time. This lets a consumer window correctly and **de-duplicate retried runs**
(same `run.id`) so a re-run does not double-count.

### 5. A versioned semantic convention

Metric names and attribute keys are a **published, versioned contract** (the tables
in this doc), so warehouse schemas built on them do not break when vowl changes.
Renames go through a deprecation window.

### Result: the consumer's rollup is just SQL

With the above in place, a windowed submission, per subscription over
`[start, end]`, is built entirely downstream on data vowl emitted:

```sql
SELECT dimension,
       SUM(rejected_rows)                                       AS rejected,
       SUM(total_rows)                                          AS total,
       (SUM(total_rows) - SUM(rejected_rows)) / SUM(total_rows) AS rate
FROM   vowl_dq_metrics
WHERE  event_time BETWEEN :start AND :end
  AND  subscription_id = :sub
GROUP  BY dimension;
```

That is exactly the numerator/denominator recomputation a per-dimension report
needs, with no rate ever having been aggregated.

## Decisions (locked for v1)

These are settled and drive the [Semantic convention](#semantic-convention-v1).

1. **All three signals ship in v1.** Metrics, traces, and logs, each an
   independent emitter that can be toggled.
2. **Default enabled signals: metrics and traces on, logs opt-in.** When a caller
   does not pass `signals`, they get metrics and traces. Logs are opt-in because
   per-failure logs can spike in volume on a bad run and many teams already have
   their own logging.
3. **Log severity mapping.** A FAILED check logs at `WARN` (a data issue, expected
   in normal operation), an ERROR check logs at `ERROR` (the check machinery
   broke), and passing checks are silent. This keeps `severity >= ERROR` meaning
   "vowl is broken" so routine data issues do not page like an outage. A consumer
   that wants every failure to page filters on the `status` attribute rather than
   relying on severity.
4. **Failed-row contents are off by default.** `max_failed_rows_sample` defaults
   to `0`, so no cell values leave the process. A positive value attaches a bounded
   sample to logs/span events only (never metrics), double-capped by the flag and
   by `max_failed_rows`. Full failed-row output stays with `save()` /
   `get_annotated_output()`. See [Failed rows](#failed-rows).
5. **Custom attributes are pass-through on all signals.** vowl attaches every
   `resource_attributes` key to every signal and never inspects or reroutes a key.
   The recommended keys (`vowl.artifact.uri`, `vowl.link.<name>`) are naming
   guidance only, with no special code behaviour. See
   [Correlation and pointers](#correlation-and-pointers).
6. **Namespace defaults to `vowl.*` and is configurable.** Tool-scoped by default
   to avoid collisions with other DQ producers writing to the same backend or
   warehouse. A `namespace` argument overrides the prefix for teams that need a
   house convention (see [Namespace](#namespace)).
7. **Dimension is reported by the check and passed through verbatim.**
   `required`, `unique`, type, and column-existence checks export the dimension
   vowl's check generator assigns them (`required` -> `completeness`, `unique`
   -> `consistency`, column-existence -> `conformity`), the same as authored
   `quality` rules export their author-asserted dimension. The exporter does not
   override or rebucket: it reads the dimension the check reports. `"unknown"` is
   used only when a check genuinely reports no dimension. These generator
   mappings are documented and intentional, so per-dimension rejected-row counts
   and rates cover **all** checks, authored and auto-generated alike. (The
   dimension the check reports is vowl's, not an ODCS enum value that a contract
   author asserted, but it is a deliberate, communicated mapping, so downstream
   dimension rollups are more complete than an authored-only view would be.)
8. **No dedicated freshness feature.** Data freshness (the gov payload's
   `TIME_OF_LAST_UPDATE`) is expressed as an ordinary ODCS `quality` rule with
   `dimension: timeliness` (e.g. "`MAX(updated_at)` within 24h"). It flows through
   the normal check path: pass/fail plus the raw timestamp in the check's
   `actual_value`. Since a timestamp is not a numeric metric, the value is carried
   on the check's **span and log record** (attributes), while metrics stay
   numeric. Processing duration (`PROCESSING_DURATION`) is the separate
   `vowl.run.duration` metric derived from `total_execution_time_ms`. Consumers
   needing a freshness value read it from the span/log or submit it out of band.
9. **CLI hook deferred.** vowl has no CLI yet (see [Roadmap](roadmap.md)). Export
   is a Python API in v1. When the planned CLI lands it can add `--otel-*` flags so
   `vowl validate` exports in one command, but nothing in this design depends on
   it.

## Out of scope (v1)

- **Defining or submitting any downstream DQ report** (payload shape, endpoint,
  auth, windowing). vowl emits telemetry, consumers build reports from it. See
  [Scope boundary](#scope-boundary).
- Landing metrics in a warehouse. That is the OTEL Collector's job, and vowl emits
  OTLP only.
- Exporting failed rows in bulk or by default. A bounded opt-in sample is supported
  (`max_failed_rows_sample`, see [Failed rows](#failed-rows)). Unbounded or
  default-on row export is out of scope.
- Per-column redaction/allowlisting of the opt-in row sample.
- A long-running/streaming metrics mode (vowl runs are batch by nature).
- Backend-specific exporters beyond OTLP (Prometheus scrape endpoint, etc.). Users
  can route via an OTEL Collector.
