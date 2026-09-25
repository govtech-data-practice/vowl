---
description: How to export vowl validation results to OpenTelemetry metrics, traces, and logs.
---

# OpenTelemetry Export

`ValidationResult.export_otel(...)` turns a finished validation run into
OpenTelemetry signals so your data-quality results land in the same observability
stack as the rest of your platform. The call reads the finished result only and
returns the generated `vowl.run.id` for correlation.

## Installation

OpenTelemetry is an optional dependency behind the `[otel]` extra:

```bash
pip install 'vowl[otel]'
```

The OTEL packages are loaded lazily on the first `export_otel(...)` call. If the
extra is missing, `export_otel(...)` raises an `ImportError` with install
instructions.

## Quick start

```python
from vowl import validate_data

result = validate_data("contract.yaml", df=df)

run_id = result.export_otel(
    endpoint="http://localhost:4317",   # or leave unset to honour OTEL_EXPORTER_OTLP_* env vars
    signals=("metrics", "traces", "logs"),
    protocol="grpc",                    # or "http/protobuf"
    service_name="orders-dq",
    resource_attributes={"deployment.environment": "prod"},
)
```

Leave `endpoint` unset to fall back to the standard `OTEL_EXPORTER_OTLP_*`
environment variables, which is the usual choice in a deployed pipeline.

## Signals

`signals` selects any subset of `"metrics"`, `"traces"`, and `"logs"`. The default
is `("metrics", "traces")`. **Logs are opt-in** because a bad run can emit one
record per failing check and volume can spike.

| Signal | What vowl emits |
| ------ | --------------- |
| `metrics` | Per-run counts (`total_checks`, `passed`, `failed`, `errors`, `success_rate`) as instruments named under `namespace`. Additive instruments use **delta** temporality so each run contributes its own counts. |
| `traces` | One root `{namespace}.validate` span plus one `{namespace}.check` child span per check, laid out as a timed waterfall. Each check span carries `failed_rows_count`. A run nested inside an instrumented orchestrator attaches to the parent trace. |
| `logs` | One `WARN` record per **FAILED** check and one `ERROR` record per **ERROR** check (the check machinery itself broke). Passing checks are silent. When traces are emitted in the same call, each record carries its check span's `trace_id` and `span_id`. |

## Parameters

| Parameter | Default | Purpose |
| --------- | ------- | ------- |
| `signals` | `("metrics", "traces")` | Which signals to emit. Logs are opt-in. |
| `endpoint` | `None` | OTLP endpoint. When omitted, `OTEL_EXPORTER_OTLP_*` env vars are used. |
| `protocol` | `"grpc"` | `"grpc"` or `"http/protobuf"`. |
| `namespace` | `"vowl"` | Prefix for metric and span **names**. Resource attribute keys stay `vowl.*` regardless. |
| `service_name` | `namespace` | The `service.name` resource attribute. |
| `headers` | `None` | Optional OTLP headers, for example auth. |
| `resource_attributes` | `None` | Additional attributes attached to every signal. See [Correlation and pointers](#correlation-and-pointers). |
| `max_failed_rows_sample` | `0` | Max failing rows attached per check to logs and span events. `0` exports no cell values. |
| `use_global_providers` | `False` | Record into the process's already-configured global providers instead of building OTLP exporters. |
| `metric_provider` / `tracer_provider` / `logger_provider` | `None` | Explicit providers for full control. Take precedence over everything else. |

## Provider resolution

Providers resolve in this order, per signal:

1. **An explicit provider you pass** (`metric_provider` / `tracer_provider` /
   `logger_provider`) is used as-is. vowl does not manage its lifecycle.
2. **`use_global_providers=True`** records into the process globals. vowl does not
   configure or shut anything down.
3. **Otherwise** vowl builds its own provider with an OTLP exporter, force-flushes,
   and shuts it down at the end of the call so a short-lived batch job delivers
   before the process exits.

!!! note "Resource attributes with external providers"
    When you pass your own provider or use `use_global_providers=True`, the signals
    carry **that provider's** resource. `service.name`, `vowl.contract.*`,
    `vowl.run.id`, and your `resource_attributes` are not applied, so set them on
    the provider you construct.

## Failed rows

By default, vowl exports **counts only**. No cell values leave the process. There
are three ways to bridge the gap between an alert and the actual failing data:

1. **`failed_rows_count`** is always present on every check span and every failing
   log record. No cell values are exported.
2. **An inline sample** via `max_failed_rows_sample`. Set a positive value and each
   failing check attaches up to that many rows as `{namespace}.failed_row` span
   events and a `{namespace}.failed_rows_sample` attribute on the log record. The
   count is capped by both this flag and the run's own `max_failed_rows` config.
3. **A pointer** to wherever you saved the rows yourself. See
   [Correlation and pointers](#correlation-and-pointers).

!!! warning "Sampled rows contain real cell values"
    A positive `max_failed_rows_sample` copies actual failing rows into your
    telemetry. Only enable it when your telemetry backend is an acceptable home for
    that data, and mind PII.

## Correlation and pointers

To link an alert back to the actual data, attach a pointer through
`resource_attributes`:

```python
result.save("s3://dq/run=0f2c9e1a/")
result.export_otel(
    resource_attributes={"vowl.artifact.uri": "s3://dq/run=0f2c9e1a/"},
)
```

The recommended key is `vowl.artifact.uri`, pointing at the output
`result.save(...)` persisted for this run. Use `vowl.link.<name>` (for example
`vowl.link.runbook` or `vowl.link.ticket`) for other correlation links.

vowl always sets `vowl.run.id`, so a run already correlates with an artifact saved
under that id even without an explicit pointer.

## Attribute reference

Metric names, attribute keys, units, and temporality are stable within a major
version. Names below use the default `vowl` namespace, which a custom `namespace`
replaces in instrument and span names.

### Resource attributes (all signals)

| Key | Source | Notes |
| --- | --- | --- |
| `service.name` | config, default `vowl` | |
| `vowl.version` | package version | |
| `vowl.run.id` | generated per run (UUID) | de-duplicates retried runs |
| `vowl.contract.id` | contract id | |
| `vowl.contract.version` | contract author's `version` | omitted when absent |
| `vowl.contract.api_version` | ODCS spec version (`apiVersion`, e.g. `v3.2.0`) | distinct field from `version` |
| `vowl.contract.status` | ODCS `status` | |
| `vowl.domain` | ODCS `domain` | v3 only, omitted when absent |
| `vowl.data_product` | ODCS `dataProduct` | v3 only, omitted when absent |
| `vowl.tenant` | ODCS `tenant` | omitted when absent |
| (user) | `resource_attributes` arg | e.g. `env`, `team`, `vowl.artifact.uri` |

### Metrics

Each run reports its own counts independently (delta temporality), so
summing a metric over a time window gives the total for that window. Attributes
below are in addition to the resource attributes.

| Metric | Instrument | Unit | Attributes | Windowed rollup |
| --- | --- | --- | --- | --- |
| `vowl.checks` | Counter | `{check}` | `status`, `schema_name`, `dimension`, `severity`, `check_name` | sum |
| `vowl.failed_rows` | Counter | `{row}` | `schema_name`, `dimension` | sum |
| `vowl.rows.total` | Counter | `{row}` | `schema_name` | sum |
| `vowl.check.duration` | Histogram | `ms` | `schema_name`, `engine`, `check_name` | percentiles |
| `vowl.run.duration` | Counter | `ms` | resource only | sum |
| `vowl.dq.pass_rate` | Gauge | `1` | `schema_name`, `dimension` | do NOT aggregate |
| `vowl.dq.data_quality` | Gauge | `1` | `schema_name` | do NOT aggregate |

The gauge instruments (`pass_rate`, `data_quality`) are per-run convenience values
for live dashboards. For windowed rates, recompute downstream from
`failed_rows` and `rows.total`.

`failed_rows` counts unique failing rows per `(schema_name, dimension)`,
deduplicated per dimension. Auto-generated checks contribute under the
dimension they report (for example `required` maps to `completeness`, `unique`
to `consistency`, column-existence to `conformity`).

### Traces

One root span plus one child span per check, laid out as a timed waterfall.
Spans join any active context, so a run nested inside an instrumented
orchestrator attaches to the parent trace.

#### Root span: `vowl.validate`

One per run. Duration is the run wall time. Span status is `OK` when the run
passed, `ERROR` otherwise.

| Attribute | Description | Presence |
| --- | --- | --- |
| `total_checks` | Total number of checks in the run | Always |
| `passed` | Number of passed checks | Always |
| `failed` | Number of failed checks | Always |
| `errors` | Number of errored checks | Always |
| `success_rate` | Pass rate as a percentage (e.g. `80.0`) | Always |

#### Child span: `vowl.check`

One per check. Span status is `ERROR` for a FAILED or ERROR check, `OK`
otherwise.

| Attribute | Description | Presence |
| --- | --- | --- |
| `check_name` | Name of the check | Always |
| `status` | `PASSED`, `FAILED`, or `ERROR` | Always |
| `schema_name` | Schema the check belongs to | When available |
| `dimension` | DQ dimension (e.g. `completeness`, `consistency`) | Always (falls back to `"unknown"`) |
| `severity` | Check severity (e.g. `error`, `warning`) | When declared |
| `engine` | Execution engine (e.g. `duckdb`) | When available |
| `operator` | Check operator (e.g. `not_null`, `mustBe`) | When available |
| `query` | The engine-rendered SQL the check ran | When available |
| `expected_value` | The expected value or threshold | When available |
| `actual_value` | The actual value observed | When available |
| `failed_rows_count` | Number of rows that failed this check | Always (defaults to 0) |
| `check.definition.*` | Flattened keys from the authored ODCS check definition | When `check_definition` exists in metadata |
| `check.definition.custom.<name>` | Author-defined `customProperties`, keyed by property name | When `customProperties` are declared |

#### Span events: `vowl.failed_row`

When `max_failed_rows_sample > 0`, each failing check span carries up to that
many `{namespace}.failed_row` events. Each event's attributes are the column
names and values of the failing row.

### Logs

One record per FAILED or ERROR check. Passing checks are silent.

| Field | Description |
| --- | --- |
| **Severity** | `WARN` for FAILED checks (data issue), `ERROR` for ERROR checks (check machinery broke) |
| **Body** | The check's detail message, or `"{namespace}.check failed: {check_name}"` |
| **`trace_id`** | Trace ID of the corresponding check span (when traces are emitted) |
| **`span_id`** | Span ID of the corresponding check span (when traces are emitted) |

#### Log record attributes

| Attribute | Description | Presence |
| --- | --- | --- |
| `check_name` | Name of the check | Always |
| `status` | `FAILED` or `ERROR` | Always |
| `schema_name` | Schema the check belongs to | When available |
| `dimension` | DQ dimension | Always (falls back to `"unknown"`) |
| `severity` | Check severity | When declared |
| `engine` | Execution engine | When available |
| `failed_rows_count` | Number of rows that failed this check | Always (defaults to 0) |
| `query` | The engine-rendered SQL the check ran | When available |
| `check.definition.*` | Flattened keys from the authored ODCS check definition | When `check_definition` exists in metadata |
| `check.definition.custom.<name>` | Author-defined `customProperties`, keyed by property name | When `customProperties` are declared |
| `{namespace}.failed_rows_sample` | JSON array of sampled failing rows | When `max_failed_rows_sample > 0` and the check has failing rows |

## Worked example

A run validates one schema `orders` on an ODCS v3 contract with five checks: two
authored `quality` rules (completeness, timeliness), two auto-generated checks
(`required`, `unique`), and one check that fails to run. Two pass, two fail, one
errors.

### Traces

```
Trace 7f3a...  (joins the active pipeline trace if one exists)
|
+- SPAN  vowl.validate                              status=ERROR   1840ms
   |   vowl.run.id           = 0f2c9e1a-...
   |   vowl.contract.id      = customer_orders
   |   vowl.contract.version = 3.0.0
   |   vowl.contract.api_version = v3.2.0
   |   vowl.domain           = sales
   |   vowl.tenant           = agency-a
   |   total_checks=5  passed=2  failed=2  errors=1  success_rate=40.0
   |
   +- SPAN vowl.check  "orders.email not null"       status=ERROR    38ms
   |     schema_name=orders  dimension=completeness  severity=error
   |     status=FAILED  engine=duckdb  operator=not_null
   |     query='SELECT COUNT(*) FROM "orders" WHERE "email" IS NULL'
   |     failed_rows_count=42
   |     check.definition.custom.owner=risk-team
   |     check.definition.custom.sla.minutes=30
   |
   +- SPAN vowl.check  "orders.updated_at < 24h"      status=OK       51ms
   |     dimension=timeliness  status=PASSED
   |     expected_value="<= 24h"
   |     actual_value=2026-09-23T04:12:00Z
   |
   +- SPAN vowl.check  "orders.order_id required"     status=OK       12ms
   |     dimension=completeness  status=PASSED
   |
   +- SPAN vowl.check  "orders.order_id unique"       status=ERROR    44ms
   |     dimension=consistency  status=FAILED  failed_rows_count=3
   |
   +- SPAN vowl.check  "orders.region in enum"        status=ERROR     9ms
         dimension=conformity  status=ERROR
         details="Binder Error: column region not found"
```

### Logs

The same run produces three log records. The two PASSED checks are silent.

```
[WARN]  vowl.check failed: orders.email not null
        trace_id=7f3a...  span_id=a11c...
        schema_name=orders  dimension=completeness  status=FAILED
        check_name=orders.email not null  failed_rows_count=42
        query='SELECT COUNT(*) FROM "orders" WHERE "email" IS NULL'
        check.definition.custom.owner=risk-team
        body="42 rows have null email"

[WARN]  vowl.check failed: orders.order_id unique
        trace_id=7f3a...  span_id=b02d...
        schema_name=orders  dimension=consistency  status=FAILED  failed_rows_count=3
        body="3 duplicate order_id values"

[ERROR] vowl.check errored: orders.region in enum
        trace_id=7f3a...  span_id=c73e...
        schema_name=orders  dimension=conformity  status=ERROR
        body="Binder Error: column region not found"
```

## Aggregating across runs

The metrics are shaped to roll up across many runs for dashboards and periodic
reports. The typical path is:

```
vowl run --OTLP--> OTEL Collector --exporter--> warehouse table --SQL--> dashboard
```

Delta temporality means each run reports its own counts, so summing a metric over a
time window gives the total for that window. `vowl.run.id` and each point's event
time support correct windowing and de-duplication of retried runs.

Example: windowed failed-rows rate per dimension:

```sql
SELECT dimension,
       SUM(failed_rows)                                       AS failed,
       SUM(rows_total)                                        AS total,
       (SUM(rows_total) - SUM(failed_rows)) / SUM(rows_total) AS pass_rate
FROM   vowl_dq_metrics
WHERE  event_time BETWEEN :start AND :end
GROUP  BY dimension;
```

On ODCS v3 contracts, `dimension` is a fixed enum (`accuracy`, `completeness`,
`conformity`, `consistency`, `coverage`, `timeliness`, `uniqueness`), so grouping
by it needs no normalisation.
