# Handoff: check-level attributed row metric

## Goal

Make `vowl.check.row.count` and `vowl.check.row.pass_rate` count attributed
rows, like the dimension, schema and run levels already do. Move the scalar
count to a new metric, `vowl.check.row.scalar_count`.

After the change, `vowl.<level>.row.count` means the same thing (attributed
rows) at every level. The scalar count gets a name that says what it is.

## Background

A check has two numbers for the rows it failed:

| Number              | What it is                                                                                                              | Where it lives today                                                                                                              |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| **Scalar count**    | The number the check reports. It decides pass or fail. Every row-level check has one.                                   | `vowl.check.row.count`, `vowl.check.row.pass_rate`, `scalar_count` in `get_row_quality_df(by="check")`, `row.count.*` on the span |
| **Attributed rows** | The rows of the table that failed, found by matching failed rows back to the table. Only attributable checks have them. | `attributed_rows` in `get_row_quality_df(by="check")`, `row.count.failed` on the `vowl.check` span                                |

The two can differ. `DISTINCT` lowers the scalar count, a join that fans out
raises it (it can exceed the table's rows, so `PASSED` goes negative), and
`COUNT(DISTINCT x)` counts values. See
`docs/design-considerations/checks/how-attributed-rows-work.md`, section
"Checks can change the rows they return".

Today a user who wants attributed rows per check as a metric has no way to
get one. The number is only on the span and in the df.

## Proposed behaviour

| Metric                        | Value                                  | Emitted for                                | Clamped                           |
| ----------------------------- | -------------------------------------- | ------------------------------------------ | --------------------------------- |
| `vowl.check.row.count`        | Attributed rows, `PASSED` and `FAILED` | Row-level checks that were attributed      | Yes, like the other levels        |
| `vowl.check.row.pass_rate`    | From attributed rows                   | Same                                       | Yes                               |
| `vowl.check.row.scalar_count` | Scalar count, `PASSED` and `FAILED`    | Every row-level check, attributable or not | No, so an overcount stays visible |

Open questions to settle before coding:

1. **Does `scalar_count` follow the naming pattern?** The pattern is
   `vowl.<level>.<unit>.<measure>` with measure `count`, `pass_rate` or
   `duration` (see `docs/dq-metrics/understanding-metrics.md`, "Metric
   names"). `scalar_count` adds a new measure. Decide whether to keep it, and
   whether a matching `vowl.check.row.scalar_pass_rate` is needed. My lean:
   add `scalar_count` only. A pass rate from the scalar count can go
   negative and is of little use.
2. **Checks that are not attributable.** They get `scalar_count` but no
   `row.count`. Confirm that sending nothing (not `0`) is right. A `0` would
   read as "every row passed". This matches how the higher levels already
   treat them.
3. **Passed checks under the default `attribute_tolerated_rows=False`.**
   They are not attributed (`attribution_note` is `passed, not attributed`). Decide
   whether they get `row.count` with `FAILED` 0, or nothing. `FAILED` 0
   matches the dimension and schema levels, where a passed check adds
   nothing.
4. **`row_counts="scalar"`.** No rows are attributed. Decide whether
   `vowl.check.row.count` is then absent, or falls back to the scalar count
   the way the higher levels do in that mode. Falling back keeps the
   higher levels and the check level consistent within a run.
   **Closed:** `row_counts` was removed (see
   `design/two-tier-row-counts-plan.md`, decision 8), so attribution is the
   only DQ-metrics mode and this case no longer exists.
5. **The span attributes.** `row.count.passed`, `row.count.failed` and
   `row.pass_rate` on the `vowl.check` span mirror the check metrics.
   `design/naming-stocktake.md` row P7 kept these names to avoid breaking
   dashboards. Decide whether they now follow the attributed metric, and
   whether to add `row.scalar_count.*`.

## This is a breaking change

`vowl.check.row.count` changes meaning. The project is pre-1.0 (latest tag
`v0.0.6`), but call it out in the commit message and the docs. The example
Grafana dashboard queries it:
`examples/6_dq_metrics/otel_stack/grafana/build_dashboard.py` around lines
149 and 153, and the generated `dashboards/vowl-dq.json`.

## Where to change

Code:

- `src/vowl/validation/dq_metrics.py`
  - `check_row_counts()` (around line 220) returns `(total, scalar_count)`.
    Add the attributed figure, or a second function. The attributed rows per
    check are in `result._row_quality().check_rows()`, field
    `attributed_rows`.
  - `_check_level()` (around line 355) emits the check metrics. Note
    `clamp=False` on the current row counts.
  - `_Points.row_counts()` and `pass_rate()` for the emit helpers.
- `src/vowl/otel/_common.py`, `check_row_attributes()` (around line 120),
  for the span attributes in question 5.

Tests:

- `tests/test_dq_metrics.py`, the block after the comment "The check level
  reports the check's own scalar count" (around line 172), including
  `test_distinct_reports_the_scalar_at_check_level`.
- `tests/test_otel_export.py`.
- Regenerate example outputs that contain `vowl.check.row.count`:
  `examples/*/outputs/*dq_metrics.json`, `examples/6_dq_metrics/outputs/metrics.json`,
  and re-execute `examples/6_dq_metrics/dq_metrics.ipynb`.

Docs:

- `docs/dq-metrics/understanding-metrics.md`: the "All metrics" table, and
  "How rows are counted" (anchor `#how-failed-rows-are-counted`, keep it).
  The section's "Why the levels use different numbers" part no longer holds
  and should become a short note on what `scalar_count` is for.
- `docs/dq-metrics/json-export.md` around lines 105 and 168.
- `docs/dq-metrics/otel-export.md` around lines 357 to 363.
- `docs/design-considerations/checks/check-results.md`, the "Each grain of
  the DQ metrics uses one number" table at the end of "Attributed rows".
- `docs/design-considerations/checks/how-attributed-rows-work.md`, the
  scalar count row of the table under "Checks can change the rows they
  return", and the grains table around line 51.
- `design/otel-export.md` around lines 264 to 273.

## Writing rules for the docs

- Use the existing terms only: scalar count, attributed rows, failed rows,
  row-level check, attributable, row counts, check counts. Don't introduce
  synonyms.
- Clear, short sentences. Prefer a table where it helps.
- No em-dashes or semicolons in prose.

## Done when

- Tests pass (`uv run pytest`).
- The docs build passes with `mkdocs build --strict`. It isn't in the
  project venv. This works in the sandbox:
  `UV_TOOL_DIR="$TMPDIR/uvtools" uvx --with mkdocs-material mkdocs build --strict -d "$TMPDIR/site"`
- Example outputs and the dashboard are regenerated.
