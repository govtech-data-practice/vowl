# Plan: two tiers for counting failed rows

Status: approved in part, not started. Branch `feat/otel-exporter`.
Starts after the handoff work in `design/handoff-check-attributed-row-metric.md` is committed.

## Goal

Two separate tiers, with one consistent rule for each.

| Tier | Methods | Cost | Counts it shows |
|---|---|---|---|
| Basic (default) | `print_summary`, `get_check_results_df`, `summary` / summary.json, `save()` | Check SQL only. Never attributes rows. | Scalar counts. Any sum across checks is labelled "approximate". |
| DQ metrics (opt in) | `get_dq_metrics`, `get_dq_metrics_df`, `export_otel`, `save_dq_metrics` | Attributes rows once, then caches the result. | Attributed counts. The check level also carries `scalar_count`. |

## Decisions

1. "Approximate" is the single word for any count that may not be exact. The DQ-metrics `approximate` flag keeps its name.
2. summary.json: `failed_rows` becomes `failed_rows_approximate`.
3. `print_summary()` never attributes. The per-schema "Passed Rows" line is replaced by "Failed Rows (approximate): N". With lazy totals (step 6) there is no denominator in the basic tier, so the line shows no "/ total".
4. `save()` writes scalar outputs only (check results CSV, row outputs, summary.json). It has no DQ-metrics flag.
5. `save_dq_metrics(output_dir=".", prefix="vowl_results", *, filesystem=None)` is the only writer of `<prefix>_dq_metrics.json`. It shares `OutputDir` and `_safe_filename_component` with `save()`.
6. Check-level `vowl.check.row.count` / `pass_rate` are attributed, and `vowl.check.row.scalar_count` / `scalar_pass_rate` carry the scalar count. The handoff session already did this.
7. `get_row_quality_df(by=)` is renamed `get_dq_metrics_df(by=)`. It is unreleased, so there is no deprecation alias.
8. The `row_counts` config is removed. Attribution is the only DQ-metrics mode.
9. `enable_additional_schema_statistics` and `max_rows_for_statistics` warn that they have no effect, and do nothing else.
10. Table totals are fetched lazily, only when DQ metrics need them, and without a cap.
11. Annotated output (`get_annotated_output`, and through it `save()` in annotated mode) still calls `merge_key()` → `report()`. Another sub-agent owns decoupling that. This plan does not touch it.

## Decided (D): drop `total_rows_by_schema` from summary.json

See "D explained" below. The user chose to drop the key. Step 6 removes it and step 7 adds the changelog line.

## Steps (tests green after each)

1. **Guard test.** Add `tests/test_dq_metrics_laziness.py`.
   - Count calls by patching `RowQuality._compute` (`src/vowl/validation/row_quality/__init__.py:284`), the single place all attribution goes through. Secondary counters: `IbisAdapter.get_total_rows` and `IbisAdapter.export_table_as_arrow`, installed after `validate_data` returns.
   - Basic tier: `print_summary`, `get_check_results_df`, `summary`, `save(mode=...)` make zero `_compute` calls and zero `get_total_rows` calls.
   - DQ tier: `get_dq_metrics`, `get_dq_metrics_df`, `save_dq_metrics`, `export_otel` together make exactly one `_compute` call.
   - Each assertion is a strict `xfail` until the step that fixes it, then the marker is removed.
   - `save()` in annotated mode stays xfail until the annotated-output decoupling lands (decision 11).
   - Build results the way `tests/test_dq_metrics.py:15-56` does (capture the unwrapped `validate_data` at import time).
2. **Summary and print_summary labels.**
   - `runner.py` `_build_summary`: rename `failed_rows` → `failed_rows_approximate`.
   - `result.py` `_get_schema_validation_breakdown` (216-229): stop calling `_row_quality_report()`. Call `_build_schema_breakdown(checks, None)` with a per-schema scalar sum over `failed_rows_count` (the same filter as `runner.py:100-102`).
   - `result_rendering.py`: replace "Passed Rows" (178, `format_passed_rows` 120-141) with "Failed Rows (approximate)". Decide whether the multi-table "Non-unique Failed Rows" line merges into it.
   - Update `tests/test_result_rendering_unit_coverage.py:9-87`, `tests/test_row_quality.py:229` and `:1106`.
3. **save / save_dq_metrics.** Remove the dq_metrics.json write from `save()` (`result.py:1444-1446` and docstring 1349-1351). Add `save_dq_metrics`. Add remote-save coverage next to `tests/test_remote_save.py`.
4. **Rename** `get_row_quality_df` → `get_dq_metrics_df` everywhere (about 54 references in 20 files: src, tests, docs, notebooks, README).
5. **Remove `row_counts`.** The full inventory came from a sub-agent and is summarised here.
   - `config.py`: `RowCounts`, `_ROW_COUNTS`, the field, its validation, the docstrings, and the `to_dict` note. The deprecated flags become warn-only (decision 9).
   - `runner.py:149-153`: drop the `!= "off"` guard (step 6 then removes the call).
   - `row_quality/__init__.py`: `ROUTE_SERVER_SCALAR`, `_scalar_only`, `_run_scalars`, `_recorded_total`, `_disabled_report`, `_key_probes` / `annotation_key` / the probe branch of `merge_key`, the scalar branch of `_leave_unattributed`, and the route checks at 802, 899 and 911-913. Line 211 becomes the `attribute_tolerated_rows` flag alone.
   - `rollup.py`: `RowQualityReport.enabled`, `CheckState.from_scalar`, and the scalar block in `roll_up_bucket` (196-204).
   - `dq_metrics.py:255-257, 437`: reword the "statistics off" comments.
   - `result.py:1270-1273`: the `export_otel` docstring.
   - Tests: delete about 11 scalar/off tests (`test_marks_match_counts.py`, `test_table_attributed_counts.py`, `test_row_quality.py`). Rewrite `test_statistics_turned_off`, `test_config_defaults_to_attributed_counts_and_serialises`, `test_the_deprecated_statistics_flag_sets_row_counts`, and `test_a_total_below_a_checks_rows_is_not_exact`.
   - Docs: `run-settings.md` (including the `#row_counts` anchor and its inbound links), `index.md:23`, `glossary.md:48-54`, `results.md:189`, `counting-mechanisms.md` (the `server_scalar` section and anchor `#route-server-scalar`, linked from `check-results.md:478`), `check-results.md`, `how-attributed-rows-work.md`, `annotating-the-source-table.md`, `capping-failed-rows.md`, README 512-517.
   - Close question 4 in the handoff doc.
6. **Lazy totals.**
   - Remove `get_total_rows_by_schema` from `runner.run`.
   - `_recorded_or_fetched_total` always fetches with no cap. Drop the max_rows branches (865, 872-875).
   - `dq_metrics.py:257` stops reading `_vs["total_rows_by_schema"]` and uses the report totals only.
   - Update `tests/test_validate_unit_coverage.py:1144-1145` and the fake multi-adapter.
   - Apply the outcome of question D.
7. **Docs, examples, changelog.**
   - Glossary: an "Approximate" entry.
   - Document the tier split in `results.md` and `dq-metrics/*`.
   - Regenerate the example outputs. Example notebooks need the sandbox lifted because Jupyter binds local ports.
   - CHANGELOG breaking changes, for behaviour released in v0.0.6:
     - the "Unique Passed Rows" print line is removed
     - `failed_rows` → `failed_rows_approximate`
     - `total_rows_by_schema` (per D)
     - the deprecated statistics flags become no-ops
     - (dq_metrics.json was never released, so moving it to `save_dq_metrics` needs no changelog entry)
   - Docs check: `UV_TOOL_DIR="$TMPDIR/uvtools" uvx --with mkdocs-material mkdocs build --strict -d "$TMPDIR/site"`. Tests: `uv run pytest`.

## D explained

Today `validate()` runs a `SELECT COUNT(*)` per table and records it in `summary["validation_summary"]["total_rows_by_schema"]`. The key shipped in v0.0.6.

Who reads it now:
- Nothing in the basic tier. On this branch `print_summary` already takes totals from the attributed report.
- `row_counts="scalar"`, which step 5 removes.
- `dq_metrics.check_row_counts`, only as a fallback that step 6 removes.

So after steps 5 and 6 the key would only be an output for users, with no reader inside vowl.

Step 6 (lazy totals) means `validate()` no longer knows the totals, so summary.json cannot be filled at validate time. The options:

| Option | Effect |
|---|---|
| Drop the key (recommended) | summary.json becomes purely about check outcomes. Totals live in the DQ-metrics output (`vowl.schema.row.count` and friends), the tier that pays for them. One changelog line. |
| Keep it as `{}` | Stable shape, but an empty dict looks like "zero tables" and misleads readers. |
| Fill it once computed | The key appears only if a DQ-metrics method ran before `save()`. The same call can produce different summary.json files depending on call order. Avoid. |
| Keep counting eagerly (undo B) | Keeps the key at the cost of one COUNT(*) per table in every run. Cheap on DuckDB and Postgres, but can mean a full scan on Spark or files. |
