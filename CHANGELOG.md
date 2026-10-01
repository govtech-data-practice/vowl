# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Row-quality statistics**: `result.get_row_quality_df(by="schema" | "dimension" | "check")` reports, for each table, how many rows failed at least one check, how many passed, the pass rate, and whether each number is exact. `by="check"` lists which checks were counted, the route their rows took and the reason a check was left out. `print_summary()`, the OTEL row gauges and annotated output all read the same numbers.
- Row numbers are counted inside the data source where it can (the `pushdown` and `table_match` routes), so they are exact on large tables, count every copy of a duplicated row, and no longer depend on `max_failed_rows`. Tested on DuckDB, SQLite, Spark and Postgres. Other data sources fall back to fetched failed rows and report `exact = false` where a number could be off. See [Handling of Failed Rows](docs/failed-rows.md#how-vowl-counts-failed-rows).
- `ValidationConfig(row_issue_scope="all_violations")` counts rows caught by a check that passed within its tolerance as failed rows. The default, `"failed_checks"`, reports them separately as `tolerated_rows`. Annotated output follows the setting and marks such items with `"tolerated": true`.
- `get_check_results_df()` has a `dimension` column, with `"unknown"` for a check that declares none.
- The OTEL schema and dimension row gauges carry a `vowl.row_quality.exact` attribute.
- **DQ metrics** at four levels, check, dimension, schema and run, named `vowl.<level>.<unit>.<measure>`. New: `vowl.dimension.check.count`, `vowl.schema.check.count`, `vowl.run.check.count`, `vowl.run.check.pass_rate`, `vowl.run.row.count`, `vowl.run.row.pass_rate` and `vowl.run.schema.count`. Check counts send `PASSED`, `FAILED` and `ERROR` every run, zeros included. See [DQ Metrics](docs/dq-metrics/index.md).
- `save()` writes the DQ metrics to `<prefix>_dq_metrics.json`, and `result.get_dq_metrics()` returns them. The file and the OpenTelemetry metrics come from one calculation, with the same names, attributes and values. `summary.json` is unchanged. See [Exporting to dq_metrics.json](docs/dq-metrics/json-export.md).
- `result.run_id`: every run gets an ID when it runs. `dq_metrics.json` and `export_otel()` both use it, so the saved files and the telemetry of one run share it without passing `run_id=`. Set it to use your own ID.
- `examples/6_otel_stack/`: a local OpenTelemetry Collector, Prometheus, Tempo, Loki and Grafana stack with a DQ dashboard for a sample catalog of four contracts in two domains, for checking `export_otel` end to end. The collector also archives every batch to a local S3 bucket with no expiry, queryable with SQL through DuckDB. It has its own Makefile: run `make otel-up` and `make otel-add-vowl-runs` in that folder.

### Changed
- The OTEL pass-rate metrics are renamed from `*_pass_rate` to `*.pass_rate`: `vowl.check.row.pass_rate`, `vowl.dimension.check.pass_rate`, `vowl.dimension.row.pass_rate`, `vowl.schema.check.pass_rate` and `vowl.schema.row.pass_rate`. `vowl.check.count` is now `vowl.check.check.count`, so every name follows `vowl.<level>.<unit>.<measure>`.
- The OTEL `vowl.validate` span's `total_checks`, `passed`, `failed`, `errors` and `success_rate` attributes are now `check.count.passed`, `check.count.failed`, `check.count.error` and `check.pass_rate`. The pass rate is from 0 to 1 instead of a percentage.
- The OTEL spans and log records carry the DQ metrics' numbers under the metrics' names, without the level. The `vowl.validate` span adds `schema.count.passed`, `schema.count.failed`, `schema.count.error`, `row.count.passed`, `row.count.failed`, `row.pass_rate` and `vowl.row_quality.exact`. Each `vowl.check` span and log record replaces `failed_rows_count` with `row.count.passed`, `row.count.failed` and `row.pass_rate`, sent only for the checks that get `vowl.check.row.count`. An aggregate or errored check no longer reports a `0` that reads as every row passing.
- Every check-level metric carries the same attributes: `check_name`, `schema_name`, `dimension`, `severity` and `engine`. `vowl.check.row.count` and `vowl.check.row.pass_rate` add `severity` and `engine`, and `vowl.check.duration` adds `dimension` and `severity`.
- The guides are regrouped: [DQ Metrics](docs/dq-metrics/index.md) holds the metric vocabulary shared by [Exporting to OpenTelemetry](docs/dq-metrics/otel-export.md) and [Exporting to dq_metrics.json](docs/dq-metrics/json-export.md). Investigating Failed Rows is now [Handling of Failed Rows](docs/failed-rows.md), under How It Works.
- The summary's **Unique Passed Rows** line under **Single Table** is now **Passed Rows** under each schema's **Overall** block. It counts physical rows, so a duplicated failing row counts once per copy, and it includes checks across tables whose failed rows can be found in the table. It shows `(approx.)` when a number is not exact, and `N/A` when no check was counted.
- The OTEL `vowl.schema.row.count`, `vowl.schema.row.pass_rate`, `vowl.dimension.row.count` and `vowl.dimension.row.pass_rate` values change to the same physical-row numbers. A pass rate is no longer sent for an empty table, or for a dimension without a counted check, instead of reporting 100%.
- Checks whose operator does not mark the matched rows as bad, such as `mustBeGreaterThan` or `mustBe: 5`, are no longer counted in the row numbers and no longer marked on annotated tables or kept as residues. Their verdict is in the summary as before.
- `max_rows_for_statistics` no longer caps the row numbers.
- Annotated output now merges a check whose failed rows hold only the table's declared primary key columns, when the row numbers count it that way: the key is unique and the data source counts rows for vowl. Before, such a check was counted but became a residue.
- A unique declared primary key is now the match key for every check of its table, not only when a check returns just some of the columns. The numbers are the same, the queries are smaller, and values outside the key are no longer compared. When the key has a repeated value, or vowl can't check it, a check that holds the key but not every column reports `primary key has duplicate values` or `primary key uniqueness could not be checked` in `get_row_quality_df(by="check")`.
- A cross-table check whose tables share one connection now runs in that database even when adapters have filter conditions. Each table keeps the filters of the adapter that reads it, so the answer matches a run on local copies. Previously any filter condition made vowl download every table in the join. For these checks `rendered_implementation` now shows the filter subqueries that ran. A custom adapter without `with_filter_conditions` still downloads the tables when their filters differ.
- Schemas given the same `PooledAdapter`, such as `adapter=pooled` on a contract with several schemas, now run their single-table checks side by side through the pool, up to `max_concurrency` in total. Previously the pool ran one schema's checks at a time.
- A cross-table check whose tables are all served by one `PooledAdapter` now runs in the database on one of the pool's connections, with each table's filter conditions. Previously vowl downloaded every table in the join. A join across two pools, or between a pool and another adapter, is still copied. See [PooledAdapter: Joins Across Pools Are Copied](docs/known-issues.md#pooledadapter-joins-across-pools-are-copied).
- `DataSourceMapper.get_adapter` given an adapter other than `IbisAdapter` now raises a `TypeError` that says to pass the adapter to `validate_data` as it is. The old message, `Only IbisAdapter is supported`, was wrong since `validate_data` accepts any adapter.

### Fixed
- A composite primary key, two or more properties with `primaryKey: true`, is now checked as one key. vowl generates one `<schema>_<col1>_<col2>_primary_key_check`, with its columns in `primaryKeyPosition` order, that fails rows with a NULL in any key column and rows whose key tuple repeats. Previously every key column got its own `<col>_primary_key_check`, so valid data failed whenever one column repeated on its own. A single-column key keeps its check unchanged. The per-column check names of a composite key are gone, so update any dashboards or alerts that use them. On SQL Server, BigQuery and Trino the check uses `EXISTS` in place of a row-value `IN`.
- A SQL check that joins a table the contract doesn't declare, such as a lookup table, now reads it through the adapter of the schema the check sits under. That is the same connection a check reading the table alone already used. Previously such a join always failed with `No adapter configured for table '...'`, and so did a foreign key to an external table with no adapter registered. The check now runs when that connection can see the table, and otherwise errors with the database's own "table not found" message.
- `validate_data` now accepts a `PooledAdapter` or a custom `BaseAdapter` subclass through `adapter=` or in `adapters={...}`. Previously it raised `TypeError: Unsupported adapter type: PooledAdapter` unless you wrapped the adapter in a `MultiSourceAdapter` yourself.
- A `PooledAdapter` shared by several schemas now keeps to `max_concurrency` connections in total. Previously each schema's copy of the pool kept its own count, so the copies together could open more connections than the limit.
- A `PooledAdapter` and an `IbisAdapter` on the same connection no longer route a join between their tables to the database through the pool, which could fail. The join is copied to a local DuckDB.
- Capped `sqlglot` at `<30.18`. With sqlglot 30.18.0 or newer, ibis's DuckDB `create_table` emits an incomplete `DROP VIEW/TABLE IF EXISTS`, so DataFrame and Arrow input (ibis 12.0.0) and multi-source checks (ibis 11.x and 12.0.0) failed with `Parser Error: syntax error at end of input`.
- A check whose scalar query aliases its count, such as `SELECT COUNT(*) AS n`, now derives a failed-rows query. Before, it was dropped from the row numbers with `exact = false` and marked nothing on annotated output.
- The failed-rows query derived from a `COUNT(col)` check now adds `col IS NOT NULL`, so it returns only the rows the count counts. Before, it also returned the NULL rows, and annotated output flagged them.
- `PooledAdapter.get_total_rows` now leases an instance from the pool instead of using the primary one, which another thread may be using. Building annotated output from several threads over one DuckDB pool no longer crashes.
- The `max_failed_rows` cap is no longer skipped when a failed-rows query merely mentions `LIMIT`, for example in a `credit_limit` column, a string literal or a subquery. Only an outer `LIMIT`, `TOP` or `FETCH` now counts.
- `get_annotated_output()` now raises when `max_failed_rows=0`, instead of silently marking every row as passing.
- A failed-rows fetch that returns zero rows now keeps its column names, so the check is still recognised as mergeable.
- Annotated output now flags every copy of a failing row that holds NaN, keeps `-0.0` apart from `0.0`, supports list, struct, map and fixed-size array columns, and keeps nanosecond timestamps and `UBIGINT` values above 2^63 with their original types. Previously NaN rows were never flagged, a `-0.0` failure also flagged `0.0` rows, nested columns raised, and nanosecond timestamps were truncated so only one copy matched.

## [0.0.6] - 2026-09-21

### Added
- **ODCS v3.2.0 support**: the official v3.2.0 JSON schema is bundled and registered as the newest supported apiVersion. v3.2.0 is a strict additive superset of v3.1.0 (the DataQuality definition is unchanged), so contracts declaring `apiVersion` v3.2.0 validate and run with no engine changes. Adds coverage for 3.2-only fields (`context`, `synonyms`, `deprecated`, `enum`, `semanticType`, and the new `vector` logicalType) (#60).
- **Enum value-set auto-check**: any property declaring `enum` (the first-class property field introduced in ODCS v3.2.0) now generates a "value must be in the allowed set" check. Non-NULL values outside the allowed set are counted (NULLs stay with the required check), and allowed values are built as SQL literals rather than interpolated (#61).
- **Relationships / foreignKey auto-checks**: ODCS `relationships` (type `foreignKey`, available since ODCS v3.1.0) now generate executed referential-integrity checks at property and schema level, as `NOT EXISTS` anti-joins. Composite and self-referential keys, MATCH SIMPLE null semantics, and shorthand / fully-qualified / external-file reference resolution are supported. Failed rows project the referencing table's columns so they merge onto its annotated output, and an unresolvable reference surfaces as a per-check `ERROR` (carrying the reason) rather than aborting the whole run. External FK targets are recognised as valid adapter keys, so registering the adapter for a cross-file target no longer warns spuriously (#61).
- **Native array-type auto-checks**: array properties (`logicalType: array`, available since ODCS v3.1.0) now generate cardinality checks (`minItems` / `maxItems` / `uniqueItems`) and element validation from the `items` sub-schema (element type, options, and enum). NULL arrays are skipped, and non-array properties carrying array keys degrade to an unsupported check (#61).

### Docs
- Completed the README and `SECURITY.md` governance sections (#57).
- Updated the security acknowledgement target to 5 working days (#58).

### Security
- Closed SQL replacement-scan and DNS-rebinding SSRF gaps in contract and reference resolution (#59).
- Remediated verified security scanner findings (#55).

### Dependencies
- Bumped `tornado` from 6.5.7 to 6.5.8 (#53).
- Bumped `pymdown-extensions` (#52).
- Bumped `cryptography` (#51).
- Bumped `setuptools` from 82.0.1 to 83.0.0 (#49).
- Bumped `pillow` from 12.2.0 to 12.3.0 (#48).
- Bumped the `uv` dependency group with 2 updates (#50).

## [0.0.5] - 2026-07-17

### Added
- **Cross-table merge for annotated output**: referential checks that use a subquery projection now merge onto their anchor table instead of becoming a residue, giving you a single consolidated view across tables (#45).
- **SQL check execution design doc** (`docs/design-considerations.md`): covers the two-query model (scalar + failed-rows), automatic `COUNT(*)` ↔ `SELECT *` derivation, sqlglot-powered rewriting, and how query shape affects annotated output mergeability (#46).
- Restructured example notebooks into three focused, self-contained tutorials: core tutorial, multiple sources, and real databases (#45).
### Deprecated
- `get_consolidated_output_dfs()` and `output_mode="failed_rows"` / `"both"` now emit a `DeprecationWarning`. Use `get_annotated_output()` / `output_mode="annotated"` instead — pass `output_mode` explicitly to pin behaviour.

### Fixed
- Fixed flaky parallel SQLite test caused by `check_same_thread=True` default; pooled SQLite connections now use thread-safe mode (#44).

## [0.0.4] - 2026-07-06

### Fixed
- vowl now works with Spark Connect, including on Databricks. Previously, validating a Spark Connect DataFrame failed outright; now it's detected and runs as expected. Installing with `pip install vowl[spark]` gives you Spark Connect out of the box (this raises the minimum PySpark to `3.4.0`), and a new `spark-classic` option keeps support for older setups (PySpark `3.0.0`+) (#39, #40).
- Fixed Spark Connect / Databricks validation returning an error on every check (on PySpark 4.x). Checks now run and return real pass/fail results (#41).
- Fixed a crash when generating a report for an empty table; it now shows `N/A` instead (#41).
- Generated SQL now uses the correct syntax for your database (e.g. Databricks), both for the checks that run and for the SQL shown when a check fails (#41).
- Fixed installing vowl from source failing on older environments such as Databricks clusters, caused by how the license was declared requiring newer build tools (#41).

## [0.0.3] - 2026-06-29

### ✨ Annotated table output

This release introduces **annotated table output**: failed-row results can now be merged back into a single consolidated, annotated view of your source data, making it far easier to see *which* rows failed and *why* in context.

### Added
- **Annotated table output**: merge per-check failed rows into a consolidated, annotated copy of the source table. See the [Annotated Output](https://github.com/govtech-data-practice/vowl/blob/main/README.md#annotated-output) section in the README and the [usage patterns notebook](https://github.com/govtech-data-practice/vowl/blob/main/examples/vowl_usage_patterns_demo.ipynb) for usage (#35).
- **Pooled adapter** for concurrent query execution. The pooled adapter maintains a connection pool so multiple checks can run in parallel against a backend, with new tests covering concurrency and multi-source parallel execution (#32).
- Unique, primaryKey, and duplicateValues checks are now mergeable into the annotated output. Previously these auto-generated checks landed in `residues`; they now emit full participating rows that fold into the consolidated table (#36).
- **Preset-driven `check_info` column** on annotated output: a JSON array of objects (one per failing check) selectable via `names` (default), `summary`, or `full` presets, exposing each check's `dimension`, `tags`, and `target` per row. Set it on `ValidationConfig.annotated_check_info` or per call (`get_annotated_output(check_info=...)` / `save(check_info=...)`). Residues are now emitted one entry per non-mergeable check, carrying the same `check_info` column (#37).
- Security CI/CD pipeline for GitHub Actions, complementing existing security measures (#29).
- `SECURITY.md` security policy (#30).

### Changed
- Auto-generated unique, primaryKey, and duplicateValues checks now count *participating rows* (rather than duplicate groups), so `actual_value` / `failed_rows_count` match the annotated row count. The PASS/FAIL verdict is unchanged. The percent-unit `duplicateValues` variant remains non-mergeable as its result is a ratio (#36).

### Fixed
- Percent-unit library checks (`unit: "percent"`, e.g. `nullValues`, `missingValues`, `invalidValues`, `duplicateValues`) generated invalid SQL (aliased scalar subqueries) and came back as `ERROR` instead of a real pass/fail verdict. They now render valid SQL and return a correct ratio-based verdict, guarded by SQL re-parse and end-to-end DuckDB tests (#37).

### Dependencies
- Bumped `cryptography` (#34).
- Bumped `tornado` from 6.5.6 to 6.5.7 (#33) and from 6.5.5 to 6.5.6 (#31).
- Bumped `urllib3` from 2.6.3 to 2.7.0 (#27).
- Bumped the `uv` dependency group with 2 updates (#28).

## [0.0.2] - 2026-04-27

### 🎉 vowl is now an official ODCS vendor!

We're thrilled to announce that **vowl** has been recognised as an official [Open Data Contract Standard (ODCS)](https://bitol.io/open-data-contract-standard/) vendor. This is a proud milestone for the project and a testament to the community's commitment to open, interoperable data contracts.

### Fixed
- Getting outputs no longer crashes when a contract contains a list-valued `mustBe` field (#23).
- Broken link on the Known Issues documentation page (#19).
- Git link sanitisation issue in documentation (#15).

### Changed (Breaking)
- Check metadata now uses `check_definition` and `contract_definition` (#21, #26). This replaces several top-level metadata fields:
  - **Renamed fields:** `schema` → `schema_name`, `rule` → `rendered_implementation`.
  - **Removed from top-level:** `dimension`, `type`, `description`, `severity`, `unit` — these are no longer top-level keys in `CheckResultMetadata`.
  - **`check_definition`** carries the resolved/generated check definition dict. Auto-generated checks are tagged with `vowl_generated_check`.
  - **`contract_definition`** carries the raw ODCS contract content at the check's JSONPath.
  - **Output DataFrame:** `get_check_results_df()` no longer flattens definitions into top-level columns. Pass `include_check_definition=True` and/or `include_contract_definition=True` to include them as JSON-serialised columns. `save()` accepts the same keyword arguments.
  - Code that accesses `metadata["schema"]`, `metadata["rule"]`, `metadata["unit"]`, etc. must be updated to use the new key names or read from `metadata["check_definition"]` / `metadata["contract_definition"]`.

### Added
- Logical type `options.format` validation — contracts can now specify format constraints (e.g. date formats) on logical types, and vowl will auto-generate format checks. See the [Format Checks](https://govtech-data-practice.github.io/vowl/contracts/#format-checks) docs for details (#25).
- DuckDB attach example demonstrating cross-database validation (#18).
- Open Graph and SEO metadata for the documentation site (#14).
- Google site verification meta tag (#12).
- Updated Jupyter notebook examples (#16).
- README badge and data contract example link (#13, #22).

### Dependencies
- Bumped `pytest` from 9.0.2 to 9.0.3 (#20).
- Bumped `cryptography` (#17).
- Updated Python Docker tag to 3.14.3 (#11).

## [0.0.1] - 2026-04-02

### 🎉 Celebrating Open Source

Initial public release of **vowl**.

**Background:**

- vowl originated as an internal tool for demonstrating data contracts within our prototyping workflows. Over time, we recognised its potential value to the wider international community.
- With that in mind, we refined the library and published it as open source.
- As the project is still in its early stages, there may be rough edges and bugs. We appreciate your patience and warmly welcome contributions to help improve vowl for everyone.

### Added
- Core SQL-powered data quality validation engine backed by Ibis and DuckDB.
- Contract-based validation with YAML/JSON schema definitions.
- Adapters for pandas, Spark, and database backends (DuckDB attach).
- CTE wrapper for robust query transformation and complex query support.
- Multi-table and multi-source materialisation support.
- Export results as Arrow tables.
- Jupyter notebook examples and demo outputs.
- MkDocs documentation site (architecture, contracts, usage patterns).
- MIT license.
- GitHub Actions CI for testing, linting, and PyPI publishing.
- `THIRD_PARTY_NOTICES` and `LICENSE_AUDIT_REPORT.md`.
- `CONTRIBUTING.md` with development setup and release workflow.

[Unreleased]: https://github.com/govtech-data-practice/vowl/compare/v0.0.5...HEAD
[0.0.5]: https://github.com/govtech-data-practice/vowl/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/govtech-data-practice/vowl/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/govtech-data-practice/vowl/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/govtech-data-practice/vowl/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/govtech-data-practice/vowl/releases/tag/v0.0.1
