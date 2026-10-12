# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This release adds **DQ metrics** and **OpenTelemetry export**, so you can track how many rows pass your checks over time and send the results to your monitoring stack. `save()` now takes `outputs=[...]` to choose which files to write, and can write straight to cloud storage. Several outputs were renamed or reshaped along the way, so read **Changed (Breaking)** before upgrading.

### Changed (Breaking)
- **`save()` takes `outputs=[...]` instead of `output_mode`**, and by default now writes every output except `"failed_query_outputs"`. Pass `outputs=["consolidated_query_outputs"]` to keep the 0.0.6 default. See [Outputs](docs/results.md#outputs).
- **Output keys and file names include the column**, so checks with the same name on different columns no longer overwrite each other. See [Per-check files](docs/results.md#per-check-files).
- **`get_output_dfs()` returns only checks with failed rows.** Use `get_output_dfs(scope="all")` for the 0.0.6 behaviour. See [Failed rows per check](docs/results.md#failed-rows-per-check).
- **The grouped failed-rows CSVs hold one file per table set**, instead of splitting by the columns each check returns. See [Consolidated files](docs/results.md#consolidated-files).
- **`print_summary()` and summary.json no longer show passed rows or table totals**, as the 0.0.6 numbers could be approximate. `failed_rows` is renamed `failed_rows_approximate` and `total_rows_by_schema` is removed. For exact row counts, use `result.get_dq_metrics_df()`. See [Row counts](docs/results.md#row-counts).
- **Composite primary keys are checked as one key**, with one `<schema>_<col1>_<col2>_primary_key_check`. Update any dashboards or alerts that use the old per-column check names. See [Composite Primary Keys](docs/contracts.md#composite-primary-keys).
- **Some checks are no longer row-level**: those whose operator doesn't mark the matched rows as bad, such as `mustBeGreaterThan`, and `rowCount` outside an upper limit. Their verdict is unchanged. See [Check result types](docs/design-considerations/checks/check-results.md#counted-checks).
- **Check names are unique on each column and schema, generated ones included.** A name you give two checks on one column or one schema, or a name that matches one vowl generates there (such as `email_required_check`), raises `ValueError` when the contract loads. An `id` counts as a name. Two checks whose names vowl made up, such as two unnamed checks, are numbered `_2`, `_3` instead. Two schemas with the same name also raise, where 0.0.6 silently dropped the first one's checks. See [Check names](docs/contracts.md#check-names).
- **Foreign key checks name both ends**: `<schema>_<columns>_to_<target schema>_<target columns>_foreign_key_check`, for example `orders_customer_id_to_customers_customer_id_foreign_key_check`. Two foreign keys from one column now get different names. Update any filters or alerts on the old names. See [Relationships (Foreign Keys)](docs/contracts.md#relationships-foreign-keys).
- **`check_results.csv` has no `type` or `description` column**, which no check ever filled. It gains `severity` when a check sets one.

### Added
- **Every check is identified by `schema_name`, `target` and `check_name`** in DQ metrics, `dq_metrics.json`, OpenTelemetry metrics, spans and logs, and `get_dq_metrics_df(by="check")`, the same as in `check_results.csv`. `target` is `<schema>.<column>`, or `<schema>` for a schema-level check. Checks with the same name on different columns, or on a column and its schema, are no longer merged. See [How a check is identified](docs/dq-metrics/understanding-metrics.md#how-a-check-is-identified).
- Check spans and logs carry `check_path` and `tables_in_query`. `get_dq_metrics_df(by="check")` has `target`, `severity` and `engine` columns.
- **DQ metrics**: how many rows of each table passed or failed, at check, dimension, schema and run level. Get them with `result.get_dq_metrics()`, `result.get_dq_metrics_df()` or `save(outputs=[..., "dq_metrics"])`. See [Understanding DQ Metrics](docs/dq-metrics/understanding-metrics.md).
- **OpenTelemetry export**: `result.export_otel()` sends a run's results as OTLP metrics, traces and logs. Install with `pip install vowl[otel]`. See [Exporting to OpenTelemetry](docs/dq-metrics/otel-export.md). To try it locally, `examples/6_dq_metrics/otel_stack/` has a Grafana stack with a DQ dashboard.
- **Saving to cloud storage**: `save()` accepts an `s3://`, `gs://`, `abfs://` or `hdfs://` URI, or a `pyarrow.fs.FileSystem`. See [Cloud storage](docs/results.md#cloud-storage).
- **Per-check row files**, `get_output_dfs(scope="all")` and a `saved_outputs` list in summary.json. See [Saving results](docs/results.md#saving-results).
- **Tolerated rows**: `ValidationConfig(fetch_tolerated_rows=True)` also fetches the failed rows of checks that passed within their threshold. See [Tolerated rows](docs/design-considerations/checks/check-results.md#tolerated-rows).
- `ValidationConfig(outputs=[...])` sets the default for `save()`. See [Run settings](docs/run-settings.md#outputs).
- `get_check_results_df()` has a `dimension` column.
- Optional adapter hooks let custom adapters count rows inside their data source. See [Custom Adapters and Executors](docs/usage-patterns.md#custom-adapters-and-executors).

### Changed
- **Annotated output loads every table into memory**, and DQ metrics may too. See [What each method costs](docs/results.md#what-each-method-costs) before using them on large tables.
- **More cross-table checks run inside the database**, including with filter conditions or a shared `PooledAdapter`. See [Copying tables into memory](docs/design-considerations/cross-table/how-it-works.md#copying-tables-into-memory).
- **Schemas sharing one `PooledAdapter` run their checks in parallel**, up to `max_concurrency`.
- **`"consolidated_query_outputs"` is no longer the only default output.** `save()` now writes the grouped failed-rows CSVs alongside the other outputs. The 0.0.5 deprecation of `get_consolidated_output_dfs()` and `"consolidated_query_outputs"` is withdrawn, as they are the cheapest choice for large tables. See [Outputs](docs/results.md#outputs).
- `get_annotated_output()` warns instead of raising `ValueError` when `max_failed_rows` cuts a check short. See [Capping Failed Rows](docs/design-considerations/checks/capping-failed-rows.md).
- Annotated output can merge more checks, including `COUNT(DISTINCT x)` checks and checks that return only a unique primary key.
- Clearer messages when an adapter is the wrong type or a table can't be downloaded.

### Deprecated
- `output_mode` in `save()` and `ValidationConfig` emits a `FutureWarning`. Use `outputs=` instead. See [output_mode](docs/results.md#deprecated-output_mode).
- `ValidationConfig(enable_additional_schema_statistics=...)` and `ValidationConfig(max_rows_for_statistics=...)` have no effect.
- `ValidationResult.save_dataframe()`. Write the frame with its own library instead.
- `SQLCheckReference.get_failed_rows_query()` is renamed `get_row_query()`. The old name still works with a `FutureWarning`, and a subclass that overrides it is still used.
- `vowl.config.OutputMode`, the type of `output_mode`. Use `SaveOutput` with `outputs=` instead.

### Fixed
- DQ metrics on Oracle count plain row filters in the database, instead of always downloading the table. The statements vowl sends no longer put `AS` before a table alias or leave `_vowl_` names unquoted, and use `DECODE` where Oracle has no `IS NOT DISTINCT FROM`.
- Unsupported checks nested under a property, such as an unknown `logicalTypeOptions.format` or an `items` option, now report their schema and column instead of a wrong or missing one.
- A check whose engine has no executor, or whose schema has no adapter, keeps its schema and target in its `ERROR` result.
- `save()` no longer fails with "File name too long" on long schema, column or check names. A name over about 230 bytes is shortened and ends in a hash of the full name. See [Consolidated files](docs/results.md#consolidated-files).
- A SQL check with a syntax error now ends as `ERROR` instead of stopping the whole run.
- Quality rules with a `metric` and no `type`, and ODCS v3.0.2 rules with `rule:` and no `type`, no longer crash the run.
- SQL checks can join a lookup table the contract doesn't declare.
- `validate_data` accepts a `PooledAdapter` or a custom `BaseAdapter` directly.
- `PooledAdapter.get_total_rows` no longer crashes on DuckDB when called from several threads.
- External references next to a contract loaded from `s3://` now resolve.
- The `max_failed_rows` cap now works on SQL Server, Fabric and Oracle, and is no longer skipped when a column name contains `limit`.
- The grouped failed-rows CSVs no longer list the passing rows of checks such as `mustBeGreaterThan`.
- Annotated output handles NaN, `-0.0`, nested types, nanosecond timestamps, large unsigned integers and mixed column types correctly.
- Checks that alias their count, such as `SELECT COUNT(*) AS n`, now get a row query, and `COUNT(col)` row queries leave out NULLs.
- A failed-rows query that returns no rows keeps its columns, so the check can still be merged.
- DQ metrics no longer fail on engines that return unquoted column names in upper case, such as Snowflake.
- `save()` no longer overwrites a file when two table or schema names clean to the same file name, or when a table is named `check_results`. The later file gets `_2`. See [Consolidated files](docs/results.md#consolidated-files).
- Output file names keep non-ASCII letters and digits, so schema `客户` writes `<prefix>_客户_annotated.csv` instead of `<prefix>_output_annotated.csv`, and `café` no longer becomes `caf`. ASCII names are unchanged.
- The `max_failed_rows` cap no longer adds a second `OFFSET` or `FETCH` to an ordered `UNION` that already has one on SQL Server, Fabric and Oracle.
- `export_otel()` gives the `pip install vowl[otel]` message when only `opentelemetry-api` is installed, and shuts down any provider it built if a later one fails to build.

### Docs
- Regrouped the guides into Results, Run settings, DQ metrics and a new Design Considerations tab. The old Design Considerations page redirects to the new pages.
- New example notebooks: `examples/5_results/results_tour.ipynb` and `examples/6_dq_metrics/dq_metrics.ipynb`.

### Dependencies
- `sqlglot` now needs `>=30.5,<30.18`. The new minimum skips 30.2 and 30.4, which leak memory and slow down long DuckDB runs. 30.18 breaks DataFrame, Arrow and multi-source checks through ibis, so it stays out until ibis releases the fix ([ibis-project/ibis#12104](https://github.com/ibis-project/ibis/pull/12104)), expected in 13.0.0.
- New optional `otel` extra, also included in `all`.
- The source distribution now holds only the package source, `pyproject.toml`, the README, CHANGELOG and license files. Docs, examples and tests stay in the git repository.

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

[Unreleased]: https://github.com/govtech-data-practice/vowl/compare/v0.0.6...HEAD
[0.0.6]: https://github.com/govtech-data-practice/vowl/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/govtech-data-practice/vowl/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/govtech-data-practice/vowl/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/govtech-data-practice/vowl/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/govtech-data-practice/vowl/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/govtech-data-practice/vowl/releases/tag/v0.0.1
