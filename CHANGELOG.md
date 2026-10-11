# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed (Breaking)
- **Each method does only the work it needs.** `print_summary()`, `get_check_results_df()` and `result.summary` use what the checks returned. `show_failed_rows()`, `get_output_dfs()` and `save()` with `"failed_query_outputs"` or `"consolidated_query_outputs"` also run the row query of each failed check. `get_output_dfs(scope="all")` and `"all_query_outputs"` run it for every check that did not end in `ERROR`. `get_dq_metrics()`, `get_dq_metrics_df()`, `export_otel()` and `"dq_metrics"` also attribute failed rows to their tables, which can download a table. `get_annotated_output()` and `"annotated_table"` download every table. See [What each method costs](docs/results.md#what-each-method-costs).
- `print_summary()` no longer prints **Unique Passed Rows**. Each schema's **Overall** block shows **Failed Rows (approximate)** instead, the sum of the failed counts of the checks that return rows. Use `get_dq_metrics_df(by="schema")` for exact row counts and pass rates.
- `print_summary()`: the **Multi Table** block's **Non-unique Failed Rows** is renamed **Failed Rows (approximate)**, the same name as in **Overall**. It now sums every cross-table row-level check, not only the failed ones, and shows `N/A` instead of `0` when there are none.
- summary.json and `result.summary["validation_summary"]`: `failed_rows` is renamed `failed_rows_approximate`, and `total_rows_by_schema` is removed. `validate_data()` no longer counts each table's rows. Table totals are in the `total_rows` column of `get_dq_metrics_df(by="schema")`.
- **`save()` takes `outputs=[...]`**, the list of files to write, in place of `output_mode`. With no `outputs`, it now writes every output except `"failed_query_outputs"`: per-check row files, the grouped failed-rows CSVs, annotated tables with residues and `dq_metrics.json`. In 0.0.6 it wrote the grouped CSVs only. Pass `outputs=["consolidated_query_outputs"]` to keep that. See [Saving results](docs/results.md#saving-results).
- Output keys and file names now include the column, so checks with the same name on different columns no longer overwrite each other:
  - `get_output_dfs()` and the `"residues"` of `get_annotated_output()` key a column-level check by `"<schema>.<column>::<check_name>"`. Schema-level checks keep `"<schema>::<check_name>"`.
  - Residue files are `<prefix>_<schema>__<column>__<check>_residue.csv` (`<prefix>_<schema>__<check>_residue.csv` for a schema-level check), not `<prefix>_<schema>_<check>_residue.csv`.
  - File names spell out comparison operators (`amount > 0` becomes `amount_gt_0`). Names that still clash, ignoring case, get `_2`, `_3` and so on (`amount_2`), in the order the checks ran. summary.json's `saved_outputs` maps each file to its check.
- Checks whose operator does not mark the matched rows as bad, such as `mustBeGreaterThan` or `mustBe: 5`, are no longer row-level. They are left out of annotated tables, residues and the DQ metrics row counts. Their verdict is unchanged.
- The `rowCount` library check now behaves like `SELECT COUNT(*) FROM t`: its `failed_rows_count` is the table size. Under an upper limit such as `mustBeLessThan` it annotates every row. Under other operators it is not row-level and has no residue. Before, it reported `0` failed rows and a failing `rowCount` made the whole table a residue.
- A contract with two checks of the same name on one column, or two schema-level checks of the same name, now raises `ValueError` when it loads.
- The grouped failed-rows CSVs and `get_consolidated_output_dfs()` hold one file per table set, `<prefix>_<tables>.csv`. Checks that return different columns of the same tables are no longer split into `<tables>__1` and `<tables>__2` keys and `_1`, `_2` files. Missing columns are null.
- `get_output_dfs()` leaves out checks that passed, and failed checks whose operator sets no upper limit (such as `mustBeGreaterThan`), whose rows are the passing ones. With `fetch_tolerated_rows=True` it also holds the checks that passed within their threshold. In 0.0.6 it returned every check that did not end in `ERROR`. `get_output_dfs(scope="all")` still does, with a `status` column.
- summary.json's `config` no longer holds `output_mode`. It holds `outputs` and `fetch_tolerated_rows` instead.
- A composite primary key (two or more properties with `primaryKey: true`) is now checked as one key, with one `<schema>_<col1>_<col2>_primary_key_check`. Previously each column got its own check, so valid data failed whenever one column repeated on its own. The per-column check names are gone, so update any dashboards or alerts that use them. Single-column keys are unchanged.

### Added
- **OpenTelemetry export**: `result.export_otel()` sends a run's results as OTLP metrics, traces and logs. Install with `pip install vowl[otel]`. See [Exporting to OpenTelemetry](docs/dq-metrics/otel-export.md).
- Each `result.save(...)` made before `export_otel()` adds a `vowl.save` span inside the `vowl.validate` span, with its real start and end, its outputs and its file count. The run span is extended to cover it, and `vowl.run.duration` still measures the validation only. See [Save span](docs/dq-metrics/otel-export.md#save-span-vowlsave).
- **DQ metrics** at four levels (check, dimension, schema and run), named `vowl.<level>.<unit>.<measure>`. `result.get_dq_metrics()` returns them and `save(outputs=[..., "dq_metrics"])` writes them to `<prefix>_dq_metrics.json`, with the same names and values as the OpenTelemetry metrics. See [Understanding DQ Metrics](docs/dq-metrics/understanding-metrics.md).
- **Row-quality statistics**: `result.get_dq_metrics_df(by="schema" | "dimension" | "check")` reports how many rows of each table failed at least one row-level check, how many passed, the pass rate and whether each number is approximate. `by="check"` shows each check's `scalar_count`, `attributed_rows`, `attribution_method` and an `attribution_note` saying why a check was left out or is approximate. Annotated output and the DQ metrics share the same numbers.
- Row counts are computed inside the data source where vowl can, so they are exact on large tables and count every copy of a duplicated row. This is tested on DuckDB, SQLite, Spark and PostgreSQL. Databricks uses the same code as Spark. Elsewhere vowl matches the failed rows onto the downloaded table, and marks a number approximate where it could be off. See [How Attributed Rows Work](docs/design-considerations/checks/how-attributed-rows-work.md).
- `vowl.<level>.row.approximate` gauges, `1` when that level's row counts could be off, so you can alert on them. See [Approximate row counts](docs/dq-metrics/understanding-metrics.md#approximate-row-counts).
- `result.run_id`, an ID given to every run and shared by `dq_metrics.json` and `export_otel()`. Set it to use your own.
- **Saving to cloud storage**: `save()` takes a URI (`s3://`, `gs://`, `abfs://`, `hdfs://`, `file://`) as `output_dir`, or a `pyarrow.fs.FileSystem` through `filesystem=`. See [Cloud storage](docs/results.md#cloud-storage).
- Per-check row files, `<prefix>_checks/<schema>__<column>__<check>.csv`. `outputs=["failed_query_outputs"]` writes them for failed checks. `"all_query_outputs"` writes them for every check that did not end in `ERROR`, with a `status` column, and is in the default outputs.
- `get_output_dfs(scope="all")` returns what the row query of each check returned, with a `status` column. It includes passed checks and checks with no upper limit, and leaves out checks that ended in `ERROR`.
- summary.json has a `saved_outputs` key. It lists what each output wrote, with the check, tables or schema, the row count and the file. A check with no rows is listed with `"rows": 0` and no file.
- `ValidationConfig(fetch_tolerated_rows=True)` also fetches the failed rows of checks that passed within their threshold. Every output then includes them: `get_output_dfs()` adds a `tolerated` column, the grouped CSVs add `tolerated_check_ids`, annotated `check_info` items carry `"tolerated": true` and `show_failed_rows()` labels them `(tolerated)`. Off by default.
- `ValidationConfig(outputs=[...])` sets what `save()` writes when you pass no `outputs`. See [Run settings](docs/run-settings.md#outputs).
- Adapter hooks: `BaseAdapter.run_arrow_query()` and `BaseAdapter.get_column_types()` are optional, and an adapter that implements them gets its row counts computed in the data source. `IbisAdapter.with_filter_conditions()` returns a copy with other filters, which lets a filtered cross-table join run in the database.
- `get_check_results_df()` has a `dimension` column, `"unknown"` when a check declares none.
- `examples/6_dq_metrics/otel_stack/`: a local OpenTelemetry Collector, Prometheus, Tempo, Loki and Grafana stack with a DQ dashboard, for trying `export_otel()` end to end. It also keeps an S3 archive you can query with SQL. Run `make otel-up` and `make otel-add-vowl-runs` in that folder.

### Changed
- Annotated output downloads every table in full into memory on the machine running vowl. DQ metrics download a table when it has a row-level check the data source cannot attribute, such as a cross-source check. At 6 columns this took about 3 s and 1 to 1.5 GiB for 1 million rows. Annotated output and DQ metrics download each table at most once per run. The methods that only read check results or failed rows never download a table, except that a cross-source check copies its tables into memory again to fetch its failed rows. See [What each method costs](docs/results.md#what-each-method-costs).
- When `max_failed_rows` cuts a check's failed rows short, `get_annotated_output()` warns and marks its `check_info` items `"truncated": true` instead of raising `ValueError`. This now also applies at `max_failed_rows=0` and to `DISTINCT` checks.
- Annotated output now merges a check whose failed rows hold the table's declared primary key but not all its columns, when that key is unique.
- `COUNT(DISTINCT x)` checks are row-level. Their failed rows are the rows holding the counted values.
- A cross-table check whose tables share one connection now runs in that database even when adapters have filter conditions, with each table keeping its own filters. Before, any filter made vowl download every table in the join. A cross-table check whose tables are all served by one `PooledAdapter` also runs in the database. See [PooledAdapter: Joins Across Pools Are Copied](docs/known-issues.md#pooledadapter-joins-across-pools-are-copied).
- Schemas sharing one `PooledAdapter` now run their checks side by side, up to `max_concurrency` connections in total. Before, the pool ran one schema's checks at a time.
- `get_consolidated_output_dfs()` and `save(outputs=["consolidated_query_outputs"])` are no longer deprecated. They are the cheap choice for large tables. They run one query per failed check and never download a table, except that a cross-source check copies its tables again.
- `DataSourceMapper.get_adapter` given an adapter other than `IbisAdapter` still raises `TypeError`. The message now says to pass the adapter to `validate_data` as it is.
- The warnings when a table cannot be downloaded now read `No adapter for schema '<name>', so its table cannot be downloaded.` and `Could not download the table of '<name>': <error>`.

### Deprecated
- `save(output_mode=...)` and `ValidationConfig(output_mode=...)`. The 0.0.6 names still work with a `FutureWarning`: `"failed_rows"` maps to `outputs=["consolidated_query_outputs"]`, `"annotated"` to `["annotated_table", "dq_metrics"]` and `"both"` to all three. Passing both `output_mode=` and `outputs=` raises `ValueError`.
- `ValidationConfig(enable_additional_schema_statistics=...)` and `ValidationConfig(max_rows_for_statistics=...)` emit a `DeprecationWarning` and have no effect.
- `ValidationResult.save_dataframe()` emits a `DeprecationWarning`. Write the frame with its own library instead, for example `pyarrow.parquet.write_table(df.to_arrow(), "my-bucket/rows.parquet", filesystem=fs)`.

### Fixed
- A SQL check with a syntax error now ends as `ERROR` and the other checks keep their results. Previously `validate_data` raised a sqlglot `ParseError` and the whole run was lost.
- A quality rule with a `metric` and no `type` now runs as a library check, the ODCS default. An ODCS v3.0.2 rule with `rule:` and no `type` ends as `ERROR` saying to use `metric:`. Both previously crashed the run.
- External references next to a contract loaded from `s3://`, including `../` paths, now resolve.
- The failed-row cap uses `TOP` on SQL Server and Fabric and `FETCH FIRST` on Oracle, which reject `LIMIT`.
- The `max_failed_rows` cap is no longer skipped when a query merely mentions `LIMIT`, for example in a `credit_limit` column.
- A SQL check that joins a table the contract doesn't declare, such as a lookup table, now reads it through its own schema's adapter. Previously it failed with `No adapter configured for table '...'`.
- `validate_data` accepts a `PooledAdapter` or a custom `BaseAdapter` subclass directly. Previously it raised `TypeError: Unsupported adapter type`.
- `PooledAdapter.get_total_rows` leases a connection from the pool, so calling it from several threads no longer crashes on DuckDB.
- The grouped failed-rows CSVs no longer list the passing rows of a failed check with no upper limit, such as `mustBeGreaterThan`.
- Annotated output now flags every copy of a failing row that holds NaN, keeps `-0.0` apart from `0.0`, supports list, struct, map and fixed-size array columns, matches nanosecond timestamps and keeps `UBIGINT` values above 2^63. On PySpark, NaN values are no longer turned into NULL on download.
- `get_annotated_output()` no longer raises `ArrowTypeError` when two checks return different types for one column, such as `int32` and `int64`.
- A check that aliases its count, such as `SELECT COUNT(*) AS n`, now derives a row query, and a `COUNT(col)` check's row query leaves out the rows where `col` is NULL.
- A failed-rows fetch that returns zero rows keeps its column names, so the check is still mergeable.

### Docs
- Regrouped the guides: [Results](docs/results.md), [Run settings](docs/run-settings.md), DQ metrics ([Understanding DQ Metrics](docs/dq-metrics/understanding-metrics.md), [OpenTelemetry](docs/dq-metrics/otel-export.md), [dq_metrics.json](docs/dq-metrics/json-export.md)) and a Design Considerations tab covering [Checks](docs/design-considerations/checks/check-results.md) and [Cross-Server and Cross-Table Checks](docs/design-considerations/cross-table/how-it-works.md). The old Design Considerations page redirects to the new pages.
- New example notebooks: `examples/5_results/results_tour.ipynb` tours the results object and `save()`, and `examples/6_dq_metrics/dq_metrics.ipynb` covers DQ metrics and OpenTelemetry export.

### Dependencies
- Capped `sqlglot` below `30.18`. Newer versions broke DataFrame, Arrow and multi-source checks with `Parser Error: syntax error at end of input`.
- New optional `otel` extra (`opentelemetry-sdk`, `opentelemetry-exporter-otlp`), also included in `all`.

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
