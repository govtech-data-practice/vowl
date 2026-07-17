# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.5] - 2026-07-17

### Added
- **Cross-table merge for annotated output**: referential checks that use a subquery projection now merge onto their anchor table instead of becoming a residue, giving you a single consolidated view across tables (#45).
- **SQL check execution design doc** (`docs/design-considerations.md`): covers the two-query model (scalar + failed-rows), automatic `COUNT(*)` ↔ `SELECT *` derivation, sqlglot-powered rewriting, and how query shape affects annotated output mergeability (#46).
- Restructured example notebooks into three focused, self-contained tutorials: core tutorial, multiple sources, and real databases (#45).
- Deprecation warnings for `get_consolidated_output_dfs()` and the legacy `output_mode="failed_rows"` / `"both"` save modes, guiding users toward annotated output.

### Deprecated
- `ValidationResult.get_consolidated_output_dfs()` and the `output_mode="failed_rows"` / `"both"` save modes (the legacy consolidated failed-rows CSVs) are deprecated in favour of annotated output (`get_annotated_output()` / `output_mode="annotated"`). Calling them now emits a `DeprecationWarning`. The `save()` default `output_mode` is still `"failed_rows"` but will change to `"annotated"` in a future minor release — pass `output_mode` explicitly to pin the behaviour you want. Annotated output supersedes the consolidated view: it returns your full tables with failing rows flagged in place via a per-row `check_info` column, plus per-check residues.

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
