# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- Nil

## [0.0.2] - 2026-04-27

### 🎉 vowl is now an official ODCS vendor!

We're thrilled to announce that **vowl** has been recognised as an official [Open Data Contract Standard (ODCS)](https://bitol.io/open-data-contract-standard/) vendor. This is a proud milestone for the project and a testament to the community's commitment to open, interoperable data contracts.

### Fixed
- Getting outputs no longer crashes when a contract contains a list-valued `mustBe` field (#23).
- Broken link on the Known Issues documentation page (#19).
- Git link sanitisation issue in documentation (#15).

### Changed (Breaking)
- `contract_definition` is now the single source of truth for check metadata (#21). This replaces several top-level metadata fields with a single `contract_definition` dict that carries the raw ODCS quality entry:
  - **Renamed fields:** `schema` → `schema_name`, `rule` → `rendered_implementation`.
  - **Moved into `contract_definition`:** `dimension`, `type`, `description`, `severity`, `unit` — these are no longer top-level keys in `CheckResultMetadata`.
  - **Output DataFrame:** `get_check_results_df()` now flattens `contract_definition` into top-level columns, so all contract fields appear automatically. The column order in the results CSV has changed accordingly.
  - Code that accesses `metadata["schema"]`, `metadata["rule"]`, `metadata["unit"]`, etc. must be updated to use the new key names or read from `metadata["contract_definition"]`.

### Added
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

[Unreleased]: https://github.com/govtech-data-practice/vowl/compare/v0.0.2...HEAD
[0.0.2]: https://github.com/govtech-data-practice/vowl/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/govtech-data-practice/vowl/releases/tag/v0.0.1
