# Rename Checklist: `qbc` → `vowl`

## 1. Package Metadata & Build Configuration

- [ ] **pyproject.toml** — `name = "qbc"` → `name = "vowl"`
- [ ] **pyproject.toml** — `project.urls` Documentation URL (`https://pypi.org/project/qbc/`)
- [ ] **pyproject.toml** — `tool.setuptools.package-data` key `"qbc.contracts.models"` → `"vowl.contracts.models"`
- [ ] **pyproject.toml** — `tool.setuptools.packages.find` `where = ["src"]` (no change needed, but verify discovery picks up `src/vowl`)

## 2. Source Directory Rename

- [ ] Rename `src/qbc/` → `src/vowl/`
- [ ] Delete `src/qbc.egg-info/` (will be regenerated on next install)
- [ ] Delete `src/qbc/__pycache__/` and all nested `__pycache__` (will be regenerated)

## 3. Top-Level Module (`__init__.py`)

- [ ] `src/vowl/__init__.py` — update docstring (`"""qbc"""` → `"""vowl"""`)
- [ ] `src/vowl/__init__.py` — `version("qbc")` → `version("vowl")`
- [ ] `src/vowl/__init__.py` — `AttributeError` message: `module 'qbc' has no attribute` → `module 'vowl' has no attribute`

## 4. Internal Imports (all `from qbc.` / `import qbc`)

Every `from qbc.…` and `import qbc` must become `from vowl.…` / `import vowl`. Files containing these:

- [ ] `src/vowl/validate.py`
- [ ] `src/vowl/mapper.py`
- [ ] `src/vowl/adapters/__init__.py`
- [ ] `src/vowl/adapters/base.py` (TYPE_CHECKING imports)
- [ ] `src/vowl/adapters/ibis_adapter.py`
- [ ] `src/vowl/adapters/multi_source_adapter.py`
- [ ] `src/vowl/contracts/check_reference.py`
- [ ] `src/vowl/contracts/contract.py`
- [ ] `src/vowl/contracts/models/__init__.py`
- [ ] `src/vowl/contracts/models/ODCS_types.py` (verify — likely no `qbc` import)
- [ ] `src/vowl/executors/__init__.py`
- [ ] `src/vowl/executors/base.py` (TYPE_CHECKING imports)
- [ ] `src/vowl/executors/ibis_sql_executor.py`
- [ ] `src/vowl/executors/multi_source_sql_executor.py`
- [ ] `src/vowl/executors/security.py` (verify — likely no `qbc` import)
- [ ] Any other `.py` files under `src/vowl/` (check with `grep -r "from qbc\|import qbc" src/`)

## 5. Internal SQL Alias

- [ ] `src/vowl/contracts/check_reference_generated.py` — alias `_qbc_column_exists` → `_vowl_column_exists` (optional, cosmetic; still works either way)

## 6. Default Save Prefix

- [ ] `src/vowl/validation/result.py` — `prefix: str = "qbc_results"` → `prefix: str = "vowl_results"`

## 7. Monkeypatch Paths in Tests

Many tests monkeypatch using string paths like `"qbc.validate.ValidationRunner"`. All must be updated.

- [ ] `test/conftest.py` — `import qbc` / `from qbc import …` → `import vowl` / `from vowl import …`
- [ ] `test/test_adapter_and_mapper_unit_coverage.py` — imports and monkeypatch strings
- [ ] `test/test_aggregation_support.py`
- [ ] `test/test_check_reference_unit_coverage.py`
- [ ] `test/test_contract_unit_coverage.py`
- [ ] `test/test_coverage_plan_workstream1.py` — imports, monkeypatch strings, and the `test_qbc_*` function names (rename or keep as-is for history)
- [ ] `test/test_database_backends.py`
- [ ] `test/test_executor_base_unit_coverage.py`
- [ ] `test/test_export_table_as_arrow.py`
- [ ] `test/test_models.py`
- [ ] `test/test_sql_executors_unit_coverage.py`
- [ ] `test/test_sql_security.py`
- [ ] `test/test_usage_patterns.py`
- [ ] `test/test_validate_unit_coverage.py`

> **Tip:** Run `grep -rn "qbc" test/` to catch every occurrence.

## 8. Makefile

- [ ] `MODELS_DIR` path: `src/qbc/contracts/models` → `src/vowl/contracts/models`
- [ ] `security-scan` target: `bandit -r src/qbc` → `bandit -r src/vowl`
- [ ] `security-scan-json` target: same change
- [ ] `security-audit` target: temp file name `qbc-requirements-audit.txt` → `vowl-requirements-audit.txt` (optional, cosmetic)

## 9. Documentation

- [ ] **README.md** — all references to `qbc` in prose, headings, code blocks, and `pip install` commands
- [ ] **README.md** — `save(… prefix="qbc_results")` example → `"vowl_results"`
- [ ] **CONTRIBUTING.md** — title, prose, `cd qbc`, and all backtick references
- [ ] **docs/Doxyfile** — `PROJECT_NAME`, `PROJECT_BRIEF`, `INPUT` path, `EXCLUDE` pattern
- [ ] **docs/oracle-sql-compatibility-fix.md** — file path references and `_qbc_column_exists` alias mentions
- [ ] **docs/plan-engine-agnostic-check-references.md** — file path table
- [ ] **docs/plan-library-metrics.md** — file path table

## 10. Notebooks

- [ ] `test/hdb_resale/test_ibis_validation.ipynb` — all `from qbc …` imports and `qbc_results` output filenames in cells

## 11. Test Golden Files / Expected Outputs

These CSVs contain rendered SQL with the `_qbc_column_exists` alias. If you rename the alias in step 5, regenerate goldens:

- [ ] `test/expected_outputs/` — regenerate via `UPDATE_GOLDENS=1 pytest test/`
- [ ] `test/failed_outputs/` — safe to delete (transient artifacts)

## 12. Saved Output Files in Test Fixtures

- [ ] `test/hdb_resale/qbc_results_check_results.csv` — rename file and update any references (or regenerate from notebook)

## 13. Coverage Reports (Transient — No Action Unless Committed)

- [ ] `coverage.json` and `coverage-rescan.json` contain `src/qbc/…` paths. If these are committed, regenerate after rename. If gitignored, they will self-heal on next test run.

## 14. CI/CD

- [ ] `.gitlab-ci.yml` — review for any hardcoded `qbc` references (currently none found, but double-check after rename)
- [ ] Verify the GitLab PyPI registry upload endpoints and any `--extra-index-url` still work (the project ID `64873` is separate from the package name, but the package name on the registry will change)

## 15. Git History

- [ ] Use `git mv src/qbc src/vowl` to preserve rename history
- [ ] Consider a single atomic commit for the rename to keep `git blame` clean

## 16. Post-Rename Validation

- [ ] `make clean` — clear all cached artifacts
- [ ] `make install-dev` — verify package installs as `vowl`
- [ ] `python -c "import vowl; print(vowl.__version__)"` — confirm importable
- [ ] `make test` — full test suite passes
- [ ] `make lint` — linting passes
- [ ] `make security-scan` — Bandit finds the new path
- [ ] `make release-check` — built dist says `Name: vowl`

## 17. Downstream / Ecosystem (Outside This Repo)

- [ ] Update any `pip install qbc` / `requirements.txt` / `pyproject.toml` in downstream projects
- [ ] Update any `from qbc import …` in downstream consumer code
- [ ] If publishing to a package registry, consider publishing a final `qbc` version that re-exports from `vowl` as a transition shim
- [ ] Update any internal wikis, Confluence pages, or Slack pinned docs referencing `qbc`
