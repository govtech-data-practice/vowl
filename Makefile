.PHONY: help install install-dev install-lean-ci-test install-all generate-models doxygen doxygen-open doxygen-clean clean test lint lint-fix format format-fix typecheck check verify security-scan security-audit security-secrets release-check release-upload-testpypi release-tag docs-serve docs-build docs-clean docs-lint docs-fix

UV ?= uv

# Prettier (Markdown docs) — run via npx, no Node project install required.
PRETTIER ?= npx --yes prettier
DOCS_GLOB ?= "**/*.md"

# Default target
help:
	@echo "Available targets:"
	@echo "  install          Install the package with core dependencies"
	@echo "  install-dev      Install with development dependencies"
	@echo "  install-lean-ci-test Install the lean CI test dependency set"
	@echo "  install-all      Install with all optional dependencies"
	@echo "  lint             Run ruff lint checks"
	@echo "  lint-fix         Lint and auto-fix with ruff"
	@echo "  format           Check code formatting with ruff"
	@echo "  format-fix       Format code with ruff"
	@echo "  typecheck        Type check with ty"
	@echo "  docs-lint        Check Markdown formatting with Prettier"
	@echo "  docs-fix         Format Markdown docs with Prettier"
	@echo "  check            Run all code quality checks (lint, format, typecheck, docs-lint)"
	@echo "  generate-models  Generate Pydantic models from ODCS JSON schemas"
	@echo "  doxygen          Regenerate Doxygen code structure documentation"
	@echo "  doxygen-open     Open generated Doxygen documentation in browser"
	@echo "  doxygen-clean    Remove generated Doxygen documentation"
	@echo "  clean            Remove build artifacts and cache files"
	@echo "  test             Run tests"
	@echo "  verify           Run all checks and tests"
	@echo "  security-scan    Run Semgrep SAST scan"
	@echo "  security-audit   Run dependency vulnerability audit (pip-audit)"
	@echo "  security-secrets Reproduce the CI TruffleHog secret scan locally (needs Docker)"
	@echo "  release-check    Build package artifacts and run Twine validation"
	@echo "  release-upload-testpypi Upload dist artifacts to TestPyPI"
	@echo "  release-tag       Create annotated tag after version consistency check"
	@echo "  docs-serve       Start local documentation preview server"
	@echo "  docs-build       Build documentation site"
	@echo "  docs-clean       Remove generated documentation site"

# Installation targets
install:
	$(UV) sync

install-dev:
	$(UV) sync --group dev

install-lean-ci-test:
	$(UV) sync --group lean-ci-test

install-all:
	$(UV) sync --all-extras --group dev

# Model generation
MODELS_DIR := src/vowl/contracts/models
SCHEMAS_DIR := $(MODELS_DIR)/schemas
GENERATE_SCRIPT := $(MODELS_DIR)/generate_models.py

generate-models:
	@echo "Generating Pydantic models from ODCS JSON schemas..."
	python $(GENERATE_SCRIPT) --all --schemas-dir $(SCHEMAS_DIR) --output-dir $(MODELS_DIR)
	@echo "Model generation complete!"
	@echo "Note: Generated files have '_raw' suffix. Refactored versions should be manually maintained."

# Generate model for a specific version (usage: make generate-model-version VERSION=v3.1.0)
generate-model-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make generate-model-version VERSION=v3.1.0"; \
		exit 1; \
	fi
	@SCHEMA_FILE=$(SCHEMAS_DIR)/odcs-json-schema-$(VERSION).json; \
	if [ ! -f "$$SCHEMA_FILE" ]; then \
		echo "Error: Schema file not found: $$SCHEMA_FILE"; \
		exit 1; \
	fi; \
	python $(GENERATE_SCRIPT) --schema $$SCHEMA_FILE --output-dir $(MODELS_DIR)

# Doxygen documentation
DOXYFILE := docs/Doxyfile
DOXYGEN_OUTPUT := docs/doxygen

doxygen:
	@if ! command -v doxygen >/dev/null 2>&1; then \
		echo "Error: doxygen is not installed. Install with: brew install doxygen"; \
		exit 1; \
	fi
	@if [ ! -f "$(DOXYFILE)" ]; then \
		echo "Error: $(DOXYFILE) not found"; \
		exit 1; \
	fi
	@echo "Regenerating Doxygen documentation..."
	doxygen $(DOXYFILE)
	@echo "Doxygen docs generated at $(DOXYGEN_OUTPUT)/html/index.html"

doxygen-open:
	@if [ ! -f "$(DOXYGEN_OUTPUT)/html/index.html" ]; then \
		echo "Error: Doxygen docs not found. Run 'make doxygen' first."; \
		exit 1; \
	fi
	open $(DOXYGEN_OUTPUT)/html/index.html

doxygen-clean:
	rm -rf $(DOXYGEN_OUTPUT)

# Cleaning
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf $(DOXYGEN_OUTPUT)
	rm -rf *.egg-info/
	rm -rf src/*.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true

# Testing
test:
	$(UV) run pytest tests/

# Code quality
lint:
	$(UV) run ruff check src/ tests/

lint-fix:
	$(UV) run ruff check --fix src/ tests/

format:
	$(UV) run ruff format --check src/ tests/

format-fix:
	$(UV) run ruff format src/ tests/

typecheck:
	$(UV) run ty check src/

# Markdown documentation linting/formatting (Prettier)
docs-lint:
	@if ! command -v npx >/dev/null 2>&1; then \
		echo "Error: npx is not installed. Install Node.js (e.g. brew install node)"; \
		exit 1; \
	fi
	$(PRETTIER) --check $(DOCS_GLOB)

docs-fix:
	@if ! command -v npx >/dev/null 2>&1; then \
		echo "Error: npx is not installed. Install Node.js (e.g. brew install node)"; \
		exit 1; \
	fi
	$(PRETTIER) --write $(DOCS_GLOB)

check: lint format typecheck docs-lint

# Security scanning
security-scan:
	uvx semgrep scan --error --config p/python --config p/bandit --config p/secrets .

security-audit:
	$(UV) export --frozen --format requirements-txt --all-extras --group dev --no-hashes --no-annotate --no-header | grep -v '^-e \.$$' > /tmp/vowl-requirements-audit.txt
	uvx pip-audit -r /tmp/vowl-requirements-audit.txt --no-deps --disable-pip --progress-spinner off

# Reproduce the CI TruffleHog secret scan locally (requires Docker running).
# Scans the full commit range against the default branch, exactly like CI, and
# honours the same .trufflehog-exclude.txt allowlist. Exits non-zero on a finding.
security-secrets:
	docker run --rm -v "$(CURDIR):/pwd" -w /pwd trufflesecurity/trufflehog:latest \
		git file:///pwd --since-commit=$$(git merge-base HEAD origin/main) \
		--results=verified,unknown --exclude-paths=.trufflehog-exclude.txt --no-update --fail

# Release validation
release-check: clean
	$(UV) pip install --python .venv/bin/python --upgrade build twine
	SETUPTOOLS_SCM_LOCAL_SCHEME=no-local-version $(UV) run python -m build
	$(UV) run python -m twine check dist/*

release-upload-testpypi: release-check
	python -m twine upload --repository testpypi dist/* --config-file .pypirc

release-tag:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make release-tag VERSION=1.0.1"; \
		exit 1; \
	fi
	@if git rev-parse -q --verify "refs/tags/v$(VERSION)" >/dev/null; then \
		echo "Error: tag v$(VERSION) already exists"; \
		exit 1; \
	fi; \
	git tag -a "v$(VERSION)" -m "Release v$(VERSION)"; \
	echo "Created tag v$(VERSION). setuptools-scm will use this as the package version"

# Verify (all checks + tests)
verify: check test
	@echo "All checks passed!"

# Documentation (Zensical)
docs-serve:
	$(UV) run zensical serve

docs-build:
	$(UV) run zensical build --clean

docs-clean:
	rm -rf site/
