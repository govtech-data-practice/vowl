.PHONY: help install install-dev install-lean-ci-test install-all generate-models doxygen doxygen-open doxygen-clean clean test lint security-scan security-scan-json security-audit release-check release-upload-testpypi release-upload-nexus release-upload-gitlab release-tag

UV ?= uv

# Default target
help:
	@echo "Available targets:"
	@echo "  install          Install the package with core dependencies"
	@echo "  install-dev      Install with development dependencies"
	@echo "  install-lean-ci-test Install the lean CI test dependency set"
	@echo "  install-all      Install with all optional dependencies"
	@echo "  generate-models  Generate Pydantic models from ODCS JSON schemas"
	@echo "  doxygen          Regenerate Doxygen code structure documentation"
	@echo "  doxygen-open     Open generated Doxygen documentation in browser"
	@echo "  doxygen-clean    Remove generated Doxygen documentation"
	@echo "  clean            Remove build artifacts and cache files"
	@echo "  test             Run tests"
	@echo "  lint             Run linting checks"
	@echo "  security-scan    Run Bandit security scan"
	@echo "  security-scan-json Run Bandit security scan and write JSON report"
	@echo "  security-audit   Run dependency vulnerability audit (pip-audit)"
	@echo "  release-check    Build package artifacts and run Twine validation"
	@echo "  release-upload-testpypi Upload dist artifacts to TestPyPI"
	@echo "  release-upload-nexus    Upload dist artifacts to Nexus (repository=nexus)"
	@echo "  release-upload-gitlab   Upload dist artifacts to GitLab Package Registry (repository=gitlab)"
	@echo "  release-tag       Create annotated tag after version consistency check"

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
	$(UV) run pytest test/

# Linting
lint:
	$(UV) run ruff check src/
	$(UV) run ruff format --check src/

# Security scanning
security-scan:
	$(UV) run bandit -r src/vowl

security-scan-json:
	mkdir -p reports
	$(UV) run bandit -r src/vowl -f json -o reports/bandit.json

security-audit:
	$(UV) export --frozen --format requirements-txt --all-extras --group dev --no-hashes --no-annotate --no-header | grep -v '^-e \.$$' > /tmp/vowl-requirements-audit.txt
	uvx pip-audit -r /tmp/vowl-requirements-audit.txt --no-deps --disable-pip --progress-spinner off

# Release validation
release-check: clean
	$(UV) pip install --python .venv/bin/python --upgrade build twine
	python -m build
	python -m twine check dist/*

release-upload-testpypi: release-check
	python -m twine upload --repository testpypi dist/* --config-file .pypirc 

release-upload-nexus: release-check
	python -m twine upload --repository nexus dist/* --config-file .pypirc 

release-upload-gitlab: release-check
	python -m twine upload --repository gitlab dist/* --config-file .pypirc 

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
	echo "Created tag v$(VERSION) — setuptools-scm will use this as the package version"
